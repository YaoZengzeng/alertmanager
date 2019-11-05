// Copyright 2015 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package inhibit

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/provider"
	"github.com/prometheus/alertmanager/store"
	"github.com/prometheus/alertmanager/types"
)

// An Inhibitor determines whether a given label set is muted based on the
// currently active alerts and a set of inhibition rules. It implements the
// Muter interface.
// 一个Inhibitor定义了一个给定的label set是否被抑制，基于当前活跃的alerts以及一系列的抑制规则
// 它实现了Muter接口
type Inhibitor struct {
	alerts provider.Alerts
	rules  []*InhibitRule
	marker types.Marker
	logger log.Logger

	mtx    sync.RWMutex
	cancel func()
}

// NewInhibitor returns a new Inhibitor.
// NewInhibitor返回一个新的Inhibitor
func NewInhibitor(ap provider.Alerts, rs []*config.InhibitRule, mk types.Marker, logger log.Logger) *Inhibitor {
	ih := &Inhibitor{
		alerts: ap,
		marker: mk,
		logger: logger,
	}
	// 构建InhibitRule
	for _, cr := range rs {
		r := NewInhibitRule(cr)
		ih.rules = append(ih.rules, r)
	}
	return ih
}

func (ih *Inhibitor) run(ctx context.Context) {
	// 订阅alerts
	it := ih.alerts.Subscribe()
	defer it.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case a := <-it.Next():
			// 遍历订阅的alert
			if err := it.Err(); err != nil {
				level.Error(ih.logger).Log("msg", "Error iterating alerts", "err", err)
				continue
			}
			// Update the inhibition rules' cache.
			// 更新inhibition rules的cache
			for _, r := range ih.rules {
				// 如果alert和某条rule的SourceMatchers相匹配，则将它加入到这条rule的cache中
				if r.SourceMatchers.Match(a.Labels) {
					if err := r.scache.Set(a); err != nil {
						level.Error(ih.logger).Log("msg", "error on set alert", "err", err)
					}
				}
			}
		}
	}
}

// Run the Inhibitor's background processing.
// Run执行Inhibitor的后台处理
func (ih *Inhibitor) Run() {
	var (
		g   run.Group
		ctx context.Context
	)

	ih.mtx.Lock()
	// 当执行ih.cancel，则整个Inhibitor被Cancel
	ctx, ih.cancel = context.WithCancel(context.Background())
	ih.mtx.Unlock()
	runCtx, runCancel := context.WithCancel(ctx)

	for _, rule := range ih.rules {
		// 运行每个inhibitor的rules
		// scache中包含的是符合source的alerts
		rule.scache.Run(runCtx)
	}

	g.Add(func() error {
		ih.run(runCtx)
		return nil
	}, func(err error) {
		runCancel()
	})

	if err := g.Run(); err != nil {
		level.Warn(ih.logger).Log("msg", "error running inhibitor", "err", err)
	}
}

// Stop the Inhibitor's background processing.
func (ih *Inhibitor) Stop() {
	if ih == nil {
		return
	}

	ih.mtx.RLock()
	defer ih.mtx.RUnlock()
	if ih.cancel != nil {
		ih.cancel()
	}
}

// Mutes returns true iff the given label set is muted. It implements the Muter
// interface.
// Mutes返回true，如果给定的label set被静音了，它实现了Muter接口
func (ih *Inhibitor) Mutes(lset model.LabelSet) bool {
	fp := lset.Fingerprint()

	for _, r := range ih.rules {
		// 首先匹配target matcher
		if !r.TargetMatchers.Match(lset) {
			// If target side of rule doesn't match, we don't need to look any further.
			// 如果一个rule的target side都不满足，则没有必要进一步比较了
			continue
		}
		// If we are here, the target side matches. If the source side matches, too, we
		// need to exclude inhibiting alerts for which the same is true.
		// 如果我们到了这里，说明target side匹配，如果source side也匹配，我们需要排除inhibiting alerts
		if inhibitedByFP, eq := r.hasEqual(lset, r.SourceMatchers.Match(lset)); eq {
			// 第一个参数被第二个参数抑制
			ih.marker.SetInhibited(fp, inhibitedByFP.String())
			return true
		}
	}
	// 将这个fingerprint设置为inhibited
	ih.marker.SetInhibited(fp)

	return false
}

// An InhibitRule specifies that a class of (source) alerts should inhibit
// notifications for another class of (target) alerts if all specified matching
// labels are equal between the two alerts. This may be used to inhibit alerts
// from sending notifications if their meaning is logically a subset of a
// higher-level alert.
// 一个InhibitRule指定了一系列的alerts应该抑制另一系列的alerts，如果两个alerts之间所有指定的
// matching labels都匹配，它用来抑制那些更高级别的alert的子集发送notifications
type InhibitRule struct {
	// The set of Filters which define the group of source alerts (which inhibit
	// the target alerts).
	// 一系列的Filters用来定义一系列的source alerts（用来抑制target alerts）
	SourceMatchers types.Matchers
	// The set of Filters which define the group of target alerts (which are
	// inhibited by the source alerts).
	// 一系列的Filters用来定义一系列的target alerts（被source alerts抑制）
	TargetMatchers types.Matchers
	// A set of label names whose label values need to be identical in source and
	// target alerts in order for the inhibition to take effect.
	// 一系列的label names，它们的值在source和target中必须相等，为了保证inhibition生效
	Equal map[model.LabelName]struct{}

	// Cache of alerts matching source labels.
	// 匹配source labels的alerts
	scache *store.Alerts
}

// NewInhibitRule returns a new InhibitRule based on a configuration definition.
// NewInhibitRule基于配置返回一个新的InhibitRule
func NewInhibitRule(cr *config.InhibitRule) *InhibitRule {
	var (
		sourcem types.Matchers
		targetm types.Matchers
	)

	// 根据SourceMatch，SourceMatchRE，TargetMatch和TargetMatchRE构建Matcher
	for ln, lv := range cr.SourceMatch {
		sourcem = append(sourcem, types.NewMatcher(model.LabelName(ln), lv))
	}
	for ln, lv := range cr.SourceMatchRE {
		sourcem = append(sourcem, types.NewRegexMatcher(model.LabelName(ln), lv.Regexp))
	}

	for ln, lv := range cr.TargetMatch {
		targetm = append(targetm, types.NewMatcher(model.LabelName(ln), lv))
	}
	for ln, lv := range cr.TargetMatchRE {
		targetm = append(targetm, types.NewRegexMatcher(model.LabelName(ln), lv.Regexp))
	}

	equal := map[model.LabelName]struct{}{}
	for _, ln := range cr.Equal {
		equal[ln] = struct{}{}
	}

	return &InhibitRule{
		// 一系列的matchers
		SourceMatchers: sourcem,
		TargetMatchers: targetm,
		// 一系列需要相等的label
		Equal:          equal,
		// 构建store.NewAlerts进行缓存
		scache:         store.NewAlerts(15 * time.Minute),
	}
}

// hasEqual checks whether the source cache contains alerts matching the equal
// labels for the given label set. If so, the fingerprint of one of those alerts
// is returned. If excludeTwoSidedMatch is true, alerts that match both the
// source and the target side of the rule are disregarded.
// hasEqual检查source cache是否包含alerts，equal labels和给定的label set相匹配，如果是的话
// 这些alerts的其中一个fingerprint会返回
// 如果excludeTwoSideMatch，则同时匹配source和target side的alerts会被忽略
func (r *InhibitRule) hasEqual(lset model.LabelSet, excludeTwoSidedMatch bool) (model.Fingerprint, bool) {
Outer:
	for _, a := range r.scache.List() {
		// The cache might be stale and contain resolved alerts.
		// cache可能太老了并且包含了resolved alerts
		if a.Resolved() {
			continue
		}
		// rule里面equal中的label必须匹配
		for n := range r.Equal {
			// 已经firing的alerts和lset的equal指定的label必须匹配
			if a.Labels[n] != lset[n] {
				continue Outer
			}
		}
		// 如果缓存中的alert也和TargetMatcher相匹配，则忽略
		// 双向匹配，即缓存中的alert既匹配source也匹配target，而目标的alert也既匹配target也匹配source
		if excludeTwoSidedMatch && r.TargetMatchers.Match(a.Labels) {
			continue Outer
		}
		// 如果到这一步，说明已经完全匹配了，可以将进行抑制的alert的fingerprint返回
		return a.Fingerprint(), true
	}
	return model.Fingerprint(0), false
}
