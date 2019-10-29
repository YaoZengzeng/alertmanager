// Copyright 2018 Prometheus Team
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

package dispatch

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/provider"
	"github.com/prometheus/alertmanager/store"
	"github.com/prometheus/alertmanager/types"
)

// Dispatcher sorts incoming alerts into aggregation groups and
// assigns the correct notifiers to each.
// Dispatcher将所有进入的alerts进行排序并且加入aggregation groups
// 并且为每个aggregation groups赋予一个correct notifiers
type Dispatcher struct {
	route  *Route
	alerts provider.Alerts
	stage  notify.Stage

	marker  types.Marker
	timeout func(time.Duration) time.Duration

	// aggrGroups首先根据*Route进行分类，在每个分类中再以Fingerprint为key作为划分
	aggrGroups map[*Route]map[model.Fingerprint]*aggrGroup
	mtx        sync.RWMutex

	done   chan struct{}
	ctx    context.Context
	cancel func()

	logger log.Logger
}

// NewDispatcher returns a new Dispatcher.
// NewDispatcher返回一个新的Dispatcher
func NewDispatcher(
	ap provider.Alerts,
	r *Route,
	// Stage就是Pipeline
	s notify.Stage,
	mk types.Marker,
	to func(time.Duration) time.Duration,
	l log.Logger,
) *Dispatcher {
	disp := &Dispatcher{
		alerts:  ap,
		stage:   s,
		route:   r,
		marker:  mk,
		timeout: to,
		logger:  log.With(l, "component", "dispatcher"),
	}
	return disp
}

// Run starts dispatching alerts incoming via the updates channel.
// Run启动incoming alerts，通过updates channel
func (d *Dispatcher) Run() {
	d.done = make(chan struct{})

	d.mtx.Lock()
	// aggrGroups表示各个group，首先以Route作为key，之后再以group的Fingerprint作为key
	d.aggrGroups = map[*Route]map[model.Fingerprint]*aggrGroup{}
	d.mtx.Unlock()

	// 创建相应的context和cancel函数
	d.ctx, d.cancel = context.WithCancel(context.Background())

	// 调用d.alerts.Subscribe()对告警进行订阅
	d.run(d.alerts.Subscribe())
	close(d.done)
}

func (d *Dispatcher) run(it provider.AlertIterator) {
	cleanup := time.NewTicker(30 * time.Second)
	defer cleanup.Stop()

	defer it.Close()

	for {
		select {
		case alert, ok := <-it.Next():
			if !ok {
				// Iterator exhausted for some reason.
				if err := it.Err(); err != nil {
					level.Error(d.logger).Log("msg", "Error on alert update", "err", err)
				}
				return
			}

			level.Debug(d.logger).Log("msg", "Received alert", "alert", alert)

			// Log errors but keep trying.
			if err := it.Err(); err != nil {
				level.Error(d.logger).Log("msg", "Error on alert update", "err", err)
				continue
			}

			// 对于某个alert，在dispatcher中进行路由匹配
			for _, r := range d.route.Match(alert.Labels) {
				d.processAlert(alert, r)
			}

		case <-cleanup.C:
			// 定期对空的aggrGroups进行删除
			// 每隔30s
			d.mtx.Lock()

			for _, groups := range d.aggrGroups {
				for _, ag := range groups {
					// 如果aggrGroups为空就直接删除
					if ag.empty() {
						ag.stop()
						delete(groups, ag.fingerprint())
					}
				}
			}

			d.mtx.Unlock()

		case <-d.ctx.Done():
			return
		}
	}
}

// AlertGroup represents how alerts exist within an aggrGroup.
// AlertGroup代表了alerts在一个aggGroup中存在的状态
type AlertGroup struct {
	Alerts   []*types.Alert
	Labels   model.LabelSet
	Receiver string
}

type AlertGroups []*AlertGroup

func (ag AlertGroups) Swap(i, j int)      { ag[i], ag[j] = ag[j], ag[i] }
func (ag AlertGroups) Less(i, j int) bool { return ag[i].Labels.Before(ag[j].Labels) }
func (ag AlertGroups) Len() int           { return len(ag) }

// Groups returns a slice of AlertGroups from the dispatcher's internal state.
func (d *Dispatcher) Groups(routeFilter func(*Route) bool, alertFilter func(*types.Alert, time.Time) bool) (AlertGroups, map[model.Fingerprint][]string) {
	groups := AlertGroups{}

	d.mtx.RLock()
	defer d.mtx.RUnlock()

	seen := map[model.Fingerprint]*AlertGroup{}

	// Keep a list of receivers for an alert to prevent checking each alert
	// again against all routes. The alert has already matched against this
	// route on ingestion.
	receivers := map[model.Fingerprint][]string{}

	for route, ags := range d.aggrGroups {
		if !routeFilter(route) {
			continue
		}

		for _, ag := range ags {
			receiver := route.RouteOpts.Receiver
			alertGroup, ok := seen[ag.fingerprint()]
			if !ok {
				alertGroup = &AlertGroup{
					Labels:   ag.labels,
					Receiver: receiver,
				}

				seen[ag.fingerprint()] = alertGroup
			}

			now := time.Now()

			alerts := ag.alerts.List()
			filteredAlerts := make([]*types.Alert, 0, len(alerts))
			for _, a := range alerts {
				if !alertFilter(a, now) {
					continue
				}

				fp := a.Fingerprint()
				if r, ok := receivers[fp]; ok {
					// Receivers slice already exists. Add
					// the current receiver to the slice.
					receivers[fp] = append(r, receiver)
				} else {
					// First time we've seen this alert fingerprint.
					// Initialize a new receivers slice.
					receivers[fp] = []string{receiver}
				}

				filteredAlerts = append(filteredAlerts, a)
			}
			if len(filteredAlerts) == 0 {
				continue
			}
			alertGroup.Alerts = filteredAlerts

			groups = append(groups, alertGroup)
		}
	}

	sort.Sort(groups)

	return groups, receivers
}

// Stop the dispatcher.
func (d *Dispatcher) Stop() {
	if d == nil || d.cancel == nil {
		return
	}
	d.cancel()
	d.cancel = nil

	<-d.done
}

// notifyFunc is a function that performs notification for the alert
// with the given fingerprint. It aborts on context cancelation.
// Returns false iff notifying failed.
// notifyFunc是用于通知给定fingerprint的alert的函数
// 如果发生context cancelation或者通知失败就会返回false
type notifyFunc func(context.Context, ...*types.Alert) bool

// processAlert determines in which aggregation group the alert falls
// and inserts it.
// processAlert决定一个alert应该落到哪个aggregation group并且插入它
func (d *Dispatcher) processAlert(alert *types.Alert, route *Route) {
	// 从labels中筛选出group labels
	groupLabels := getGroupLabels(alert, route)

	// 获取group labels的fingerprint
	fp := groupLabels.Fingerprint()

	d.mtx.Lock()
	defer d.mtx.Unlock()

	// route --> group --> alert
	group, ok := d.aggrGroups[route]
	if !ok {
		group = map[model.Fingerprint]*aggrGroup{}
		d.aggrGroups[route] = group
	}

	// If the group does not exist, create it.
	// 如果group不存在，则创建它
	ag, ok := group[fp]
	if !ok {
		// 创建新的聚合组
		ag = newAggrGroup(d.ctx, groupLabels, route, d.timeout, d.logger)
		group[fp] = ag

		// ag.run()的参数是通知函数,notify
		go ag.run(func(ctx context.Context, alerts ...*types.Alert) bool {
			// 执行stage的各个阶段用于发送通知
			_, _, err := d.stage.Exec(ctx, d.logger, alerts...)
			if err != nil {
				lvl := level.Error(d.logger)
				if ctx.Err() == context.Canceled {
					// It is expected for the context to be canceled on
					// configuration reload or shutdown. In this case, the
					// message should only be logged at the debug level.
					lvl = level.Debug(d.logger)
				}
				lvl.Log("msg", "Notify for alerts failed", "num_alerts", len(alerts), "err", err)
			}
			return err == nil
		})
	}

	// 在alert group中加入alert
	ag.insert(alert)
}

func getGroupLabels(alert *types.Alert, route *Route) model.LabelSet {
	groupLabels := model.LabelSet{}
	for ln, lv := range alert.Labels {
		// 如果alert中有labels在RouteOpts的GroupBy中，或者RouteOpts有GroupByAll
		// 则将该label加入groupLabels
		if _, ok := route.RouteOpts.GroupBy[ln]; ok || route.RouteOpts.GroupByAll {
			groupLabels[ln] = lv
		}
	}

	// 根据GroupBy筛选出用于group的labels
	return groupLabels
}

// aggrGroup aggregates alert fingerprints into groups to which a
// common set of routing options applies.
// It emits notifications in the specified intervals.
// aggrGroup将alert fingerprints聚合到groups，这些groups会应用同样的routing options
// 它在指定的intervals发射notifications
type aggrGroup struct {
	labels   model.LabelSet
	opts     *RouteOpts
	logger   log.Logger
	routeKey string

	// 用于存储这个group中的所有告警
	alerts  *store.Alerts
	ctx     context.Context
	cancel  func()
	done    chan struct{}
	// next对应GroupWait
	next    *time.Timer
	timeout func(time.Duration) time.Duration

	mtx        sync.RWMutex
	hasFlushed bool
}

// newAggrGroup returns a new aggregation group.
// newAggrGroup返回一个新的aggregation group
func newAggrGroup(ctx context.Context, labels model.LabelSet, r *Route, to func(time.Duration) time.Duration, logger log.Logger) *aggrGroup {
	if to == nil {
		to = func(d time.Duration) time.Duration { return d }
	}
	ag := &aggrGroup{
		labels:   labels,
		routeKey: r.Key(),
		// opts为route group
		opts:     &r.RouteOpts,
		// timeout是一个函数，根据输入的time.Duration，返回一个time.Duration
		timeout:  to,
		// 创建新的alert store
		alerts:   store.NewAlerts(15 * time.Minute),
	}
	ag.ctx, ag.cancel = context.WithCancel(ctx)
	ag.alerts.Run(ag.ctx)

	ag.logger = log.With(logger, "aggrGroup", ag)

	// Set an initial one-time wait before flushing
	// the first batch of notifications.
	// 设置一个初始的one-time wait，在刷新第一批的notification之前
	ag.next = time.NewTimer(ag.opts.GroupWait)

	return ag
}

func (ag *aggrGroup) fingerprint() model.Fingerprint {
	return ag.labels.Fingerprint()
}

func (ag *aggrGroup) GroupKey() string {
	return fmt.Sprintf("%s:%s", ag.routeKey, ag.labels)
}

func (ag *aggrGroup) String() string {
	return ag.GroupKey()
}

func (ag *aggrGroup) run(nf notifyFunc) {
	ag.done = make(chan struct{})

	defer close(ag.done)
	defer ag.next.Stop()

	for {
		select {
		// ag.next.C开始是groupwait，后来是groupinterval
		case now := <-ag.next.C:
			// 将信息都存放在group中
			// Give the notifications time until the next flush to
			// finish before terminating them.
			// 给定notifications time直到下一次flush结束，在终止它们之前
			ctx, cancel := context.WithTimeout(ag.ctx, ag.timeout(ag.opts.GroupInterval))

			// The now time we retrieve from the ticker is the only reliable
			// point of time reference for the subsequent notification pipeline.
			// 从ticker中取得的now time是唯一可靠的time reference对于之后的notification pipeline
			// Calculating the current time directly is prone to flaky behavior,
			// 直接计算current time会易于产生flaky behavior
			// which usually only becomes apparent in tests.
			// 在context中添加now
			ctx = notify.WithNow(ctx, now)

			// Populate context with information needed along the pipeline.
			// 在context中填充pipeline一路所需的信息
			// 包括group key，group labels，receiver name以及repeat interval
			ctx = notify.WithGroupKey(ctx, ag.GroupKey())
			ctx = notify.WithGroupLabels(ctx, ag.labels)
			ctx = notify.WithReceiverName(ctx, ag.opts.Receiver)
			ctx = notify.WithRepeatInterval(ctx, ag.opts.RepeatInterval)

			// Wait the configured interval before calling flush again.
			ag.mtx.Lock()
			// 在再次调用flush之前，将next重置为group interval
			ag.next.Reset(ag.opts.GroupInterval)
			ag.hasFlushed = true
			ag.mtx.Unlock()

			ag.flush(func(alerts ...*types.Alert) bool {
				// 调用通知函数，发送通知
				return nf(ctx, alerts...)
			})

			cancel()

		case <-ag.ctx.Done():
			return
		}
	}
}

func (ag *aggrGroup) stop() {
	// Calling cancel will terminate all in-process notifications
	// and the run() loop.
	ag.cancel()
	<-ag.done
}

// insert inserts the alert into the aggregation group.
// insert把alert插入aggregation group
func (ag *aggrGroup) insert(alert *types.Alert) {
	// 在ag.alerts中设置alert
	if err := ag.alerts.Set(alert); err != nil {
		level.Error(ag.logger).Log("msg", "error on set alert", "err", err)
	}

	// Immediately trigger a flush if the wait duration for this
	// alert is already over.
	// 马上出发一个flush，如果这个alert的wait duration已经结束了
	ag.mtx.Lock()
	defer ag.mtx.Unlock()
	// 如果ag没有flush过并且alert的起始时间+groupwait小于当前时间，则立即触发flush
	if !ag.hasFlushed && alert.StartsAt.Add(ag.opts.GroupWait).Before(time.Now()) {
		ag.next.Reset(0)
	}
}

func (ag *aggrGroup) empty() bool {
	return ag.alerts.Empty()
}

// flush sends notifications for all new alerts.
// flush发送notifications到所有新的alerts
// 将notify函数作为参数传入
func (ag *aggrGroup) flush(notify func(...*types.Alert) bool) {
	if ag.empty() {
		return
	}

	var (
		// 获取aggregation group中的所有alerts
		alerts      = ag.alerts.List()
		alertsSlice = make(types.AlertSlice, 0, len(alerts))
		now         = time.Now()
	)
	for _, alert := range alerts {
		a := *alert
		// Ensure that alerts don't resolve as time move forwards.
		// 确保随着时间的推移，alert没有被resolve
		if !a.ResolvedAt(now) {
			// 如果alert还没有resolve，则将EndsAt设置为空
			a.EndsAt = time.Time{}
		}
		alertsSlice = append(alertsSlice, &a)
	}
	sort.Stable(alertsSlice)

	level.Debug(ag.logger).Log("msg", "flushing", "alerts", fmt.Sprintf("%v", alertsSlice))

	// 调用notify函数对alertsSlice进行通知
	// 后期每隔groupinterval就进行flush
	if notify(alertsSlice...) {
		// 如果flush成功了
		for _, a := range alertsSlice {
			// Only delete if the fingerprint has not been inserted
			// again since we notified about it.
			// 只有在fingerprint在我们通知它之后没有再次插入，才删除它
			fp := a.Fingerprint()
			got, err := ag.alerts.Get(fp)
			if err != nil {
				// This should only happen if the Alert was
				// deleted from the store during the flush.
				// 这之会在flush期间Alert被删除，这只情况下发生
				level.Error(ag.logger).Log("msg", "failed to get alert", "err", err)
				continue
			}
			// 如果alert已经被resolved并且获取到的UpdatedAt的时间和alert的Updated的时间相等
			// 即表示没有更新，则将它从alerts中删除
			if a.Resolved() && got.UpdatedAt == a.UpdatedAt {
				if err := ag.alerts.Delete(fp); err != nil {
					level.Error(ag.logger).Log("msg", "error on delete alert", "err", err)
				}
			}
		}
	}
}
