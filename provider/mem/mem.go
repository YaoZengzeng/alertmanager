// Copyright 2016 Prometheus Team
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

package mem

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/provider"
	"github.com/prometheus/alertmanager/store"
	"github.com/prometheus/alertmanager/types"
)

const alertChannelLength = 200

// Alerts gives access to a set of alerts. All methods are goroutine-safe.
// Alerts提供了对于一系列alerts的访问，所有的方法都是线程安全的
// Alerts中的数据只能被添加，不能主动删除，只能靠它自己GC
type Alerts struct {
	cancel context.CancelFunc

	mtx       sync.Mutex
	alerts    *store.Alerts
	listeners map[int]listeningAlerts
	next      int

	logger log.Logger
}

type listeningAlerts struct {
	alerts chan *types.Alert
	done   chan struct{}
}

// NewAlerts returns a new alert provider.
// NewAlerts返回一个新的alert provider
func NewAlerts(ctx context.Context, m types.Marker, intervalGC time.Duration, l log.Logger) (*Alerts, error) {
	ctx, cancel := context.WithCancel(ctx)
	a := &Alerts{
		alerts:    store.NewAlerts(intervalGC),
		cancel:    cancel,
		listeners: map[int]listeningAlerts{},
		next:      0,
		logger:    log.With(l, "component", "provider"),
	}
	a.alerts.SetGCCallback(func(alerts []*types.Alert) {
		for _, alert := range alerts {
			// As we don't persist alerts, we no longer consider them after
			// they are resolved. Alerts waiting for resolved notifications are
			// held in memory in aggregation groups redundantly.
			// 因为我们不会持久化alerts，当它们被resolved之后我们不会再考虑它们
			// 等待resolved notification的Alerts会冗余地被存放在内存的聚合组中

			// 从marker中将alert删除
			m.Delete(alert.Fingerprint())
		}

		a.mtx.Lock()
		for i, l := range a.listeners {
			select {
			// 遍历Alerts的各个listeners，如果listener已经done了的话，从a.listeners中删除
			// 再关闭listener的alerts这个channel
			case <-l.done:
				delete(a.listeners, i)
				close(l.alerts)
			default:
				// listener is not closed yet, hence proceed.
			}
		}
		a.mtx.Unlock()
	})
	a.alerts.Run(ctx)

	return a, nil
}

// Close the alert provider.
func (a *Alerts) Close() {
	if a.cancel != nil {
		a.cancel()
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Subscribe returns an iterator over active alerts that have not been
// resolved and successfully notified about.
// Subscribe返回一个还没有被resolved并且成功notified about的active alerts的iterator
// They are not guaranteed to be in chronological order.
// 它们不保证按照顺序返回
func (a *Alerts) Subscribe() provider.AlertIterator {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	var (
		done   = make(chan struct{})
		alerts = a.alerts.List()
		// 总是能装下所有的alerts
		ch     = make(chan *types.Alert, max(len(alerts), alertChannelLength))
	)

	// 先将alerts全都发送到channel中
	for _, a := range alerts {
		ch <- a
	}

	// 将相应的订阅者加入
	// a.next用于编号，从零开始
	a.listeners[a.next] = listeningAlerts{alerts: ch, done: done}
	a.next++

	return provider.NewAlertIterator(ch, done, nil)
}

// GetPending returns an iterator over all the alerts that have
// pending notifications.
// GetPending返回一个所有有着pending notifications的alerts的iterator
func (a *Alerts) GetPending() provider.AlertIterator {
	var (
		ch   = make(chan *types.Alert, alertChannelLength)
		done = make(chan struct{})
	)

	go func() {
		defer close(ch)

		for _, a := range a.alerts.List() {
			select {
			case ch <- a:
			case <-done:
				return
			}
		}
	}()

	return provider.NewAlertIterator(ch, done, nil)
}

// Get returns the alert for a given fingerprint.
func (a *Alerts) Get(fp model.Fingerprint) (*types.Alert, error) {
	return a.alerts.Get(fp)
}

// Put adds the given alert to the set.
// Put将给定的alert加入集合
func (a *Alerts) Put(alerts ...*types.Alert) error {

	for _, alert := range alerts {
		// 得到alert的Fingerprint()
		fp := alert.Fingerprint()

		// Check that there's an alert existing within the store before
		// trying to merge.
		// 检查store中是否已经存在alert，在试着合并之前
		if old, err := a.alerts.Get(fp); err == nil {
			// Merge alerts if there is an overlap in activity range.
			// 如果在alerts之间有overlap，则合并alerts
			// 如果新的alert的end在老的alert的start和end之间
			// 或者新的alert的start在老的alert的start和end之间
			if (alert.EndsAt.After(old.StartsAt) && alert.EndsAt.Before(old.EndsAt)) ||
				(alert.StartsAt.After(old.StartsAt) && alert.StartsAt.Before(old.EndsAt)) {
				alert = old.Merge(alert)
			}
		}

		// 即使有label相同的alert也会设置并且分发给listener
		if err := a.alerts.Set(alert); err != nil {
			level.Error(a.logger).Log("msg", "error on set alert", "err", err)
			continue
		}

		a.mtx.Lock()
		// 遍历listener，将alert进行分发
		for _, l := range a.listeners {
			select {
			// 如果存在listener的话，将新的alert发送给各个订阅者
			case l.alerts <- alert:
			case <-l.done:
			}
		}
		a.mtx.Unlock()
	}

	return nil
}
