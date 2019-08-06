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

package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/common/model"
)

var (
	// ErrNotFound is returned if a Store cannot find the Alert.
	ErrNotFound = errors.New("alert not found")
)

// Alerts provides lock-coordinated to an in-memory map of alerts, keyed by
// their fingerprint. Resolved alerts are removed from the map based on
// gcInterval. An optional callback can be set which receives a slice of all
// resolved alerts that have been removed.
// Alerts提供了对于在内存中的map of alerts的lock-coordinated
// key是它们的fingerprint，已经被resolved alerts会基于gcInterval从map中移除
// 可以增加一个可选的callback，能够用它接收到所有已经被移除的resolved alerts
type Alerts struct {
	// 一个Alerts有垃圾回收的时间
	gcInterval time.Duration

	sync.Mutex
	c  map[model.Fingerprint]*types.Alert
	cb func([]*types.Alert)
}

// NewAlerts returns a new Alerts struct.
// NewAlerts返回一个新的Alerts结构
func NewAlerts(gcInterval time.Duration) *Alerts {
	if gcInterval == 0 {
		// 默认的gc的时间间隔为1min
		gcInterval = time.Minute
	}

	a := &Alerts{
		c:          make(map[model.Fingerprint]*types.Alert),
		cb:         func(_ []*types.Alert) {},
		gcInterval: gcInterval,
	}

	return a
}

// SetGCCallback sets a GC callback to be executed after each GC.
// SetGCCallback设置了在每次GC之后需要执行的回调函数
func (a *Alerts) SetGCCallback(cb func([]*types.Alert)) {
	a.Lock()
	defer a.Unlock()

	a.cb = cb
}

// Run starts the GC loop.
// Run启动GC loop
func (a *Alerts) Run(ctx context.Context) {
	go func(t *time.Ticker) {
		for {
			select {
				// 等待context结束
			case <-ctx.Done():
				return
			case <-t.C:
				// 定时GC
				a.gc()
			}
		}
	}(time.NewTicker(a.gcInterval))
}

// 定期把已经resolved的alert给gc掉
func (a *Alerts) gc() {
	a.Lock()
	defer a.Unlock()

	var resolved []*types.Alert
	for fp, alert := range a.c {
		if alert.Resolved() {
			delete(a.c, fp)
			resolved = append(resolved, alert)
		}
	}
	a.cb(resolved)
}

// Get returns the Alert with the matching fingerprint, or an error if it is
// not found.
func (a *Alerts) Get(fp model.Fingerprint) (*types.Alert, error) {
	a.Lock()
	defer a.Unlock()

	alert, prs := a.c[fp]
	if !prs {
		return nil, ErrNotFound
	}
	return alert, nil
}

// Set unconditionally sets the alert in memory.
// Set无条件地将alert设置在memory中
func (a *Alerts) Set(alert *types.Alert) error {
	a.Lock()
	defer a.Unlock()

	// 根据fingerprint设置相应的alert
	a.c[alert.Fingerprint()] = alert
	return nil
}

// Delete removes the Alert with the matching fingerprint from the store.
func (a *Alerts) Delete(fp model.Fingerprint) error {
	a.Lock()
	defer a.Unlock()

	delete(a.c, fp)
	return nil
}

// List returns a slice of Alerts currently held in memory.
// List返回当前存储在内存中的一系列Alerts
func (a *Alerts) List() []*types.Alert {
	a.Lock()
	defer a.Unlock()

	alerts := make([]*types.Alert, 0, len(a.c))
	for _, alert := range a.c {
		alerts = append(alerts, alert)
	}

	return alerts
}

// Empty returns true if the store is empty.
func (a *Alerts) Empty() bool {
	a.Lock()
	defer a.Unlock()

	return len(a.c) == 0
}
