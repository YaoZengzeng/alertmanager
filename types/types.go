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

package types

import (
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
)

// AlertState is used as part of AlertStatus.
type AlertState string

// Possible values for AlertState.
const (
	// 可能的AlertState的数目为"unprocessed", "active"或者"suppressed"
	AlertStateUnprocessed AlertState = "unprocessed"
	AlertStateActive      AlertState = "active"
	AlertStateSuppressed  AlertState = "suppressed"
)

// AlertStatus stores the state of an alert and, as applicable, the IDs of
// silences silencing the alert and of other alerts inhibiting the alert. Note
// that currently, SilencedBy is supposed to be the complete set of the relevant
// silences while InhibitedBy may contain only a subset of the inhibiting alerts
// – in practice exactly one ID. (This somewhat confusing semantics might change
// in the future.)
// AlertStatus存储了一个alert的状态，包括静默了这个alert的silences IDs以及其他已知了这个
// alert的其他alert，需要注意的是，SilencedBy应该包含所有的相关的silences，然而InhibitedBy
// 可能只包含inhibiting alerts的一个子集，在实践中只有一个ID
type AlertStatus struct {
	State       AlertState `json:"state"`
	SilencedBy  []string   `json:"silencedBy"`
	InhibitedBy []string   `json:"inhibitedBy"`

	silencesVersion int
}

// Marker helps to mark alerts as silenced and/or inhibited.
// All methods are goroutine-safe.
// Marker用来将alerts标记为silenced以及/或者inhibited
// 所有的方法都是goroutine-safe的
type Marker interface {
	// SetActive sets the provided alert to AlertStateActive and deletes all
	// SilencedBy and InhibitedBy entries.
	// SetActive将给定的alert设置为AlertStateActive并且移除所有SilencedBy以及
	// InhibitedBy entries
	SetActive(alert model.Fingerprint)
	// SetSilenced replaces the previous SilencedBy by the provided IDs of
	// silences, including the version number of the silences state. The set
	// of provided IDs is supposed to represent the complete set of relevant
	// silences. If no ID is provided and InhibitedBy is already empty, this
	// call is equivalent to SetActive. Otherwise, it sets
	// AlertStateSuppressed.
	SetSilenced(alert model.Fingerprint, version int, silenceIDs ...string)
	// SetInhibited replaces the previous InhibitedBy by the provided IDs of
	// alerts. In contrast to SetSilenced, the set of provided IDs is not
	// expected to represent the complete set of inhibiting alerts. (In
	// practice, this method is only called with one or zero IDs. However,
	// this expectation might change in the future.) If no ID is provided and
	// SilencedBy is already empty, this call is equivalent to
	// SetActive. Otherwise, it sets AlertStateSuppressed.
	SetInhibited(alert model.Fingerprint, alertIDs ...string)

	// Count alerts of the given state(s). With no state provided, count all
	// alerts.
	// Count统计给定的state的数目，如果没有提供state，计算所有的alerts
	Count(...AlertState) int

	// Status of the given alert.
	// Status返回给定的alert的状态
	Status(model.Fingerprint) AlertStatus
	// Delete the given alert.
	// 删除给定的alert
	Delete(model.Fingerprint)

	// Various methods to inquire if the given alert is in a certain
	// AlertState. Silenced also returns all the silencing silences, while
	// Inhibited may return only a subset of inhibiting alerts. Silenced
	// also returns the version of the silences state the result is based
	// on.
	// 各种方法用于查询给定的alert是否处于某个AlertState，Silenced返回所有相关的silences
	// 而Inhibited可能只会返回inhibiting alerts的一个子集，Silenced也会返回result基于的
	// silences的状态
	Unprocessed(model.Fingerprint) bool
	Active(model.Fingerprint) bool
	Silenced(model.Fingerprint) ([]string, int, bool)
	Inhibited(model.Fingerprint) ([]string, bool)
}

// NewMarker returns an instance of a Marker implementation.
// NewMarker返回一个Marker实现的一个实例
func NewMarker(r prometheus.Registerer) Marker {
	m := &memMarker{
		m: map[model.Fingerprint]*AlertStatus{},
	}

	m.registerMetrics(r)

	return m
}

type memMarker struct {
	// 以alert的Fingerprint作为key
	m map[model.Fingerprint]*AlertStatus

	mtx sync.RWMutex
}

func (m *memMarker) registerMetrics(r prometheus.Registerer) {
	newAlertMetricByState := func(st AlertState) prometheus.GaugeFunc {
		return prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name:        "alertmanager_alerts",
				Help:        "How many alerts by state.",
				ConstLabels: prometheus.Labels{"state": string(st)},
			},
			func() float64 {
				return float64(m.Count(st))
			},
		)
	}

	alertsActive := newAlertMetricByState(AlertStateActive)
	alertsSuppressed := newAlertMetricByState(AlertStateSuppressed)

	r.MustRegister(alertsActive)
	r.MustRegister(alertsSuppressed)
}

// Count implements Marker.
func (m *memMarker) Count(states ...AlertState) int {
	count := 0

	m.mtx.RLock()
	defer m.mtx.RUnlock()

	if len(states) == 0 {
		count = len(m.m)
	} else {
		for _, status := range m.m {
			for _, state := range states {
				if status.State == state {
					count++
				}
			}
		}
	}
	return count
}

// SetSilenced implements Marker.
func (m *memMarker) SetSilenced(alert model.Fingerprint, version int, ids ...string) {
	m.mtx.Lock()

	s, found := m.m[alert]
	if !found {
		s = &AlertStatus{}
		m.m[alert] = s
	}
	// 重新设置silencesVersion
	s.silencesVersion = version

	// If there are any silence or alert IDs associated with the
	// fingerprint, it is suppressed. Otherwise, set it to
	// AlertStateUnprocessed.
	// 如果有silence或者alert IDs和fingerprint相绑定，则设置为suppressed
	// 否则，设置为AlertStateUnprocessed
	if len(ids) == 0 && len(s.InhibitedBy) == 0 {
		m.mtx.Unlock()
		m.SetActive(alert)
		return
	}

	s.State = AlertStateSuppressed
	s.SilencedBy = ids

	m.mtx.Unlock()
}

// SetInhibited implements Marker.
func (m *memMarker) SetInhibited(alert model.Fingerprint, ids ...string) {
	m.mtx.Lock()

	s, found := m.m[alert]
	if !found {
		s = &AlertStatus{}
		m.m[alert] = s
	}

	// If there are any silence or alert IDs associated with the
	// fingerprint, it is suppressed. Otherwise, set it to
	// AlertStateUnprocessed.
	// 如果有任何的silence或者alert IDs和fingerprint相关，则它被抑制
	// 否则，将它设置为AlertStateUnprocessed
	if len(ids) == 0 && len(s.SilencedBy) == 0 {
		m.mtx.Unlock()
		m.SetActive(alert)
		return
	}

	// 设置为suppressed
	s.State = AlertStateSuppressed
	s.InhibitedBy = ids

	m.mtx.Unlock()
}

// SetActive implements Marker.
func (m *memMarker) SetActive(alert model.Fingerprint) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	s, found := m.m[alert]
	if !found {
		s = &AlertStatus{
			SilencedBy:  []string{},
			InhibitedBy: []string{},
		}
		m.m[alert] = s
	}

	// 将状态设置为active，把silence和inhibited都消除
	s.State = AlertStateActive
	s.SilencedBy = []string{}
	s.InhibitedBy = []string{}
}

// Status implements Marker.
// Status实现了Marker
func (m *memMarker) Status(alert model.Fingerprint) AlertStatus {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	// 根据fingerprint，在marker中找到相应的printer
	s, found := m.m[alert]
	if !found {
		// 如果没有在Marker中找到，则设置为unprocessed
		s = &AlertStatus{
			State:       AlertStateUnprocessed,
			SilencedBy:  []string{},
			InhibitedBy: []string{},
		}
	}
	return *s
}

// Delete implements Marker.
func (m *memMarker) Delete(alert model.Fingerprint) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	delete(m.m, alert)
}

// Unprocessed implements Marker.
func (m *memMarker) Unprocessed(alert model.Fingerprint) bool {
	return m.Status(alert).State == AlertStateUnprocessed
}

// Active implements Marker.
func (m *memMarker) Active(alert model.Fingerprint) bool {
	return m.Status(alert).State == AlertStateActive
}

// Inhibited implements Marker.
func (m *memMarker) Inhibited(alert model.Fingerprint) ([]string, bool) {
	s := m.Status(alert)
	return s.InhibitedBy,
		s.State == AlertStateSuppressed && len(s.InhibitedBy) > 0
}

// Silenced returns whether the alert for the given Fingerprint is in the
// Silenced state, any associated silence IDs, and the silences state version
// the result is based on.
// Silenced返回是否给定的Fingerprint的alert处于Silenced状态，任何相关的silence IDs
// 以及基于的state version
func (m *memMarker) Silenced(alert model.Fingerprint) ([]string, int, bool) {
	s := m.Status(alert)
	return s.SilencedBy, s.silencesVersion,
		// 是否处于suppressed状态并且s.SilencedBy大于0
		s.State == AlertStateSuppressed && len(s.SilencedBy) > 0
}

// MultiError contains multiple errors and implements the error interface. Its
// zero value is ready to use. All its methods are goroutine safe.
type MultiError struct {
	mtx    sync.Mutex
	errors []error
}

// Add adds an error to the MultiError.
func (e *MultiError) Add(err error) {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	e.errors = append(e.errors, err)
}

// Len returns the number of errors added to the MultiError.
func (e *MultiError) Len() int {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	return len(e.errors)
}

// Errors returns the errors added to the MuliError. The returned slice is a
// copy of the internal slice of errors.
func (e *MultiError) Errors() []error {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	return append(make([]error, 0, len(e.errors)), e.errors...)
}

func (e *MultiError) Error() string {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	es := make([]string, 0, len(e.errors))
	for _, err := range e.errors {
		es = append(es, err.Error())
	}
	return strings.Join(es, "; ")
}

// Alert wraps a model.Alert with additional information relevant
// to internal of the Alertmanager.
// The type is never exposed to external communication and the
// embedded alert has to be sanitized beforehand.
// Alert用和Alertmanager有关的额外的信息封装了model.Alert
// 本类型不能被暴露用于external communication
type Alert struct {
	model.Alert

	// The authoritative timestamp.
	UpdatedAt time.Time
	Timeout   bool
}

// AlertSlice is a sortable slice of Alerts.
type AlertSlice []*Alert

func (as AlertSlice) Less(i, j int) bool {
	// Look at labels.job, then labels.instance.
	for _, overrideKey := range [...]model.LabelName{"job", "instance"} {
		iVal, iOk := as[i].Labels[overrideKey]
		jVal, jOk := as[j].Labels[overrideKey]
		if !iOk && !jOk {
			continue
		}
		if !iOk {
			return false
		}
		if !jOk {
			return true
		}
		if iVal != jVal {
			return iVal < jVal
		}
	}
	return as[i].Labels.Before(as[j].Labels)
}
func (as AlertSlice) Swap(i, j int) { as[i], as[j] = as[j], as[i] }
func (as AlertSlice) Len() int      { return len(as) }

// Alerts turns a sequence of internal alerts into a list of
// exposable model.Alert structures.
func Alerts(alerts ...*Alert) model.Alerts {
	res := make(model.Alerts, 0, len(alerts))
	for _, a := range alerts {
		v := a.Alert
		// If the end timestamp is not reached yet, do not expose it.
		if !a.Resolved() {
			v.EndsAt = time.Time{}
		}
		res = append(res, &v)
	}
	return res
}

// Merge merges the timespan of two alerts based and overwrites annotations
// based on the authoritative timestamp.  A new alert is returned, the labels
// are assumed to be equal.
// Merge合并两个alerts的timespan，并且基于authoritative timestamp来覆盖annotations
// 返回一个新的alert，labels假设是相等的
func (a *Alert) Merge(o *Alert) *Alert {
	// Let o always be the younger alert.
	// 总是选择更年轻的alert
	if o.UpdatedAt.Before(a.UpdatedAt) {
		return o.Merge(a)
	}

	res := *o

	// Always pick the earliest starting time.
	// 永远选择最早的start time
	if a.StartsAt.Before(o.StartsAt) {
		res.StartsAt = a.StartsAt
	}

	if o.Resolved() {
		// The latest explicit resolved timestamp wins if both alerts are effectively resolved.
		// 如果两个alerts都被resolved，则最新的resolved timestamp获胜
		if a.Resolved() && a.EndsAt.After(o.EndsAt) {
			// 如果两个alert都resolve了，选择更大的End time
			res.EndsAt = a.EndsAt
		}
	} else {
		// A non-timeout timestamp always rules if it is the latest.
		// non-timeout的时间戳总是占优势，如果它是最新的话
		if a.EndsAt.After(o.EndsAt) && !a.Timeout {
			// 如果新的alert的endsAt在old之后，且没有设置timeout，则将其设置
			res.EndsAt = a.EndsAt
		}
	}

	return &res
}

// A Muter determines whether a given label set is muted. Implementers that
// maintain an underlying Marker are expected to update it during a call of
// Mutes.
// Muter决定一个给定的label set是否被静默，一个底层Marker的实现者期望在调用Mutes的时候
// 更新它
type Muter interface {
	Mutes(model.LabelSet) bool
}

// A MuteFunc is a function that implements the Muter interface.
type MuteFunc func(model.LabelSet) bool

// Mutes implements the Muter interface.
func (f MuteFunc) Mutes(lset model.LabelSet) bool { return f(lset) }

// A Silence determines whether a given label set is muted.
// Silence决定一个给定的label set是否静音
type Silence struct {
	// A unique identifier across all connected instances.
	// 所有实例的唯一标识
	ID string `json:"id"`
	// A set of matchers determining if a label set is affect
	// by the silence.
	// 一系列的matchers用于决定是否一个label set被这个silence影响
	Matchers Matchers `json:"matchers"`

	// Time range of the silence.
	//
	// * StartsAt must not be before creation time
	// * EndsAt must be after StartsAt
	// * Deleting a silence means to set EndsAt to now
	// * Time range must not be modified in different ways
	// * StartsAt必须不在creation time之前，EndsAt必须在StartsAt之后
	// * 删除一个silence意味着将EndsAt设置为现在
	// * Time range必须不能以其他方式改变
	//
	// TODO(fabxc): this may potentially be extended by
	// creation and update timestamps.
	StartsAt time.Time `json:"startsAt"`
	EndsAt   time.Time `json:"endsAt"`

	// The last time the silence was updated.
	// 上一次silence更新的时间
	UpdatedAt time.Time `json:"updatedAt"`

	// Information about who created the silence for which reason.
	// 谁创建了silence以及什么原因创建
	CreatedBy string `json:"createdBy"`
	Comment   string `json:"comment,omitempty"`

	Status SilenceStatus `json:"status"`
}

// Expired return if the silence is expired
// meaning that both StartsAt and EndsAt are equal
func (s *Silence) Expired() bool {
	return s.StartsAt.Equal(s.EndsAt)
}

// SilenceStatus stores the state of a silence.
// SilenceStatus存储一个silence的状态
type SilenceStatus struct {
	State SilenceState `json:"state"`
}

// SilenceState is used as part of SilenceStatus.
type SilenceState string

// Possible values for SilenceState.
const (
	// 可能的silence的状态为"expected", "active"以及"pending"
	SilenceStateExpired SilenceState = "expired"
	SilenceStateActive  SilenceState = "active"
	SilenceStatePending SilenceState = "pending"
)

// CalcSilenceState returns the SilenceState that a silence with the given start
// and end time would have right now.
func CalcSilenceState(start, end time.Time) SilenceState {
	current := time.Now()
	if current.Before(start) {
		return SilenceStatePending
	}
	if current.Before(end) {
		return SilenceStateActive
	}
	return SilenceStateExpired
}
