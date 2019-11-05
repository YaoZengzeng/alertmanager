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

package notify

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/cespare/xxhash"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/cluster"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/inhibit"
	"github.com/prometheus/alertmanager/nflog"
	"github.com/prometheus/alertmanager/nflog/nflogpb"
	"github.com/prometheus/alertmanager/silence"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/alertmanager/types"
)

var (
	numNotifications = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "alertmanager",
		Name:      "notifications_total",
		Help:      "The total number of attempted notifications.",
	}, []string{"integration"})

	numFailedNotifications = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "alertmanager",
		Name:      "notifications_failed_total",
		Help:      "The total number of failed notifications.",
	}, []string{"integration"})

	notificationLatencySeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "alertmanager",
		Name:      "notification_latency_seconds",
		Help:      "The latency of notifications in seconds.",
		Buckets:   []float64{1, 5, 10, 15, 20},
	}, []string{"integration"})
)

func init() {
	numNotifications.WithLabelValues("email")
	numNotifications.WithLabelValues("hipchat")
	numNotifications.WithLabelValues("pagerduty")
	numNotifications.WithLabelValues("wechat")
	numNotifications.WithLabelValues("pushover")
	numNotifications.WithLabelValues("slack")
	numNotifications.WithLabelValues("opsgenie")
	numNotifications.WithLabelValues("webhook")
	numNotifications.WithLabelValues("victorops")
	numFailedNotifications.WithLabelValues("email")
	numFailedNotifications.WithLabelValues("hipchat")
	numFailedNotifications.WithLabelValues("pagerduty")
	numFailedNotifications.WithLabelValues("wechat")
	numFailedNotifications.WithLabelValues("pushover")
	numFailedNotifications.WithLabelValues("slack")
	numFailedNotifications.WithLabelValues("opsgenie")
	numFailedNotifications.WithLabelValues("webhook")
	numFailedNotifications.WithLabelValues("victorops")
	notificationLatencySeconds.WithLabelValues("email")
	notificationLatencySeconds.WithLabelValues("hipchat")
	notificationLatencySeconds.WithLabelValues("pagerduty")
	notificationLatencySeconds.WithLabelValues("wechat")
	notificationLatencySeconds.WithLabelValues("pushover")
	notificationLatencySeconds.WithLabelValues("slack")
	notificationLatencySeconds.WithLabelValues("opsgenie")
	notificationLatencySeconds.WithLabelValues("webhook")
	notificationLatencySeconds.WithLabelValues("victorops")

	prometheus.MustRegister(numNotifications)
	prometheus.MustRegister(numFailedNotifications)
	prometheus.MustRegister(notificationLatencySeconds)
}

type notifierConfig interface {
	SendResolved() bool
}

// MinTimeout is the minimum timeout that is set for the context of a call
// to a notification pipeline.
const MinTimeout = 10 * time.Second

// notifyKey defines a custom type with which a context is populated to
// avoid accidental collisions.
type notifyKey int

const (
	keyReceiverName notifyKey = iota
	keyRepeatInterval
	keyGroupLabels
	keyGroupKey
	keyFiringAlerts
	keyResolvedAlerts
	keyNow
)

// WithReceiverName populates a context with a receiver name.
func WithReceiverName(ctx context.Context, rcv string) context.Context {
	return context.WithValue(ctx, keyReceiverName, rcv)
}

// WithGroupKey populates a context with a group key.
func WithGroupKey(ctx context.Context, s string) context.Context {
	return context.WithValue(ctx, keyGroupKey, s)
}

// WithFiringAlerts populates a context with a slice of firing alerts.
// WithFiringAlerts用一系列的firing alerts填充context
func WithFiringAlerts(ctx context.Context, alerts []uint64) context.Context {
	return context.WithValue(ctx, keyFiringAlerts, alerts)
}

// WithResolvedAlerts populates a context with a slice of resolved alerts.
// WithResolvedAlerts用一系列的resolved alerts填充context
func WithResolvedAlerts(ctx context.Context, alerts []uint64) context.Context {
	return context.WithValue(ctx, keyResolvedAlerts, alerts)
}

// WithGroupLabels populates a context with grouping labels.
func WithGroupLabels(ctx context.Context, lset model.LabelSet) context.Context {
	return context.WithValue(ctx, keyGroupLabels, lset)
}

// WithNow populates a context with a now timestamp.
func WithNow(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, keyNow, t)
}

// WithRepeatInterval populates a context with a repeat interval.
func WithRepeatInterval(ctx context.Context, t time.Duration) context.Context {
	return context.WithValue(ctx, keyRepeatInterval, t)
}

// RepeatInterval extracts a repeat interval from the context. Iff none exists, the
// second argument is false.
func RepeatInterval(ctx context.Context) (time.Duration, bool) {
	v, ok := ctx.Value(keyRepeatInterval).(time.Duration)
	return v, ok
}

// ReceiverName extracts a receiver name from the context. Iff none exists, the
// second argument is false.
func ReceiverName(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(keyReceiverName).(string)
	return v, ok
}

func receiverName(ctx context.Context, l log.Logger) string {
	recv, ok := ReceiverName(ctx)
	if !ok {
		level.Error(l).Log("msg", "Missing receiver")
	}
	return recv
}

// GroupKey extracts a group key from the context. Iff none exists, the
// second argument is false.
func GroupKey(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(keyGroupKey).(string)
	return v, ok
}

func groupLabels(ctx context.Context, l log.Logger) model.LabelSet {
	groupLabels, ok := GroupLabels(ctx)
	if !ok {
		level.Error(l).Log("msg", "Missing group labels")
	}
	return groupLabels
}

// GroupLabels extracts grouping label set from the context. Iff none exists, the
// second argument is false.
func GroupLabels(ctx context.Context) (model.LabelSet, bool) {
	v, ok := ctx.Value(keyGroupLabels).(model.LabelSet)
	return v, ok
}

// Now extracts a now timestamp from the context. Iff none exists, the
// second argument is false.
func Now(ctx context.Context) (time.Time, bool) {
	v, ok := ctx.Value(keyNow).(time.Time)
	return v, ok
}

// FiringAlerts extracts a slice of firing alerts from the context.
// Iff none exists, the second argument is false.
// FiringAlerts从context中抽取出一系列的firing alerts
func FiringAlerts(ctx context.Context) ([]uint64, bool) {
	v, ok := ctx.Value(keyFiringAlerts).([]uint64)
	return v, ok
}

// ResolvedAlerts extracts a slice of firing alerts from the context.
// Iff none exists, the second argument is false.
func ResolvedAlerts(ctx context.Context) ([]uint64, bool) {
	v, ok := ctx.Value(keyResolvedAlerts).([]uint64)
	return v, ok
}

// A Stage processes alerts under the constraints of the given context.
// Stage处理alerts在给定的上下文的限制下
type Stage interface {
	Exec(ctx context.Context, l log.Logger, alerts ...*types.Alert) (context.Context, []*types.Alert, error)
}

// StageFunc wraps a function to represent a Stage.
type StageFunc func(ctx context.Context, l log.Logger, alerts ...*types.Alert) (context.Context, []*types.Alert, error)

// Exec implements Stage interface.
func (f StageFunc) Exec(ctx context.Context, l log.Logger, alerts ...*types.Alert) (context.Context, []*types.Alert, error) {
	return f(ctx, l, alerts...)
}

type NotificationLog interface {
	// 根据receiver和gkey，对firingAlerts以及resolvedAlerts进行记录
	Log(r *nflogpb.Receiver, gkey string, firingAlerts, resolvedAlerts []uint64) error
	// 根据Queryparam参数，返回相应的Entry
	Query(params ...nflog.QueryParam) ([]*nflogpb.Entry, error)
}

// BuildPipeline builds a map of receivers to Stages.
// BuildPipeline构建一系列的receivers
func BuildPipeline(
	confs []*config.Receiver,
	tmpl *template.Template,
	wait func() time.Duration,
	inhibitor *inhibit.Inhibitor,
	silencer *silence.Silencer,
	notificationLog NotificationLog,
	peer *cluster.Peer,
	logger log.Logger,
) RoutingStage {
	rs := RoutingStage{}

	// gossip settle，inhibitor和silencer是每个receiver都必须经历的阶段
	ms := NewGossipSettleStage(peer)
	is := NewMuteStage(inhibitor)
	ss := NewMuteStage(silencer)

	// 遍历receiver
	for _, rc := range confs {
		// receiver对应的stage都是并行执行的
		rs[rc.Name] = MultiStage{ms, is, ss, createStage(rc, tmpl, wait, notificationLog, logger)}
	}
	return rs
}

// createStage creates a pipeline of stages for a receiver.
// createStage为一个receiver创建一个pipeline of stages
func createStage(rc *config.Receiver, tmpl *template.Template, wait func() time.Duration, notificationLog NotificationLog, logger log.Logger) Stage {
	var fs FanoutStage
	for _, i := range BuildReceiverIntegrations(rc, tmpl, logger) {
		recv := &nflogpb.Receiver{
			GroupName:   rc.Name,
			Integration: i.name,
			Idx:         uint32(i.idx),
		}
		var s MultiStage
		s = append(s, NewWaitStage(wait))
		// 设置dedup，根据notification log进行去重
		s = append(s, NewDedupStage(i, notificationLog, recv))
		// RetryStage真正将通知发送出去
		s = append(s, NewRetryStage(i, rc.Name))
		// 设置Notification Stage
		s = append(s, NewSetNotifiesStage(notificationLog, recv))

		// 同一个receiver的各种发送方法应该并行运行，所以使用了FanoutStage
		fs = append(fs, s)
	}
	return fs
}

// RoutingStage executes the inner stages based on the receiver specified in
// the context.
// RoutingStage基于指定在上下文的receiver执行inner stages
type RoutingStage map[string]Stage

// Exec implements the Stage interface.
func (rs RoutingStage) Exec(ctx context.Context, l log.Logger, alerts ...*types.Alert) (context.Context, []*types.Alert, error) {
	// 从ctx中获取receiver name
	receiver, ok := ReceiverName(ctx)
	if !ok {
		return ctx, nil, fmt.Errorf("receiver missing")
	}

	// 找到receiver对应的stage
	s, ok := rs[receiver]
	if !ok {
		return ctx, nil, fmt.Errorf("stage for receiver missing")
	}

	// 再对同一个receiver中的各种发送方法并行处理
	return s.Exec(ctx, l, alerts...)
}

// A MultiStage executes a series of stages sequentially.
// 一个MultiStage顺序执行一系列的stages
type MultiStage []Stage

// Exec implements the Stage interface.
func (ms MultiStage) Exec(ctx context.Context, l log.Logger, alerts ...*types.Alert) (context.Context, []*types.Alert, error) {
	var err error
	// 遍历各个Stage用于过滤alerts
	for _, s := range ms {
		// 如果没有alerts就直接返回
		if len(alerts) == 0 {
			return ctx, nil, nil
		}

		ctx, alerts, err = s.Exec(ctx, l, alerts...)
		if err != nil {
			return ctx, nil, err
		}
	}
	return ctx, alerts, nil
}

// FanoutStage executes its stages concurrently
// FanoutStage并行地执行它的stages
type FanoutStage []Stage

// Exec attempts to execute all stages concurrently and discards the results.
// It returns its input alerts and a types.MultiError if one or more stages fail.
// Exec试着并行地执行所有的stages并且丢弃results
// 它返回输入的results以及一个types.MultiError，如果一个或多个stages失败了
func (fs FanoutStage) Exec(ctx context.Context, l log.Logger, alerts ...*types.Alert) (context.Context, []*types.Alert, error) {
	var (
		wg sync.WaitGroup
		me types.MultiError
	)
	wg.Add(len(fs))

	for _, s := range fs {
		// 并行地执行多个Stage
		go func(s Stage) {
			// 每一个s都是MultiStage
			if _, _, err := s.Exec(ctx, l, alerts...); err != nil {
				me.Add(err)
				lvl := level.Error(l)
				if ctx.Err() == context.Canceled {
					// It is expected for the context to be canceled on
					// configuration reload or shutdown. In this case, the
					// message should only be logged at the debug level.
					// 当配置reload或者关闭的时候context会被取消，在这种情况下，message应该只在debug层面被记录
					lvl = level.Debug(l)
				}
				lvl.Log("msg", "Error on notify", "err", err)
			}
			wg.Done()
		}(s)
	}
	wg.Wait()

	// 如果有错误，则返回
	if me.Len() > 0 {
		return ctx, alerts, &me
	}
	return ctx, alerts, nil
}

// GossipSettleStage waits until the Gossip has settled to forward alerts.
// GossipSettleStage等待直到Gossip已经稳定了，再发送alerts
type GossipSettleStage struct {
	peer *cluster.Peer
}

// NewGossipSettleStage returns a new GossipSettleStage.
func NewGossipSettleStage(p *cluster.Peer) *GossipSettleStage {
	return &GossipSettleStage{peer: p}
}

func (n *GossipSettleStage) Exec(ctx context.Context, l log.Logger, alerts ...*types.Alert) (context.Context, []*types.Alert, error) {
	if n.peer != nil {
		// 如果peer不为nil，则运行n.peer.WaitReady()
		n.peer.WaitReady()
	}
	// 如果peer为nil，则直接返回
	return ctx, alerts, nil
}

// MuteStage filters alerts through a Muter.
// MuteStage通过一个Muter过滤alerts
type MuteStage struct {
	muter types.Muter
}

// NewMuteStage return a new MuteStage.
func NewMuteStage(m types.Muter) *MuteStage {
	return &MuteStage{muter: m}
}

// Exec implements the Stage interface.
func (n *MuteStage) Exec(ctx context.Context, l log.Logger, alerts ...*types.Alert) (context.Context, []*types.Alert, error) {
	var filtered []*types.Alert
	for _, a := range alerts {
		// TODO(fabxc): increment total alerts counter.
		// Do not send the alert if muted.
		// 遍历alerts，如果被静默的话，就过滤
		if !n.muter.Mutes(a.Labels) {
			// 如果没有被静默，则加入filtered这个slice中
			filtered = append(filtered, a)
		}
		// TODO(fabxc): increment muted alerts counter if muted.
	}
	return ctx, filtered, nil
}

// WaitStage waits for a certain amount of time before continuing or until the
// context is done.
// WaitStage等待一个时间段，在继续之前或者直到context结束
type WaitStage struct {
	wait func() time.Duration
}

// NewWaitStage returns a new WaitStage.
// NewWaitStage返回一个新的WaitStage
func NewWaitStage(wait func() time.Duration) *WaitStage {
	return &WaitStage{
		wait: wait,
	}
}

// Exec implements the Stage interface.
func (ws *WaitStage) Exec(ctx context.Context, l log.Logger, alerts ...*types.Alert) (context.Context, []*types.Alert, error) {
	select {
	// WaitStage等待固定时间段
	case <-time.After(ws.wait()):
	case <-ctx.Done():
		return ctx, nil, ctx.Err()
	}
	return ctx, alerts, nil
}

// DedupStage filters alerts.
// Filtering happens based on a notification log.
// DedupStage过滤alerts
// 过滤基于一个notification log
type DedupStage struct {
	nflog NotificationLog
	recv  *nflogpb.Receiver
	conf  notifierConfig

	now  func() time.Time
	hash func(*types.Alert) uint64
}

// NewDedupStage wraps a DedupStage that runs against the given notification log.
// NewDedupStage封装了一个DedupStage，基于给定的notification log
func NewDedupStage(i Integration, l NotificationLog, recv *nflogpb.Receiver) *DedupStage {
	return &DedupStage{
		nflog: l,
		recv:  recv,
		conf:  i.conf,
		now:   utcNow,
		hash:  hashAlert,
	}
}

func utcNow() time.Time {
	return time.Now().UTC()
}

var hashBuffers = sync.Pool{}

func getHashBuffer() []byte {
	b := hashBuffers.Get()
	if b == nil {
		return make([]byte, 0, 1024)
	}
	return b.([]byte)
}

func putHashBuffer(b []byte) {
	b = b[:0]
	//lint:ignore SA6002 relax staticcheck verification.
	hashBuffers.Put(b)
}

func hashAlert(a *types.Alert) uint64 {
	const sep = '\xff'

	b := getHashBuffer()
	defer putHashBuffer(b)

	names := make(model.LabelNames, 0, len(a.Labels))

	for ln := range a.Labels {
		names = append(names, ln)
	}
	sort.Sort(names)

	for _, ln := range names {
		b = append(b, string(ln)...)
		b = append(b, sep)
		b = append(b, string(a.Labels[ln])...)
		b = append(b, sep)
	}

	hash := xxhash.Sum64(b)

	return hash
}

func (n *DedupStage) needsUpdate(entry *nflogpb.Entry, firing, resolved map[uint64]struct{}, repeat time.Duration) bool {
	// If we haven't notified about the alert group before, notify right away
	// unless we only have resolved alerts.
	// 如果我们之前还没有通知alert group，马上进行通知
	// 除非我们只有resolved alerts
	if entry == nil {
		return len(firing) > 0
	}

	// 如果当前的firing不是已经发送过的firing的一个子集，则返回true
	if !entry.IsFiringSubset(firing) {
		return true
	}

	// Notify about all alerts being resolved.
	// This is done irrespective of the send_resolved flag to make sure that
	// the firing alerts are cleared from the notification log.
	// 通知所有的alerts都已经被resolved
	// 不管send_resolved这个flag是否已经被设置，这只是用来确保firing alerts已经从notification
	// log中移除了
	if len(firing) == 0 {
		// If the current alert group and last notification contain no firing
		// alert, it means that some alerts have been fired and resolved during the
		// last interval. In this case, there is no need to notify the receiver
		// since it doesn't know about them.
		// 如果当前的alert group以及last notification没有包含firing alert，这意味着
		// 有些alerts已经fired了并且resolved在上一个interval，在这种情况下，没有必要再
		// 通知receiver，因为它不知道它们
		return len(entry.FiringAlerts) > 0
	}

	if n.conf.SendResolved() && !entry.IsResolvedSubset(resolved) {
		// 如果配置了发送已消除的并且当前的resolved alerts不是上一次通知时的子集，则返回true
		return true
	}

	// Nothing changed, only notify if the repeat interval has passed.
	// 什么改变都没有发生，只会在repeat interval过去的时候才进行notify
	return entry.Timestamp.Before(n.now().Add(-repeat))
}

// Exec implements the Stage interface.
func (n *DedupStage) Exec(ctx context.Context, l log.Logger, alerts ...*types.Alert) (context.Context, []*types.Alert, error) {
	gkey, ok := GroupKey(ctx)
	if !ok {
		return ctx, nil, fmt.Errorf("group key missing")
	}

	// 从ctx中获取repeat interval
	repeatInterval, ok := RepeatInterval(ctx)
	if !ok {
		return ctx, nil, fmt.Errorf("repeat interval missing")
	}

	firingSet := map[uint64]struct{}{}
	resolvedSet := map[uint64]struct{}{}
	firing := []uint64{}
	resolved := []uint64{}

	var hash uint64
	for _, a := range alerts {
		hash = n.hash(a)
		if a.Resolved() {
			// 将当前的alerts分别划入resolved和firing
			resolved = append(resolved, hash)
			resolvedSet[hash] = struct{}{}
		} else {
			firing = append(firing, hash)
			firingSet[hash] = struct{}{}
		}
	}

	ctx = WithFiringAlerts(ctx, firing)
	ctx = WithResolvedAlerts(ctx, resolved)

	// 用group key和receiver查询nflog
	entries, err := n.nflog.Query(nflog.QGroupKey(gkey), nflog.QReceiver(n.recv))

	if err != nil && err != nflog.ErrNotFound {
		return ctx, nil, err
	}
	var entry *nflogpb.Entry
	switch len(entries) {
	case 0:
	case 1:
		entry = entries[0]
	case 2:
		return ctx, nil, fmt.Errorf("unexpected entry result size %d", len(entries))
	}
	// 根据entry解析是否需要update
	// 如果需要，则说明有新的告警需要发送
	if n.needsUpdate(entry, firingSet, resolvedSet, repeatInterval) {
		// 需要更新，则直接返回alerts
		return ctx, alerts, nil
	}
	return ctx, nil, nil
}

// RetryStage notifies via passed integration with exponential backoff until it
// succeeds. It aborts if the context is canceled or timed out.
// RetryStage通过传入的integration发送通知并且指数回退直到成功
// 它会abort直到context被cancel或者超时
type RetryStage struct {
	integration Integration
	groupName   string
}

// NewRetryStage returns a new instance of a RetryStage.
func NewRetryStage(i Integration, groupName string) *RetryStage {
	return &RetryStage{
		integration: i,
		groupName:   groupName,
	}
}

// Exec implements the Stage interface.
func (r RetryStage) Exec(ctx context.Context, l log.Logger, alerts ...*types.Alert) (context.Context, []*types.Alert, error) {
	var sent []*types.Alert

	// If we shouldn't send notifications for resolved alerts, but there are only
	// resolved alerts, report them all as successfully notified (we still want the
	// notification log to log them for the next run of DedupStage).
	// 如果我们不应该为resolved alerts发送通知，但是只有resolved alerts,则汇报它们已经成功进行了通知
	// 我们还是需要notification log去记录它们，为了DedupStage的下一轮运行
	if !r.integration.conf.SendResolved() {
		// 如果不发送resolved alerts
		firing, ok := FiringAlerts(ctx)
		if !ok {
			return ctx, nil, fmt.Errorf("firing alerts missing")
		}
		if len(firing) == 0 {
			// 如果没有firing的告警，则直接返回所有的alerts
			return ctx, alerts, nil
		}
		for _, a := range alerts {
			if a.Status() != model.AlertResolved {
				sent = append(sent, a)
			}
		}
	} else {
		sent = alerts
	}

	var (
		i    = 0
		b    = backoff.NewExponentialBackOff()
		// 根据回退创建一个ticker
		tick = backoff.NewTicker(b)
		iErr error
	)
	defer tick.Stop()

	// Notify会一直重试，直到重试的时间间隔为60s
	for {
		i++
		// Always check the context first to not notify again.
		// 总是先检查context，从而不会重复通知
		select {
		case <-ctx.Done():
			if iErr != nil {
				return ctx, nil, iErr
			}

			return ctx, nil, ctx.Err()
		default:
		}

		select {
		// 利用回退机制
		case <-tick.C:
			now := time.Now()
			// 调用integration.Notify发送通知
			retry, err := r.integration.Notify(ctx, sent...)
			notificationLatencySeconds.WithLabelValues(r.integration.name).Observe(time.Since(now).Seconds())
			numNotifications.WithLabelValues(r.integration.name).Inc()
			if err != nil {
				numFailedNotifications.WithLabelValues(r.integration.name).Inc()
				level.Debug(l).Log("msg", "Notify attempt failed", "attempt", i, "integration", r.integration.name, "receiver", r.groupName, "err", err)
				if !retry {
					// 因为不可恢复的错误，取消notify retry
					return ctx, alerts, fmt.Errorf("cancelling notify retry for %q due to unrecoverable error: %s", r.integration.name, err)
				}

				// Save this error to be able to return the last seen error by an
				// integration upon context timeout.
				iErr = err
			} else {
				return ctx, alerts, nil
			}
		case <-ctx.Done():
			if iErr != nil {
				return ctx, nil, iErr
			}

			return ctx, nil, ctx.Err()
		}
	}
}

// SetNotifiesStage sets the notification information about passed alerts. The
// passed alerts should have already been sent to the receivers.
// SetNotifiesStage设置关于passed alerts的通知信息
// passed alerts应该已经被发送给了receivers
type SetNotifiesStage struct {
	nflog NotificationLog
	recv  *nflogpb.Receiver
}

// NewSetNotifiesStage returns a new instance of a SetNotifiesStage.
func NewSetNotifiesStage(l NotificationLog, recv *nflogpb.Receiver) *SetNotifiesStage {
	return &SetNotifiesStage{
		nflog: l,
		recv:  recv,
	}
}

// Exec implements the Stage interface.
func (n SetNotifiesStage) Exec(ctx context.Context, l log.Logger, alerts ...*types.Alert) (context.Context, []*types.Alert, error) {
	gkey, ok := GroupKey(ctx)
	if !ok {
		return ctx, nil, fmt.Errorf("group key missing")
	}

	// 获取firing alerts和resolved alerts的值
	firing, ok := FiringAlerts(ctx)
	if !ok {
		return ctx, nil, fmt.Errorf("firing alerts missing")
	}

	resolved, ok := ResolvedAlerts(ctx)
	if !ok {
		return ctx, nil, fmt.Errorf("resolved alerts missing")
	}

	// 调用n.nflog.Log，记录firing和resolved alerts
	return ctx, alerts, n.nflog.Log(n.recv, gkey, firing, resolved)
}
