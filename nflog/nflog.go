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

// Package nflog implements a garbage-collected and snapshottable append-only log of
// active/resolved notifications. Each log entry stores the active/resolved state,
// the notified receiver, and a hash digest of the notification's identifying contents.
// The log can be queried along different parameters.
// nflog包实现了垃圾回收以及对于active/resolved notifications的append-only的log
// 每个log entry保存了active/resolve状态，notified receiver以及notification的identifying contents
// 的hash digest
package nflog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/matttproud/golang_protobuf_extensions/pbutil"
	"github.com/prometheus/alertmanager/cluster"
	pb "github.com/prometheus/alertmanager/nflog/nflogpb"
	"github.com/prometheus/client_golang/prometheus"
)

// ErrNotFound is returned for empty query results.
var ErrNotFound = errors.New("not found")

// ErrInvalidState is returned if the state isn't valid.
var ErrInvalidState = fmt.Errorf("invalid state")

// query currently allows filtering by and/or receiver group key.
// It is configured via QueryParameter functions.
// query当前允许通过receiver group key进行过滤，它通过QueryParameter函数进行配置
//
// TODO(fabxc): Future versions could allow querying a certain receiver
// group or a given time interval.
type query struct {
	recv     *pb.Receiver
	groupKey string
}

// QueryParam is a function that modifies a query to incorporate
// a set of parameters. Returns an error for invalid or conflicting
// parameters.
// QueryParam是一个函数用于修改一个query来包含一系列的参数
// 返回error，对于非法的或者有矛盾的参数
type QueryParam func(*query) error

// 如果一个结构体有很多参数，又想对其中的某些参数进行选择性地配置，则可以将一个参数包装为用一个函数进行设置
// 然后以此类函数作为参数，在New函数中传入
// QReceiver adds a receiver parameter to a query.
// QReceiver在query中增加一个receiver参数
func QReceiver(r *pb.Receiver) QueryParam {
	return func(q *query) error {
		q.recv = r
		return nil
	}
}

// QGroupKey adds a group key as querying argument.
// QGroupKey增加一个group key作为querying argument
func QGroupKey(gk string) QueryParam {
	return func(q *query) error {
		q.groupKey = gk
		return nil
	}
}

type Log struct {
	logger    log.Logger
	metrics   *metrics
	now       func() time.Time
	retention time.Duration

	runInterval time.Duration
	snapf       string
	stopc       chan struct{}
	done        func()

	// For now we only store the most recently added log entry.
	// The key is a serialized concatenation of group key and receiver.
	// 对于现在，我们只最新被添加的log entry
	// 其中的key是group key和receiver的serialized concatenation
	mtx       sync.RWMutex
	// state是一个键值对，key是string
	st        state
	broadcast func([]byte)
}

type metrics struct {
	gcDuration              prometheus.Summary
	snapshotDuration        prometheus.Summary
	snapshotSize            prometheus.Gauge
	queriesTotal            prometheus.Counter
	queryErrorsTotal        prometheus.Counter
	queryDuration           prometheus.Histogram
	propagatedMessagesTotal prometheus.Counter
}

func newMetrics(r prometheus.Registerer) *metrics {
	m := &metrics{}

	m.gcDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "alertmanager_nflog_gc_duration_seconds",
		Help: "Duration of the last notification log garbage collection cycle.",
	})
	m.snapshotDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "alertmanager_nflog_snapshot_duration_seconds",
		Help: "Duration of the last notification log snapshot.",
	})
	m.snapshotSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "alertmanager_nflog_snapshot_size_bytes",
		Help: "Size of the last notification log snapshot in bytes.",
	})
	m.queriesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_nflog_queries_total",
		Help: "Number of notification log queries were received.",
	})
	m.queryErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_nflog_query_errors_total",
		Help: "Number notification log received queries that failed.",
	})
	m.queryDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "alertmanager_nflog_query_duration_seconds",
		Help: "Duration of notification log query evaluation.",
	})
	m.propagatedMessagesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_nflog_gossip_messages_propagated_total",
		Help: "Number of received gossip messages that have been further gossiped.",
	})

	if r != nil {
		r.MustRegister(
			m.gcDuration,
			m.snapshotDuration,
			m.snapshotSize,
			m.queriesTotal,
			m.queryErrorsTotal,
			m.queryDuration,
			m.propagatedMessagesTotal,
		)
	}
	return m
}

// Option configures a new Log implementation.
// Option配置了一个新的Log实现
type Option func(*Log) error

// WithRetention sets the retention time for log st.
func WithRetention(d time.Duration) Option {
	return func(l *Log) error {
		l.retention = d
		return nil
	}
}

// WithNow overwrites the function used to retrieve a timestamp
// for the current point in time.
// This is generally useful for injection during tests.
func WithNow(f func() time.Time) Option {
	return func(l *Log) error {
		l.now = f
		return nil
	}
}

// WithLogger configures a logger for the notification log.
func WithLogger(logger log.Logger) Option {
	return func(l *Log) error {
		l.logger = logger
		return nil
	}
}

// WithMetrics registers metrics for the notification log.
func WithMetrics(r prometheus.Registerer) Option {
	return func(l *Log) error {
		l.metrics = newMetrics(r)
		return nil
	}
}

// WithMaintenance configures the Log to run garbage collection
// and snapshotting, if configured, at the given interval.
// WithMaintenance配置Log以给定的interval运行gc以及snapshotting，如果配置的话
//
// The maintenance terminates on receiving from the provided channel.
// The done function is called after the final snapshot was completed.
// 当从给定的channel中收到通知之后，maintenance终止，done函数在final snapshot结束之后被调用
func WithMaintenance(d time.Duration, stopc chan struct{}, done func()) Option {
	return func(l *Log) error {
		if d == 0 {
			return fmt.Errorf("maintenance interval must not be 0")
		}
		// 每15min运行一次
		l.runInterval = d
		l.stopc = stopc
		l.done = done
		return nil
	}
}

// WithSnapshot configures the log to be initialized from a given snapshot file.
// If maintenance is configured, a snapshot will be saved periodically and on
// shutdown as well.
// WithSnapshot配置log从给定的snapshot file进行初始化
// 如果配置了maintenance，一个snapshot会阶段性地被保存
func WithSnapshot(sf string) Option {
	return func(l *Log) error {
		l.snapf = sf
		return nil
	}
}

func utcNow() time.Time {
	return time.Now().UTC()
}

type state map[string]*pb.MeshEntry

func (s state) clone() state {
	c := make(state, len(s))
	for k, v := range s {
		c[k] = v
	}
	return c
}

// merge returns true or false whether the MeshEntry was merged or
// not. This information is used to decide to gossip the message further.
// merge返回true或者false，表示MeshEntry是否被合并
// 返回的信息用于决定是否将该信息进一步gossip
func (s state) merge(e *pb.MeshEntry, now time.Time) bool {
	if e.ExpiresAt.Before(now) {
		return false
	}
	// 根据group key和receiver key构建state key
	k := stateKey(string(e.Entry.GroupKey), e.Entry.Receiver)

	prev, ok := s[k]
	if !ok || prev.Entry.Timestamp.Before(e.Entry.Timestamp) {
		// 如果之前entry不存在，或者新的entry的时间戳在旧的entry之后
		s[k] = e
		return true
	}
	return false
}

func (s state) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	for _, e := range s {
		if _, err := pbutil.WriteDelimited(&buf, e); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeState(r io.Reader) (state, error) {
	// 从reader中解析出MeshEntry
	st := state{}
	for {
		var e pb.MeshEntry
		_, err := pbutil.ReadDelimited(r, &e)
		if err == nil {
			if e.Entry == nil || e.Entry.Receiver == nil {
				return nil, ErrInvalidState
			}
			st[stateKey(string(e.Entry.GroupKey), e.Entry.Receiver)] = &e
			continue
		}
		if err == io.EOF {
			break
		}
		return nil, err
	}
	return st, nil
}

func marshalMeshEntry(e *pb.MeshEntry) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := pbutil.WriteDelimited(&buf, e); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// New creates a new notification log based on the provided options.
// The snapshot is loaded into the Log if it is set.
// New基于给定的options创建一个新的notification log
// snapshot会加载进Log，如果设置了的话
func New(opts ...Option) (*Log, error) {
	l := &Log{
		logger:    log.NewNopLogger(),
		now:       utcNow,
		st:        state{},
		// 默认的broadcast函数为空
		broadcast: func([]byte) {},
	}
	for _, o := range opts {
		if err := o(l); err != nil {
			return nil, err
		}
	}
	if l.metrics == nil {
		l.metrics = newMetrics(nil)
	}

	if l.snapf != "" {
		// 如果设置了snapshot file，则加载
		if f, err := os.Open(l.snapf); !os.IsNotExist(err) {
			if err != nil {
				return l, err
			}
			defer f.Close()

			if err := l.loadSnapshot(f); err != nil {
				return l, err
			}
		}
	}

	go l.run()

	return l, nil
}

// run periodic background maintenance.
// run阶段性地运行background maintenance
func (l *Log) run() {
	if l.runInterval == 0 || l.stopc == nil {
		return
	}
	t := time.NewTicker(l.runInterval)
	defer t.Stop()

	if l.done != nil {
		defer l.done()
	}

	f := func() error {
		start := l.now()
		var size int64

		level.Debug(l.logger).Log("msg", "Running maintenance")
		defer func() {
			level.Debug(l.logger).Log("msg", "Maintenance done", "duration", l.now().Sub(start), "size", size)
			l.metrics.snapshotSize.Set(float64(size))
		}()

		// 定期运行gc
		if _, err := l.GC(); err != nil {
			return err
		}
		if l.snapf == "" {
			return nil
		}
		f, err := openReplace(l.snapf)
		if err != nil {
			return err
		}
		// 进行snapshot
		if size, err = l.Snapshot(f); err != nil {
			return err
		}
		return f.Close()
	}

Loop:
	for {
		select {
		case <-l.stopc:
			break Loop
		case <-t.C:
			// 阶段性地运行函数
			if err := f(); err != nil {
				level.Error(l.logger).Log("msg", "Running maintenance failed", "err", err)
			}
		}
	}
	// No need to run final maintenance if we don't want to snapshot.
	// 如果我们不想要snapshot，则不需要运行final maintenance
	if l.snapf == "" {
		return
	}
	if err := f(); err != nil {
		level.Error(l.logger).Log("msg", "Creating shutdown snapshot failed", "err", err)
	}
}

func receiverKey(r *pb.Receiver) string {
	return fmt.Sprintf("%s/%s/%d", r.GroupName, r.Integration, r.Idx)
}

// stateKey returns a string key for a log entry consisting of the group key
// and receiver.
func stateKey(k string, r *pb.Receiver) string {
	return fmt.Sprintf("%s:%s", k, receiverKey(r))
}

func (l *Log) Log(r *pb.Receiver, gkey string, firingAlerts, resolvedAlerts []uint64) error {
	// Write all st with the same timestamp.
	// 用同一个timestamp写所有的st
	now := l.now()
	// stateKey返回一个由group key和receiver组成的string key用于一个log entry
	key := stateKey(gkey, r)

	l.mtx.Lock()
	defer l.mtx.Unlock()

	if prevle, ok := l.st[key]; ok {
		// Entry already exists, only overwrite if timestamp is newer.
		// This may happen with raciness or clock-drift across AM nodes.
		// Entry已经存在了额，只有在timestamp更新的时候才overwrite
		if prevle.Entry.Timestamp.After(now) {
			return nil
		}
	}

	e := &pb.MeshEntry{
		Entry: &pb.Entry{
			// Entry中记载了receiver，GroupKey，Timestamp以及FiringAlerts和ResolvedAlerts
			Receiver:       r,
			GroupKey:       []byte(gkey),
			Timestamp:      now,
			FiringAlerts:   firingAlerts,
			ResolvedAlerts: resolvedAlerts,
		},
		// ExpiresAt为当前时间加上l.retentation
		// retention默认为120小时
		ExpiresAt: now.Add(l.retention),
	}

	b, err := marshalMeshEntry(e)
	if err != nil {
		return err
	}
	// 将entry进行合并
	l.st.merge(e, l.now())
	// 广播entry
	l.broadcast(b)

	return nil
}

// GC implements the Log interface.
// GC实现了Log接口
func (l *Log) GC() (int, error) {
	start := time.Now()
	defer func() { l.metrics.gcDuration.Observe(time.Since(start).Seconds()) }()

	now := l.now()
	var n int

	l.mtx.Lock()
	defer l.mtx.Unlock()

	for k, le := range l.st {
		if le.ExpiresAt.IsZero() {
			return n, errors.New("unexpected zero expiration timestamp")
		}
		// 如果一条entry已经过期了，则直接从l.st中删除
		if !le.ExpiresAt.After(now) {
			delete(l.st, k)
			n++
		}
	}

	return n, nil
}

// Query implements the Log interface.
func (l *Log) Query(params ...QueryParam) ([]*pb.Entry, error) {
	start := time.Now()
	l.metrics.queriesTotal.Inc()

	entries, err := func() ([]*pb.Entry, error) {
		q := &query{}
		// 遍历params，填充query{}
		for _, p := range params {
			if err := p(q); err != nil {
				return nil, err
			}
		}
		// TODO(fabxc): For now our only query mode is the most recent entry for a
		// receiver/group_key combination.
		// 现在，我们唯一的query mode就是最近的receiver/group_key的组合
		if q.recv == nil || q.groupKey == "" {
			// TODO(fabxc): allow more complex queries in the future.
			// How to enable pagination?
			return nil, errors.New("no query parameters specified")
		}

		l.mtx.RLock()
		defer l.mtx.RUnlock()

		// 根据groupKey和receiver返回相应的Entry
		if le, ok := l.st[stateKey(q.groupKey, q.recv)]; ok {
			return []*pb.Entry{le.Entry}, nil
		}
		return nil, ErrNotFound
	}()
	if err != nil {
		l.metrics.queryErrorsTotal.Inc()
	}
	l.metrics.queryDuration.Observe(time.Since(start).Seconds())
	return entries, err
}

// loadSnapshot loads a snapshot generated by Snapshot() into the state.
// loadSnapshot加载由Snapshot()产生的snapshot
func (l *Log) loadSnapshot(r io.Reader) error {
	st, err := decodeState(r)
	if err != nil {
		return err
	}

	l.mtx.Lock()
	l.st = st
	l.mtx.Unlock()

	return nil
}

// Snapshot implements the Log interface.
// Snapshot实现了Log interface
func (l *Log) Snapshot(w io.Writer) (int64, error) {
	start := time.Now()
	defer func() { l.metrics.snapshotDuration.Observe(time.Since(start).Seconds()) }()

	l.mtx.RLock()
	defer l.mtx.RUnlock()

	// 对log的state进行marshal
	b, err := l.st.MarshalBinary()
	if err != nil {
		return 0, err
	}

	// 将marshal的内容写入snapshot文件中
	return io.Copy(w, bytes.NewReader(b))
}

// MarshalBinary serializes all contents of the notification log.
// MarshalBinary序列化所有的notification log的内容
func (l *Log) MarshalBinary() ([]byte, error) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	return l.st.MarshalBinary()
}

// Merge merges notification log state received from the cluster with the local state.
// Merge将从cluster收到的notifcation log state和local state合并
func (l *Log) Merge(b []byte) error {
	st, err := decodeState(bytes.NewReader(b))
	if err != nil {
		return err
	}
	l.mtx.Lock()
	defer l.mtx.Unlock()
	now := l.now()

	for _, e := range st {
		// 遍历各个state进行合并
		if merged := l.st.merge(e, now); merged && !cluster.OversizedMessage(b) {
			// If this is the first we've seen the message and it's
			// not oversized, gossip it to other nodes. We don't
			// propagate oversized messages because they're sent to
			// all nodes already.
			// 如果我们是第一次看到这个message并且它没有oversized，将它gossip到其他nodes
			// 我们不传播oversized message，因为它们已经被发送给所有节点了
			l.broadcast(b)
			l.metrics.propagatedMessagesTotal.Inc()
			level.Debug(l.logger).Log("msg", "gossiping new entry", "entry", e)
		}
	}
	return nil
}

// SetBroadcast sets a broadcast callback that will be invoked with serialized state
// on updates.
// SetBroadcast设置一个broadcast callback，它会在serialized state更新的时候被调用
// 它会将数据进行广播到全网
func (l *Log) SetBroadcast(f func([]byte)) {
	l.mtx.Lock()
	l.broadcast = f
	l.mtx.Unlock()
}

// replaceFile wraps a file that is moved to another filename on closing.
// replaceFile封装了一个file，它会在关闭的时候文件名变为filename
type replaceFile struct {
	*os.File
	filename string
}

func (f *replaceFile) Close() error {
	if err := f.File.Sync(); err != nil {
		return err
	}
	if err := f.File.Close(); err != nil {
		return err
	}
	return os.Rename(f.File.Name(), f.filename)
}

// openReplace opens a new temporary file that is moved to filename on closing.
// openReplace打开一个新的temporary file，并且会在close的时候文件名转换成filename
func openReplace(filename string) (*replaceFile, error) {
	tmpFilename := fmt.Sprintf("%s.%x", filename, uint64(rand.Int63()))

	f, err := os.Create(tmpFilename)
	if err != nil {
		return nil, err
	}

	rf := &replaceFile{
		File:     f,
		filename: filename,
	}
	return rf, nil
}
