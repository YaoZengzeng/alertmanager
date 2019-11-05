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

package cluster

import (
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/memberlist"
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// Maximum number of messages to be held in the queue.
	// 在队列中维护的最大数目的messages
	maxQueueSize = 4096
	fullState    = "full_state"
	update       = "update"
)

// delegate implements memberlist.Delegate and memberlist.EventDelegate
// and broadcasts its peer's state in the cluster.
// delegate实现了memberlist.Delegate以及memberlist.EventDelegate，并且将它的peer的state
// 广播到cluster
type delegate struct {
	*Peer

	logger log.Logger
	bcast  *memberlist.TransmitLimitedQueue

	messagesReceived     *prometheus.CounterVec
	messagesReceivedSize *prometheus.CounterVec
	messagesSent         *prometheus.CounterVec
	messagesSentSize     *prometheus.CounterVec
	messagesPruned       prometheus.Counter
}

func newDelegate(l log.Logger, reg prometheus.Registerer, p *Peer, retransmit int) *delegate {
	// 创建bcast
	bcast := &memberlist.TransmitLimitedQueue{
		NumNodes:       p.ClusterSize,
		RetransmitMult: retransmit,
	}
	messagesReceived := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "alertmanager_cluster_messages_received_total",
		Help: "Total number of cluster messages received.",
	}, []string{"msg_type"})
	messagesReceivedSize := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "alertmanager_cluster_messages_received_size_total",
		Help: "Total size of cluster messages received.",
	}, []string{"msg_type"})
	messagesSent := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "alertmanager_cluster_messages_sent_total",
		Help: "Total number of cluster messages sent.",
	}, []string{"msg_type"})
	messagesSentSize := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "alertmanager_cluster_messages_sent_size_total",
		Help: "Total size of cluster messages sent.",
	}, []string{"msg_type"})
	messagesPruned := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_cluster_messages_pruned_total",
		Help: "Total number of cluster messages pruned.",
	})
	gossipClusterMembers := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "alertmanager_cluster_members",
		Help: "Number indicating current number of members in cluster.",
	}, func() float64 {
		return float64(p.ClusterSize())
	})
	peerPosition := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "alertmanager_peer_position",
		// Alertmanager认为的它在集群中的位置，这个位置决定了peer在集群中的行为
		Help: "Position the Alertmanager instance believes it's in. The position determines a peer's behavior in the cluster.",
	}, func() float64 {
		return float64(p.Position())
	})
	healthScore := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "alertmanager_cluster_health_score",
		// 集群的健康得分，值越低越好，零值代表完全健康
		Help: "Health score of the cluster. Lower values are better and zero means 'totally healthy'.",
	}, func() float64 {
		return float64(p.mlist.GetHealthScore())
	})
	messagesQueued := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "alertmanager_cluster_messages_queued",
		// 在队列中的cluster messages的数目
		Help: "Number of cluster messages which are queued.",
	}, func() float64 {
		return float64(bcast.NumQueued())
	})

	messagesReceived.WithLabelValues(fullState)
	messagesReceivedSize.WithLabelValues(fullState)
	messagesReceived.WithLabelValues(update)
	messagesReceivedSize.WithLabelValues(update)
	messagesSent.WithLabelValues(fullState)
	messagesSentSize.WithLabelValues(fullState)
	messagesSent.WithLabelValues(update)
	messagesSentSize.WithLabelValues(update)

	reg.MustRegister(messagesReceived, messagesReceivedSize, messagesSent, messagesSentSize,
		gossipClusterMembers, peerPosition, healthScore, messagesQueued, messagesPruned)

	d := &delegate{
		logger:               l,
		Peer:                 p,
		bcast:                bcast,
		messagesReceived:     messagesReceived,
		messagesReceivedSize: messagesReceivedSize,
		messagesSent:         messagesSent,
		messagesSentSize:     messagesSentSize,
		messagesPruned:       messagesPruned,
	}

	go d.handleQueueDepth()

	return d
}

// NodeMeta retrieves meta-data about the current node when broadcasting an alive message.
// NodeMeta检索当前节点的meta-data，当前广播一个alive message的时候
func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

// NotifyMsg is the callback invoked when a user-level gossip message is received.
// NotifyMsg是收到一个user-level gossip message之后的回调函数
func (d *delegate) NotifyMsg(b []byte) {
	d.messagesReceived.WithLabelValues(update).Inc()
	d.messagesReceivedSize.WithLabelValues(update).Add(float64(len(b)))

	var p clusterpb.Part
	if err := proto.Unmarshal(b, &p); err != nil {
		level.Warn(d.logger).Log("msg", "decode broadcast", "err", err)
		return
	}

	// 如果state不存在，则返回
	// d.states其实就是peer的states
	s, ok := d.states[p.Key]
	if !ok {
		return
	}
	// 否则，将broadcast的结果合并
	if err := s.Merge(p.Data); err != nil {
		level.Warn(d.logger).Log("msg", "merge broadcast", "err", err, "key", p.Key)
		return
	}
}

// GetBroadcasts is called when user data messages can be broadcasted.
// GetBroadcasts会被调用，当用户的数据可以被广播时
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	// delegate的GetBroadcasts被调用时，队列中的内容才会真正被发送
	msgs := d.bcast.GetBroadcasts(overhead, limit)
	d.messagesSent.WithLabelValues(update).Add(float64(len(msgs)))
	for _, m := range msgs {
		d.messagesSentSize.WithLabelValues(update).Add(float64(len(m)))
	}
	return msgs
}

// LocalState is called when gossip fetches local state.
// 当gossip需要获取local state的时候会调用LocalState
func (d *delegate) LocalState(_ bool) []byte {
	all := &clusterpb.FullState{
		Parts: make([]clusterpb.Part, 0, len(d.states)),
	}

	// 这边的states就两种，nfl和sil
	for key, s := range d.states {
		// 对各个states进行marshal
		b, err := s.MarshalBinary()
		if err != nil {
			level.Warn(d.logger).Log("msg", "encode local state", "err", err, "key", key)
			return nil
		}
		all.Parts = append(all.Parts, clusterpb.Part{Key: key, Data: b})
	}
	// 最后将整体进行marshal
	b, err := proto.Marshal(all)
	if err != nil {
		level.Warn(d.logger).Log("msg", "encode local state", "err", err)
		return nil
	}
	d.messagesSent.WithLabelValues(fullState).Inc()
	d.messagesSentSize.WithLabelValues(fullState).Add(float64(len(b)))
	return b
}

func (d *delegate) MergeRemoteState(buf []byte, _ bool) {
	d.messagesReceived.WithLabelValues(fullState).Inc()
	d.messagesReceivedSize.WithLabelValues(fullState).Add(float64(len(buf)))

	var fs clusterpb.FullState
	if err := proto.Unmarshal(buf, &fs); err != nil {
		level.Warn(d.logger).Log("msg", "merge remote state", "err", err)
		return
	}
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	// 获取全局的数据
	for _, p := range fs.Parts {
		// 获取sil或者nfl
		s, ok := d.states[p.Key]
		if !ok {
			level.Warn(d.logger).Log("received", "unknown state key", "len", len(buf), "key", p.Key)
			// 对于不知道的state key就直接跳过
			continue
		}
		// 直接调用对应的方法进行Merge
		if err := s.Merge(p.Data); err != nil {
			level.Warn(d.logger).Log("msg", "merge remote state", "err", err, "key", p.Key)
			return
		}
	}
}

// NotifyJoin is called if a peer joins the cluster.
func (d *delegate) NotifyJoin(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyJoin", "node", n.Name, "addr", n.Address())
	d.Peer.peerJoin(n)
}

// NotifyLeave is called if a peer leaves the cluster.
func (d *delegate) NotifyLeave(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyLeave", "node", n.Name, "addr", n.Address())
	d.Peer.peerLeave(n)
}

// NotifyUpdate is called if a cluster peer gets updated.
func (d *delegate) NotifyUpdate(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyUpdate", "node", n.Name, "addr", n.Address())
	d.Peer.peerUpdate(n)
}

// handleQueueDepth ensures that the queue doesn't grow unbounded by pruning
// older messages at regular interval.
// handleQueueDepth确保队列不会无限制地增长，通过定期清除older meesages
func (d *delegate) handleQueueDepth() {
	for {
		select {
		case <-d.stopc:
			return
		case <-time.After(15 * time.Minute):
			n := d.bcast.NumQueued()
			if n > maxQueueSize {
				level.Warn(d.logger).Log("msg", "dropping messages because too many are queued", "current", n, "limit", maxQueueSize)
				d.bcast.Prune(maxQueueSize)
				d.messagesPruned.Add(float64(n - maxQueueSize))
			}
		}
	}
}