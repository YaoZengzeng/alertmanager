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
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/memberlist"
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	"github.com/prometheus/client_golang/prometheus"
)

// Channel allows clients to send messages for a specific state type that will be
// broadcasted in a best-effort manner.
// Channel允许clients发送一个特定的state type，它会以best-effort的形式被发送
type Channel struct {
	key          string
	send         func([]byte)
	peers        func() []*memberlist.Node
	sendOversize func(*memberlist.Node, []byte) error

	msgc   chan []byte
	logger log.Logger

	oversizeGossipMessageFailureTotal prometheus.Counter
	oversizeGossipMessageDroppedTotal prometheus.Counter
	oversizeGossipMessageSentTotal    prometheus.Counter
	oversizeGossipDuration            prometheus.Histogram
}

// NewChannel creates a new Channel struct, which handles sending normal and
// oversize messages to peers.
// NewChannel创建一个新的Channel struct，它处理对于normal以及oversize的messages的发送
func NewChannel(
	key string,
	send func([]byte),
	peers func() []*memberlist.Node,
	sendOversize func(*memberlist.Node, []byte) error,
	logger log.Logger,
	stopc chan struct{},
	reg prometheus.Registerer,
) *Channel {
	oversizeGossipMessageFailureTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "alertmanager_oversized_gossip_message_failure_total",
		Help:        "Number of oversized gossip message sends that failed.",
		ConstLabels: prometheus.Labels{"key": key},
	})
	oversizeGossipMessageSentTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "alertmanager_oversized_gossip_message_sent_total",
		Help:        "Number of oversized gossip message sent.",
		ConstLabels: prometheus.Labels{"key": key},
	})
	oversizeGossipMessageDroppedTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "alertmanager_oversized_gossip_message_dropped_total",
		Help:        "Number of oversized gossip messages that were dropped due to a full message queue.",
		ConstLabels: prometheus.Labels{"key": key},
	})
	oversizeGossipDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "alertmanager_oversize_gossip_message_duration_seconds",
		Help:        "Duration of oversized gossip message requests.",
		ConstLabels: prometheus.Labels{"key": key},
	})

	reg.MustRegister(oversizeGossipDuration, oversizeGossipMessageFailureTotal, oversizeGossipMessageDroppedTotal, oversizeGossipMessageSentTotal)

	c := &Channel{
		key:                               key,
		send:                              send,
		peers:                             peers,
		logger:                            logger,
		msgc:                              make(chan []byte, 200),
		sendOversize:                      sendOversize,
		oversizeGossipMessageFailureTotal: oversizeGossipMessageFailureTotal,
		oversizeGossipMessageDroppedTotal: oversizeGossipMessageDroppedTotal,
		oversizeGossipMessageSentTotal:    oversizeGossipMessageSentTotal,
		oversizeGossipDuration:            oversizeGossipDuration,
	}

	go c.handleOverSizedMessages(stopc)

	return c
}

// handleOverSizedMessages prevents memberlist from opening too many parallel
// TCP connections to its peers.
// handleOverSizedMessages防止memberlist打开太多parallel TCP connections到它的peers
func (c *Channel) handleOverSizedMessages(stopc chan struct{}) {
	var wg sync.WaitGroup
	for {
		select {
		case b := <-c.msgc:
			for _, n := range c.peers() {
				wg.Add(1)
				go func(n *memberlist.Node) {
					defer wg.Done()
					c.oversizeGossipMessageSentTotal.Inc()
					start := time.Now()
					// 发送oversized message
					if err := c.sendOversize(n, b); err != nil {
						level.Debug(c.logger).Log("msg", "failed to send reliable", "key", c.key, "node", n, "err", err)
						c.oversizeGossipMessageFailureTotal.Inc()
						return
					}
					c.oversizeGossipDuration.Observe(time.Since(start).Seconds())
				}(n)
			}

			wg.Wait()
		case <-stopc:
			return
		}
	}
}

// Broadcast enqueues a message for broadcasting.
// Braodcast将一个message入队用于发送
func (c *Channel) Broadcast(b []byte) {
	// 将channel的key，也就是nfl或者sil写入
	b, err := proto.Marshal(&clusterpb.Part{Key: c.key, Data: b})
	if err != nil {
		return
	}

	if OversizedMessage(b) {
		select {
		case c.msgc <- b:
		default:
			level.Debug(c.logger).Log("msg", "oversized gossip channel full")
			c.oversizeGossipMessageDroppedTotal.Inc()
		}
	} else {
		// 调用send方法，将结果序列化的信息发送
		c.send(b)
	}
}

// OversizedMessage indicates whether or not the byte payload should be sent
// via TCP.
// OversizedMessage表示负载是否需要通过TCP进行发送
func OversizedMessage(b []byte) bool {
	return len(b) > maxGossipPacketSize/2
}
