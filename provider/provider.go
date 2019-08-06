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

package provider

import (
	"fmt"

	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/types"
)

var (
	// ErrNotFound is returned if a provider cannot find a requested item.
	ErrNotFound = fmt.Errorf("item not found")
)

// Iterator provides the functions common to all iterators. To be useful, a
// specific iterator interface (e.g. AlertIterator) has to be implemented that
// provides a Next method.
// Iterator提供了所有iterator都共用的函数，为了保证有用，一个特定的iterator接口
// 例如，AlertIterator还需要实现一个Next方法
type Iterator interface {
	// Err returns the current error. It is not safe to call it concurrently
	// with other iterator methods or while reading from a channel returned
	// by the iterator.
	Err() error
	// Close must be called to release resources once the iterator is not
	// used anymore.
	// Close必须被调用用以释放资源，一旦iterator不再被使用
	Close()
}

// AlertIterator is an Iterator for Alerts.
type AlertIterator interface {
	Iterator
	// Next returns a channel that will be closed once the iterator is
	// exhausted. It is not necessary to exhaust the iterator but Close must
	// be called in any case to release resources used by the iterator (even
	// if the iterator is exhausted).
	// Next返回一个channel，它会在iterator被耗尽的时候关闭
	// 并不一定每次都要耗尽iterator，但是在任何情况下都必须调用Close关闭。从而释放iterator使用的资源
	Next() <-chan *types.Alert
}

// NewAlertIterator returns a new AlertIterator based on the generic alertIterator type
// NewAlertIterator返回一个新的AlertIterator基于alertIterator类型
func NewAlertIterator(ch <-chan *types.Alert, done chan struct{}, err error) AlertIterator {
	return &alertIterator{
		ch:   ch,
		done: done,
		err:  err,
	}
}

// alertIterator implements AlertIterator. So far, this one fits all providers.
// alertIterator实现了AlertIterator
type alertIterator struct {
	ch   <-chan *types.Alert
	done chan struct{}
	err  error
}

func (ai alertIterator) Next() <-chan *types.Alert {
	return ai.ch
}

func (ai alertIterator) Err() error { return ai.err }
func (ai alertIterator) Close()     { close(ai.done) }

// Alerts gives access to a set of alerts. All methods are goroutine-safe.
// Alerts提供了对于一系列alerts的访问权限，所有的方法都是goroutine-safe的
type Alerts interface {
	// Subscribe returns an iterator over active alerts that have not been
	// resolved and successfully notified about.
	// They are not guaranteed to be in chronological order.
	// Subscribe返回一个关于那些还没有被resolved并且成功notified about的iterator
	// 它们保证不按时间排序
	Subscribe() AlertIterator
	// GetPending returns an iterator over all alerts that have
	// pending notifications.
	// GetPending返回一个关于有着pending的notification的alerts的iterator
	GetPending() AlertIterator
	// Get returns the alert for a given fingerprint.
	// Get返回有着给定的fingerprint的alert
	Get(model.Fingerprint) (*types.Alert, error)
	// Put adds the given alert to the set.
	// Put将给定的alert加入set
	Put(...*types.Alert) error
}
