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

package dispatch

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/types"
)

// DefaultRouteOpts are the defaulting routing options which apply
// to the root route of a routing tree.
// DefaultRouteOpts是默认的路由选项，它会应用到routing tree到根节点
var DefaultRouteOpts = RouteOpts{
	GroupWait:      30 * time.Second,
	GroupInterval:  5 * time.Minute,
	RepeatInterval: 4 * time.Hour,
	GroupBy:        map[model.LabelName]struct{}{},
	GroupByAll:     false,
}

// A Route is a node that contains definitions of how to handle alerts.
// 一个Route是一个node，包含了如何处理alerts的定义
type Route struct {
	parent *Route

	// The configuration parameters for matches of this route.
	// 匹配这条路由的配置参数
	RouteOpts RouteOpts

	// Equality or regex matchers an alert has to fulfill to match
	// this route.
	// 为了满足这个route，一个alert需要匹配的Equality或者regex matchers
	Matchers types.Matchers

	// If true, an alert matches further routes on the same level.
	// 如果为true，则alert在同一个level继续路由匹配
	Continue bool

	// Children routes of this route.
	// 子路由
	Routes []*Route
}

// NewRoute returns a new route.
// NewRoute返回一个新的route
func NewRoute(cr *config.Route, parent *Route) *Route {
	// Create default and overwrite with configured settings.
	// 创建默认的RouteOpts并且用configured settings覆写
	opts := DefaultRouteOpts
	if parent != nil {
		opts = parent.RouteOpts
	}

	if cr.Receiver != "" {
		opts.Receiver = cr.Receiver
	}
	if cr.GroupBy != nil {
		opts.GroupBy = map[model.LabelName]struct{}{}
		for _, ln := range cr.GroupBy {
			opts.GroupBy[ln] = struct{}{}
		}
	}

	opts.GroupByAll = cr.GroupByAll

	if cr.GroupWait != nil {
		opts.GroupWait = time.Duration(*cr.GroupWait)
	}
	if cr.GroupInterval != nil {
		opts.GroupInterval = time.Duration(*cr.GroupInterval)
	}
	// 设置重复告警的时间间隔
	if cr.RepeatInterval != nil {
		opts.RepeatInterval = time.Duration(*cr.RepeatInterval)
	}

	// Build matchers.
	// 构建matchers
	var matchers types.Matchers

	for ln, lv := range cr.Match {
		matchers = append(matchers, types.NewMatcher(model.LabelName(ln), lv))
	}
	for ln, lv := range cr.MatchRE {
		matchers = append(matchers, types.NewRegexMatcher(model.LabelName(ln), lv.Regexp))
	}
	sort.Sort(matchers)

	route := &Route{
		parent:    parent,
		RouteOpts: opts,
		Matchers:  matchers,
		Continue:  cr.Continue,
	}

	// 构建子route
	route.Routes = NewRoutes(cr.Routes, route)

	return route
}

// NewRoutes returns a slice of routes.
func NewRoutes(croutes []*config.Route, parent *Route) []*Route {
	res := []*Route{}
	for _, cr := range croutes {
		res = append(res, NewRoute(cr, parent))
	}
	return res
}

// Match does a depth-first left-to-right search through the route tree
// and returns the matching routing nodes.
// Match做一个深度优先，从左到右的遍历，并且返回匹配的routing nodes
func (r *Route) Match(lset model.LabelSet) []*Route {
	if !r.Matchers.Match(lset) {
		return nil
	}

	var all []*Route

	for _, cr := range r.Routes {
		matches := cr.Match(lset)

		all = append(all, matches...)

		// 如果匹配到了Route并且不进一步continue，则跳出循环并且返回
		if matches != nil && !cr.Continue {
			break
		}
	}

	// If no child nodes were matches, the current node itself is a match.
	// 如果没有一个子节点是匹配的，则返回当前节点作为一个match
	if len(all) == 0 {
		all = append(all, r)
	}

	return all
}

// Key returns a key for the route. It does not uniquely identify a the route in general.
// Key返回route的一个key，一般来说它不能唯一地标识一个route
func (r *Route) Key() string {
	b := make([]byte, 0, 1024)

	if r.parent != nil {
		b = append(b, r.parent.Key()...)
		b = append(b, '/')
	}
	// 从根Route的matcher字符串匹配而来的Key
	return string(append(b, r.Matchers.String()...))
}

// RouteOpts holds various routing options necessary for processing alerts
// that match a given route.
// RouteOpts包含处理匹配给定的route的alerts的各种路由选项
type RouteOpts struct {
	// The identifier of the associated notification configuration.
	// 相关联的notification configuration的identifier
	Receiver string

	// What labels to group alerts by for notifications.
	// 用于聚合alerts进行通知的labels
	GroupBy map[model.LabelName]struct{}

	// Use all alert labels to group.
	// 使用所有的alert labels用于group
	GroupByAll bool

	// How long to wait to group matching alerts before sending
	// a notification.
	// 在发送notification之前，等待聚合matching alerts的时间
	GroupWait      time.Duration
	GroupInterval  time.Duration
	RepeatInterval time.Duration
}

func (ro *RouteOpts) String() string {
	var labels []model.LabelName
	for ln := range ro.GroupBy {
		labels = append(labels, ln)
	}
	return fmt.Sprintf("<RouteOpts send_to:%q group_by:%q group_by_all:%t timers:%q|%q>",
		ro.Receiver, labels, ro.GroupByAll, ro.GroupWait, ro.GroupInterval)
}

// MarshalJSON returns a JSON representation of the routing options.
func (ro *RouteOpts) MarshalJSON() ([]byte, error) {
	v := struct {
		Receiver       string           `json:"receiver"`
		GroupBy        model.LabelNames `json:"groupBy"`
		GroupByAll     bool             `json:"groupByAll"`
		GroupWait      time.Duration    `json:"groupWait"`
		GroupInterval  time.Duration    `json:"groupInterval"`
		RepeatInterval time.Duration    `json:"repeatInterval"`
	}{
		Receiver:       ro.Receiver,
		GroupByAll:     ro.GroupByAll,
		GroupWait:      ro.GroupWait,
		GroupInterval:  ro.GroupInterval,
		RepeatInterval: ro.RepeatInterval,
	}
	for ln := range ro.GroupBy {
		v.GroupBy = append(v.GroupBy, ln)
	}

	return json.Marshal(&v)
}
