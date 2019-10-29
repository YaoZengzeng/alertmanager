// Copyright 2017 Prometheus Team
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

package nflogpb

// IsFiringSubset returns whether the given subset is a subset of the alerts
// that were firing at the time of the last notification.
// IsFiringSubset返回给定的subset是否是alerts的一个subset
// 在上次的notification中firing的
func (m *Entry) IsFiringSubset(subset map[uint64]struct{}) bool {
	set := map[uint64]struct{}{}
	for i := range m.FiringAlerts {
		set[m.FiringAlerts[i]] = struct{}{}
	}

	return isSubset(set, subset)
}

// IsResolvedSubset returns whether the given subset is a subset of the alerts
// that were resolved at the time of the last notification.
// IsResolvedSubset返回给定的subset是不是上一次通知的时候已经resolved的alerts的一个子集
func (m *Entry) IsResolvedSubset(subset map[uint64]struct{}) bool {
	set := map[uint64]struct{}{}
	for i := range m.ResolvedAlerts {
		set[m.ResolvedAlerts[i]] = struct{}{}
	}

	return isSubset(set, subset)
}

func isSubset(set, subset map[uint64]struct{}) bool {
	// 如果subset里面的元素都在set里面，则返回true，因此对于一个alert一个group的我们来说，新的告警永远不会触发
	for k := range subset {
		_, exists := set[k]
		if !exists {
			return false
		}
	}

	return true
}
