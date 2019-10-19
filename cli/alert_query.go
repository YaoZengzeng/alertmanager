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

package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/api"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/prometheus/alertmanager/cli/format"
	"github.com/prometheus/alertmanager/client"
	"github.com/prometheus/alertmanager/pkg/parse"
)

type alertQueryCmd struct {
	inhibited, silenced, active, unprocessed, resolved bool
	receiver                                 string
	matcherGroups                            []string
	within					 time.Duration
}

const alertQueryHelp = `View and search through current alerts.

Amtool has a simplified prometheus query syntax, but contains robust support for
bash variable expansions. The non-option section of arguments constructs a list
of "Matcher Groups" that will be used to filter your query. The following
examples will attempt to show this behaviour in action:

amtool alert query alertname=foo node=bar

	This query will match all alerts with the alertname=foo and node=bar label
	value pairs set.

amtool alert query foo node=bar

	If alertname is omitted and the first argument does not contain a '=' or a
	'=~' then it will be assumed to be the value of the alertname pair.

amtool alert query 'alertname=~foo.*'

	As well as direct equality, regex matching is also supported. The '=~' syntax
	(similar to prometheus) is used to represent a regex match. Regex matching
	can be used in combination with a direct match.

amtool alert query --within 2h

	Query alerts within (now - 2h, now) time period.

amtool alert query --resolved

	Query alerts that has been resolved, support --within as will.

Amtool supports several flags for filtering the returned alerts by state
(inhibited, silenced, active, unprocessed). If none of these flags is given,
only active alerts are returned.
`

func configureQueryAlertsCmd(cc *kingpin.CmdClause) {
	var (
		a        = &alertQueryCmd{}
		queryCmd = cc.Command("query", alertQueryHelp).Default()
	)
	queryCmd.Flag("inhibited", "Show inhibited alerts").Short('i').BoolVar(&a.inhibited)
	queryCmd.Flag("silenced", "Show silenced alerts").Short('s').BoolVar(&a.silenced)
	queryCmd.Flag("active", "Show active alerts").Short('a').BoolVar(&a.active)
	queryCmd.Flag("unprocessed", "Show unprocessed alerts").Short('u').BoolVar(&a.unprocessed)
	queryCmd.Flag("resolved", "Show resolved alerts").Short('d').BoolVar(&a.resolved)
	queryCmd.Flag("receiver", "Show alerts matching receiver (Supports regex syntax)").Short('r').StringVar(&a.receiver)
	queryCmd.Arg("matcher-groups", "Query filter").StringsVar(&a.matcherGroups)
	queryCmd.Flag("within", "Show silences that will expire or have expired within a duration").DurationVar(&a.within)
	queryCmd.Action(execWithTimeout(a.queryAlerts))
}

func (a *alertQueryCmd) queryAlerts(ctx context.Context, _ *kingpin.ParseContext) error {
	var filterString = ""
	if len(a.matcherGroups) > 0 {
		// Attempt to parse the first argument. If the parser fails
		// then we likely don't have a (=|=~|!=|!~) so lets assume that
		// the user wants alertname=<arg> and prepend `alertname=` to
		// the front.
		m := a.matcherGroups[0]
		_, err := parse.Matcher(m)
		if err != nil {
			a.matcherGroups[0] = fmt.Sprintf("alertname=%s", m)
		}
		filterString = fmt.Sprintf("{%s}", strings.Join(a.matcherGroups, ","))
	}

	c, err := api.NewClient(api.Config{Address: alertmanagerURL.String()})
	if err != nil {
		return err
	}
	alertAPI := client.NewAlertAPI(c)
	// If no selector was passed, default to showing active alerts.
	if !a.silenced && !a.inhibited && !a.active && !a.unprocessed {
		a.active = true
	}

	var fetchedAlerts []*client.ExtendedAlert

	// By default, get the alerts from (now - within, now) time period.
	now := time.Now()
	if a.resolved {
		fetchedAlerts, err = alertAPI.Resolved(ctx, filterString, now.Add(-a.within), now)
	} else {
		fetchedAlerts, err = alertAPI.List(ctx, filterString, a.receiver, a.silenced, a.inhibited, a.active, a.unprocessed, now.Add(-a.within), now)
	}

	if err != nil {
		return err
	}

	formatter, found := format.Formatters[output]
	if !found {
		return errors.New("unknown output formatter")
	}
	return formatter.FormatAlerts(fetchedAlerts)
}
