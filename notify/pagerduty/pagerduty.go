// Copyright 2019 Prometheus Team
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

package pagerduty

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	commoncfg "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/alertmanager/types"
)

// Notifier implements a Notifier for PagerDuty notifications.
type Notifier struct {
	conf    *config.PagerdutyConfig
	tmpl    *template.Template
	logger  log.Logger
	apiV1   string // for tests.
	client  *http.Client
	retrier *notify.Retrier
}

// New returns a new PagerDuty notifier.
func New(c *config.PagerdutyConfig, t *template.Template, l log.Logger) (*Notifier, error) {
	client, err := commoncfg.NewClientFromConfig(*c.HTTPConfig, "pagerduty")
	if err != nil {
		return nil, err
	}
	n := &Notifier{conf: c, tmpl: t, logger: l, client: client}
	if c.ServiceKey != "" {
		n.apiV1 = "https://events.pagerduty.com/generic/2010-04-15/create_event.json"
		// Retrying can solve the issue on 403 (rate limiting) and 5xx response codes.
		// https://v2.developer.pagerduty.com/docs/trigger-events
		n.retrier = &notify.Retrier{RetryCodes: []int{http.StatusForbidden}, CustomDetailsFunc: errDetails}
	} else {
		// Retrying can solve the issue on 429 (rate limiting) and 5xx response codes.
		// https://v2.developer.pagerduty.com/docs/events-api-v2#api-response-codes--retry-logic
		n.retrier = &notify.Retrier{RetryCodes: []int{http.StatusTooManyRequests}, CustomDetailsFunc: errDetails}
	}
	return n, nil
}

const (
	pagerDutyEventTrigger = "trigger"
	pagerDutyEventResolve = "resolve"
)

type pagerDutyMessage struct {
	RoutingKey  string            `json:"routing_key,omitempty"`
	ServiceKey  string            `json:"service_key,omitempty"`
	DedupKey    string            `json:"dedup_key,omitempty"`
	IncidentKey string            `json:"incident_key,omitempty"`
	EventType   string            `json:"event_type,omitempty"`
	Description string            `json:"description,omitempty"`
	EventAction string            `json:"event_action"`
	Payload     *pagerDutyPayload `json:"payload"`
	Client      string            `json:"client,omitempty"`
	ClientURL   string            `json:"client_url,omitempty"`
	Details     map[string]string `json:"details,omitempty"`
	Images      []pagerDutyImage  `json:"images,omitempty"`
	Links       []pagerDutyLink   `json:"links,omitempty"`
}

type pagerDutyLink struct {
	HRef string `json:"href"`
	Text string `json:"text"`
}

type pagerDutyImage struct {
	Src  string `json:"src"`
	Alt  string `json:"alt"`
	Href string `json:"href"`
}

type pagerDutyPayload struct {
	Summary       string            `json:"summary"`
	Source        string            `json:"source"`
	Severity      string            `json:"severity"`
	Timestamp     string            `json:"timestamp,omitempty"`
	Class         string            `json:"class,omitempty"`
	Component     string            `json:"component,omitempty"`
	Group         string            `json:"group,omitempty"`
	CustomDetails map[string]string `json:"custom_details,omitempty"`
}

func (n *Notifier) notifyV1(
	ctx context.Context,
	eventType string,
	key notify.Key,
	data *template.Data,
	details map[string]string,
	as ...*types.Alert,
) (bool, error) {
	var tmplErr error
	tmpl := notify.TmplText(n.tmpl, data, &tmplErr)

	description, truncated := notify.Truncate(tmpl(n.conf.Description), 1024)
	if truncated {
		level.Debug(n.logger).Log("msg", "Truncated description", "description", description, "key", key)
	}

	msg := &pagerDutyMessage{
		ServiceKey:  tmpl(string(n.conf.ServiceKey)),
		EventType:   eventType,
		IncidentKey: key.Hash(),
		Description: description,
		Details:     details,
	}

	if eventType == pagerDutyEventTrigger {
		msg.Client = tmpl(n.conf.Client)
		msg.ClientURL = tmpl(n.conf.ClientURL)
	}

	if tmplErr != nil {
		return false, fmt.Errorf("failed to template PagerDuty v1 message: %v", tmplErr)
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(msg); err != nil {
		return false, err
	}

	resp, err := notify.PostJSON(ctx, n.client, n.apiV1, &buf)
	if err != nil {
		return true, err
	}
	defer notify.Drain(resp)

	return n.retrier.Check(resp.StatusCode, resp.Body)
}

func (n *Notifier) notifyV2(
	ctx context.Context,
	eventType string,
	key notify.Key,
	data *template.Data,
	details map[string]string,
	as ...*types.Alert,
) (bool, error) {
	var tmplErr error
	tmpl := notify.TmplText(n.tmpl, data, &tmplErr)

	if n.conf.Severity == "" {
		n.conf.Severity = "error"
	}

	summary, truncated := notify.Truncate(tmpl(n.conf.Description), 1024)
	if truncated {
		level.Debug(n.logger).Log("msg", "Truncated summary", "summary", summary, "key", key)
	}

	msg := &pagerDutyMessage{
		Client:      tmpl(n.conf.Client),
		ClientURL:   tmpl(n.conf.ClientURL),
		RoutingKey:  tmpl(string(n.conf.RoutingKey)),
		EventAction: eventType,
		DedupKey:    key.Hash(),
		Images:      make([]pagerDutyImage, len(n.conf.Images)),
		Links:       make([]pagerDutyLink, len(n.conf.Links)),
		Payload: &pagerDutyPayload{
			Summary:       summary,
			Source:        tmpl(n.conf.Client),
			Severity:      tmpl(n.conf.Severity),
			CustomDetails: details,
			Class:         tmpl(n.conf.Class),
			Component:     tmpl(n.conf.Component),
			Group:         tmpl(n.conf.Group),
		},
	}

	for index, item := range n.conf.Images {
		msg.Images[index].Src = tmpl(item.Src)
		msg.Images[index].Alt = tmpl(item.Alt)
		msg.Images[index].Href = tmpl(item.Href)
	}

	for index, item := range n.conf.Links {
		msg.Links[index].HRef = tmpl(item.HRef)
		msg.Links[index].Text = tmpl(item.Text)
	}

	if tmplErr != nil {
		return false, fmt.Errorf("failed to template PagerDuty v2 message: %v", tmplErr)
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(msg); err != nil {
		return false, fmt.Errorf("failed to encode PagerDuty v2 message: %v", err)
	}

	resp, err := notify.PostJSON(ctx, n.client, n.conf.URL.String(), &buf)
	if err != nil {
		return true, fmt.Errorf("failed to post message to PagerDuty: %v", err)
	}
	defer notify.Drain(resp)

	return n.retrier.Check(resp.StatusCode, resp.Body)
}

// Notify implements the Notifier interface.
//
// https://v2.developer.pagerduty.com/docs/events-api-v2
func (n *Notifier) Notify(ctx context.Context, as ...*types.Alert) (bool, error) {
	key, err := notify.ExtractGroupKey(ctx)
	if err != nil {
		return false, err
	}

	var (
		alerts    = types.Alerts(as...)
		data      = notify.GetTemplateData(ctx, n.tmpl, as, n.logger)
		eventType = pagerDutyEventTrigger
	)
	if alerts.Status() == model.AlertResolved {
		eventType = pagerDutyEventResolve
	}

	level.Debug(n.logger).Log("incident", key, "eventType", eventType)

	details := make(map[string]string, len(n.conf.Details))
	for k, v := range n.conf.Details {
		detail, err := n.tmpl.ExecuteTextString(v, data)
		if err != nil {
			return false, fmt.Errorf("failed to template %q: %v", v, err)
		}
		details[k] = detail
	}

	if n.apiV1 != "" {
		return n.notifyV1(ctx, eventType, key, data, details, as...)
	}
	return n.notifyV2(ctx, eventType, key, data, details, as...)
}

func errDetails(status int, body io.Reader) string {
	// See https://v2.developer.pagerduty.com/docs/trigger-events for the v1 events API.
	// See https://v2.developer.pagerduty.com/docs/send-an-event-events-api-v2 for the v2 events API.
	if status != http.StatusBadRequest || body == nil {
		return ""
	}
	var pgr struct {
		Status  string   `json:"status"`
		Message string   `json:"message"`
		Errors  []string `json:"errors"`
	}
	if err := json.NewDecoder(body).Decode(&pgr); err != nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", pgr.Message, strings.Join(pgr.Errors, ","))
}
