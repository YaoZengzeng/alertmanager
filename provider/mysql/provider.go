package mysql

import (
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"github.com/prometheus/common/model"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/alertmanager/provider"
)

const alertChannelLength = 200

// Alerts give access to a set of alerts. All methods are goroutine-safe.
type Alerts struct {
	db 			*DB

	mtx 		sync.Mutex
	listeners	map[int]listeningAlerts
	next 		int

	logger 		log.Logger
}

type listeningAlerts struct {
	alerts 	chan *types.Alert
	done 	chan struct{}
}

// NewAlerts returns a new alert provider.
func NewAlerts(dbconfig *MysqlConfig, m types.Marker, l log.Logger) (*Alerts, error) {
	db, err := initializeMysql(dbconfig, l)
	if err != nil {
		return nil, err
	}

	a := &Alerts{
		db:			db,
		listeners:	map[int]listeningAlerts{}
		next:		0,
		logger:		log.With(l, "component", "provider"),
	}

	return a, nil
}

// Close the alert provider.
func (a *Alerts) Close() {
	return
}

// Subscribe returns an iterator over active alerts that have not been
// resolved and successfully notified about.
// They are guaranteed to be in chronological order.
func (a *Alerts) Subscribe() provider.AlertIterator {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	var (
		done = make(chan struct{})
		alerts = a.db.ListUnresolved()
		ch = make(chan *types.Alert, max(len(alerts), alertChannelLength))
	)

	for _, alert := range alerts {
		ch <- alert
	}

	a.listeners[a.next] = listeningAlerts{alerts: ch, done: done}
	a.next++

	return provider.NewAlertIterator(ch, done, nil)
}

// GetPending returns an iterator over all the alerts that have pending notifications.
func (a *Alerts) GetPending() provider.AlertIterator {
	var (
		ch = make(chan *types.Alert, alertChannelLength)
		done = make(chan struct{})
	)

	go func() {
		defer close(ch)

		for _, a := range a.db.ListUnresolved() {
			select {
			case ch <- a:
			case <-done:
				return
			}
		}
	}()

	return provider.NewAlertIterator(ch, done, nil)
}

// Get returns the alert for a given fingerprint.
func (a *Alerts) Get(fp model.Fingerprint) (*types.Alert, error) {
	return a.db.GetLastAlert(fp)
}

// Put adds the given alert to the set.
func (a *Alerts) Put(alerts ...*types.Alert) error {
	for _, alert := range alerts {
		// Set the alert first, db would aggregate alerts if necessary.
		err := a.db.Set(alert)
		if err != nil {
			level.Error(a.logger).Log("msg", "error on set alert", "err", err)
			continue
		}

		fp := alert.Fingerprint()

		err = a.db.GetLastAlert(fp)
		if err != nil {
			level.Error(a.logger).Log("msg", "error on get alert", "err", err)
			continue
		}

		a.mtx.Lock()
		for _, l := range a.listeners {
			select {
			case l.alerts <- alert:
			case <-l.done:
			}
		}
		a.mtx.Unlock()
	}

	return nil
}
