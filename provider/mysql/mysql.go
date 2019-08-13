package mysql

import (
	"errors"
	"fmt"
	"time"
	"encoding/json"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"github.com/prometheus/common/model"
	"github.com/prometheus/alertmanager/types"
)

var (
	// ErrNotFound is returned if cannot find the Alert.
	ErrNotFound = errors.New("alert not found")
)

type MysqlConfig struct {
	User		string
	Password 	string
	Address		string
	Port 		string
}

type AlertItem struct {
	Id           string
	Alertname    string
	Severity    string
	Resourcetype string
	Source       string
	Start        time.Time
	End          time.Time

	// Optional fields
	Organization string
	Project      string
	Cluster      string
	Namespace    string
	Node         string
	Pod          string
	Deployment   string
	Statefulset  string

	// All annotations are marshalled into this field.
	Annotations string

	// Extend fileds, all other labels will be marshalled into this field.
	Extend string

	// ExtendLabels is used to match alerts with labels other than fixed ones.
	// Only used in query.
	Extend map[string]string
}

// The alert item stored in db will include `counter` field, so wrap it with AlertDBItem.
type AlertDBItem struct {
	AlertItem
	Count int
}

type DB struct {
	*sqlx.DB

	logger 		log.Logger
}

func initializeMysql(config *MysqlConfig, l log.Logger) (*DB, error) {
	// Create alertdb and alerts table if necessary.
	db, err := sqlx.Connect("mysql", fmt.Sprintf("%s:%s@(%s:%s)/mysql?parseTime=true&loc=Local", config.User, config.Password, config.Address, config.Port))
	if err != nil {
		return nil, fmt.Errorf("Connect database failed: %v", err)
	}

	schema := `CREATE DATABASE IF NOT EXISTS ALERTDB;`
	_, err = db.Exec(schema)
	if err != nil {
		return nil, fmt.Errorf("Create database alertdb failed: %v", err)
	}

	schema = `USE ALERTDB;`
	_, err = db.Exec(schema)
	if err != nil {
		return nil, fmt.Errorf("Change to database alertdb failed: %v", err)
	}

	schema = `CREATE TABLE IF NOT EXISTS alerts (
			id text,
			alertname text,
			severity text,
			resourcetype text,
			source text,
			count integer,
			start timestamp,
			end timestamp,
			organization text,
			project text,
			cluster text,
			namespace text,
			node text,
			pod text,
			deployment text,
			statefulset text,
			annotations text,
			extend text);`
	_, err = db.Exec(schema)
	if err != nil {
		return nil, fmt.Errorf("Create table failed: %v", err)
	}

	// Connect to the alertdb.
	db, err = sqlx.Connect("mysql", fmt.Sprintf("%s:%s@(%s:%s)/ALERTDB?parseTime=true&loc=Local", config.User, config.Password, config.Address, config.Port))
	if err != nil {
		return nil, fmt.Errorf("Connect database ALERTDB failed: %v", err)
	}

	return &DB{DB: db, logger: log.With(l, "component", "mysql"),}, nil
}

// queryResolved query the matching alerts from db directly, matched by labels, start and end time.
func (db *DB) queryResolved(alert AlertDBItem) ([]AlertDBItem, error) {
	// Get alerts with matched labels and start time falls between start and end.
	schema := `SELECT * FROM alerts WHERE alertname REGEXP :alertname and severity REGEXP :severity and resourcetype REGEXP :resourcetype and source REGEXP :source and organization REGEXP :organization
					and project REGEXP :project and cluster REGEXP :cluster and namespace REGEXP :namespace and node REGEXP :node and pod REGEXP :pod and deployment REGEXP :deployment and statefulset REGEXP :statefulset`
	for k, v := range alert.ExtendLabels {
		// For non-fixed labels, use them directly to match the extend field.
		schema = schema + fmt.Sprintf(" and extend REGEXP %s and extend %s ", k, v)
	}
	schema := schema + fmt.Sprintf(`and start >= :start and start <= :end and end >= NOW() ORDER BY start DESC`)

	res := []AlertDBItem{}
	nstmt, err := db.PrepareNamed(schema)
	err = nstmt.Select(&res, alert)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// queryLastAlert query the matching alert from db directly, matched by id.
func (db *DB) queryLastAlert(alert AlertDBItem) ([]AlertDBItem, error) {
	schema := fmt.Sprintf(`SELECT * FROM alerts WHERE id REGEXP :id ORDER BY start DESC LIMIT 1`)
	res := []AlertDBItem{}
	nstmt, err := db.PrepareNamed(schema)
	err = nstmt.Select(&res, alert)
	if err != nil {
		return nil, err
	}

	return res, nil	
}

// queryUnresolved query the unresolved alerts from db directly.
func (db *DB) queryUnresolved() ([]AlertDBItem, error) {
	res := []AlertDBItem{}
	err := db.Select(&res, `SELECT * FROM alerts WHERE end > ?`, time.Now())
	if err != nil {
		return nil, err
	}

	return res, nil
}

// updateAlert update the alert in db directly.
func (db *DB) updateAlert(alert AlertDBItem) error {
	_, err := db.NamedExec("UPDATE alerts SET count=:count, start=:start, end=:end WHERE id=:id", alert)
	if err != nil {
		return err
	}
	return nil
}

// insertAlert insert the alert to db directly.
func (db *DB) insertAlert(alert AlertDBItem) error {
	_, err := db.NamedExec(`INSERT INTO alerts VALUES (:id, :alertname, :severity, :resourcetype, :source, :count,
							:start, :end, :organization, :project, :cluster, :namespace, :node, :pod, :deployment,
							:statefulset, :annotations, :extend)`, alert)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) InsertAlert(alert AlertItem) error {
	item := AlertDBItem{}

	// Check if the AlertItem exists.
	err := db.Get(&item, "SELECT * from alerts WHERE id = ? and end > ?", alert.Id, alert.Start)
	// If return error, we assume that there is no overlapping item.
	if err != nil {
		item = AlertDBItem{AlertItem: alert, Count: 1}
		return db.insertAlert(item)
	}

	// Aggregate the alerts, then update.
	item.End = alert.End
	item.Count++

	return db.updateAlert(item)
}

func (db *DB) itemToAlert(item AlertDBItem) *types.Alert {
	res := &types.Alert{Alert: model.Alert{Labels: model.LabelSet{}, Annotations: model.LabelSet{}}}
	res.Labels[model.LabelName("alertname")] = model.LabelValue(item.Alertname)
	res.Labels[model.LabelName("severity")] = model.LabelValue(item.Severity)
	res.Labels[model.LabelName("resourcetype")] = model.LabelValue(item.Resourcetype)
	res.Labels[model.LabelName("source")] = model.LabelValue(item.Source)

	res.StartsAt = item.Start
	res.EndsAt = item.End

	converse := func(field string, value string) {
		if len(value) != 0 {
			res.Labels[model.LabelName(field)] = model.LabelValue(value)
		}
	}

	converse("organization", item.Organization)
	converse("project", item.Project)
	converse("cluster", item.Cluster)
	converse("namespace", item.Namespace)
	converse("node", item.Node)
	converse("pod", item.Pod)
	converse("deployment", item.Deployment)
	converse("statefulset", item.Statefulset)

	err := json.Unmarshal([]byte(item.Annotations), &res.Annotations)
	if err != nil {
		// TODO: handle the unmarshal error more correctly.
		level.Error(db.logger).Log("msg", "error on unmarshal annotations of alert", "err", err)
	}

	labels := model.LabelSet{}
	err = json.Unmarshal([]byte(item.Extend), &labels)
	if err != nil {
		level.Error(db.logger).Log("msg", "error on unmarshal extend of alert", "err", err)
	}

	for k, v := range labels {
		res.Labels[k] = v
	}

	return res
}

func (db *DB) alertToItem(alert *types.Alert) AlertDBItem {
	item := AlertDBItem{
		AlertItem: AlertItem{
			Id:	alert.Fingerprint().String(),
			Alertname:		string(alert.Labels[model.LabelName("alertname")]),
			Severity:		string(alert.Labels[model.LabelName("severity")]),
			Resourcetype:	string(alert.Labels[model.LabelName("resourcetype")]),
			Source:			string(alert.Labels[model.LabelName("source")]),
			Start:			alert.StartsAt,
			End:			alert.EndsAt,

			Organization:	string(alert.Labels[model.LabelName("organization")]),
			Project:		string(alert.Labels[model.LabelName("project")]),
			Cluster:		string(alert.Labels[model.LabelName("cluster")]),
			Namespace:		string(alert.Labels[model.LabelName("namespace")]),
			Node:			string(alert.Labels[model.LabelName("node")]),
			Pod:			string(alert.Labels[model.LabelName("pod")]),
			Deployment:		string(alert.Labels[model.LabelName("deployment")]),
			Statefulset:	string(alert.Labels[model.LabelName("statefulset")]),
		},
	}

	annotations, err := json.Marshal(alert.Annotations)
	if err != nil {
		// TODO: handle the marshal error more correctly.
		level.Error(db.logger).Log("msg", "error on marshal annotations of alert", "err", err)
		annotations = []byte{}
	}
	item.Annotations = string(annotations)

	// Remove the already known field from alerts directly.
	// TODO: more elegant way?
	for _, key := range []string{"alertname", "severity", "resourcetype", "source", "organization", "project", "cluster", "namespace", "node", "pod", "deployment", "statefulset"} {
		delete(alert.Labels, model.LabelName(key))
	}

	extend, err := json.Marshal(alert.Labels)
	if err != nil {
		level.Error(db.logger).Log("msg", "error on marshal extend labels of alert", "err", err)
		extend = []byte{}
	}
	item.Extend = string(extend)

	return item
}

func formMatcher(labels map[string]string, start time.Time, end time.Time) AlertDBItem {
	item := AlertDBItem{
		AlertItem: AlertItem{
			Alertname:		labels["alertname"],
			Severity:		labels["severity"],
			Resourcetype:	labels["resourcetype"],
			Source:			labels["source"],
			Start:			start,
			End:			end,

			Organization:	labels["organization"],
			Project:		labels["project"],
			Cluster:		labels["cluster"],
			Namespace:		labels["namespace"],
			Node:			labels["node"],
			Pod:			labels["pod"],
			Deployment:		labels["deployment"],
			Statefulset:	labels["statefulset"],
		},
	}

	// Remove the already known field from labels directly.
	// TODO: more elegant way?
	for _, key := range []string{"alertname", "severity", "resourcetype", "source", "organization", "project", "cluster", "namespace", "node", "pod", "deployment", "statefulset"} {
		delete(labels, key)
	}

	item.ExtendLabels

	return item
}

func (db *DB) GetLastAlert(fp model.Fingerprint) (*types.Alert, error) {
	matcher := AlertDBItem{AlertItem: AlertItem{Id: fp.String()}}
	items, err := db.queryLastAlert(matcher)
	if err != nil {
		return nil, err
	}

	// Just double check to avoid panic.
	if len(items) < 1 {
		return nil, ErrNotFound
	}

	return db.itemToAlert(items[0]), nil
}

func (db *DB) Set(a *types.Alert) error {
	return db.InsertAlert(db.alertToItem(a).AlertItem)
}

func (db *DB) ListUnresolved() []*types.Alert {
	items, err := db.queryUnresolved()
	if err != nil {
		level.Error(db.logger).Log("msg", "error on query unresolved alerts", "err", err)
		return nil
	}

	res := []*types.Alert{}
	for _, item := range items {
		res = append(res, db.itemToAlert(item))
	}

	return res
}

func (db *DB) ListMatched(labels map[string]string, start time.Time, end time.Time) []*types.Alert {
	items, err := db.queryResolved(formMatcher(labels, start, end))
	if err != nil {
		level.Error(db.logger).Log("msg", "error on query alerts", "err", err)
		return nil
	}

	res := []*types.Alert{}
	for _, item := range items {
		res = append(res, db.itemToAlert(item))
	}

	return res
}
