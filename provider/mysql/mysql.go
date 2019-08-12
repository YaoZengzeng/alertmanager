package mysql

import (
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"github.com/prometheus/common/model"
)

var (
	// ErrNotFound is returned if cannot find the Alert.
	ErrNotFound = errors.New("alert not found")
)

type MysqlConfig {
	User		string
	Password 	string
	Address		string
	Port 		string
}

type AlertItem struct {
	Id           string
	Alertname    string
	Serverity    string
	Resourcetype string
	Source       string
	Info         string
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

	// Extend fileds, all other labels will be marshalled into this field.
	Extend string
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
	db, err := sqlx.Connect("mysql", fmt.Sprintf("%s:%s@(%s:%s)/mysql?parseTime=true", config.User, config.Password, config.Address, config.Port))
	if err != nil {
		return nil, fmt.Errorf("Connect database failed: %v", err)
	}

	schema := `CREATE DATABASE IF NOT EXISTS alertdb;`
	result, err := db.Exec(schema)
	if err != nil {
		return nil, fmt.Errorf("Create database alertdb failed: %v", err)
	}

	schema = `USE alertdb;`
	result, err = db.Exec(schema)
	if err != nil {
		return nil, fmt.Errorf("Change to database alertdb failed: %v", err)
	}

	schema = `CREATE TABLE IF NOT EXISTS alerts (
			id text,
			alertname text,
			serverity text,
			resourcetype text,
			source text,
			info text,
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
			extend text);`
	_, err = db.Exec(schema)
	if err != nil {
		return nil, fmt.Errorf("Create table failed: %v", err)
	}

	return &DB{DB: db, logger: log.With(l, "component", "mysql"),}, nil
}

// queryAlert query the matching alerts from db directly.
func (db *DB) queryAlert(alert AlertDBItem, limit int) ([]AlertDBItem, error) {
	schema := fmt.Sprintf("SELECT * FROM alerts WHERE alertname REGEXP :alertname and serverity REGEXP :serverity and resourcetype REGEXP :resourcetype and source REGEXP :source and organization REGEXP :organization
					and project REGEXP :project and cluster REGEXP :cluster and namespace REGEXP :namespace and node REGEXP :node and pod REGEXP :pod and deployment REGEXP :deployment and statefulset REGEXP :statefulset
					and extend REGEXP :extend ORDER BY start DESC LIMIT %d", limit)
	res := []AlertDBItem{}
	nstmt, err := db.PrepareNamed(schema)
	err = nstmt.Select(&res, alert)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// queryUnresolved query the unresolved alerts from db directly.
func (db *DB) queryUnresolved() ([]AlertItem, error) {
	res := []AlertDBItem{}
	err := db.Select(&res, `SELECT * FROM alerts WHERE end > ?`, time.Now())
	if err != nil {
		return nil, err
	}

	return res, nil
}

// updateAlert update the alert in db directly.
func (db *DB) updateAlert(alert AlertDBItem) error {
	_, err := db.NamedExec("UPDATE alerts SET count=:count, end=:end WHERE id=:id", alert)
	if err != nil {
		return err
	}
	return nil
}

// insertAlert insert the alert to db directly.
func (db *DB) insertAlert(alert AlertDBItem) error {
	_, err := db.NamedExec("INSERT INTO alerts VALUES (:id, :alertname, :serverity, :resourcetype, :source, :info, :count, :start, :end, :organization, :project, :cluster, :namespace, :node, :pod, :deployment, :statefulset, :extend)", alert)
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

func itemToAlert(item AlertDBItem) *types.Alert {
	res := &types.Alert{Alert: model.Alert{Labels: model.LabelSet{}, Annotations: model.LabelSet{}}}
	res.Labels[model.LabelName("alertname")] = model.LabelValue(item.Alertname)
	res.Labels[model.LabelName("serverity")] = model.LabelValue(item.Serverity)
	res.Labels[model.LabelName("resourcetype")] = model.LabelValue(item.Resourcetype)
	res.Labels[model.LabelName("source")] = model.LabelValue(item.Source)
	res.Annotations[model.LabelName("info")] = model.LabelValue(item.Info)

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

	// TODO: converse extend field.

	return res
}

func alertToItem(alert *types.Alert) AlertDBItem {
	return AlertDBItem{
		Id:	alert.Fingerprint().String(),
		Alertname:		string(alert.Labels[model.LabelName("alertname")]),
		Serverity:		string(alert.Labels[model.LabelName("serverity")]),
		Resourcetype:	string(alert.Labels[model.LabelName("resourcetype")]),
		Source:			string(alert.Labels[model.LabelName("source")]),
		Info:			string(alert.Annotations[model.LabelName("info")]),
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

		// TODO: fullfill extend field.
	}
}

func (db *DB) GetLastAlert(fp model.Fingerprint) (*types.Alert, error) {
	alert := AlertDBItem{AlertItem: AlertItem{Id: fp.String()}}
	alerts, err := db.queryAlert(alert, limit)
	if err != nil {
		return nil, err
	}

	// Just double check to avoid panic.
	if len(alerts) < 1 {
		return nil, ErrNotFound
	}

	return itemToAlert(alerts[0]), nil
}

func (db *DB) Set(a *types.Alert) error {
	return db.InsertAlert(alertToItem(a))
}

func (db *DB) ListUnresolved() []*types.Alert {
	items, err := db.queryUnresolved()
	if err != nil {
		level.Error(db.logger).Log("msg", "error on query unresolved alerts", "err", err)
		return nil
	}

	res := []*types.Alert{}
	for _, item := range items {
		res = append(res, itemToAlert(item))
	}

	return res
}
