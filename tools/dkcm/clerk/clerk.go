/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package clerk

import (
	"database/sql"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/uuid"
	"knative.dev/test-infra/pkg/mysql"
)

// Cluster stores a row in the "Cluster" db table
// Table Schema: knative.dev/test-infra/tools/dkcm/clerk/schema.sql
type Cluster struct {
	ClusterId   string
	accessToken string
	BoskosId    string
	ProwId      string
	Status      string
	Zone        string
	lastUpdate  time.Time
}

// ClusterConfig is the struct to return to Prow once the cluster is available
type ClusterConfig struct {
	ClusterName string
	BoskosId    string
	Zone        string
}

type ClerkOperations interface {
	//check cluster available, if available, return cluster id and access token
	CheckAvail() (bool, string, string)
	// get with token
	GetCluster(token string) (ClusterConfig, error)
	// delete a cluster entry
	DeleteCluster(accessToken string) error
	// Insert a cluster entry
	InsertCluster(accessToken string, boskosId string, prowId string, status string, zone string, lastUpdate time.Time) error
	// List clutsers within a time interval
	ListClusters(window time.Duration) ([]Cluster, error)
	// Generate Unique access token for Prow
	GenerateToken() string
}

// DB holds an active database connection
type DB struct {
	*sql.DB
}

func (c Cluster) String() string {
	return fmt.Sprintf("[%v] (ClusterId: %s, BoskosId: %s, ProwId: %s, Status: %s, Zone: %s)",
		c.lastUpdate, c.ClusterId, c.BoskosId, c.ProwId, c.Status, c.Zone)
}

// NewDB returns the DB object with an active database connection
func NewDB(c *mysql.DBConfig) (*DB, error) {
	db, err := c.Connect()
	return &DB{db}, err
}

func (db *DB) CheckAvail() (bool, string, string) {
	// check whether available cluster exists
	row := db.QueryRow("SELECT * FROM Cluster WHERE Status = 'Ready' AND ProwId = '0' ")
	cl := &Cluster{}
	err := row.Scan(&cl.ClusterId, &cl.accessToken, &cl.BoskosId, &cl.ProwId, &cl.Status, &cl.Zone, &cl.lastUpdate)
	// no available cluster is found
	if err != nil {
		return false, "", ""
	}
	return true, cl.ClusterId, cl.accessToken
}

// insert a cluster entry into db
func (db *DB) InsertCluster(accessToken string, boskosId string, prowId string, status string, zone string) error {
	stmt, err := db.Prepare(`INSERT INTO Cluster(AccessToken,BoskosId,ProwId,Status,Zone,LastUpdate)
							VALUES (?,?,?,?,?,?)`)
	defer stmt.Close()

	if err != nil {
		return err
	}

	_, err = stmt.Exec(accessToken, boskosId, prowId, status, zone, time.Now())
	return err
}

// List all clusters within a certain time window
func (db *DB) ListClusters(window time.Duration) ([]Cluster, error) {
	var result []Cluster

	startTime := time.Now().Add(-1 * window)

	rows, err := db.Query(`
	SELECT ID, BoskosId, AccessToken, ProwId, Status, Zone, LastUpdate
	FROM Cluster
	WHERE LastUpdate > ?`, startTime)

	for rows.Next() {
		entry := Cluster{}
		err = rows.Scan(&entry.ClusterId, &entry.BoskosId, &entry.accessToken, &entry.ProwId, &entry.Status, &entry.Zone, &entry.lastUpdate)
		if err != nil {
			return result, err
		}
		result = append(result, entry)
	}

	return result, nil

}

// DeleteCluster deletes a row from Cluster db
func (db *DB) DeleteCluster(accessToken string) error {
	stmt, err := db.Prepare(`
				DELETE FROM Cluster
				WHERE AccessToken = ?`)
	defer stmt.Close()

	if err == nil {
		err = execAffectingOneRow(stmt, accessToken)
	}

	return err
}

func (db *DB) GenerateToken() string {
	var randomid string
	var count int
	for {
		randomid = string(uuid.NewUUID())
		db.QueryRow("SELECT count(*) FROM Cluster Where AccessToken = ?", randomid).Scan(&count)
		if count == 0 {
			return randomid
		}
	}
}

func (db *DB) getCluster(token string) (*ClusterConfig, error) {
	// check in with token
	row := db.QueryRow("SELECT * FROM Cluster WHERE AccessToken = $1", token)
	cl := &Cluster{}
	err := row.Scan(&cl.ClusterId, &cl.accessToken, &cl.BoskosId, &cl.ProwId, &cl.Status, &cl.Zone, &cl.lastUpdate)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("Illegal token! No trepassing!: %v", err)
	} else if err != nil {
		return nil, fmt.Errorf("We don't understand your request. Please try again!: %v", err)
	} else {
		if cl.Status == "Ready" {
			clusterName := nameCluster(cl.ClusterId, cl.ProwId)
			return &ClusterConfig{clusterName, cl.BoskosId, cl.Zone}, nil
		} else {
			return &ClusterConfig{}, nil
		}

	}

}

// execAffectingOneRow executes a given statement, expecting one row to be affected.
func execAffectingOneRow(stmt *sql.Stmt, args ...interface{}) error {
	r, err := stmt.Exec(args...)
	if err != nil {
		return fmt.Errorf("could not execute statement: %v", err)
	}
	if rowsAffected, err := r.RowsAffected(); err != nil {
		return fmt.Errorf("could not get rows affected: %v", err)
	} else if rowsAffected != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", rowsAffected)
	}
	return nil
}

// name cluster in the following format: e2e-cluster{id}-{prowid}
func nameCluster(clusterId string, prowId string) string {
	return fmt.Sprintf("e2e-cluster%s-%s", clusterId, prowId)
}
