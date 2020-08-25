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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/uuid"
	"knative.dev/test-infra/pkg/mysql"
)

// Cluster stores a row in the "Cluster" db table
// Table Schema:
type Cluster struct {
	ClusterId   string
	accessToken string
	BoskosId    string
	ProwId      string
	Status      string
	Zone        string
	lastUpdate  time.Time
}

// DB holds an active database connection
type DB struct {
	*sql.DB
}

func (c Cluster) String() string {
	return fmt.Sprintf("[%v] (ClusterId: %s, BoskosId: %s, ProwId: %s, Status: %s, Zone: %s)",
		c.lastUpdate, c.ClusterId, c.BoskosId, c.ProwId, c.Status, c.Zone)
}

var (
	ctx = context.Background()

	// ErrNotFound indicates that a Prow request wasn't found in the database
	ErrNotFound = errors.New("request not found")

	// ErrKeyConflict indicates that a row cannot be inserted due to key conflict
	ErrKeyConflict = errors.New("Key Conflict")
)

// NewDB returns the DB object with an active database connection
func NewDB(c *mysql.DBConfig) (*DB, error) {
	db, err := c.Connect()
	return &DB{db}, err
}

func (db *DB) InsertCluster(accessToken string, boskosId string, prowId string, status string, zone string, lastUpdate time.Time) error {
	stmt, err := db.Prepare(`INSERT INTO Cluster(AccessToken,BoskosId,ProwId,Status,Zone,lastUpdate)
							VALUES (?,?,?,?,?,?)`)
	defer stmt.Close()

	if err != nil {
		return err
	}

	_, err = stmt.Exec(accessToken, boskosId, prowId, status, zone, lastUpdate, time.Now())
	return err
}

// List all clusters within a certain time window
func (db *DB) ListClusters(window time.Duration) ([]Cluster, error) {
	var result []Cluster

	startTime := time.Now().Add(-1 * window)

	rows, err := db.Query(`
	SELECT ID, BoskosId, AccessToken, ProwId, Status, Zone, LastUpdate
	FROM Cluster
	WHERE lastUpdate > ?`, startTime)

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
func (db *DB) DeleteCluster(clusterId string) error {
	stmt, err := db.Prepare(`
				DELETE FROM Cluster
				WHERE ErrorPattern = ?`)
	defer stmt.Close()

	if err == nil {
		err = execAffectingOneRow(stmt, clusterId)
	}

	return err
}

func (db *DB) generateUnique(idSize int, key string) string {
	var randomid string
	var count int
	for {
		randomid = string(uuid.NewUUID())
		db.QueryRow("SELECT count(*) FROM Cluster Where $1 = $2", key, randomid).Scan(&count)
		if count == 0 {
			return randomid
		}
	}
}

func query() (bool, string, string) {
	// check whether available cluster exists
	return true, "", ""
}

func getWithToken(token string, status chan string) {
	// check in with token

}

func updateCluster(zone, prowid, boskosid string) {
	// assign cluster if available

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
