package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/steinarvk/recdex/lib/flatten"
)

func flattenRecords(reader io.Reader) ([]*flatten.Record, error) {
	scanner := bufio.NewScanner(reader)

	const (
		maxLineLen = 1024 * 1024
	)

	// Handle longer lines
	buf := make([]byte, maxLineLen)
	scanner.Buffer(buf, maxLineLen)

	var rv []*flatten.Record

	flattener := flatten.Flattener{
		MaxSerializedLength:       maxLineLen,
		MaxExploredObjectElements: 100,
		MaxTotalFields:            1000,
		MaxCapturedValueLength:    100,
	}

	for scanner.Scan() {
		line := scanner.Text()

		record, err := flattener.FlattenJSON([]byte(line))
		if err != nil {
			return nil, err
		}

		rv = append(rv, record)
	}

	return rv, nil
}

func main() {
	if err := mainCore(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

type DB struct {
	db *sql.DB

	mu             sync.Mutex
	namespaceCache map[string]int
	keyCache       map[int]map[string]int
}

func (d *DB) GetNamespace(tx *sql.Tx, namespaceName string) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if nsid, ok := d.namespaceCache[namespaceName]; ok {
		return nsid, nil
	}

	rv, err := getOrCreateNamespace(tx, namespaceName)
	if err != nil {
		return 0, err
	}

	d.namespaceCache[namespaceName] = rv
	return rv, nil
}

func (d *DB) GetKey(tx *sql.Tx, namespaceID int, keyName string) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	nskeyCache, ok := d.keyCache[namespaceID]
	if !ok {
		nskeyCache = map[string]int{}
		d.keyCache[namespaceID] = nskeyCache
	}

	keyID, ok := nskeyCache[keyName]
	if ok {
		return keyID, nil
	}

	rv, err := getOrCreateKey(tx, namespaceID, keyName)
	if err != nil {
		return 0, err
	}

	nskeyCache[keyName] = rv

	return rv, nil
}

func (d *DB) Close() error {
	return d.db.Close()
}

func getOrCreateKey(tx *sql.Tx, namespaceID int, keyName string) (int, error) {
	var keyID int

	log.Printf("getOrCreateKey: %d, %s", namespaceID, keyName)

	err := tx.QueryRow("SELECT key_id FROM indexing_keys WHERE namespace_id = $1 AND key_name = $2", namespaceID, keyName).Scan(&keyID)
	switch {
	case err == sql.ErrNoRows:
		err = tx.QueryRow("INSERT INTO indexing_keys(namespace_id, key_name) VALUES($1, $2) RETURNING key_id", namespaceID, keyName).Scan(&keyID)

		if err != nil {
			return 0, fmt.Errorf("query error creating new key: %w", err)
		}
	case err != nil:
		return 0, fmt.Errorf("query error: %w", err)
	}

	return keyID, nil
}

func getOrCreateNamespace(tx *sql.Tx, namespaceName string) (int, error) {
	var namespaceID int

	log.Printf("getOrCreateNamespace: %s", namespaceName)

	err := tx.QueryRow("SELECT namespace_id FROM namespaces WHERE namespace_name = $1", namespaceName).Scan(&namespaceID)
	switch {
	case err == sql.ErrNoRows:
		err = tx.QueryRow("INSERT INTO namespaces(namespace_name) VALUES($1) RETURNING namespace_id", namespaceName).Scan(&namespaceID)

		if err != nil {
			return 0, fmt.Errorf("query error creating new namespace: %w", err)
		}
	case err != nil:
		return 0, fmt.Errorf("query error: %w", err)
	}

	return namespaceID, nil
}

func (d *DB) InsertRecords(records []*flatten.Record) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}

	nsid, err := d.GetNamespace(tx, "main")
	if err != nil {
		tx.Rollback()
		return err
	}

	insertRecordsStmt, err := tx.Prepare(`
		INSERT INTO records (
			record_namespace,
			record_uuid,
			record_timestamp,
			record_hash,
			record_data
		) VALUES ($1, $2, $3, $4, $5) RETURNING record_id
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer insertRecordsStmt.Close()

	insertIndexStmt, err := tx.Prepare(`
		INSERT INTO indexing_data (
			namespace_id,
			key_id,
			record_id,
			value
		) VALUES ($1, $2, $3, $4)
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer insertIndexStmt.Close()

	var numRecords, numIndexEntries int

	numTotalRecords := len(records)

	for recordNo, record := range records {
		_, err = tx.Exec("SAVEPOINT before_insert")
		if err != nil {
			return err
		}

		var recordID int
		err := insertRecordsStmt.QueryRow(nsid, record.RecordID, record.Timestamp, record.Hash, record.CanonicalJSON).Scan(&recordID)

		if pqErr, ok := err.(*pq.Error); ok {
			if pqErr.Code == "23505" {
				log.Printf("Skipping duplicate record %d of %d violated constraint %s", recordNo+1, numTotalRecords, pqErr.Constraint)

				_, rollbackErr := tx.Exec("ROLLBACK TO SAVEPOINT before_insert")
				if rollbackErr != nil {
					return err
				}

				continue
			} else {
				tx.Rollback()
				return err
			}
		} else if err != nil {
			tx.Rollback()
			return err
		}
		numRecords++

		log.Printf("Inserted record %d of %d", recordNo+1, numTotalRecords)

		for _, k := range record.Fields {
			keyID, err := d.GetKey(tx, nsid, k)
			if err != nil {
				tx.Rollback()
				return err
			}

			values, hasValue := record.FieldValues[k]
			if !hasValue {
				numIndexEntries++
				if _, err := insertIndexStmt.Exec(nsid, keyID, recordID, nil); err != nil {
					tx.Rollback()
					return err
				}
			} else {
				for _, value := range values {
					numIndexEntries++
					if _, err := insertIndexStmt.Exec(nsid, keyID, recordID, value); err != nil {
						tx.Rollback()
						return err
					}
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Inserted %d records and %d index entries", numRecords, numIndexEntries)

	return nil
}

func mainCore() error {
	user := os.Getenv("PGUSER")
	dbname := os.Getenv("PGDATABASE")
	host := os.Getenv("PGHOST")
	password := os.Getenv("PGPASSWORD")
	connStr := fmt.Sprintf("user=%s dbname=%s host=%s password=%s sslmode=disable", user, dbname, host, password)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	d := &DB{
		db:             db,
		namespaceCache: map[string]int{},
		keyCache:       map[int]map[string]int{},
	}

	records, err := flattenRecords(os.Stdin)
	if err != nil {
		return err
	}

	return d.InsertRecords(records)
}
