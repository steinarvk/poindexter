package main

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

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

	t0 := time.Now()

	nsid, err := d.GetNamespace(tx, "main")
	if err != nil {
		tx.Rollback()
		return err
	}

	keyIDs := map[string]int{}

	for _, record := range records {
		for _, k := range record.Fields {
			keyID, err := d.GetKey(tx, nsid, k)
			if err != nil {
				tx.Rollback()
				return err
			}
			keyIDs[k] = keyID
		}
	}

	log.Printf("ready with %v field names at %v", len(keyIDs), time.Since(t0))

	copyRecordsStmt, err := tx.Prepare(pq.CopyIn("records", "record_namespace", "record_id", "record_timestamp", "record_hash", "record_data"))
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("error preparing copy records: %w", err)
	}

	numTotalRecords := len(records)

	for recordNo, record := range records {
		_, err := copyRecordsStmt.Exec(nsid, record.RecordID, record.Timestamp, record.Hash, record.CanonicalJSON)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error copying record: %w", err)
		}

		duration := time.Since(t0)
		log.Printf("Inserted record %d of %d after %v", recordNo+1, numTotalRecords, duration)
	}

	if _, err := copyRecordsStmt.Exec(); err != nil {
		tx.Rollback()
		return err
	}

	copyIndexStmt, err := tx.Prepare(pq.CopyIn("indexing_data", "namespace_id", "key_id", "record_id", "value"))
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("error preparing copy index: %w", err)
	}

	var numRecords, numIndexEntries int

	for recordNo, record := range records {
		/*
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
		*/
		numRecords++

		duration := time.Since(t0)
		log.Printf("Inserted index for record %d of %d after %v", recordNo+1, numTotalRecords, duration)

		for _, k := range record.Fields {
			keyID, ok := keyIDs[k]
			if !ok {
				tx.Rollback()
				return errors.New("key not found in keyIDs map; internal error")
			}

			values, hasValue := record.FieldValues[k]
			if !hasValue {
				numIndexEntries++

				if _, err := copyIndexStmt.Exec(nsid, keyID, record.RecordID, nil); err != nil {
					tx.Rollback()
					return fmt.Errorf("error copying index entry: %w", err)
				}
			} else {
				for _, value := range values {
					numIndexEntries++
					if _, err := copyIndexStmt.Exec(nsid, keyID, record.RecordID, value); err != nil {
						tx.Rollback()
						return fmt.Errorf("error copying index entry: %w", err)
					}
				}
			}
		}
	}

	if _, err := copyIndexStmt.Exec(); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Inserted %d records and %d index entries after %v", numRecords, numIndexEntries, time.Since(t0))

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
