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

	"github.com/google/uuid"
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

	err := tx.QueryRow("SELECT key_id FROM indexing_keys WHERE namespace_id = $1 AND key_name = ANY($2)", namespaceID, keyName).Scan(&keyID)
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

func (d *DB) GetKeys(tx *sql.Tx, namespaceID int, keysRequired map[string]struct{}) (map[string]int, error) {
	rv := map[string]int{}

	d.mu.Lock()
	defer d.mu.Unlock()

	kc, ok := d.keyCache[namespaceID]
	if !ok {
		kc = map[string]int{}
		d.keyCache[namespaceID] = kc
	}

	var keysToQuery []string
	remainingKeys := map[string]struct{}{}
	for keyName := range keysRequired {
		keyID, ok := kc[keyName]
		if ok {
			rv[keyName] = keyID
		} else {
			keysToQuery = append(keysToQuery, keyName)
			remainingKeys[keyName] = struct{}{}
		}
		rv[keyName] = keyID
	}

	doFetchKeys := func() error {
		if len(keysToQuery) == 0 {
			return nil
		}

		rows, err := tx.Query("SELECT key_id, key_name FROM indexing_keys WHERE namespace_id = $1 AND key_name = ANY($2)", namespaceID, pq.Array(keysToQuery))
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var keyID int
			var keyName string

			if err := rows.Scan(&keyID, &keyName); err != nil {
				return err
			}

			rv[keyName] = keyID
			kc[keyName] = keyID
			delete(remainingKeys, keyName)
		}

		return nil
	}

	if err := doFetchKeys(); err != nil {
		return nil, err
	}

	if len(remainingKeys) > 0 {
		copyKeysStmt, err := tx.Prepare(pq.CopyIn("indexing_keys", "namespace_id", "key_name"))
		if err != nil {
			return nil, err
		}

		for keyName := range remainingKeys {
			_, err := copyKeysStmt.Exec(namespaceID, keyName)
			if err != nil {
				return nil, err
			}
		}

		if _, err := copyKeysStmt.Exec(); err != nil {
			return nil, err
		}
	}

	if err := doFetchKeys(); err != nil {
		return nil, err
	}

	if len(remainingKeys) > 0 {
		return nil, errors.New("internal error: some keys still missing after insert")
	}

	return kc, nil
}

func (d *DB) InsertRecordBatch(records []*flatten.Record) error {
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

	keysRequiredMap := map[string]struct{}{}
	toInsertRecordIDs := map[uuid.UUID]struct{}{}
	var allRecordIDs []uuid.UUID
	var allRecordHashes []string
	for _, record := range records {
		allRecordIDs = append(allRecordIDs, record.RecordUUID)
		allRecordHashes = append(allRecordHashes, record.Hash)
		toInsertRecordIDs[record.RecordUUID] = struct{}{}
		for _, k := range record.Fields {
			keysRequiredMap[k] = struct{}{}
		}
	}

	keyIDs, err := d.GetKeys(tx, nsid, keysRequiredMap)
	if err != nil {
		tx.Rollback()
		return err
	}

	rows, err := tx.Query("SELECT record_id FROM records WHERE namespace_id = $1 AND (record_id = ANY($2) OR record_hash = ANY($3))", nsid, pq.Array(allRecordIDs), pq.Array(allRecordHashes))
	if err != nil {
		tx.Rollback()
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var recordID uuid.UUID
		if err := rows.Scan(&recordID); err != nil {
			tx.Rollback()
			return err
		}
		log.Printf("already exists: %s", recordID)
		delete(toInsertRecordIDs, recordID)
	}

	if len(toInsertRecordIDs) == 0 {
		log.Printf("all records already present")
		return nil
	}

	log.Printf("ready with %v field names at %v; to insert %d/%d", len(keyIDs), time.Since(t0), len(toInsertRecordIDs), len(records))

	copyRecordsStmt, err := tx.Prepare(pq.CopyIn("records", "namespace_id", "record_id", "record_timestamp", "record_hash", "record_data"))
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("error preparing copy records: %w", err)
	}

	numTotalRecords := len(toInsertRecordIDs)

	var numInsertedRecords, numIndexEntriesInserted int

	for recordNo, record := range records {
		_, present := toInsertRecordIDs[record.RecordUUID]
		if !present {
			continue
		}

		_, err := copyRecordsStmt.Exec(nsid, record.RecordUUID, record.Timestamp, record.Hash, record.CanonicalJSON)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error copying record: %w", err)
		}

		numInsertedRecords++

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

	for recordNo, record := range records {
		_, present := toInsertRecordIDs[record.RecordUUID]
		if !present {
			continue
		}

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
				numIndexEntriesInserted++

				if _, err := copyIndexStmt.Exec(nsid, keyID, record.RecordUUID, nil); err != nil {
					tx.Rollback()
					return fmt.Errorf("error copying index entry: %w", err)
				}
			} else {
				for _, value := range values {
					numIndexEntriesInserted++
					if _, err := copyIndexStmt.Exec(nsid, keyID, record.RecordUUID, string(value)); err != nil {
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

	log.Printf("%d/%d records were new; inserted %d records and %d index entries after %v", len(toInsertRecordIDs), len(records), numInsertedRecords, numIndexEntriesInserted, time.Since(t0))

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

	return d.InsertRecordBatch(records)
}
