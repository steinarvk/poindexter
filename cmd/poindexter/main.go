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
	"github.com/steinarvk/poindexter/lib/flatten"
	"go.uber.org/zap"
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

	zap.L().Sugar().Infof("getOrCreateKey: %d, %s", namespaceID, keyName)

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

	zap.L().Sugar().Infof("getOrCreateNamespace: %s", namespaceName)

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

type InsertionResult struct {
	RecordUUID uuid.UUID
	Ok         bool
	Duplicate  bool
	Inserted   bool
	Error      error
}

type InsertionResultSummary struct {
	NumOk        int
	NumDuplicate int
	NumInserted  int
	NumError     int
}

type BatchInsertionResult struct {
	Results []InsertionResult
	Summary InsertionResultSummary
}

func summarize(result []InsertionResult) *BatchInsertionResult {
	rv := BatchInsertionResult{
		Results: result,
	}
	for _, r := range result {
		if r.Ok {
			rv.Summary.NumOk++
		}
		if r.Duplicate {
			rv.Summary.NumDuplicate++
		}
		if r.Inserted {
			rv.Summary.NumInserted++
		}
		if r.Error != nil {
			rv.Summary.NumError++
		}
	}
	return &rv
}

func (d *DB) InsertRecordBatch(records []*flatten.Record) (*BatchInsertionResult, error) {
	var results []InsertionResult

	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	var success bool

	defer func() {
		if !success {
			tx.Rollback()
		}
	}()

	t0 := time.Now()

	nsid, err := d.GetNamespace(tx, "main")
	if err != nil {
		return nil, err
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
		return nil, err
	}

	rows, err := tx.Query("SELECT record_id FROM records WHERE namespace_id = $1 AND (record_id = ANY($2) OR record_hash = ANY($3))", nsid, pq.Array(allRecordIDs), pq.Array(allRecordHashes))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var recordID uuid.UUID
		if err := rows.Scan(&recordID); err != nil {
			return nil, err
		}
		results = append(results, InsertionResult{
			RecordUUID: recordID,
			Ok:         true,
			Duplicate:  true,
		})
		delete(toInsertRecordIDs, recordID)
	}

	if len(toInsertRecordIDs) == 0 {
		return summarize(results), nil
	}

	copyRecordsStmt, err := tx.Prepare(pq.CopyIn("records", "namespace_id", "record_id", "record_timestamp", "record_hash", "record_data"))
	if err != nil {
		return nil, fmt.Errorf("error preparing copy records: %w", err)
	}

	var numInsertedRecords, numIndexEntriesInserted int

	for _, record := range records {
		_, present := toInsertRecordIDs[record.RecordUUID]
		if !present {
			continue
		}

		results = append(results, InsertionResult{
			RecordUUID: record.RecordUUID,
			Ok:         true,
			Duplicate:  false,
			Inserted:   true,
		})

		_, err := copyRecordsStmt.Exec(nsid, record.RecordUUID, record.Timestamp, record.Hash, record.CanonicalJSON)
		if err != nil {
			return nil, fmt.Errorf("error copying record: %w", err)
		}

		numInsertedRecords++
	}

	if _, err := copyRecordsStmt.Exec(); err != nil {
		return nil, fmt.Errorf("error copying record (flush): %w", err)
	}

	copyIndexStmt, err := tx.Prepare(pq.CopyIn("indexing_data", "namespace_id", "key_id", "record_id", "value"))
	if err != nil {
		return nil, fmt.Errorf("error preparing copy index: %w", err)
	}

	for _, record := range records {
		_, present := toInsertRecordIDs[record.RecordUUID]
		if !present {
			continue
		}

		for _, k := range record.Fields {
			keyID, ok := keyIDs[k]
			if !ok {
				return nil, errors.New("key not found in keyIDs map; internal error")
			}

			values, hasValue := record.FieldValues[k]
			if !hasValue {
				numIndexEntriesInserted++

				if _, err := copyIndexStmt.Exec(nsid, keyID, record.RecordUUID, nil); err != nil {
					return nil, fmt.Errorf("error copying index entry: %w", err)
				}
			} else {
				for _, value := range values {
					numIndexEntriesInserted++
					if _, err := copyIndexStmt.Exec(nsid, keyID, record.RecordUUID, string(value)); err != nil {
						return nil, fmt.Errorf("error copying index entry: %w", err)
					}
				}
			}
		}
	}

	if _, err := copyIndexStmt.Exec(); err != nil {
		return nil, fmt.Errorf("error copying index (flush): %w", err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	success = true

	zap.L().Sugar().Infof("%d/%d records were new; inserted %d records and %d index entries after %v", len(toInsertRecordIDs), len(records), numInsertedRecords, numIndexEntriesInserted, time.Since(t0))

	return summarize(results), nil
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

	result, err := d.InsertRecordBatch(records)
	if err != nil {
		return err
	}

	zap.L().Sugar().Infof("result: %+v", result.Summary)
	return nil
}
