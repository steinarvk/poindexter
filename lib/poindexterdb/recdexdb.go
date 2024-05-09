package poindexterdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/steinarvk/poindexter/lib/config"
	"github.com/steinarvk/poindexter/lib/flatten"
)

type Namespace int
type IndexKey int
type RecordIndex int

type indexedUnmarshalledEntry struct {
	index  RecordIndex
	record interface{}
}

type indexedSerializedEntry struct {
	index RecordIndex
	line  string
}

type indexedFlattenedEntry struct {
	index  RecordIndex
	record *flatten.Record
}

type InsertionResult struct {
	Index      RecordIndex
	RecordUUID uuid.UUID
	Ok         bool
	Duplicate  bool
	Inserted   bool
	Error      error
}

type InsertionResultSummary struct {
	NumProcessed int
	NumOk        int
	NumDuplicate int
	NumInserted  int
	NumError     int
}

type BatchInsertionResult struct {
	Results []InsertionResult
	Summary InsertionResultSummary
}

type nonsensitiveOptions struct {
	batchSize          int
	insertionWorkers   int
	flattenWorkers     int
	verbosity          int
	disableAutoMigrate bool
}

type dbOptions struct {
	nonsensitiveOptions nonsensitiveOptions
	sensitiveConfig     config.Config
}

func (o dbOptions) isVerbose(level int) bool {
	return o.nonsensitiveOptions.verbosity >= level
}

func (o dbOptions) getLimits() config.Limits {
	return o.sensitiveConfig.Limits
}

func (o *nonsensitiveOptions) validateAndSetDefaults() error {
	if o.batchSize == 0 {
		o.batchSize = 1000
	}

	if o.insertionWorkers == 0 {
		o.insertionWorkers = 16
	}

	if o.flattenWorkers == 0 {
		o.flattenWorkers = 16
	}

	return nil
}

func (o dbOptions) getConfiguredNamespaces() []string {
	return o.sensitiveConfig.Namespaces
}

func (o dbOptions) describeOptionsForLogging() string {
	return fmt.Sprintf("%+v namespaces:%v", o.nonsensitiveOptions, o.getConfiguredNamespaces())
}

type caches struct {
	mu             sync.Mutex
	namespaceCache map[string]Namespace
	keyCache       map[Namespace]map[string]IndexKey
}

func newCaches() *caches {
	return &caches{
		namespaceCache: map[string]Namespace{},
		keyCache:       map[Namespace]map[string]IndexKey{},
	}
}

type DB struct {
	db      *sql.DB
	caches  *caches
	options dbOptions
}

type errNoSuchNamespace struct {
	namespaceName string
}

func (e errNoSuchNamespace) Error() string {
	return fmt.Sprintf("no such namespace: %s", e.namespaceName)
}

func (d *DB) getNamespaceID(namespaceName string) (Namespace, error) {
	d.caches.mu.Lock()
	defer d.caches.mu.Unlock()

	// The namespace "cache" is actually fully pre-loaded and exhaustive.

	nsid, ok := d.caches.namespaceCache[namespaceName]
	if !ok {
		return 0, errNoSuchNamespace{namespaceName}
	}
	return Namespace(nsid), nil
}

func (d *DB) prepopulateNamespaceCache(ctx context.Context) error {
	d.caches.mu.Lock()
	defer d.caches.mu.Unlock()

	// Namespaces must all mentioned in the config, and should be immutable,
	// so this is not really so much a "cache" as pre-loading them all into memory.
	// Including assigning them all IDs if they don't already exist.

	configuredNamespaces := d.options.getConfiguredNamespaces()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if !success {
			tx.Rollback()
		}
	}()

	namespaceMap := map[string]Namespace{}

	loadNamespaces := func(namespaceNames []string) error {
		// Load IDs for all configured namespaces.
		rows, err := tx.QueryContext(ctx, "SELECT namespace_id, namespace_name FROM namespaces WHERE namespace_name = ANY($1)", pq.Array(namespaceNames))
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var namespaceID int
			var namespaceName string

			if err := rows.Scan(&namespaceID, &namespaceName); err != nil {
				return err
			}
			namespaceMap[namespaceName] = Namespace(namespaceID)
		}

		return nil
	}

	insertNewNamespaces := func(namespaceNames []string) error {
		copyNamespacesStmt, err := tx.PrepareContext(ctx, pq.CopyIn("namespaces", "namespace_name"))
		if err != nil {
			return err
		}

		for _, ns := range namespaceNames {
			_, err := copyNamespacesStmt.ExecContext(ctx, ns)
			if err != nil {
				return err
			}
		}

		if _, err := copyNamespacesStmt.ExecContext(ctx); err != nil {
			return err
		}

		return nil
	}

	getMissingNamespaces := func() []string {
		var missingNamespaces []string
		for _, ns := range configuredNamespaces {
			if _, ok := namespaceMap[ns]; !ok {
				missingNamespaces = append(missingNamespaces, ns)
			}
		}
		return missingNamespaces
	}

	if err := loadNamespaces(configuredNamespaces); err != nil {
		return err
	}

	if missingNamespaces := getMissingNamespaces(); len(missingNamespaces) > 0 {
		if err := insertNewNamespaces(missingNamespaces); err != nil {
			return err
		}

		// New namespaces may have been inserted with auto-generated IDs; reload.
		if err := loadNamespaces(configuredNamespaces); err != nil {
			return err
		}
	}

	if missingNamespaces := getMissingNamespaces(); len(missingNamespaces) > 0 {
		return errors.New("internal error: some namespaces still missing after insert")
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	d.caches.namespaceCache = namespaceMap

	if d.options.isVerbose(2) {
		log.Printf("prepopulated namespace map: %v", namespaceMap)
	}

	success = true
	return nil
}

func (d *DB) getKeys(ctx context.Context, tx *sql.Tx, namespaceID Namespace, keysRequired map[string]struct{}) (map[string]IndexKey, error) {
	rv := map[string]IndexKey{}

	d.caches.mu.Lock()
	defer d.caches.mu.Unlock()

	kc, ok := d.caches.keyCache[namespaceID]
	if !ok {
		kc = map[string]IndexKey{}
		d.caches.keyCache[namespaceID] = kc
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

		rows, err := tx.QueryContext(ctx, "SELECT key_id, key_name FROM indexing_keys WHERE namespace_id = $1 AND key_name = ANY($2)", namespaceID, pq.Array(keysToQuery))
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

			k := IndexKey(keyID)

			rv[keyName] = k
			kc[keyName] = k
			delete(remainingKeys, keyName)
		}

		return nil
	}

	if err := doFetchKeys(); err != nil {
		return nil, err
	}

	if len(remainingKeys) > 0 {
		copyKeysStmt, err := tx.PrepareContext(ctx, pq.CopyIn("indexing_keys", "namespace_id", "key_name"))
		if err != nil {
			return nil, err
		}

		for keyName := range remainingKeys {
			_, err := copyKeysStmt.ExecContext(ctx, namespaceID, keyName)
			if err != nil {
				return nil, err
			}
		}

		if _, err := copyKeysStmt.ExecContext(ctx); err != nil {
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

func summarize(result []InsertionResult) *BatchInsertionResult {
	rv := BatchInsertionResult{
		Results: result,
	}
	for _, r := range result {
		rv.Summary.NumProcessed++
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

func (d *DB) insertFlattenedRecordsInBatch(ctx context.Context, nsid Namespace, recordEntries []indexedFlattenedEntry) ([]InsertionResult, error) {
	if d.options.isVerbose(10) {
		log.Printf("insertFlattenedRecordsInBatch: %d records", len(recordEntries))
	}

	result, err := d.internalInsertFlattenedRecordsInBatch(ctx, nsid, recordEntries)
	if d.options.isVerbose(10) {
		log.Printf("insertFlattenedRecordsInBatch: %+v %v", summarize(result).Summary, err)
	}

	return result, err
}

func (d *DB) internalInsertFlattenedRecordsInBatch(ctx context.Context, nsid Namespace, recordEntries []indexedFlattenedEntry) ([]InsertionResult, error) {
	// Note that we cannot require that the superseded record exists,
	// because batches can be arbitrarily reordered.
	// This should be required in the single-item insertion function.

	var results []InsertionResult

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	var success bool

	defer func() {
		if !success && tx != nil {
			tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
		return nil, err
	}

	t0 := time.Now()

	indexByUUID := map[uuid.UUID]RecordIndex{}
	for _, recordEntry := range recordEntries {
		indexByUUID[recordEntry.record.RecordUUID] = recordEntry.index
	}

	keysRequiredMap := map[string]struct{}{}
	toInsertRecordIDs := map[uuid.UUID]struct{}{}
	supersededUUIDs := map[uuid.UUID]struct{}{}
	var allRecordIDs []uuid.UUID
	var allRecordHashes []string
	for _, recordEntry := range recordEntries {
		record := recordEntry.record
		allRecordIDs = append(allRecordIDs, record.RecordUUID)
		allRecordHashes = append(allRecordHashes, record.Hash)
		toInsertRecordIDs[record.RecordUUID] = struct{}{}
		for _, k := range record.Fields {
			keysRequiredMap[k] = struct{}{}
		}
		if record.SupersedesUUID != nil {
			k := *record.SupersedesUUID
			_, alreadyPresent := supersededUUIDs[k]
			if alreadyPresent {
				return nil, fmt.Errorf("duplicate superseded UUID (within batch): %v", k)
			}
			supersededUUIDs[k] = struct{}{}
		}
	}

	keyIDs, err := d.getKeys(ctx, tx, nsid, keysRequiredMap)
	if err != nil {
		return nil, fmt.Errorf("error getting keys: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("error committing keys: %w", err)
	}
	tx = nil

	newTx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("error starting new transaction: %w", err)
	}
	tx = newTx

	// Check which superseded records already exist in the database.
	// Records can be superseded exactly once, but it might be within the batch or in the database.
	// Any duplicate within the batch is an error, and any collision with the DB is also an error.
	// We already checked within the batch.
	if len(supersededUUIDs) > 0 {
		rows, err := tx.QueryContext(ctx, "SELECT record_supersedes_id FROM records WHERE namespace_id = $1 AND record_supersedes_id = ANY($2)", nsid, pq.Array(supersededUUIDs))
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var recordID uuid.UUID
			if err := rows.Scan(&recordID); err != nil {
				return nil, err
			}

			return nil, fmt.Errorf("duplicate superseded UUID (vs. database): %v", recordID)
		}
	}

	// Check for duplicates, to omit them from the batch insert.
	rows, err := tx.QueryContext(ctx, "SELECT record_id FROM records WHERE namespace_id = $1 AND (record_id = ANY($2) OR record_hash = ANY($3))", nsid, pq.Array(allRecordIDs), pq.Array(allRecordHashes))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var recordID uuid.UUID
		if err := rows.Scan(&recordID); err != nil {
			return nil, err
		}

		if d.options.isVerbose(10) {
			log.Printf("record %v is a duplicate", recordID)
		}

		results = append(results, InsertionResult{
			Index:      indexByUUID[recordID],
			RecordUUID: recordID,
			Ok:         true,
			Duplicate:  true,
		})
		delete(toInsertRecordIDs, recordID)
	}

	if len(toInsertRecordIDs) == 0 {
		return results, nil
	}

	copyRecordsStmt, err := tx.PrepareContext(ctx, pq.CopyIn("records", "namespace_id", "record_id", "record_timestamp", "record_hash", "record_data", "record_shape_hash", "record_locked_until", "record_supersedes_id"))
	if err != nil {
		return nil, fmt.Errorf("error preparing copy records: %w", err)
	}

	var numInsertedRecords, numIndexEntriesInserted int

	for _, entry := range recordEntries {
		record := entry.record

		_, present := toInsertRecordIDs[record.RecordUUID]
		if !present {
			continue
		}

		if d.options.isVerbose(10) {
			log.Printf("record %v was successfully inserted", record.RecordUUID)
		}
		results = append(results, InsertionResult{
			Index:      indexByUUID[record.RecordUUID],
			RecordUUID: record.RecordUUID,
			Ok:         true,
			Duplicate:  false,
			Inserted:   true,
		})

		_, err := copyRecordsStmt.ExecContext(
			ctx,
			nsid,
			record.RecordUUID,
			record.Timestamp,
			record.Hash,
			record.CanonicalJSON,
			record.ShapeHash,
			record.LockedUntil,
			record.SupersedesUUID,
		)
		if err != nil {
			return nil, fmt.Errorf("error copying record: %w", err)
		}

		numInsertedRecords++
	}

	if _, err := copyRecordsStmt.ExecContext(ctx); err != nil {
		return nil, fmt.Errorf("error copying record (flush): %w", err)
	}

	copyIndexStmt, err := tx.PrepareContext(ctx, pq.CopyIn("indexing_data", "namespace_id", "key_id", "record_id", "value"))
	if err != nil {
		return nil, fmt.Errorf("error preparing copy index: %w", err)
	}

	for _, entry := range recordEntries {
		record := entry.record

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

				if _, err := copyIndexStmt.ExecContext(ctx, nsid, keyID, record.RecordUUID, nil); err != nil {
					return nil, fmt.Errorf("error copying index entry: %w", err)
				}
			} else {
				for _, value := range values {
					numIndexEntriesInserted++
					if _, err := copyIndexStmt.ExecContext(ctx, nsid, keyID, record.RecordUUID, string(value)); err != nil {
						return nil, fmt.Errorf("error copying index entry: %w", err)
					}
				}
			}
		}
	}

	if _, err := copyIndexStmt.ExecContext(ctx); err != nil {
		return nil, fmt.Errorf("error copying index (flush): %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("error committing new transaction: %w", err)
	}
	success = true

	if d.options.isVerbose(2) {
		log.Printf("%d/%d records were new; inserted %d records and %d index entries after %v", len(toInsertRecordIDs), len(recordEntries), numInsertedRecords, numIndexEntriesInserted, time.Since(t0))
	}

	return results, nil
}

type PostgresConfig struct {
	PostgresUser string
	PostgresDB   string
	PostgresHost string
	PostgresPass string
}

func (p PostgresConfig) MakeConnectionString() (string, error) {
	connStr := fmt.Sprintf("user=%s dbname=%s host=%s password=%s sslmode=disable", p.PostgresUser, p.PostgresDB, p.PostgresHost, p.PostgresPass)
	return connStr, nil
}

type Params struct {
	Postgres      PostgresConfig
	SQLDriverName string
	Verbosity     int
}

func Open(ctx context.Context, params Params, sensitiveConfig config.Config) (*DB, error) {
	connStr, err := params.Postgres.MakeConnectionString()
	if err != nil {
		return nil, err
	}

	sqlDriverName := params.SQLDriverName
	if sqlDriverName == "" {
		sqlDriverName = "postgres"
	}

	log.Printf("using driver name: %q", sqlDriverName)

	rawDB, err := sql.Open(sqlDriverName, connStr)
	if err != nil {
		return nil, err
	}

	if params.Verbosity > 1 {
		log.Printf("connected to database")
	}

	nonsensitiveOptions := nonsensitiveOptions{
		verbosity: params.Verbosity,
	}
	if err := nonsensitiveOptions.validateAndSetDefaults(); err != nil {
		return nil, err
	}

	opts := dbOptions{
		sensitiveConfig:     sensitiveConfig,
		nonsensitiveOptions: nonsensitiveOptions,
	}

	if params.Verbosity > 0 {
		log.Printf("configuration: %s", opts.describeOptionsForLogging())
	}

	db := &DB{
		db:      rawDB,
		caches:  newCaches(),
		options: opts,
	}

	if !opts.nonsensitiveOptions.disableAutoMigrate {
		if params.Verbosity > 1 {
			log.Printf("running migrations")
		}

		if err := db.runMigrations(); err != nil {
			return nil, err
		}
	}

	if params.Verbosity > 1 {
		log.Printf("prepopulating namespace cache")
	}

	if err := db.prepopulateNamespaceCache(ctx); err != nil {
		return nil, err
	}

	if params.Verbosity > 1 {
		log.Printf("poindexterdb ready")
	}

	return db, nil
}

func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) makeFlattener() flatten.Flattener {
	limits := d.options.getLimits()

	return flatten.Flattener{
		MaxSerializedLength:       limits.MaxBytesPerRecord,
		MaxExploredObjectElements: limits.ExplorationObjectFieldLimit,
		MaxTotalFields:            limits.TotalKeyLimit,
		MaxCapturedValueLength:    limits.CapturedValueLengthLimit,
	}
}

func (d *DB) insertSerializedRecords(ctx context.Context, nsid Namespace, inputCh <-chan indexedSerializedEntry, outCh chan<- InsertionResult) error {
	flattenedCh := make(chan indexedFlattenedEntry, 100)

	flattener := d.makeFlattener()

	flattenSerializedWorker := func(ctx context.Context, workerno int) error {
		if d.options.isVerbose(10) {
			log.Printf("insertSerializedRecords: starting flattenSerializedWorker %d", workerno)
		}

		for entry := range inputCh {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			record, err := flattener.FlattenJSON([]byte(entry.line))
			if err != nil {
				if d.options.isVerbose(10) {
					log.Printf("record #%d was invalid (%v)", entry.index, err)
				}
				outCh <- InsertionResult{
					Index:      entry.index,
					RecordUUID: uuid.UUID{},
					Ok:         false,
					Duplicate:  false,
					Error:      err,
				}
				continue
			}

			flattenedCh <- indexedFlattenedEntry{
				index:  entry.index,
				record: record,
			}
		}

		if d.options.isVerbose(10) {
			log.Printf("insertSerializedRecords: flattenSerializedWorker %d finished", workerno)
		}

		return nil
	}

	// Create a new cancellable context to use for workers.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	nWorkers := d.options.nonsensitiveOptions.flattenWorkers
	if nWorkers == 0 {
		return errors.New("internal error: no flatten workers configured")
	}

	backgroundErrors := make(chan error, nWorkers)
	wg := sync.WaitGroup{}

	wg.Add(nWorkers)
	for i := 0; i < nWorkers; i++ {
		go func(workerno int) {
			defer wg.Done()
			if err := flattenSerializedWorker(ctx, workerno); err != nil {
				backgroundErrors <- err
			}
		}(i)
	}
	go func() {
		wg.Wait()
		close(backgroundErrors)
		close(flattenedCh)
	}()

	flatteningResult := make(chan error, 1)
	go func() {
		if d.options.isVerbose(10) {
			log.Printf("insertSerializedRecords: spawning insertFlattenedRecords")
		}
		flatteningResult <- d.insertFlattenedRecords(ctx, nsid, flattenedCh, outCh)
		if d.options.isVerbose(10) {
			log.Printf("insertSerializedRecords: done with insertFlattenedRecords")
		}
		close(flatteningResult)
	}()

	if d.options.isVerbose(10) {
		log.Printf("insertSerializedRecords: waiting for errors")
	}

	for err := range backgroundErrors {
		if err != nil {
			if d.options.isVerbose(2) {
				log.Printf("insertSerializedRecords: cancelling due to error: %v", err)
			}
			cancel(err)
			return err
		}
	}

	if d.options.isVerbose(10) {
		log.Printf("insertSerializedRecords: waiting for flatteningResult")
	}

	if err := <-flatteningResult; err != nil {
		return err
	}

	if d.options.isVerbose(10) {
		log.Printf("insertSerializedRecords: all done")
	}

	return nil
}

func (d *DB) insertFlattenedRecords(ctx context.Context, nsid Namespace, inputCh <-chan indexedFlattenedEntry, outCh chan<- InsertionResult) error {
	// Create a new cancellable context to use for workers.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	batchedCh := make(chan []indexedFlattenedEntry, 100)

	// Goroutine 1: consume records from stream, group into batches.
	batchingWorker := func() error {
		defer func() {
			if d.options.isVerbose(10) {
				log.Printf("batchingWorker done")
			}
			close(batchedCh)
		}()

		var nextBatch []indexedFlattenedEntry

		for entry := range inputCh {
			nextBatch = append(nextBatch, entry)
			if len(nextBatch) >= d.options.nonsensitiveOptions.batchSize {
				batchedCh <- nextBatch
				nextBatch = nil
			}
		}

		if len(nextBatch) > 0 {
			batchedCh <- nextBatch
		}

		return nil
	}

	// Goroutine 2: consume batches, call insertFlattenedRecordsInBatch, send results to batchResults.
	backgroundErrors := make(chan error, 100)

	insertionWorker := func(workerno int) error {
		for batch := range batchedCh {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if d.options.isVerbose(10) {
				log.Printf("insertionWorker %d starting unit of work", workerno)
			}

			results, err := d.insertFlattenedRecordsInBatch(ctx, nsid, batch)
			if err != nil {
				return err
			}
			if len(results) != len(batch) {
				return fmt.Errorf("internal error: mismatched batch sizes: %d vs %d", len(results), len(batch))
			}
			for _, result := range results {
				if d.options.isVerbose(100) {
					log.Printf("insertionWorker %d trying to send result", workerno)
				}
				outCh <- result
				if d.options.isVerbose(100) {
					log.Printf("insertionWorker %d sent result", workerno)
				}
			}
			if d.options.isVerbose(10) {
				log.Printf("insertionWorker %d sent result", workerno)
			}
		}
		if d.options.isVerbose(10) {
			log.Printf("insertionWorker %d done processing batches", workerno)
		}
		return nil
	}

	nWorkers := d.options.nonsensitiveOptions.insertionWorkers
	if nWorkers == 0 {
		return errors.New("internal error: no insertion workers configured")
	}

	wg := sync.WaitGroup{}
	wg.Add(nWorkers)

	for i := 0; i < nWorkers; i++ {
		go func(workerno int) {
			defer func() {
				wg.Done()
				if d.options.isVerbose(10) {
					log.Printf("insertionWorker %d done", workerno)
				}
			}()

			if err := insertionWorker(workerno); err != nil {
				backgroundErrors <- err
			}
		}(i)
	}

	go func() {
		if d.options.isVerbose(10) {
			log.Printf("waiting for insertionWorkers")
		}
		wg.Wait()
		if d.options.isVerbose(10) {
			log.Printf("done waiting for insertionWorkers; closing channels")
		}
		close(backgroundErrors)
	}()

	if d.options.isVerbose(10) {
		log.Printf("insertFlattenedRecords: starting batchingWorker")
	}

	if err := batchingWorker(); err != nil {
		return err
	}

	if d.options.isVerbose(10) {
		log.Printf("insertFlattenedRecords: waiting for errors")
	}

	for err := range backgroundErrors {
		if err != nil {
			if d.options.isVerbose(2) {
				log.Printf("insertFlattenedRecords: cancelling due to error: %v", err)
			}
			cancel(err)
			return err
		}
	}

	if d.options.isVerbose(10) {
		log.Printf("insertFlattenedRecords: all done")
	}

	return nil
}

type TableStats struct {
	SchemaName          string
	TableName           string
	PgRelationSize      int64
	PgIndexesSize       int64
	PgTotalRelationSize int64
	NLiveTuples         int64
	NDeadTuples         int64
}

type Stats struct {
	NumRecords      int
	NumIndexingKeys int
	NumIndexingRows int

	MaxRecordLength       int
	TotalLengthAllRecords int64

	TableStats map[string]TableStats

	TotalSizeAllIndexes   int64
	TotalSizeAllRelations int64
}

func (d *DB) GetStats(ctx context.Context) (*Stats, error) {
	ts, err := d.getTableStats(ctx)
	if err != nil {
		return nil, err
	}

	rv := Stats{
		TableStats: ts,
	}

	for _, t := range ts {
		rv.TotalSizeAllRelations += t.PgTotalRelationSize
		rv.TotalSizeAllIndexes += t.PgIndexesSize
	}

	if err := d.db.QueryRowContext(ctx, "SELECT COUNT(*), SUM(LENGTH(record_data)), MAX(LENGTH(record_data)) FROM records").Scan(&rv.NumRecords, &rv.TotalLengthAllRecords, &rv.MaxRecordLength); err != nil {
		return nil, err
	}

	if err := d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM indexing_keys").Scan(&rv.NumIndexingKeys); err != nil {
		return nil, err
	}

	if err := d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM indexing_data").Scan(&rv.NumIndexingRows); err != nil {
		return nil, err
	}

	return &rv, nil
}

func (d *DB) getTableStats(ctx context.Context) (map[string]TableStats, error) {
	rows, err := d.db.QueryContext(ctx, `
	SELECT
		tables.schemaname
		, tables.relname
		, pg_relation_size(tables.schemaname || '.' || tables.relname) AS pg_relation_size
		, pg_indexes_size(tables.schemaname || '.' || tables.relname) AS pg_indexes_size
		, pg_total_relation_size(tables.schemaname || '.' || tables.relname) AS pg_total_relation_size
		, tables.n_live_tup
		, tables.n_dead_tup
	FROM pg_stat_all_tables AS tables
	WHERE tables.schemaname = 'public'
	;
`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	tableStats := map[string]TableStats{}
	for rows.Next() {
		var tableStatsEntry TableStats
		if err := rows.Scan(
			&tableStatsEntry.SchemaName,
			&tableStatsEntry.TableName,
			&tableStatsEntry.PgRelationSize,
			&tableStatsEntry.PgIndexesSize,
			&tableStatsEntry.PgTotalRelationSize,
			&tableStatsEntry.NLiveTuples,
			&tableStatsEntry.NDeadTuples,
		); err != nil {
			return nil, err
		}
		name := fmt.Sprintf("%s.%s", tableStatsEntry.SchemaName, tableStatsEntry.TableName)
		tableStats[name] = tableStatsEntry
	}

	return tableStats, nil
}

func (d *DB) InsertObject(ctx context.Context, namespaceName string, value interface{}) (*uuid.UUID, error) {
	flattener := d.makeFlattener()

	nsid, err := d.getNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	flat, err := flattener.FlattenObject(value)
	if err != nil {
		return nil, err
	}

	if flat.SupersedesUUID != nil {
		// Check that the object actually exists.
		var count int
		if err := d.db.QueryRowContext(ctx, "SELECT COUNT(record_id) FROM records WHERE namespace_id = $1 AND record_id = $2", nsid, *flat.SupersedesUUID).Scan(&count); err != nil {
			return nil, err
		}

		if count == 0 {
			return nil, fmt.Errorf("record to be superseded (%q) does not exist", *flat.SupersedesUUID)
		}

		if count > 1 {
			return nil, errors.New("internal error: multiple records with same UUID")
		}
	}

	singletonBatch := []indexedFlattenedEntry{
		{
			index:  0,
			record: flat,
		},
	}

	results, err := d.insertFlattenedRecordsInBatch(ctx, nsid, singletonBatch)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, errors.New("internal error: expected exactly one result")
	}

	result := results[0]
	if !result.Ok {
		if result.Error != nil {
			return nil, result.Error
		}
		return nil, errors.New("internal error: unexpected result")
	}

	return &result.RecordUUID, nil
}

func (d *DB) InsertFlattenedRecords(ctx context.Context, namespaceName string, lines []string) (*BatchInsertionResult, error) {
	t0 := time.Now()

	nsid, err := d.getNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	indexedEntries := make(chan indexedSerializedEntry, len(lines))
	for i, line := range lines {
		indexedEntries <- indexedSerializedEntry{
			index: RecordIndex(i),
			line:  line,
		}
	}
	close(indexedEntries)

	results := make(chan InsertionResult, 5)
	bgErr := make(chan error, 1)

	go func() {
		if d.options.isVerbose(10) {
			log.Printf("InsertFlattenedRecords: spawning insertSerializedRecords")
		}
		bgErr <- d.insertSerializedRecords(ctx, nsid, indexedEntries, results)
		if d.options.isVerbose(10) {
			log.Printf("InsertFlattenedRecords: done with insertSerializedRecords")
		}
		close(bgErr)
		close(results)
	}()

	var result []InsertionResult
	if d.options.isVerbose(10) {
		log.Printf("starting to consume results")
	}
	for r := range results {
		if d.options.isVerbose(10) {
			log.Printf("received result; now %d", len(result))
		}
		result = append(result, r)
	}
	if d.options.isVerbose(10) {
		log.Printf("done receiving results")
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Index < result[j].Index
	})

	if err := <-bgErr; err != nil {
		return nil, err
	}

	if d.options.isVerbose(10) {
		log.Printf("InsertFlattenedRecords: all done; received %d results", len(result))
	}

	if len(result) != len(lines) {
		return nil, errors.New("internal error: mismatched result lengths")
	}

	summary := summarize(result)

	if d.options.isVerbose(1) {
		numProcessed := summary.Summary.NumProcessed
		numInserted := summary.Summary.NumInserted
		duration := time.Since(t0)
		processedRate := float64(numProcessed) / duration.Seconds()
		insertedRate := float64(numInserted) / duration.Seconds()
		log.Printf("inserted %d/%d records in %v (%.2f/s processed, %.2f/s inserted)", numInserted, numProcessed, duration, processedRate, insertedRate)
	}

	return summary, nil
}

type RawRecordItem struct {
	Namespace string
	RecordID  uuid.UUID
	Timestamp time.Time
	Data      []byte
}

func (d *DB) queryRecords(ctx context.Context, q Query, outCh chan<- RawRecordItem) error {
	if q.OmitLocked {
		return fmt.Errorf("not yet supported (TODO): omit_locked")
	}

	if q.OmitSuperseded {
		return fmt.Errorf("not yet supported (TODO): omit_superseded")
	}

	if q.TimestampBefore != nil || q.TimestampAfter != nil {
		return fmt.Errorf("not yet supported (TODO): timestamp filter")
	}

	nsid, err := d.getNamespaceID(q.Namespace)
	if err != nil {
		return err
	}

	qb := newQueryBuilder((nsid))

	for _, key := range q.FieldsPresent {
		if q.TreatNullsAsAbsent {
			if err := qb.addFieldPresentAndNotNull(key); err != nil {
				return err
			}
		} else {
			if err := qb.addFieldPresent(key); err != nil {
				return err
			}
		}
	}

	for key, values := range q.FieldValues {
		for _, value := range values {
			if err := qb.addFieldHasValue(key, value); err != nil {
				return err
			}
		}
	}

	qb.selectClause = "records.record_id, records.record_timestamp, records.record_data"
	qb.limit = q.Limit

	queryString, queryArgs, err := qb.buildQuery()
	if err != nil {
		return err
	}

	log.Printf("executing query with arguments %v: query %s", queryArgs, queryString)

	executeQueryTwiceAndExplain := q.Debug
	if executeQueryTwiceAndExplain {
		rows, err := d.db.QueryContext(ctx, "EXPLAIN ANALYZE "+queryString, queryArgs...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var explainLine string
			if err := rows.Scan(&explainLine); err != nil {
				return err
			}
			log.Printf("explain analyze: %s", explainLine)
		}

		rows.Close()
	}

	rows, err := d.db.QueryContext(ctx, queryString, queryArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		log.Printf("processing new result")

		var recordID uuid.UUID
		var recordTimestamp time.Time
		var recordData []byte

		if err := rows.Scan(&recordID, &recordTimestamp, &recordData); err != nil {
			return err
		}

		item := RawRecordItem{
			Namespace: q.Namespace,
			RecordID:  recordID,
			Timestamp: recordTimestamp,
			Data:      recordData,
		}

		outCh <- item
	}

	log.Printf("done processing results")

	return nil
}

func (d *DB) QueryRecordsRawList(ctx context.Context, q Query) ([]RawRecordItem, error) {
	ch := make(chan RawRecordItem, 100)

	var items []RawRecordItem

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for item := range ch {
			items = append(items, item)
		}
		wg.Done()
	}()

	if err := d.queryRecords(ctx, q, ch); err != nil {
		close(ch)
		return nil, err
	}
	close(ch)

	wg.Wait()

	return items, nil
}
