package poindexterdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/steinarvk/poindexter/lib/config"
	"github.com/steinarvk/poindexter/lib/dexapi"
	"github.com/steinarvk/poindexter/lib/dexerror"
	"github.com/steinarvk/poindexter/lib/flatten"
	"github.com/steinarvk/poindexter/lib/logging"
	"go.uber.org/zap"
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
	Index             RecordIndex
	RecordUUID        uuid.UUID
	Ok                bool
	Duplicate         bool
	HarmlessDuplicate bool
	Inserted          bool
	Error             error
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
	logger := logging.FromContext(ctx)

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
		logger.Sugar().Infof("prepopulated namespace map: %v", namespaceMap)
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

func summarizeAsIngestionResponse(ctx context.Context, result []InsertionResult) (*dexapi.IngestionResponse, error) {
	logger := logging.FromContext(ctx)

	resultsCopy := make([]InsertionResult, len(result))
	copy(resultsCopy, result)

	sort.Slice(resultsCopy, func(i, j int) bool {
		return resultsCopy[i].Index < resultsCopy[j].Index
	})

	var stats dexapi.IngestionStatsResponse

	var ranges []dexapi.IngestionItemRangeStatus

	uuidAsString := func(u uuid.UUID) string {
		zero := "00000000-0000-0000-0000-000000000000"
		rv := u.String()
		if zero == rv {
			return ""
		}
		return rv
	}

	addToRanges := func(ok bool, errmsg string, index int, firstRecordID uuid.UUID) error {
		if len(ranges) == 0 {
			ranges = append(ranges, dexapi.IngestionItemRangeStatus{
				Index:        index,
				Count:        1,
				Ok:           ok,
				ErrorMessage: errmsg,
				FirstUUID:    uuidAsString(firstRecordID),
			})
			return nil
		}

		last := &ranges[len(ranges)-1]
		same := last.Ok == ok && last.ErrorMessage == errmsg

		nextExpectedIndex := last.Index + last.Count

		if index != nextExpectedIndex {
			return fmt.Errorf("internal error: expected next index %d, got %d", nextExpectedIndex, index)
		}

		if same {
			last.Count++
			return nil
		}

		ranges = append(ranges, dexapi.IngestionItemRangeStatus{
			Index:        index,
			Count:        1,
			Ok:           ok,
			ErrorMessage: errmsg,
			FirstUUID:    uuidAsString(firstRecordID),
		})
		return nil
	}

	for i, r := range resultsCopy {
		if i != int(r.Index) {
			return nil, fmt.Errorf("internal error: at [%d] got index %d", i, r.Index)
		}

		stats.NumProcessed++

		if r.Ok {
			stats.NumOk++
		}

		if r.HarmlessDuplicate {
			stats.NumAlreadyPresent++
		}

		if r.Duplicate && !r.HarmlessDuplicate {
			if r.Ok {
				return nil, fmt.Errorf("internal error: non-harmless duplicate should be an error")
			}
			if r.Error == nil {
				return nil, fmt.Errorf("internal error: non-harmless duplicate should be an error")
			}
		}

		if r.Inserted {
			stats.NumInserted++
		}

		if r.Error != nil {
			stats.NumError++

			errmsg := dexerror.AsPoindexterError(r.Error).PublicMessage()

			logger.Debug("error in response", zap.Error(r.Error))

			if err := addToRanges(false, errmsg, i, r.RecordUUID); err != nil {
				return nil, fmt.Errorf("summarizing response error: %w", err)
			}
		} else {
			if err := addToRanges(r.Ok, "", i, r.RecordUUID); err != nil {
				return nil, fmt.Errorf("summarizing response error: %w", err)
			}
		}
	}

	return &dexapi.IngestionResponse{
		Stats:      stats,
		ItemStatus: ranges,
		AllOK:      stats.NumProcessed == stats.NumOk,
	}, nil
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
	logger := logging.FromContext(ctx)

	if d.options.isVerbose(10) {
		logger.Sugar().Infof("insertFlattenedRecordsInBatch: %d records", len(recordEntries))
	}

	result, err := d.internalInsertFlattenedRecordsInBatch(ctx, nsid, recordEntries)
	if d.options.isVerbose(10) {
		logger.Sugar().Infof("insertFlattenedRecordsInBatch: %+v %v", summarize(result).Summary, err)
	}

	return result, err
}

func (d *DB) internalInsertFlattenedRecordsInBatch(ctx context.Context, nsid Namespace, recordEntries []indexedFlattenedEntry) ([]InsertionResult, error) {
	// Note that we cannot require that the superseded record exists,
	// because batches can be arbitrarily reordered.
	// This should be required in the single-item insertion function.

	logger := logging.FromContext(ctx)

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

	// Guard against duplicates within same batch.
	// Note that there's probably a bunch of bugs with this, but it would be an error in any case.
	indicesByUUID := map[uuid.UUID][]RecordIndex{}

	indexByUUID := map[uuid.UUID]RecordIndex{}
	for _, recordEntry := range recordEntries {
		id := recordEntry.record.RecordUUID
		indexByUUID[id] = recordEntry.index
		indicesByUUID[id] = append(indicesByUUID[id], recordEntry.index)
	}

	keysRequiredMap := map[string]struct{}{}
	toInsertRecordIDs := map[uuid.UUID]struct{}{}
	supersededUUIDs := map[uuid.UUID]struct{}{}
	toInsertHashes := map[uuid.UUID]string{}
	toInsertHashesByIndex := map[int]string{}
	var allRecordIDs []uuid.UUID
	var allRecordHashes []string
	for _, recordEntry := range recordEntries {
		record := recordEntry.record
		allRecordIDs = append(allRecordIDs, record.RecordUUID)
		allRecordHashes = append(allRecordHashes, record.Hash)
		toInsertHashes[record.RecordUUID] = record.Hash
		toInsertHashesByIndex[int(recordEntry.index)] = record.Hash
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
		var supersededList []uuid.UUID
		for k := range supersededUUIDs {
			supersededList = append(supersededList, k)
		}

		rows, err := tx.QueryContext(ctx, "SELECT record_supersedes_id FROM records WHERE namespace_id = $1 AND record_supersedes_id = ANY($2)", nsid, pq.Array(supersededList))
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

	var duplicatesForChecking []uuid.UUID

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
			logger.Sugar().Infof("record %v is a duplicate", recordID)
		}

		duplicatesForChecking = append(duplicatesForChecking, recordID)

		for _, index := range indicesByUUID[recordID] {
			results = append(results, InsertionResult{
				Index:      index,
				RecordUUID: recordID,
				Ok:         true,
				Duplicate:  true,
			})
		}
		delete(toInsertRecordIDs, recordID)
	}

	// TODO similar check in the single-record insertion function?
	if len(duplicatesForChecking) > 0 {
		logger.Info("need to check for duplicates", zap.Int("num_duplicates", len(duplicatesForChecking)))

		hashesInDB, err := d.getRecordHashes(ctx, tx, nsid, duplicatesForChecking)
		if err != nil {
			return nil, fmt.Errorf("error checking for duplicates: %w", err)
		}

		for i := range results {
			if !results[i].Duplicate {
				continue
			}
			wantedToInsertHash := toInsertHashesByIndex[int(results[i].Index)]
			hashInDB := hashesInDB[results[i].RecordUUID]
			isHarmless := wantedToInsertHash == hashInDB

			if isHarmless {
				results[i].HarmlessDuplicate = true
			} else {
				id := results[i].RecordUUID
				results[i].Ok = false
				results[i].Error = dexerror.New(
					dexerror.WithErrorID("bad_record.reusing_id"),
					dexerror.WithHTTPCode(http.StatusConflict),
					dexerror.WithPublicMessage("different record with this ID already exists"),
					dexerror.WithInternalData("record_id", id.String()),
					dexerror.WithInternalData("to_insert_hash", hashesInDB[id]),
					dexerror.WithInternalData("database_hash", hashesInDB[id]),
				)
			}
		}
	}

	logger.Info("checked for duplicates, now time to insert records", zap.Int("num_to_insert", len(toInsertRecordIDs)), zap.Int("num_duplicates", len(duplicatesForChecking)), zap.Int("num_results", len(results)))

	var numInsertedRecords, numIndexEntriesInserted int

	if len(toInsertRecordIDs) > 0 {
		newlySupersededRecords := map[uuid.UUID]uuid.UUID{}

		for _, entry := range recordEntries {
			record := entry.record
			if _, present := toInsertRecordIDs[record.RecordUUID]; !present {
				continue
			}
			if record.SupersedesUUID != nil {
				newlySupersededRecords[*record.SupersedesUUID] = record.RecordUUID
			}
		}

		copyRecordsStmt, err := tx.PrepareContext(ctx, pq.CopyIn("records", "namespace_id", "record_id", "record_timestamp", "record_hash", "record_data", "record_shape_hash", "record_locked_until", "record_supersedes_id", "record_is_deletion_marker", "record_is_superseded_by_id"))
		if err != nil {
			return nil, fmt.Errorf("error preparing copy records: %w", err)
		}

		for _, entry := range recordEntries {
			record := entry.record

			_, present := toInsertRecordIDs[record.RecordUUID]
			if !present {
				continue
			}

			if d.options.isVerbose(10) {
				logger.Sugar().Infof("record %v was successfully inserted", record.RecordUUID)
			}
			results = append(results, InsertionResult{
				Index:      indexByUUID[record.RecordUUID],
				RecordUUID: record.RecordUUID,
				Ok:         true,
				Duplicate:  false,
				Inserted:   true,
			})

			var isSupersededBy *uuid.UUID

			if newID, present := newlySupersededRecords[record.RecordUUID]; present {
				isSupersededBy = &newID
				delete(newlySupersededRecords, record.RecordUUID)
			}

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
				record.IsDeletionMarker,
				isSupersededBy,
			)
			if err != nil {
				return nil, fmt.Errorf("error copying record: %w", err)
			}

			numInsertedRecords++
		}

		if _, err := copyRecordsStmt.ExecContext(ctx); err != nil {
			return nil, fmt.Errorf("error copying record (flush): %w", err)
		}

		if len(newlySupersededRecords) > 0 {
			// Update the superseded records to point to their new superseding records.
			// TODO do this in bulk?
			// But usually superseding records will be for single-insert usecases, so not too terrible.
			for oldID, newID := range newlySupersededRecords {
				if _, err := tx.ExecContext(
					ctx,
					`
					UPDATE records
					SET record_is_superseded_by_id = $1
					WHERE namespace_id = $2 AND record_id = $3
					`,
					newID,
					nsid,
					oldID,
				); err != nil {
					return nil, fmt.Errorf("error updating superseded record: %w", err)
				}
			}
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
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("error committing new transaction: %w", err)
	}
	success = true

	if d.options.isVerbose(2) {
		logger.Sugar().Infof("%d/%d records were new; inserted %d records and %d index entries after %v", len(toInsertRecordIDs), len(recordEntries), numInsertedRecords, numIndexEntriesInserted, time.Since(t0))
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
	logger := logging.FromContext(ctx)

	connStr, err := params.Postgres.MakeConnectionString()
	if err != nil {
		return nil, err
	}

	sqlDriverName := params.SQLDriverName
	if sqlDriverName == "" {
		sqlDriverName = "postgres"
	}

	logger.Sugar().Infof("using driver name: %q", sqlDriverName)

	rawDB, err := sql.Open(sqlDriverName, connStr)
	if err != nil {
		return nil, err
	}

	if params.Verbosity > 1 {
		logger.Sugar().Infof("connected to database")
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
		logger.Sugar().Infof("configuration: %s", opts.describeOptionsForLogging())
	}

	db := &DB{
		db:      rawDB,
		caches:  newCaches(),
		options: opts,
	}

	if !opts.nonsensitiveOptions.disableAutoMigrate {
		if params.Verbosity > 1 {
			logger.Sugar().Infof("running migrations")
		}

		if err := db.runMigrations(); err != nil {
			return nil, err
		}
	}

	if params.Verbosity > 1 {
		logger.Sugar().Infof("prepopulating namespace cache")
	}

	if err := db.prepopulateNamespaceCache(ctx); err != nil {
		return nil, err
	}

	if params.Verbosity > 1 {
		logger.Sugar().Infof("poindexterdb ready")
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
	logger := logging.FromContext(ctx)

	flattenedCh := make(chan indexedFlattenedEntry, 100)

	flattener := d.makeFlattener()

	flattenSerializedWorker := func(ctx context.Context, workerno int) error {
		if d.options.isVerbose(10) {
			logger.Sugar().Infof("insertSerializedRecords: starting flattenSerializedWorker %d", workerno)
		}

		for entry := range inputCh {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			record, err := flattener.FlattenJSON([]byte(entry.line))
			if err != nil {
				if d.options.isVerbose(10) {
					logger.Sugar().Infof("record #%d was invalid (%v)", entry.index, err)
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
			logger.Sugar().Infof("insertSerializedRecords: flattenSerializedWorker %d finished", workerno)
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
			logger.Sugar().Infof("insertSerializedRecords: spawning insertFlattenedRecords")
		}
		flatteningResult <- d.insertFlattenedRecords(ctx, nsid, flattenedCh, outCh)
		if d.options.isVerbose(10) {
			logger.Sugar().Infof("insertSerializedRecords: done with insertFlattenedRecords")
		}
		close(flatteningResult)
	}()

	if d.options.isVerbose(10) {
		logger.Sugar().Infof("insertSerializedRecords: waiting for errors")
	}

	for err := range backgroundErrors {
		if err != nil {
			if d.options.isVerbose(2) {
				logger.Sugar().Infof("insertSerializedRecords: cancelling due to error: %v", err)
			}
			cancel(err)
			return err
		}
	}

	if d.options.isVerbose(10) {
		logger.Sugar().Infof("insertSerializedRecords: waiting for flatteningResult")
	}

	if err := <-flatteningResult; err != nil {
		return err
	}

	if d.options.isVerbose(10) {
		logger.Sugar().Infof("insertSerializedRecords: all done")
	}

	return nil
}

func (d *DB) insertFlattenedRecords(ctx context.Context, nsid Namespace, inputCh <-chan indexedFlattenedEntry, outCh chan<- InsertionResult) error {
	logger := logging.FromContext(ctx)

	// Create a new cancellable context to use for workers.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	batchedCh := make(chan []indexedFlattenedEntry, 100)

	// Goroutine 1: consume records from stream, group into batches.
	batchingWorker := func() error {
		defer func() {
			if d.options.isVerbose(10) {
				logger.Sugar().Infof("batchingWorker done")
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
				logger.Sugar().Infof("insertionWorker %d starting unit of work", workerno)
			}

			results, err := d.insertFlattenedRecordsInBatch(ctx, nsid, batch)
			if err != nil {
				return err
			}
			if len(results) != len(batch) {
				return fmt.Errorf("internal error: mismatched batch sizes: sent %d items got %d results", len(batch), len(results))
			}
			for _, result := range results {
				if d.options.isVerbose(100) {
					logger.Sugar().Infof("insertionWorker %d trying to send result", workerno)
				}
				outCh <- result
				if d.options.isVerbose(100) {
					logger.Sugar().Infof("insertionWorker %d sent result", workerno)
				}
			}
			if d.options.isVerbose(10) {
				logger.Sugar().Infof("insertionWorker %d sent result", workerno)
			}
		}
		if d.options.isVerbose(10) {
			logger.Sugar().Infof("insertionWorker %d done processing batches", workerno)
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
					logger.Sugar().Infof("insertionWorker %d done", workerno)
				}
			}()

			if err := insertionWorker(workerno); err != nil {
				backgroundErrors <- err
			}
		}(i)
	}

	go func() {
		if d.options.isVerbose(10) {
			logger.Sugar().Infof("waiting for insertionWorkers")
		}
		wg.Wait()
		if d.options.isVerbose(10) {
			logger.Sugar().Infof("done waiting for insertionWorkers; closing channels")
		}
		close(backgroundErrors)
	}()

	if d.options.isVerbose(10) {
		logger.Sugar().Infof("insertFlattenedRecords: starting batchingWorker")
	}

	if err := batchingWorker(); err != nil {
		return err
	}

	if d.options.isVerbose(10) {
		logger.Sugar().Infof("insertFlattenedRecords: waiting for errors")
	}

	for err := range backgroundErrors {
		if err != nil {
			if d.options.isVerbose(2) {
				logger.Sugar().Infof("insertFlattenedRecords: cancelling due to error: %v", err)
			}
			cancel(err)
			return err
		}
	}

	if d.options.isVerbose(10) {
		logger.Sugar().Infof("insertFlattenedRecords: all done")
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
			return nil, dexerror.New(
				dexerror.WithErrorID("bad_request.cannot_supersede_nonexistent"),
				dexerror.WithHTTPCode(http.StatusBadRequest),
				dexerror.WithPublicMessage("record to be superseded does not exist"),
				dexerror.WithPublicData(
					"supersedes_id",
					flat.SupersedesUUID.String(),
				),
			)
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

func (d *DB) InsertFlattenedRecords(ctx context.Context, namespaceName string, lines []string) (*dexapi.IngestionResponse, error) {
	return d.InsertFlattenedRecordsAsBatch(ctx, namespaceName, lines, "")
}

func (d *DB) InsertFlattenedRecordsAsBatch(ctx context.Context, namespaceName string, lines []string, batchID string) (*dexapi.IngestionResponse, error) {
	logger := logging.FromContext(ctx)

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
			logger.Sugar().Infof("InsertFlattenedRecords: spawning insertSerializedRecords")
		}
		bgErr <- d.insertSerializedRecords(ctx, nsid, indexedEntries, results)
		if d.options.isVerbose(10) {
			logger.Sugar().Infof("InsertFlattenedRecords: done with insertSerializedRecords")
		}
		close(bgErr)
		close(results)
	}()

	var result []InsertionResult

	if d.options.isVerbose(10) {
		logger.Sugar().Infof("starting to consume results")
	}
	for r := range results {
		if d.options.isVerbose(10) {
			logger.Sugar().Infof("received result; now %d", len(result))
		}
		result = append(result, r)
	}
	if d.options.isVerbose(10) {
		logger.Sugar().Infof("done receiving results")
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Index < result[j].Index
	})

	if err := <-bgErr; err != nil {
		return nil, err
	}

	if d.options.isVerbose(10) {
		logger.Sugar().Infof("InsertFlattenedRecords: all done; received %d results", len(result))
	}

	if len(result) != len(lines) {
		return nil, errors.New("internal error: mismatched result lengths")
	}

	if batchID != "" {
		// insert if not exist batch ID in batch table
		_, err := d.db.ExecContext(ctx, "INSERT INTO processed_batches (namespace_id, batch_data_hash) VALUES ($1, $2) ON CONFLICT DO NOTHING", nsid, batchID)
		if err != nil {
			return nil, err
		}

		logger.Info("registering batch processed", zap.String("batch_id", batchID), zap.String("namespace", namespaceName))
	}

	resp, err := summarizeAsIngestionResponse(ctx, result)
	if err != nil {
		logger.Error("error summarizing ingestion response", zap.Error(err))
		return nil, err
	}

	resp.Namespace = namespaceName

	if batchID != "" {
		resp.BatchName = batchID
	}

	if d.options.isVerbose(1) {
		numProcessed := resp.Stats.NumProcessed
		numInserted := resp.Stats.NumInserted
		duration := time.Since(t0)
		processedRate := float64(numProcessed) / duration.Seconds()
		insertedRate := float64(numInserted) / duration.Seconds()
		logger.Sugar().Infof("inserted %d/%d records in %v (%.2f/s processed, %.2f/s inserted)", numInserted, numProcessed, duration, processedRate, insertedRate)
	}

	return resp, nil
}

func (d *DB) getRecordHashes(ctx context.Context, tx *sql.Tx, nsid Namespace, recordIDs []uuid.UUID) (map[uuid.UUID]string, error) {
	rows, err := tx.QueryContext(ctx, "SELECT record_id, record_hash FROM records WHERE namespace_id = $1 AND record_id = ANY($2)", nsid, pq.Array(recordIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	hashes := map[uuid.UUID]string{}
	for rows.Next() {
		var recordID uuid.UUID
		var recordHash string

		if err := rows.Scan(&recordID, &recordHash); err != nil {
			return nil, err
		}

		hashes[recordID] = recordHash
	}

	return hashes, nil
}

func (d *DB) buildQuery(ctx context.Context, nsid Namespace, q *CompiledQuery) (*queryBuilder, error) {
	qb := newQueryBuilder((nsid))

	if q.OmitLocked {
		return nil, fmt.Errorf("not yet supported (TODO): omit_locked")
	}

	if q.OmitSuperseded {
		if err := qb.setOmitSuperseded(); err != nil {
			return nil, err
		}
	}

	if q.OmitHidden {
		if err := qb.setOmitHidden(); err != nil {
			return nil, err
		}
	}

	if q.TimestampStart != nil {
		if err := qb.setTimestampStartFilterInclusive(*q.TimestampStart); err != nil {
			return nil, err
		}
	}

	if q.TimestampEnd != nil {
		if err := qb.setTimestampEndFilterExclusive(*q.TimestampEnd); err != nil {
			return nil, err
		}
	}

	for _, key := range q.Filter.FieldsPresent {
		if q.TreatNullsAsAbsent {
			if err := qb.addFieldPresentAndNotNull(key); err != nil {
				return nil, err
			}
		} else {
			if err := qb.addFieldPresent(key); err != nil {
				return nil, err
			}
		}
	}

	for key, values := range q.Filter.FieldValues {
		for _, value := range values {
			if err := qb.addFieldHasValue(key, value); err != nil {
				return nil, err
			}
		}
	}

	for _, key := range q.Exclude.FieldsPresent {
		if q.TreatNullsAsAbsent {
			if err := qb.addNegatedFieldPresentAndNotNull(key); err != nil {
				return nil, err
			}
		} else {
			if err := qb.addNegatedFieldPresent(key); err != nil {
				return nil, err
			}
		}
	}

	for key, values := range q.Exclude.FieldValues {
		for _, value := range values {
			if err := qb.addNegatedFieldHasValue(key, value); err != nil {
				return nil, err
			}
		}
	}

	if err := qb.setOrderBy(q.OrderBy); err != nil {
		return nil, err
	}

	return qb, nil

}

func (d *DB) queryRecords(ctx context.Context, namespace string, q *CompiledQuery, outCh chan<- dexapi.RawRecordItem, options ...RequestOption) error {
	opts := requestOptions{}
	for _, o := range options {
		if err := o(&opts); err != nil {
			return err
		}
	}

	nsid, err := d.getNamespaceID(namespace)
	if err != nil {
		return err
	}

	qb, err := d.buildQuery(ctx, nsid, q)
	if err != nil {
		return err
	}

	qb.selectClause = "records.record_id, records.record_timestamp, records.record_data"
	qb.limit = q.Limit

	queryString, queryArgs, err := qb.buildQuery()
	if err != nil {
		return err
	}

	rows, err := d.executeQueryRows(ctx, queryString, queryArgs, true)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var recordID uuid.UUID
		var recordTimestamp time.Time
		var recordData []byte

		if err := rows.Scan(&recordID, &recordTimestamp, &recordData); err != nil {
			return err
		}

		item := dexapi.RawRecordItem{
			RecordMetadata: dexapi.RecordMetadata{
				Namespace:         namespace,
				RecordID:          recordID.String(),
				Timestamp:         recordTimestamp.Format(time.RFC3339Nano),
				TimestampUnixNano: fmt.Sprintf("%d", recordTimestamp.UnixNano()),
			},
			RawRecord: recordData,
		}

		outCh <- item
	}

	return nil
}

func (d *DB) QueryRecordsRawList(ctx context.Context, namespace string, q *CompiledQuery, options ...RequestOption) ([]dexapi.RawRecordItem, error) {
	ch := make(chan dexapi.RawRecordItem, 100)

	var items []dexapi.RawRecordItem

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for item := range ch {
			items = append(items, item)
		}
		wg.Done()
	}()

	if err := d.queryRecords(ctx, namespace, q, ch, options...); err != nil {
		close(ch)
		return nil, err
	}
	close(ch)

	wg.Wait()

	return items, nil
}

func (d *DB) QueryRecordsList(ctx context.Context, namespace string, q *CompiledQuery, options ...RequestOption) ([]dexapi.RecordItem, error) {
	rv, err := d.QueryRecordsRawList(ctx, namespace, q, options...)
	if err != nil {
		return nil, err
	}

	items := make([]dexapi.RecordItem, len(rv))
	for i, rawItem := range rv {
		item := dexapi.RecordItem{
			RecordMetadata: rawItem.RecordMetadata,
		}

		if err := json.Unmarshal(rawItem.RawRecord, &item.Record); err != nil {
			return nil, err
		}

		items[i] = item
	}

	return items, nil
}

func makeRecordItem(namespace string, recordID uuid.UUID, recordTimestamp time.Time, recordData []byte) (dexapi.RecordItem, error) {
	var record dexapi.RecordItem
	record.RecordMetadata.Namespace = namespace
	record.RecordMetadata.RecordID = recordID.String()
	record.RecordMetadata.Timestamp = recordTimestamp.Format(time.RFC3339Nano)
	record.RecordMetadata.TimestampUnixNano = fmt.Sprintf("%d", recordTimestamp.UnixNano())

	if err := json.Unmarshal(recordData, &record.Record); err != nil {
		return dexapi.RecordItem{}, err
	}

	return record, nil
}

func (d *DB) LookupObjectByID(ctx context.Context, namespaceName string, recordID uuid.UUID) (*dexapi.RecordItem, error) {
	nsid, err := d.getNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	var recordTimestamp time.Time
	var recordData []byte

	if err := d.db.QueryRowContext(ctx, `
		SELECT record_timestamp, record_data
		FROM records
		WHERE namespace_id = $1 AND record_id = $2
	`, nsid, recordID).Scan(&recordTimestamp, &recordData); err != nil {
		if err == sql.ErrNoRows {
			return nil, dexerror.New(
				dexerror.WithErrorID("not_found.record"),
				dexerror.WithHTTPCode(http.StatusNotFound),
				dexerror.WithPublicMessage("record not found"),
				dexerror.WithPublicData("record_id", recordID.String()),
			)
		}
		return nil, err
	}

	recorditem, err := makeRecordItem(namespaceName, recordID, recordTimestamp, recordData)
	if err != nil {
		return nil, err
	}

	return &recorditem, nil
}

func (d *DB) CheckBatch(ctx context.Context, namespaceName string, batchName string) (bool, error) {
	nsid, err := d.getNamespaceID(namespaceName)
	if err != nil {
		return false, err
	}

	var dummy int

	if err := d.db.QueryRowContext(ctx, `
		SELECT 1
		FROM processed_batches
		WHERE namespace_id = $1 AND batch_data_hash = $2
	`, nsid, batchName).Scan(&dummy); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (d *DB) executeQueryRows(ctx context.Context, queryString string, queryArgs []interface{}, safeTwice bool) (*sql.Rows, error) {
	contextdata := logging.DataFromContext(ctx)
	logger := contextdata.Logger

	wrapErr := func(err error) error {
		return dexerror.New(
			dexerror.WithErrorID("postgres.query_error"),
			dexerror.WithPublicMessage("internal error: error executing database query"),
			dexerror.WithInternalMessage(err.Error()),
			dexerror.WithInternalData("query_string", queryString),
		)
	}

	debugFlag := contextdata.Debug
	executeQueryTwiceAndExplain := safeTwice && debugFlag

	if debugFlag {
		fmt.Println("executing query: ", queryString, queryArgs)
	}

	if executeQueryTwiceAndExplain {
		rows, err := d.db.QueryContext(ctx, "EXPLAIN ANALYZE "+queryString, queryArgs...)
		if err != nil {
			return nil, wrapErr(err)
		}
		for rows.Next() {
			var explainLine string
			if err := rows.Scan(&explainLine); err != nil {
				return nil, wrapErr(err)
			}
			logger.Sugar().Infof("explain analyze: %s", explainLine)
		}

		rows.Close()
	}

	t0 := time.Now()
	rows, err := d.db.QueryContext(ctx, queryString, queryArgs...)
	duration := time.Since(t0)

	logger.Info(
		"executed query",
		zap.String("query_string", queryString),
		zap.Duration("duration", duration),
	)

	if err != nil {
		return nil, wrapErr(err)
	}

	return rows, nil
}

func (d *DB) LookupObjectByField(ctx context.Context, namespaceName string, fieldName string, canonicalizedFieldValue string, options ...RequestOption) (*dexapi.RecordItem, error) {
	opts := requestOptions{}
	for _, o := range options {
		if err := o(&opts); err != nil {
			return nil, err
		}
	}

	nsid, err := d.getNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	qb := newQueryBuilder((nsid))

	qb.selectClause = "records.record_id, records.record_timestamp, records.record_data"

	qb.limit = 2

	if err := qb.setOmitSuperseded(); err != nil {
		return nil, err
	}

	if err := qb.setOmitHidden(); err != nil {
		return nil, err
	}

	if !strings.HasPrefix(fieldName, ".") {
		fieldName = "." + fieldName
	}

	if err := qb.addFieldHasValue(fieldName, canonicalizedFieldValue); err != nil {
		return nil, err
	}

	queryString, queryArgs, err := qb.buildQuery()
	if err != nil {
		return nil, err
	}

	rows, err := d.executeQueryRows(ctx, queryString, queryArgs, true)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rv *dexapi.RecordItem

	for rows.Next() {
		var recordID uuid.UUID
		var recordTimestamp time.Time
		var recordData []byte

		if err := rows.Scan(&recordID, &recordTimestamp, &recordData); err != nil {
			return nil, err
		}

		if rv != nil {
			return nil, dexerror.New(
				dexerror.WithErrorID("lookup_error.multiple_records"),
				dexerror.WithHTTPCode(http.StatusNotFound),
				dexerror.WithPublicMessage("multiple records found for field"),
				dexerror.WithPublicData("field_name", fieldName),
				dexerror.WithPublicData("field_value", canonicalizedFieldValue),
			)
		}

		item, err := makeRecordItem(namespaceName, recordID, recordTimestamp, recordData)
		if err != nil {
			return nil, err
		}
		rv = &item
	}

	if rv == nil {
		return nil, dexerror.New(
			dexerror.WithErrorID("not_found.record"),
			dexerror.WithHTTPCode(http.StatusNotFound),
			dexerror.WithPublicMessage("no record found for field"),
			dexerror.WithPublicData("field_name", fieldName),
			dexerror.WithPublicData("field_value", canonicalizedFieldValue),
		)
	}

	return rv, nil
}

func (d *DB) CheckBatches(ctx context.Context, namespaceName string, batchNames []string) ([]dexapi.BatchStatus, error) {
	nsid, err := d.getNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	rows, err := d.db.QueryContext(ctx, `
		SELECT batch_data_hash
		FROM processed_batches
		WHERE namespace_id = $1 AND batch_data_hash = ANY($2)
	`, nsid, pq.Array(batchNames))
	if err != nil {
		return nil, err
	}

	seen := map[string]bool{}

	for rows.Next() {
		var batchName string
		if err := rows.Scan(&batchName); err != nil {
			return nil, err
		}
		seen[batchName] = true
	}

	var result []dexapi.BatchStatus
	for _, batchName := range batchNames {
		result = append(result, dexapi.BatchStatus{
			Namespace: namespaceName,
			BatchName: batchName,
			Processed: seen[batchName],
		})
	}

	return result, nil
}

func (d *DB) QueryFieldsList(ctx context.Context, namespace string, q *CompiledQuery, options ...RequestOption) (*dexapi.QueryFieldsResponse, error) {
	opts := requestOptions{}
	for _, o := range options {
		if err := o(&opts); err != nil {
			return nil, err
		}
	}

	nsid, err := d.getNamespaceID(namespace)
	if err != nil {
		return nil, err
	}

	qb, err := d.buildQuery(ctx, nsid, q)
	if err != nil {
		return nil, err
	}

	qb.addCustomJoin(qb.queryf(`
		INNER JOIN indexing_data AS d ON (d.namespace_id = %s AND d.record_id = records.record_id)
		INNER JOIN indexing_keys AS k ON (k.namespace_id = %s AND k.key_id = d.key_id)
	`, qb.getNamespaceArgName(), qb.getNamespaceArgName()))

	qb.selectClause = "k.key_name, COUNT(1)"
	qb.groupByClause = "k.key_name"
	qb.orderClause = "COUNT(1) DESC, k.key_name ASC"

	queryString, queryArgs, err := qb.buildQuery()
	if err != nil {
		return nil, err
	}

	rows, err := d.executeQueryRows(ctx, queryString, queryArgs, true)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resp dexapi.QueryFieldsResponse

	for rows.Next() {
		var fieldName string
		var count int

		if err := rows.Scan(&fieldName, &count); err != nil {
			return nil, err
		}

		fieldName = strings.TrimPrefix(fieldName, ".")

		fieldResp := dexapi.FieldResponse{
			Field:  fieldName,
			Count:  &count,
			Type:   "",
			Values: nil,
		}

		resp.Fields = append(resp.Fields, fieldResp)
	}

	return &resp, nil
}

func normalizeFieldName(s string) string {
	if !strings.HasPrefix(s, ".") {
		return "." + s
	}
	return s
}

func (d *DB) QueryValuesList(ctx context.Context, namespace string, q *CompiledQuery, fieldNames []string, options ...RequestOption) (*dexapi.QueryFieldsResponse, error) {
	opts := requestOptions{}
	for _, o := range options {
		if err := o(&opts); err != nil {
			return nil, err
		}
	}

	normFieldNames := make([]string, len(fieldNames))
	for i, fn := range fieldNames {
		normFieldNames[i] = normalizeFieldName(fn)
	}

	nsid, err := d.getNamespaceID(namespace)
	if err != nil {
		return nil, err
	}

	qb, err := d.buildQuery(ctx, nsid, q)
	if err != nil {
		return nil, err
	}

	nsArg := qb.addArg(nsid)

	fieldNamesArg := qb.addArg(pq.Array(normFieldNames))

	qb.surroundingQueryBefore = fmt.Sprintf(`
		SELECT k.key_name, d.value, COUNT(1)
		FROM indexing_data AS d
		INNER JOIN indexing_keys AS k ON (
				k.namespace_id = %s
			AND d.key_id = k.key_id
		)
		WHERE
			d.namespace_id = %s
		AND k.key_name = ANY(%s)
		AND d.record_id IN (
	`, nsArg, nsArg, fieldNamesArg)

	qb.surroundingQueryAfter = `
		)
		GROUP BY k.key_name, d.value
		ORDER BY k.key_name ASC, d.value ASC, COUNT(1) DESC
	`

	qb.selectClause = "records.record_id"
	qb.orderClause = ""

	queryString, queryArgs, err := qb.buildQuery()
	if err != nil {
		return nil, err
	}

	rows, err := d.executeQueryRows(ctx, queryString, queryArgs, true)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resp dexapi.QueryFieldsResponse

	var currentField *dexapi.FieldResponse

	for rows.Next() {
		var fieldName string
		var canonicalizedValue string
		var count int

		if err := rows.Scan(&fieldName, &canonicalizedValue, &count); err != nil {
			return nil, err
		}

		fieldName = strings.TrimPrefix(fieldName, ".")

		if currentField == nil || currentField.Field != fieldName {
			if currentField != nil {
				resp.Fields = append(resp.Fields, *currentField)
			}

			var initialCount int = 0

			currentField = &dexapi.FieldResponse{
				Field:  fieldName,
				Type:   "",
				Count:  &initialCount,
				Values: nil,
			}
		}

		var unmarshalled interface{}
		if err := json.Unmarshal([]byte(canonicalizedValue), &unmarshalled); err != nil {
			return nil, dexerror.New(
				dexerror.WithErrorID("internal_error.unmarshal"),
				dexerror.WithHTTPCode(500),
				dexerror.WithInternalMessage("error unmarshalling value from database"),
			)
		}

		currentField.Values = append(currentField.Values, dexapi.ValueResponse{
			Value: unmarshalled,
			Count: &count,
		})

		*currentField.Count = *currentField.Count + count
	}

	if currentField != nil {
		resp.Fields = append(resp.Fields, *currentField)
	}

	return &resp, nil
}
