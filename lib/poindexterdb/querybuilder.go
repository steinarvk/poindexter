package poindexterdb

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/steinarvk/poindexter/lib/dexapi"
	"go.uber.org/zap"
)

type queryBuilder struct {
	namespaceId Namespace

	selectClause string
	orderClause  string
	limit        int

	joinClauses []string

	whereClauses []string

	args []interface{}

	namespaceArgName string

	keyNameToKeyTableAlias  map[string]string
	keyNameToDataTableAlias map[string]string

	surroundingQueryBefore string
	surroundingQueryAfter  string
}

func (qb *queryBuilder) addArg(value interface{}) string {
	n := len(qb.args)
	name := fmt.Sprintf("$%d", (n + 1))
	qb.args = append(qb.args, value)
	return name
}

func newQueryBuilder(nsid Namespace) *queryBuilder {
	qb := &queryBuilder{
		namespaceId:             nsid,
		selectClause:            `DISTINCT records.record_id, records.record_timestamp, record_data`,
		orderClause:             `records.record_timestamp ASC`,
		limit:                   1000,
		keyNameToKeyTableAlias:  make(map[string]string),
		keyNameToDataTableAlias: make(map[string]string),
	}
	qb.namespaceArgName = qb.addArg(qb.namespaceId)
	return qb
}

func (qb *queryBuilder) addUUIDFilter(uuids []uuid.UUID) {
	if len(uuids) == 0 {
		return
	}

	qb.whereClauses = append(qb.whereClauses,
		fmt.Sprintf(
			"records.uuid IN ANY(%s)",
			qb.addArg(pq.Array(uuids)),
		))
}

func (qb *queryBuilder) addTimestampFilter(t0, t1 *time.Time) {
	if t0 != nil {
		qb.whereClauses = append(qb.whereClauses,
			fmt.Sprintf(
				"records.timestamp >= %s",
				qb.addArg(*t0),
			))
	}

	if t1 != nil {
		qb.whereClauses = append(qb.whereClauses,
			fmt.Sprintf(
				"records.timestamp <= %s",
				qb.addArg(*t1),
			))
	}
}

func (qb *queryBuilder) addInnerJoinForKeyName(keyName string) (string, error) {
	alias, ok := qb.keyNameToKeyTableAlias[keyName]
	if ok {
		return alias, nil
	}

	n := len(qb.keyNameToKeyTableAlias) + 1
	alias = fmt.Sprintf("k%d", n)
	qb.keyNameToKeyTableAlias[keyName] = alias

	argName := qb.addArg(keyName)

	qb.joinClauses = append(qb.joinClauses, fmt.Sprintf(
		"INNER JOIN indexing_keys AS %s ON (%s.namespace_id = %s AND %s.key_name = %s)",
		alias,
		alias,
		qb.namespaceArgName,
		alias, argName,
	))

	return alias, nil
}

func (qb *queryBuilder) addInnerJoinForKeyValue(keyName string) (string, error) {
	keyNameAlias, err := qb.addInnerJoinForKeyName(keyName)
	if err != nil {
		return "", err
	}

	alias, ok := qb.keyNameToDataTableAlias[keyName]
	if ok {
		return alias, nil
	}

	n := len(qb.keyNameToDataTableAlias) + 1
	alias = fmt.Sprintf("d%d", n)
	qb.keyNameToDataTableAlias[keyName] = alias

	// INNER JOIN indexing_data AS t2 ON (namespace.namespace_id = ? AND t1.key_id = t2.key_id)
	qb.joinClauses = append(qb.joinClauses, fmt.Sprintf(
		"INNER JOIN indexing_data AS %s "+
			"ON (%s.namespace_id = %s AND "+
			"records.record_id = %s.record_id AND "+
			"%s.key_id = %s.key_id)",
		alias,
		alias,
		qb.namespaceArgName,
		alias, keyNameAlias, alias,
	))

	return alias, nil
}

func (qb *queryBuilder) addOuterJoinForKeyName(keyName string) (string, error) {
	alias, ok := qb.keyNameToKeyTableAlias[keyName]
	if ok {
		return alias, nil
	}

	n := len(qb.keyNameToKeyTableAlias) + 1
	alias = fmt.Sprintf("k%d", n)
	qb.keyNameToKeyTableAlias[keyName] = alias

	argName := qb.addArg(keyName)

	qb.joinClauses = append(qb.joinClauses, fmt.Sprintf(
		"LEFT OUTER JOIN indexing_keys AS %s ON (%s.namespace_id = %s AND %s.key_name = %s)",
		alias,
		alias,
		qb.namespaceArgName,
		alias, argName,
	))

	return alias, nil
}

func (qb *queryBuilder) addOuterJoinForKeyValue(keyName string) (string, error) {
	keyNameAlias, err := qb.addOuterJoinForKeyName(keyName)
	if err != nil {
		return "", err
	}

	alias, ok := qb.keyNameToDataTableAlias[keyName]
	if ok {
		return alias, nil
	}

	n := len(qb.keyNameToDataTableAlias) + 1
	alias = fmt.Sprintf("d%d", n)
	qb.keyNameToDataTableAlias[keyName] = alias

	qb.joinClauses = append(qb.joinClauses, fmt.Sprintf(
		"LEFT OUTER JOIN indexing_data AS %s "+
			"ON (%s.namespace_id = %s AND "+
			"records.record_id = %s.record_id AND "+
			"%s.key_id = %s.key_id)",
		alias,
		alias,
		qb.namespaceArgName,
		alias, keyNameAlias, alias,
	))

	return alias, nil
}

func (qb *queryBuilder) addFieldPresent(keyName string) error {
	_, err := qb.addInnerJoinForKeyValue(keyName)
	if err != nil {
		return err
	}

	return err
}

func (qb *queryBuilder) addFieldPresentAndNotNull(keyName string) error {
	alias, err := qb.addInnerJoinForKeyValue(keyName)
	if err != nil {
		return err
	}

	// Note: subtlety.
	// Database NULL is fine (denotes a map field).
	// Literal string 'null' is not fine; generally just a way to represent field absence.

	qb.whereClauses = append(qb.whereClauses,
		fmt.Sprintf("(%s.value IS NULL OR %s.value != 'null')", alias, alias))

	return err
}

func (qb *queryBuilder) addFieldHasValue(keyName string, canonicalizedValue string) error {
	alias, err := qb.addInnerJoinForKeyValue(keyName)
	if err != nil {
		return err
	}

	argname := qb.addArg(canonicalizedValue)

	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s.value = %s", alias, argname))

	return nil
}

func (qb *queryBuilder) buildQuery() (string, []interface{}, error) {
	query := `SELECT ` + qb.selectClause
	query += "\nFROM records"

	for _, joinClause := range qb.joinClauses {
		query += "\n" + joinClause
	}

	query += "\nWHERE records.namespace_id = " + qb.namespaceArgName

	for _, whereClause := range qb.whereClauses {
		query += "\nAND   " + whereClause + " "
	}

	if qb.orderClause != "" {
		query += "\n" + `ORDER BY ` + qb.orderClause
	}

	if qb.limit > 0 {
		query += "\nLIMIT " + qb.addArg(qb.limit)
	}

	if qb.surroundingQueryBefore != "" || qb.surroundingQueryAfter != "" {
		query = qb.surroundingQueryBefore + "\n" + query + "\n" + qb.surroundingQueryAfter
	}

	return query, qb.args, nil
}

func (qb *queryBuilder) addNegatedFieldPresentAndNotNull(keyName string) error {
	alias, err := qb.addOuterJoinForKeyValue(keyName)
	if err != nil {
		return err
	}

	qb.whereClauses = append(qb.whereClauses,
		fmt.Sprintf("NOT (%s.value IS NULL OR %s.value != 'null')", alias, alias))

	return err
}

func (qb *queryBuilder) addNegatedFieldPresent(keyName string) error {
	alias, err := qb.addOuterJoinForKeyName(keyName)
	if err != nil {
		return err
	}

	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s.key_name IS NULL", alias))

	return err
}

func (qb *queryBuilder) addNegatedFieldHasValue(keyName string, canonicalizedValue string) error {
	alias, err := qb.addOuterJoinForKeyValue(keyName)
	if err != nil {
		return err
	}

	argname := qb.addArg(canonicalizedValue)

	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("(%s.value IS NULL OR %s.value != %s)", alias, alias, argname))

	return nil
}

func (qb *queryBuilder) setOrderBy(orderBy dexapi.OrderBy) error {
	var prefix string
	suffix := "ASC"

	zap.L().Sugar().Infof("setting orderby: %+v", orderBy)

	if orderBy.Descending {
		suffix = "DESC"
	}

	switch orderBy.Variant {
	case dexapi.OrderByTime:
		prefix = "records.record_timestamp"
	case dexapi.OrderByID:
		prefix = "records.record_id"
	case dexapi.OrderByRandom:
		prefix = "RANDOM()"
	default:
		return fmt.Errorf("unknown order-by variant: %s", orderBy.Variant)
	}

	qb.orderClause = prefix + " " + suffix

	return nil
}

func (qb *queryBuilder) setOmitHidden() error {
	return qb.addNegatedFieldHasValue("._deletion_marker", "true")
}

func (qb *queryBuilder) setOmitSuperseded() error {
	qb.joinClauses = append(qb.joinClauses,
		"LEFT OUTER JOIN records AS sup_records "+
			"ON ( "+
			"(records.namespace_id = sup_records.namespace_id) "+
			"AND "+
			"(records.record_id = sup_records.record_supersedes_id) "+
			")",
	)

	qb.whereClauses = append(qb.whereClauses,
		"sup_records.record_id IS NULL",
	)

	return nil
}

func (qb *queryBuilder) setTimestampStartFilterInclusive(t time.Time) error {
	arg := qb.addArg(t)

	qb.whereClauses = append(qb.whereClauses,
		"records.record_timestamp >= "+arg,
	)

	return nil
}

func (qb *queryBuilder) setTimestampEndFilterExclusive(t time.Time) error {
	arg := qb.addArg(t)

	qb.whereClauses = append(qb.whereClauses,
		"records.record_timestamp < "+arg,
	)

	return nil
}

func (qb *queryBuilder) addSurroundingQuery(before, after string) {
	qb.surroundingQueryBefore = before
	qb.surroundingQueryAfter = after
}
