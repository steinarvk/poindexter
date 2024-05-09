package poindexterdb

import (
	"fmt"
	"time"

	"github.com/gibson042/canonicaljson-go"
	"github.com/google/uuid"
	"github.com/lib/pq"
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

func (qb *queryBuilder) addJoinForKeyName(keyName string) (string, error) {
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

func (qb *queryBuilder) addJoinForKeyValue(keyName string) (string, error) {
	keyNameAlias, err := qb.addJoinForKeyName(keyName)
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

func (qb *queryBuilder) addFieldPresent(keyName string) error {
	_, err := qb.addJoinForKeyValue(keyName)
	if err != nil {
		return err
	}

	return err
}

func (qb *queryBuilder) addFieldPresentAndNotNull(keyName string) error {
	alias, err := qb.addJoinForKeyValue(keyName)
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

func (qb *queryBuilder) addFieldHasValue(keyName string, value interface{}) error {
	alias, err := qb.addJoinForKeyValue(keyName)
	if err != nil {
		return err
	}

	serializedValue, err := canonicaljson.Marshal(value)
	if err != nil {
		return err
	}
	serializedString := string(serializedValue)

	argname := qb.addArg(serializedString)

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

	query += "\n" + `ORDER BY ` + qb.orderClause

	if qb.limit > 0 {
		query += "\nLIMIT " + qb.addArg(qb.limit)
	}

	return query, qb.args, nil
}

func BuildTestQuery() (string, []interface{}, error) {
	qb := newQueryBuilder(1)

	if err := qb.addFieldPresent(".artifact"); err != nil {
		return "", nil, err
	}

	if err := qb.addFieldHasValue(".artifact.ok", "true"); err != nil {
		return "", nil, err
	}

	return qb.buildQuery()
}
