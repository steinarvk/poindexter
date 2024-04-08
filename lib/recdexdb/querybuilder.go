package recdexdb

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type queryBuilder struct {
	namespaceId int

	selectClause string
	orderClause  string
	limit        int

	joinClauses    []string
	joinClauseArgs []interface{}

	whereClauses    []string
	whereClauseArgs []interface{}

	keyNameToKeyTableAlias  map[string]string
	keyNameToDataTableAlias map[string]string
}

func newQueryBuilder(nsid int) *queryBuilder {
	return &queryBuilder{
		namespaceId:             nsid,
		selectClause:            `DISTINCT records.record_id, records.record_timestamp, record_data`,
		orderClause:             `records.record_timestamp ASC`,
		limit:                   1000,
		keyNameToKeyTableAlias:  make(map[string]string),
		keyNameToDataTableAlias: make(map[string]string),
	}
}

func (qb *queryBuilder) addUUIDFilter(uuids []uuid.UUID) {
	if len(uuids) == 0 {
		return
	}

	qb.whereClauses = append(qb.whereClauses, "records.uuid IN (?)")
	qb.whereClauseArgs = append(qb.whereClauseArgs, uuids)
}

func (qb *queryBuilder) addTimestampFilter(t0, t1 *time.Time) {
	if t0 != nil {
		qb.whereClauses = append(qb.whereClauses, "records.timestamp >= ?")
		qb.whereClauseArgs = append(qb.whereClauseArgs, *t0)
	}

	if t1 != nil {
		qb.whereClauses = append(qb.whereClauses, "records.timestamp <= ?")
		qb.whereClauseArgs = append(qb.whereClauseArgs, *t1)
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

	qb.joinClauses = append(qb.joinClauses, fmt.Sprintf(
		"INNER JOIN indexing_keys AS %s ON (%s.namespace_id = ? AND %s.key_name = ?)", alias, alias, alias,
	))
	qb.joinClauseArgs = append(qb.joinClauseArgs, qb.namespaceId, keyName)

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
		"INNER JOIN indexing_data AS %s ON (%s.namespace_id = ? AND records.record_id = %s.record_id AND %s.key_id = %s.key_id)", alias, alias, alias, keyNameAlias, alias,
	))
	qb.joinClauseArgs = append(qb.joinClauseArgs, qb.namespaceId)

	return alias, nil
}

func (qb *queryBuilder) addFieldPresent(keyName string) error {
	_, err := qb.addJoinForKeyValue(keyName)
	if err != nil {
		return err
	}

	return err
}

func (qb *queryBuilder) addFieldHasValue(keyName, value string) error {
	alias, err := qb.addJoinForKeyValue(keyName)
	if err != nil {
		return err
	}

	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s.value = ?", alias))
	qb.whereClauseArgs = append(qb.whereClauseArgs, value)

	return nil
}

func (qb *queryBuilder) buildQuery() (string, []interface{}, error) {
	query := `SELECT ` + qb.selectClause
	query += "\nFROM records"

	var args []interface{}

	for _, joinClause := range qb.joinClauses {
		query += "\n" + joinClause
	}
	args = append(args, qb.joinClauseArgs...)

	query += "\nWHERE records.namespace_id = ?"

	for _, whereClause := range qb.whereClauses {
		query += "\nAND   " + whereClause + " "
	}
	args = append(args, qb.whereClauseArgs...)

	query += "\n" + `ORDER BY ` + qb.orderClause

	if qb.limit > 0 {
		query += "\nLIMIT ?"
		args = append(args, qb.limit)
	}

	return query, args, nil
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
