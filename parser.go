package parser

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	Insert = "i"
	Update = "u"
	Delete = "d"

	fieldID    = "_id"
	fieldDiff  = "diff"
	fieldSet   = "u"
	fieldUnset = "d"
)

type OpLog struct {
	Operation string         `json:"op"`
	Namespace string         `json:"ns"`
	Data      map[string]any `json:"o"`
	O2        *O2Field       `json:"o2,omitempty"`
}

type O2Field struct {
	ID string `json:"_id"`
}

func Parse(opLogJson string) ([]string, error) {
	var opLog OpLog
	if err := json.Unmarshal([]byte(opLogJson), &opLog); err != nil {
		return nil, fmt.Errorf("Error unmarshaling oplog")
	}

	switch opLog.Operation {
	case Insert:
		return parseInsertOpLog(opLog)
	case Update:
		statement, err := parseUpdateOpLog(opLog)
		if err != nil {
			return nil, err
		}
		return []string{statement}, nil
	case Delete:
		statement, err := parseDeleteOpLog(opLog)
		if err != nil {
			return nil, err
		}
		return []string{statement}, nil
	default:
		return nil, fmt.Errorf("oplog operation not supported: received operation %s", opLog.Operation)
	}
}

func parseInsertOpLog(opLog OpLog) ([]string, error) {
	schema, table, err := parseNamespace(opLog.Namespace)
	if err != nil {
		return nil, err
	}
	if len(opLog.Data) == 0 {
		return nil, fmt.Errorf("empty data field for insert")
	}
	var statements []string
	schemaStatement := fmt.Sprintf("CREATE SCHEMA %s", schema)
	tableStatement, err := prepareTableStatement(schema, table, opLog)
	if err != nil {
		return nil, err
	}
	insertStatement, err := prepareInsertStatement(schema, table, opLog)
	if err != nil {
		return nil, err
	}
	statements = append(statements, schemaStatement, tableStatement, insertStatement)
	return statements, nil
}

func parseUpdateOpLog(opLog OpLog) (string, error) {
	diff := opLog.Data[fieldDiff].(map[string]any)
	schema, table, err := parseNamespace(opLog.Namespace)
	if err != nil {
		return "", err
	}
	if opLog.O2 == nil || opLog.O2.ID == "" {
		return "", fmt.Errorf("_id field is missing")
	}
	var setClauses []string

	id := opLog.O2.ID
	if u, ok := diff[fieldSet]; ok {
		setFields := u.(map[string]any)
		var sets []string
		for field, value := range setFields {
			sets = append(sets, fmt.Sprintf("%s = %s", field, formatValue(value)))
		}
		if len(sets) > 0 {
			sort.Strings(sets)
			setClauses = append(setClauses, sets...)
		}
	}

	if d, ok := diff[fieldUnset]; ok {
		unsetFields := d.(map[string]any)
		var sets []string
		for field := range unsetFields {
			sets = append(sets, fmt.Sprintf("%s = NULL", field))
		}
		if len(sets) > 0 {
			sort.Strings(sets)
			setClauses = append(setClauses, sets...)
		}

	}
	return fmt.Sprintf("UPDATE %s.%s SET %s WHERE _id = '%s';",
		schema, table, strings.Join(setClauses, ", "), id), nil
}

func parseDeleteOpLog(opLog OpLog) (string, error) {
	id, ok := opLog.Data[fieldID]
	if !ok {
		return "", fmt.Errorf("_id field is missing")
	}
	schema, table, err := parseNamespace(opLog.Namespace)
	if err != nil {
		return "", err
	}
	return prepareDeleteStatement(schema, table, formatValue(id)), nil
}

func prepareDeleteStatement(schema, table, id string) string {
	return fmt.Sprintf("DELETE FROM %s.%s WHERE _id = %s;", schema, table, id)
}

func parseNamespace(namespace string) (schema, table string, err error) {
	parts := strings.Split(namespace, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("error parsing namespace, invalid namespace")
	}
	return parts[0], parts[1], nil
}

func prepareTableStatement(schema, table string, opLog OpLog) (string, error) {
	var columns []string
	for colName := range opLog.Data {
		columns = append(columns, colName)
	}
	sort.Strings(columns)

	var tableFields []string
	for _, colName := range columns {
		value := opLog.Data[colName]
		sqlType, err := getSqlType(colName, value)
		if err != nil {
			return "", err
		}
		tableFields = append(tableFields, fmt.Sprintf("%s %s", colName, sqlType))
	}
	return fmt.Sprintf("CREATE TABLE %s.%s (%s);", schema, table, strings.Join(tableFields, ", ")), nil
}

func prepareInsertStatement(schema, table string, opLog OpLog) (string, error) {
	columns := []string{}
	values := []string{}

	for colName := range opLog.Data {
		columns = append(columns, colName)
	}
	sort.Strings(columns)
	for _, colName := range columns {
		value := opLog.Data[colName]
		values = append(values, formatValue(value))
	}

	sqlStatement := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES (%s);",
		schema,
		table,
		strings.Join(columns, ", "),
		strings.Join(values, ", "),
	)
	return sqlStatement, nil
}

func getSqlType(fieldName string, value any) (string, error) {
	if fieldName == fieldID {
		return "VARCHAR(255) PRIMARY KEY", nil
	}

	switch value.(type) {
	case string:
		return "VARCHAR(255)", nil
	case bool:
		return "BOOLEAN", nil
	case float64, int64:
		return "FLOAT", nil
	default:
		return "", fmt.Errorf("error converting: %v to sql type", value)
	}
}

func formatValue(v any) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", val)
	case bool:
		return fmt.Sprintf("%t", val)
	case float64:
		if val == float64(int(val)) {
			return fmt.Sprintf("%d", int(val))
		}
		return fmt.Sprintf("%f", val)
	default:
		return fmt.Sprintf("'%v'", val)
	}
}
