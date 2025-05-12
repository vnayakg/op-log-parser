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

type Parser interface {
	Parse(oplogJson string) ([]string, error)
}

type opLogParser struct {
	ddlTracker     map[string]bool
	columnsTracker map[string]map[string]bool
}

func CreateParser() Parser {
	return &opLogParser{
		ddlTracker:     make(map[string]bool),
		columnsTracker: make(map[string]map[string]bool),
	}
}

func (op *opLogParser) Parse(opLogJson string) ([]string, error) {
	var opLogs []OpLog
	if err := json.Unmarshal([]byte(opLogJson), &opLogs); err != nil {
		return nil, fmt.Errorf("Error unmarshaling oplog")
	}

	var statements []string
	for _, opLog := range opLogs {

		processedStatements, err := op.processOpLog(opLog)
		if err != nil {
			return nil, err
		}
		statements = append(statements, processedStatements...)
	}

	return statements, nil
}

func (op *opLogParser) processOpLog(opLog OpLog) ([]string, error) {
	switch opLog.Operation {
	case Insert:
		return op.handleInsert(opLog)
	case Update:
		return op.handleUpdate(opLog)
	case Delete:
		return op.handleDelete(opLog)
	default:
		return nil, fmt.Errorf("unsupported oplog operation: %s", opLog.Operation)
	}
}

func (op *opLogParser) handleInsert(opLog OpLog) ([]string, error) {
	schema, table, err := parseNamespace(opLog.Namespace)
	if err != nil {
		return nil, err
	}

	var statements []string

	if !op.isDDLGenerated(opLog.Namespace) {
		schemaStatement, tableStatement, err := prepareTableDDL(opLog)
		if err != nil {
			return nil, err
		}
		statements = append(statements, schemaStatement, tableStatement)

		op.markDDLGenerated(opLog.Namespace)
		op.initializeColumnTracker(opLog.Namespace, opLog.Data)
	} else {
		newFields := make(map[string]any)
		knownColumns := op.getKnownColumns(opLog.Namespace)
		for col, value := range opLog.Data {
			if !knownColumns[col] {
				newFields[col] = value
			}
		}

		if len(newFields) > 0 {
			alterStatement, err := prepareAlterStatement(schema, table, opLog, newFields)
			if err != nil {
				return nil, err
			}
			statements = append(statements, alterStatement)
			op.updateColumnsTracker(opLog.Namespace, newFields)
		}
	}
	knownColumns := op.getKnownColumns(opLog.Namespace)
	insertStatement, err := parseInsertOpLog(schema, table, opLog, knownColumns)
	if err != nil {
		return nil, err
	}
	statements = append(statements, insertStatement)
	return statements, nil
}

func (op *opLogParser) handleUpdate(opLog OpLog) ([]string, error) {
	if opLog.O2 == nil || opLog.O2.ID == "" {
		return nil, fmt.Errorf("_id field is missing")
	}

	diff, ok := opLog.Data[fieldDiff].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid diff field in update oplog")
	}

	schema, table, err := parseNamespace(opLog.Namespace)
	if err != nil {
		return nil, err
	}

	var setClauses []string
	if setFields, ok := diff[fieldSet].(map[string]any); ok {
		var sets []string
		for field, value := range setFields {
			sets = append(sets, fmt.Sprintf("%s = %s", field, formatValue(value)))
		}
		if len(sets) > 0 {
			sort.Strings(sets)
			setClauses = append(setClauses, sets...)
		}
	}

	if unsetFields, ok := diff[fieldUnset].(map[string]any); ok {
		var sets []string
		for field := range unsetFields {
			sets = append(sets, fmt.Sprintf("%s = NULL", field))
		}
		if len(sets) > 0 {
			sort.Strings(sets)
			setClauses = append(setClauses, sets...)
		}

	}
	return []string{fmt.Sprintf("UPDATE %s.%s SET %s WHERE _id = '%s';",
		schema, table, strings.Join(setClauses, ", "), opLog.O2.ID)}, nil
}

func (op *opLogParser) handleDelete(opLog OpLog) ([]string, error) {
	id, ok := opLog.Data[fieldID]
	if !ok {
		return nil, fmt.Errorf("_id field is missing")
	}
	schema, table, err := parseNamespace(opLog.Namespace)
	if err != nil {
		return nil, err
	}
	return []string{fmt.Sprintf("DELETE FROM %s.%s WHERE _id = %s;", schema, table, formatValue(id))}, nil
}

func (op *opLogParser) getKnownColumns(namespace string) map[string]bool {
	if columns, exists := op.columnsTracker[namespace]; exists {
		return columns
	}
	return make(map[string]bool)
}

func (op *opLogParser) isDDLGenerated(namespace string) bool {
	return op.ddlTracker[namespace]
}

func (op *opLogParser) markDDLGenerated(namespace string) {
	op.ddlTracker[namespace] = true
}

func (op *opLogParser) initializeColumnTracker(namespace string, data map[string]any) {
	op.columnsTracker[namespace] = make(map[string]bool)
	for col, _ := range data {
		op.columnsTracker[namespace][col] = true
	}
}

func (op *opLogParser) updateColumnsTracker(namespace string, newFields map[string]any) {
	if columns, exists := op.columnsTracker[namespace]; exists {
		for field := range newFields {
			columns[field] = true
		}
	}
}

func prepareAlterStatement(schema, table string, opLog OpLog, newFields map[string]any) (string, error) {
	var fields []string
	for field := range newFields {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	var columnDefinitions []string
	for _, col := range fields {
		sqlType, err := getSqlType(col, newFields[col])
		if err != nil {
			return "", err
		}

		columnDefinitions = append(columnDefinitions, fmt.Sprintf("%s %s", col, sqlType))
	}

	return fmt.Sprintf("ALTER TABLE %s.%s ADD %s;", schema, table, strings.Join(columnDefinitions, ", ")), nil
}

func parseInsertOpLog(schema, table string, opLog OpLog, knownColumns map[string]bool) (string, error) {
	if len(opLog.Data) == 0 {
		return "", fmt.Errorf("empty data field for insert")
	}
	values := []string{}
	var columns []string
	for col := range knownColumns {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	for _, col := range columns {
		value, ok := opLog.Data[col]
		if !ok {
			values = append(values, "NULL")
		} else {
			values = append(values, formatValue(value))
		}
	}

	statement := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES (%s);",
		schema,
		table,
		strings.Join(columns, ", "),
		strings.Join(values, ", "),
	)
	return statement, nil
}

func parseNamespace(namespace string) (schema, table string, err error) {
	parts := strings.Split(namespace, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("error parsing namespace, invalid namespace")
	}
	return parts[0], parts[1], nil
}

func prepareTableDDL(opLog OpLog) (schemaStatement, tableStatement string, err error) {
	schema, table, err := parseNamespace(opLog.Namespace)
	if err != nil {
		return "", "", err
	}
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
			return "", "", err
		}
		tableFields = append(tableFields, fmt.Sprintf("%s %s", colName, sqlType))
	}
	tableStatement = fmt.Sprintf("CREATE TABLE %s.%s (%s);", schema, table, strings.Join(tableFields, ", "))
	schemaStatement = fmt.Sprintf("CREATE SCHEMA %s;", schema)

	return schemaStatement, tableStatement, nil
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
