package parsers

import (
	"encoding/json"
	"fmt"
	"op-log-parser/application/domain/models"
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
	fieldNull  = "NULL"
)

type OpLog struct {
	Operation string         `bson:"op" json:"op"`
	Namespace string         `bson:"ns" json:"ns"`
	Data      map[string]any `bson:"o" json:"o"`
	O2        *O2Field       `bson:"o2,omitempty" json:"o2,omitempty"`
}

type O2Field struct {
	ID string `bson:"_id" json:"_id"`
}

type Parser interface {
	Parse(oplogJson string) ([]string, error)
	ProcessOpLog(opLog models.OpLog) ([]string, error)
}

type opLogParser struct {
	ddlTracker     map[string]bool
	columnsTracker map[string]map[string]bool
	uuidGenerator  UUIDGenerator
}

type UUIDGenerator func() string

func NewParser(uuidGenerator UUIDGenerator) Parser {
	return &opLogParser{
		ddlTracker:     make(map[string]bool),
		columnsTracker: make(map[string]map[string]bool),
		uuidGenerator:  uuidGenerator,
	}
}

func (op *opLogParser) Parse(opLogJson string) ([]string, error) {
	var opLogs []models.OpLog
	if err := json.Unmarshal([]byte(opLogJson), &opLogs); err != nil {
		return nil, fmt.Errorf("Error unmarshaling oplog")
	}
	var statements []string

	for _, opLog := range opLogs {
		processedStatements, err := op.ProcessOpLog(opLog)
		if err != nil {
			return nil, err
		}
		statements = append(statements, processedStatements...)
	}
	return statements, nil
}

func (op *opLogParser) ProcessOpLog(opLog models.OpLog) ([]string, error) {
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

func (op *opLogParser) handleInsert(opLog models.OpLog) ([]string, error) {
	schema, table, err := parseNamespace(opLog.Namespace)
	if err != nil {
		return nil, err
	}

	var statements []string
	mainData, nestedData, arrayData := splitData(opLog.Data)

	if !op.isDDLGenerated(opLog.Namespace) {
		schemaStatement := fmt.Sprintf("CREATE SCHEMA %s;", schema)
		tableStatement, err := prepareTableDDL(schema, table, mainData)
		if err != nil {
			return nil, err
		}
		statements = append(statements, schemaStatement, tableStatement)

		for field, nestedObj := range nestedData {
			nestedTable := fmt.Sprintf("%s_%s", table, field)
			nestedStatements, err := op.generateTableDDLAndInsertForNestedObject(schema, nestedTable, opLog.Data[fieldID].(string), table, nestedObj)
			if err != nil {
				return nil, err
			}
			statements = append(statements, nestedStatements...)
		}

		for field, nestedArray := range arrayData {
			nestedTable := fmt.Sprintf("%s_%s", table, field)
			nestedStatements, err := op.generateTableDDLAndInsertForArray(schema, nestedTable, opLog.Data[fieldID].(string), table, nestedArray)
			if err != nil {
				return nil, err
			}
			statements = append(statements, nestedStatements...)
		}

		op.markDDLGenerated(opLog.Namespace)
		op.initializeColumnTracker(opLog.Namespace, mainData)
	} else {
		newFields := make(map[string]any)
		knownColumns := op.getKnownColumns(opLog.Namespace)
		for col, value := range mainData {
			if !knownColumns[col] {
				newFields[col] = value
			}
		}

		if len(newFields) > 0 {
			alterStatement, err := prepareAlterStatement(schema, table, newFields)
			if err != nil {
				return nil, err
			}
			statements = append(statements, alterStatement)
			op.updateColumnsTracker(opLog.Namespace, newFields)
		}
		for field, nestedObj := range nestedData {
			nestedTable := fmt.Sprintf("%s_%s", table, field)
			nestedStatements, err := op.generateTableDDLAndInsertForNestedObject(schema, nestedTable, opLog.Data[fieldID].(string), table, nestedObj)
			if err != nil {
				return nil, err
			}
			statements = append(statements, nestedStatements...)
		}
		for field, nestedArray := range arrayData {
			nestedTable := fmt.Sprintf("%s_%s", table, field)
			nestedStatements, err := op.generateTableDDLAndInsertForArray(schema, nestedTable, opLog.Data[fieldID].(string), table, nestedArray)
			if err != nil {
				return nil, err
			}
			statements = append(statements, nestedStatements...)
		}
	}
	knownColumns := op.getKnownColumns(opLog.Namespace)
	insertStatement, err := prepareInsertStatement(schema, table, opLog.Data, knownColumns)
	if err != nil {
		return nil, err
	}
	statements = append(statements, insertStatement)
	return statements, nil
}

func (op *opLogParser) handleUpdate(opLog models.OpLog) ([]string, error) {
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
			sets = append(sets, fmt.Sprintf("%s = %s", field, fieldNull))
		}
		if len(sets) > 0 {
			sort.Strings(sets)
			setClauses = append(setClauses, sets...)
		}

	}
	return []string{fmt.Sprintf("UPDATE %s.%s SET %s WHERE _id = '%s';",
		schema, table, strings.Join(setClauses, ", "), opLog.O2.ID)}, nil
}

func (op *opLogParser) handleDelete(opLog models.OpLog) ([]string, error) {
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

func splitData(data map[string]any) (main, nested map[string]any, arrays map[string][]any) {
	main = make(map[string]any)
	nested = make(map[string]any)
	arrays = make(map[string][]any)

	for key, value := range data {
		switch val := value.(type) {
		case map[string]any:
			nested[key] = val
		case []any:
			if len(val) > 0 {
				if _, ok := val[0].(map[string]any); ok {
					arrays[key] = val
				} else {
					main[key] = val
				}
			}
		default:
			main[key] = val
		}
	}
	return main, nested, arrays
}

func (op *opLogParser) generateTableDDLAndInsertForArray(schema, table, parentID, parentTable string, arrayData []any) ([]string, error) {
	var statements []string
	for _, item := range arrayData {
		statement, err := op.generateTableDDLAndInsertForNestedObject(schema, table, parentID, parentTable, item)
		if err != nil {
			return nil, err
		}
		statements = append(statements, statement...)
	}
	return statements, nil
}

func (op *opLogParser) generateTableDDLAndInsertForNestedObject(schema, table, parentID, parentTable string, data any) ([]string, error) {
	var statements []string

	nestedData, ok := data.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map[string]any for %s, got %T", table, data)
	}

	nestedData[fieldID] = op.uuidGenerator()
	nestedData[fmt.Sprintf("%s_%s", parentTable, fieldID)] = parentID
	tableSchemaName := fmt.Sprintf("%s.%s", schema, table)

	if !op.isDDLGenerated(tableSchemaName) {
		tableStatement, err := prepareNestedTableDDL(schema, table, nestedData, parentTable, parentID)
		if err != nil {
			return nil, err
		}

		op.markDDLGenerated(tableSchemaName)
		op.initializeColumnTracker(tableSchemaName, nestedData)
		statements = append(statements, tableStatement)
	}

	knownColumns := op.getKnownColumns(tableSchemaName)
	insertStmt, err := prepareInsertStatement(schema, table, nestedData, knownColumns)
	if err != nil {
		return nil, err
	}
	statements = append(statements, insertStmt)

	return statements, nil
}

func prepareAlterStatement(schema, table string, newFields map[string]any) (string, error) {
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

func prepareInsertStatement(schema, table string, data map[string]any, knownColumns map[string]bool) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("empty data field for insert")
	}
	values := []string{}
	var columns []string
	for col := range knownColumns {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	for _, col := range columns {
		value, ok := data[col]
		if !ok {
			values = append(values, fieldNull)
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

func prepareNestedTableDDL(schema, table string, data map[string]any, parentTable, referenceTableId string) (tableStatement string, err error) {
	var columns []string
	for colName := range data {
		columns = append(columns, colName)
	}
	sort.Strings(columns)

	var tableFields []string
	for _, colName := range columns {
		var value any
		if colName == fmt.Sprintf("%s__id", parentTable) {
			value = referenceTableId
		} else {
			value = data[colName]
		}
		sqlType, err := getSqlType(colName, value)
		if err != nil {
			return "", err
		}
		tableFields = append(tableFields, fmt.Sprintf("%s %s", colName, sqlType))
	}
	tableStatement = fmt.Sprintf("CREATE TABLE %s.%s (%s);", schema, table, strings.Join(tableFields, ", "))

	return tableStatement, nil
}

func prepareTableDDL(schema, table string, data map[string]any) (tableStatement string, err error) {
	var columns []string
	for colName := range data {
		columns = append(columns, colName)
	}
	sort.Strings(columns)

	var tableFields []string
	for _, colName := range columns {
		value := data[colName]
		sqlType, err := getSqlType(colName, value)
		if err != nil {
			return "", err
		}
		tableFields = append(tableFields, fmt.Sprintf("%s %s", colName, sqlType))
	}
	tableStatement = fmt.Sprintf("CREATE TABLE %s.%s (%s);", schema, table, strings.Join(tableFields, ", "))

	return tableStatement, nil
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
	case float64, int64, int32, int16, int8:
		return "FLOAT", nil
	default:
		return "", fmt.Errorf("error converting: %v to sql type for field %v, type: %T", value, fieldName, value)
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
