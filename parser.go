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

func Parse(opLogJson string) (string, error) {
	var opLog OpLog
	if err := json.Unmarshal([]byte(opLogJson), &opLog); err != nil {
		return "", fmt.Errorf("Error unmarshaling oplog")
	}

	switch opLog.Operation {
	case Insert:
		return parseInsertOpLog(opLog)
	case Update:
		return parseUpdateOpLog(opLog)
	case Delete:
		return parseDeleteOpLog(opLog)
	default:
		return "", fmt.Errorf("oplog operation not supported: received operation %s", opLog.Operation)
	}
}

func parseInsertOpLog(opLog OpLog) (string, error) {
	schema, table, err := parseNamespace(opLog.Namespace)
	if err != nil {
		return "", err
	}

	if len(opLog.Data) == 0 {
		return "", fmt.Errorf("empty data field for insert")
	}

	statement, err := prepareInsertStatement(schema, table, opLog)
	if err != nil {
		return "", err
	}

	return statement, nil
}

func parseUpdateOpLog(opLog OpLog) (string, error) {
	diff := opLog.Data["diff"].(map[string]any)
	schema, table, err := parseNamespace(opLog.Namespace)
	if err != nil {
		return "", err
	}
	id := opLog.O2.ID
	if u, ok := diff["u"]; ok {
		setFields := u.(map[string]any)
		var sets []string
		for field, value := range setFields {
			sets = append(sets, fmt.Sprintf("%s = %s", field, formatValue(value)))
		}
		sort.Strings(sets)
		return fmt.Sprintf("UPDATE %s.%s SET %s WHERE _id = '%s';",
			schema, table, strings.Join(sets, ", "), id), nil
	}

	if d, ok := diff["d"]; ok {
		unsetFields := d.(map[string]any)
		var sets []string
		for field := range unsetFields {
			sets = append(sets, fmt.Sprintf("%s = NULL", field))
		}
		sort.Strings(sets)
		return fmt.Sprintf("UPDATE %s.%s SET %s WHERE _id = '%s';",
			schema, table, strings.Join(sets, ", "), id), nil
	}

	return "", fmt.Errorf("unsupported update format")
}

func parseDeleteOpLog(opLog OpLog) (string, error) {
	id, ok := opLog.Data["_id"]
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
