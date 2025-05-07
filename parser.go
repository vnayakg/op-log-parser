package parser

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
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

func parseInsertOpLog(opLogJson string) (string, error) {
	var opLog OpLog
	err := json.Unmarshal([]byte(opLogJson), &opLog)
	if err != nil {
		return "", fmt.Errorf("Error unmarshaling oplog")
	}

	if opLog.Operation != "i" {
		return "", fmt.Errorf("oplog operation not supported")
	}

	schema, table, err := parseNamespace(opLog.Namespace)
	if err != nil {
		return "", err
	}

	statement, err := prepareInsertStatement(schema, table, opLog)
	if err != nil {
		return "", err
	}

	return statement, nil
}

func parseUpdateOpLog(opLogJson string) (string, error) {
	var updateLog OpLog
	if err := json.Unmarshal([]byte(opLogJson), &updateLog); err != nil {
		return "", err
	}

	diff := updateLog.Data["diff"].(map[string]any)
	schema, table, err := parseNamespace(updateLog.Namespace)
	if err != nil {
		return "", err
	}
	id := updateLog.O2.ID

	if u, ok := diff["u"]; ok {
		setFields := u.(map[string]any)
		var sets []string
		for field, value := range setFields {
			sets = append(sets, fmt.Sprintf("%s = %s", field, formatValue(value)))
		}
		return fmt.Sprintf("UPDATE %s.%s SET %s WHERE _id = '%s';",
			schema, table, strings.Join(sets, ", "), id), nil
	}

	if d, ok := diff["d"]; ok {
		unsetFields := d.(map[string]any)
		var sets []string
		for field := range unsetFields {
			sets = append(sets, fmt.Sprintf("%s = NULL", field))
		}
		return fmt.Sprintf("UPDATE %s.%s SET %s WHERE _id = '%s';",
			schema, table, strings.Join(sets, ", "), id), nil
	}

	return "", fmt.Errorf("unsupported update format")
}

func parseNamespace(namespace string) (schema, table string, err error) {
	parts := strings.Split(namespace, ".")
	if len(parts) != 2 {
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

func formatValue(v interface{}) string {
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
