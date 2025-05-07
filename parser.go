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
		switch v := value.(type) {
		case string:
			values = append(values, fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")))
		case bool:
			values = append(values, fmt.Sprintf("%t", v))
		case float64:
			values = append(values, fmt.Sprintf("%v", v))
		default:
			return "", fmt.Errorf("unsupported data type for key %s: %T", colName, v)
		}
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
