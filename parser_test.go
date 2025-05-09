package parser

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	testCases := []struct {
		name        string
		inputJSON   string
		expectedSQL string
		expectedErr error
	}{
		{
			name: "Insert: Valid student data",
			inputJSON: `{
                "op": "i",
                "ns": "test.student",
                "o": {
                    "_id": "635b79e231d82a8ab1de863b",
                    "name": "Selena O'Malley",
                    "roll_no": 51,
                    "is_graduated": false,
                    "date_of_birth": "2000-01-30",
                    "score": 95.5,
                    "age": 23.0
                }
            }`,
			expectedSQL: "INSERT INTO test.student (_id, age, date_of_birth, is_graduated, name, roll_no, score) VALUES ('635b79e231d82a8ab1de863b', 23, '2000-01-30', false, 'Selena O'Malley', 51, 95.500000);",
			expectedErr: nil,
		},
		{
			name:        "Insert: Invalid JSON",
			expectedSQL: "",
			expectedErr: fmt.Errorf("Error unmarshaling oplog"),
		},
		{
			name: "Insert: Invalid namespace",
			inputJSON: `{
                "op": "i",
                "ns": "teststudent",
                "o": {"_id": "1"}
            }`,
			expectedSQL: "",
			expectedErr: fmt.Errorf("error parsing namespace, invalid namespace"),
		},
		{
			name: "Insert: Invalid namespace",
			inputJSON: `{
                "op": "i",
                "ns": ".student",
                "o": {"_id": "1"}
            }`,
			expectedSQL: "",
			expectedErr: fmt.Errorf("error parsing namespace, invalid namespace"),
		},
		{
			name: "Insert: No data in 'o' field",
			inputJSON: `{
                "op": "i",
                "ns": "test.student",
                "o": {}
            }`,
			expectedSQL: "",
			expectedErr: fmt.Errorf("empty data field for insert"),
		},

		{
			name: "Update: Valid set single field",
			inputJSON: `{
                "op": "u",
                "ns": "test.student",
                "o": {
                    "diff": { "u": { "is_graduated": true } }
                },
                "o2": { "_id": "id123" }
            }`,
			expectedSQL: "UPDATE test.student SET is_graduated = true WHERE _id = 'id123';",
			expectedErr: nil,
		},
		{
			name: "Update: Valid set multiple fields (sorted)",
			inputJSON: `{
                "op": "u",
                "ns": "test.student",
                "o": {
                    "diff": { "u": { "name": "New Name", "age": 30 } }
                },
                "o2": { "_id": "id123" }
            }`,
			expectedSQL: "UPDATE test.student SET age = 30, name = 'New Name' WHERE _id = 'id123';",
			expectedErr: nil,
		},
		{
			name: "Update: Valid unset single field",
			inputJSON: `{
                "op": "u",
                "ns": "test.student",
                "o": {
                    "diff": { "d": { "roll_no": true } }
                },
                "o2": { "_id": "id123" }
            }`,
			expectedSQL: "UPDATE test.student SET roll_no = NULL WHERE _id = 'id123';",
			expectedErr: nil,
		},
		{
			name: "Delete: Valid oplog entry",
			inputJSON: `{
                "op": "d",
                "ns": "test.student",
                "o": { "_id": "someObjectIDString" }
            }`,
			expectedSQL: "DELETE FROM test.student WHERE _id = 'someObjectIDString';",
			expectedErr: nil,
		},
		{
			name: "Unsupported operation type",
			inputJSON: `{
                "op": "n", 
                "ns": "test.student",
                "o": {"_id": "1"}
            }`,
			expectedErr: fmt.Errorf("oplog operation not supported: received operation n"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualSQL, err := Parse(tc.inputJSON)
			if tc.expectedErr != nil {
				if err == nil {
					t.Errorf("Expected error, but got nil. Expected error type/content: %v", tc.expectedErr)
				} else if !errors.Is(err, tc.expectedErr) && !strings.Contains(err.Error(), tc.expectedErr.Error()) {
					t.Errorf("Expected error type/content '%v', but got '%v'", tc.expectedErr, err)
				}
			} else {
				if err != nil {
					t.Errorf("Did not expect an error, but got: %v", err)
				}
			}

			if actualSQL != tc.expectedSQL {
				t.Errorf("SQL mismatch:\nExpected: %s\nActual  : %s", tc.expectedSQL, actualSQL)
			}
		})
	}
}
