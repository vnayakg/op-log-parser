package parser

import (
	"fmt"
	"reflect"
	"testing"
)

func TestOplogToSQL(t *testing.T) {
	testCases := []struct {
		name        string
		inputJSON   string
		expectedSQL string
		expectError bool
		err         error
	}{
		{
			name: "Valid student insert",
			inputJSON: `{
				"op": "i",
				"ns": "test.student",
				"o": {
					"_id": "635b79e231d82a8ab1de863b",
					"name": "Selena Miller",
					"roll_no": 51,
					"is_graduated": false,
					"date_of_birth": "2000-01-30"
				}
			}`,
			expectedSQL: "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
			expectError: false,
		},
		{
			name:        "Invalid student insert json",
			inputJSON:   `{"op": "i""ns": "test.student"`,
			err:         fmt.Errorf("Error unmarshaling oplog"),
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualSQL, err := parseInsertOpLog(tc.inputJSON)

			if tc.expectError {
				if err == nil {
					t.Errorf("Expected an error, but got nil")
				} else if tc.err.Error() != err.Error() {
					t.Errorf("Expected error message to contain '%s', but got '%s'", tc.err, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Did not expect an error, but got: %v", err)
				}
				if !reflect.DeepEqual(actualSQL, tc.expectedSQL) {
					if actualSQL != tc.expectedSQL {
						t.Errorf("Expected SQL:\n%s\nGot SQL:\n%s", tc.expectedSQL, actualSQL)
					}
				}
			}
		})
	}
}
