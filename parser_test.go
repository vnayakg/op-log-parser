package parser

import (
	"fmt"
	"reflect"
	"testing"
)

func TestOplogToInsertSQL(t *testing.T) {
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

func TestOplogToUpdateSQL(t *testing.T) {
	testCases := []struct {
		name        string
		inputJSON   string
		expectedSQL string
		expectError bool
		err         error
	}{
		{
			name: "Valid update set field",
			inputJSON: `{
				"op": "u",
				"ns": "test.student",
				"o": {
					"$v": 2,
					"diff": {
						"u": {
							"is_graduated": true
						}
					}
				},
				"o2": {
					"_id": "635b79e231d82a8ab1de863b"
				}
			}`,
			expectedSQL: "UPDATE test.student SET is_graduated = true WHERE _id = '635b79e231d82a8ab1de863b';",
			expectError: false,
		},
		{
			name: "Valid update unset field",
			inputJSON: `{
				"op": "u",
				"ns": "test.student",
				"o": {
					"$v": 2,
					"diff": {
						"d": {
							"roll_no": false
						}
					}
				},
				"o2": {
					"_id": "635b79e231d82a8ab1de863b"
				}
			}`,
			expectedSQL: "UPDATE test.student SET roll_no = NULL WHERE _id = '635b79e231d82a8ab1de863b';",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualSQL, err := parseUpdateOpLog(tc.inputJSON)

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

func TestOplogToDeleteSQL(t *testing.T) {
	testCases := []struct {
		name        string
		inputJSON   string
		expectedSQL string
		expectError bool
		err         error
	}{
		{
			name: "Valid delete log",
			inputJSON: `{
				"op": "d",
  				"ns": "test.student",
				"o": {
					"_id": "635b79e231d82a8ab1de863b"
				}
			}`,
			expectedSQL: "DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';",
			expectError: false,
		},
		{
			name: "Invalid delete log, id field missing",
			inputJSON: `{
				"op": "d",
				"ns": "test.student",
				"o": {
					"some": "other"
				}
			}`,
			expectError: true,
			err:         fmt.Errorf("_id field is missing"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualSQL, err := parseDeleteOpLog(tc.inputJSON)

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
