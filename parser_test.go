package parser

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	testCases := []struct {
		name        string
		inputJSON   string
		expectedSQL []string
		expectedErr error
	}{
		{
			name: "Insert: Valid student data",
			inputJSON: `[{
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
            },
			{
                "op": "i",
                "ns": "test.student",
                "o": {
                    "_id": "123b79e231d82a8ab1de863b",
                    "name": "Ramesh Ramesh",
                    "roll_no": 52,
                    "is_graduated": false,
                    "date_of_birth": "2001-01-30",
                    "score": 80,
                    "age": 24.0
                }
            }]`,
			expectedSQL: []string{
				"CREATE SCHEMA test",
				"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, age FLOAT, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT, score FLOAT);",
				"INSERT INTO test.student (_id, age, date_of_birth, is_graduated, name, roll_no, score) VALUES ('635b79e231d82a8ab1de863b', 23, '2000-01-30', false, 'Selena O'Malley', 51, 95.500000);",
				"INSERT INTO test.student (_id, age, date_of_birth, is_graduated, name, roll_no, score) VALUES ('123b79e231d82a8ab1de863b', 24, '2001-01-30', false, 'Ramesh Ramesh', 52, 80);"},
			expectedErr: nil,
		},
		{
			name: "Insert and Alter: for updated columns",
			inputJSON: `[{
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
            },
			{
                "op": "i",
                "ns": "test.student",
                "o": {
                    "_id": "123b79e231d82a8ab1de863b",
                    "name": "Ramesh Ramesh",
                    "roll_no": 52,
                    "is_graduated": false,
                    "date_of_birth": "2001-01-30",
                    "score": 80,
                    "age": 24.0,
					"gender": "Male"
                }
            },
			{
                "op": "i",
                "ns": "test.student",
                "o": {
                    "_id": "098b79e231d82a8ab1de863b",
                    "name": "Superman",
                    "roll_no": 1,
                    "is_graduated": true,
                    "date_of_birth": "1920-01-30",
                    "score": 100,
                    "age": 110,
					"gender": "Male",
					"height": 6.1,
					"weight": 90
                }
            }
			]`,
			expectedSQL: []string{
				"CREATE SCHEMA test",
				"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, age FLOAT, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT, score FLOAT);",
				"INSERT INTO test.student (_id, age, date_of_birth, is_graduated, name, roll_no, score) VALUES ('635b79e231d82a8ab1de863b', 23, '2000-01-30', false, 'Selena O'Malley', 51, 95.500000);",
				"ALTER TABLE test.student ADD gender VARCHAR(255);",
				"INSERT INTO test.student (_id, age, date_of_birth, gender, is_graduated, name, roll_no, score) VALUES ('123b79e231d82a8ab1de863b', 24, '2001-01-30', 'Male', false, 'Ramesh Ramesh', 52, 80);",
				"ALTER TABLE test.student ADD height FLOAT, weight FLOAT;",
				"INSERT INTO test.student (_id, age, date_of_birth, gender, height, is_graduated, name, roll_no, score, weight) VALUES ('098b79e231d82a8ab1de863b', 110, '1920-01-30', 'Male', 6.100000, true, 'Superman', 1, 100, 90);",
			},
			expectedErr: nil,
		},
		{
			name:        "Insert: Invalid JSON",
			expectedSQL: nil,
			expectedErr: fmt.Errorf("Error unmarshaling oplog"),
		},
		{
			name: "Insert: Invalid namespace",
			inputJSON: `[{
                "op": "i",
                "ns": "teststudent",
                "o": {"_id": "1"}
            }]`,
			expectedSQL: nil,
			expectedErr: fmt.Errorf("error parsing namespace, invalid namespace"),
		},
		{
			name: "Insert: Invalid namespace",
			inputJSON: `[{
                "op": "i",
                "ns": ".student",
                "o": {"_id": "1"}
            }]`,
			expectedSQL: nil,
			expectedErr: fmt.Errorf("error parsing namespace, invalid namespace"),
		},
		{
			name: "Insert: No data in 'o' field",
			inputJSON: `[{
                "op": "i",
                "ns": "test.student",
                "o": {}
            }]`,
			expectedSQL: nil,
			expectedErr: fmt.Errorf("empty data field for insert"),
		},

		{
			name: "Update: Valid set single field",
			inputJSON: `[{
                "op": "u",
                "ns": "test.student",
                "o": {
                    "diff": { "u": { "is_graduated": true } }
                },
                "o2": { "_id": "id123" }
            }]`,
			expectedSQL: []string{"UPDATE test.student SET is_graduated = true WHERE _id = 'id123';"},
			expectedErr: nil,
		},
		{
			name: "Update: Valid set multiple fields (sorted)",
			inputJSON: `[{
                "op": "u",
                "ns": "test.student",
                "o": {
                    "diff": { "u": { "name": "New Name", "age": 30 } }
                },
                "o2": { "_id": "id123" }
            }]`,
			expectedSQL: []string{"UPDATE test.student SET age = 30, name = 'New Name' WHERE _id = 'id123';"},
			expectedErr: nil,
		},
		{
			name: "Update: Valid unset single field",
			inputJSON: `[{
                "op": "u",
                "ns": "test.student",
                "o": {
                    "diff": { "d": { "roll_no": true } }
                },
                "o2": { "_id": "id123" }
            }]`,
			expectedSQL: []string{"UPDATE test.student SET roll_no = NULL WHERE _id = 'id123';"},
			expectedErr: nil,
		},
		{
			name: "Update: Valid set and unset fields (sorted, set then unset internally)",
			inputJSON: `[{
                "op": "u",
                "ns": "test.student",
                "o": {
                    "diff": {
                        "u": { "name": "Updated Name", "status": "active" },
                        "d": { "old_field": true, "temp_data": 1 }
                    }
                },
                "o2": { "_id": "idXYZ" }
            }]`,
			expectedSQL: []string{"UPDATE test.student SET name = 'Updated Name', status = 'active', old_field = NULL, temp_data = NULL WHERE _id = 'idXYZ';"},
			expectedErr: nil,
		},
		{
			name: "Update: o2 field with empty _id",
			inputJSON: `[{
                "op": "u",
                "ns": "test.student",
                "o": {"diff": {"u": {"name": "test"}}},
                "o2": {"_id": ""}
            }]`,
			expectedSQL: nil,
			expectedErr: fmt.Errorf("_id field is missing"),
		},
		{
			name: "Delete: Valid oplog entry",
			inputJSON: `[{
                "op": "d",
                "ns": "test.student",
                "o": { "_id": "someObjectIDString" }
            }]`,
			expectedSQL: []string{"DELETE FROM test.student WHERE _id = 'someObjectIDString';"},
			expectedErr: nil,
		},
		{
			name: "Unsupported operation type",
			inputJSON: `[{
                "op": "n", 
                "ns": "test.student",
                "o": {"_id": "1"}
            }]`,
			expectedErr: fmt.Errorf("oplog operation not supported: received operation n"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualSQL, err := Parse(tc.inputJSON)
			if tc.expectedErr != nil {
				if err == nil {
					t.Errorf("Expected error, but got nil. Expected error type/content: %v", tc.expectedErr)
				} else if !errors.Is(err, tc.expectedErr) && err.Error() != tc.expectedErr.Error() {
					t.Errorf("Expected error type/content '%v', but got '%v'", tc.expectedErr, err)
				}
			} else {
				if err != nil {
					t.Errorf("Did not expect an error, but got: %v", err)
				}
			}

			if !reflect.DeepEqual(actualSQL, tc.expectedSQL) {
				t.Errorf("SQL mismatch:\nExpected: %s\nActual  : %s", tc.expectedSQL, actualSQL)
			}
		})
	}
}
