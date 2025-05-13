package file

import (
	"os"
	"testing"

	"op-log-parser/parser"
)

func TestParseFromFile(t *testing.T) {
	inputJSON := `[
		{
			"op": "i",
			"ns": "test.student",
			"o":
			{
				"_id": "635b79e231d82a8ab1de863b",
				"name": "Selena Miller",
				"roll_no": 100,
				"is_graduated": false,
				"date_of_birth": "2000-01-30",
				"address":
				[
					{
						"line1": "481 Harborsburgh",
						"zip": "89799"
					},
					{
						"line1": "329 Flatside",
						"zip": "80872"
					}
				],
				"phone":
				{
					"personal": "7678456640",
					"work": "8130097989"
				}
			}
		},
		{
			"op": "i",
			"ns": "test.student",
			"o":
			{
				"_id": "635b79e231d82a8ab1de863b",
				"name": "Selena Miller",
				"roll_no": 100,
				"is_graduated": false,
				"date_of_birth": "2000-01-30",
				"address":
				[
					{
						"line1": "481 Harborsburgh",
						"zip": "89799"
					},
					{
						"line1": "329 Flatside",
						"zip": "80872"
					}
				],
				"phone":
				{
					"personal": "7678456640",
					"work": "8130097989"
				}
			}
		}
	]`
	inputFile, err := os.CreateTemp("", "input-*.json")
	if err != nil {
		t.Fatalf("Creating temp input file: %v", err)
	}
	defer os.Remove(inputFile.Name())
	if _, err := inputFile.Write([]byte(inputJSON)); err != nil {
		t.Fatalf("Writing to temp input file: %v", err)
	}
	inputFile.Close()
	outputFile := os.TempDir() + "/output.sql"
	defer os.Remove(outputFile)
	p := parser.CreateParser(func() string {
		return "random-uuid"
	})

	if err := ParseFromFile(p, inputFile.Name(), outputFile); err != nil {
		t.Fatalf("ParseFromFile failed: %v", err)
	}

	outputBytes, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Reading output file: %v", err)
	}
	output := string(outputBytes)
	expectedOutput := `CREATE SCHEMA test;
CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);
CREATE TABLE test.student_phone (_id VARCHAR(255) PRIMARY KEY, personal VARCHAR(255), student__id VARCHAR(255), work VARCHAR(255));
INSERT INTO test.student_phone (_id, personal, student__id, work) VALUES ('random-uuid', '7678456640', '635b79e231d82a8ab1de863b', '8130097989');
CREATE TABLE test.student_address (_id VARCHAR(255) PRIMARY KEY, line1 VARCHAR(255), student__id VARCHAR(255), zip VARCHAR(255));
INSERT INTO test.student_address (_id, line1, student__id, zip) VALUES ('random-uuid', '481 Harborsburgh', '635b79e231d82a8ab1de863b', '89799');
INSERT INTO test.student_address (_id, line1, student__id, zip) VALUES ('random-uuid', '329 Flatside', '635b79e231d82a8ab1de863b', '80872');
INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 100);
INSERT INTO test.student_phone (_id, personal, student__id, work) VALUES ('random-uuid', '7678456640', '635b79e231d82a8ab1de863b', '8130097989');
INSERT INTO test.student_address (_id, line1, student__id, zip) VALUES ('random-uuid', '481 Harborsburgh', '635b79e231d82a8ab1de863b', '89799');
INSERT INTO test.student_address (_id, line1, student__id, zip) VALUES ('random-uuid', '329 Flatside', '635b79e231d82a8ab1de863b', '80872');
INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 100);
`
	if output != expectedOutput {
		t.Errorf("Output mismatch.\nExpected:\n%s\nGot:\n%s", expectedOutput, output)
	}
}
