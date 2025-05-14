package mongo

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.mongodb.org/mongo-driver/bson"

	"op-log-parser/parser"
)

type MockCursor struct {
	mock.Mock
}

func (m *MockCursor) Next(ctx context.Context) bool {
	args := m.Called(ctx)
	return args.Bool(0)
}

func (m *MockCursor) Decode(val interface{}) error {
	args := m.Called(val)
	return args.Error(0)
}

func (m *MockCursor) Err() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockCursor) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

type MockParser struct {
	mock.Mock
}

func (m *MockParser) Parse(opLogJSON string) ([]string, error) {
	args := m.Called(opLogJSON)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockParser) ProcessOpLog(opLog parser.OpLog) ([]string, error) {
	args := m.Called(opLog)
	return args.Get(0).([]string), args.Error(1)
}

func TestNewClient(t *testing.T) {
	ctx := context.Background()
	_, err := NewClient(ctx, "invalid-uri")
	assert.Error(t, err, "Expected error for invalid MongoDB URI")
}

func TestConvertToOpLog(t *testing.T) {
	tests := []struct {
		name     string
		input    bson.M
		expected parser.OpLog
		err      string
	}{
		{
			name: "Valid oplog",
			input: bson.M{
				"op": "i",
				"ns": "test.student",
				"o": bson.M{
					"_id":           "635b79e231d82a8ab1de863b",
					"name":          "Selena Miller",
					"roll_no":       51,
					"is_graduated":  false,
					"date_of_birth": "2000-01-30",
				},
				"o2": bson.M{
					"_id": "635b79e231d82a8ab1de863b",
				},
			},
			expected: parser.OpLog{
				Operation: "i",
				Namespace: "test.student",
				Data: map[string]any{
					"_id":           "635b79e231d82a8ab1de863b",
					"name":          "Selena Miller",
					"roll_no":       int32(51),
					"is_graduated":  false,
					"date_of_birth": "2000-01-30",
				},
				O2: &parser.O2Field{ID: "635b79e231d82a8ab1de863b"},
			},
			err: "",
		},
		{
			name: "Missing op field",
			input: bson.M{
				"ns": "test.student",
				"o":  bson.M{"_id": "123"},
			},
			expected: parser.OpLog{},
			err:      "invalid op field",
		},
		{
			name: "Invalid ns field",
			input: bson.M{
				"op": "i",
				"ns": 123,
				"o":  bson.M{"_id": "123"},
			},
			expected: parser.OpLog{},
			err:      "invalid ns field",
		},
		{
			name: "Missing o field",
			input: bson.M{
				"op": "i",
				"ns": "test.student",
			},
			expected: parser.OpLog{},
			err:      "invalid o field",
		},
		{
			name: "Invalid o field",
			input: bson.M{
				"op": "i",
				"ns": "test.student",
				"o":  "not-a-map",
			},
			expected: parser.OpLog{},
			err:      "marshaling o field",
		},
		{
			name: "Missing o2 field",
			input: bson.M{
				"op": "i",
				"ns": "test.student",
				"o": bson.M{
					"_id": "635b79e231d82a8ab1de863b",
				},
			},
			expected: parser.OpLog{
				Operation: "i",
				Namespace: "test.student",
				Data: map[string]any{
					"_id": "635b79e231d82a8ab1de863b",
				},
				O2: nil,
			},
			err: "invalid o2 field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToOpLog(tt.input)
			if tt.err != "" {
				assert.ErrorContains(t, err, tt.err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected.Operation, result.Operation)
			assert.Equal(t, tt.expected.Namespace, result.Namespace)
			assert.Equal(t, tt.expected.Data, result.Data)
			assert.Equal(t, tt.expected.O2, result.O2)
		})
	}
}

func TestStreamOplogsToFile(t *testing.T) {

	outputFile, err := os.CreateTemp("", "output-*.sql")
	if err != nil {
		t.Fatalf("Creating temp output file: %v", err)
	}
	defer os.Remove(outputFile.Name())

	mockParser := &MockParser{}
	mockParser.On("ProcessOpLog", mock.Anything, mock.Anything).Return(
		[]string{
			fmt.Sprintf("INSERT INTO test.student (_id,name) VALUES ('635b79e231d82a8ab1de863b', 'Test User');")}, nil)

	mockClient := &Client{client: nil}

	oplogs := []bson.M{
		{
			"op": "i",
			"ns": "test.student",
			"o": bson.M{
				"_id": "635b79e231d82a8ab1de863b",
			},
			"o2": bson.M{
				"_id": "1635b79e231d82a8ab1de863b",
			},
		},
		{
			"op": "i",
			"ns": "test.student",
			"o": bson.M{
				"_id": "635b79e231d82a8ab1de863b",
			},
			"o2": bson.M{
				"_id": "1635b79e231d82a8ab1de863b",
			},
		},
	}

	ctx := t.Context()

	originalStreamOplogs := streamOplogs
	defer func() { streamOplogs = originalStreamOplogs }()
	streamOplogs = func(ctx context.Context, client *Client, p parser.Parser, processStmt func(string) error) error {
		for i, raw := range oplogs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				opLog, err := convertToOpLog(raw)
				if err != nil {
					return err
				}
				stmts, err := p.ProcessOpLog(opLog)
				if err != nil {
					return err
				}
				for _, stmt := range stmts {
					if err := processStmt(stmt); err != nil {
						return err
					}
				}
				if i == len(oplogs)-1 {
					return nil
				}
			}
		}
		return nil
	}

	err = StreamOplogsToFile(ctx, mockClient, mockParser, outputFile.Name())
	assert.NoError(t, err)

	outputBytes, err := os.ReadFile(outputFile.Name())
	assert.NoError(t, err)
	output := string(outputBytes)
	expectedOutput := strings.Join([]string{
		"INSERT INTO test.student (_id,name) VALUES ('635b79e231d82a8ab1de863b', 'Test User');",
		"INSERT INTO test.student (_id,name) VALUES ('635b79e231d82a8ab1de863b', 'Test User');",
		"",
	}, "\n")
	assert.Equal(t, expectedOutput, output)

	err = StreamOplogsToFile(ctx, mockClient, mockParser, outputFile.Name())
	assert.NoError(t, err)

	outputBytes, err = os.ReadFile(outputFile.Name())
	assert.NoError(t, err)
	output = string(outputBytes)
	expectedOutput = strings.Join([]string{
		"INSERT INTO test.student (_id,name) VALUES ('635b79e231d82a8ab1de863b', 'Test User');",
		"INSERT INTO test.student (_id,name) VALUES ('635b79e231d82a8ab1de863b', 'Test User');",
		"INSERT INTO test.student (_id,name) VALUES ('635b79e231d82a8ab1de863b', 'Test User');",
		"INSERT INTO test.student (_id,name) VALUES ('635b79e231d82a8ab1de863b', 'Test User');",
		"",
	}, "\n")
	assert.Equal(t, expectedOutput, output)

	originalStreamOplogs = streamOplogs
	streamOplogs = func(ctx context.Context, client *Client, p parser.Parser, processStmt func(string) error) error {
		return processStmt("TEST")
	}
	err = StreamOplogsToFile(ctx, mockClient, mockParser, "/invalid/path/output.sql")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "opening output file")
}
