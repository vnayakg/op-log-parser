package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var sampleOplog = []map[string]any{
	{
		"op": "i",
		"ns": "test.student",
		"o": bson.M{
			"_id":           "635b79e231d82a8ab1de863b",
			"name":          "Selena Miller",
			"roll_no":       51,
			"is_graduated":  false,
			"date_of_birth": "2000-01-30",
			"phone": bson.M{
				"personal": "7678456640",
				"work":     "8130097989",
			},
		},
	},
}

func TestE2E(t *testing.T) {
	ctx := context.Background()

	// Start MongoDB container
	mongoContainer, err := mongodb.Run(ctx, "mongodb/mongodb-community-server:latest",
		mongodb.WithReplicaSet("rs0"),
	)

	assert.NoError(t, err, "Failed to start MongoDB container")
	defer func() { assert.NoError(t, mongoContainer.Terminate(ctx)) }()

	// Get MongoDB connection string
	mongoPort, _ := mongoContainer.MappedPort(ctx, "27017/tcp")
	mongoURI := fmt.Sprintf("mongodb://localhost:%s?directConnection=true&serverSelectionTimeoutMS=2000&replicaSet=rs0", mongoPort)
	assert.NoError(t, err)

	// Start PostgreSQL container
	postgresContainer, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("pass"),
	)
	assert.NoError(t, err, "Failed to start PostgreSQL container")
	defer func() { assert.NoError(t, postgresContainer.Terminate(ctx)) }()
	time.Sleep(time.Second * 5)
	postgresURI, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)

	// temporary directories and files
	tempDir, err := os.MkdirTemp("", "e2e-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	inputFile := filepath.Join(tempDir, "input.json")
	outputFile := filepath.Join(tempDir, "output.sql")

	// Write sample oplog to input file
	inputData, err := json.Marshal(sampleOplog)
	assert.NoError(t, err)
	err = os.WriteFile(inputFile, inputData, 0644)
	assert.NoError(t, err)

	uuidPattern := `[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`
	re := regexp.MustCompile(uuidPattern)

	// Function to clean up PostgreSQL database
	cleanupPostgres := func() {
		conn, err := pgx.Connect(ctx, postgresURI)
		if err != nil {
			t.Logf("Error connecting to PostgreSQL for cleanup: %v", err)
			return
		}
		defer conn.Close(ctx)
		conn.Exec(ctx, "DROP SCHEMA test CASCADE")
		t.Log("postgres cleanup completed...")
	}

	t.Run("File to File", func(t *testing.T) {
		cmd := exec.Command("go", "run", "./cmd/main.go",
			"-input-type", "file",
			"-input-file", inputFile,
			"-output-type", "file",
			"-output-file", outputFile,
		)

		output, err := cmd.CombinedOutput()
		assert.NoError(t, err, "File to File failed: %s", string(output))

		sqlOutput, err := os.ReadFile(outputFile)
		assert.NoError(t, err)

		normalizedContent := re.ReplaceAllString(string(sqlOutput), "<UUID>")
		expectedFragments := []string{
			"CREATE SCHEMA test;",
			"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);",
			"CREATE TABLE test.student_phone (_id VARCHAR(255) PRIMARY KEY, personal VARCHAR(255), student__id VARCHAR(255), work VARCHAR(255));",
			"INSERT INTO test.student_phone (_id, personal, student__id, work) VALUES ('<UUID>', '7678456640', '635b79e231d82a8ab1de863b', '8130097989');",
			"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
		}
		for _, fragment := range expectedFragments {
			assert.Contains(t, normalizedContent, fragment, "Output SQL missing expected fragment")
		}
	})

	t.Run("File to PostgreSQL", func(t *testing.T) {
		cleanupPostgres()
		cmd := exec.Command("go", "run", "./cmd/main.go",
			"-input-type", "file",
			"-input-file", inputFile,
			"-output-type", "postgres",
			"-postgres-uri", postgresURI,
		)
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err, "File to PostgreSQL failed: %s", string(output))

		// Verify PostgreSQL state
		conn, err := pgx.Connect(ctx, postgresURI)
		assert.NoError(t, err)
		defer conn.Close(ctx)

		// Check student table
		var count int
		err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM test.student WHERE _id = $1", "635b79e231d82a8ab1de863b").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)

		var name string
		err = conn.QueryRow(ctx, "SELECT name FROM test.student WHERE _id = $1", "635b79e231d82a8ab1de863b").Scan(&name)
		assert.NoError(t, err)
		assert.Equal(t, "Selena Miller", name)

		// Check student_phone table
		err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM test.student_phone WHERE student__id = $1", "635b79e231d82a8ab1de863b").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("MongoDB to File", func(t *testing.T) {
		// Wait for replica set to be ready
		time.Sleep(1 * time.Second)

		// Insert test data
		err = populateMongoDB(ctx, mongoURI)
		assert.NoError(t, err)

		// Run parser in background to stream oplogs
		mongoOutputFile := filepath.Join(tempDir, "mongo-output.sql")
		cmd := exec.Command("go", "run", "./cmd/main.go",
			"-input-type", "mongo",
			"-mongo-uri", mongoURI,
			"-output-type", "file",
			"-output-file", mongoOutputFile,
		)

		// Create a channel to signal when the process should stop
		done := make(chan struct{})
		go func() {
			err = cmd.Start()
			assert.NoError(t, err)
			cmd.Wait()
			close(done)
		}()

		// wait for some oplogs to be written and send SIGTERM to stop the parser
		time.Sleep(time.Second)
		err = cmd.Process.Signal(os.Interrupt)
		assert.NoError(t, err)

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			// Force kill if it doesn't stop
			err = cmd.Process.Kill()
			assert.NoError(t, err)
		}

		// assert
		sqlOutput, err := os.ReadFile(mongoOutputFile)
		assert.NoError(t, err)
		sqlContent := string(sqlOutput)

		expectedFragments := []string{
			"CREATE SCHEMA test;",
			"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, name VARCHAR(255));",
			"INSERT INTO test.student (_id, name) VALUES ('test-id-1', 'Test User');",
		}
		for _, fragment := range expectedFragments {
			assert.Contains(t, sqlContent, fragment, "Output SQL missing expected fragment")
		}
	})

	t.Run("MongoDB to PostgreSQL", func(t *testing.T) {
		cleanupPostgres()
		cmd := exec.Command("go", "run", "./cmd/main.go",
			"-input-type", "mongo",
			"-mongo-uri", mongoURI,
			"-output-type", "postgres",
			"-postgres-uri", postgresURI,
		)

		// Capture command output for debugging
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		// Create a channel to signal when the process should stop
		done := make(chan struct{})
		var cmdErr error

		// Start the process
		t.Log("Starting MongoDB to PostgreSQL process...")
		err = cmd.Start()
		assert.NoError(t, err)

		go func() {
			cmdErr = cmd.Wait()
			t.Logf("Process wait completed with error: %v", cmdErr)
			close(done)
		}()

		t.Log("Waiting for oplogs to be written...")
		time.Sleep(2 * time.Second)

		t.Log("Sending interrupt signal...")
		if cmd.Process != nil {
			err = cmd.Process.Signal(os.Interrupt)
			if err != nil {
				t.Logf("Error sending interrupt signal: %v", err)
			} else {
				t.Log("Interrupt signal sent successfully")
			}
		}

		select {
		case <-done:
			t.Log("Process finished normally")
			if cmdErr != nil {
				t.Logf("Process exited with error: %v", cmdErr)
			}
		case <-time.After(2 * time.Second):
			t.Log("Process did not finish within timeout")
			if cmd.Process != nil {
				err = cmd.Process.Kill()
				if err != nil {
					t.Logf("Error killing process: %v", err)
				} else {
					t.Log("Process killed successfully")
				}
			} else {
				t.Log("Process is nil, cannot kill")
			}
		}

		// assert
		t.Log("Verifying PostgreSQL state...")
		conn, err := pgx.Connect(ctx, postgresURI)
		assert.NoError(t, err)
		defer conn.Close(ctx)

		var count int
		err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM test.student WHERE _id = $1", "test-id-1").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)

		var name string
		err = conn.QueryRow(ctx, "SELECT name FROM test.student WHERE _id = $1", "test-id-1").Scan(&name)
		assert.NoError(t, err)
		assert.Equal(t, "Test User", name)
	})
}

func populateMongoDB(ctx context.Context, uri string) error {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("connecting to MongoDB: %w", err)
	}
	defer client.Disconnect(ctx)

	db := client.Database("test")
	collection := db.Collection("student")

	_, err = collection.InsertOne(ctx, bson.M{
		"_id":  "test-id-1",
		"name": "Test User",
	})

	// _, err = collection.DeleteOne(ctx, bson.M{
	// 	"_id": "test-id-1"})
	if err != nil {
		return fmt.Errorf("inserting test data: %w", err)
	}

	return nil
}
