package dblite

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

type Person struct {
	Name  string
	Email string
	Age   int
}

func TestPutAndGet(t *testing.T) {
	dbFile, err := os.CreateTemp("", "dblite_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(dbFile.Name())

	db, err := NewDBLite(dbFile.Name())
	if err != nil {
		t.Fatalf("Failed to create DBLite: %v", err)
	}
	defer db.Close()

	err = db.Put("greeting", "Hello, World!")
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	var greeting string
	err = db.Get("greeting", &greeting)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if greeting != "Hello, World!" {
		t.Errorf("Expected 'Hello, World!', got '%s'", greeting)
	}

	person := Person{Name: "John Doe", Email: "john.doe@example.com", Age: 30}
	err = db.Put("person1", person)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	var retrievedPerson Person
	err = db.Get("person1", &retrievedPerson)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if retrievedPerson != person {
		t.Errorf("Expected %+v, got %+v", person, retrievedPerson)
	}
}

func TestDelete(t *testing.T) {
	dbFile, err := os.CreateTemp("", "dblite_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(dbFile.Name())

	db, err := NewDBLite(dbFile.Name())
	if err != nil {
		t.Fatalf("Failed to create DBLite: %v", err)
	}
	defer db.Close()

	err = db.Put("greeting", "Hello, World!")
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	err = db.Delete("greeting")
	if err != nil {
		t.Fatalf("Failed to delete value: %v", err)
	}

	var greeting string
	err = db.Get("greeting", &greeting)
	if err == nil {
		t.Errorf("Expected error when getting deleted key, got value: '%s'", greeting)
	}

	err = db.Delete("nonexistent")
	if err == nil {
		t.Errorf("Expected error when deleting non-existent key, got nil")
	}
}

func TestWipe(t *testing.T) {
	dbFile, err := os.CreateTemp("", "dblite_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(dbFile.Name())

	db, err := NewDBLite(dbFile.Name())
	if err != nil {
		t.Fatalf("Failed to create DBLite: %v", err)
	}
	defer db.Close()

	err = db.Put("greeting", "Hello, World!")
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	err = db.Wipe()
	if err != nil {
		t.Fatalf("Failed to wipe database: %v", err)
	}

	var greeting string
	err = db.Get("greeting", &greeting)
	if err == nil {
		t.Errorf("Expected error when getting key after wipe, got value: '%s'", greeting)
	}
}

func TestConcurrency(t *testing.T) {
	dbFile, err := os.CreateTemp("", "dblite_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(dbFile.Name())

	db, err := NewDBLite(dbFile.Name())
	if err != nil {
		t.Fatalf("Failed to create DBLite: %v", err)
	}
	defer db.Close()

	done := make(chan bool)

	for i := 0; i < 100; i++ {
		go func(i int) {
			key := fmt.Sprintf("key%d", i)
			err := db.Put(key, fmt.Sprintf("value%d", i))
			if err != nil {
				t.Errorf("Failed to put value: %v", err)
			}
			done <- true
		}(i)
	}

	for i := 0; i < 100; i++ {
		go func(i int) {
			key := fmt.Sprintf("key%d", i)
			var value string
			err := db.Get(key, &value)
			if err != nil && err.Error() != "key not found" {
				t.Errorf("Failed to get value: %v", err)
			}
			done <- true
		}(i)
	}

	for i := 0; i < 200; i++ {
		<-done
	}
}

func TestEncryption(t *testing.T) {
	dbFile, err := os.CreateTemp("", "dblite_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(dbFile.Name())

	encryptionKey := []byte("0123456789abcdef")
	db, err := NewDBLite(dbFile.Name(), WithEncryption(encryptionKey))
	if err != nil {
		t.Fatalf("Failed to create DBLite: %v", err)
	}
	defer db.Close()

	err = db.Put("secret", "This is a secret message")
	if err != nil {
		t.Fatalf("Failed to put encrypted value: %v", err)
	}

	var retrievedSecret string
	err = db.Get("secret", &retrievedSecret)
	if err != nil {
		t.Fatalf("Failed to get encrypted value: %v", err)
	}

	if retrievedSecret != "This is a secret message" {
		t.Errorf("Expected 'This is a secret message', got '%s'", retrievedSecret)
	}

	// Verify that the data is actually encrypted in the file
	fileContents, err := os.ReadFile(dbFile.Name())
	if err != nil {
		t.Fatalf("Failed to read file contents: %v", err)
	}
	if strings.Contains(string(fileContents), "This is a secret message") {
		t.Errorf("Found unencrypted data in file")
	}
}

func TestCompression(t *testing.T) {
	dbFile, err := os.CreateTemp("", "dblite_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(dbFile.Name())

	db, err := NewDBLite(dbFile.Name(), WithCompression())
	if err != nil {
		t.Fatalf("Failed to create DBLite: %v", err)
	}
	defer db.Close()

	largeString := strings.Repeat("abcdefghijklmnopqrstuvwxyz", 1000)

	err = db.Put("large_data", largeString)
	if err != nil {
		t.Fatalf("Failed to put compressed value: %v", err)
	}

	var retrievedLargeString string
	err = db.Get("large_data", &retrievedLargeString)
	if err != nil {
		t.Fatalf("Failed to get compressed value: %v", err)
	}

	if retrievedLargeString != largeString {
		t.Errorf("Retrieved data doesn't match original data")
	}

	fileInfo, err := os.Stat(dbFile.Name())
	if err != nil {
		t.Fatalf("Failed to get file info: %v", err)
	}
	if fileInfo.Size() >= int64(len(largeString)) {
		t.Errorf("File size (%d) is not smaller than uncompressed data size (%d)", fileInfo.Size(), len(largeString))
	}
}

func TestEncryptionAndCompression(t *testing.T) {
	dbFile, err := os.CreateTemp("", "dblite_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(dbFile.Name())

	encryptionKey := []byte("0123456789abcdef")
	db, err := NewDBLite(dbFile.Name(), WithEncryption(encryptionKey), WithCompression())
	if err != nil {
		t.Fatalf("Failed to create DBLite: %v", err)
	}
	defer db.Close()

	largeString := strings.Repeat("abcdefghijklmnopqrstuvwxyz", 1000)

	err = db.Put("large_secret", largeString)
	if err != nil {
		t.Fatalf("Failed to put compressed and encrypted value: %v", err)
	}

	var retrievedLargeString string
	err = db.Get("large_secret", &retrievedLargeString)
	if err != nil {
		t.Fatalf("Failed to get compressed and encrypted value: %v", err)
	}

	if retrievedLargeString != largeString {
		t.Errorf("Retrieved data doesn't match original data")
	}

	fileContents, err := os.ReadFile(dbFile.Name())
	if err != nil {
		t.Fatalf("Failed to read file contents: %v", err)
	}
	if strings.Contains(string(fileContents), "abcdefghijklmnopqrstuvwxyz") {
		t.Errorf("Found unencrypted data in file")
	}
	if int64(len(fileContents)) >= int64(len(largeString)) {
		t.Errorf("File size (%d) is not smaller than uncompressed data size (%d)", len(fileContents), len(largeString))
	}
}
