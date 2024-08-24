package dblite

import (
	"os"
	"strings"
	"testing"
)

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

	fileContents, err := os.ReadFile(dbFile.Name())
	if err != nil {
		t.Fatalf("Failed to read file contents: %v", err)
	}
	if strings.Contains(string(fileContents), "This is a secret message") {
		t.Errorf("Found unencrypted data in file")
	}
}
