package dblite

import (
	"os"
	"strings"
	"testing"
)

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
