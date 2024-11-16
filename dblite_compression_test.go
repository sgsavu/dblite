package dblite

import (
	"os"
	"strings"
	"testing"
)

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

	err = db.Set("large_data", largeString)
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
