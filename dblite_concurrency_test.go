package dblite

import (
	"fmt"
	"os"
	"testing"
)

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
			err := db.Set(key, fmt.Sprintf("value%d", i))
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
