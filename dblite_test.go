package dblite

import (
	"os"
	"testing"
)

type Person struct {
	Name  string
	Email string
	Age   int
}

func TestSetAndGet(t *testing.T) {
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

	err = db.Set("greeting", "Hello, World!")
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

	err = db.Set("person", person)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	var retrievedPerson Person
	err = db.Get("person", &retrievedPerson)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if retrievedPerson != person {
		t.Errorf("Expected %+v, got %+v", person, retrievedPerson)
	}
}

func TestSetReplacement(t *testing.T) {
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

	person := Person{Name: "John Doe", Email: "john.doe@example.com", Age: 30}
	otherPerson := Person{Name: "Johnny Bravo", Email: "johnny.bravo@example.com", Age: 69}

	err = db.Set("person", person)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	err = db.Set("person", otherPerson)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	var retrievedPerson Person
	err = db.Get("person", &retrievedPerson)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if retrievedPerson != otherPerson {
		t.Errorf("Expected %+v, got %+v", otherPerson, retrievedPerson)
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

	err = db.Set("greeting", "Hello, World!")
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

func TestDeleteCrossLink(t *testing.T) {
	dbFile, err := os.CreateTemp(".", "dblite_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(dbFile.Name())

	db, err := NewDBLite(dbFile.Name())
	if err != nil {
		t.Fatalf("Failed to create DBLite: %v", err)
	}
	defer db.Close()

	err = db.Set("greeting", "Hello, World!")
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

	err = db.Set("greeting", "Hello, World!")
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
