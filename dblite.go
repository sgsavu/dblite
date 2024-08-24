package dblite

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
)

type DBLite struct {
	file     *os.File
	fileName string
	mu       sync.RWMutex
}

func NewDBLite(filename string) (*DBLite, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &DBLite{file: file, fileName: filename}, nil
}

func (db *DBLite) Put(key string, value interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	jsonValue, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = db.file.WriteString(fmt.Sprintf("%s=%s\n", key, string(jsonValue)))
	return err
}

func (db *DBLite) Get(key string, value interface{}) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	_, err := db.file.Seek(0, 0)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(db.file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 && parts[0] == key {
			return json.Unmarshal([]byte(parts[1]), value)
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return fmt.Errorf("key not found")
}

func (db *DBLite) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tempFile, err := os.CreateTemp("", "dblite")
	if err != nil {
		return err
	}
	defer os.Remove(tempFile.Name())

	_, err = db.file.Seek(0, 0)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(db.file)
	found := false
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 && parts[0] == key {
			found = true
			continue
		}
		_, err := tempFile.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	if err := db.file.Close(); err != nil {
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}

	if err := os.Rename(tempFile.Name(), db.fileName); err != nil {
		return err
	}

	db.file, err = os.OpenFile(db.fileName, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	if !found {
		return fmt.Errorf("key not found")
	}
	return nil
}

func (db *DBLite) Wipe() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.file.Close(); err != nil {
		return err
	}

	err := os.Remove(db.fileName)
	if err != nil {
		return err
	}

	db.file, err = os.OpenFile(db.fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	return err
}

func (db *DBLite) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.file.Close()
}
