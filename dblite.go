package dblite

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

var ErrKeyNotFound = errors.New("key not found")

type DBLite struct {
	file           *os.File
	fileName       string
	mu             sync.RWMutex
	encryptionKey  []byte
	useCompression bool
}

func NewDBLite(filename string, options ...func(*DBLite)) (*DBLite, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		return nil, err
	}
	db := &DBLite{
		file:           file,
		fileName:       filename,
		useCompression: false,
	}
	for _, option := range options {
		option(db)
	}
	return db, nil
}

func WithEncryption(key []byte) func(*DBLite) {
	return func(db *DBLite) {
		db.encryptionKey = key
	}
}

func WithCompression() func(*DBLite) {
	return func(db *DBLite) {
		db.useCompression = true
	}
}

func (db *DBLite) Set(key string, value interface{}) error {
	v := make(map[string]interface{})
	err := db.Get(key, &v)
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			return fmt.Errorf("failed to get during set replacement comparison")
		}
	} else {
		db.Delete(key)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	jsonValue, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if db.useCompression {
		jsonValue, err = compress(jsonValue)
		if err != nil {
			return err
		}
	}

	if db.encryptionKey != nil {
		jsonValue, err = encrypt(jsonValue, db.encryptionKey)
		if err != nil {
			return err
		}
	}

	encodedValue := base64.StdEncoding.EncodeToString(jsonValue)
	_, err = db.file.WriteString(fmt.Sprintf("%s=%s\n", key, encodedValue))
	return err
}

func (db *DBLite) Len() (uint64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	_, err := db.file.Seek(0, 0)
	if err != nil {
		return 0, err
	}

	var lines uint64 = 0

	scanner := bufio.NewScanner(db.file)
	for scanner.Scan() {
		lines += 1
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return lines, nil
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
			decodedValue, err := base64.StdEncoding.DecodeString(parts[1])
			if err != nil {
				return err
			}

			if db.encryptionKey != nil {
				decodedValue, err = decrypt(decodedValue, db.encryptionKey)
				if err != nil {
					return err
				}
			}

			if db.useCompression {
				decodedValue, err = decompress(decodedValue)
				if err != nil {
					return err
				}
			}

			return json.Unmarshal(decodedValue, value)
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return ErrKeyNotFound
}

func (db *DBLite) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tempFile, err := os.CreateTemp("", "dblite")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %v", err)
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	if _, err = db.file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek file: %v", err)
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
		if _, err := tempFile.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("failed to write to temp file: %v", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %v", err)
	}

	if err := db.file.Close(); err != nil {
		return fmt.Errorf("failed to close original file: %v", err)
	}

	if err := copyFile(tempFile.Name(), db.fileName); err != nil {
		return fmt.Errorf("failed to replace file: %v", err)
	}

	db.file, err = os.OpenFile(db.fileName, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen file: %v", err)
	}

	if !found {
		return ErrKeyNotFound
	}
	return nil
}

func copyFile(srcFile, destFile string) error {
	src, err := os.Open(srcFile)
	if err != nil {
		return fmt.Errorf("failed to open source file: %v", err)
	}
	defer src.Close()

	dest, err := os.Create(destFile)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %v", err)
	}
	defer dest.Close()

	if _, err := io.Copy(dest, src); err != nil {
		return fmt.Errorf("failed to copy file content: %v", err)
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

func compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(data)
	if err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompress(data []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}

func encrypt(data, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, data, nil), nil
}

func decrypt(data, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	return gcm.Open(nil, nonce, ciphertext, nil)
}
