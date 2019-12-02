package db

import (
	"bytes"
	"strconv"
	"testing"
)

func TestAPIInsert(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}
	err = db.Insert("test", map[string][]byte{"value": []byte("test")})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	err = db.Insert("test", map[string][]byte{"value": []byte("another test")})
	if _, ok := err.(*ErrKeyAlreadyExists); !ok {
		t.Fatalf("Expected: ErrKeyAlreadyExists, Got: %v\n", err)
	}
	entry, err := db.Read("test", []string{"value"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	if string(entry.Fields["value"].Data) != "test" {
		t.Fatalf("Got wrong read\n")
	}
}

func TestAPIUpdate(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}
	err = db.Update("test", map[string][]byte{"value": []byte("test")})
	if _, ok := err.(*ErrKeyNotFound); !ok {
		t.Fatalf("Expected: ErrKeyNotFound, Got: %v\n", err)
	}
	err = db.Insert("test", map[string][]byte{"value": []byte("test")})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	err = db.Update("test", map[string][]byte{"value": []byte("another test")})
	if err != nil {
		t.Fatalf("Error updating db: %v\n", err)
	}
	entry, err := db.Read("test", []string{"value"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	if string(entry.Fields["value"].Data) != "another test" {
		t.Fatalf("Got wrong read\n")
	}
}

func TestAPIDelete(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}
	err = db.Delete("test")
	if err != nil {
		t.Fatalf("Error deleting from db: %v\n", err)
	}
	err = db.Insert("test", map[string][]byte{"value": []byte("test")})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	entry, err := db.Read("test", []string{"value"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	if string(entry.Fields["value"].Data) != "test" {
		t.Fatalf("Got wrong read\n")
	}
	err = db.Delete("test")
	if err != nil {
		t.Fatalf("Error deleting from db: %v\n", err)
	}
	entry, err = db.Read("test", []string{"value"})
	if _, ok := err.(*ErrKeyNotFound); !ok {
		t.Fatalf("Expected: ErrKeyNotFound, Got: %v\n", err)
	}
}

func TestAPIRead(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}
	err = db.Insert("test", map[string][]byte{"1": []byte("1"), "2": []byte("2"), "3": []byte("3")})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	entry, err := db.Read("test", []string{"1", "2", "3", "4"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	for i := 1; i < 5; i++ {
		key := strconv.Itoa(i)
		if i == 4 {
			if entry.Fields[key] != nil {
				t.Fatalf("values[1] Expected: nil, Got: %v\n", entry.Fields[key])
			}
		} else {
			if !bytes.Equal(entry.Fields[key].Data, []byte(key)) {
				t.Fatalf("values[1] Expected: %v, Got: %v\n", []byte(key), entry.Fields[key].Data)
			}
		}
	}
	err = db.Update("test", map[string][]byte{"4": []byte("4")})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	entry, err = db.Read("test", []string{"1", "2", "3", "4"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	for i := 1; i < 5; i++ {
		key := strconv.Itoa(i)
		if !bytes.Equal(entry.Fields[key].Data, []byte(key)) {
			t.Fatalf("values[1] Expected: %v, Got: %v\n", []byte(key), entry.Fields[key].Data)
		}
	}
}

func TestAPIScan(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}
	err = db.Insert("test", map[string][]byte{"value": []byte("test")})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	entries, err := db.Scan("0", []string{"value"})
	if err != nil {
		t.Fatalf("Error scanning db: %v\n", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Scan length, Expected: 1, Got: %d\n", len(entries))
	}
	if v, ok := entries[0].Fields["value"]; !(ok && bytes.Equal(v.Data, []byte("test"))) {
		t.Fatalf("Wrong value for scan\n")
	}
	entries, err = db.Scan("u", []string{"value"})
	if err != nil {
		t.Fatalf("Error scanning db: %v\n", err)
	}
	if len(entries) != 0 {
		t.Fatalf("Scan length, Expected: 0, Got: %d\n", len(entries))
	}
	err = db.Insert("z", map[string][]byte{"value": []byte("test")})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	err = db.Insert("zz999", map[string][]byte{"value": []byte("test")})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	entries, err = db.Scan("0", []string{"value"})
	if err != nil {
		t.Fatalf("Error scanning db: %v\n", err)
	}
	if len(entries) != 3 {
		t.Fatalf("Scan length, Expected: 1, Got: %d\n", len(entries))
	}
	for _, entry := range entries {
		if v, ok := entry.Fields["value"]; !(ok && bytes.Equal(v.Data, []byte("test"))) {
			t.Fatalf("Wrong value for scan\n")
		}
	}
}
