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
	values, err := db.Read("test", []string{"value"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	if string(values["value"]) != "test" {
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
	values, err := db.Read("test", []string{"value"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	if string(values["value"]) != "another test" {
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
	values, err := db.Read("test", []string{"value"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	if string(values["value"]) != "test" {
		t.Fatalf("Got wrong read\n")
	}
	err = db.Delete("test")
	if err != nil {
		t.Fatalf("Error deleting from db: %v\n", err)
	}
	values, err = db.Read("test", []string{"value"})
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
	values, err := db.Read("test", []string{"1", "2", "3", "4"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	for i := 1; i < 5; i++ {
		key := strconv.Itoa(i)
		if i == 4 {
			if !bytes.Equal(values[key], []byte{}) {
				t.Fatalf("values[1] Expected: %v, Got: %v\n", []byte{}, values[key])
			}
		} else {
			if !bytes.Equal(values[key], []byte(key)) {
				t.Fatalf("values[1] Expected: %v, Got: %v\n", []byte(key), values[key])
			}
		}
	}
	err = db.Update("test", map[string][]byte{"4": []byte("4")})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	values, err = db.Read("test", []string{"1", "2", "3", "4"})
	if err != nil {
		t.Fatalf("Error reading from db: %v\n", err)
	}
	for i := 1; i < 5; i++ {
		key := strconv.Itoa(i)
		if !bytes.Equal(values[key], []byte(key)) {
			t.Fatalf("values[1] Expected: %v, Got: %v\n", []byte(key), values[key])
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
	values, err := db.Scan("0", []string{"value"})
	if err != nil {
		t.Fatalf("Error scanning db: %v\n", err)
	}
	if len(values) != 1 {
		t.Fatalf("Scan length, Expected: 1, Got: %d\n", len(values))
	}
	if v, ok := values[0]["value"]; !(ok && bytes.Equal(v, []byte("test"))) {
		t.Fatalf("Wrong value for scan\n")
	}
	values, err = db.Scan("u", []string{"value"})
	if err != nil {
		t.Fatalf("Error scanning db: %v\n", err)
	}
	if len(values) != 0 {
		t.Fatalf("Scan length, Expected: 0, Got: %d\n", len(values))
	}
	err = db.Insert("z", map[string][]byte{"value": []byte("test")})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	err = db.Insert("zz999", map[string][]byte{"value": []byte("test")})
	if err != nil {
		t.Fatalf("Error inserting into db: %v\n", err)
	}
	values, err = db.Scan("0", []string{"value"})
	if err != nil {
		t.Fatalf("Error scanning db: %v\n", err)
	}
	if len(values) != 3 {
		t.Fatalf("Scan length, Expected: 1, Got: %d\n", len(values))
	}
	for _, value := range values {
		if v, ok := value["value"]; !(ok && bytes.Equal(v, []byte("test"))) {
			t.Fatalf("Wrong value for scan\n")
		}
	}
}
