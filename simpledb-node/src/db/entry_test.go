package db

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func setupEntry() (map[string]interface{}, *Entry, error) {
	fields := make(map[string]interface{})
	fields["id"] = uuid.New().String()
	fields["balance"] = float64(100.0)
	fields["isUser"] = true
	fields["likes"] = int64(10)
	b, err := json.Marshal(map[string]string{"test": "test"})
	if err != nil {
		return nil, nil, err
	}
	fields["info"] = b
	entry, err := createEntry(uint64(0), "test", fields)
	if err != nil {
		return nil, nil, err
	}
	return fields, entry, nil
}

func TestEntryCreate(t *testing.T) {
	fields, entry, err := setupEntry()
	if err != nil {
		t.Fatalf("Error setting up entry: %v\n", err)
	}
	if entry.ts != 0 {
		t.Fatalf("Incorrect entry: %v\n", entry)
	}
	if entry.key != "test" {
		t.Fatalf("Incorrect entry: %v\n", entry)
	}
	for name, v1 := range fields {
		if v2, ok := entry.fields[name]; ok {
			v, err := parseValue(v2)
			if err != nil {
				t.Fatalf("Error parsing value: %v\n", err)
			}
			switch v1.(type) {
			case []byte:
				if !bytes.Equal(v.([]byte), v1.([]byte)) {
					t.Fatalf("Incorrect entry: %v\n", entry)
				}
			default:
				if v != v1 {
					t.Fatalf("Incorrect entry: %v\n", entry)
				}
			}
		} else {
			t.Fatalf("Incorrect entry: %v\n", entry)
		}
	}
}

func TestEntryEncode(t *testing.T) {
	_, entry, err := setupEntry()
	if err != nil {
		t.Fatalf("Error setting up entry: %v\n", err)
	}
	data := encodeEntry(entry)
	totalSize := binary.LittleEndian.Uint32(data[0:4])
	if int(totalSize)+4 != len(data) {
		t.Fatalf("Wrong size in entry encode\n")
	}
}

func TestEntryDecode(t *testing.T) {
	fields, entry, err := setupEntry()
	if err != nil {
		t.Fatalf("Error setting up entry: %v\n", err)
	}
	data := encodeEntry(entry)
	result, err := decodeEntry(data[4:])
	if err != nil {
		t.Fatalf("Error decoding entry: %v\n", err)
	}
	if result.ts != 0 {
		t.Fatalf("Incorrect entry: %v\n", entry)
	}
	if result.key != "test" {
		t.Fatalf("Incorrect entry: %v\n", entry)
	}
	for name, v1 := range fields {
		if v2, ok := result.fields[name]; ok {
			v, err := parseValue(v2)
			if err != nil {
				t.Fatalf("Error parsing value: %v\n", err)
			}
			switch v1.(type) {
			case []byte:
				if !bytes.Equal(v.([]byte), v1.([]byte)) {
					t.Fatalf("Incorrect entry: %v\n", entry)
				}
			default:
				if v != v1 {
					t.Fatalf("Incorrect entry: %v\n", entry)
				}
			}
		} else {
			t.Fatalf("Incorrect entry: %v\n", entry)
		}
	}
}

func TestEntryWrite(t *testing.T) {
	fields, entry, err := setupEntry()
	entries := []*Entry{}
	for i := 0; i < 100; i++ {
		entries = append(entries, entry)
	}
	dataBlocks, _, _, _, err := writeEntries(entries)
	if err != nil {
		t.Fatalf("Error writing entries: %v\n", err)
	}
	result, err := decodeEntries(dataBlocks)
	if err != nil {
		t.Fatalf("Error decoding entries: %v\n", err)
	}
	if len(result) != len(entries) {
		t.Fatalf("Length result does not match length entries. Expected: %d, Got: %d\n", len(entries), len(result))
	}
	for _, entry := range result {
		if entry.ts != 0 {
			t.Fatalf("Incorrect entry: %v\n", entry)
		}
		if entry.key != "test" {
			t.Fatalf("Incorrect entry: %v\n", entry)
		}
		for name, v1 := range fields {
			if v2, ok := entry.fields[name]; ok {
				v, err := parseValue(v2)
				if err != nil {
					t.Fatalf("Error parsing value: %v\n", err)
				}
				switch v1.(type) {
				case []byte:
					if !bytes.Equal(v.([]byte), v1.([]byte)) {
						t.Fatalf("Incorrect entry: %v\n", entry)
					}
				default:
					if v != v1 {
						t.Fatalf("Expected: %v, Got: %v\n", v1, v)
					}
				}
			} else {
				t.Fatalf("Incorrect entry: %v\n", entry)
			}
		}
	}
}
