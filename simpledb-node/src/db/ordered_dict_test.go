package db

import (
	"strings"
	"testing"
)

func TestOrderedDict(t *testing.T) {
	od := newOrderedDict()

	od.Set("1", "a")
	od.Set("2", "b")
	od.Set("3", "c")

	result := []string{}
	for val := range od.Iterate() {
		result = append(result, val.(string))
	}

	if strings.Join(result, ",") != "a,b,c" {
		t.Fatalf("Ordered dict iterate expected: %v, got %v\n", "a,b,c", strings.Join(result, ","))
	}

	od.Set("1", "z")

	result = []string{}
	for val := range od.Iterate() {
		result = append(result, val.(string))
	}

	if strings.Join(result, ",") != "z,b,c" {
		t.Fatalf("Ordered dict iterate expected: %v, got %v\n", "z,b,c", strings.Join(result, ","))
	}
}
