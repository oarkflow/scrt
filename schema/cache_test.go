package schema_test

import (
	"testing"

	"github.com/oarkflow/scrt/schema"
)

func TestCacheLoadFile(t *testing.T) {
	cache := schema.NewCache()
	first, err := cache.LoadFile("../data.scrt")
	if err != nil {
		t.Fatalf("load first: %v", err)
	}
	second, err := cache.LoadFile("../data.scrt")
	if err != nil {
		t.Fatalf("load second: %v", err)
	}
	if first != second {
		t.Fatalf("expected cached document pointer to match")
	}
	if _, ok := cache.Schema("Message"); !ok {
		t.Fatalf("cache missing message schema")
	}
}
