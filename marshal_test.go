package scrt_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/oarkflow/scrt"
)

func TestMarshalFilesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.scrt")
	dataPath := filepath.Join(dir, "data.scrtbin")
	schemaSource := `@schema Log
@field ID uint64
@field Msg string`

	if err := os.WriteFile(schemaPath, []byte(schemaSource), 0o644); err != nil {
		t.Fatalf("write schema: %v", err)
	}

	input := []map[string]any{
		{"ID": uint64(10), "Msg": "hello"},
		{"ID": uint64(11), "Msg": "world"},
	}

	if err := scrt.MarshalFiles(schemaPath, "Log", dataPath, input); err != nil {
		t.Fatalf("MarshalFiles: %v", err)
	}

	var out []map[string]any
	if err := scrt.UnmarshalFiles(schemaPath, "Log", dataPath, &out); err != nil {
		t.Fatalf("UnmarshalFiles: %v", err)
	}

	if len(out) != 2 || out[1]["Msg"].(string) != "world" {
		t.Fatalf("unexpected file roundtrip result: %+v", out)
	}
}
