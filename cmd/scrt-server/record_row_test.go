package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	scrt "github.com/oarkflow/scrt"
	"github.com/oarkflow/scrt/schema"
	"github.com/oarkflow/scrt/storage"
)

func TestHandleRecordRowPatch(t *testing.T) {
	t.Parallel()
	reg := schema.NewDocumentRegistry()
	const userSchema = `@schema:User
@field ID uint64 auto_increment
@field Name string
@field Email string
`
	if _, err := reg.Upsert("User", []byte(userSchema), "test", time.Now().UTC()); err != nil {
		t.Fatalf("upsert schema: %v", err)
	}
	backend, err := storage.NewSnapshotBackend(t.TempDir())
	if err != nil {
		t.Fatalf("storage backend: %v", err)
	}
	srv := &server{registry: reg, store: backend}
	doc, _, _, err := reg.Snapshot("User")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	sch, ok := doc.Schema("User")
	if !ok {
		t.Fatalf("schema User missing")
	}
	seedRows := []map[string]any{
		{"ID": uint64(1001), "Name": "John", "Email": "john@example.com"},
		{"ID": uint64(1002), "Name": "Jane", "Email": "jane@example.com"},
	}
	payload, err := scrt.Marshal(sch, seedRows)
	if err != nil {
		t.Fatalf("marshal seed rows: %v", err)
	}
	if _, err := backend.Persist("User", sch, payload, storage.PersistOptions{Indexes: storage.AutoIndexSpecs(sch)}); err != nil {
		t.Fatalf("persist seed rows: %v", err)
	}
	if err := reg.SetPayload("User", payload); err != nil {
		t.Fatalf("set payload: %v", err)
	}
	replacement, err := scrt.Marshal(sch, []map[string]any{
		{"ID": uint64(1002), "Name": "Jenny", "Email": "jenny@example.com"},
	})
	if err != nil {
		t.Fatalf("marshal replacement: %v", err)
	}
	req := httptest.NewRequest(http.MethodPatch, "/records/User/row/ID/1002", bytes.NewReader(replacement))
	req.Header.Set("Content-Type", "application/x-scrt")
	resp := httptest.NewRecorder()
	srv.handleRecords(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.Code)
	}
	var envelope struct {
		Schema string                 `json:"schema"`
		Field  string                 `json:"field"`
		Key    string                 `json:"key"`
		Row    map[string]interface{} `json:"row"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if envelope.Row["Name"] != "Jenny" {
		t.Fatalf("expected Name=Jenny, got %v", envelope.Row["Name"])
	}
	reloaded, err := backend.LoadPayload("User")
	if err != nil {
		t.Fatalf("reload payload: %v", err)
	}
	var rows []map[string]any
	if err := scrt.Unmarshal(reloaded, sch, &rows); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[1]["Name"] != "Jenny" {
		t.Fatalf("patch did not persist change, got %v", rows[1]["Name"])
	}
}
