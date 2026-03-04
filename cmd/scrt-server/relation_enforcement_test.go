package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	scrt "github.com/oarkflow/scrt"
	"github.com/oarkflow/scrt/schema"
	"github.com/oarkflow/scrt/storage"
)

func TestDeleteRestrictRelation(t *testing.T) {
	t.Parallel()
	reg := schema.NewDocumentRegistry()
	userDSL := `@schema:User
@field ID uint64 serial pk
@field Name string
`
	orderDSL := `@schema:Order
@field ID uint64 serial pk
@field UserID ref:User:ID onDelete:restrict onUpdate:restrict
`
	if _, err := reg.Upsert("User", []byte(userDSL), "test", time.Now().UTC()); err != nil {
		t.Fatalf("upsert user schema: %v", err)
	}
	if _, err := reg.Upsert("Order", []byte(orderDSL), "test", time.Now().UTC()); err != nil {
		t.Fatalf("upsert order schema: %v", err)
	}
	backend, err := storage.NewSnapshotBackend(t.TempDir())
	if err != nil {
		t.Fatalf("storage backend: %v", err)
	}
	srv := &server{registry: reg, store: backend}

	seedSchemaRows(t, reg, backend, "User", []map[string]any{
		{"ID": uint64(1), "Name": "Alice"},
	})
	seedSchemaRows(t, reg, backend, "Order", []map[string]any{
		{"ID": uint64(10), "UserID": uint64(1)},
	})

	req := httptest.NewRequest(http.MethodDelete, "/records/User/row/ID/1", nil)
	resp := httptest.NewRecorder()
	srv.handleRecords(resp, req)
	if resp.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d body=%s", resp.Code, resp.Body.String())
	}
}

func TestDeleteCascadeRelation(t *testing.T) {
	t.Parallel()
	reg := schema.NewDocumentRegistry()
	userDSL := `@schema:User
@field ID uint64 serial pk
@field Name string
`
	orderDSL := `@schema:Order
@field ID uint64 serial pk
@field UserID ref:User:ID onDelete:cascade onUpdate:restrict
`
	if _, err := reg.Upsert("User", []byte(userDSL), "test", time.Now().UTC()); err != nil {
		t.Fatalf("upsert user schema: %v", err)
	}
	if _, err := reg.Upsert("Order", []byte(orderDSL), "test", time.Now().UTC()); err != nil {
		t.Fatalf("upsert order schema: %v", err)
	}
	backend, err := storage.NewSnapshotBackend(t.TempDir())
	if err != nil {
		t.Fatalf("storage backend: %v", err)
	}
	srv := &server{registry: reg, store: backend}

	seedSchemaRows(t, reg, backend, "User", []map[string]any{
		{"ID": uint64(1), "Name": "Alice"},
	})
	seedSchemaRows(t, reg, backend, "Order", []map[string]any{
		{"ID": uint64(10), "UserID": uint64(1)},
	})

	req := httptest.NewRequest(http.MethodDelete, "/records/User/row/ID/1", nil)
	resp := httptest.NewRecorder()
	srv.handleRecords(resp, req)
	if resp.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d body=%s", resp.Code, resp.Body.String())
	}

	doc, _, _, err := reg.Snapshot("Order")
	if err != nil {
		t.Fatalf("snapshot order: %v", err)
	}
	orderSchema, ok := doc.Schema("Order")
	if !ok {
		t.Fatalf("order schema missing")
	}
	payload, err := backend.LoadPayload("Order")
	if err != nil {
		t.Fatalf("load order payload: %v", err)
	}
	var rows []map[string]any
	if err := scrt.Unmarshal(payload, orderSchema, &rows); err != nil {
		t.Fatalf("unmarshal order payload: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected child rows to be deleted, got %d", len(rows))
	}
}

func seedSchemaRows(t *testing.T, reg *schema.DocumentRegistry, backend storage.Backend, schemaName string, rows []map[string]any) {
	t.Helper()
	doc, _, _, err := reg.Snapshot(schemaName)
	if err != nil {
		t.Fatalf("snapshot %s: %v", schemaName, err)
	}
	sch, ok := doc.Schema(schemaName)
	if !ok {
		t.Fatalf("schema %s missing", schemaName)
	}
	payload, err := scrt.Marshal(sch, rows)
	if err != nil {
		t.Fatalf("marshal %s rows: %v", schemaName, err)
	}
	if _, err := backend.Persist(schemaName, sch, payload, storage.PersistOptions{Indexes: storage.AutoIndexSpecs(sch)}); err != nil {
		t.Fatalf("persist %s rows: %v", schemaName, err)
	}
	if err := reg.SetPayload(schemaName, payload); err != nil {
		t.Fatalf("set payload %s: %v", schemaName, err)
	}
}
