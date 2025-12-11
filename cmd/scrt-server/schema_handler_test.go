package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/oarkflow/scrt/schema"
)

func TestHandleSchemasPostInferredName(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srv := &server{
		registry:  schema.NewDocumentRegistry(),
		schemaDir: dir,
	}
	const dsl = `@schema:Widget
@field ID uint64 auto_increment
@field Label string
`
	req := httptest.NewRequest(http.MethodPost, "/schemas", strings.NewReader(dsl))
	resp := httptest.NewRecorder()
	srv.handleSchemas(resp, req)
	if resp.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.Code)
	}
	var envelope struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if envelope.Schema != "Widget" {
		t.Fatalf("expected schema Widget, got %s", envelope.Schema)
	}
	if _, err := os.Stat(filepath.Join(dir, "Widget.scrt")); err != nil {
		t.Fatalf("schema file missing: %v", err)
	}
	listReq := httptest.NewRequest(http.MethodGet, "/schemas", nil)
	listResp := httptest.NewRecorder()
	srv.handleSchemas(listResp, listReq)
	if listResp.Code != http.StatusOK {
		t.Fatalf("expected GET status 200, got %d", listResp.Code)
	}
	if !strings.Contains(listResp.Body.String(), "Widget") {
		t.Fatalf("schema list missing entry, body=%q", listResp.Body.String())
	}
}
