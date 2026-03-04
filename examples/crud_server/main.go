package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	scrt "github.com/oarkflow/scrt"
	"github.com/oarkflow/scrt/schema"
)

type rowEnvelope struct {
	Schema string         `json:"schema"`
	Field  string         `json:"field"`
	Key    string         `json:"key"`
	Row    map[string]any `json:"row"`
}

func main() {
	baseURL := flag.String("base", "http://localhost:8080", "SCRT server base URL")
	scrtFile := flag.String("scrt", "examples/complete_feature_showcase/all_reference_cases.scrt", "path to SCRT file")
	targetSchema := flag.String("schema", "Order", "schema used for row-level CRUD demo")
	keyField := flag.String("key-field", "OrderID", "lookup field for row-level CRUD")
	updateKey := flag.String("update-key", "1001", "row key to update")
	deleteKey := flag.String("delete-key", "1002", "row key to delete")
	flag.Parse()

	doc, err := schema.ParseFile(*scrtFile)
	if err != nil {
		fail("parse SCRT file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	order, err := dependencyOrder(doc)
	if err != nil {
		fail("resolve schema dependency order: %v", err)
	}

	fmt.Printf("Using SCRT file: %s\n", *scrtFile)
	fmt.Printf("Uploading schemas in dependency order: %s\n", strings.Join(order, " -> "))

	for _, name := range order {
		sch, _ := doc.Schema(name)
		dsl := renderSchemaDSL(sch)
		if err := upsertSchema(ctx, *baseURL, name, []byte(dsl)); err != nil {
			fail("upsert schema %s: %v", name, err)
		}
		fmt.Printf("Schema upserted: %s\n", name)
	}

	for _, name := range order {
		sch, _ := doc.Schema(name)
		rows, _ := doc.Records(name)
		if len(rows) == 0 {
			continue
		}
		payload, err := scrt.Marshal(sch, rows)
		if err != nil {
			fail("marshal records for %s: %v", name, err)
		}
		if err := replaceRecords(ctx, *baseURL, name, payload); err != nil {
			fail("upload records for %s: %v", name, err)
		}
		fmt.Printf("Records uploaded: %s (%d rows)\n", name, len(rows))
	}

	sch, ok := doc.Schema(*targetSchema)
	if !ok {
		fail("target schema %q not found in %s", *targetSchema, *scrtFile)
	}
	if _, ok := sch.FieldIndex(*keyField); !ok {
		fail("field %q not found in schema %q", *keyField, *targetSchema)
	}

	recordsPayload, err := fetchRecords(ctx, *baseURL, *targetSchema)
	if err != nil {
		fail("fetch records for %s: %v", *targetSchema, err)
	}
	var rows []map[string]any
	if err := scrt.Unmarshal(recordsPayload, sch, &rows); err != nil {
		fail("decode records for %s: %v", *targetSchema, err)
	}
	fmt.Printf("Read: %s has %d row(s)\n", *targetSchema, len(rows))

	rowToUpdate, ok := findRowByKey(rows, *keyField, *updateKey)
	if !ok {
		fail("update key %s not found on %s.%s", *updateKey, *targetSchema, *keyField)
	}
	mutateRow(rowToUpdate)
	rowPayload, err := scrt.Marshal(sch, []map[string]any{rowToUpdate})
	if err != nil {
		fail("marshal updated row: %v", err)
	}
	updated, err := patchRow(ctx, *baseURL, *targetSchema, *keyField, *updateKey, rowPayload)
	if err != nil {
		fail("patch row %s=%s: %v", *keyField, *updateKey, err)
	}
	fmt.Printf("Updated row %s=%s -> %v\n", *keyField, *updateKey, updated.Row)

	if err := deleteRow(ctx, *baseURL, *targetSchema, *keyField, *deleteKey); err != nil {
		fail("delete row %s=%s: %v", *keyField, *deleteKey, err)
	}
	fmt.Printf("Deleted row %s=%s\n", *keyField, *deleteKey)

	_, status, err := getRow(ctx, *baseURL, *targetSchema, *keyField, *deleteKey)
	if err != nil {
		fail("verify deleted row: %v", err)
	}
	if status == http.StatusNotFound {
		fmt.Printf("Delete verification passed: %s=%s is not found\n", *keyField, *deleteKey)
	} else {
		fail("delete verification failed: expected 404, got %d", status)
	}
}

func upsertSchema(ctx context.Context, baseURL, name string, dsl []byte) error {
	u := strings.TrimRight(baseURL, "/") + "/schemas/" + url.PathEscape(name)
	_, status, err := request(ctx, http.MethodPost, u, "text/plain; charset=utf-8", dsl)
	if err != nil {
		return err
	}
	if status != http.StatusCreated {
		return fmt.Errorf("unexpected status %d", status)
	}
	return nil
}

func replaceRecords(ctx context.Context, baseURL, schemaName string, payload []byte) error {
	u := strings.TrimRight(baseURL, "/") + "/records/" + url.PathEscape(schemaName) + "?mode=replace"
	_, status, err := request(ctx, http.MethodPut, u, "application/x-scrt", payload)
	if err != nil {
		return err
	}
	if status != http.StatusNoContent {
		return fmt.Errorf("unexpected status %d", status)
	}
	return nil
}

func fetchRecords(ctx context.Context, baseURL, schemaName string) ([]byte, error) {
	u := strings.TrimRight(baseURL, "/") + "/records/" + url.PathEscape(schemaName)
	body, status, err := request(ctx, http.MethodGet, u, "", nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", status)
	}
	return body, nil
}

func patchRow(ctx context.Context, baseURL, schemaName, keyField, key string, payload []byte) (*rowEnvelope, error) {
	u := strings.TrimRight(baseURL, "/") + "/records/" + url.PathEscape(schemaName) + "/row/" + url.PathEscape(keyField) + "/" + url.PathEscape(key)
	body, status, err := request(ctx, http.MethodPatch, u, "application/x-scrt", payload)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d: %s", status, strings.TrimSpace(string(body)))
	}
	var env rowEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("decode row envelope: %w", err)
	}
	return &env, nil
}

func getRow(ctx context.Context, baseURL, schemaName, keyField, key string) (*rowEnvelope, int, error) {
	u := strings.TrimRight(baseURL, "/") + "/records/" + url.PathEscape(schemaName) + "/row/" + url.PathEscape(keyField) + "/" + url.PathEscape(key)
	body, status, err := request(ctx, http.MethodGet, u, "", nil)
	if err != nil {
		return nil, status, err
	}
	if status == http.StatusNotFound {
		return nil, status, nil
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("unexpected status %d: %s", status, strings.TrimSpace(string(body)))
	}
	var env rowEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, status, fmt.Errorf("decode row envelope: %w", err)
	}
	return &env, status, nil
}

func deleteRow(ctx context.Context, baseURL, schemaName, keyField, key string) error {
	u := strings.TrimRight(baseURL, "/") + "/records/" + url.PathEscape(schemaName) + "/row/" + url.PathEscape(keyField) + "/" + url.PathEscape(key)
	body, status, err := request(ctx, http.MethodDelete, u, "", nil)
	if err != nil {
		return err
	}
	if status != http.StatusNoContent {
		return fmt.Errorf("unexpected status %d: %s", status, strings.TrimSpace(string(body)))
	}
	return nil
}

func request(ctx context.Context, method, urlStr, contentType string, body []byte) ([]byte, int, error) {
	var r io.Reader
	if len(body) > 0 {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, urlStr, r)
	if err != nil {
		return nil, 0, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return b, resp.StatusCode, nil
}

func renderSchemaDSL(s *schema.Schema) string {
	var b strings.Builder
	fmt.Fprintf(&b, "@schema:%s\n", s.Name)
	for _, f := range s.Fields {
		fmt.Fprintf(&b, "@field %s %s", f.Name, f.RawType)
		if f.AutoIncrement {
			b.WriteString(" serial")
		}
		if f.PrimaryKey {
			b.WriteString(" pk")
		}
		if f.Unique {
			b.WriteString(" unique")
		}
		if f.Nullable && !strings.HasPrefix(f.RawType, "?") {
			b.WriteString(" nullable")
		}
		b.WriteString("\n")
	}
	for _, idx := range s.Indexes {
		fmt.Fprintf(&b, "@index:%s(%s)", idx.Name, strings.Join(idx.Fields, ", "))
		if idx.Unique {
			b.WriteString(" unique")
		}
		b.WriteString("\n")
	}
	return b.String()
}

func dependencyOrder(doc *schema.Document) ([]string, error) {
	names := make([]string, 0, len(doc.Schemas))
	for name := range doc.Schemas {
		names = append(names, name)
	}
	sort.Strings(names)

	seen := map[string]bool{}
	stack := map[string]bool{}
	out := make([]string, 0, len(names))

	var visit func(string) error
	visit = func(name string) error {
		if seen[name] {
			return nil
		}
		if stack[name] {
			return fmt.Errorf("cycle detected around schema %s", name)
		}
		stack[name] = true
		s := doc.Schemas[name]
		for _, f := range s.Fields {
			if f.Kind != schema.KindRef || f.TargetSchema == "" {
				continue
			}
			if _, ok := doc.Schemas[f.TargetSchema]; !ok {
				return fmt.Errorf("schema %s references missing schema %s", name, f.TargetSchema)
			}
			if err := visit(f.TargetSchema); err != nil {
				return err
			}
		}
		stack[name] = false
		seen[name] = true
		out = append(out, name)
		return nil
	}

	for _, name := range names {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func findRowByKey(rows []map[string]any, keyField, wanted string) (map[string]any, bool) {
	for _, row := range rows {
		if fmt.Sprint(row[keyField]) == wanted {
			clone := make(map[string]any, len(row))
			for k, v := range row {
				clone[k] = v
			}
			return clone, true
		}
	}
	return nil, false
}

func mutateRow(row map[string]any) {
	if row == nil {
		return
	}
	if raw, ok := row["Quantity"]; ok {
		switch v := raw.(type) {
		case int:
			row["Quantity"] = v + 1
		case int64:
			row["Quantity"] = v + 1
		case uint64:
			row["Quantity"] = v + 1
		case float64:
			row["Quantity"] = int64(v) + 1
		}
	}
	if raw, ok := row["Subtotal"]; ok {
		switch v := raw.(type) {
		case float64:
			row["Subtotal"] = v + 10
		case int:
			row["Subtotal"] = float64(v) + 10
		case int64:
			row["Subtotal"] = float64(v) + 10
		case uint64:
			row["Subtotal"] = float64(v) + 10
		}
	}
	if raw, ok := row["TotalAmount"]; ok {
		switch v := raw.(type) {
		case float64:
			row["TotalAmount"] = v + 10
		case int:
			row["TotalAmount"] = float64(v) + 10
		case int64:
			row["TotalAmount"] = float64(v) + 10
		case uint64:
			row["TotalAmount"] = float64(v) + 10
		}
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
