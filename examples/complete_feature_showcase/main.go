package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oarkflow/scrt/schema"
)

func main() {
	scrtPath := flag.String("scrt", "examples/complete_feature_showcase/all_reference_cases.scrt", "path to SCRT file")
	flag.Parse()

	path := resolvePath(*scrtPath)
	doc, err := schema.ParseFile(path)
	if err != nil {
		fail("parse %s: %v", path, err)
	}
	if err := doc.ValidateData(); err != nil {
		fail("validate data: %v", err)
	}

	fmt.Printf("Loaded %s\n", path)
	printSummary(doc)

	fmt.Println("\nCRUD demo on schema: OrderItem")

	createRow := map[string]any{
		"OrderID":    uint64(1001),
		"ProductSKU": "SKU-BLUE-2",
		"Quantity":   int64(3),
		"UnitPrice":  39.50,
		"Subtotal":   118.50,
	}
	created, err := insertRow(doc, "OrderItem", createRow)
	if err != nil {
		fail("create: %v", err)
	}
	fmt.Printf("CREATE -> %+v\n", created)

	updated, err := updateRow(doc, "OrderItem", "ItemID", created["ItemID"], map[string]any{
		"Quantity":  int64(4),
		"Subtotal":  158.00,
		"UpdatedAt": time.Now().UTC(),
	})
	if err != nil {
		fail("update: %v", err)
	}
	fmt.Printf("UPDATE -> %+v\n", updated)

	found, ok := getRow(doc, "OrderItem", "ItemID", created["ItemID"])
	if !ok {
		fail("read: created row not found")
	}
	fmt.Printf("READ -> %+v\n", found)

	if err := deleteRow(doc, "OrderItem", "ItemID", created["ItemID"]); err != nil {
		fail("delete: %v", err)
	}
	fmt.Printf("DELETE -> ItemID=%v\n", created["ItemID"])

	if _, ok := getRow(doc, "OrderItem", "ItemID", created["ItemID"]); ok {
		fail("delete verify: row still exists")
	}
	fmt.Println("DELETE verification passed")
}

func printSummary(doc *schema.Document) {
	names := make([]string, 0, len(doc.Schemas))
	for name := range doc.Schemas {
		names = append(names, name)
	}
	sortStrings(names)
	for _, name := range names {
		sch := doc.Schemas[name]
		fmt.Printf("\n[%s]\n", sch.Name)
		for _, f := range sch.Fields {
			parts := []string{f.RawType}
			if f.PrimaryKey {
				parts = append(parts, "pk")
			}
			if f.AutoIncrement {
				parts = append(parts, "serial")
			}
			if f.ReadOnly {
				parts = append(parts, "readonly")
			}
			if f.Default != nil {
				parts = append(parts, "default:"+formatDefaultValue(f.Default))
			}
			if f.IsArray {
				parts = append(parts, "array")
			}
			if f.IsMap {
				parts = append(parts, "map")
			}
			if f.IsObject {
				parts = append(parts, "object")
			}
			fmt.Printf("  - %s (%s)\n", f.Name, strings.Join(parts, ", "))
		}
		for _, idx := range sch.Indexes {
			kind := "index"
			if idx.Unique {
				kind = "unique"
			}
			fmt.Printf("  - %s %s(%s)\n", kind, idx.Name, strings.Join(idx.Fields, ", "))
		}
		for _, rel := range sch.Relations {
			fmt.Printf("  - relation %s -> %s.%s onDelete=%s onUpdate=%s\n", rel.Field, rel.TargetSchema, rel.TargetField, rel.OnDelete, rel.OnUpdate)
		}
	}
}

func insertRow(doc *schema.Document, schemaName string, row map[string]any) (map[string]any, error) {
	sch, ok := doc.Schema(schemaName)
	if !ok {
		return nil, fmt.Errorf("schema %s not found", schemaName)
	}
	candidate := cloneMap(row)
	applyAutoValues(candidate, sch, doc.Data[schemaName])
	if err := sch.ValidateRow(candidate); err != nil {
		return nil, err
	}
	if err := ensureUniqueIndexes(sch, doc.Data[schemaName], candidate, -1); err != nil {
		return nil, err
	}
	doc.Data[schemaName] = append(doc.Data[schemaName], candidate)
	return candidate, nil
}

func getRow(doc *schema.Document, schemaName, keyField string, key any) (map[string]any, bool) {
	rows := doc.Data[schemaName]
	for _, row := range rows {
		if equal(row[keyField], key) {
			return cloneMap(row), true
		}
	}
	return nil, false
}

func updateRow(doc *schema.Document, schemaName, keyField string, key any, patch map[string]any) (map[string]any, error) {
	sch, ok := doc.Schema(schemaName)
	if !ok {
		return nil, fmt.Errorf("schema %s not found", schemaName)
	}
	rows := doc.Data[schemaName]
	for i := range rows {
		if !equal(rows[i][keyField], key) {
			continue
		}
		next := cloneMap(rows[i])
		for k, v := range patch {
			next[k] = v
		}
		if err := ensureReadOnlyUnchanged(sch, rows[i], next); err != nil {
			return nil, err
		}
		if err := sch.ValidateRow(next); err != nil {
			return nil, err
		}
		if err := ensureUniqueIndexes(sch, rows, next, i); err != nil {
			return nil, err
		}
		rows[i] = next
		doc.Data[schemaName] = rows
		return cloneMap(next), nil
	}
	return nil, fmt.Errorf("row not found for %s=%v", keyField, key)
}

func deleteRow(doc *schema.Document, schemaName, keyField string, key any) error {
	rows := doc.Data[schemaName]
	for i := range rows {
		if equal(rows[i][keyField], key) {
			doc.Data[schemaName] = append(rows[:i], rows[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("row not found for %s=%v", keyField, key)
}

func ensureReadOnlyUnchanged(sch *schema.Schema, before, after map[string]any) error {
	for _, field := range sch.Fields {
		if field.ReadOnly && !equal(before[field.Name], after[field.Name]) {
			return fmt.Errorf("field %s is readonly", field.Name)
		}
	}
	return nil
}

func ensureUniqueIndexes(sch *schema.Schema, rows []map[string]any, candidate map[string]any, ignoreIdx int) error {
	for _, idx := range sch.Indexes {
		if !idx.Unique || len(idx.Fields) == 0 {
			continue
		}
		for rowIdx, row := range rows {
			if rowIdx == ignoreIdx {
				continue
			}
			if sameOnFields(row, candidate, idx.Fields) {
				return fmt.Errorf("unique index %s conflict on fields (%s)", idx.Name, strings.Join(idx.Fields, ", "))
			}
		}
	}
	return nil
}

func applyAutoValues(row map[string]any, sch *schema.Schema, existing []map[string]any) {
	now := time.Now().UTC()
	for _, f := range sch.Fields {
		if _, ok := row[f.Name]; ok {
			continue
		}
		if f.AutoIncrement {
			row[f.Name] = nextUint64(existing, f.Name)
			continue
		}
		if f.Default == nil || !strings.EqualFold(strings.TrimSpace(f.Default.Expression), "now()") {
			continue
		}
		switch f.ValueKind() {
		case schema.KindDate:
			row[f.Name] = now.Truncate(24 * time.Hour)
		case schema.KindDateTime, schema.KindTimestamp, schema.KindTimestampTZ:
			row[f.Name] = now
		}
	}
}

func nextUint64(rows []map[string]any, field string) uint64 {
	var max uint64
	for _, row := range rows {
		switch v := row[field].(type) {
		case uint64:
			if v > max {
				max = v
			}
		case int64:
			if v > 0 && uint64(v) > max {
				max = uint64(v)
			}
		case int:
			if v > 0 && uint64(v) > max {
				max = uint64(v)
			}
		}
	}
	return max + 1
}

func sameOnFields(a, b map[string]any, fields []string) bool {
	for _, field := range fields {
		if !equal(a[field], b[field]) {
			return false
		}
	}
	return true
}

func equal(a, b any) bool {
	return fmt.Sprint(a) == fmt.Sprint(b)
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func sortStrings(values []string) {
	if len(values) < 2 {
		return
	}
	for i := 0; i < len(values)-1; i++ {
		for j := i + 1; j < len(values); j++ {
			if values[j] < values[i] {
				values[i], values[j] = values[j], values[i]
			}
		}
	}
}

func resolvePath(path string) string {
	if _, err := os.Stat(path); err == nil {
		return path
	}
	base := filepath.Base(path)
	if _, err := os.Stat(base); err == nil {
		return base
	}
	return path
}

func formatDefaultValue(d *schema.DefaultValue) string {
	if d == nil {
		return ""
	}
	if strings.TrimSpace(d.Expression) != "" {
		return d.Expression
	}
	switch d.Kind {
	case schema.KindBool:
		return fmt.Sprint(d.Bool)
	case schema.KindInt64:
		return fmt.Sprint(d.Int)
	case schema.KindUint64, schema.KindRef:
		return fmt.Sprint(d.Uint)
	case schema.KindFloat64:
		return fmt.Sprint(d.Float)
	case schema.KindString:
		return d.String
	case schema.KindBytes:
		return string(d.Bytes)
	case schema.KindDate, schema.KindDateTime, schema.KindTimestamp, schema.KindDuration:
		return fmt.Sprint(d.Int)
	case schema.KindTimestampTZ:
		return d.String
	default:
		return ""
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
