package storage

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/oarkflow/scrt"
	"github.com/oarkflow/scrt/codec"
	"github.com/oarkflow/scrt/schema"
)

type benchUserRecord struct {
	ID    uint64
	Email string
	Name  string
	Bio   string
}

func benchmarkUserSchema() *schema.Schema {
	return &schema.Schema{
		Name: "User",
		Fields: []schema.Field{
			{Name: "ID", Kind: schema.KindUint64, RawType: "uint64", AutoIncrement: true},
			{Name: "Email", Kind: schema.KindString, RawType: "string"},
			{Name: "Name", Kind: schema.KindString, RawType: "string"},
			{Name: "Bio", Kind: schema.KindString, RawType: "string"},
		},
	}
}

func setupBenchmarkBackend(b *testing.B, rows int) (*SnapshotBackend, *schema.Schema) {
	b.Helper()
	root := b.TempDir()
	backend, err := NewSnapshotBackend(filepath.Join(root, "bench-store"))
	if err != nil {
		b.Fatal(err)
	}
	sch := benchmarkUserSchema()

	records := make([]benchUserRecord, 0, rows)
	for i := 1; i <= rows; i++ {
		records = append(records, benchUserRecord{
			ID:    uint64(i),
			Email: fmt.Sprintf("user-%d@example.com", i),
			Name:  fmt.Sprintf("User %d", i),
			Bio:   fmt.Sprintf("profile %d with fulltext tokens storage query benchmark", i),
		})
	}
	payload, err := scrt.Marshal(sch, records)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := backend.Persist(sch.Name, sch, payload, PersistOptions{Indexes: []IndexSpec{{Field: "ID", Unique: true}, {Field: "Email", Unique: false}}}); err != nil {
		b.Fatal(err)
	}
	return backend, sch
}

func BenchmarkSnapshotStoreLookupByUintParallel(b *testing.B) {
	const rows = 20000
	backend, sch := setupBenchmarkBackend(b, rows)

	var seq uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		row := codec.NewRow(sch)
		for pb.Next() {
			n := atomic.AddUint64(&seq, 1)
			id := (n % rows) + 1
			found, err := backend.LookupByUint(sch.Name, sch, "ID", id, row)
			if err != nil {
				b.Fatal(err)
			}
			if !found {
				b.Fatalf("row %d not found", id)
			}
		}
	})
}

func BenchmarkSnapshotStoreReplaceByUintKeyParallel(b *testing.B) {
	const rows = 20000
	backend, sch := setupBenchmarkBackend(b, rows)

	replacementByID := make([][]byte, rows+1)
	for i := 1; i <= rows; i++ {
		data, err := scrt.Marshal(sch, []benchUserRecord{{
			ID:    uint64(i),
			Email: fmt.Sprintf("user-%d@example.com", i),
			Name:  fmt.Sprintf("User %d", i),
			Bio:   fmt.Sprintf("updated profile %d with fulltext tokens storage query benchmark", i),
		}})
		if err != nil {
			b.Fatal(err)
		}
		replacementByID[i] = data
	}

	var seq uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddUint64(&seq, 1)
			id := int((n % rows) + 1)
			ok, err := backend.ReplaceByUintKey(sch.Name, sch, "ID", uint64(id), replacementByID[id])
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				b.Fatalf("replace missed id %d", id)
			}
		}
	})
}

func BenchmarkSnapshotStoreSearchFullTextParallel(b *testing.B) {
	const rows = 20000
	backend, sch := setupBenchmarkBackend(b, rows)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hits, err := backend.SearchFullText(sch.Name, sch, "fulltext benchmark", 20)
			if err != nil {
				b.Fatal(err)
			}
			if len(hits) == 0 {
				b.Fatal("expected fulltext results")
			}
		}
	})
}

func BenchmarkSnapshotStoreNextAutoValueParallel(b *testing.B) {
	const rows = 20000
	backend, sch := setupBenchmarkBackend(b, rows)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			next, err := backend.NextAutoValue(sch.Name, sch, "ID")
			if err != nil {
				b.Fatal(err)
			}
			if next == 0 {
				b.Fatal("next auto value must be non-zero")
			}
		}
	})
}
