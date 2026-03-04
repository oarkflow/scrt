package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/oarkflow/scrt/codec"
	"github.com/oarkflow/scrt/schema"
)

// FullTextIndex stores token -> rowID postings for string-like fields.
type FullTextIndex struct {
	mu       sync.RWMutex
	postings map[string]map[uint64]struct{}
	rowTerms map[uint64][]string
}

func NewFullTextIndex() *FullTextIndex {
	return &FullTextIndex{
		postings: make(map[string]map[uint64]struct{}),
		rowTerms: make(map[uint64][]string),
	}
}

func BuildFullTextIndex(sch *schema.Schema, payload []byte) (*FullTextIndex, error) {
	idx := NewFullTextIndex()
	if len(payload) == 0 {
		return idx, nil
	}
	reader := codec.NewReader(bytes.NewReader(payload), sch)
	row := codec.NewRow(sch)
	var rowID uint64
	for {
		ok, err := reader.ReadRow(row)
		if errors.Is(err, io.EOF) || !ok {
			break
		}
		if err != nil {
			return nil, err
		}
		idx.AddRow(rowID, sch, row)
		rowID++
	}
	return idx, nil
}

func (f *FullTextIndex) AddRow(rowID uint64, sch *schema.Schema, row codec.Row) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	terms := f.rowTerms[rowID]
	for _, token := range terms {
		bucket := f.postings[token]
		if bucket == nil {
			continue
		}
		delete(bucket, rowID)
		if len(bucket) == 0 {
			delete(f.postings, token)
		}
	}
	delete(f.rowTerms, rowID)

	seen := make(map[string]struct{})
	for _, token := range rowTokens(sch, row) {
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		bucket := f.postings[token]
		if bucket == nil {
			bucket = make(map[uint64]struct{})
			f.postings[token] = bucket
		}
		bucket[rowID] = struct{}{}
	}
	uniqueTerms := make([]string, 0, len(seen))
	for token := range seen {
		uniqueTerms = append(uniqueTerms, token)
	}
	sort.Strings(uniqueTerms)
	if len(uniqueTerms) > 0 {
		f.rowTerms[rowID] = uniqueTerms
	}
}

func (f *FullTextIndex) RemoveRow(rowID uint64) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	terms := f.rowTerms[rowID]
	for _, token := range terms {
		bucket := f.postings[token]
		if bucket == nil {
			continue
		}
		delete(bucket, rowID)
		if len(bucket) == 0 {
			delete(f.postings, token)
		}
	}
	delete(f.rowTerms, rowID)
}

func (f *FullTextIndex) Search(query string, limit int) []uint64 {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	tokens := tokenize(query)
	if len(tokens) == 0 {
		return nil
	}
	unique := make([]string, 0, len(tokens))
	seen := make(map[string]struct{})
	for _, token := range tokens {
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		unique = append(unique, token)
	}
	if len(unique) == 0 {
		return nil
	}
	base := f.postings[unique[0]]
	if len(base) == 0 {
		return nil
	}
	candidates := make([]uint64, 0, len(base))
	for rowID := range base {
		candidates = append(candidates, rowID)
	}
	for i := 1; i < len(unique); i++ {
		bucket := f.postings[unique[i]]
		if len(bucket) == 0 {
			return nil
		}
		next := candidates[:0]
		for _, rowID := range candidates {
			if _, ok := bucket[rowID]; ok {
				next = append(next, rowID)
			}
		}
		candidates = next
		if len(candidates) == 0 {
			return nil
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] < candidates[j] })
	if limit > 0 && len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return append([]uint64(nil), candidates...)
}

type fullTextDisk struct {
	Postings map[string][]uint64 `json:"postings"`
	RowTerms map[string][]string `json:"rowTerms"`
}

func (f *FullTextIndex) MarshalJSON() ([]byte, error) {
	if f == nil {
		return json.Marshal(fullTextDisk{Postings: map[string][]uint64{}, RowTerms: map[string][]string{}})
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	disk := fullTextDisk{
		Postings: make(map[string][]uint64, len(f.postings)),
		RowTerms: make(map[string][]string, len(f.rowTerms)),
	}
	for token, bucket := range f.postings {
		rows := make([]uint64, 0, len(bucket))
		for rowID := range bucket {
			rows = append(rows, rowID)
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i] < rows[j] })
		disk.Postings[token] = rows
	}
	for rowID, terms := range f.rowTerms {
		copyTerms := append([]string(nil), terms...)
		disk.RowTerms[strconvUint(rowID)] = copyTerms
	}
	return json.Marshal(disk)
}

func (f *FullTextIndex) UnmarshalJSON(data []byte) error {
	if f == nil {
		return nil
	}
	var disk fullTextDisk
	if err := json.Unmarshal(data, &disk); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.postings = make(map[string]map[uint64]struct{}, len(disk.Postings))
	for token, rows := range disk.Postings {
		bucket := make(map[uint64]struct{}, len(rows))
		for _, rowID := range rows {
			bucket[rowID] = struct{}{}
		}
		f.postings[token] = bucket
	}
	f.rowTerms = make(map[uint64][]string, len(disk.RowTerms))
	for rowKey, terms := range disk.RowTerms {
		rowID, ok := parseUint(rowKey)
		if !ok {
			continue
		}
		f.rowTerms[rowID] = append([]string(nil), terms...)
	}
	return nil
}

func rowTokens(sch *schema.Schema, row codec.Row) []string {
	values := row.Values()
	out := make([]string, 0, 8)
	for idx, field := range sch.Fields {
		kind := field.ValueKind()
		if kind != schema.KindString && kind != schema.KindTimestampTZ {
			continue
		}
		val := values[idx]
		if !val.Set || strings.TrimSpace(val.Str) == "" {
			continue
		}
		out = append(out, tokenize(val.Str)...)
	}
	return out
}

func tokenize(text string) []string {
	text = strings.ToLower(text)
	var tokens []string
	var b strings.Builder
	flush := func() {
		if b.Len() == 0 {
			return
		}
		tokens = append(tokens, b.String())
		b.Reset()
	}
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		} else {
			flush()
		}
	}
	flush()
	return tokens
}
