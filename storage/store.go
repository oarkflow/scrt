package storage

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/scrt/codec"
	"github.com/oarkflow/scrt/schema"
)

// SnapshotStore writes SCRT payloads + indexes to disk and serves random access lookups.
type SnapshotStore struct {
	root         string
	mu           sync.RWMutex
	rowIndexes   map[string]*RowIndex
	colIndexes   map[string]map[string]*ColumnIndex
	autoCounters map[string]map[string]uint64
}

// PersistOptions configures how a snapshot should be stored.
type PersistOptions struct {
	Indexes []IndexSpec
}

// SnapshotMeta captures the metadata persisted alongside each snapshot.
type SnapshotMeta struct {
	SchemaName   string            `json:"schemaName"`
	Fingerprint  uint64            `json:"fingerprint"`
	UpdatedAt    time.Time         `json:"updatedAt"`
	RowCount     uint64            `json:"rowCount"`
	PayloadPath  string            `json:"payloadPath"`
	RowIndex     string            `json:"rowIndex"`
	Indexes      []IndexDescriptor `json:"indexes"`
	AutoCounters map[string]uint64 `json:"autoCounters,omitempty"`
}

// IndexDescriptor describes a single column index on disk.
type IndexDescriptor struct {
	Field  string `json:"field"`
	Path   string `json:"path"`
	Unique bool   `json:"unique"`
	Kind   string `json:"kind"`
}

// AutoIndexSpecs derives index specifications (auto-increment fields, etc.).
func AutoIndexSpecs(sch *schema.Schema) []IndexSpec {
	specs := make([]IndexSpec, 0, len(sch.Fields))
	seen := make(map[string]struct{})
	for _, field := range sch.Fields {
		if field.AutoIncrement {
			if _, ok := seen[field.Name]; !ok {
				specs = append(specs, IndexSpec{Field: field.Name, Unique: true})
				seen[field.Name] = struct{}{}
			}
			continue
		}
		if field.HasAttribute("unique") || field.HasAttribute("uuid") || field.HasAttribute("uuidv7") {
			if _, ok := seen[field.Name]; !ok {
				specs = append(specs, IndexSpec{Field: field.Name, Unique: true})
				seen[field.Name] = struct{}{}
			}
		}
	}
	return specs
}

// NewSnapshotStore ensures root exists and returns a store handle.
func NewSnapshotStore(root string) (*SnapshotStore, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &SnapshotStore{
		root:         root,
		rowIndexes:   make(map[string]*RowIndex),
		colIndexes:   make(map[string]map[string]*ColumnIndex),
		autoCounters: make(map[string]map[string]uint64),
	}, nil
}

// Persist writes payload + row indexes + configured column indexes atomically.
func (s *SnapshotStore) Persist(schemaName string, sch *schema.Schema, payload []byte, opts PersistOptions) (*SnapshotMeta, error) {
	if schemaName == "" {
		return nil, fmt.Errorf("storage: schema name required")
	}
	if sch == nil {
		return nil, fmt.Errorf("storage: schema handle is nil")
	}
	if sch.Name != schemaName {
		return nil, fmt.Errorf("storage: schema mismatch: %s vs %s", sch.Name, schemaName)
	}
	schemaDir := filepath.Join(s.root, schemaName)
	if err := os.MkdirAll(schemaDir, 0o755); err != nil {
		return nil, err
	}
	payloadPath := filepath.Join(schemaDir, "payload.scrt")
	if err := atomicWrite(payloadPath, payload); err != nil {
		return nil, err
	}
	rowIndex, err := BuildRowIndex(payload)
	if err != nil {
		return nil, err
	}
	rowIndexPath := filepath.Join(schemaDir, "row.idx")
	if err := writeRowIndexFile(rowIndexPath, rowIndex); err != nil {
		return nil, err
	}
	idxMeta := make([]IndexDescriptor, 0, len(opts.Indexes))
	var columnIndexes map[string]*ColumnIndex
	if len(opts.Indexes) > 0 {
		columnIndexes, err = buildColumnIndexes(sch, payload, opts.Indexes)
		if err != nil {
			return nil, err
		}
		for field, index := range columnIndexes {
			fileName := fmt.Sprintf("idx_%s.bin", sanitize(field))
			idxPath := filepath.Join(schemaDir, fileName)
			if err := writeColumnIndexFile(idxPath, index); err != nil {
				return nil, err
			}
			idxMeta = append(idxMeta, IndexDescriptor{
				Field:  field,
				Path:   fileName,
				Unique: index.Unique,
				Kind:   fieldKindLabel(index.Kind),
			})
			s.cacheColumnIndex(schemaName, field, index)
		}
	}
	if len(idxMeta) > 1 {
		sort.Slice(idxMeta, func(i, j int) bool {
			return idxMeta[i].Field < idxMeta[j].Field
		})
	}
	autoCounters := computeAutoCounters(sch, columnIndexes, rowIndex)
	meta := &SnapshotMeta{
		SchemaName:   schemaName,
		Fingerprint:  sch.Fingerprint(),
		UpdatedAt:    time.Now().UTC(),
		RowCount:     rowIndex.RowCount(),
		PayloadPath:  "payload.scrt",
		RowIndex:     "row.idx",
		Indexes:      idxMeta,
		AutoCounters: autoCounters,
	}
	if err := writeMetaFile(filepath.Join(schemaDir, "meta.json"), meta); err != nil {
		return nil, err
	}
	if err := s.saveCounters(schemaName, autoCounters); err != nil {
		return nil, err
	}
	s.cacheRowIndex(schemaName, rowIndex)
	s.cacheAutoCounters(schemaName, autoCounters)
	return meta, nil
}

// LoadMeta reads the on-disk metadata for schemaName.
func (s *SnapshotStore) LoadMeta(schemaName string) (*SnapshotMeta, error) {
	metaPath := filepath.Join(s.root, schemaName, "meta.json")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, err
	}
	meta := &SnapshotMeta{}
	if err := json.Unmarshal(data, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// ListMeta enumerates all schema metadata files.
func (s *SnapshotStore) ListMeta() ([]*SnapshotMeta, error) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	metas := make([]*SnapshotMeta, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		meta, err := s.LoadMeta(entry.Name())
		if err != nil {
			continue
		}
		metas = append(metas, meta)
	}
	if len(metas) > 1 {
		sort.Slice(metas, func(i, j int) bool {
			return metas[i].SchemaName < metas[j].SchemaName
		})
	}
	return metas, nil
}

// LookupRow decodes the row identified by rowID into dst.
func (s *SnapshotStore) LookupRow(schemaName string, sch *schema.Schema, rowID uint64, dst codec.Row) error {
	rowIndex, err := s.rowIndex(schemaName)
	if err != nil {
		return err
	}
	locator, ok := rowIndex.Lookup(rowID)
	if !ok {
		return fmt.Errorf("storage: row %d out of range", rowID)
	}
	payloadPath := filepath.Join(s.root, schemaName, "payload.scrt")
	pageChunk, err := readPageChunk(payloadPath, locator.PageOffset)
	if err != nil {
		return err
	}
	return decodeRowFromChunk(sch, pageChunk, int(locator.RowInPage), dst)
}

// LookupByUint resolves a numeric key via a column index and decodes the matching row.
func (s *SnapshotStore) LookupByUint(schemaName string, sch *schema.Schema, field string, key uint64, dst codec.Row) (bool, error) {
	idx, err := s.columnIndex(schemaName, field)
	if err != nil {
		return false, err
	}
	if idx == nil {
		return false, fmt.Errorf("storage: field %s is not indexed", field)
	}
	rowID, ok := idx.LookupUint(key)
	if !ok {
		return false, nil
	}
	if err := s.LookupRow(schemaName, sch, rowID, dst); err != nil {
		return false, err
	}
	return true, nil
}

// LookupByString resolves a string key via a column index.
func (s *SnapshotStore) LookupByString(schemaName string, sch *schema.Schema, field, key string, dst codec.Row) (bool, error) {
	idx, err := s.columnIndex(schemaName, field)
	if err != nil {
		return false, err
	}
	if idx == nil {
		return false, fmt.Errorf("storage: field %s is not indexed", field)
	}
	rowID, ok := idx.LookupString(key)
	if !ok {
		return false, nil
	}
	if err := s.LookupRow(schemaName, sch, rowID, dst); err != nil {
		return false, err
	}
	return true, nil
}

func (s *SnapshotStore) rowIndex(schemaName string) (*RowIndex, error) {
	s.mu.RLock()
	idx, ok := s.rowIndexes[schemaName]
	s.mu.RUnlock()
	if ok {
		return idx, nil
	}
	path := filepath.Join(s.root, schemaName, "row.idx")
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	loaded, err := ReadRowIndex(bufio.NewReader(file))
	if err != nil {
		return nil, err
	}
	s.cacheRowIndex(schemaName, loaded)
	return loaded, nil
}

func (s *SnapshotStore) columnIndex(schemaName, field string) (*ColumnIndex, error) {
	s.mu.RLock()
	fields := s.colIndexes[schemaName]
	if fields != nil {
		if idx, ok := fields[field]; ok {
			s.mu.RUnlock()
			return idx, nil
		}
	}
	s.mu.RUnlock()
	meta, err := s.LoadMeta(schemaName)
	if err != nil {
		return nil, err
	}
	var entry *IndexDescriptor
	for i := range meta.Indexes {
		if strings.EqualFold(meta.Indexes[i].Field, field) {
			entry = &meta.Indexes[i]
			break
		}
	}
	if entry == nil {
		return nil, nil
	}
	path := filepath.Join(s.root, schemaName, entry.Path)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	idx, err := LoadColumnIndex(bufio.NewReader(file))
	if err != nil {
		return nil, err
	}
	s.cacheColumnIndex(schemaName, entry.Field, idx)
	return idx, nil
}

// LoadPayload reads the raw SCRT payload for schemaName from disk.
func (s *SnapshotStore) LoadPayload(schemaName string) ([]byte, error) {
	path := filepath.Join(s.root, schemaName, "payload.scrt")
	return os.ReadFile(path)
}

// Delete removes the schema directory and cached indexes.
func (s *SnapshotStore) Delete(schemaName string) error {
	s.mu.Lock()
	delete(s.rowIndexes, schemaName)
	delete(s.colIndexes, schemaName)
	delete(s.autoCounters, schemaName)
	s.mu.Unlock()
	return os.RemoveAll(filepath.Join(s.root, schemaName))
}

// NextAutoValue returns the next sequential value for the given field.
func (s *SnapshotStore) NextAutoValue(schemaName string, sch *schema.Schema, field string) (uint64, error) {
	counters, err := s.ensureAutoCounters(schemaName, sch)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	local := s.autoCounters[schemaName]
	if local == nil {
		local = make(map[string]uint64, len(counters))
		for k, v := range counters {
			local[k] = v
		}
		s.autoCounters[schemaName] = local
	}
	value := local[field]
	if value == 0 {
		value = 1
	}
	local[field] = value + 1
	snapshot := copyCounterMap(local)
	s.mu.Unlock()
	if err := s.saveCounters(schemaName, snapshot); err != nil {
		return 0, err
	}
	return value, nil
}

func (s *SnapshotStore) cacheRowIndex(schemaName string, idx *RowIndex) {
	s.mu.Lock()
	s.rowIndexes[schemaName] = idx
	s.mu.Unlock()
}

func (s *SnapshotStore) cacheColumnIndex(schemaName, field string, idx *ColumnIndex) {
	s.mu.Lock()
	fieldMap, ok := s.colIndexes[schemaName]
	if !ok {
		fieldMap = make(map[string]*ColumnIndex)
		s.colIndexes[schemaName] = fieldMap
	}
	fieldMap[field] = idx
	s.mu.Unlock()
}

func (s *SnapshotStore) cacheAutoCounters(schemaName string, counters map[string]uint64) {
	s.mu.Lock()
	if counters == nil {
		delete(s.autoCounters, schemaName)
	} else {
		clone := make(map[string]uint64, len(counters))
		for key, val := range counters {
			clone[key] = val
		}
		s.autoCounters[schemaName] = clone
	}
	s.mu.Unlock()
}

func (s *SnapshotStore) ensureAutoCounters(schemaName string, sch *schema.Schema) (map[string]uint64, error) {
	s.mu.RLock()
	if counters, ok := s.autoCounters[schemaName]; ok {
		clone := copyCounterMap(counters)
		s.mu.RUnlock()
		return clone, nil
	}
	s.mu.RUnlock()
	counters, err := s.loadCounters(schemaName)
	if err == nil {
		s.cacheAutoCounters(schemaName, counters)
		return copyCounterMap(counters), nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	rebuilt, err := s.rebuildAutoCounters(schemaName, sch)
	if err != nil {
		return nil, err
	}
	s.cacheAutoCounters(schemaName, rebuilt)
	return copyCounterMap(rebuilt), nil
}

func (s *SnapshotStore) rebuildAutoCounters(schemaName string, sch *schema.Schema) (map[string]uint64, error) {
	payload, err := s.LoadPayload(schemaName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]uint64{}, nil
		}
		return nil, err
	}
	rowIndex, err := BuildRowIndex(payload)
	if err != nil {
		return nil, err
	}
	indices, err := buildColumnIndexes(sch, payload, AutoIndexSpecs(sch))
	if err != nil {
		return nil, err
	}
	counters := computeAutoCounters(sch, indices, rowIndex)
	if err := s.saveCounters(schemaName, counters); err != nil {
		return nil, err
	}
	return counters, nil
}

func computeAutoCounters(sch *schema.Schema, indexes map[string]*ColumnIndex, rowIndex *RowIndex) map[string]uint64 {
	counters := make(map[string]uint64)
	if rowIndex == nil {
		rowIndex = &RowIndex{}
	}
	for _, field := range sch.Fields {
		if !field.AutoIncrement {
			continue
		}
		var next uint64 = 1
		if indexes != nil {
			if idx, ok := indexes[field.Name]; ok && idx != nil {
				if max, ok := idx.MaxKey(); ok {
					next = max + 1
				} else {
					next = 1
				}
				counters[field.Name] = next
				continue
			}
		}
		if rc := rowIndex.RowCount(); rc > 0 {
			next = rc + 1
		}
		counters[field.Name] = next
	}
	return counters
}

func (s *SnapshotStore) saveCounters(schemaName string, counters map[string]uint64) error {
	if counters == nil || len(counters) == 0 {
		if err := os.Remove(s.countersPath(schemaName)); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	data, err := json.MarshalIndent(counters, "", "  ")
	if err != nil {
		return err
	}
	return atomicWrite(s.countersPath(schemaName), data)
}

func (s *SnapshotStore) loadCounters(schemaName string) (map[string]uint64, error) {
	data, err := os.ReadFile(s.countersPath(schemaName))
	if err != nil {
		return nil, err
	}
	var counters map[string]uint64
	if err := json.Unmarshal(data, &counters); err != nil {
		return nil, err
	}
	return counters, nil
}

func (s *SnapshotStore) countersPath(schemaName string) string {
	return filepath.Join(s.root, schemaName, "counters.json")
}

func copyCounterMap(src map[string]uint64) map[string]uint64 {
	clone := make(map[string]uint64, len(src))
	for k, v := range src {
		clone[k] = v
	}
	return clone
}

func decodeRowFromChunk(sch *schema.Schema, chunk []byte, rowInPage int, dst codec.Row) error {
	var buf bytes.Buffer
	buf.Grow(4 + 1 + 8 + len(chunk))
	buf.WriteString("SCRT")
	buf.WriteByte(byte(2))
	var fp [8]byte
	binary.LittleEndian.PutUint64(fp[:], sch.Fingerprint())
	buf.Write(fp[:])
	buf.Write(chunk)
	reader := codec.NewReader(bufio.NewReader(&buf), sch)
	for i := 0; i <= rowInPage; i++ {
		ok, err := reader.ReadRow(dst)
		if err != nil {
			return err
		}
		if !ok {
			return io.EOF
		}
	}
	return nil
}

func readPageChunk(path string, pageOffset uint64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if _, err := file.Seek(int64(pageOffset), io.SeekStart); err != nil {
		return nil, err
	}
	varintBuf := make([]byte, binary.MaxVarintLen64)
	var consumed int
	for consumed < len(varintBuf) {
		if _, err := file.Read(varintBuf[consumed : consumed+1]); err != nil {
			return nil, err
		}
		if varintBuf[consumed]&0x80 == 0 {
			consumed++
			break
		}
		consumed++
	}
	length, n := binary.Uvarint(varintBuf[:consumed])
	if n <= 0 {
		return nil, fmt.Errorf("storage: malformed varint at offset %d", pageOffset)
	}
	chunk := make([]byte, consumed+int(length))
	copy(chunk[:consumed], varintBuf[:consumed])
	if _, err := io.ReadFull(file, chunk[consumed:]); err != nil {
		return nil, err
	}
	return chunk, nil
}

func writeRowIndexFile(path string, idx *RowIndex) error {
	var buf bytes.Buffer
	if err := idx.WriteTo(&buf); err != nil {
		return err
	}
	return atomicWrite(path, buf.Bytes())
}

func writeColumnIndexFile(path string, idx *ColumnIndex) error {
	var buf bytes.Buffer
	if err := idx.Persist(&buf); err != nil {
		return err
	}
	return atomicWrite(path, buf.Bytes())
}

func writeMetaFile(path string, meta *SnapshotMeta) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return atomicWrite(path, data)
}

func atomicWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(name)
		return err
	}
	return os.Rename(name, path)
}

func sanitize(field string) string {
	clean := strings.ToLower(field)
	clean = strings.ReplaceAll(clean, " ", "_")
	clean = strings.ReplaceAll(clean, string(os.PathSeparator), "_")
	return clean
}

func fieldKindLabel(kind schema.FieldKind) string {
	switch kind {
	case schema.KindUint64:
		return "uint64"
	case schema.KindString:
		return "string"
	case schema.KindRef:
		return "ref"
	case schema.KindBool:
		return "bool"
	case schema.KindInt64:
		return "int64"
	case schema.KindFloat64:
		return "float64"
	case schema.KindBytes:
		return "bytes"
	case schema.KindDate:
		return "date"
	case schema.KindDateTime:
		return "datetime"
	case schema.KindTimestamp:
		return "timestamp"
	case schema.KindTimestampTZ:
		return "timestamptz"
	case schema.KindDuration:
		return "duration"
	default:
		return fmt.Sprintf("kind_%d", int(kind))
	}
}

// GenerateUUIDv7 emits a RFC 9562 compliant UUID version 7 string.
func GenerateUUIDv7() (string, error) {
	var uuid [16]byte
	ts := time.Now().UnixMilli()
	uuid[0] = byte(ts >> 40)
	uuid[1] = byte(ts >> 32)
	uuid[2] = byte(ts >> 24)
	uuid[3] = byte(ts >> 16)
	uuid[4] = byte(ts >> 8)
	uuid[5] = byte(ts)
	if _, err := rand.Read(uuid[6:]); err != nil {
		return "", err
	}
	uuid[6] &= 0x0F
	uuid[6] |= 0x70 // version 7
	uuid[8] &= 0x3F
	uuid[8] |= 0x80 // variant RFC4122
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16]), nil
}
