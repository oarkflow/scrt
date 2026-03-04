package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/oarkflow/scrt/codec"
	"github.com/oarkflow/scrt/schema"
)

// MutationRecord stores row-level overrides without rewriting full payloads.
type MutationRecord struct {
	Deleted     bool   `json:"deleted"`
	Replacement []byte `json:"replacement,omitempty"`
}

// MutationLog stores row mutations keyed by row ID.
type MutationLog struct {
	mu   sync.RWMutex
	Rows map[uint64]MutationRecord
}

func NewMutationLog() *MutationLog {
	return &MutationLog{Rows: make(map[uint64]MutationRecord)}
}

func (m *MutationLog) Get(rowID uint64) (MutationRecord, bool) {
	if m == nil {
		return MutationRecord{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.Rows == nil {
		return MutationRecord{}, false
	}
	rec, ok := m.Rows[rowID]
	return rec, ok
}

func (m *MutationLog) SetDelete(rowID uint64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Rows == nil {
		m.Rows = make(map[uint64]MutationRecord)
	}
	m.Rows[rowID] = MutationRecord{Deleted: true}
}

func (m *MutationLog) SetReplacement(rowID uint64, replacement []byte) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Rows == nil {
		m.Rows = make(map[uint64]MutationRecord)
	}
	m.Rows[rowID] = MutationRecord{Replacement: append([]byte(nil), replacement...)}
}

type mutationDisk struct {
	Rows map[string]MutationRecord `json:"rows"`
}

func (m *MutationLog) MarshalJSON() ([]byte, error) {
	if m == nil {
		return json.Marshal(mutationDisk{Rows: map[string]MutationRecord{}})
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	disk := mutationDisk{Rows: make(map[string]MutationRecord, len(m.Rows))}
	for rowID, rec := range m.Rows {
		disk.Rows[strconvUint(rowID)] = MutationRecord{Deleted: rec.Deleted, Replacement: append([]byte(nil), rec.Replacement...)}
	}
	return json.Marshal(disk)
}

func (m *MutationLog) UnmarshalJSON(data []byte) error {
	if m == nil {
		return nil
	}
	var disk mutationDisk
	if err := json.Unmarshal(data, &disk); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Rows = make(map[uint64]MutationRecord, len(disk.Rows))
	for rowKey, rec := range disk.Rows {
		rowID, ok := parseUint(rowKey)
		if !ok {
			continue
		}
		m.Rows[rowID] = MutationRecord{Deleted: rec.Deleted, Replacement: append([]byte(nil), rec.Replacement...)}
	}
	return nil
}

func (s *SnapshotStore) LookupRowEffective(schemaName string, sch *schema.Schema, rowID uint64, dst codec.Row) (bool, error) {
	mutations, err := s.mutationsForSchema(schemaName)
	if err != nil {
		return false, err
	}
	if rec, ok := mutations.Get(rowID); ok {
		if rec.Deleted {
			return false, nil
		}
		if len(rec.Replacement) > 0 {
			if err := decodeSingleRowPayload(rec.Replacement, sch, dst); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	if err := s.lookupRowFromBase(schemaName, sch, rowID, dst); err != nil {
		if strings.Contains(err.Error(), "out of range") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *SnapshotStore) FindRowIDByUint(schemaName string, sch *schema.Schema, field string, key uint64) (uint64, bool, error) {
	idx, err := s.columnIndex(schemaName, field)
	if err != nil {
		return 0, false, err
	}
	if idx == nil {
		return 0, false, fmt.Errorf("storage: field %s is not indexed", field)
	}
	rowID, ok := idx.LookupUint(key)
	if !ok {
		return 0, false, nil
	}
	row := codec.NewRow(sch)
	found, err := s.LookupRowEffective(schemaName, sch, rowID, row)
	if err != nil {
		return 0, false, err
	}
	if !found {
		return 0, false, nil
	}
	return rowID, true, nil
}

func (s *SnapshotStore) FindRowIDByString(schemaName string, sch *schema.Schema, field, key string) (uint64, bool, error) {
	idx, err := s.columnIndex(schemaName, field)
	if err != nil {
		return 0, false, err
	}
	if idx == nil {
		return 0, false, fmt.Errorf("storage: field %s is not indexed", field)
	}
	rowID, ok := idx.LookupString(key)
	if !ok {
		return 0, false, nil
	}
	row := codec.NewRow(sch)
	found, err := s.LookupRowEffective(schemaName, sch, rowID, row)
	if err != nil {
		return 0, false, err
	}
	if !found {
		return 0, false, nil
	}
	return rowID, true, nil
}

func (s *SnapshotStore) DeleteByUintKey(schemaName string, sch *schema.Schema, field string, key uint64) (bool, error) {
	rowID, found, err := s.FindRowIDByUint(schemaName, sch, field, key)
	if err != nil || !found {
		return found, err
	}
	return true, s.applyRowMutation(schemaName, sch, rowID, nil, true)
}

func (s *SnapshotStore) DeleteByStringKey(schemaName string, sch *schema.Schema, field, key string) (bool, error) {
	rowID, found, err := s.FindRowIDByString(schemaName, sch, field, key)
	if err != nil || !found {
		return found, err
	}
	return true, s.applyRowMutation(schemaName, sch, rowID, nil, true)
}

func (s *SnapshotStore) ReplaceByUintKey(schemaName string, sch *schema.Schema, field string, key uint64, replacement []byte) (bool, error) {
	rowID, found, err := s.FindRowIDByUint(schemaName, sch, field, key)
	if err != nil || !found {
		return found, err
	}
	return true, s.applyRowMutation(schemaName, sch, rowID, replacement, false)
}

func (s *SnapshotStore) ReplaceByStringKey(schemaName string, sch *schema.Schema, field, key string, replacement []byte) (bool, error) {
	rowID, found, err := s.FindRowIDByString(schemaName, sch, field, key)
	if err != nil || !found {
		return found, err
	}
	return true, s.applyRowMutation(schemaName, sch, rowID, replacement, false)
}

func (s *SnapshotStore) SearchFullText(schemaName string, sch *schema.Schema, query string, limit int) ([]uint64, error) {
	idx, err := s.fullTextIndex(schemaName)
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, nil
	}
	candidateIDs := idx.Search(query, limit)
	out := make([]uint64, 0, len(candidateIDs))
	row := codec.NewRow(sch)
	for _, rowID := range candidateIDs {
		found, err := s.LookupRowEffective(schemaName, sch, rowID, row)
		if err != nil {
			return nil, err
		}
		if found {
			out = append(out, rowID)
		}
	}
	return out, nil
}

func (s *SnapshotStore) applyRowMutation(schemaName string, sch *schema.Schema, rowID uint64, replacement []byte, deleted bool) error {
	meta, err := s.LoadMeta(schemaName)
	if err != nil {
		return err
	}
	oldRow := codec.NewRow(sch)
	found, err := s.LookupRowEffective(schemaName, sch, rowID, oldRow)
	if err != nil {
		return err
	}
	if !found {
		return os.ErrNotExist
	}
	var newRow codec.Row
	if !deleted {
		if len(replacement) == 0 {
			return fmt.Errorf("storage: replacement payload required")
		}
		newRow = codec.NewRow(sch)
		if err := decodeSingleRowPayload(replacement, sch, newRow); err != nil {
			return err
		}
	}
	for _, desc := range meta.Indexes {
		fieldIdx, ok := sch.FieldIndex(desc.Field)
		if !ok {
			continue
		}
		colIdx, err := s.columnIndex(schemaName, desc.Field)
		if err != nil {
			return err
		}
		if colIdx == nil {
			continue
		}
		if err := mutateColumnIndex(colIdx, oldRow, newRow, fieldIdx, rowID, deleted); err != nil {
			return err
		}
		if err := writeColumnIndexFile(filepath.Join(s.root, schemaName, desc.Path), colIdx); err != nil {
			return err
		}
		s.cacheColumnIndex(schemaName, desc.Field, colIdx)
	}
	ft, err := s.fullTextIndex(schemaName)
	if err != nil {
		return err
	}
	if ft != nil {
		ft.RemoveRow(rowID)
		if !deleted {
			ft.AddRow(rowID, sch, newRow)
		}
		if err := writeFullTextIndexFile(filepath.Join(s.root, schemaName, "fulltext.json"), ft); err != nil {
			return err
		}
		s.cacheFullText(schemaName, ft)
	}
	mutations, err := s.mutationsForSchema(schemaName)
	if err != nil {
		return err
	}
	if deleted {
		mutations.SetDelete(rowID)
	} else {
		mutations.SetReplacement(rowID, replacement)
	}
	if err := saveMutationLogFile(s.mutationPath(schemaName), mutations); err != nil {
		return err
	}
	s.cacheMutations(schemaName, mutations)
	return nil
}

func mutateColumnIndex(idx *ColumnIndex, oldRow codec.Row, newRow codec.Row, fieldIdx int, rowID uint64, deleted bool) error {
	oldVal := oldRow.Values()[fieldIdx]
	if oldVal.Set {
		switch idx.Kind {
		case schema.KindUint64, schema.KindRef:
			idx.DeleteUint(oldVal.Uint, rowID)
		case schema.KindString:
			idx.DeleteString(oldVal.Str, rowID)
		}
	}
	if deleted {
		return nil
	}
	newVal := newRow.Values()[fieldIdx]
	if !newVal.Set {
		return nil
	}
	switch idx.Kind {
	case schema.KindUint64, schema.KindRef:
		return idx.UpsertUint(newVal.Uint, rowID)
	case schema.KindString:
		return idx.UpsertString(newVal.Str, rowID)
	default:
		return nil
	}
}

func decodeSingleRowPayload(payload []byte, sch *schema.Schema, dst codec.Row) error {
	reader := codec.NewReader(bytes.NewReader(payload), sch)
	ok, err := reader.ReadRow(dst)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("storage: row payload contained no rows")
	}
	second, err := reader.ReadRow(dst)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if second {
		return fmt.Errorf("storage: row payload must contain a single row")
	}
	return nil
}

func (s *SnapshotStore) mutationPath(schemaName string) string {
	return filepath.Join(s.root, schemaName, "mutations.json")
}

func (s *SnapshotStore) mutationsForSchema(schemaName string) (*MutationLog, error) {
	s.mu.RLock()
	if log := s.mutations[schemaName]; log != nil {
		s.mu.RUnlock()
		return log, nil
	}
	s.mu.RUnlock()
	log, err := loadMutationLogFile(s.mutationPath(schemaName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log = NewMutationLog()
		} else {
			return nil, err
		}
	}
	s.cacheMutations(schemaName, log)
	return log, nil
}

func (s *SnapshotStore) fullTextIndex(schemaName string) (*FullTextIndex, error) {
	s.mu.RLock()
	if idx := s.fullText[schemaName]; idx != nil {
		s.mu.RUnlock()
		return idx, nil
	}
	s.mu.RUnlock()
	meta, err := s.LoadMeta(schemaName)
	if err != nil {
		return nil, err
	}
	path := meta.FullTextPath
	if path == "" {
		path = "fulltext.json"
	}
	idx, err := loadFullTextIndexFile(filepath.Join(s.root, schemaName, path))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	s.cacheFullText(schemaName, idx)
	return idx, nil
}

func saveMutationLogFile(path string, log *MutationLog) error {
	if log == nil {
		log = NewMutationLog()
	}
	data, err := json.MarshalIndent(log, "", "  ")
	if err != nil {
		return err
	}
	return atomicWrite(path, data)
}

func loadMutationLogFile(path string) (*MutationLog, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	log := NewMutationLog()
	if err := json.Unmarshal(data, log); err != nil {
		return nil, err
	}
	return log, nil
}

func clearMutationLogFile(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func writeFullTextIndexFile(path string, idx *FullTextIndex) error {
	if idx == nil {
		idx = NewFullTextIndex()
	}
	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	return atomicWrite(path, data)
}

func loadFullTextIndexFile(path string) (*FullTextIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	idx := NewFullTextIndex()
	if err := json.Unmarshal(data, idx); err != nil {
		return nil, err
	}
	return idx, nil
}

func strconvUint(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func parseUint(s string) (uint64, bool) {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
