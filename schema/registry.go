package schema

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// DocumentRegistry keeps multiple SCRT documents in memory along with their raw DSL and payloads.
type DocumentRegistry struct {
	mu   sync.RWMutex
	docs map[string]*registryDocument
}

type registryDocument struct {
	name     string
	doc      *Document
	raw      []byte
	source   string
	updated  time.Time
	payloads map[string][]byte
}

// DocumentSummary describes a stored SCRT document.
type DocumentSummary struct {
	Name        string    `json:"name"`
	Fingerprint string    `json:"fingerprint"`
	SchemaCount int       `json:"schemaCount"`
	UpdatedAt   time.Time `json:"updatedAt"`
	Source      string    `json:"source"`
}

// NewDocumentRegistry creates an empty registry.
func NewDocumentRegistry() *DocumentRegistry {
	return &DocumentRegistry{docs: make(map[string]*registryDocument)}
}

// LoadFile reads a .scrt file from disk and stores it under the provided name.
func (r *DocumentRegistry) LoadFile(name, path string) (*Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if name == "" {
		base := filepath.Base(path)
		name = strings.TrimSuffix(base, filepath.Ext(base))
	}
	return r.Upsert(name, data, path, info.ModTime())
}

// Upsert parses raw SCRT DSL bytes and stores/overwrites the document.
func (r *DocumentRegistry) Upsert(name string, raw []byte, source string, updatedAt time.Time) (*Document, error) {
	if name == "" {
		return nil, fmt.Errorf("document name cannot be empty")
	}
	doc, err := Parse(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	doc.Source = source
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	entry := &registryDocument{
		name:     name,
		doc:      doc,
		raw:      append([]byte(nil), raw...),
		source:   source,
		updated:  updatedAt,
		payloads: make(map[string][]byte),
	}
	r.mu.Lock()
	r.docs[name] = entry
	r.mu.Unlock()
	return doc, nil
}

// Snapshot returns the parsed document, raw DSL, and timestamp for a document name.
func (r *DocumentRegistry) Snapshot(name string) (*Document, []byte, time.Time, error) {
	r.mu.RLock()
	entry, ok := r.docs[name]
	r.mu.RUnlock()
	if !ok {
		return nil, nil, time.Time{}, os.ErrNotExist
	}
	rawCopy := append([]byte(nil), entry.raw...)
	return entry.doc, rawCopy, entry.updated, nil
}

// List returns all document summaries.
func (r *DocumentRegistry) List() []DocumentSummary {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]DocumentSummary, 0, len(r.docs))
	for name, entry := range r.docs {
		out = append(out, DocumentSummary{
			Name:        name,
			Fingerprint: fmt.Sprintf("%016x", documentFingerprint(entry.doc)),
			SchemaCount: len(entry.doc.Schemas),
			UpdatedAt:   entry.updated,
			Source:      entry.source,
		})
	}
	return out
}

// Payload returns a copy of the stored payload bytes for a schema.
func (r *DocumentRegistry) Payload(docName, schemaName string) ([]byte, bool) {
	r.mu.RLock()
	entry, ok := r.docs[docName]
	if !ok {
		r.mu.RUnlock()
		return nil, false
	}
	data, ok := entry.payloads[schemaName]
	r.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return append([]byte(nil), data...), true
}

// Payloads returns a shallow copy of all payloads for a document.
func (r *DocumentRegistry) Payloads(docName string) map[string][]byte {
	r.mu.RLock()
	entry, ok := r.docs[docName]
	if !ok {
		r.mu.RUnlock()
		return nil
	}
	clone := make(map[string][]byte, len(entry.payloads))
	for name, data := range entry.payloads {
		clone[name] = append([]byte(nil), data...)
	}
	r.mu.RUnlock()
	return clone
}

// SetPayload saves/overwrites the payload bytes for a schema inside a document.
func (r *DocumentRegistry) SetPayload(docName, schemaName string, data []byte) error {
	if schemaName == "" {
		return fmt.Errorf("schema name cannot be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.docs[docName]
	if !ok {
		return os.ErrNotExist
	}
	entry.payloads[schemaName] = append([]byte(nil), data...)
	return nil
}

// DeleteDocument removes a document from the registry.
func (r *DocumentRegistry) DeleteDocument(name string) {
	r.mu.Lock()
	delete(r.docs, name)
	r.mu.Unlock()
}

// HasDocument returns true if a named document exists.
func (r *DocumentRegistry) HasDocument(name string) bool {
	r.mu.RLock()
	_, ok := r.docs[name]
	r.mu.RUnlock()
	return ok
}

// CopyDSL streams the stored DSL into w.
func (r *DocumentRegistry) CopyDSL(name string, w io.Writer) error {
	r.mu.RLock()
	entry, ok := r.docs[name]
	r.mu.RUnlock()
	if !ok {
		return os.ErrNotExist
	}
	_, err := w.Write(entry.raw)
	return err
}
