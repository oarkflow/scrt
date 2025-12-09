package schema

import (
	"bytes"
	"os"
	"sync"
	"time"
)

// Cache keeps compiled schemas resident in memory for repeated use.
type Cache struct {
	mu      sync.RWMutex
	docs    map[string]*cachedDoc
	schemas map[string]*Schema
}

type cachedDoc struct {
	doc     *Document
	raw     []byte
	modTime time.Time
}

// NewCache creates an empty schema cache.
func NewCache() *Cache {
	return &Cache{
		docs:    make(map[string]*cachedDoc),
		schemas: make(map[string]*Schema),
	}
}

// LoadFile parses schemas from path and caches them. Subsequent calls reuse the
// cached version when the file has not changed.
func (c *Cache) LoadFile(path string) (*Document, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	c.mu.RLock()
	if entry, ok := c.docs[path]; ok && !entry.modTime.Before(info.ModTime()) {
		docCopy := entry.doc
		c.mu.RUnlock()
		return docCopy, nil
	}
	c.mu.RUnlock()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	parsed, err := Parse(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	parsed.Source = path

	c.mu.Lock()
	c.docs[path] = &cachedDoc{doc: parsed, raw: append([]byte(nil), data...), modTime: info.ModTime()}
	for name, sch := range parsed.Schemas {
		c.schemas[name] = sch
	}
	c.mu.Unlock()

	return parsed, nil
}

// Schema returns a cached schema by name.
func (c *Cache) Schema(name string) (*Schema, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	sch, ok := c.schemas[name]
	return sch, ok
}

// Snapshot ensures schemaPath is loaded and returns the parsed document,
// original DSL bytes, and last modification time in a single call.
func (c *Cache) Snapshot(path string) (*Document, []byte, time.Time, error) {
	doc, err := c.LoadFile(path)
	if err != nil {
		return nil, nil, time.Time{}, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.docs[path]
	if !ok {
		return nil, nil, time.Time{}, os.ErrNotExist
	}
	raw := append([]byte(nil), entry.raw...)
	return doc, raw, entry.modTime, nil
}
