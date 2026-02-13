package schema

import (
	"os"
)

// ParseFile loads and parses a schema document from disk.
func ParseFile(path string) (*Document, error) {
	doc := NewDocument()
	if err := doc.LoadFile(path); err != nil {
		return nil, err
	}
	doc.Source = path
	return doc, nil
}

// LoadFile loads and parses a schema document from disk into the current document.
func (doc *Document) LoadFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return doc.Load(f)
}
