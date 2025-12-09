package schema

import (
	"os"
)

// ParseFile loads and parses a schema document from disk.
func ParseFile(path string) (*Document, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	doc, err := Parse(f)
	if err != nil {
		return nil, err
	}
	doc.Source = path
	return doc, nil
}
