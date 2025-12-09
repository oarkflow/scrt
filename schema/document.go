package schema

// Document is the top-level container for schemas parsed from a .scrt file.
type Document struct {
	Schemas map[string]*Schema
	Data    map[string][]map[string]interface{} // schema name -> rows
	Source  string
}

// Schema returns a schema by name.
func (d *Document) Schema(name string) (*Schema, bool) {
	if d == nil {
		return nil, false
	}
	s, ok := d.Schemas[name]
	return s, ok
}

// Records returns the parsed data rows for a schema by name.
func (d *Document) Records(name string) ([]map[string]interface{}, bool) {
	if d == nil {
		return nil, false
	}
	records, ok := d.Data[name]
	return records, ok
}
