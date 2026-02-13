package schema

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// Save writes the document back to a file in SCRT format.
func (d *Document) Save(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return d.Write(f)
}

// SaveData writes only the data sections to a file.
func (d *Document) SaveData(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return d.WriteData(f)
}

// WriteData writes only the data sections to an io.Writer.
func (d *Document) WriteData(w io.Writer) error {
	var schemaNames []string
	for name := range d.Schemas {
		schemaNames = append(schemaNames, name)
	}
	sort.Strings(schemaNames)

	for _, name := range schemaNames {
		rows := d.Data[name]
		if len(rows) == 0 {
			continue
		}
		fmt.Fprintf(w, "@%s\n", name)

		schema := d.Schemas[name]
		for _, row := range rows {
			var vals []string
			for _, field := range schema.Fields {
				val, ok := row[field.Name]
				if !ok || val == nil {
					vals = append(vals, "null")
					continue
				}

				switch v := val.(type) {
				case string:
					vals = append(vals, fmt.Sprintf("%q", v))
				case []byte:
					vals = append(vals, fmt.Sprintf("%q", string(v)))
				case time.Time:
					if field.Kind == KindDate {
						vals = append(vals, v.Format("2006-01-02"))
					} else if field.Kind == KindDateTime {
						vals = append(vals, v.Format("2006-01-02T15:04:05"))
					} else {
						vals = append(vals, v.Format(time.RFC3339))
					}
				default:
					vals = append(vals, fmt.Sprintf("%v", v))
				}
			}
			fmt.Fprintln(w, strings.Join(vals, ", "))
		}
		fmt.Fprintln(w)
	}
	return nil
}

// Write writes the document to an io.Writer.
func (d *Document) Write(w io.Writer) error {
	// 1. Write Schemas
	// Map iteration is random, so we sort keys for deterministic output
	var schemaNames []string
	for name := range d.Schemas {
		schemaNames = append(schemaNames, name)
	}
	sort.Strings(schemaNames)

	for _, name := range schemaNames {
		s := d.Schemas[name]
		fmt.Fprintf(w, "@schema:%s\n", s.Name)
		for _, f := range s.Fields {
			fmt.Fprintf(w, "@field %s %s", f.Name, f.RawType)
			if f.AutoIncrement {
				fmt.Fprintf(w, " serial") // or auto_increment
			}
			if f.PrimaryKey {
				fmt.Fprintf(w, " pk")
			}
			if f.Unique {
				fmt.Fprintf(w, " unique")
			}
			if f.TargetSchema != "" {
				// RawType usually contains the Ref, so we might duplicate if we aren't careful.
				// But Parser places "ref:X:Y" in RawType.
				// However, if we reconstructed it, we would need logic.
				// Simplest is to trust RawType from parser.
			}
			if f.Default != nil {
				if f.Default.Expression != "" {
					fmt.Fprintf(w, " default=%s", f.Default.Expression)
				} else {
					// Use a cleaner string representation than hashKey which adds type prefixes
					valStr := ""
					switch f.Default.Kind {
					case KindBool:
						valStr = fmt.Sprintf("%v", f.Default.Bool)
					case KindInt64: // And checking other int types if they map here
						valStr = fmt.Sprintf("%d", f.Default.Int)
					case KindUint64:
						valStr = fmt.Sprintf("%d", f.Default.Uint)
					case KindFloat64:
						valStr = fmt.Sprintf("%g", f.Default.Float)
					case KindString:
						valStr = fmt.Sprintf("%q", f.Default.String)
					default:
						// Fallback to hashKey if complex or unknown, hoping parser works or just strict
						valStr = f.Default.hashKey()
					}
					fmt.Fprintf(w, " default=%s", valStr)
				}
			}
			// Other attributes?
			fmt.Fprintln(w)
		}

		// Write Indexes
		for _, idx := range s.Indexes {
			fmt.Fprintf(w, "@index:%s(%s)", idx.Name, strings.Join(idx.Fields, ", "))
			if idx.Unique {
				fmt.Fprint(w, " unique")
			}
			fmt.Fprintln(w)
		}
		fmt.Fprintln(w)
	}

	// 2. Write Functions
	var funcNames []string
	for name := range d.Functions {
		funcNames = append(funcNames, name)
	}
	sort.Strings(funcNames)

	for _, name := range funcNames {
		fn := d.Functions[name]
		var args []string
		for _, arg := range fn.Args {
			args = append(args, fmt.Sprintf("%s %s", arg.Name, arg.Type))
		}
		fmt.Fprintf(w, "@function:%s(%s) %s\n", fn.Name, strings.Join(args, ", "), fn.ReturnType)
		// Access body, ensuring it's indented or formatted? Parser keeps raw body.
		fmt.Fprintf(w, "%s\n\n", fn.Body)
	}

	// 3. Write Queries
	var queryNames []string
	for name := range d.Queries {
		queryNames = append(queryNames, name)
	}
	sort.Strings(queryNames)

	for _, name := range queryNames {
		q := d.Queries[name]
		var args []string
		for _, arg := range q.Args {
			args = append(args, fmt.Sprintf("%s %s", arg.Name, arg.Type))
		}
		fmt.Fprintf(w, "@query:%s(%s)\n", q.Name, strings.Join(args, ", "))
		fmt.Fprintf(w, "%s\n\n", q.SQL)
	}

	// 4. Write Data
	// For data, we should also maintain order of schemas or just iterate
	for _, name := range schemaNames {
		rows := d.Data[name]
		if len(rows) == 0 {
			continue
		}
		fmt.Fprintf(w, "@%s\n", name)

		schema := d.Schemas[name]
		for _, row := range rows {
			var vals []string
			for _, field := range schema.Fields {
				val, ok := row[field.Name]
				if !ok || val == nil {
					// Handle defaults or missing?
					// Use nil/null representation or skip if not present (but CSV expects position)
					// If strictly positional, must output something.
					// If CSV line parsing allows skipping, we act accordingly.
					// For this writer, let's output basic string rep.
					vals = append(vals, "null")
					continue
				}

				// string quoting
				switch v := val.(type) {
				case string:
					vals = append(vals, fmt.Sprintf("%q", v))
				case []byte:
					vals = append(vals, fmt.Sprintf("%q", string(v)))
				case time.Time:
					if field.Kind == KindDate {
						vals = append(vals, v.Format("2006-01-02"))
					} else if field.Kind == KindDateTime {
						vals = append(vals, v.Format("2006-01-02T15:04:05"))
					} else {
						vals = append(vals, v.Format(time.RFC3339))
					}
				default:
					vals = append(vals, fmt.Sprintf("%v", v))
				}
			}
			fmt.Fprintln(w, strings.Join(vals, ", "))
		}
		fmt.Fprintln(w)
	}
	return nil
}
