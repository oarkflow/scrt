package schema

import (
	"errors"
	"fmt"
	"net"
	"net/mail"
	neturl "net/url"
	"regexp"
	"strings"

	"github.com/oarkflow/scrt/temporal"
)

var uuidRegex = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)

// ValidateData validates all parsed rows in the document against schema constraints.
func (d *Document) ValidateData() error {
	if d == nil {
		return nil
	}
	var errs []string
	for schemaName, rows := range d.Data {
		sch, ok := d.Schemas[schemaName]
		if !ok || sch == nil {
			continue
		}
		for i, row := range rows {
			if err := sch.ValidateRow(row); err != nil {
				errs = append(errs, fmt.Sprintf("%s row %d: %v", schemaName, i, err))
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("scrt validation failed: %s", strings.Join(errs, "; "))
	}
	return nil
}

// ValidateRow validates a single row against schema-level constraints.
func (s *Schema) ValidateRow(row map[string]interface{}) error {
	if s == nil {
		return nil
	}
	var errs []string
	for _, field := range s.Fields {
		val, exists := row[field.Name]
		if !exists || val == nil {
			if field.AutoIncrement || field.Nullable || field.Default != nil {
				continue
			}
			errs = append(errs, fmt.Sprintf("field %s is required", field.Name))
			continue
		}
		if err := validateFieldValue(field, val); err != nil {
			errs = append(errs, fmt.Sprintf("field %s: %v", field.Name, err))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func validateFieldValue(field Field, value interface{}) error {
	if value == nil {
		if field.Nullable {
			return nil
		}
		return fmt.Errorf("null is not allowed")
	}

	if len(field.Enum) > 0 {
		actual := fmt.Sprint(value)
		matched := false
		for _, allowed := range field.Enum {
			if actual == allowed {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("value %q is not in enum [%s]", actual, strings.Join(field.Enum, ", "))
		}
	}

	if field.MinLength != nil || field.MaxLength != nil || field.Pattern != "" {
		asString, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string-compatible value for string constraints")
		}
		if field.MinLength != nil && len(asString) < *field.MinLength {
			return fmt.Errorf("length %d is less than minlength %d", len(asString), *field.MinLength)
		}
		if field.MaxLength != nil && len(asString) > *field.MaxLength {
			return fmt.Errorf("length %d exceeds maxlength %d", len(asString), *field.MaxLength)
		}
		if field.Pattern != "" {
			matched, err := regexp.MatchString(field.Pattern, asString)
			if err != nil {
				return fmt.Errorf("invalid pattern %q: %w", field.Pattern, err)
			}
			if !matched {
				return fmt.Errorf("value %q does not match pattern %q", asString, field.Pattern)
			}
		}
	}

	if field.Format != "" {
		if asString, ok := value.(string); ok {
			if err := validateFormat(field.Format, asString); err != nil {
				return err
			}
		} else {
			switch strings.ToLower(strings.TrimSpace(field.Format)) {
			case "date", "datetime", "timestamp", "timestamptz", "duration":
				// Temporal fields are commonly decoded to typed values (time.Time/time.Duration).
			default:
				return fmt.Errorf("expected string value for format %s", field.Format)
			}
		}
	}

	if field.Minimum != nil || field.Maximum != nil {
		num, ok := numericValue(value)
		if !ok {
			return fmt.Errorf("expected numeric-compatible value for min/max constraints")
		}
		if field.Minimum != nil && num < *field.Minimum {
			return fmt.Errorf("value %g is less than minimum %g", num, *field.Minimum)
		}
		if field.Maximum != nil && num > *field.Maximum {
			return fmt.Errorf("value %g exceeds maximum %g", num, *field.Maximum)
		}
	}

	return nil
}

func numericValue(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func validateFormat(format, value string) error {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "string":
		return nil
	case "email":
		if _, err := mail.ParseAddress(value); err != nil {
			return fmt.Errorf("invalid email format")
		}
	case "uri", "url":
		u, err := neturl.ParseRequestURI(value)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("invalid URI format")
		}
	case "uuid":
		if !uuidRegex.MatchString(value) {
			return fmt.Errorf("invalid UUID format")
		}
	case "hostname":
		if strings.TrimSpace(value) == "" || strings.Contains(value, " ") {
			return fmt.Errorf("invalid hostname format")
		}
	case "ipv4":
		ip := net.ParseIP(value)
		if ip == nil || ip.To4() == nil {
			return fmt.Errorf("invalid IPv4 format")
		}
	case "ipv6":
		ip := net.ParseIP(value)
		if ip == nil || ip.To4() != nil {
			return fmt.Errorf("invalid IPv6 format")
		}
	case "date":
		if _, err := temporal.ParseDate(value); err != nil {
			return fmt.Errorf("invalid date format")
		}
	case "datetime":
		if _, err := temporal.ParseDateTime(value); err != nil {
			return fmt.Errorf("invalid datetime format")
		}
	case "timestamp":
		if _, err := temporal.ParseTimestamp(value); err != nil {
			return fmt.Errorf("invalid timestamp format")
		}
	case "timestamptz":
		if _, err := temporal.ParseTimestampTZ(value); err != nil {
			return fmt.Errorf("invalid timestamptz format")
		}
	case "duration":
		if _, err := temporal.ParseDuration(value); err != nil {
			return fmt.Errorf("invalid duration format")
		}
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
	return nil
}
