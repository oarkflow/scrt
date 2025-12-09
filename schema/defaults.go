package schema

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/oarkflow/scrt/temporal"
)

// DefaultValue keeps the typed literal configured for a field.
type DefaultValue struct {
	Kind   FieldKind
	Bool   bool
	Int    int64
	Uint   uint64
	Float  float64
	String string
	Bytes  []byte
}

func (d *DefaultValue) hashKey() string {
	if d == nil {
		return ""
	}
	switch d.Kind {
	case KindBool:
		if d.Bool {
			return "bool:1"
		}
		return "bool:0"
	case KindInt64:
		return fmt.Sprintf("int:%d", d.Int)
	case KindUint64, KindRef:
		return fmt.Sprintf("uint:%d", d.Uint)
	case KindFloat64:
		return fmt.Sprintf("float:%g", d.Float)
	case KindString:
		return fmt.Sprintf("str:%s", d.String)
	case KindBytes:
		return fmt.Sprintf("bytes:%s", base64.StdEncoding.EncodeToString(d.Bytes))
	case KindDate:
		return fmt.Sprintf("date:%d", d.Int)
	case KindDateTime:
		return fmt.Sprintf("datetime:%d", d.Int)
	case KindTimestamp:
		return fmt.Sprintf("timestamp:%d", d.Int)
	case KindDuration:
		return fmt.Sprintf("duration:%d", d.Int)
	case KindTimestampTZ:
		return fmt.Sprintf("timestamptz:%s", d.String)
	default:
		return ""
	}
}

func parseDefaultLiteral(kind FieldKind, raw string) (*DefaultValue, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("default value missing literal")
	}
	val := &DefaultValue{Kind: kind}
	switch kind {
	case KindBool:
		b, err := strconv.ParseBool(strings.ToLower(raw))
		if err != nil {
			return nil, fmt.Errorf("invalid bool default %q: %w", raw, err)
		}
		val.Bool = b
	case KindInt64:
		i, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid int64 default %q: %w", raw, err)
		}
		val.Int = i
	case KindUint64, KindRef:
		u, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid uint64 default %q: %w", raw, err)
		}
		val.Uint = u
	case KindFloat64:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float64 default %q: %w", raw, err)
		}
		val.Float = f
	case KindString:
		str, err := parseStringLiteral(raw)
		if err != nil {
			return nil, err
		}
		val.String = str
	case KindBytes:
		bytesVal, err := parseBytesLiteral(raw)
		if err != nil {
			return nil, err
		}
		val.Bytes = bytesVal
	case KindDate:
		unquoted, err := parseStringLiteral(raw)
		if err != nil {
			return nil, err
		}
		t, err := temporal.ParseDate(unquoted)
		if err != nil {
			return nil, err
		}
		val.Int = temporal.EncodeDate(t)
	case KindDateTime:
		unquoted, err := parseStringLiteral(raw)
		if err != nil {
			return nil, err
		}
		t, err := temporal.ParseDateTime(unquoted)
		if err != nil {
			return nil, err
		}
		val.Int = temporal.EncodeInstant(t)
	case KindTimestamp:
		unquoted, err := parseStringLiteral(raw)
		if err != nil {
			return nil, err
		}
		t, err := temporal.ParseTimestamp(unquoted)
		if err != nil {
			return nil, err
		}
		val.Int = temporal.EncodeInstant(t)
	case KindTimestampTZ:
		unquoted, err := parseStringLiteral(raw)
		if err != nil {
			return nil, err
		}
		t, err := temporal.ParseTimestampTZ(unquoted)
		if err != nil {
			return nil, err
		}
		val.String = temporal.FormatTimestampTZ(t)
	case KindDuration:
		unquoted, err := parseStringLiteral(raw)
		if err != nil {
			return nil, err
		}
		dur, err := temporal.ParseDuration(unquoted)
		if err != nil {
			return nil, err
		}
		val.Int = int64(dur)
	default:
		return nil, fmt.Errorf("defaults not supported for kind %d", kind)
	}
	return val, nil
}

func parseStringLiteral(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if raw[0] == '"' || raw[0] == '`' || raw[0] == '\'' {
		parsed, err := strconv.Unquote(raw)
		if err != nil {
			return "", fmt.Errorf("invalid string literal %q: %w", raw, err)
		}
		return parsed, nil
	}
	return raw, nil
}

func parseBytesLiteral(raw string) ([]byte, error) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "0x") || strings.HasPrefix(raw, "0X") {
		decoded, err := hex.DecodeString(raw[2:])
		if err != nil {
			return nil, fmt.Errorf("invalid hex literal %q: %w", raw, err)
		}
		return decoded, nil
	}
	str, err := parseStringLiteral(raw)
	if err != nil {
		return nil, err
	}
	return []byte(str), nil
}
