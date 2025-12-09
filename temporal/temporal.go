package temporal

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	dateLayouts = []string{
		"2006-01-02",
		"02-01-2006",
		"2006/01/02",
		"02/01/2006",
		"01/02/2006",
		"2006.01.02",
		"02.01.2006",
		"20060102",
		"02 Jan 2006",
		"Jan 02 2006",
		"2 Jan 2006",
		"January 2, 2006",
		"2 January 2006",
	}

	datetimeLayouts = []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
		"02-01-2006 15:04:05",
		"02-01-2006 15:04",
		"2006/01/02 15:04:05",
		"2006/01/02 15:04",
		"02/01/2006 15:04:05",
		"02/01/2006 15:04",
		"01/02/2006 15:04:05",
		"01/02/2006 15:04",
		"2006-01-02 03:04:05 PM",
		"2006-01-02 03:04 PM",
		"02-01-2006 03:04:05 PM",
		"02-01-2006 03:04 PM",
		"01/02/2006 03:04:05 PM",
		"01/02/2006 03:04 PM",
	}

	timestampZoneLayouts = []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05 -0700",
		"2006-01-02 15:04:05 -07:00",
		"2006/01/02 15:04:05 -0700",
		"2006-01-02 03:04:05 PM -0700",
		"2006/01/02 03:04:05 PM -0700",
		time.RFC1123Z,
		time.RFC1123,
	}
)

var dayPattern = regexp.MustCompile(`(?i)(\d+(?:\.\d+)?)d`)

// ParseDuration parses human friendly durations, extending Go's syntax with day units.
func ParseDuration(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, fmt.Errorf("temporal: empty duration literal")
	}
	normalized, err := normalizeDurationDays(trimmed)
	if err != nil {
		return 0, err
	}
	d, err := time.ParseDuration(normalized)
	if err != nil {
		return 0, fmt.Errorf("temporal: parse duration %q: %w", raw, err)
	}
	return d, nil
}

// ParseDate parses several common date formats into a UTC time truncated to midnight.
func ParseDate(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("temporal: empty date literal")
	}
	if t, err := parseNoZoneLayouts(trimmed, dateLayouts); err == nil {
		return DecodeDate(EncodeDate(t)), nil
	}
	return time.Time{}, fmt.Errorf("temporal: unable to parse date %q", raw)
}

// ParseDateTime parses date+time inputs without requiring an explicit timezone.
func ParseDateTime(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("temporal: empty datetime literal")
	}
	if t, err := parseNoZoneLayouts(trimmed, datetimeLayouts); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("temporal: unable to parse datetime %q", raw)
}

// ParseTimestamp parses timestamps, accepting timezone-aware strings or epoch numbers.
func ParseTimestamp(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("temporal: empty timestamp literal")
	}
	if t, err := parseZoneLayouts(trimmed, timestampZoneLayouts); err == nil {
		return t.UTC(), nil
	}
	if t, err := parseNoZoneLayouts(trimmed, datetimeLayouts); err == nil {
		return t.UTC(), nil
	}
	if t, ok := parseEpochString(trimmed); ok {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("temporal: unable to parse timestamp %q", raw)
}

// ParseTimestampTZ parses timestamp inputs that include an explicit timezone.
func ParseTimestampTZ(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("temporal: empty timestamptz literal")
	}
	if t, err := parseZoneLayouts(trimmed, timestampZoneLayouts); err == nil {
		return t, nil
	}
	if t, ok := parseEpochString(trimmed); ok {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("temporal: unable to parse timestamptz %q", raw)
}

// EncodeInstant normalizes a timestamp to UTC nanoseconds.
func EncodeInstant(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixNano()
}

// EncodeDate stores a date at midnight UTC.
func EncodeDate(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	utc := t.UTC()
	midnight := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
	return midnight.UnixNano()
}

// DecodeInstant converts stored nanoseconds into a UTC time.
func DecodeInstant(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns).UTC()
}

// DecodeDate converts stored date nanoseconds back to UTC midnight.
func DecodeDate(ns int64) time.Time {
	return DecodeInstant(ns)
}

// FormatDate renders a date as YYYY-MM-DD.
func FormatDate(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02")
}

// FormatInstant renders a timestamp using RFC3339Nano.
func FormatInstant(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// FormatTimestampTZ renders a timestamp-with-zone preserving its offset.
func FormatTimestampTZ(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}

// CanonicalTimestampTZ normalizes arbitrary timestamp strings into RFC3339Nano.
func CanonicalTimestampTZ(raw string) (string, error) {
	t, err := ParseTimestampTZ(raw)
	if err != nil {
		return "", err
	}
	if t.IsZero() {
		return "", nil
	}
	return FormatTimestampTZ(t), nil
}

// InferEpochNanoseconds guesses the epoch precision for integer literals.
func InferEpochNanoseconds(v int64) int64 {
	abs := math.Abs(float64(v))
	switch {
	case abs < 1e11: // seconds precision
		return v * int64(time.Second)
	case abs < 1e14: // milliseconds
		return v * int64(time.Millisecond)
	case abs < 1e17: // microseconds
		return v * int64(time.Microsecond)
	default:
		return v // already in nanoseconds
	}
}

func parseNoZoneLayouts(raw string, layouts []string) (time.Time, error) {
	var lastErr error
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, raw, time.UTC); err == nil {
			return t, nil
		} else {
			lastErr = err
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("temporal: no layouts provided")
	}
	return time.Time{}, lastErr
}

func parseZoneLayouts(raw string, layouts []string) (time.Time, error) {
	var lastErr error
	for _, layout := range layouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t, nil
		} else {
			lastErr = err
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("temporal: no layouts provided")
	}
	return time.Time{}, lastErr
}

func parseEpochString(raw string) (time.Time, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, false
	}
	if strings.ContainsAny(trimmed, "eE") {
		f, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return time.Time{}, false
		}
		sec, frac := math.Modf(f)
		return time.Unix(int64(sec), int64(frac*float64(time.Second))).UTC(), true
	}
	if strings.Contains(trimmed, ".") {
		parts := strings.SplitN(trimmed, ".", 2)
		secPart := parts[0]
		fracDigits := digitPrefix(strings.TrimSpace(parts[1]))
		if secPart == "" || secPart == "+" || secPart == "-" {
			secPart = secPart + "0"
		}
		sec, err := strconv.ParseInt(secPart, 10, 64)
		if err != nil {
			return time.Time{}, false
		}
		if fracDigits == "" {
			fracDigits = "0"
		}
		for len(fracDigits) < 9 {
			fracDigits += "0"
		}
		if len(fracDigits) > 9 {
			fracDigits = fracDigits[:9]
		}
		frac, err := strconv.ParseInt(fracDigits, 10, 64)
		if err != nil {
			return time.Time{}, false
		}
		if sec < 0 {
			frac = -frac
		}
		return time.Unix(sec, frac).UTC(), true
	}
	if !isSignedDigits(trimmed) {
		return time.Time{}, false
	}
	v, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, InferEpochNanoseconds(v)).UTC(), true
}

func isSignedDigits(s string) bool {
	if s == "" {
		return false
	}
	if s[0] == '+' || s[0] == '-' {
		s = s[1:]
	}
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func digitPrefix(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r < '0' || r > '9' {
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}

func normalizeDurationDays(raw string) (string, error) {
	var firstErr error
	normalized := dayPattern.ReplaceAllStringFunc(raw, func(match string) string {
		groups := dayPattern.FindStringSubmatch(match)
		if len(groups) < 2 {
			return match
		}
		hours, err := strconv.ParseFloat(groups[1], 64)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			return match
		}
		totalHours := hours * 24
		return strconv.FormatFloat(totalHours, 'f', -1, 64) + "h"
	})
	if firstErr != nil {
		return "", firstErr
	}
	return normalized, nil
}
