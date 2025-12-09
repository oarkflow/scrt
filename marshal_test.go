package scrt_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/scrt"
	"github.com/oarkflow/scrt/schema"
	"github.com/oarkflow/scrt/temporal"
)

func TestMarshalFilesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.scrt")
	dataPath := filepath.Join(dir, "data.scrtbin")
	schemaSource := `@schema Log
@field ID uint64
@field Msg string`

	if err := os.WriteFile(schemaPath, []byte(schemaSource), 0o644); err != nil {
		t.Fatalf("write schema: %v", err)
	}

	input := []map[string]any{
		{"ID": uint64(10), "Msg": "hello"},
		{"ID": uint64(11), "Msg": "world"},
	}

	if err := scrt.MarshalFiles(schemaPath, "Log", dataPath, input); err != nil {
		t.Fatalf("MarshalFiles: %v", err)
	}

	var out []map[string]any
	if err := scrt.UnmarshalFiles(schemaPath, "Log", dataPath, &out); err != nil {
		t.Fatalf("UnmarshalFiles: %v", err)
	}

	if len(out) != 2 || out[1]["Msg"].(string) != "world" {
		t.Fatalf("unexpected file roundtrip result: %+v", out)
	}
}

func TestMarshalTemporalFields(t *testing.T) {
	src := `@schema Event
@field ID uint64
@field Day date
@field Seen timestamp
@field Logged timestamptz
@field Window duration
`
	doc, err := schema.Parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	sch, ok := doc.Schema("Event")
	if !ok {
		t.Fatalf("Event schema missing")
	}
	type Event struct {
		ID     uint64
		Day    time.Time
		Seen   time.Time
		Logged time.Time
		Window time.Duration
	}
	input := []Event{{
		ID:     42,
		Day:    time.Date(2025, time.January, 2, 15, 4, 5, 0, time.FixedZone("-0500", -5*3600)),
		Seen:   time.Date(2025, time.January, 3, 10, 30, 0, 0, time.UTC),
		Logged: time.Date(2025, time.January, 3, 12, 0, 0, 0, time.FixedZone("+0530", 19800)),
		Window: 36*time.Hour + 30*time.Minute,
	}}
	payload, err := scrt.Marshal(sch, input)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded []Event
	if err := scrt.Unmarshal(payload, sch, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("expected 1 decoded event, got %d", len(decoded))
	}
	got := decoded[0]
	if got.ID != input[0].ID {
		t.Fatalf("id mismatch: got %d", got.ID)
	}
	expectedDay := time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)
	if !got.Day.Equal(expectedDay) {
		t.Fatalf("day mismatch: got %s want %s", got.Day, expectedDay)
	}
	if !got.Seen.Equal(input[0].Seen.UTC()) {
		t.Fatalf("seen mismatch: got %s want %s", got.Seen, input[0].Seen.UTC())
	}
	if got.Window != input[0].Window {
		t.Fatalf("duration mismatch: got %s want %s", got.Window, input[0].Window)
	}
	if got.Logged.Format(time.RFC3339Nano) != input[0].Logged.Format(time.RFC3339Nano) {
		t.Fatalf("logged mismatch: got %s want %s", got.Logged.Format(time.RFC3339Nano), input[0].Logged.Format(time.RFC3339Nano))
	}
	var anyOut []map[string]any
	if err := scrt.Unmarshal(payload, sch, &anyOut); err != nil {
		t.Fatalf("unmarshal map:any: %v", err)
	}
	if _, ok := anyOut[0]["Day"].(time.Time); !ok {
		t.Fatalf("expected Day to decode as time.Time")
	}
	if _, ok := anyOut[0]["Window"].(time.Duration); !ok {
		t.Fatalf("expected Window to decode as time.Duration")
	}
	if ts, ok := anyOut[0]["Logged"].(time.Time); !ok || ts.Format(time.RFC3339Nano) != input[0].Logged.Format(time.RFC3339Nano) {
		t.Fatalf("logged map decode mismatch")
	}
}

func TestTemporalStringMapRoundTrip(t *testing.T) {
	src := `@schema Schedule
@field Day date
@field Run duration
@field Stamp timestamptz
`
	doc, err := schema.Parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	sch, ok := doc.Schema("Schedule")
	if !ok {
		t.Fatalf("Schedule schema missing")
	}
	input := []map[string]string{{
		"Day":   "01/02/2025",
		"Run":   "1d2h",
		"Stamp": "2025-02-01 15:04:05 -0300",
	}}
	payload, err := scrt.Marshal(sch, input)
	if err != nil {
		t.Fatalf("marshal string map: %v", err)
	}
	var strOut []map[string]string
	if err := scrt.Unmarshal(payload, sch, &strOut); err != nil {
		t.Fatalf("unmarshal string map: %v", err)
	}
	if strOut[0]["Day"] != "2025-02-01" {
		t.Fatalf("day normalized string mismatch: %s", strOut[0]["Day"])
	}
	runDur, err := time.ParseDuration(strOut[0]["Run"])
	if err != nil {
		t.Fatalf("parse returned duration: %v", err)
	}
	if runDur != (26 * time.Hour) {
		t.Fatalf("duration mismatch: got %s", runDur)
	}
	gotStamp, err := temporal.ParseTimestampTZ(strOut[0]["Stamp"])
	if err != nil {
		t.Fatalf("parse returned stamp: %v", err)
	}
	expectedStamp, err := temporal.ParseTimestampTZ("2025-02-01 15:04:05 -0300")
	if err != nil {
		t.Fatalf("parse expected stamp: %v", err)
	}
	if gotStamp.Format(time.RFC3339Nano) != expectedStamp.Format(time.RFC3339Nano) {
		t.Fatalf("stamp mismatch: got %s want %s", gotStamp, expectedStamp)
	}
}
