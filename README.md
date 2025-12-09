# SCRT: Schema-Compressed Record Transport

SCRT is an experimental binary transport format for structured data that targets:

- near-zero heap allocations by relying on arena-backed buffers and reusable structs
- deterministic schema-driven layouts with cached metadata
- 90%+ smaller footprint than JSON on large datasets thanks to columnar compression and prefix dictionaries
- a minimal, Go-native implementation that avoids the `encoding/json` package entirely

## Format Overview

A `.scrt` document consists of three logical segments written back-to-back:

1. **Preamble** – a fixed magic number (`SCRT\u0001`), semantic version, and feature bits.
2. **Schema Segment** – compiled representation of the `@schema` DSL. Each schema definition is hashed (X24 fingerprint) and stored once per file.
3. **Payload Segment** – columnar pages (default 64KB) that store rows encoded as delta-compressed primitive columns ordered by schema.

Within a schema:

- Fields carry type, optional constraints, and auto-increment rules.
- Refs are encoded as unsigned ints referencing the primary key of the target schema.
- Strings live in a deduplicated dictionary for the current page and are referenced via varint handles.

## Package Layout

```
scrt/
  schema/      // DSL parser, validation, canonicalization, fingerprinting
  column/      // Column writers/readers per primitive (varint, zigzag, dict strings)
  page/        // Page builder with allocator-free buffers
  codec/       // High-level Encoder/Decoder APIs
```

## Usage Example

```go
cache := schema.NewCache()
doc, err := cache.LoadFile("data.scrt")
if err != nil { log.Fatal(err) }
message := doc.Schemas["Message"]

var buf bytes.Buffer
writer := codec.NewWriter(&buf, message, 1024)
row := codec.NewRow(message)
row.SetUint("MsgID", 1)
row.SetUint("User", 1001)
row.SetString("Text", "Hello World!")
row.SetString("Lang", "en")
writer.WriteRow(row)
writer.Close()

reader := codec.NewReader(bytes.NewReader(buf.Bytes()), message)
decoded := codec.NewRow(message)
for {
    ok, err := reader.ReadRow(decoded)
    if err != nil || !ok {
        break
    }
    fmt.Println(decoded.Values())
}
```

## High-level API

The `scrt` package exposes Marshaling helpers that operate on structs, maps, and slices without touching `encoding/json`:

```go
type Message struct {
  MsgID uint64
  User  uint64
  Text  string
  Lang  string `scrt:"Lang"`
}

schemaDoc, _ := schema.ParseFile("./data.scrt")
msgSchema, _ := schemaDoc.Schema("Message")

payload, err := scrt.Marshal(msgSchema, []Message{{MsgID: 1, User: 42, Text: "hey"}})
if err != nil { panic(err) }

var out []Message
if err := scrt.Unmarshal(payload, msgSchema, &out); err != nil { panic(err) }
```

See `examples/basic` for a runnable sample.

## Caching Strategy

`schema.Cache` retains compiled schemas keyed by fingerprint and file path. Each cache entry stores:

- canonical field order and offsets
- encoder/decoder function pointers for each field type
- reusable buffers for column scratch space

The cache is concurrency-safe and supports hot reload by comparing fingerprint + modified time.

## Performance Targets

- **Encoding throughput**: >1.5GB/s on M2 baseline with default page size
- **Decoding throughput**: >1.2GB/s
- **Allocation rate**: <2 allocs/op (amortized) by reusing arenas and column writers
- **Space savings**: 90–95% vs. JSON on string-heavy datasets thanks to dedup dictionaries

## Next Steps

1. Tighten binary compatibility tests across Go versions.
2. Layer on SIMD-backed column codecs for even higher throughput.
3. Implement streaming readers for partial column fetches.
4. Benchmark against protobuf/flatbuffers for space and CPU comparisons.
