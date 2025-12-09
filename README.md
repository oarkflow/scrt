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

## Frontend Demo

The repository now includes a Vite-powered UI that reuses the same TypeScript helpers:

```bash
# install deps once
npm install

# run the Go schema server for data + schema endpoints
GOEXPERIMENT=arenas go run ./examples/schema_server -schema data.scrt -addr :8080

# in another terminal start the Vite client (defaults to localhost:8080)
VITE_SCRT_SERVER=http://localhost:8080 npm run dev
```

The client is located under `src/` and pulls schemas via `SchemaHttpClient`, fetches
`/api/messages`, and renders the decoded rows in the browser.

## Shared Schema Gateway

The `schema.HTTPService` exposes cached schema documents over HTTP so TypeScript
clients can reuse the exact same DSL without shipping duplicate copies.

```sh
go run ./examples/schema_server -schema ./data.scrt -addr :8080
```

This starts a tiny server with two routes:

- `GET /schemas/index` – JSON metadata (name, fingerprint, field count).
- `GET /schemas/doc` – raw `.scrt` DSL with `X-SCRT-Fingerprint` header.

On the client side, use the `SchemaHttpClient` helper to stay in sync:

```ts
import { SchemaHttpClient } from "./ts/apiClient";

const client = new SchemaHttpClient("http://localhost:8080");
const messageSchema = await client.schema("Message");
const payload = await client.marshal("Message", [
  { MsgID: 1, User: 42, Text: "hey", Lang: "en" },
]);

// Later, hydrate rows back into structs using the shared schema cache
const decoded = await client.unmarshal("Message", payload, () => ({
  MsgID: 0,
  User: 0,
  Text: "",
  Lang: "",
}));
```

Both sides rely on the same `.scrt` file, fingerprints, and field order,
ensuring marshaling/unmarshaling stays deterministic across Go and TypeScript.
