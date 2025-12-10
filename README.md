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

## DSL Data Rows

The data section that follows each `@schema` block now has a more forgiving parser:

- **Auto-increment columns can be omitted**. If a field is marked `auto_increment`, you no longer have to supply a placeholder value—SCRT will assign the next sequence value automatically.
- **Explicit overrides use named assignments**. Prefix any cell with `@FieldName=` to override the generated value (e.g. `@MsgID=9001`), or to backfill a sparse column while leaving earlier auto-increment fields empty.
- **Reference fields store raw target keys**. The legacy `@ref:Schema:Field=value` tokens have been removed; simply emit the referenced primary key and SCRT will validate it against the schema metadata.

Example:

```text
@schema:Message
fields:
  MsgID uint64 auto_increment
  User  ref User.ID
  Text  string
  Lang  string default "en"

@Message
1001, "Hey there"             # MsgID auto-populates, Lang defaults to "en"
@MsgID=77, 2001, "Override"   # Override auto-increment using @Field=value
2002, "Hola", "es"            # Provide every field explicitly when needed
```

The same rules apply to every schema in the file, so datasets stay terse even when many reference or serial columns exist.

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

### Temporal Field Types

The schema DSL understands five time-aware primitives in addition to the existing numeric/string kinds:

| DSL Type    | Go Type        | Storage       | Accepted Literals |
|-------------|----------------|---------------|-------------------|
| `date`      | `time.Time`    | UTC midnight  | `yyyy-mm-dd`, `dd/mm/yyyy`, `mm-dd-yyyy`, `Jan 02 2006`, etc. |
| `datetime`  | `time.Time`    | UTC instant   | Any supported date format with 24h/12h clock (no timezone). |
| `timestamp` | `time.Time`    | UTC instant   | Same as `datetime` plus Unix epoch integers/decimals. |
| `timestamptz` | `time.Time`  | RFC3339 string| Any timestamp with explicit zone/offset (e.g. `2025-01-02T10:30:00-05:00`). |
| `duration`  | `time.Duration`| int64 nanos   | Go durations plus day suffixes (`1d2h`, `90m`, `4d`). |

At marshal time SCRT accepts `time.Time`, `time.Duration`, numeric epochs, or strings in the formats above. During unmarshal these fields map back to the native Go types, while map targets can opt into strings (ISO8601/RFC3339) or the raw `time.Time`/`time.Duration` values.

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
GOEXPERIMENT=arenas go run ./examples/schema_server \
  -schema message=data.scrt \
  -schema user=/path/to/user.scrt \
  -addr :8080 -base-path /schemas

# in another terminal start the Vite client (defaults to localhost:8080)
VITE_SCRT_SERVER=http://localhost:8080 npm run dev
```

The client is located under `src/` and pulls schema + payload data from
`/schemas/bundle?document=message&schema=Message`. That endpoint now emits a compact binary
envelope (magic `SCB1`, version byte, document fingerprint, unix-nano timestamp,
u32-length schema DSL, u16-count index entries, optional payload section). No JSON
parsing is required on either side, and the TypeScript helper reconstructs the
`Document` directly from the bytes.

## Shared Schema Gateway

The `schema.HTTPService` exposes cached schema documents over HTTP so TypeScript
clients can reuse the exact same DSL without shipping duplicate copies.

```sh
go run ./examples/schema_server \
  -schema message=data.scrt \
  -schema another=/path/to/another.scrt \
  -addr :8080 -base-path /schemas
```

This starts a tiny server with multi-document CRUD endpoints:

- `GET /schemas/documents` – enumerate every loaded SCRT document.
- `POST /schemas/documents?name={doc}` – upload/replace a `.scrt` document (request body is raw DSL text).
- `GET /schemas/documents/{doc}` – download the DSL text for a specific document.
- `POST /schemas/documents/{doc}/records/{schema}` – attach binary SCRT rows for a schema.
- `GET /schemas/documents/{doc}/records/{schema}` – fetch the stored rows.
- `GET /schemas/bundle?document={doc}&schema={Schema}` – emit the binary bundle described above (schema text + metadata + optional payload) in a single round-trip.
- `GET /schemas/index?document={doc}` / `GET /schemas/doc?document={doc}` – debugging helpers for the DSL/index view.

On the client side, use the `SchemaHttpClient` helper to stay in sync:

```ts
import { SchemaHttpClient } from "./ts/apiClient";

const client = new SchemaHttpClient({
  baseUrl: "http://localhost:8080",
  defaultDocument: "message",
  // paths are customizable if your routes live elsewhere
  // paths: {
  //   bundle: "/api/scrt/bundle",
  //   documents: "/api/scrt/docs",
  //   documentRecords: (doc, schema) => `/api/scrt/docs/${doc}/records/${schema}`,
  // },
});
const decoded = await client.fetchRecords("Message", () => ({
  MsgID: 0,
  User: 0,
  Text: "",
  Lang: "",
}), { document: "message" });

// marshal/unmarshal helpers are still available when you have local data
const messageSchema = await client.schema("Message", "message");
const payload = await client.marshal("Message", [
  { MsgID: 1, User: 42, Text: "hey", Lang: "en" },
]);

// Document management helpers
await client.upsertDocument("inventory", scrtDslString); // raw `.scrt` text
const docs = await client.listDocuments(); // [{ name: "message", ... }, ...]
await client.pushRecords("inventory", "Stock", binaryPayload); // binary SCRT rows
```

Both sides rely on the same `.scrt` file, fingerprints, and field order,
ensuring marshaling/unmarshaling stays deterministic across Go and TypeScript.
