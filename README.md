# SCRT: Schema-Compressed Record Transport
```bash
# install deps once
npm install

# run the Go SCRT server for schema + record endpoints
go run ./cmd/scrt-server -addr :8080

# in another terminal start the Vite client (defaults to localhost:8080)
- Strings live in a deduplicated dictionary for the current page and are referenced via varint handles.
```

The client lives under `src/` and pulls schema + payload data from
`/bundle?schema=Message`. That endpoint emits a compact binary envelope
(magic `SCB1`, version byte, registry fingerprint, unix-nano timestamp,
u32-length schema DSL, u16-count index entries, optional payload section). No JSON
parsing is required on either side, and the TypeScript helper reconstructs the
schema directly from the bytes.

### Binary Encoding v2

- **Presence bitmaps** – every column carries a bitmap that records whether a row supplied a value. If a field is omitted (or relies on a schema default) no bytes are written for that row.
- **Implicit defaults** – decoders rebuild omitted values from the schema defaults, so round-trips behave as if the field had been stored explicitly.
- **Delta-compressed integers** – monotonic `uint64` streams (auto-increment IDs, refs) and all `int64`-backed fields emit a base value plus varint deltas, matching or beating protobuf varints on sparse key sequences.

## DSL Data Rows

The data section that follows each `@schema` block now has a more forgiving parser:

- **Auto-increment columns can be omitted**. If a field is marked `auto_increment`, you no longer have to supply a placeholder value—SCRT will assign the next sequence value automatically.
- **Explicit overrides use named assignments**. Prefix any cell with `@FieldName=` to override the generated value (e.g. `@MsgID=9001`), or to backfill a sparse column while leaving earlier auto-increment fields empty.
- **Reference fields store raw target keys**. The legacy `@ref:Schema:Field=value` tokens have been removed; simply emit the referenced primary key and SCRT will validate it against the schema metadata.

You can describe fields using either explicit `@field Name Type` lines or the older
`fields:` block—both compile to the same structure.

Example:

```text
@schema:Message
@field MsgID uint64 auto_increment
@field User  ref User.ID
@field Text  string
@field Lang  string default "en"

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

## TypeScript / JavaScript Port

The `src/` directory now ships a zero-dependency TypeScript implementation of
the SCRT codec so browser and Node.js applications can swap JSON/CSV payloads
for the same binary format produced by the Go libraries.

```ts
import { marshalRecords, parseSchema, streamDecodedRows, unmarshalRecords } from "@scrt/index";

const dsl = await fs.promises.readFile("./data.scrt", "utf8");
const doc = parseSchema(dsl);
const messageSchema = doc.schema("Message");
if (!messageSchema) throw new Error("schema missing");

const payload = marshalRecords(messageSchema, [
  { MsgID: 1n, User: 42n, Text: "hey", Lang: "en", CreatedAt: new Date() },
  { MsgID: 2n, User: 1001n, Text: "hola", Lang: "es", CreatedAt: Date.now() },
]);

const decoded = unmarshalRecords(payload, messageSchema, {
  numericMode: "auto",     // return JS numbers when safe, bigint otherwise
  temporalMode: "date",    // emit Date objects ("string" yields RFC3339 text)
  durationMode: "string",  // render Go-style 1h2m3s strings
});

for (const row of streamDecodedRows(payload, messageSchema, { objectFactory: () => new Map() })) {
  console.log(row.get("Text"));
}
```

Key features:

- `marshalRecords(schema, source)` accepts plain objects, `Map<string, any>`, or
  existing `Row` instances and emits a binary `Uint8Array` identical to the Go
  encoder.
- `unmarshalRecords` builds an array of objects (or maps via `objectFactory`).
- `streamDecodedRows` exposes a generator for large payloads so data can be
  processed incrementally without materialising every row.
- Flexible decode options mirror Go’s high-level API: zero-copy bytes,
  automatic bigint/number promotion, and configurable temporal/duration
  rendering.

All primitives (`Schema`, `Row`, `Writer`, `Reader`, temporal helpers, etc.) are
re-exported via `src/index.ts` so applications can combine low-level and
high-level APIs as needed.

## Binary HTTP Server (No JSON)

Run `go run ./cmd/scrt-server -addr :8080` to start a fully binary backend that
never emits or accepts JSON. The process boots with an empty registry—upload
schemas through the `/schemas` endpoint (or via the TypeScript helper)
before pushing payloads. The server keeps SCRT schemas and payloads in memory
and exposes the following routes:

- `GET /schemas` → newline-delimited schema names (`text/plain`).
- `POST /schemas/{name}` / `GET /schemas/{name}` / `DELETE ...` → raw SCRT
  DSL text for CRUD without JSON envelopes.
- `POST /records/{schema}` → persist SCRT binary payloads exactly as
  produced by the Go/TypeScript codecs.
- `GET /records/{schema}` → retrieve the stored SCRT stream.
- `GET /bundle?schema=Name` → compact binary envelope (`SCB1`)
  containing schema fingerprints, raw DSL, and the current payload.

The Vite UI (`src/main.ts`) uses `fetch` with `arrayBuffer()` and the shared
TypeScript codecs to manage schemas, upload SCRT payloads, and stream decoded
rows—there are no JSON round-trips anywhere in the flow.

The server now enables CORS by default so a Vite/JS client can interact directly
from a browser at a different origin. Preflight requests (OPTIONS) are handled
by the server and the allowed headers include Content-Type, Accept, and
Authorization.

## CRUD Walkthrough (Go Server + TypeScript Client)

The Go server keeps schemas and row payloads fully in memory, so the simplest
CRUD flow is: create a schema, push binary records, read them back (either as
raw SCRT or via the bundle endpoint), then delete the schema when you are
done.

1. Start the server (it launches with no schemas, so you will upload one
  in the next step):

   ```bash
  go run ./cmd/scrt-server -addr :8080
   ```

2. Point a TypeScript client at the running instance. The snippet below can run
   under `tsx`/`ts-node` and mirrors what the frontend does without touching
   JSON.

   ```ts
   import {
     marshalRecords,
     parseSchema,
     ScrtHttpClient,
     unmarshalRecords,
   } from "./src";

   const client = new ScrtHttpClient("http://localhost:8080");
   const schemaName = "Message";
   const schemaDsl = `@schema:Message
   @field MsgID uint64 auto_increment
   @field User  uint64
   @field Text  string
   @field Lang  string default "en"

   @Message
   1, 42, "hi"`;

  // C: create or overwrite the schema on the server.
  await client.saveSchema(schemaName, schemaDsl);

  // R: round-trip the raw DSL or list schemas if you need confirmation.
  const available = await client.listSchemas();
  console.log("schemas", available);

   // Marshal a pair of rows locally and upload them as the canonical payload.
  const parsed = parseSchema(schemaDsl);
  const messageSchema = parsed.schema(schemaName);
   if (!messageSchema) throw new Error("schema missing");

   const payload = marshalRecords(messageSchema, [
     { MsgID: 1n, User: 42n, Text: "hi" },
     { MsgID: 2n, User: 7n, Text: "hola", Lang: "es" },
   ]);
  await client.uploadRecords(schemaName, payload);

   // R: fetch the binary rows back and decode them on the client side.
  const stored = await client.fetchRecords(schemaName);
   const decoded = unmarshalRecords(stored, messageSchema, { numericMode: "bigint" });
   console.log(decoded);

   // U: replace the payload with an updated set of rows (no JSON patches).
   const updatedPayload = marshalRecords(messageSchema, [
     ...decoded,
     { MsgID: 3n, User: 999n, Text: "update" },
   ]);
  await client.uploadRecords(schemaName, updatedPayload);

   // Optional: pull the compact bundle (schema text + payload) in one go.
  const bundle = await client.fetchBundle(schemaName);
   const bundleDoc = parseSchema(bundle.schemaText);
   const bundleSchema = bundleDoc.schema(bundle.schemaName);
   if (bundleSchema) {
     console.log(unmarshalRecords(bundle.payload, bundleSchema));
   }

  // D: remove the schema (and its payload) when you are finished testing.
  await client.deleteSchema(schemaName);
   ```

Every request in the flow above carries only SCRT DSL text or binary payloads,
so the server and client stay JSON-free end to end. Uploading records always
stores the most recent binary blob, which keeps updates deterministic while
still supporting append/replace semantics at the application layer.

## Publishing the TypeScript Bundle

Use Vite’s library build to ship the shared codecs:

```bash
npm run bundle:ts
```

This produces ESM + CJS bundles (with sourcemaps) under `dist/` so downstream
services or package registries can consume the exact same SCRT implementation
that powers the frontend.

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

For a scripted walk-through of the CRUD sample, open
`http://localhost:5173/showcase.html`. The showcase page uploads a demo schema,
pushes rows, fetches the binary bundle, and renders the decoded payload so you
can inspect every step without leaving the browser.

Both the Go backend and the TypeScript helpers operate on the same `.scrt`
definitions, fingerprints, and field order, ensuring marshaling/unmarshaling
stays deterministic across platforms.
