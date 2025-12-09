# Schema Server + TypeScript Client Example

This example shows how the Go HTTP schema service and the TypeScript helpers share
the same cached SCRT schema while exchanging real payloads.

## 1. Start the Go schema server

```bash
cd /Users/sujit/Sites/scrt
GOEXPERIMENT=arenas go run ./examples/schema_server \
	-schema message=data.scrt \
	-schema user=/path/to/user.scrt \
	-addr :8080 -base-path /schemas

Each `-schema` flag accepts either `name=path` or just the file path (the name
defaults to the filename without extension). Add as many as you need: `user.scrt`,
`message.scrt`, and any other domain-specific documents.
```

Key endpoints (all under the configurable `-base-path`, default `/schemas`):

- `GET /schemas/documents` – list every loaded SCRT document with fingerprint + metadata.
- `POST /schemas/documents?name={doc}` – upload/replace a schema by streaming raw `.scrt` DSL (body is the text file, no JSON required).
- `GET /schemas/documents/{doc}` – download the DSL text for an existing document.
- `POST /schemas/documents/{doc}/records/{schema}` – attach binary SCRT rows to a schema (body is the binary payload produced by `scrt.Marshal`).
- `GET /schemas/documents/{doc}/records/{schema}` – read the last stored payload for that schema/doc pair.
- `GET /schemas/bundle?document={doc}&schema={Schema}` – compact binary response (see below) that streams the DSL + optional payload in one hop.
- `GET /schemas/index?document={doc}` / `GET /schemas/doc?document={doc}` – debugging helpers for a specific document.

`/schemas/bundle` now speaks a binary envelope so no JSON ever crosses the wire. The layout is:

| Offset | Size       | Description |
|--------|------------|-------------|
| 0-3    | 4 bytes    | Magic `SCB1` |
| 4      | 1 byte     | Protocol version (`1`) |
| 5-12   | 8 bytes    | Document fingerprint (big-endian uint64) |
| 13-20  | 8 bytes    | `updatedAt` as Unix nanos (big-endian int64) |
| 21+    | u32 + data | Schema DSL length + UTF-8 bytes |
| …      | u16 + rows | Schema index count followed by entries (`u16 name length + utf-8 name + u64 fingerprint + u16 field count`) |
| …      | 1 byte     | Payload flag (0 = none, 1 = present) |
| …      | if flag=1  | `u16` schema name length + name, `u64` schema fingerprint, `u32` payload length + raw SCRT bytes |

The TypeScript client decodes this via `SchemaHttpClient`, so you never need to parse JSON to hydrate schemas or payloads.

## 2. Run the TypeScript client

Use Node 18+ (for built-in `fetch`). Any TS runner works; the example below uses `ts-node`:

```bash
cd /Users/sujit/Sites/scrt
SCRT_SERVER=http://localhost:8080 SCRT_DOCUMENT=message \
	npx ts-node examples/schema_server/client.ts
```

The script will:

1. Request `/schemas/bundle?document=message&schema=Message` which returns the schema text plus the binary SCRT payload in one round-trip.
2. Parse the schema via `SchemaHttpClient` and decode the bundled payload with the shared metadata.

You should see output similar to:

```
Decoded 3 messages from document "message"
#1 [user=1001] (en) Hello World!
#2 [user=1002] (es) Hola Mundo!
#3 [user=1003] (fr) Bonjour Monde!
```

This end-to-end loop exercises the new marshaling/unmarshaling helpers and verifies
that both Go and TypeScript code paths remain in sync via the HTTP schema cache.
