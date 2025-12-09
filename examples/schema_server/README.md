# Schema Server + TypeScript Client Example

This example shows how the Go HTTP schema service and the TypeScript helpers share
the same cached SCRT schema while exchanging real payloads.

## 1. Start the Go schema server

```bash
cd /Users/sujit/Sites/scrt
GOEXPERIMENT=arenas go run ./examples/schema_server -schema data.scrt -addr :8080
```

The server exposes:

- `GET /schemas/index` – JSON index of schemas + fingerprints.
- `GET /schemas/doc` – raw `.scrt` schema file (cached server-side).
- `GET /api/messages` – binary SCRT payload by default, or textual SCRT when `?format=text` is supplied (handy for the current TS example).

## 2. Run the TypeScript client

Use Node 18+ (for built-in `fetch`). Any TS runner works; the example below uses `ts-node`:

```bash
cd /Users/sujit/Sites/scrt
SCRT_SERVER=http://localhost:8080 npx ts-node examples/schema_server/client.ts
```

The script will:

1. Download the schema document via `SchemaHttpClient` (which caches fingerprints).
2. Fetch `/api/messages?format=text` so it can parse the textual SCRT payload without needing the binary decoder yet.
3. Decode the rows into typed objects using the same schema metadata.

You should see output similar to:

```
Decoded 3 messages
#1 [user=1001] (en) Hello World!
#2 [user=1002] (es) Hola Mundo!
#3 [user=1003] (fr) Bonjour Monde!
```

This end-to-end loop exercises the new marshaling/unmarshaling helpers and verifies
that both Go and TypeScript code paths remain in sync via the HTTP schema cache.
