# Go CRUD Example (Server API)

This example performs end-to-end CRUD using a provided SCRT file:

1. Upserts all schemas in dependency order (`User`, `Product`, `Order`).
2. Uploads records for each schema.
3. Reads back records for a target schema.
4. Updates one row with `PATCH /records/{schema}/row/{field}/{key}`.
5. Deletes one row with `DELETE /records/{schema}/row/{field}/{key}`.

## Run

Start the server:

```bash
go run ./cmd/scrt-server -addr :8080
```

In another terminal, run the CRUD example:

```bash
go run ./examples/crud_server \
  -base http://localhost:8080 \
  -scrt examples/complete_feature_showcase/all_reference_cases.scrt \
  -schema Order \
  -key-field OrderID \
  -update-key 1001 \
  -delete-key 1002
```

If your SCRT file uses a different target schema/key field, set `-schema` and `-key-field` accordingly.
