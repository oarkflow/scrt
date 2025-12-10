import { ScrtHttpClient } from "./client";
import { marshalRecords, unmarshalRecords } from "./marshal";
import { FieldKind, parseSchema } from "./schema";
const statusEl = document.getElementById("status");
const serverInput = document.getElementById("server");
const schemaSelect = document.getElementById("schema-select");
const schemaPreview = document.getElementById("schema-preview");
const recordsList = document.getElementById("records");
const documentForm = document.getElementById("document-form");
const recordForm = document.getElementById("record-form");
const refreshDocsBtn = document.getElementById("refresh-docs");
const loadRecordsBtn = document.getElementById("load-records");
const recordModeSelect = document.getElementById("record-mode");
const deleteRecordsBtn = document.getElementById("delete-records");
const client = new ScrtHttpClient(serverInput.value || undefined);
const cache = new Map();
const embeddedDocuments = {
    message: `@schema:Message
@field MsgID uint64 auto_increment
@field User  uint64
@field Text  string
@field Lang  string default "en"

@Message
1, 42, "hello", "en"
2, 7, "hola", "es"
`,
};
serverInput.addEventListener("change", () => {
    client.setBaseUrl(serverInput.value);
});
refreshDocsBtn.addEventListener("click", () => {
    void refreshSchemas();
});
loadRecordsBtn.addEventListener("click", () => {
    void loadRecords();
});
deleteRecordsBtn.addEventListener("click", () => {
    void deleteRecords();
});
schemaSelect.addEventListener("change", () => {
    void ensureSchemaCachedOrBootstrap(schemaSelect.value).then(() => {
        logStatus(`Schema set to ${schemaSelect.value}`);
        renderSchemaDetails(schemaSelect.value);
    }).catch(logError);
});
documentForm.addEventListener("submit", (event) => {
    event.preventDefault();
    void saveSchema();
});
recordForm.addEventListener("submit", (event) => {
    event.preventDefault();
    void uploadRecords();
});
void refreshSchemas();
async function refreshSchemas() {
    try {
        logStatus("Loading schemas...");
        const schemas = await client.listSchemas();
        schemaSelect.innerHTML = "";
        schemas.forEach((name) => {
            const option = document.createElement("option");
            option.value = name;
            option.textContent = name;
            schemaSelect.append(option);
        });
        if (schemas.length === 0) {
            logStatus("No schemas uploaded yet.");
            schemaPreview.innerHTML = `<div class="schema-preview-message">Upload a schema to begin.</div>`;
            return;
        }
        schemaSelect.value = schemas[0];
        await ensureSchemaCachedOrBootstrap(schemas[0]);
        renderSchemaDetails(schemas[0]);
    }
    catch (err) {
        logError(err);
    }
}
async function ensureSchemaCached(name) {
    if (!name) {
        return;
    }
    if (cache.has(name)) {
        renderSchemaDetails(name);
        return;
    }
    logStatus(`Fetching schema ${name}...`);
    const text = await client.downloadSchema(name);
    const parsed = parseSchema(text);
    cache.set(name, { text, doc: parsed });
    renderSchema(name, parsed);
    logStatus(`Loaded schema ${name}.`);
}
function renderSchema(schemaName, doc) {
    const schema = doc.schema(schemaName);
    if (!schema) {
        schemaPreview.innerHTML = `<div class="schema-preview-message">Schema ${schemaName} missing.</div>`;
        return;
    }
    const fragment = document.createElement("div");
    const heading = document.createElement("h3");
    heading.textContent = `${schema.name} (${schema.fields.length} fields)`;
    fragment.append(heading);
    const list = document.createElement("dl");
    list.className = "schema-fields";
    schema.fields.forEach((field) => {
        const dt = document.createElement("dt");
        dt.textContent = field.name;
        const dd = document.createElement("dd");
        dd.textContent = describeField(field);
        list.append(dt, dd);
    });
    fragment.append(list);
    schemaPreview.innerHTML = "";
    schemaPreview.append(fragment);
}
function describeField(field) {
    const parts = [field.rawType];
    if (field.autoIncrement) {
        parts.push("auto");
    }
    if (field.attributes.length) {
        parts.push(field.attributes.join(","));
    }
    if (field.defaultValue) {
        parts.push(`default=${field.defaultValue.hashKey()}`);
    }
    return parts.join(" | ");
}
function renderSchemaDetails(schemaName) {
    const cached = cache.get(schemaName);
    if (!cached) {
        return;
    }
    renderSchema(schemaName, cached.doc);
}
async function loadRecords() {
    const schemaName = schemaSelect.value;
    if (!schemaName) {
        logStatus("Select a schema first.");
        return;
    }
    try {
        logStatus(`Fetching bundle for ${schemaName}...`);
        const bundle = await client.fetchBundle(schemaName);
        const parsed = parseSchema(bundle.schemaText);
        const schema = parsed.schema(schemaName);
        if (!schema) {
            throw new Error(`Bundle missing schema ${schemaName}`);
        }
        const records = unmarshalRecords(bundle.payload, schema, {
            numericMode: "auto",
            temporalMode: "string",
            durationMode: "string",
        });
        renderRecords(records);
        logStatus(`Decoded ${records.length} rows (updated ${bundle.updatedAt.toISOString()}).`);
    }
    catch (err) {
        renderRecords([]);
        logError(err);
    }
}
function renderRecords(rows) {
    recordsList.innerHTML = "";
    if (!rows.length) {
        const li = document.createElement("li");
        li.textContent = "No rows available.";
        recordsList.append(li);
        return;
    }
    rows.forEach((row, idx) => {
        const li = document.createElement("li");
        const title = document.createElement("strong");
        title.textContent = `Row ${idx + 1}`;
        li.append(title);
        const dl = document.createElement("dl");
        Object.entries(row).forEach(([key, value]) => {
            const dt = document.createElement("dt");
            dt.textContent = key;
            const dd = document.createElement("dd");
            dd.textContent = formatValue(value);
            dl.append(dt, dd);
        });
        li.append(dl);
        recordsList.append(li);
    });
}
function formatValue(value) {
    if (value === null || value === undefined) {
        return "";
    }
    if (value instanceof Date) {
        return value.toISOString();
    }
    if (value instanceof Uint8Array) {
        return `bytes(${value.length})`;
    }
    return String(value);
}
async function saveSchema() {
    const name = document.getElementById("document-name").value.trim();
    const body = document.getElementById("document-body").value;
    if (!name || !body.trim()) {
        logStatus("Schema name and body are required.");
        return;
    }
    try {
        logStatus(`Saving schema ${name}...`);
        await client.saveSchema(name, body);
        cache.delete(name);
        await refreshSchemas();
        logStatus(`Schema ${name} saved.`);
    }
    catch (err) {
        logError(err);
    }
}
async function uploadRecords() {
    const schemaNameInput = document.getElementById("record-schema").value.trim();
    const schemaName = schemaNameInput || schemaSelect.value;
    const textInput = document.getElementById("record-text").value;
    const fileInput = document.getElementById("record-file");
    const mode = recordModeSelect?.value === "replace" ? "replace" : "append";
    if (!schemaName) {
        logStatus("Schema name is required.");
        return;
    }
    try {
        let payload;
        if (fileInput.files && fileInput.files.length) {
            const buffer = await fileInput.files[0].arrayBuffer();
            payload = new Uint8Array(buffer);
        }
        else {
            const cached = await ensureSchemaCachedOrBootstrap(schemaName);
            if (!cached) {
                throw new Error(`Schema ${schemaName} is not available locally.`);
            }
            const schema = cached.doc.schema(schemaName);
            if (!schema) {
                throw new Error(`Schema ${schemaName} not found.`);
            }
            const rows = parseUserRows(textInput, schema);
            if (!rows.length) {
                throw new Error("No rows to marshal. Provide a file or text rows.");
            }
            payload = marshalRecords(schema, rows);
        }
        logStatus(`Uploading ${payload.byteLength} bytes to schema ${schemaName} (${mode})...`);
        await client.uploadRecords(schemaName, payload, { mode });
        logStatus(mode === "replace" ? `Replaced records for ${schemaName}.` : `Appended records to ${schemaName}.`);
    }
    catch (err) {
        logError(err);
    }
}
async function deleteRecords() {
    const schemaNameInput = document.getElementById("record-schema").value.trim();
    const schemaName = schemaNameInput || schemaSelect.value;
    if (!schemaName) {
        logStatus("Schema name is required.");
        return;
    }
    try {
        logStatus(`Deleting records for ${schemaName}...`);
        await client.deleteRecords(schemaName);
        renderRecords([]);
        logStatus(`Records cleared for ${schemaName}.`);
    }
    catch (err) {
        logError(err);
    }
}
async function ensureSchemaCachedOrBootstrap(name) {
    await ensureSchemaCached(name).catch(async (err) => {
        const bootstrapped = await maybeBootstrapSchema(name, err);
        if (!bootstrapped) {
            throw err;
        }
    });
    const cached = cache.get(name);
    if (!cached) {
        throw new Error(`Schema ${name} unavailable`);
    }
    return cached;
}
async function maybeBootstrapSchema(name, originalError) {
    const desired = name.toLowerCase();
    const schemaNameInput = document.getElementById("document-name").value.trim();
    const schemaBodyInput = document.getElementById("document-body").value;
    const embedded = embeddedDocuments[desired];
    const candidate = schemaBodyInput.trim() || embedded;
    if (!candidate) {
        return false;
    }
    if (schemaNameInput && schemaNameInput.toLowerCase() !== desired && !embedded) {
        throw originalError ?? new Error(`Schema ${name} not found. Provide the matching schema DSL and save it first.`);
    }
    logStatus(`Schema ${name} missing on server. Creating it now...`);
    await client.saveSchema(name, candidate);
    cache.delete(name);
    await refreshSchemas();
    schemaSelect.value = name;
    await ensureSchemaCached(name);
    logStatus(`Schema ${name} created and cached.`);
    return true;
}
function parseUserRows(raw, schema) {
    const rows = [];
    const fieldLookup = new Map(schema.fields.map((field) => [field.name, field]));
    raw.split(/\r?\n/).forEach((line) => {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith("#")) {
            return;
        }
        if (isDslDirective(trimmed)) {
            return;
        }
        const cells = splitCells(trimmed);
        const positional = [];
        const named = new Map();
        cells.forEach((cell) => {
            const token = cell.trim();
            if (!token) {
                return;
            }
            const assignment = parseNamedAssignment(token, fieldLookup);
            if (assignment) {
                named.set(assignment.field.name, assignment.value);
                return;
            }
            positional.push(token);
        });
        const freeFieldCount = schema.fields.filter((field) => {
            if (field.autoIncrement) {
                return false;
            }
            return !named.has(field.name);
        }).length;
        if (positional.length > freeFieldCount) {
            throw new Error(`Row has more positional values than available fields (${positional.length} > ${freeFieldCount})`);
        }
        let positionalIndex = 0;
        const row = {};
        schema.fields.forEach((field) => {
            if (named.has(field.name)) {
                const explicit = stripQuotes(named.get(field.name) ?? "");
                if (explicit) {
                    row[field.name] = coerceLiteral(field.valueKind(), explicit);
                }
                return;
            }
            if (field.autoIncrement) {
                return;
            }
            if (positionalIndex >= positional.length) {
                return;
            }
            const value = stripQuotes(positional[positionalIndex]);
            positionalIndex += 1;
            if (!value) {
                return;
            }
            row[field.name] = coerceLiteral(field.valueKind(), value);
        });
        rows.push(row);
    });
    return rows;
}
function isDslDirective(line) {
    if (!line.startsWith("@")) {
        return false;
    }
    if (line.includes("=")) {
        return false;
    }
    if (line.includes(",")) {
        return false;
    }
    return true;
}
function parseNamedAssignment(token, fields) {
    if (!token.startsWith("@")) {
        return null;
    }
    const eq = token.indexOf("=");
    if (eq === -1) {
        return null;
    }
    const name = token.slice(1, eq).trim();
    const value = token.slice(eq + 1).trim();
    if (!name) {
        throw new Error("Named assignment missing field name");
    }
    const field = fields.get(name);
    if (!field) {
        throw new Error(`Unknown field ${name} in assignment`);
    }
    return { field, value };
}
function splitCells(line) {
    const cells = [];
    let current = "";
    let quote = null;
    for (let i = 0; i < line.length; i += 1) {
        const ch = line[i];
        if (quote) {
            if (ch === quote) {
                if (line[i + 1] === quote) {
                    current += ch;
                    i += 1;
                }
                else {
                    quote = null;
                }
            }
            else {
                current += ch;
            }
            continue;
        }
        if (ch === '"' || ch === "'" || ch === "`") {
            quote = ch;
            continue;
        }
        if (ch === ',') {
            cells.push(current);
            current = "";
            continue;
        }
        current += ch;
    }
    cells.push(current);
    return cells.map((token) => token.trim());
}
function stripQuotes(token) {
    if (!token) {
        return token;
    }
    if ((token.startsWith('"') && token.endsWith('"')) || (token.startsWith("'") && token.endsWith("'")) || (token.startsWith("`") && token.endsWith("`"))) {
        return token.slice(1, -1);
    }
    return token;
}
function coerceLiteral(kind, value) {
    switch (kind) {
        case FieldKind.Bool:
            return value;
        case FieldKind.Uint64:
        case FieldKind.Int64:
        case FieldKind.Float64:
        case FieldKind.Date:
        case FieldKind.DateTime:
        case FieldKind.Timestamp:
        case FieldKind.TimestampTZ:
        case FieldKind.Duration:
        case FieldKind.Ref:
            return value;
        case FieldKind.Bytes:
            return value;
        default:
            return value;
    }
}
function logStatus(message) {
    statusEl.textContent = message;
}
function logError(err) {
    const message = err instanceof Error ? err.message : String(err);
    statusEl.textContent = `Error: ${message}`;
    console.error(err);
}
