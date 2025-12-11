import { ScrtHttpClient } from "./client";
import { marshalRecords, unmarshalRecords } from "./marshal";
import { FieldKind, Schema, Document as ScrtDocument, parseSchema } from "./schema";

interface CachedDocument {
    text: string;
    doc: ScrtDocument;
}

const statusEl = document.getElementById("status") as HTMLPreElement;
const serverInput = document.getElementById("server") as HTMLInputElement;
const schemaSelect = document.getElementById("schema-select") as HTMLSelectElement;
const schemaPreview = document.getElementById("schema-preview") as HTMLDivElement;
const recordsList = document.getElementById("records") as HTMLUListElement;
const documentForm = document.getElementById("document-form") as HTMLFormElement;
const recordForm = document.getElementById("record-form") as HTMLFormElement;
const refreshDocsBtn = document.getElementById("refresh-docs") as HTMLButtonElement;
const loadRecordsBtn = document.getElementById("load-records") as HTMLButtonElement;
const recordModeSelect = document.getElementById("record-mode") as HTMLSelectElement;
const deleteRecordsBtn = document.getElementById("delete-records") as HTMLButtonElement;
const recordFieldInput = document.getElementById("record-field") as HTMLInputElement;
const recordKeyInput = document.getElementById("record-key") as HTMLInputElement;
const recordRowTextarea = document.getElementById("record-row") as HTMLTextAreaElement;
const loadRecordBtn = document.getElementById("load-record") as HTMLButtonElement;
const saveRecordBtn = document.getElementById("save-record") as HTMLButtonElement;
const deleteRecordRowBtn = document.getElementById("delete-record-row") as HTMLButtonElement;

const client = new ScrtHttpClient(serverInput.value || undefined);
const cache = new Map<string, CachedDocument>();

const embeddedDocuments: Record<string, string> = {
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

let currentSchemaName: string | null = null;
let currentSchemaDef: Schema | null = null;
let currentLookupField: string | null = null;
let currentRows: Record<string, unknown>[] = [];

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

recordsList.addEventListener("click", (event) => {
    handleRecordListClick(event as MouseEvent);
});

loadRecordBtn.addEventListener("click", () => {
    void loadSingleRecord();
});

saveRecordBtn.addEventListener("click", () => {
    void saveSingleRecord();
});

deleteRecordRowBtn.addEventListener("click", () => {
    void deleteSingleRecord();
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

async function refreshSchemas(): Promise<void> {
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
    } catch (err) {
        logError(err);
    }
}

async function ensureSchemaCached(name: string): Promise<void> {
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

function renderSchema(schemaName: string, doc: ScrtDocument): void {
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

function describeField(field: Schema["fields"][number]): string {
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

function renderSchemaDetails(schemaName: string): void {
    const cached = cache.get(schemaName);
    if (!cached) {
        return;
    }
    renderSchema(schemaName, cached.doc);
}

async function loadRecords(): Promise<void> {
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
        currentSchemaName = schemaName;
        currentSchemaDef = schema;
        currentLookupField = pickLookupField(schema);
        if (currentLookupField && !recordFieldInput.value.trim()) {
            recordFieldInput.value = currentLookupField;
        }
        currentRows = records;
        renderRecords(records);
        logStatus(`Decoded ${records.length} rows (updated ${bundle.updatedAt.toISOString()}).`);
    } catch (err) {
        currentRows = [];
        currentSchemaDef = null;
        currentLookupField = null;
        renderRecords([]);
        logError(err);
    }
}

function renderRecords(rows: Record<string, unknown>[]): void {
    currentRows = rows;
    recordsList.innerHTML = "";
    if (!rows.length) {
        const li = document.createElement("li");
        li.textContent = "No rows available.";
        recordsList.append(li);
        return;
    }
    rows.forEach((row, idx) => {
        const li = document.createElement("li");
        const header = document.createElement("div");
        header.className = "record-row-header";
        const title = document.createElement("strong");
        title.textContent = `Row ${idx + 1}`;
        header.append(title);
        const keyField = currentLookupField;
        const keyValue = keyField ? normalizeKeyValue(row[keyField]) : null;
        if (keyField && keyValue) {
            const actions = document.createElement("div");
            actions.className = "record-actions";
            const editBtn = document.createElement("button");
            editBtn.type = "button";
            editBtn.textContent = "Edit";
            editBtn.dataset.action = "edit-row";
            editBtn.dataset.keyField = keyField;
            editBtn.dataset.keyValue = keyValue;
            const delBtn = document.createElement("button");
            delBtn.type = "button";
            delBtn.textContent = "Delete";
            delBtn.dataset.action = "delete-row";
            delBtn.dataset.keyField = keyField;
            delBtn.dataset.keyValue = keyValue;
            delBtn.classList.add("danger");
            actions.append(editBtn, delBtn);
            header.append(actions);
        }
        li.append(header);
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

function formatValue(value: unknown): string {
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

function normalizeKeyValue(value: unknown): string | null {
    if (value === null || value === undefined) {
        return null;
    }
    if (value instanceof Date) {
        return value.toISOString();
    }
    const asString = String(value).trim();
    if (!asString) {
        return null;
    }
    return asString;
}

function formatRowForEditor(row: Record<string, unknown>, schema: Schema): string {
    const cells = schema.fields.map((field) => formatCellLiteral(row[field.name], field.valueKind()));
    return cells.join(", ").trim();
}

function formatCellLiteral(value: unknown, kind: FieldKind): string {
    if (value === null || value === undefined) {
        return "";
    }
    const raw = String(value);
    if (needsQuotedLiteral(kind)) {
        return `"${escapeLiteral(raw)}"`;
    }
    return raw;
}

function needsQuotedLiteral(kind: FieldKind): boolean {
    switch (kind) {
        case FieldKind.String:
        case FieldKind.Bytes:
        case FieldKind.TimestampTZ:
        case FieldKind.Date:
        case FieldKind.DateTime:
        case FieldKind.Timestamp:
        case FieldKind.Duration:
            return true;
        default:
            return false;
    }
}

function escapeLiteral(value: string): string {
    return value.replace(/"/g, "\"\"");
}

function pickLookupField(schema: Schema): string | null {
    const auto = schema.fields.find((field) => field.autoIncrement);
    if (auto) {
        return auto.name;
    }
    const uniqueAttr = schema.fields.find((field) => field.attributes.some((attr) => attr.toLowerCase() === "unique"));
    if (uniqueAttr) {
        return uniqueAttr.name;
    }
    const uuidAttr = schema.fields.find((field) => field.attributes.some((attr) => attr.toLowerCase().includes("uuid")));
    if (uuidAttr) {
        return uuidAttr.name;
    }
    const idSuffix = schema.fields.find((field) => field.name.toLowerCase().endsWith("id"));
    if (idSuffix) {
        return idSuffix.name;
    }
    return schema.fields.length ? schema.fields[0].name : null;
}

function handleRecordListClick(event: MouseEvent): void {
    const target = event.target as HTMLElement;
    if (!(target instanceof HTMLButtonElement)) {
        return;
    }
    const action = target.dataset.action;
    if (!action) {
        return;
    }
    const field = target.dataset.keyField || currentLookupField || "";
    const key = target.dataset.keyValue || "";
    if (!field || !key) {
        logStatus("Record is missing a lookup key.");
        return;
    }
    recordFieldInput.value = field;
    recordKeyInput.value = key;
    if (action === "edit-row") {
        void loadSingleRecord();
    } else if (action === "delete-row") {
        void deleteSingleRecord();
    }
}

function reloadRecordsIfCurrent(schemaName: string): void {
    if (schemaName && schemaName === schemaSelect.value) {
        void loadRecords();
    }
}

async function saveSchema(): Promise<void> {
    const name = (document.getElementById("document-name") as HTMLInputElement).value.trim();
    const body = (document.getElementById("document-body") as HTMLTextAreaElement).value;
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
    } catch (err) {
        logError(err);
    }
}

async function uploadRecords(): Promise<void> {
    const schemaNameInput = (document.getElementById("record-schema") as HTMLInputElement).value.trim();
    const schemaName = schemaNameInput || schemaSelect.value;
    const textInput = (document.getElementById("record-text") as HTMLTextAreaElement).value;
    const fileInput = document.getElementById("record-file") as HTMLInputElement;
    const mode = recordModeSelect?.value === "replace" ? "replace" : "append";
    if (!schemaName) {
        logStatus("Schema name is required.");
        return;
    }
    try {
        let payload: Uint8Array;
        if (fileInput.files && fileInput.files.length) {
            const buffer = await fileInput.files[0].arrayBuffer();
            payload = new Uint8Array(buffer);
        } else {
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
    } catch (err) {
        logError(err);
    }
}

async function deleteRecords(): Promise<void> {
    const schemaNameInput = (document.getElementById("record-schema") as HTMLInputElement).value.trim();
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
    } catch (err) {
        logError(err);
    }
}

async function loadSingleRecord(): Promise<void> {
    const target = resolveRecordTarget();
    if (!target) {
        return;
    }
    try {
        const schema = await schemaForName(target.schema);
        currentSchemaDef = schema;
        logStatus(`Loading ${target.schema} ${target.field}=${target.key}...`);
        const envelope = await client.fetchRecordRow(target.schema, target.field, target.key);
        const rowText = formatRowForEditor(envelope.row ?? {}, schema);
        recordRowTextarea.value = rowText;
        logStatus(`Loaded ${target.schema} ${target.field}=${target.key}. Edit the row and click Save.`);
    } catch (err) {
        recordRowTextarea.value = "";
        logError(err);
    }
}

async function saveSingleRecord(): Promise<void> {
    const target = resolveRecordTarget({ allowEmptyKey: true });
    if (!target) {
        return;
    }
    let schema: Schema;
    try {
        schema = await schemaForName(target.schema);
    } catch (err) {
        logError(err);
        return;
    }
    let rows: Record<string, unknown>[];
    try {
        rows = parseUserRows(recordRowTextarea.value, schema);
    } catch (err) {
        logError(err);
        return;
    }
    if (!rows.length) {
        logStatus("Provide a SCRT row to save.");
        return;
    }
    if (rows.length > 1) {
        logStatus("Only one row can be edited at a time.");
        return;
    }
    const row = rows[0];
    if (!target.key) {
        try {
            logStatus(`Creating new ${target.schema} row...`);
            await createSingleRecord(target.schema, schema, row);
            recordRowTextarea.value = formatRowForEditor(row, schema);
            reloadRecordsIfCurrent(target.schema);
            logStatus(`Created ${target.schema} row (IDs assigned automatically).`);
        } catch (err) {
            logError(err);
        }
        return;
    }
    row[target.field] = target.key;
    let payload: Uint8Array;
    try {
        payload = marshalRecords(schema, [row]);
    } catch (err) {
        logError(err);
        return;
    }
    try {
        logStatus(`Saving ${target.schema} ${target.field}=${target.key}...`);
        const envelope = await client.updateRecordRow(target.schema, target.field, target.key, payload);
        recordRowTextarea.value = formatRowForEditor(envelope.row ?? row, schema);
        reloadRecordsIfCurrent(target.schema);
        logStatus(`Updated ${target.schema} ${target.field}=${target.key}.`);
    } catch (err) {
        logError(err);
    }
}

async function deleteSingleRecord(): Promise<void> {
    const target = resolveRecordTarget();
    if (!target) {
        return;
    }
    try {
        logStatus(`Deleting ${target.schema} ${target.field}=${target.key}...`);
        await client.deleteRecordRow(target.schema, target.field, target.key);
        recordRowTextarea.value = "";
        recordKeyInput.value = "";
        reloadRecordsIfCurrent(target.schema);
        logStatus(`Deleted ${target.schema} ${target.field}=${target.key}.`);
    } catch (err) {
        logError(err);
    }
}

async function createSingleRecord(schemaName: string, schema: Schema, row: Record<string, unknown>): Promise<void> {
    const payload = marshalRecords(schema, [row]);
    await client.uploadRecords(schemaName, payload, { mode: "append" });
}

async function schemaForName(schemaName: string): Promise<Schema> {
    const cached = await ensureSchemaCachedOrBootstrap(schemaName);
    const schema = cached.doc.schema(schemaName);
    if (!schema) {
        throw new Error(`Schema ${schemaName} is not available locally.`);
    }
    return schema;
}

interface RecordTargetOptions {
    allowEmptyKey?: boolean;
}

function resolveRecordTarget(options: RecordTargetOptions = {}): { schema: string; field: string; key: string } | null {
    const schemaNameInput = (document.getElementById("record-schema") as HTMLInputElement).value.trim();
    const schemaName = schemaNameInput || schemaSelect.value;
    if (!schemaName) {
        logStatus("Schema name is required.");
        return null;
    }
    let field = recordFieldInput.value.trim();
    if (!field && currentLookupField) {
        field = currentLookupField;
        recordFieldInput.value = currentLookupField;
    }
    if (!field) {
        logStatus("Lookup field is required.");
        return null;
    }
    const key = recordKeyInput.value.trim();
    if (!key && !options.allowEmptyKey) {
        logStatus("Lookup key is required.");
        return null;
    }
    return { schema: schemaName, field, key };
}

async function ensureSchemaCachedOrBootstrap(name: string): Promise<CachedDocument> {
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

async function maybeBootstrapSchema(name: string, originalError?: unknown): Promise<boolean> {
    const desired = name.toLowerCase();
    const schemaNameInput = (document.getElementById("document-name") as HTMLInputElement).value.trim();
    const schemaBodyInput = (document.getElementById("document-body") as HTMLTextAreaElement).value;
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

function parseUserRows(raw: string, schema: Schema): Record<string, unknown>[] {
    const rows: Record<string, unknown>[] = [];
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
        const positional: string[] = [];
        const named = new Map<string, string>();
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
        const availableFieldCount = schema.fields.filter((field) => !named.has(field.name)).length;
        if (positional.length > availableFieldCount) {
            throw new Error(`Row has more positional values than available fields (${positional.length} > ${availableFieldCount})`);
        }
        let positionalIndex = 0;
        const row: Record<string, unknown> = {};
        schema.fields.forEach((field) => {
            if (named.has(field.name)) {
                const explicit = stripQuotes(named.get(field.name) ?? "");
                if (explicit) {
                    row[field.name] = coerceLiteral(field.valueKind(), explicit);
                }
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

function isDslDirective(line: string): boolean {
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

function parseNamedAssignment(token: string, fields: Map<string, Schema["fields"][number]>): { field: Schema["fields"][number]; value: string } | null {
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

function splitCells(line: string): string[] {
    const cells: string[] = [];
    let current = "";
    let quote: string | null = null;
    for (let i = 0; i < line.length; i += 1) {
        const ch = line[i];
        if (quote) {
            if (ch === quote) {
                if (line[i + 1] === quote) {
                    current += ch;
                    i += 1;
                } else {
                    quote = null;
                }
            } else {
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

function stripQuotes(token: string): string {
    if (!token) {
        return token;
    }
    if ((token.startsWith('"') && token.endsWith('"')) || (token.startsWith("'") && token.endsWith("'")) || (token.startsWith("`") && token.endsWith("`"))) {
        return token.slice(1, -1);
    }
    return token;
}

function coerceLiteral(kind: FieldKind, value: string): unknown {
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

function logStatus(message: string): void {
    statusEl.textContent = message;
}

function logError(err: unknown): void {
    const message = err instanceof Error ? err.message : String(err);
    statusEl.textContent = `Error: ${message}`;
    console.error(err);
}
