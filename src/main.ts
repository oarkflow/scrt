import { SchemaHttpClient, DocumentSummary } from "../ts/apiClient";
import { Schema, marshalRows, parseSCRT } from "../ts/scrt";

type GenericRecord = Record<string, unknown>;

const utf8Decoder = new TextDecoder();

interface UIElements {
    serverInput: HTMLInputElement;
    documentSelect: HTMLSelectElement;
    schemaSelect: HTMLSelectElement;
    schemaPreview: HTMLElement;
    loadButton: HTMLButtonElement;
    refreshButton: HTMLButtonElement;
    status: HTMLElement;
    recordList: HTMLUListElement;
    documentForm: HTMLFormElement;
    documentNameInput: HTMLInputElement;
    documentBodyInput: HTMLTextAreaElement;
    recordForm: HTMLFormElement;
    recordDocumentInput: HTMLInputElement;
    recordSchemaInput: HTMLInputElement;
    recordTextInput: HTMLTextAreaElement;
    recordFileInput: HTMLInputElement;
}

interface UIState {
    client: SchemaHttpClient;
    baseUrl: string;
    defaultDocument?: string;
    documents: DocumentSummary[];
    schemaIndex: Map<string, string[]>;
}

const elements = queryElements();
const initialBase = import.meta.env.VITE_SCRT_SERVER ?? elements.serverInput.value;
elements.serverInput.value = initialBase;
const state: UIState = {
    client: new SchemaHttpClient({ baseUrl: initialBase }),
    baseUrl: initialBase,
    defaultDocument: undefined,
    documents: [],
    schemaIndex: new Map<string, string[]>(),
};
let schemaPreviewTicket = 0;

renderSchemaPreviewMessage("Select a document and schema to inspect field layout.");

attachEventListeners();
void refreshDocuments();

function queryElements(): UIElements {
    const serverInput = mustQuery<HTMLInputElement>("#server");
    const documentSelect = mustQuery<HTMLSelectElement>("#document-select");
    const schemaSelect = mustQuery<HTMLSelectElement>("#schema-select");
    const schemaPreview = mustQuery<HTMLElement>("#schema-preview");
    const loadButton = mustQuery<HTMLButtonElement>("#load-records");
    const refreshButton = mustQuery<HTMLButtonElement>("#refresh-docs");
    const status = mustQuery<HTMLElement>("#status");
    const recordList = mustQuery<HTMLUListElement>("#records");
    const documentForm = mustQuery<HTMLFormElement>("#document-form");
    const documentNameInput = mustQuery<HTMLInputElement>("#document-name");
    const documentBodyInput = mustQuery<HTMLTextAreaElement>("#document-body");
    const recordForm = mustQuery<HTMLFormElement>("#record-form");
    const recordDocumentInput = mustQuery<HTMLInputElement>("#record-document");
    const recordSchemaInput = mustQuery<HTMLInputElement>("#record-schema");
    const recordTextInput = mustQuery<HTMLTextAreaElement>("#record-text");
    const recordFileInput = mustQuery<HTMLInputElement>("#record-file");
    return {
        serverInput,
        documentSelect,
        schemaSelect,
        schemaPreview,
        loadButton,
        refreshButton,
        status,
        recordList,
        documentForm,
        documentNameInput,
        documentBodyInput,
        recordForm,
        recordDocumentInput,
        recordSchemaInput,
        recordTextInput,
        recordFileInput,
    };
}

function mustQuery<T extends Element>(selector: string): T {
    const node = document.querySelector<T>(selector);
    if (!node) {
        throw new Error(`missing DOM node: ${selector}`);
    }
    return node;
}

function attachEventListeners(): void {
    elements.refreshButton.addEventListener("click", () => {
        void refreshDocuments();
    });
    elements.loadButton.addEventListener("click", () => {
        void loadRecords();
    });
    elements.documentSelect.addEventListener("change", () => {
        const nextDoc = elements.documentSelect.value;
        state.defaultDocument = nextDoc || undefined;
        state.client.useDocument(state.defaultDocument);
        elements.recordDocumentInput.value = nextDoc;
        void populateSchemas(nextDoc);
    });
    elements.schemaSelect.addEventListener("change", () => {
        elements.recordSchemaInput.value = elements.schemaSelect.value;
        void renderSelectedSchemaPreview();
    });
    elements.documentForm.addEventListener("submit", (event) => {
        event.preventDefault();
        void handleDocumentSubmit();
    });
    elements.recordTextInput.addEventListener("input", () => {
        void maybeApplySchemaHint(elements.recordTextInput.value);
    });
    elements.recordFileInput.addEventListener("change", () => {
        const file = elements.recordFileInput.files?.[0];
        if (!file || !isTextScrtFile(file)) {
            return;
        }
        void file
            .text()
            .then((text) => maybeApplySchemaHint(text))
            .catch(() => undefined);
    });
    elements.recordForm.addEventListener("submit", (event) => {
        event.preventDefault();
        void handleRecordSubmit();
    });
}

async function refreshDocuments(): Promise<void> {
    const baseUrl = normalizeBaseUrl(elements.serverInput.value);
    state.baseUrl = baseUrl;
    state.client = new SchemaHttpClient({ baseUrl, defaultDocument: state.defaultDocument });
    state.schemaIndex.clear();
    setStatus("Loading documents…");
    try {
        const docs = await state.client.listDocuments();
        state.documents = docs;
        populateDocumentOptions(docs);
        if (docs.length) {
            const selected = elements.documentSelect.value || docs[0].name;
            elements.documentSelect.value = selected;
            elements.recordDocumentInput.value = selected;
            state.defaultDocument = selected;
            state.client.useDocument(selected);
            await populateSchemas(selected);
            setStatus(`Loaded ${docs.length} document(s)`);
        } else {
            clearSchemaOptions("No schemas");
            setStatus("No documents found. Use the form below to create one.");
        }
    } catch (err) {
        setStatus((err as Error).message);
    }
}

async function populateSchemas(documentName: string, preferred?: string): Promise<void> {
    if (!documentName) {
        clearSchemaOptions("Select a schema");
        return;
    }
    try {
        const names = await getSchemasForDocument(documentName);
        setSchemaOptions(names);
        const target = chooseSchema(names, preferred);
        elements.schemaSelect.value = target;
        elements.recordSchemaInput.value = target;
        void renderSelectedSchemaPreview();
    } catch (err) {
        clearSchemaOptions((err as Error).message);
    }
}

async function loadRecords(): Promise<void> {
    const documentName = elements.documentSelect.value;
    const schemaName = elements.schemaSelect.value;
    if (!documentName || !schemaName) {
        setStatus("Select a document and schema first.");
        return;
    }
    setStatus(`Fetching ${documentName}/${schemaName}…`);
    try {
        const [schema, records] = await Promise.all([
            state.client.schema(schemaName, documentName),
            state.client.fetchRecords<GenericRecord>(
                schemaName,
                () => ({} as GenericRecord),
                { document: documentName },
            ),
        ]);
        renderRecords(records, schema);
        schemaPreviewTicket += 1;
        renderSchemaPreview(schema, documentName);
        setStatus(`Loaded ${records.length} record(s) from ${documentName}/${schemaName}`);
    } catch (err) {
        setStatus((err as Error).message);
        elements.recordList.replaceChildren();
    }
}

async function handleDocumentSubmit(): Promise<void> {
    const name = elements.documentNameInput.value.trim();
    const body = elements.documentBodyInput.value.trim();
    if (!name || !body) {
        setStatus("Document name and SCRT body are required.");
        return;
    }
    setStatus(`Saving document ${name}…`);
    try {
        await state.client.upsertDocument(name, body);
        setStatus(`Document ${name} saved.`);
        elements.documentForm.reset();
        await refreshDocuments();
        elements.documentSelect.value = name;
        elements.recordDocumentInput.value = name;
        await populateSchemas(name);
    } catch (err) {
        setStatus((err as Error).message);
    }
}

async function handleRecordSubmit(): Promise<void> {
    const inlineText = elements.recordTextInput.value.trim();
    const file = elements.recordFileInput.files?.[0];
    let textPayload = inlineText;
    if (!textPayload && file && isTextScrtFile(file)) {
        try {
            textPayload = (await file.text()).trim();
        } catch {
            textPayload = "";
        }
    }

    if (!textPayload && !file) {
        setStatus("Provide SCRT rows or upload a payload file.");
        return;
    }

    const schemaHints = textPayload ? detectSchemaHints(textPayload) : [];
    const hintedSchema = schemaHints[0];
    if (schemaHints.length > 1 && !elements.recordSchemaInput.value.trim()) {
        setStatus(`Multiple schema scopes detected (${schemaHints.join(", ")}). Select a schema to continue.`);
        return;
    }

    if (hintedSchema && !elements.recordSchemaInput.value.trim()) {
        syncSchemaSelection(hintedSchema);
        await ensureDocumentSelectionForSchema(hintedSchema);
    }

    let schemaName =
        elements.recordSchemaInput.value.trim() ||
        elements.schemaSelect.value ||
        hintedSchema ||
        "";
    if (!schemaName) {
        setStatus("Schema is required. Add an @Schema scope or pick one from the list.");
        return;
    }

    let documentName = elements.recordDocumentInput.value.trim() || elements.documentSelect.value;
    if (!documentName) {
        documentName = await ensureDocumentSelectionForSchema(schemaName);
    } else {
        await ensureDocumentSelectionForSchema(schemaName);
    }
    if (!documentName) {
        setStatus(`Schema ${schemaName} was not found in any loaded document.`);
        return;
    }

    let payload: Uint8Array | undefined;
    if (textPayload) {
        try {
            const rows = await parseRowsFromScrt(documentName, schemaName, textPayload);
            payload = await state.client.marshal(schemaName, rows, { document: documentName });
        } catch (err) {
            if (!file) {
                setStatus(`SCRT parse error: ${(err as Error).message}`);
                return;
            }
        }
    }
    if (!payload && file) {
        payload = new Uint8Array(await file.arrayBuffer());
    }
    if (!payload) {
        setStatus("Unable to build an SCRT payload. Check the inputs and try again.");
        return;
    }

    setStatus(`Uploading SCRT payload to ${documentName}/${schemaName}…`);
    try {
        await state.client.pushRecords(documentName, schemaName, payload);
        setStatus(`Stored SCRT payload for ${documentName}/${schemaName}.`);
        elements.recordForm.reset();
        elements.recordDocumentInput.value = documentName;
        elements.recordSchemaInput.value = schemaName;
        elements.recordTextInput.value = "";
        elements.recordFileInput.value = "";
        await loadRecords();
    } catch (err) {
        setStatus((err as Error).message);
    }
}

function populateDocumentOptions(documents: DocumentSummary[]): void {
    const current = elements.documentSelect.value;
    elements.documentSelect.replaceChildren();
    if (!documents.length) {
        const option = new Option("No documents", "", true, true);
        option.disabled = true;
        elements.documentSelect.add(option);
        return;
    }
    for (const doc of documents) {
        const label = `${doc.name} (${doc.schemaCount} schema${doc.schemaCount === 1 ? "" : "s"})`;
        const option = new Option(label, doc.name, false, doc.name === current);
        elements.documentSelect.add(option);
    }
}

function setSchemaOptions(schemaNames: string[]): void {
    const current = elements.schemaSelect.value;
    elements.schemaSelect.replaceChildren();
    if (!schemaNames.length) {
        const option = new Option("No schemas found", "", true, true);
        option.disabled = true;
        elements.schemaSelect.add(option);
        return;
    }
    for (const name of schemaNames) {
        const option = new Option(name, name, false, name === current);
        elements.schemaSelect.add(option);
    }
}

function clearSchemaOptions(label: string): void {
    elements.schemaSelect.replaceChildren();
    const option = new Option(label, "", true, true);
    option.disabled = true;
    elements.schemaSelect.add(option);
    elements.recordSchemaInput.value = "";
    schemaPreviewTicket += 1;
    renderSchemaPreviewMessage(label);
}

function renderRecords(records: GenericRecord[], schema: Schema): void {
    if (!records.length) {
        elements.recordList.replaceChildren();
        return;
    }
    const nodes = records.map((record, index) => {
        const li = document.createElement("li");
        const header = document.createElement("header");
        header.innerHTML = `<strong>@${schema.name}</strong> · row ${index + 1}`;
        const pre = document.createElement("pre");
        pre.textContent = utf8Decoder.decode(marshalRows(schema, [record], { includeSchema: false })).trim();
        li.append(header, pre);
        return li;
    });
    elements.recordList.replaceChildren(...nodes);
}

async function renderSelectedSchemaPreview(): Promise<void> {
    const documentName = elements.documentSelect.value;
    const schemaName = elements.schemaSelect.value;
    if (!documentName || !schemaName) {
        schemaPreviewTicket += 1;
        renderSchemaPreviewMessage("Select a document and schema to inspect field layout.");
        return;
    }
    const ticket = ++schemaPreviewTicket;
    renderSchemaPreviewMessage(`Loading ${documentName}/${schemaName}…`);
    try {
        const schema = await state.client.schema(schemaName, documentName);
        if (ticket !== schemaPreviewTicket) {
            return;
        }
        renderSchemaPreview(schema, documentName);
    } catch (err) {
        if (ticket !== schemaPreviewTicket) {
            return;
        }
        renderSchemaPreviewMessage((err as Error).message, true);
    }
}

function renderSchemaPreview(schema?: Schema, documentName?: string): void {
    if (!schema) {
        renderSchemaPreviewMessage("Schema layout unavailable.", true);
        return;
    }
    const qualified = documentName ? `${documentName} / @${schema.name}` : `@${schema.name}`;
    if (!schema.fields.length) {
        renderSchemaPreviewMessage(`${qualified} has no fields defined.`);
        return;
    }
    const rows = schema.fields
        .map((field, index) => {
            const meta = formatFieldMeta(field);
            const order = String(index + 1).padStart(2, "0");
            return `<li class="schema-field"><span class="field-order">${order}</span><div><div class="field-name">${field.name}</div><div class="field-meta">${meta}</div></div></li>`;
        })
        .join("\n");
    elements.schemaPreview.innerHTML = `
        <div class="schema-preview-header">
            <span>${qualified}</span>
            <span>${schema.fields.length} field${schema.fields.length === 1 ? "" : "s"}</span>
        </div>
        <ul class="schema-preview-list">
            ${rows}
        </ul>
    `;
}

function renderSchemaPreviewMessage(message: string, isError = false): void {
    const cls = isError ? "schema-preview-message error" : "schema-preview-message";
    elements.schemaPreview.innerHTML = `<div class="${cls}">${message}</div>`;
}

function formatFieldMeta(field: Schema["fields"][number]): string {
    const parts = [field.rawType];
    const extras: string[] = [];
    if (field.autoIncrement && !field.attributes?.includes("serial")) {
        extras.push("auto");
    }
    for (const attr of field.attributes ?? []) {
        if (!attr) {
            continue;
        }
        if (attr === "serial" && field.autoIncrement) {
            continue;
        }
        extras.push(attr);
    }
    if (field.targetSchema) {
        const targetField = field.targetField ? `.${field.targetField}` : "";
        extras.push(`ref→${field.targetSchema}${targetField}`);
    }
    return [...parts, ...extras].filter(Boolean).join(" • ");
}

function setStatus(message: string): void {
    elements.status.textContent = message;
}

function normalizeBaseUrl(value: string): string {
    return value.trim() || window.location.origin;
}

async function parseRowsFromScrt(documentName: string, schemaName: string, body: string): Promise<GenericRecord[]> {
    const doc = await state.client.ensureDocument(false, documentName);
    const schema = doc.schema(schemaName);
    if (!schema) {
        throw new Error(`Schema ${schemaName} not found in document ${documentName}.`);
    }
    const snippet = composeDocumentForRows(schema, body);
    const parsed = parseSCRT(snippet, `${documentName}:${schemaName}`);
    const rows = parsed.records(schema.name);
    if (!rows.length) {
        throw new Error(`No SCRT rows found for ${schemaName}.`);
    }
    return rows as GenericRecord[];
}

function composeDocumentForRows(schema: Schema, body: string): string {
    const sanitized = sanitizeScrtBody(body);
    if (!sanitized) {
        throw new Error("SCRT rows are empty.");
    }
    if (/^@schema\b/im.test(sanitized)) {
        return sanitized;
    }
    const section = ensureSchemaSection(schema.name, sanitized);
    return `${buildSchemaDefinition(schema)}\n\n${section}`;
}

function buildSchemaDefinition(schema: Schema): string {
    const lines = [`@schema:${schema.name}`];
    for (const field of schema.fields) {
        let line = `@field ${field.name} ${field.rawType}`;
        if (field.attributes?.length) {
            line += ` ${field.attributes.join(" ")}`;
        }
        lines.push(line.trim());
    }
    return lines.join("\n");
}

function ensureSchemaSection(schemaName: string, body: string): string {
    const sectionPattern = /^[ \t]*@(?!schema\b)[A-Za-z0-9_:-]+\s*$/im;
    if (sectionPattern.test(body)) {
        return body;
    }
    return `@${schemaName}\n${body}`;
}

function sanitizeScrtBody(input: string): string {
    return input
        .split(/\r?\n/)
        .map((line) => line.trimEnd())
        .join("\n")
        .trim();
}

function detectSchemaHints(text: string): string[] {
    const matches = new Set<string>();
    const pattern = /^[ \t]*@(?!schema\b)([A-Za-z0-9_:-]+)\s*$/gm;
    let match: RegExpExecArray | null;
    while ((match = pattern.exec(text)) !== null) {
        matches.add(match[1]);
    }
    return Array.from(matches);
}

async function maybeApplySchemaHint(input: string): Promise<void> {
    const hint = detectSchemaHints(input)[0];
    if (!hint) {
        return;
    }
    syncSchemaSelection(hint);
    await ensureDocumentSelectionForSchema(hint);
}

async function ensureDocumentSelectionForSchema(schemaName: string): Promise<string | undefined> {
    if (!schemaName) {
        return undefined;
    }
    const currentDoc = elements.documentSelect.value;
    if (currentDoc) {
        try {
            const schemas = await getSchemasForDocument(currentDoc);
            if (schemas.includes(schemaName)) {
                elements.recordDocumentInput.value = currentDoc;
                return currentDoc;
            }
        } catch {
            // ignore and continue
        }
    }
    for (const summary of state.documents) {
        try {
            const schemas = await getSchemasForDocument(summary.name);
            if (schemas.includes(schemaName)) {
                await setActiveDocument(summary.name, schemaName);
                return summary.name;
            }
        } catch {
            continue;
        }
    }
    return undefined;
}

async function setActiveDocument(docName: string, preferredSchema?: string): Promise<void> {
    if (elements.documentSelect.value !== docName) {
        elements.documentSelect.value = docName;
    }
    elements.recordDocumentInput.value = docName;
    state.defaultDocument = docName;
    state.client.useDocument(docName);
    await populateSchemas(docName, preferredSchema);
}

async function getSchemasForDocument(documentName: string): Promise<string[]> {
    const cached = state.schemaIndex.get(documentName);
    if (cached) {
        return cached;
    }
    const doc = await state.client.ensureDocument(false, documentName);
    const names = Array.from(doc.schemas.keys()).sort();
    state.schemaIndex.set(documentName, names);
    return names;
}

function chooseSchema(names: string[], preferred?: string): string {
    const desired = preferred?.trim();
    if (desired && names.includes(desired)) {
        return desired;
    }
    const manual = elements.recordSchemaInput.value.trim();
    if (manual && names.includes(manual)) {
        return manual;
    }
    const current = elements.schemaSelect.value;
    if (current && names.includes(current)) {
        return current;
    }
    return names[0] ?? "";
}

function syncSchemaSelection(schemaName: string): void {
    if (!schemaName) {
        return;
    }
    elements.recordSchemaInput.value = schemaName;
    elements.schemaSelect.value = schemaName;
}

function isTextScrtFile(file?: File): boolean {
    if (!file) {
        return false;
    }
    if (!file.type || file.type === "application/octet-stream") {
        const lower = file.name.toLowerCase();
        return lower.endsWith(".scrt") || lower.endsWith(".txt");
    }
    return file.type.startsWith("text/");
}
