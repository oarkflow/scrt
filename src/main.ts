import { SchemaHttpClient, DocumentSummary } from "../ts/apiClient";

type GenericRecord = Record<string, unknown>;

interface UIElements {
    serverInput: HTMLInputElement;
    documentSelect: HTMLSelectElement;
    schemaSelect: HTMLSelectElement;
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
    recordBodyInput: HTMLTextAreaElement;
}

interface UIState {
    client: SchemaHttpClient;
    baseUrl: string;
    defaultDocument?: string;
    documents: DocumentSummary[];
}

const elements = queryElements();
const initialBase = import.meta.env.VITE_SCRT_SERVER ?? elements.serverInput.value;
elements.serverInput.value = initialBase;
const state: UIState = {
    client: new SchemaHttpClient({ baseUrl: initialBase }),
    baseUrl: initialBase,
    defaultDocument: undefined,
    documents: [],
};

attachEventListeners();
void refreshDocuments();

function queryElements(): UIElements {
    const serverInput = mustQuery<HTMLInputElement>("#server");
    const documentSelect = mustQuery<HTMLSelectElement>("#document-select");
    const schemaSelect = mustQuery<HTMLSelectElement>("#schema-select");
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
    const recordBodyInput = mustQuery<HTMLTextAreaElement>("#record-body");
    return {
        serverInput,
        documentSelect,
        schemaSelect,
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
        recordBodyInput,
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
    });
    elements.documentForm.addEventListener("submit", (event) => {
        event.preventDefault();
        void handleDocumentSubmit();
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

async function populateSchemas(documentName: string): Promise<void> {
    if (!documentName) {
        clearSchemaOptions("Select a schema");
        return;
    }
    try {
        const doc = await state.client.ensureDocument(false, documentName);
        const names = Array.from(doc.schemas.keys()).sort();
        setSchemaOptions(names);
        const next = names[0] ?? "";
        elements.schemaSelect.value = next;
        elements.recordSchemaInput.value = next;
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
        const records = await state.client.fetchRecords<GenericRecord>(
            schemaName,
            () => ({} as GenericRecord),
            { document: documentName },
        );
        renderRecords(records, schemaName);
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
    const documentName = elements.recordDocumentInput.value.trim() || elements.documentSelect.value;
    const schemaName = elements.recordSchemaInput.value.trim() || elements.schemaSelect.value;
    const body = elements.recordBodyInput.value.trim();
    if (!documentName || !schemaName || !body) {
        setStatus("Document, schema, and JSON rows are required.");
        return;
    }
    let rows: GenericRecord[];
    try {
        const parsed = JSON.parse(body);
        rows = Array.isArray(parsed) ? parsed : [parsed];
    } catch (err) {
        setStatus(`Invalid JSON: ${(err as Error).message}`);
        return;
    }
    setStatus(`Uploading ${rows.length} record(s) to ${documentName}/${schemaName}…`);
    try {
        const payload = await state.client.marshal(schemaName, rows, { document: documentName });
        await state.client.pushRecords(documentName, schemaName, payload);
        setStatus(`Stored ${rows.length} record(s) for ${documentName}/${schemaName}.`);
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
}

function renderRecords(records: GenericRecord[], schemaName: string): void {
    if (!records.length) {
        elements.recordList.replaceChildren();
        return;
    }
    const nodes = records.map((record, index) => {
        const li = document.createElement("li");
        const header = document.createElement("header");
        header.innerHTML = `<strong>${schemaName}</strong> · row ${index + 1}`;
        const pre = document.createElement("pre");
        pre.textContent = JSON.stringify(record, null, 2);
        li.append(header, pre);
        return li;
    });
    elements.recordList.replaceChildren(...nodes);
}

function setStatus(message: string): void {
    elements.status.textContent = message;
}

function normalizeBaseUrl(value: string): string {
    return value.trim() || window.location.origin;
}
