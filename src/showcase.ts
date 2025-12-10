import { ScrtHttpClient } from "./client";
import { marshalRecords, unmarshalRecords } from "./marshal";
import { parseSchema } from "./schema";

const form = document.getElementById("demo-form") as HTMLFormElement;
const serverInput = document.getElementById("demo-server") as HTMLInputElement;
const schemaInput = document.getElementById("demo-schema") as HTMLInputElement;
const dslTextarea = document.getElementById("demo-dsl") as HTMLTextAreaElement;
const logViewport = document.getElementById("demo-log") as HTMLDivElement;
const resultsList = document.getElementById("demo-results") as HTMLUListElement;
const resetButton = document.getElementById("demo-reset") as HTMLButtonElement;

const sampleRows = [
    {
        MsgID: 1n,
        User: 42n,
        Text: "hello",
        Lang: "en",
        CreatedAt: new Date("2025-01-01T12:00:00Z"),
    },
    {
        MsgID: 2n,
        User: 7n,
        Text: "hola",
        Lang: "es",
        CreatedAt: new Date("2025-01-02T08:45:00Z"),
    },
];

const logLines: string[] = [];

form.addEventListener("submit", (event) => {
    event.preventDefault();
    void runDemo();
});

resetButton.addEventListener("click", () => {
    void deleteSchema();
});

function getClient(): ScrtHttpClient {
    return new ScrtHttpClient(serverInput.value || undefined);
}


async function runDemo(): Promise<void> {
    const schemaName = schemaInput.value.trim();
    const dsl = dslTextarea.value.trim();
    if (!schemaName || !dsl) {
        pushLog("Schema name and DSL are required.");
        return;
    }
    resetLog();
    pushLog("Starting showcase run...");
    const client = getClient();
    setFormDisabled(true);
    clearResults();
    try {
        pushLog("Cleaning up any prior run...");
        await tryDelete(client, schemaName);
        pushLog(`Uploading schema ${schemaName}...`);
        await client.saveSchema(schemaName, dsl);
        const doc = parseSchema(dsl);
        const schema = doc.schema(schemaName);
        if (!schema) {
            throw new Error(`Schema ${schemaName} missing in DSL`);
        }
        const payload = marshalRecords(schema, sampleRows);
        pushLog(`Marshalled ${sampleRows.length} rows (${payload.byteLength} bytes).`);
        await client.uploadRecords(schemaName, payload, { mode: "replace" });
        pushLog("Records uploaded. Fetching bundle...");
        const bundle = await client.fetchBundle(schemaName);
        pushLog(`Bundle fetched (updated ${bundle.updatedAt.toISOString()}). Decoding payload...`);
        const bundleDoc = parseSchema(bundle.schemaText);
        const bundleSchema = bundleDoc.schema(bundle.schemaName);
        if (!bundleSchema) {
            throw new Error(`Bundle missing schema ${bundle.schemaName}`);
        }
        const decoded = unmarshalRecords(bundle.payload, bundleSchema, {
            numericMode: "auto",
            temporalMode: "string",
            durationMode: "string",
        });
        renderResults(decoded);
        pushLog("Decoded payload rendered below. Demo complete.");
    } catch (err) {
        renderResults([]);
        pushError(err);
    } finally {
        setFormDisabled(false);
    }
}


async function deleteSchema(): Promise<void> {
    const schemaName = schemaInput.value.trim();
    if (!schemaName) {
        pushLog("Schema name required to delete.");
        return;
    }
    resetLog();
    try {
        pushLog(`Deleting ${schemaName}...`);
        await tryDelete(getClient(), schemaName, true);
        pushLog(`Schema ${schemaName} deleted.`);
        clearResults();
    } catch (err) {
        pushError(err);
    }
}

async function tryDelete(client: ScrtHttpClient, schemaName: string, throwOnMissing = false): Promise<void> {
    try {
        await client.deleteSchema(schemaName);
    } catch (err) {
        if (throwOnMissing) {
            throw err;
        }
    }
}

function setFormDisabled(disabled: boolean): void {
    const controls = Array.from(form.elements);
    controls.forEach((control) => {
        if ("disabled" in control) {
            (control as HTMLInputElement).disabled = disabled;
        }
    });
    resetButton.disabled = disabled;
}

function pushLog(message: string): void {
    const stamp = new Date().toLocaleTimeString();
    logLines.push(`[${stamp}] ${message}`);
    updateLog();
}

function pushError(err: unknown): void {
    const message = err instanceof Error ? err.message : String(err);
    pushLog(`Error: ${message}`);
    console.error(err);
}

function resetLog(): void {
    logLines.length = 0;
    updateLog();
}

function updateLog(): void {
    const recent = logLines.slice(-8).join("\n");
    logViewport.textContent = recent || "Idle";
}

function clearResults(): void {
    resultsList.innerHTML = "";
}

function renderResults(rows: Record<string, unknown>[]): void {
    clearResults();
    if (!rows.length) {
        return;
    }
    rows.forEach((row, idx) => {
        const card = document.createElement("li");
        card.className = "result-card";
        const header = document.createElement("header");
        const title = document.createElement("strong");
        title.textContent = `Row ${idx + 1}`;
        const tag = document.createElement("span");
        tag.textContent = "from bundle";
        header.append(title, tag);
        const dl = document.createElement("dl");
        Object.entries(row).forEach(([key, value]) => {
            const dt = document.createElement("dt");
            dt.textContent = key;
            const dd = document.createElement("dd");
            dd.textContent = formatValue(value);
            dl.append(dt, dd);
        });
        card.append(header, dl);
        resultsList.append(card);
    });
}

function formatValue(value: unknown): string {
    if (value instanceof Date) {
        return value.toISOString();
    }
    if (value === null || value === undefined) {
        return "";
    }
    return String(value);
}
