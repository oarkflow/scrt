import { decodeBundle, ScrtBundleEnvelope } from "./protocol";

type RecordWriteMode = "append" | "replace";

interface UploadRecordsOptions {
    mode?: RecordWriteMode;
}

function normalizeBase(url: string): string {
    const trimmed = url.trim();
    if (!trimmed) {
        return "http://localhost:8080";
    }
    return trimmed.endsWith("/") ? trimmed.slice(0, -1) : trimmed;
}

async function ensureOk(resp: Response): Promise<Response> {
    if (resp.ok) {
        return resp;
    }
    const text = await resp.text();
    throw new Error(text || resp.statusText);
}

export class ScrtHttpClient {
    private baseUrl: string;

    constructor(baseUrl = "http://localhost:8080") {
        this.baseUrl = normalizeBase(baseUrl);
    }

    setBaseUrl(url: string): void {
        this.baseUrl = normalizeBase(url);
    }

    private url(path: string): string {
        return `${this.baseUrl}${path}`;
    }

    async listSchemas(): Promise<string[]> {
        const resp = await ensureOk(
            await fetch(this.url("/schemas"), {
                headers: { Accept: "text/plain" },
            }),
        );
        const body = await resp.text();
        return body
            .split(/\r?\n/)
            .map((line) => line.trim())
            .filter(Boolean);
    }

    async downloadSchema(name: string): Promise<string> {
        const resp = await ensureOk(
            await fetch(this.url(`/schemas/${encodeURIComponent(name)}`), {
                headers: { Accept: "text/plain" },
            }),
        );
        return resp.text();
    }

    async saveSchema(name: string, body: string): Promise<void> {
        await ensureOk(
            await fetch(this.url(`/schemas/${encodeURIComponent(name)}`), {
                method: "POST",
                headers: { "Content-Type": "text/plain; charset=utf-8" },
                body,
            }),
        );
    }

    async deleteSchema(name: string): Promise<void> {
        await ensureOk(
            await fetch(this.url(`/schemas/${encodeURIComponent(name)}`), {
                method: "DELETE",
            }),
        );
    }

    async uploadRecords(schema: string, payload: Uint8Array, options: UploadRecordsOptions = {}): Promise<void> {
        const mode = options.mode ?? "append";
        const query = mode === "replace" ? "?mode=replace" : "";
        await ensureOk(
            await fetch(this.url(`/records/${encodeURIComponent(schema)}${query}`), {
                method: "POST",
                headers: { "Content-Type": "application/x-scrt" },
                body: payload as BodyInit,
            }),
        );
    }

    async deleteRecords(schema: string): Promise<void> {
        await ensureOk(
            await fetch(this.url(`/records/${encodeURIComponent(schema)}`), {
                method: "DELETE",
            }),
        );
    }

    async fetchRecords(schema: string): Promise<Uint8Array> {
        const resp = await ensureOk(
            await fetch(this.url(`/records/${encodeURIComponent(schema)}`)),
        );
        const buffer = await resp.arrayBuffer();
        return new Uint8Array(buffer);
    }

    async fetchBundle(schema: string): Promise<ScrtBundleEnvelope> {
        const resp = await ensureOk(
            await fetch(this.url(`/bundle?schema=${encodeURIComponent(schema)}`)),
        );
        const buffer = await resp.arrayBuffer();
        return decodeBundle(buffer);
    }

    // Deprecated aliases for backwards compatibility
    async listDocuments(): Promise<string[]> {
        return this.listSchemas();
    }

    async downloadDocument(name: string): Promise<string> {
        return this.downloadSchema(name);
    }

    async saveDocument(name: string, body: string): Promise<void> {
        return this.saveSchema(name, body);
    }

    async deleteDocument(name: string): Promise<void> {
        return this.deleteSchema(name);
    }

    async uploadDocumentRecords(doc: string, schema: string, payload: Uint8Array, options: UploadRecordsOptions = {}): Promise<void> {
        // kept for compatibility with older callers
        const target = schema || doc;
        return this.uploadRecords(target, payload, options);
    }

    async fetchDocumentRecords(doc: string, schema: string): Promise<Uint8Array> {
        const target = schema || doc;
        return this.fetchRecords(target);
    }

    async fetchDocumentBundle(doc: string, schema: string): Promise<ScrtBundleEnvelope> {
        const target = schema || doc;
        return this.fetchBundle(target);
    }
}
