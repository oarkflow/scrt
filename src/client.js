import { decodeBundle } from "./protocol";
function normalizeBase(url) {
    const trimmed = url.trim();
    if (!trimmed) {
        return "http://localhost:8080";
    }
    return trimmed.endsWith("/") ? trimmed.slice(0, -1) : trimmed;
}
async function ensureOk(resp) {
    if (resp.ok) {
        return resp;
    }
    const text = await resp.text();
    throw new Error(text || resp.statusText);
}
export class ScrtHttpClient {
    baseUrl;
    constructor(baseUrl = "http://localhost:8080") {
        this.baseUrl = normalizeBase(baseUrl);
    }
    setBaseUrl(url) {
        this.baseUrl = normalizeBase(url);
    }
    url(path) {
        return `${this.baseUrl}${path}`;
    }
    async listSchemas() {
        const resp = await ensureOk(await fetch(this.url("/schemas"), {
            headers: { Accept: "text/plain" },
        }));
        const body = await resp.text();
        return body
            .split(/\r?\n/)
            .map((line) => line.trim())
            .filter(Boolean);
    }
    async downloadSchema(name) {
        const resp = await ensureOk(await fetch(this.url(`/schemas/${encodeURIComponent(name)}`), {
            headers: { Accept: "text/plain" },
        }));
        return resp.text();
    }
    async saveSchema(name, body) {
        await ensureOk(await fetch(this.url(`/schemas/${encodeURIComponent(name)}`), {
            method: "POST",
            headers: { "Content-Type": "text/plain; charset=utf-8" },
            body,
        }));
    }
    async deleteSchema(name) {
        await ensureOk(await fetch(this.url(`/schemas/${encodeURIComponent(name)}`), {
            method: "DELETE",
        }));
    }
    async uploadRecords(schema, payload) {
        await ensureOk(await fetch(this.url(`/records/${encodeURIComponent(schema)}`), {
            method: "POST",
            headers: { "Content-Type": "application/x-scrt" },
            body: payload,
        }));
    }
    async fetchRecords(schema) {
        const resp = await ensureOk(await fetch(this.url(`/records/${encodeURIComponent(schema)}`)));
        const buffer = await resp.arrayBuffer();
        return new Uint8Array(buffer);
    }
    async fetchBundle(schema) {
        const resp = await ensureOk(await fetch(this.url(`/bundle?schema=${encodeURIComponent(schema)}`)));
        const buffer = await resp.arrayBuffer();
        return decodeBundle(buffer);
    }
    // Deprecated aliases for backwards compatibility
    async listDocuments() {
        return this.listSchemas();
    }
    async downloadDocument(name) {
        return this.downloadSchema(name);
    }
    async saveDocument(name, body) {
        return this.saveSchema(name, body);
    }
    async deleteDocument(name) {
        return this.deleteSchema(name);
    }
    async uploadDocumentRecords(doc, schema, payload) {
        // kept for compatibility with older callers
        const target = schema || doc;
        return this.uploadRecords(target, payload);
    }
    async fetchDocumentRecords(doc, schema) {
        const target = schema || doc;
        return this.fetchRecords(target);
    }
    async fetchDocumentBundle(doc, schema) {
        const target = schema || doc;
        return this.fetchBundle(target);
    }
}
