import {
    Document,
    MarshalOptions,
    Schema,
    marshal,
    unmarshal,
    parseSCRT,
} from "./scrt";

export interface SchemaIndexEntry {
    name: string;
    fingerprint: string;
    fields: number;
}

export interface SchemaBundlePayload {
    schema: string;
    fingerprint: string;
    data: Uint8Array;
}

export interface DocumentSummary {
    name: string;
    fingerprint: string;
    schemaCount: number;
    updatedAt: string;
    source: string;
}

export interface MarshalRequestOptions extends MarshalOptions {
    document?: string;
}

export interface UnmarshalRequestOptions {
    document?: string;
}

interface SchemaHttpClientPaths {
    bundle: string;
    documents: string;
    documentRecords(document: string, schema: string): string;
}

const defaultPaths: SchemaHttpClientPaths = {
    bundle: "/schemas/bundle",
    documents: "/schemas/documents",
    documentRecords: (document, schema) =>
        `/schemas/documents/${encodeURIComponent(document)}/records/${encodeURIComponent(schema)}`,
};

export interface SchemaHttpClientOptions {
    baseUrl?: string;
    defaultDocument?: string;
    paths?: Partial<SchemaHttpClientPaths>;
}

const bundleMagic = "SCB1";
const textDecoder = new TextDecoder();

export class SchemaHttpClient {
    private readonly baseUrl: string;
    private readonly paths: SchemaHttpClientPaths;
    private cachedDocs = new Map<string, Document>();
    private defaultDocument?: string;

    constructor(options: SchemaHttpClientOptions = {}) {
        this.baseUrl = options.baseUrl ?? "";
        this.defaultDocument = options.defaultDocument;
        this.paths = {
            bundle: options.paths?.bundle ?? defaultPaths.bundle,
            documents: options.paths?.documents ?? defaultPaths.documents,
            documentRecords: options.paths?.documentRecords ?? defaultPaths.documentRecords,
        };
    }

    async ensureDocument(force = false, documentName?: string): Promise<Document> {
        const target = documentName ?? this.defaultDocument;
        const key = this.documentCacheKey(target);
        if (!force) {
            const cached = this.cachedDocs.get(key);
            if (cached) {
                return cached;
            }
        }
        const bundle = await this.fetchBundle(undefined, target);
        this.cachedDocs.set(key, bundle.document);
        return bundle.document;
    }

    useDocument(name: string | undefined): void {
        this.defaultDocument = name;
    }

    clearCache(documentName?: string): void {
        if (documentName) {
            this.cachedDocs.delete(this.documentCacheKey(documentName));
            return;
        }
        this.cachedDocs.clear();
    }

    async schema(name: string, documentName?: string): Promise<Schema> {
        const doc = await this.ensureDocument(false, documentName);
        const schema = doc.schema(name);
        if (!schema) {
            throw new Error(`schema ${name} not found`);
        }
        return schema;
    }

    async marshal<T>(schemaName: string, records: Iterable<T>, opts?: MarshalRequestOptions): Promise<Uint8Array> {
        const documentOverride = opts?.document;
        const { document: _doc, ...marshalOpts } = opts ?? {};
        const schema = await this.schema(schemaName, documentOverride);
        return marshal(schema, records, marshalOpts as MarshalOptions);
    }

    async unmarshal<T>(
        schemaName: string,
        payload: string | Buffer | ArrayBuffer | Uint8Array,
        factory: () => T,
        opts: UnmarshalRequestOptions = {},
    ): Promise<T[]> {
        const schema = await this.schema(schemaName, opts.document);
        return unmarshal(payload, schema, factory);
    }

    async fetchRecords<T>(schemaName: string, factory: () => T, opts: { document?: string } = {}): Promise<T[]> {
        const bundle = await this.fetchBundle(schemaName, opts.document ?? this.defaultDocument);
        const targetSchema = bundle.document.schema(schemaName);
        if (!targetSchema) {
            throw new Error(`schema ${schemaName} not found in bundle`);
        }
        if (!bundle.payload || bundle.payload.schema !== schemaName) {
            throw new Error(`payload for schema ${schemaName} not found`);
        }
        return unmarshal(bundle.payload.data, targetSchema, factory);
    }

    async listDocuments(): Promise<DocumentSummary[]> {
        const doFetch = this.requireFetch();
        const response = await doFetch(this.resolve(this.paths.documents), {
            headers: { Accept: "application/json" },
        });
        if (!response.ok) {
            throw new Error(`list documents failed: ${response.status}`);
        }
        const payload = await response.json();
        return (payload?.documents as DocumentSummary[]) ?? [];
    }

    async upsertDocument(name: string, source: string | Uint8Array | ArrayBuffer): Promise<void> {
        const doFetch = this.requireFetch();
        const params = new URLSearchParams({ name });
        const endpoint = appendQuery(this.resolve(this.paths.documents), params);
        const response = await doFetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "text/plain; charset=utf-8" },
            body: toRequestBody(source),
        });
        if (!response.ok) {
            throw new Error(`upsert document failed: ${response.status}`);
        }
        this.clearCache(name);
    }

    async pushRecords(documentName: string, schemaName: string, payload: Uint8Array): Promise<void> {
        const doFetch = this.requireFetch();
        const endpoint = this.resolve(this.paths.documentRecords(documentName, schemaName));
        const response = await doFetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/x-scrt" },
            body: payload,
        });
        if (!response.ok) {
            throw new Error(`push records failed: ${response.status}`);
        }
    }

    async fetchRawRecords(documentName: string, schemaName: string): Promise<Uint8Array> {
        const doFetch = this.requireFetch();
        const endpoint = this.resolve(this.paths.documentRecords(documentName, schemaName));
        const response = await doFetch(endpoint, { headers: { Accept: "application/x-scrt" } });
        if (!response.ok) {
            throw new Error(`fetch records failed: ${response.status}`);
        }
        return new Uint8Array(await response.arrayBuffer());
    }

    private async fetchBundle(schemaName?: string, documentName?: string): Promise<{
        document: Document;
        payload?: SchemaBundlePayload;
        fingerprint: string;
        updatedAt: string;
        schemas: SchemaIndexEntry[];
    }> {
        const doFetch = this.requireFetch();
        const params = new URLSearchParams();
        if (documentName) {
            params.set("document", documentName);
        }
        if (schemaName) {
            params.set("schema", schemaName);
        }
        const endpoint = appendQuery(this.resolve(this.paths.bundle), params);
        const response = await doFetch(endpoint, { headers: { Accept: "application/x-scrt-bundle" } });
        if (!response.ok) {
            throw new Error(`schema bundle fetch failed: ${response.status}`);
        }
        const parsed = parseBinaryBundle(await response.arrayBuffer());
        const document = parseSCRT(parsed.document, documentName ?? "bundle");
        this.cachedDocs.set(this.documentCacheKey(documentName), document);
        const payload = parsed.payload;
        return {
            document,
            payload,
            fingerprint: parsed.fingerprint,
            updatedAt: parsed.updatedAt,
            schemas: parsed.entries,
        };
    }

    private resolve(pathname: string): string {
        if (!this.baseUrl) {
            return pathname;
        }
        return new URL(pathname, this.baseUrl).toString();
    }

    private requireFetch(): typeof fetch {
        if (typeof fetch !== "function") {
            throw new Error("fetch is not available in this runtime");
        }
        return fetch;
    }

    private documentCacheKey(name?: string): string {
        return name ?? "__default__";
    }
}

interface ParsedBinaryBundle {
    fingerprint: string;
    updatedAt: string;
    document: string;
    entries: SchemaIndexEntry[];
    payload?: SchemaBundlePayload;
}

class BundleReader {
    private offset = 0;

    constructor(private readonly view: DataView) { }

    readUint8(): number {
        const value = this.view.getUint8(this.offset);
        this.offset += 1;
        return value;
    }

    readUint16(): number {
        const value = this.view.getUint16(this.offset, false);
        this.offset += 2;
        return value;
    }

    readUint32(): number {
        const value = this.view.getUint32(this.offset, false);
        this.offset += 4;
        return value;
    }

    readBigUint64(): bigint {
        const value = this.view.getBigUint64(this.offset, false);
        this.offset += 8;
        return value;
    }

    readBigInt64(): bigint {
        const value = this.view.getBigInt64(this.offset, false);
        this.offset += 8;
        return value;
    }

    readBytes(length: number): Uint8Array {
        const start = this.offset;
        const end = start + length;
        if (end > this.view.byteLength) {
            throw new Error("SCRT bundle truncated");
        }
        this.offset = end;
        return new Uint8Array(this.view.buffer, this.view.byteOffset + start, length);
    }

    readFixedASCII(length: number): string {
        const bytes = this.readBytes(length);
        let out = "";
        for (let i = 0; i < bytes.length; i++) {
            out += String.fromCharCode(bytes[i]);
        }
        return out;
    }

    readString16(): string {
        const length = this.readUint16();
        return textDecoder.decode(this.readBytes(length));
    }

    readString32(): string {
        const length = this.readUint32();
        return textDecoder.decode(this.readBytes(length));
    }
}

function parseBinaryBundle(buffer: ArrayBuffer): ParsedBinaryBundle {
    const reader = new BundleReader(new DataView(buffer));
    const magic = reader.readFixedASCII(4);
    if (magic !== bundleMagic) {
        throw new Error(`invalid SCRT bundle magic: ${magic}`);
    }
    const version = reader.readUint8();
    if (version !== 1) {
        throw new Error(`unsupported SCRT bundle version: ${version}`);
    }
    const fingerprint = toHex(reader.readBigUint64());
    const updatedAt = nanosToISO(reader.readBigInt64());
    const document = reader.readString32();
    const schemaCount = reader.readUint16();
    const entries: SchemaIndexEntry[] = [];
    for (let i = 0; i < schemaCount; i++) {
        const name = reader.readString16();
        const entryFingerprint = toHex(reader.readBigUint64());
        const fields = reader.readUint16();
        entries.push({ name, fingerprint: entryFingerprint, fields });
    }
    const hasPayload = reader.readUint8();
    let payload: SchemaBundlePayload | undefined;
    if (hasPayload === 1) {
        const schema = reader.readString16();
        const payloadFingerprint = toHex(reader.readBigUint64());
        const length = reader.readUint32();
        const data = new Uint8Array(reader.readBytes(length));
        payload = { schema, fingerprint: payloadFingerprint, data };
    }
    return { fingerprint, updatedAt, document, entries, payload };
}

function toHex(value: bigint): string {
    return value.toString(16).padStart(16, "0");
}

function nanosToISO(value: bigint): string {
    const millis = Number(value / 1_000_000n);
    return new Date(millis).toISOString();
}

function appendQuery(url: string, params: URLSearchParams): string {
    const query = params.toString();
    if (!query) {
        return url;
    }
    const separator = url.includes("?") ? "&" : "?";
    return `${url}${separator}${query}`;
}

function toRequestBody(source: string | Uint8Array | ArrayBuffer): BodyInit {
    if (typeof source === "string") {
        return source;
    }
    if (source instanceof Uint8Array) {
        return source;
    }
    return new Uint8Array(source);
}
