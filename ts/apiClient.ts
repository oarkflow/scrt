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

const bundleMagic = "SCB1";
const textDecoder = new TextDecoder();

export class SchemaHttpClient {
    private cachedDoc?: Document;

    constructor(private readonly baseUrl = "") { }

    async ensureDocument(force = false): Promise<Document> {
        if (!force && this.cachedDoc) {
            return this.cachedDoc;
        }
        const bundle = await this.fetchBundle();
        return bundle.document;
    }

    async schema(name: string): Promise<Schema> {
        const doc = await this.ensureDocument();
        const schema = doc.schema(name);
        if (!schema) {
            throw new Error(`schema ${name} not found`);
        }
        return schema;
    }

    async marshal<T>(schemaName: string, records: Iterable<T>, opts?: MarshalOptions): Promise<Uint8Array> {
        const schema = await this.schema(schemaName);
        return marshal(schema, records, opts);
    }

    async unmarshal<T>(
        schemaName: string,
        payload: string | Buffer | ArrayBuffer | Uint8Array,
        factory: () => T,
    ): Promise<T[]> {
        const schema = await this.schema(schemaName);
        return unmarshal(payload, schema, factory);
    }

    async fetchRecords<T>(schemaName: string, factory: () => T): Promise<T[]> {
        const bundle = await this.fetchBundle(schemaName);
        const targetSchema = bundle.document.schema(schemaName);
        if (!targetSchema) {
            throw new Error(`schema ${schemaName} not found in bundle`);
        }
        if (!bundle.payload || bundle.payload.schema !== schemaName) {
            throw new Error(`payload for schema ${schemaName} not found`);
        }
        return unmarshal(bundle.payload.data, targetSchema, factory);
    }

    private async fetchBundle(schemaName?: string): Promise<{
        document: Document;
        payload?: SchemaBundlePayload;
        fingerprint: string;
        updatedAt: string;
        schemas: SchemaIndexEntry[];
    }> {
        if (typeof fetch !== "function") {
            throw new Error("fetch is not available in this runtime");
        }
        let endpoint = this.resolve("/schemas/bundle");
        if (schemaName) {
            const separator = endpoint.includes("?") ? "&" : "?";
            endpoint = `${endpoint}${separator}schema=${encodeURIComponent(schemaName)}`;
        }
        const response = await fetch(endpoint, { headers: { Accept: "application/x-scrt-bundle" } });
        if (!response.ok) {
            throw new Error(`schema bundle fetch failed: ${response.status}`);
        }
        const parsed = parseBinaryBundle(await response.arrayBuffer());
        const document = parseSCRT(parsed.document, "bundle");
        this.cachedDoc = document;
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
