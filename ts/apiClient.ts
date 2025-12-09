import {
    Document,
    MarshalOptions,
    Schema,
    fetchSCRT,
    marshal,
    unmarshal,
} from "./scrt";

export interface SchemaIndexEntry {
    name: string;
    fingerprint: string;
    fields: number;
}

export interface SchemaIndexResponse {
    source: string;
    fingerprint: string;
    updatedAt: string;
    schemas: SchemaIndexEntry[];
}

export class SchemaHttpClient {
    private cachedDoc?: Document;
    private cachedFingerprint?: string;

    constructor(private readonly baseUrl = "") { }

    async ensureDocument(force = false): Promise<Document> {
        const index = await this.fetchIndex();
        if (!force && this.cachedDoc && this.cachedFingerprint === index.fingerprint) {
            return this.cachedDoc;
        }
        const doc = await fetchSCRT(this.resolve("/schemas/doc"));
        this.cachedDoc = doc;
        this.cachedFingerprint = index.fingerprint;
        return doc;
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

    private async fetchIndex(): Promise<SchemaIndexResponse> {
        if (typeof fetch !== "function") {
            throw new Error("fetch is not available in this runtime");
        }
        const response = await fetch(this.resolve("/schemas/index"), {
            headers: { Accept: "application/json" },
        });
        if (!response.ok) {
            throw new Error(`schema index fetch failed: ${response.status}`);
        }
        return response.json();
    }

    private resolve(pathname: string): string {
        if (!this.baseUrl) {
            return pathname;
        }
        return new URL(pathname, this.baseUrl).toString();
    }
}
