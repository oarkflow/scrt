import { ScrtBundleEnvelope } from "./protocol";
export declare class ScrtHttpClient {
    private baseUrl;
    constructor(baseUrl?: string);
    setBaseUrl(url: string): void;
    private url;
    listSchemas(): Promise<string[]>;
    downloadSchema(name: string): Promise<string>;
    saveSchema(name: string, body: string): Promise<void>;
    deleteSchema(name: string): Promise<void>;
    uploadRecords(schema: string, payload: Uint8Array): Promise<void>;
    fetchRecords(schema: string): Promise<Uint8Array>;
    fetchBundle(schema: string): Promise<ScrtBundleEnvelope>;
    listDocuments(): Promise<string[]>;
    downloadDocument(name: string): Promise<string>;
    saveDocument(name: string, body: string): Promise<void>;
    deleteDocument(name: string): Promise<void>;
    uploadDocumentRecords(doc: string, schema: string, payload: Uint8Array): Promise<void>;
    fetchDocumentRecords(doc: string, schema: string): Promise<Uint8Array>;
    fetchDocumentBundle(doc: string, schema: string): Promise<ScrtBundleEnvelope>;
}
