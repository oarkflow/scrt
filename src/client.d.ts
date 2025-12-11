import { ScrtBundleEnvelope } from "./protocol";
type RecordWriteMode = "append" | "replace";
interface UploadRecordsOptions {
    mode?: RecordWriteMode;
}
export interface RecordEnvelope {
    schema: string;
    field: string;
    key: string;
    row: Record<string, unknown>;
}
export declare class ScrtHttpClient {
    private baseUrl;
    constructor(baseUrl?: string);
    setBaseUrl(url: string): void;
    private url;
    listSchemas(): Promise<string[]>;
    downloadSchema(name: string): Promise<string>;
    saveSchema(name: string, body: string): Promise<void>;
    deleteSchema(name: string): Promise<void>;
    uploadRecords(schema: string, payload: Uint8Array, options?: UploadRecordsOptions): Promise<void>;
    deleteRecords(schema: string): Promise<void>;
    fetchRecords(schema: string): Promise<Uint8Array>;
    fetchRecordRow(schema: string, field: string, key: string): Promise<RecordEnvelope>;
    updateRecordRow(schema: string, field: string, key: string, payload: Uint8Array): Promise<RecordEnvelope>;
    deleteRecordRow(schema: string, field: string, key: string): Promise<void>;
    fetchBundle(schema: string): Promise<ScrtBundleEnvelope>;
    listDocuments(): Promise<string[]>;
    downloadDocument(name: string): Promise<string>;
    saveDocument(name: string, body: string): Promise<void>;
    deleteDocument(name: string): Promise<void>;
    uploadDocumentRecords(doc: string, schema: string, payload: Uint8Array, options?: UploadRecordsOptions): Promise<void>;
    fetchDocumentRecords(doc: string, schema: string): Promise<Uint8Array>;
    fetchDocumentBundle(doc: string, schema: string): Promise<ScrtBundleEnvelope>;
}
export {};
