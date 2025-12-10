export interface ScrtBundleEnvelope {
    documentName: string;
    schemaName: string;
    documentFingerprint: bigint;
    schemaFingerprint: bigint;
    updatedAt: Date;
    schemaText: string;
    payload: Uint8Array;
}
export declare function decodeBundle(buffer: ArrayBuffer): ScrtBundleEnvelope;
