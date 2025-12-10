const textDecoder = new TextDecoder();

const MAGIC = "SCB1";
const VERSION = 1;

export interface ScrtBundleEnvelope {
    documentName: string;
    schemaName: string;
    documentFingerprint: bigint;
    schemaFingerprint: bigint;
    updatedAt: Date;
    schemaText: string;
    payload: Uint8Array;
}

export function decodeBundle(buffer: ArrayBuffer): ScrtBundleEnvelope {
    const view = new DataView(buffer);
    let offset = 0;
    for (let i = 0; i < MAGIC.length; i += 1) {
        if (view.getUint8(offset) !== MAGIC.charCodeAt(i)) {
            throw new Error("scrt: invalid bundle magic");
        }
        offset += 1;
    }
    const version = view.getUint8(offset);
    offset += 1;
    if (version !== VERSION) {
        throw new Error(`scrt: unsupported bundle version ${version}`);
    }
    const docFingerprint = view.getBigUint64(offset, true);
    offset += 8;
    const schemaFingerprint = view.getBigUint64(offset, true);
    offset += 8;
    const updatedAt = new Date(Number(view.getBigInt64(offset, true) / 1_000_000n));
    offset += 8;
    const docString = readShortString(view, buffer, offset);
    offset += docString.bytes;
    const schemaString = readShortString(view, buffer, offset);
    offset += schemaString.bytes;
    const schemaBlob = readBlob(view, buffer, offset);
    offset += schemaBlob.bytes;
    const payload = readBlob(view, buffer, offset);
    return {
        documentName: docString.value,
        schemaName: schemaString.value,
        documentFingerprint: docFingerprint,
        schemaFingerprint,
        updatedAt,
        schemaText: textDecoder.decode(schemaBlob.data),
        payload: payload.data,
    };
}

function readShortString(view: DataView, buffer: ArrayBuffer, offset: number): { value: string; bytes: number } {
    const length = view.getUint16(offset, true);
    const start = offset + 2;
    const end = start + length;
    if (end > buffer.byteLength) {
        throw new Error("scrt: bundle string exceeds buffer");
    }
    return { value: textDecoder.decode(buffer.slice(start, end)), bytes: 2 + length };
}

function readBlob(view: DataView, buffer: ArrayBuffer, offset: number): { data: Uint8Array; bytes: number } {
    const length = view.getUint32(offset, true);
    const start = offset + 4;
    const end = start + length;
    if (end > buffer.byteLength) {
        throw new Error("scrt: bundle blob exceeds buffer");
    }
    return { data: new Uint8Array(buffer.slice(start, end)), bytes: 4 + length };
}
