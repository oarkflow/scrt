export type ByteBuffer = number[];
export declare function createBuffer(): ByteBuffer;
export declare function bufferLength(buf: ByteBuffer): number;
export declare function pushByte(buf: ByteBuffer, byte: number): void;
export declare function pushBytes(buf: ByteBuffer, bytes: ArrayLike<number>): void;
export declare function bufferToUint8Array(buf: ByteBuffer): Uint8Array;
export declare function concatByteBuffers(buffers: ByteBuffer[]): Uint8Array;
export declare function writeUvarint(buf: ByteBuffer, value: bigint | number): void;
export declare function writeVarint(buf: ByteBuffer, value: bigint | number): void;
export declare function readUvarint(data: Uint8Array, offset: number): {
    value: bigint;
    bytesRead: number;
};
export declare function readVarint(data: Uint8Array, offset: number): {
    value: bigint;
    bytesRead: number;
};
export declare class BinaryWriter {
    private readonly chunks;
    private current;
    writeByte(byte: number): void;
    writeBytes(bytes: ArrayLike<number>): void;
    writeBuffer(buffer: ByteBuffer): void;
    writeUint8Array(arr: Uint8Array): void;
    writeUvarint(value: bigint | number): void;
    toUint8Array(): Uint8Array;
    reset(): void;
    private flushCurrent;
}
export declare class BinaryReader {
    private readonly data;
    offset: number;
    constructor(data: Uint8Array, offset?: number);
    ensure(size: number): void;
    readByte(): number;
    readBytes(length: number): Uint8Array;
    readUvarint(): bigint;
    readVarint(): bigint;
    remaining(): number;
}
type NumberLike = number | bigint;
export declare function toSafeNumber(value: NumberLike, label: string): number;
export declare function encodeUint64LE(value: bigint | number): Uint8Array;
export {};
