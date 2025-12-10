import { ByteBuffer } from "./binary";
export declare class Uint64Column {
    private readonly values;
    append(value: bigint | number): void;
    encode(dst: ByteBuffer): void;
    reset(): void;
}
export declare class Int64Column {
    private readonly values;
    append(value: bigint | number): void;
    encode(dst: ByteBuffer): void;
    reset(): void;
}
export declare class Float64Column {
    private readonly values;
    append(value: number): void;
    encode(dst: ByteBuffer): void;
    reset(): void;
}
export declare class BoolColumn {
    private readonly values;
    append(value: boolean): void;
    encode(dst: ByteBuffer): void;
    reset(): void;
}
export declare class StringColumn {
    private readonly dict;
    private readonly entries;
    private readonly indexes;
    append(value: string): void;
    encode(dst: ByteBuffer): void;
    reset(): void;
}
export declare class BytesColumn {
    private readonly values;
    append(value: Uint8Array): void;
    encode(dst: ByteBuffer): void;
    reset(): void;
}
