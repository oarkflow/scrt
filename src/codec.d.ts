import { Schema } from "./schema";
export interface CodecValue {
    uint?: bigint;
    int?: bigint;
    float?: number;
    str?: string;
    bytes?: Uint8Array;
    bool?: boolean;
    set: boolean;
    borrowed?: boolean;
}
export declare class Row {
    readonly schema: Schema;
    private readonly values;
    constructor(schema: Schema);
    reset(): void;
    setByIndex(idx: number, value: CodecValue): void;
    valuesSlice(): CodecValue[];
    fieldIndex(name: string): number;
    setValue(field: string, value: CodecValue): void;
    setUint(field: string, value: bigint | number): void;
    setInt(field: string, value: bigint | number): void;
    setFloat(field: string, value: number): void;
    setBool(field: string, value: boolean): void;
    setString(field: string, value: string): void;
    setBytes(field: string, value: Uint8Array): void;
    private claimSlot;
}
export declare class Writer {
    private readonly schema;
    private readonly builder;
    private readonly output;
    private headerWritten;
    constructor(schema: Schema, rowsPerPage?: number);
    writeRow(row: Row): void;
    finish(): Uint8Array;
    private ensureHeader;
    private flushPage;
}
interface ReaderOptions {
    zeroCopyBytes?: boolean;
}
export declare class Reader {
    private readonly data;
    private readonly schema;
    private readonly options;
    private readonly state;
    private offset;
    private headerRead;
    constructor(data: Uint8Array, schema: Schema, options?: ReaderOptions);
    readRow(row: Row): boolean;
    private consumeHeader;
    private loadPage;
    private decodePage;
    private decodeColumn;
}
export {};
