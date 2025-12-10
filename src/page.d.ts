import { ByteBuffer } from "./binary";
import { Schema } from "./schema";
export declare class PageBuilder {
    private readonly schema;
    private readonly rowLimit;
    private readonly columns;
    private readonly columnBuf;
    private rows;
    constructor(schema: Schema, rowLimit?: number);
    appendUint(idx: number, value: bigint | number): void;
    appendString(idx: number, value: string): void;
    appendBool(idx: number, value: boolean): void;
    appendInt(idx: number, value: bigint | number): void;
    appendFloat(idx: number, value: number): void;
    appendBytes(idx: number, value: Uint8Array): void;
    recordPresence(idx: number, present: boolean): void;
    sealRow(): void;
    full(): boolean;
    rowCount(): number;
    reset(): void;
    encode(dst: ByteBuffer): void;
}
