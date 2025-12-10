import { Row } from "./codec";
import { Schema } from "./schema";
type RecordShape = Record<string, unknown> | Map<string, unknown>;
type RowInput = RecordShape | Row;
type BinarySource = ArrayBuffer | ArrayBufferView | Uint8Array;
type NumericMode = "auto" | "number" | "bigint";
type TemporalMode = "date" | "string";
type DurationMode = "bigint" | "number" | "string";
type OutputShape = Record<string, unknown> | Map<string, unknown>;
export interface MarshalOptions {
    rowsPerPage?: number;
}
export interface UnmarshalOptions<T extends OutputShape = Record<string, unknown>> {
    zeroCopyBytes?: boolean;
    numericMode?: NumericMode;
    temporalMode?: TemporalMode;
    durationMode?: DurationMode;
    objectFactory?: () => T;
}
export type MarshalSource = RowInput | Iterable<RowInput>;
export declare function marshalRecords(schema: Schema, source: MarshalSource, options?: MarshalOptions): Uint8Array;
export declare function unmarshalRecords<T extends OutputShape = Record<string, unknown>>(source: BinarySource, schema: Schema, options?: UnmarshalOptions<T>): T[];
export declare function streamDecodedRows<T extends OutputShape = Record<string, unknown>>(source: BinarySource, schema: Schema, options?: UnmarshalOptions<T>): IterableIterator<T>;
export {};
