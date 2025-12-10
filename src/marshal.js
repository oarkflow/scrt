import { Reader, Row, Writer } from "./codec";
import { FieldKind } from "./schema";
import { canonicalTimestampTZ, decodeDate, decodeInstant, encodeDate, encodeInstant, formatDate, formatDuration, formatInstant, formatTimestampTZ, inferEpochNanoseconds, parseDate, parseDateTime, parseDuration, parseTimestamp, parseTimestampTZ, } from "./temporal";
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
export function marshalRecords(schema, source, options = {}) {
    if (!schema) {
        throw new Error("scrt: schema is required for marshal");
    }
    const writer = new Writer(schema, options.rowsPerPage ?? 1024);
    const scratch = new Row(schema);
    for (const record of toIterable(source)) {
        if (record instanceof Row) {
            if (record.schema !== schema) {
                throw new Error("scrt: row schema mismatch during marshal");
            }
            writer.writeRow(record);
            continue;
        }
        if (!isRecordShape(record)) {
            throw new Error("scrt: marshal expects plain objects, maps, or Row instances");
        }
        populateRow(scratch, schema, record);
        writer.writeRow(scratch);
    }
    return writer.finish();
}
export function unmarshalRecords(source, schema, options) {
    const results = [];
    for (const record of streamDecodedRows(source, schema, options)) {
        results.push(record);
    }
    return results;
}
export function* streamDecodedRows(source, schema, options) {
    if (!schema) {
        throw new Error("scrt: schema is required for unmarshal");
    }
    const resolved = resolveUnmarshalOptions(options);
    const reader = new Reader(normalizeBinarySource(source), schema, { zeroCopyBytes: resolved.zeroCopyBytes });
    const row = new Row(schema);
    while (reader.readRow(row)) {
        const materialized = materializeRow(row, schema, resolved);
        yield materialized;
        row.reset();
    }
}
function toIterable(source) {
    if (isIterable(source)) {
        return source;
    }
    return [source];
}
function isIterable(value) {
    if (value == null) {
        return false;
    }
    if (typeof value === "string") {
        return false;
    }
    const candidate = value;
    return typeof candidate[Symbol.iterator] === "function";
}
function isRecordShape(value) {
    if (value instanceof Map) {
        return true;
    }
    if (value instanceof Date) {
        return false;
    }
    if (value && typeof value === "object") {
        if (Array.isArray(value)) {
            return false;
        }
        if (ArrayBuffer.isView(value) || value instanceof ArrayBuffer) {
            return false;
        }
        return true;
    }
    return false;
}
function populateRow(row, schema, record) {
    row.reset();
    const accessor = createAccessor(record);
    schema.fields.forEach((field, idx) => {
        const raw = accessor(field.name);
        if (raw === undefined || raw === null) {
            return;
        }
        const encoded = encodeFieldValue(field, raw);
        if (encoded) {
            row.setByIndex(idx, encoded);
        }
    });
}
function createAccessor(record) {
    if (record instanceof Map) {
        return (field) => record.get(field);
    }
    return (field) => record[field];
}
function encodeFieldValue(field, raw) {
    if (raw === undefined || raw === null) {
        return null;
    }
    const kind = field.valueKind();
    const slot = { set: true };
    switch (kind) {
        case FieldKind.Uint64:
        case FieldKind.Ref:
            slot.uint = coerceUint(raw, field.name);
            return slot;
        case FieldKind.Int64:
            slot.int = coerceInt(raw, field.name);
            return slot;
        case FieldKind.Float64:
            slot.float = coerceFloat(raw, field.name);
            return slot;
        case FieldKind.Bool:
            slot.bool = coerceBool(raw, field.name);
            return slot;
        case FieldKind.String:
            slot.str = coerceString(raw, field.name);
            return slot;
        case FieldKind.Bytes:
            slot.bytes = coerceBytes(raw, field.name);
            return slot;
        case FieldKind.Date:
            slot.int = encodeDate(coerceDate(raw, field.name, FieldKind.Date));
            return slot;
        case FieldKind.DateTime:
        case FieldKind.Timestamp:
            slot.int = encodeInstant(coerceDate(raw, field.name, kind));
            return slot;
        case FieldKind.TimestampTZ:
            slot.str = coerceTimestampTZ(raw, field.name);
            return slot;
        case FieldKind.Duration:
            slot.int = coerceDuration(raw, field.name);
            return slot;
        default:
            throw new Error(`scrt: unsupported field kind ${kind} for ${field.name}`);
    }
}
function coerceUint(value, label) {
    const result = coerceInt(value, label);
    if (result < 0n) {
        throw new Error(`scrt: ${label} cannot be negative`);
    }
    return result;
}
function coerceInt(value, label) {
    if (typeof value === "bigint") {
        return value;
    }
    if (typeof value === "number") {
        if (!Number.isFinite(value) || !Number.isInteger(value)) {
            throw new Error(`scrt: ${label} must be a finite integer`);
        }
        if (Math.abs(value) > Number.MAX_SAFE_INTEGER) {
            throw new Error(`scrt: ${label} exceeds safe integer range`);
        }
        return BigInt(value);
    }
    if (typeof value === "string") {
        const trimmed = value.trim();
        if (!trimmed) {
            throw new Error(`scrt: ${label} cannot be empty`);
        }
        return BigInt(trimmed);
    }
    throw new Error(`scrt: ${label} expects an integer-compatible value`);
}
function coerceFloat(value, label) {
    if (typeof value === "number") {
        if (!Number.isFinite(value)) {
            throw new Error(`scrt: ${label} must be finite`);
        }
        return value;
    }
    if (typeof value === "bigint") {
        return Number(value);
    }
    if (typeof value === "string") {
        const parsed = Number(value.trim());
        if (Number.isNaN(parsed)) {
            throw new Error(`scrt: ${label} cannot parse float literal`);
        }
        return parsed;
    }
    throw new Error(`scrt: ${label} expects a float-compatible value`);
}
function coerceBool(value, label) {
    if (typeof value === "boolean") {
        return value;
    }
    if (typeof value === "number") {
        if (!Number.isFinite(value)) {
            throw new Error(`scrt: ${label} must be finite`);
        }
        return value !== 0;
    }
    if (typeof value === "string") {
        const normalized = value.trim().toLowerCase();
        if (normalized === "true" || normalized === "1") {
            return true;
        }
        if (normalized === "false" || normalized === "0") {
            return false;
        }
        throw new Error(`scrt: ${label} cannot parse boolean literal`);
    }
    throw new Error(`scrt: ${label} expects a boolean-compatible value`);
}
function coerceString(value, label) {
    if (typeof value === "string") {
        return value;
    }
    if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
        return String(value);
    }
    if (value instanceof Date) {
        if (!Number.isFinite(value.getTime())) {
            throw new Error(`scrt: ${label} received invalid Date`);
        }
        return value.toISOString();
    }
    if (value instanceof Uint8Array) {
        return textDecoder.decode(value);
    }
    throw new Error(`scrt: ${label} expects a string-compatible value`);
}
function coerceBytes(value, label) {
    if (value instanceof Uint8Array) {
        return value.slice();
    }
    if (ArrayBuffer.isView(value)) {
        const view = value;
        return new Uint8Array(view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength));
    }
    if (value instanceof ArrayBuffer) {
        return new Uint8Array(value.slice(0));
    }
    if (Array.isArray(value)) {
        const out = new Uint8Array(value.length);
        value.forEach((entry, idx) => {
            if (typeof entry !== "number" || !Number.isFinite(entry)) {
                throw new Error(`scrt: ${label} byte array contains non-number at index ${idx}`);
            }
            out[idx] = entry & 0xff;
        });
        return out;
    }
    if (typeof value === "string") {
        return textEncoder.encode(value);
    }
    throw new Error(`scrt: ${label} expects bytes, ArrayBufferView, or string input`);
}
function coerceDate(value, label, kind) {
    if (value instanceof Date) {
        if (!Number.isFinite(value.getTime())) {
            throw new Error(`scrt: ${label} received invalid Date`);
        }
        return value;
    }
    if (typeof value === "string") {
        const trimmed = value.trim();
        if (!trimmed) {
            throw new Error(`scrt: ${label} cannot parse empty temporal literal`);
        }
        switch (kind) {
            case FieldKind.Date:
                return parseDate(trimmed);
            case FieldKind.DateTime:
                return parseDateTime(trimmed);
            case FieldKind.Timestamp:
                return parseTimestamp(trimmed);
            default:
                return parseTimestamp(trimmed);
        }
    }
    if (typeof value === "number") {
        if (!Number.isFinite(value)) {
            throw new Error(`scrt: ${label} must be finite`);
        }
        return dateFromNumber(value);
    }
    if (typeof value === "bigint") {
        return dateFromBigInt(value);
    }
    throw new Error(`scrt: ${label} expects Date, number, bigint, or string`);
}
function coerceTimestampTZ(value, label) {
    if (value instanceof Date) {
        if (!Number.isFinite(value.getTime())) {
            throw new Error(`scrt: ${label} received invalid Date`);
        }
        return formatTimestampTZ(value);
    }
    if (typeof value === "string") {
        return canonicalTimestampTZ(value);
    }
    if (typeof value === "number") {
        if (!Number.isFinite(value)) {
            throw new Error(`scrt: ${label} must be finite`);
        }
        return formatTimestampTZ(dateFromNumber(value));
    }
    if (typeof value === "bigint") {
        return formatTimestampTZ(dateFromBigInt(value));
    }
    throw new Error(`scrt: ${label} expects Date, number, bigint, or string`);
}
function coerceDuration(value, label) {
    if (typeof value === "bigint") {
        return value;
    }
    if (typeof value === "number") {
        if (!Number.isFinite(value) || !Number.isInteger(value)) {
            throw new Error(`scrt: ${label} duration must be a finite integer`);
        }
        if (Math.abs(value) > Number.MAX_SAFE_INTEGER) {
            throw new Error(`scrt: ${label} duration exceeds safe integer range`);
        }
        return BigInt(value);
    }
    if (typeof value === "string") {
        return parseDuration(value);
    }
    throw new Error(`scrt: ${label} expects bigint, number, or duration literal`);
}
function dateFromNumber(value) {
    if (Number.isInteger(value)) {
        return decodeInstant(inferEpochNanoseconds(BigInt(value)));
    }
    const whole = Math.trunc(value);
    const fractional = value - whole;
    const nanos = BigInt(whole) * 1000000000n + BigInt(Math.trunc(fractional * 1_000_000_000));
    return decodeInstant(nanos);
}
function dateFromBigInt(value) {
    return decodeInstant(inferEpochNanoseconds(value));
}
function normalizeBinarySource(source) {
    if (source instanceof Uint8Array) {
        return source;
    }
    if (ArrayBuffer.isView(source)) {
        const view = source;
        return new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
    }
    if (source instanceof ArrayBuffer) {
        return new Uint8Array(source);
    }
    throw new Error("scrt: unsupported binary source");
}
function resolveUnmarshalOptions(options) {
    return {
        zeroCopyBytes: options?.zeroCopyBytes ?? false,
        numericMode: options?.numericMode ?? "auto",
        temporalMode: options?.temporalMode ?? "date",
        durationMode: options?.durationMode ?? "bigint",
        objectFactory: options?.objectFactory ?? (() => ({})),
    };
}
function materializeRow(row, schema, options) {
    const target = options.objectFactory();
    const values = row.valuesSlice();
    schema.fields.forEach((field, idx) => {
        const slot = values[idx];
        if (!slot.set) {
            return;
        }
        const decoded = decodeFieldValue(field, slot, options);
        assignToTarget(target, field.name, decoded);
    });
    return target;
}
function assignToTarget(target, key, value) {
    if (target instanceof Map) {
        target.set(key, value);
        return;
    }
    target[key] = value;
}
function decodeFieldValue(field, slot, options) {
    const kind = field.valueKind();
    switch (kind) {
        case FieldKind.Uint64:
        case FieldKind.Ref:
            return convertBigInt(slot.uint ?? 0n, options.numericMode, field.name, true);
        case FieldKind.Int64:
            return convertBigInt(slot.int ?? 0n, options.numericMode, field.name, false);
        case FieldKind.Float64:
            return slot.float ?? 0;
        case FieldKind.Bool:
            return slot.bool ?? false;
        case FieldKind.String:
            return slot.str ?? "";
        case FieldKind.Bytes:
            return slot.bytes ?? new Uint8Array();
        case FieldKind.Date: {
            const decoded = decodeDate(slot.int ?? 0n);
            return options.temporalMode === "string" ? formatDate(decoded) : decoded;
        }
        case FieldKind.DateTime:
        case FieldKind.Timestamp: {
            const decoded = decodeInstant(slot.int ?? 0n);
            return options.temporalMode === "string" ? formatInstant(decoded) : decoded;
        }
        case FieldKind.TimestampTZ: {
            const str = slot.str ?? "";
            if (!str) {
                return options.temporalMode === "date" ? new Date(0) : "";
            }
            if (options.temporalMode === "string") {
                return str;
            }
            return parseTimestampTZ(str);
        }
        case FieldKind.Duration:
            return convertDuration(slot.int ?? 0n, options.durationMode, field.name);
        default:
            throw new Error(`scrt: unsupported field kind ${kind}`);
    }
}
function convertBigInt(value, mode, label, unsigned) {
    if (unsigned && value < 0n) {
        throw new Error(`scrt: ${label} stored value cannot be negative`);
    }
    switch (mode) {
        case "bigint":
            return value;
        case "number":
            if (!fitsSafeInteger(value)) {
                throw new Error(`scrt: ${label} exceeds JS safe integer range`);
            }
            return Number(value);
        default:
            return fitsSafeInteger(value) ? Number(value) : value;
    }
}
function convertDuration(value, mode, label) {
    switch (mode) {
        case "bigint":
            return value;
        case "number":
            if (!fitsSafeInteger(value)) {
                throw new Error(`scrt: duration ${label} exceeds JS safe integer range`);
            }
            return Number(value);
        case "string":
            return formatDuration(value);
        default:
            return value;
    }
}
function fitsSafeInteger(value) {
    return value <= BigInt(Number.MAX_SAFE_INTEGER) && value >= BigInt(Number.MIN_SAFE_INTEGER);
}
