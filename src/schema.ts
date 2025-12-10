import { encodeDate, encodeInstant, formatTimestampTZ, parseDate, parseDateTime, parseDuration, parseTimestamp, parseTimestampTZ } from "./temporal";

const FNV_OFFSET = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;

export enum FieldKind {
    Invalid = 0,
    Uint64 = 1,
    String = 2,
    Ref = 3,
    Bool = 4,
    Int64 = 5,
    Float64 = 6,
    Bytes = 7,
    Date = 8,
    DateTime = 9,
    Timestamp = 10,
    TimestampTZ = 11,
    Duration = 12,
}

export class DefaultValue {
    constructor(
        public kind: FieldKind,
        public boolValue?: boolean,
        public intValue?: bigint,
        public uintValue?: bigint,
        public floatValue?: number,
        public stringValue?: string,
        public bytesValue?: Uint8Array,
    ) { }

    hashKey(): string {
        switch (this.kind) {
            case FieldKind.Bool:
                return `bool:${this.boolValue ? 1 : 0}`;
            case FieldKind.Int64:
                return `int:${this.intValue ?? 0n}`;
            case FieldKind.Uint64:
            case FieldKind.Ref:
                return `uint:${this.uintValue ?? 0n}`;
            case FieldKind.Float64:
                return `float:${this.floatValue ?? 0}`;
            case FieldKind.String:
                return `string:${this.stringValue ?? ""}`;
            case FieldKind.Bytes:
                return `bytes:${bytesToBase64(this.bytesValue ?? new Uint8Array())}`;
            case FieldKind.Date:
            case FieldKind.DateTime:
            case FieldKind.Timestamp:
            case FieldKind.Duration:
                return `int:${this.intValue ?? 0n}`;
            case FieldKind.TimestampTZ:
                return `timestamptz:${this.stringValue ?? ""}`;
            default:
                return "";
        }
    }
}

export class Field {
    public resolvedKind: FieldKind = FieldKind.Invalid;
    public pendingDefault = "";

    constructor(
        public readonly name: string,
        public readonly kind: FieldKind,
        public readonly rawType: string,
        public targetSchema = "",
        public targetField = "",
        public autoIncrement = false,
        public attributes: string[] = [],
        public defaultValue?: DefaultValue,
    ) { }

    valueKind(): FieldKind {
        if (this.kind === FieldKind.Ref) {
            return this.resolvedKind === FieldKind.Invalid ? FieldKind.Uint64 : this.resolvedKind;
        }
        return this.resolvedKind === FieldKind.Invalid ? this.kind : this.resolvedKind;
    }

    isReference(): boolean {
        return this.kind === FieldKind.Ref && !!this.targetSchema && !!this.targetField;
    }
}

export class Schema {
    private fingerprintCache?: bigint;
    private fieldIndex?: Map<string, number>;

    constructor(public readonly name: string, public readonly fields: Field[]) { }

    fingerprint(): bigint {
        if (this.fingerprintCache !== undefined) {
            return this.fingerprintCache;
        }
        let hash = FNV_OFFSET;
        const write = (str: string): void => {
            for (let i = 0; i < str.length; i += 1) {
                hash ^= BigInt(str.charCodeAt(i));
                hash = BigInt.asUintN(64, hash * FNV_PRIME);
            }
        };
        write(this.name);
        for (const field of this.fields) {
            write("|");
            write(field.name);
            write(":");
            write(field.rawType);
            if (field.targetSchema) {
                write("->");
                write(`${field.targetSchema}.${field.targetField}`);
            }
            if (field.autoIncrement) {
                write("+auto");
            }
            if (field.attributes.length) {
                const attrs = [...field.attributes].sort();
                for (const attr of attrs) {
                    write(`@${attr}`);
                }
            }
            if (field.defaultValue) {
                write("=def:");
                write(field.defaultValue.hashKey());
            }
        }
        this.fingerprintCache = BigInt.asUintN(64, hash);
        return this.fingerprintCache;
    }

    fieldIndexByName(name: string): number {
        if (!this.fieldIndex) {
            this.fieldIndex = new Map();
            this.fields.forEach((field, idx) => this.fieldIndex!.set(field.name, idx));
        }
        const idx = this.fieldIndex.get(name);
        if (idx === undefined) {
            throw new Error(`scrt: field ${name} not found in schema ${this.name}`);
        }
        return idx;
    }

    tryFieldIndex(name: string): number | undefined {
        if (!this.fieldIndex) {
            this.fieldIndex = new Map();
            this.fields.forEach((field, idx) => this.fieldIndex!.set(field.name, idx));
        }
        return this.fieldIndex.get(name);
    }
}

export class Document {
    constructor(
        public readonly schemas: Map<string, Schema>,
        public readonly data: Map<string, Record<string, unknown>[]>,
        public source?: string,
    ) { }

    schema(name: string): Schema | undefined {
        return this.schemas.get(name);
    }

    records(name: string): Record<string, unknown>[] | undefined {
        return this.data.get(name);
    }

    finalize(): void {
        for (const schema of this.schemas.values()) {
            resolveSchemaKinds(this, schema);
        }
    }
}

export function parseSchema(text: string): Document {
    const lines = text.split(/\r?\n/).map((line) => line.trim());
    const schemas = new Map<string, Schema>();
    const data = new Map<string, Record<string, unknown>[]>();

    let current: Schema | undefined;
    let awaitingName = false;
    let currentData = "";

    const finishCurrent = (): void => {
        if (!current) {
            return;
        }
        if (schemas.has(current.name)) {
            throw new Error(`scrt: duplicate schema ${current.name}`);
        }
        schemas.set(current.name, current);
        current = undefined;
    };

    const startSchema = (name: string): void => {
        finishCurrent();
        if (!name) {
            throw new Error("scrt: schema name cannot be empty");
        }
        current = new Schema(name, []);
    };

    for (const line of lines) {
        if (!line) {
            continue;
        }
        if (awaitingName) {
            startSchema(line);
            awaitingName = false;
            continue;
        }
        if (line.startsWith("@schema")) {
            currentData = "";
            let rest = line.slice("@schema".length).trim();
            if (rest.startsWith(":")) {
                rest = rest.slice(1).trim();
            }
            if (!rest) {
                awaitingName = true;
                continue;
            }
            startSchema(rest);
            continue;
        }
        if (line.startsWith("@field")) {
            currentData = "";
            if (!current) {
                throw new Error("scrt: @field outside schema block");
            }
            const field = parseField(line.slice("@field".length).trim());
            current.fields.push(field);
            continue;
        }
        if (line.startsWith("@")) {
            awaitingName = false;
            finishCurrent();
            if (line.includes("=") && currentData) {
                const sch = schemas.get(currentData);
                if (sch) {
                    const row = parseDataRow(line, sch);
                    pushDataRow(data, currentData, row);
                }
                continue;
            }
            currentData = line.slice(1).trim();
            continue;
        }
        if (currentData) {
            const sch = schemas.get(currentData);
            if (!sch) {
                continue;
            }
            const row = parseDataRow(line, sch);
            pushDataRow(data, currentData, row);
            continue;
        }
    }

    finishCurrent();
    const doc = new Document(schemas, data);
    doc.finalize();
    return doc;
}

function pushDataRow(store: Map<string, Record<string, unknown>[]>, schemaName: string, row: Record<string, unknown>): void {
    if (!store.has(schemaName)) {
        store.set(schemaName, []);
    }
    store.get(schemaName)!.push(row);
}

function parseField(body: string): Field {
    const [name, typ, attrChunk] = splitFieldParts(body);
    const { kind, targetSchema, targetField } = interpretFieldType(typ);
    const field = new Field(name, kind, typ, targetSchema, targetField);
    if (attrChunk) {
        const attrs = splitFieldAttributes(attrChunk);
        for (const attr of attrs) {
            const lower = attr.toLowerCase();
            switch (true) {
                case lower === "auto_increment" || lower === "autoincrement" || lower === "serial":
                    field.autoIncrement = true;
                    break;
                case lower.startsWith("default="):
                case lower.startsWith("default:"):
                    assignFieldDefault(field, extractDefaultLiteral(attr));
                    break;
                default:
                    break;
            }
            field.attributes.push(lower);
        }
    }
    return field;
}

function splitFieldParts(body: string): [string, string, string] {
    const trimmed = body.trim();
    const firstSep = trimmed.search(/[ \t]/);
    if (firstSep === -1) {
        throw new Error(`scrt: invalid @field declaration ${body}`);
    }
    const name = trimmed.slice(0, firstSep).trim();
    const rest = trimmed.slice(firstSep + 1).trim();
    const secondSep = rest.search(/[ \t]/);
    if (secondSep === -1) {
        return [name, rest, ""];
    }
    return [name, rest.slice(0, secondSep).trim(), rest.slice(secondSep + 1).trim()];
}

function interpretFieldType(raw: string): { kind: FieldKind; targetSchema: string; targetField: string } {
    const typ = raw.toLowerCase();
    switch (true) {
        case typ === "uint64":
            return { kind: FieldKind.Uint64, targetSchema: "", targetField: "" };
        case typ === "string":
            return { kind: FieldKind.String, targetSchema: "", targetField: "" };
        case typ === "bool":
            return { kind: FieldKind.Bool, targetSchema: "", targetField: "" };
        case typ === "int64":
            return { kind: FieldKind.Int64, targetSchema: "", targetField: "" };
        case typ === "float64":
            return { kind: FieldKind.Float64, targetSchema: "", targetField: "" };
        case typ === "bytes":
            return { kind: FieldKind.Bytes, targetSchema: "", targetField: "" };
        case typ === "date":
            return { kind: FieldKind.Date, targetSchema: "", targetField: "" };
        case typ === "datetime":
            return { kind: FieldKind.DateTime, targetSchema: "", targetField: "" };
        case typ === "timestamp":
            return { kind: FieldKind.Timestamp, targetSchema: "", targetField: "" };
        case typ === "timestamptz":
            return { kind: FieldKind.TimestampTZ, targetSchema: "", targetField: "" };
        case typ === "duration":
            return { kind: FieldKind.Duration, targetSchema: "", targetField: "" };
        case typ.startsWith("ref:"):
            const [, schemaName, fieldName] = raw.split(":");
            return { kind: FieldKind.Ref, targetSchema: schemaName ?? "", targetField: fieldName ?? "" };
        default:
            throw new Error(`scrt: unsupported field type ${raw}`);
    }
}

function splitFieldAttributes(attrChunk: string): string[] {
    const attrs: string[] = [];
    let current = "";
    let quote: string | null = null;
    for (const ch of attrChunk) {
        if ((ch === '"' || ch === "'" || ch === "`") && quote === null) {
            quote = ch;
            current += ch;
        } else if (quote && ch === quote) {
            quote = null;
            current += ch;
        } else if (!quote && ch === ',') {
            if (current.trim()) {
                attrs.push(current.trim());
            }
            current = "";
        } else {
            current += ch;
        }
    }
    if (current.trim()) {
        attrs.push(current.trim());
    }
    return attrs;
}

function assignFieldDefault(field: Field, literalRaw: string): void {
    const literal = literalRaw.trim();
    if (!literal) {
        return;
    }
    if (field.kind === FieldKind.Ref) {
        field.pendingDefault = literal;
        return;
    }
    field.defaultValue = parseDefaultLiteral(field.kind, literal);
}

function extractDefaultLiteral(attr: string): string {
    const sepIdx = attr.indexOf("=") >= 0 ? attr.indexOf("=") : attr.indexOf(":");
    if (sepIdx === -1) {
        return attr;
    }
    return attr.slice(sepIdx + 1);
}

function parseDefaultLiteral(kind: FieldKind, literal: string): DefaultValue {
    switch (kind) {
        case FieldKind.Bool:
            return new DefaultValue(kind, literal.toLowerCase() === "true" || literal === "1");
        case FieldKind.Int64:
            return new DefaultValue(kind, undefined, BigInt(literal));
        case FieldKind.Uint64:
        case FieldKind.Ref:
            return new DefaultValue(kind, undefined, undefined, BigInt(literal));
        case FieldKind.Float64:
            return new DefaultValue(kind, undefined, undefined, undefined, Number(literal));
        case FieldKind.String:
            return new DefaultValue(kind, undefined, undefined, undefined, undefined, parseStringLiteral(literal));
        case FieldKind.Bytes:
            return new DefaultValue(kind, undefined, undefined, undefined, undefined, undefined, parseBytesLiteral(literal));
        case FieldKind.Date:
            return new DefaultValue(kind, undefined, encodeDate(parseDate(stripQuotes(literal))));
        case FieldKind.DateTime:
            return new DefaultValue(kind, undefined, encodeInstant(parseDateTime(stripQuotes(literal))));
        case FieldKind.Timestamp:
            return new DefaultValue(kind, undefined, encodeInstant(parseTimestamp(stripQuotes(literal))));
        case FieldKind.TimestampTZ: {
            const ts = parseTimestampTZ(stripQuotes(literal));
            return new DefaultValue(kind, undefined, undefined, undefined, undefined, formatTimestampTZ(ts));
        }
        case FieldKind.Duration:
            return new DefaultValue(kind, undefined, parseDuration(stripQuotes(literal)));
        default:
            throw new Error(`scrt: defaults not supported for kind ${kind}`);
    }
}

function parseStringLiteral(raw: string): string {
    const trimmed = raw.trim();
    if (!trimmed) {
        return "";
    }
    if (trimmed.startsWith("\"") || trimmed.startsWith("'") || trimmed.startsWith("`")) {
        return trimmed.slice(1, -1);
    }
    return trimmed;
}

function stripQuotes(raw: string): string {
    const trimmed = raw.trim();
    if (!trimmed) {
        return "";
    }
    if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) || (trimmed.startsWith("'") && trimmed.endsWith("'")) || (trimmed.startsWith("`") && trimmed.endsWith("`"))) {
        return trimmed.slice(1, -1);
    }
    return trimmed;
}

function parseBytesLiteral(raw: string): Uint8Array {
    const trimmed = raw.trim();
    if (trimmed.startsWith("0x") || trimmed.startsWith("0X")) {
        const hex = trimmed.slice(2);
        if (hex.length % 2 !== 0) {
            throw new Error(`scrt: invalid hex literal ${raw}`);
        }
        const bytes = new Uint8Array(hex.length / 2);
        for (let i = 0; i < hex.length; i += 2) {
            bytes[i / 2] = parseInt(hex.slice(i, i + 2), 16);
        }
        return bytes;
    }
    return new TextEncoder().encode(stripQuotes(trimmed));
}

function parseDataRow(line: string, schema: Schema): Record<string, unknown> {
    const row: Record<string, unknown> = {};
    const tokens = line.split(',');
    let fieldIdx = 0;
    let remaining = countValueTokens(tokens);
    const skipAuto = (): void => {
        while (fieldIdx < schema.fields.length && schema.fields[fieldIdx]!.autoIncrement) {
            const nonAuto = countNonAuto(schema.fields, fieldIdx);
            if (remaining > nonAuto) {
                return;
            }
            fieldIdx += 1;
        }
    };
    for (const token of tokens) {
        const trimmed = token.trim();
        if (!trimmed) {
            fieldIdx += 1;
            continue;
        }
        if (trimmed.startsWith("@")) {
            const { index, value } = applyExplicitAssignment(schema, trimmed.slice(1));
            if (index >= 0) {
                row[schema.fields[index]!.name] = value;
                fieldIdx = Math.max(fieldIdx, index + 1);
            }
            continue;
        }
        skipAuto();
        if (fieldIdx >= schema.fields.length) {
            throw new Error("scrt: too many values in row");
        }
        const field = schema.fields[fieldIdx]!;
        row[field.name] = parseValue(trimmed, field);
        fieldIdx += 1;
        remaining -= 1;
    }
    return row;
}

function countValueTokens(tokens: string[]): number {
    return tokens.reduce((acc, token) => {
        const trimmed = token.trim();
        if (!trimmed || trimmed.startsWith("@")) {
            return acc;
        }
        return acc + 1;
    }, 0);
}

function countNonAuto(fields: Field[], start: number): number {
    let count = 0;
    for (let i = start; i < fields.length; i += 1) {
        if (!fields[i]!.autoIncrement) {
            count += 1;
        }
    }
    return count;
}

function applyExplicitAssignment(schema: Schema, expr: string): { index: number; value: unknown } {
    const [fieldToken, rawValue] = expr.split("=", 2);
    if (!rawValue) {
        throw new Error(`scrt: invalid assignment ${expr}`);
    }
    const normalized = normalizeAssignmentTarget(fieldToken);
    const idx = schema.tryFieldIndex(normalized);
    if (idx === undefined) {
        throw new Error(`scrt: field ${normalized} not found`);
    }
    const field = schema.fields[idx]!;
    return { index: idx, value: parseValue(rawValue.trim(), field) };
}

function normalizeAssignmentTarget(token: string): string {
    const trimmed = token.trim();
    const parts = trimmed.split(":");
    if (parts.length >= 2) {
        return parts[1]!;
    }
    return parts[0]!;
}

function parseValue(raw: string, field: Field): unknown {
    const kind = field.valueKind();
    const trimmed = raw.trim();
    switch (kind) {
        case FieldKind.Uint64:
            return BigInt(trimmed);
        case FieldKind.Int64:
            return BigInt(trimmed);
        case FieldKind.Float64:
            return Number(trimmed);
        case FieldKind.Bool:
            return trimmed.toLowerCase() === "true" || trimmed === "1";
        case FieldKind.String:
            return stripQuotes(trimmed);
        case FieldKind.Bytes:
            return parseBytesLiteral(trimmed);
        case FieldKind.Date:
            return parseDate(stripQuotes(trimmed));
        case FieldKind.DateTime:
            return parseDateTime(stripQuotes(trimmed));
        case FieldKind.Timestamp:
            return parseTimestamp(stripQuotes(trimmed));
        case FieldKind.TimestampTZ:
            return parseTimestampTZ(stripQuotes(trimmed));
        case FieldKind.Duration:
            return parseDuration(stripQuotes(trimmed));
        default:
            return trimmed;
    }
}

function resolveSchemaKinds(doc: Document, schema: Schema): void {
    schema.fields.forEach((field, idx) => resolveFieldKind(doc, schema, idx, new Set()));
}

function resolveFieldKind(doc: Document, schema: Schema, idx: number, stack: Set<string>): FieldKind {
    const field = schema.fields[idx]!;
    if (field.resolvedKind !== FieldKind.Invalid) {
        return field.resolvedKind;
    }
    if (field.kind !== FieldKind.Ref) {
        field.resolvedKind = field.kind;
        if (field.pendingDefault && !field.defaultValue) {
            field.defaultValue = parseDefaultLiteral(field.resolvedKind, field.pendingDefault);
            field.pendingDefault = "";
        }
        return field.resolvedKind;
    }
    const key = `${schema.name}.${field.name}`;
    if (stack.has(key)) {
        throw new Error(`scrt: circular reference detected for ${key}`);
    }
    stack.add(key);
    const targetSchema = doc.schemas.get(field.targetSchema);
    if (!targetSchema) {
        throw new Error(`scrt: schema ${schema.name} references unknown schema ${field.targetSchema}`);
    }
    const targetIdx = targetSchema.tryFieldIndex(field.targetField);
    if (targetIdx === undefined) {
        throw new Error(`scrt: schema ${schema.name} references unknown field ${field.targetSchema}.${field.targetField}`);
    }
    const resolved = resolveFieldKind(doc, targetSchema, targetIdx, stack);
    field.resolvedKind = resolved;
    stack.delete(key);
    if (field.pendingDefault && !field.defaultValue) {
        field.defaultValue = parseDefaultLiteral(resolved, field.pendingDefault);
        field.pendingDefault = "";
    }
    return resolved;
}

function bytesToBase64(bytes: Uint8Array): string {
    if (typeof Buffer !== "undefined") {
        return Buffer.from(bytes).toString("base64");
    }
    let binary = "";
    for (let i = 0; i < bytes.length; i += 1) {
        binary += String.fromCharCode(bytes[i]!);
    }
    if (typeof btoa === "function") {
        return btoa(binary);
    }
    throw new Error("scrt: base64 encoding unavailable in this environment");
}
