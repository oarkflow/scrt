import { encodeDate, encodeInstant, formatTimestampTZ, parseDate, parseDateTime, parseDuration, parseTimestamp, parseTimestampTZ } from "./temporal";
const FNV_OFFSET = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
export var FieldKind;
(function (FieldKind) {
    FieldKind[FieldKind["Invalid"] = 0] = "Invalid";
    FieldKind[FieldKind["Uint64"] = 1] = "Uint64";
    FieldKind[FieldKind["String"] = 2] = "String";
    FieldKind[FieldKind["Ref"] = 3] = "Ref";
    FieldKind[FieldKind["Bool"] = 4] = "Bool";
    FieldKind[FieldKind["Int64"] = 5] = "Int64";
    FieldKind[FieldKind["Float64"] = 6] = "Float64";
    FieldKind[FieldKind["Bytes"] = 7] = "Bytes";
    FieldKind[FieldKind["Date"] = 8] = "Date";
    FieldKind[FieldKind["DateTime"] = 9] = "DateTime";
    FieldKind[FieldKind["Timestamp"] = 10] = "Timestamp";
    FieldKind[FieldKind["TimestampTZ"] = 11] = "TimestampTZ";
    FieldKind[FieldKind["Duration"] = 12] = "Duration";
})(FieldKind || (FieldKind = {}));
export class DefaultValue {
    kind;
    boolValue;
    intValue;
    uintValue;
    floatValue;
    stringValue;
    bytesValue;
    constructor(kind, boolValue, intValue, uintValue, floatValue, stringValue, bytesValue) {
        this.kind = kind;
        this.boolValue = boolValue;
        this.intValue = intValue;
        this.uintValue = uintValue;
        this.floatValue = floatValue;
        this.stringValue = stringValue;
        this.bytesValue = bytesValue;
    }
    hashKey() {
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
    name;
    kind;
    rawType;
    targetSchema;
    targetField;
    autoIncrement;
    attributes;
    defaultValue;
    resolvedKind = FieldKind.Invalid;
    pendingDefault = "";
    constructor(name, kind, rawType, targetSchema = "", targetField = "", autoIncrement = false, attributes = [], defaultValue) {
        this.name = name;
        this.kind = kind;
        this.rawType = rawType;
        this.targetSchema = targetSchema;
        this.targetField = targetField;
        this.autoIncrement = autoIncrement;
        this.attributes = attributes;
        this.defaultValue = defaultValue;
    }
    valueKind() {
        if (this.kind === FieldKind.Ref) {
            return this.resolvedKind === FieldKind.Invalid ? FieldKind.Uint64 : this.resolvedKind;
        }
        return this.resolvedKind === FieldKind.Invalid ? this.kind : this.resolvedKind;
    }
    isReference() {
        return this.kind === FieldKind.Ref && !!this.targetSchema && !!this.targetField;
    }
}
export class Schema {
    name;
    fields;
    fingerprintCache;
    fieldIndex;
    constructor(name, fields) {
        this.name = name;
        this.fields = fields;
    }
    fingerprint() {
        if (this.fingerprintCache !== undefined) {
            return this.fingerprintCache;
        }
        let hash = FNV_OFFSET;
        const write = (str) => {
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
    fieldIndexByName(name) {
        if (!this.fieldIndex) {
            this.fieldIndex = new Map();
            this.fields.forEach((field, idx) => this.fieldIndex.set(field.name, idx));
        }
        const idx = this.fieldIndex.get(name);
        if (idx === undefined) {
            throw new Error(`scrt: field ${name} not found in schema ${this.name}`);
        }
        return idx;
    }
    tryFieldIndex(name) {
        if (!this.fieldIndex) {
            this.fieldIndex = new Map();
            this.fields.forEach((field, idx) => this.fieldIndex.set(field.name, idx));
        }
        return this.fieldIndex.get(name);
    }
}
export class Document {
    schemas;
    data;
    source;
    constructor(schemas, data, source) {
        this.schemas = schemas;
        this.data = data;
        this.source = source;
    }
    schema(name) {
        return this.schemas.get(name);
    }
    records(name) {
        return this.data.get(name);
    }
    finalize() {
        for (const schema of this.schemas.values()) {
            resolveSchemaKinds(this, schema);
        }
    }
}
export function parseSchema(text) {
    const lines = text.split(/\r?\n/).map((line) => line.trim());
    const schemas = new Map();
    const data = new Map();
    let current;
    let awaitingName = false;
    let currentData = "";
    let fieldBlock = false;
    const finishCurrent = () => {
        if (!current) {
            return;
        }
        if (schemas.has(current.name)) {
            throw new Error(`scrt: duplicate schema ${current.name}`);
        }
        schemas.set(current.name, current);
        current = undefined;
    };
    const startSchema = (name) => {
        finishCurrent();
        fieldBlock = false;
        if (!name) {
            throw new Error("scrt: schema name cannot be empty");
        }
        current = new Schema(name, []);
    };
    for (const line of lines) {
        if (!line) {
            continue;
        }
        if (line.startsWith("#")) {
            continue;
        }
        if (fieldBlock && current && !currentData && !line.startsWith("@")) {
            const field = parseField(line);
            current.fields.push(field);
            continue;
        }
        if (awaitingName) {
            startSchema(line);
            awaitingName = false;
            fieldBlock = false;
            continue;
        }
        if (line.startsWith("@schema")) {
            fieldBlock = false;
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
            fieldBlock = false;
            currentData = "";
            if (!current) {
                throw new Error("scrt: @field outside schema block");
            }
            const field = parseField(line.slice("@field".length).trim());
            current.fields.push(field);
            continue;
        }
        if (line.toLowerCase().startsWith("fields")) {
            if (!current) {
                throw new Error("scrt: fields block outside schema");
            }
            fieldBlock = true;
            continue;
        }
        if (line.startsWith("@")) {
            awaitingName = false;
            fieldBlock = false;
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
function pushDataRow(store, schemaName, row) {
    if (!store.has(schemaName)) {
        store.set(schemaName, []);
    }
    store.get(schemaName).push(row);
}
function parseField(body) {
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
function splitFieldParts(body) {
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
function interpretFieldType(raw) {
    const typ = raw.toLowerCase();
    switch (true) {
        case typ === "uint64" || typ === "uint":
            return { kind: FieldKind.Uint64, targetSchema: "", targetField: "" };
        case typ === "string" || typ === "str" || typ === "text":
            return { kind: FieldKind.String, targetSchema: "", targetField: "" };
        case typ === "bool" || typ === "boolean":
            return { kind: FieldKind.Bool, targetSchema: "", targetField: "" };
        case typ === "int64" || typ === "int":
            return { kind: FieldKind.Int64, targetSchema: "", targetField: "" };
        case typ === "float64" || typ === "float" || typ === "double":
            return { kind: FieldKind.Float64, targetSchema: "", targetField: "" };
        case typ === "bytes" || typ === "blob":
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
function splitFieldAttributes(attrChunk) {
    const attrs = [];
    let current = "";
    let quote = null;
    for (const ch of attrChunk) {
        if ((ch === '"' || ch === "'" || ch === "`") && quote === null) {
            quote = ch;
            current += ch;
        }
        else if (quote && ch === quote) {
            quote = null;
            current += ch;
        }
        else if (!quote && ch === ',') {
            if (current.trim()) {
                attrs.push(current.trim());
            }
            current = "";
        }
        else {
            current += ch;
        }
    }
    if (current.trim()) {
        attrs.push(current.trim());
    }
    return attrs;
}
function assignFieldDefault(field, literalRaw) {
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
function extractDefaultLiteral(attr) {
    const sepIdx = attr.indexOf("=") >= 0 ? attr.indexOf("=") : attr.indexOf(":");
    if (sepIdx === -1) {
        return attr;
    }
    return attr.slice(sepIdx + 1);
}
function parseDefaultLiteral(kind, literal) {
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
function parseStringLiteral(raw) {
    const trimmed = raw.trim();
    if (!trimmed) {
        return "";
    }
    if (trimmed.startsWith("\"") || trimmed.startsWith("'") || trimmed.startsWith("`")) {
        return trimmed.slice(1, -1);
    }
    return trimmed;
}
function stripQuotes(raw) {
    const trimmed = raw.trim();
    if (!trimmed) {
        return "";
    }
    if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) || (trimmed.startsWith("'") && trimmed.endsWith("'")) || (trimmed.startsWith("`") && trimmed.endsWith("`"))) {
        return trimmed.slice(1, -1);
    }
    return trimmed;
}
function parseBytesLiteral(raw) {
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
function parseDataRow(line, schema) {
    const row = {};
    const tokens = line.split(',');
    let fieldIdx = 0;
    let remaining = countValueTokens(tokens);
    const skipAuto = () => {
        while (fieldIdx < schema.fields.length && schema.fields[fieldIdx].autoIncrement) {
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
                row[schema.fields[index].name] = value;
                fieldIdx = Math.max(fieldIdx, index + 1);
            }
            continue;
        }
        skipAuto();
        if (fieldIdx >= schema.fields.length) {
            throw new Error("scrt: too many values in row");
        }
        const field = schema.fields[fieldIdx];
        row[field.name] = parseValue(trimmed, field);
        fieldIdx += 1;
        remaining -= 1;
    }
    return row;
}
function countValueTokens(tokens) {
    return tokens.reduce((acc, token) => {
        const trimmed = token.trim();
        if (!trimmed || trimmed.startsWith("@")) {
            return acc;
        }
        return acc + 1;
    }, 0);
}
function countNonAuto(fields, start) {
    let count = 0;
    for (let i = start; i < fields.length; i += 1) {
        if (!fields[i].autoIncrement) {
            count += 1;
        }
    }
    return count;
}
function applyExplicitAssignment(schema, expr) {
    const [fieldToken, rawValue] = expr.split("=", 2);
    if (!rawValue) {
        throw new Error(`scrt: invalid assignment ${expr}`);
    }
    const normalized = normalizeAssignmentTarget(fieldToken);
    const idx = schema.tryFieldIndex(normalized);
    if (idx === undefined) {
        throw new Error(`scrt: field ${normalized} not found`);
    }
    const field = schema.fields[idx];
    return { index: idx, value: parseValue(rawValue.trim(), field) };
}
function normalizeAssignmentTarget(token) {
    const trimmed = token.trim();
    const parts = trimmed.split(":");
    if (parts.length >= 2) {
        return parts[1];
    }
    return parts[0];
}
function parseValue(raw, field) {
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
function resolveSchemaKinds(doc, schema) {
    schema.fields.forEach((field, idx) => resolveFieldKind(doc, schema, idx, new Set()));
}
function resolveFieldKind(doc, schema, idx, stack) {
    const field = schema.fields[idx];
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
function bytesToBase64(bytes) {
    if (typeof Buffer !== "undefined") {
        return Buffer.from(bytes).toString("base64");
    }
    let binary = "";
    for (let i = 0; i < bytes.length; i += 1) {
        binary += String.fromCharCode(bytes[i]);
    }
    if (typeof btoa === "function") {
        return btoa(binary);
    }
    throw new Error("scrt: base64 encoding unavailable in this environment");
}
