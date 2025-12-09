import { promises as fs } from "fs";
import { TextEncoder, TextDecoder } from "util";

export enum FieldKind {
  Invalid = 0,
  Uint64 = 1,
  String = 2,
  Ref = 3,
  Bool = 4,
  Int64 = 5,
  Float64 = 6,
  Bytes = 7,
}

export interface DefaultValue {
  kind: FieldKind;
  bool?: boolean;
  int?: number;
  uint?: number;
  float?: number;
  string?: string;
  bytes?: Uint8Array;
}

export interface Field {
  name: string;
  kind: FieldKind;
  rawType: string;
  targetSchema?: string;
  targetField?: string;
  autoIncrement?: boolean;
  attributes: string[];
  defaultValue?: DefaultValue;
}

export type Row = Record<string, unknown>;

export class Schema {
  private fieldIndex?: Map<string, number>;
  private fingerprintCache?: bigint;

  constructor(public readonly name: string, public readonly fields: Field[] = []) {}

  fieldIndexFor(name: string): number | undefined {
    if (!this.fieldIndex) {
      this.fieldIndex = new Map();
      this.fields.forEach((field, idx) => this.fieldIndex!.set(field.name, idx));
    }
    return this.fieldIndex.get(name);
  }

  fingerprint(): bigint {
    if (this.fingerprintCache !== undefined) {
      return this.fingerprintCache;
    }
    const h = fnv1a64();
    h.update(this.name);
    for (const field of this.fields) {
      h.update("|");
      h.update(field.name);
      h.update(":");
      h.update(field.rawType);
      if (field.targetSchema) {
        h.update("->");
        h.update(field.targetSchema);
        h.update(".");
        h.update(field.targetField ?? "");
      }
      if (field.autoIncrement) {
        h.update("+auto");
      }
      if (field.attributes.length) {
        const attrs = [...field.attributes].sort();
        attrs.forEach((attr) => {
          h.update("@");
          h.update(attr);
        });
      }
      if (field.defaultValue) {
        h.update("=def:");
        h.update(defaultHashKey(field.defaultValue));
      }
    }
    this.fingerprintCache = h.digest();
    return this.fingerprintCache;
  }
}

export class Document {
  constructor(
    public readonly schemas = new Map<string, Schema>(),
    public readonly data = new Map<string, Row[]>(),
    public source?: string,
  ) {}

  schema(name: string): Schema | undefined {
    return this.schemas.get(name);
  }

  records(name: string): Row[] {
    return this.data.get(name) ?? [];
  }
}

export interface MarshalOptions {
  newline?: "\n" | "\r\n";
  includeSchema?: boolean;
}

export async function fetchSCRT(url: string, init?: RequestInit): Promise<Document> {
  if (typeof fetch !== "function") {
    throw new Error("Global fetch is not available in this runtime");
  }
  const response = await fetch(url, init);
  if (!response.ok) {
    throw new Error(`SCRT fetch failed: ${response.status} ${response.statusText}`);
  }
  const text = await response.text();
  return parseSCRT(text, url);
}

export function marshalToString<T>(schema: Schema, input: Iterable<T>, opts?: MarshalOptions): string {
  const encodeRecord = createRowEncoder<T>(schema);
  function* rowsFromInput(): Generator<Row> {
    for (const record of input) {
      yield encodeRecord(record);
    }
  }
  return serializeSchemaWithRows(schema, rowsFromInput(), opts);
}

export function marshal<T>(schema: Schema, input: Iterable<T>, opts?: MarshalOptions): Uint8Array {
  const text = marshalToString(schema, input, opts);
  return encoder.encode(text);
}

export function marshalRows(schema: Schema, rows: Iterable<Row>, opts?: MarshalOptions): Uint8Array {
  const text = serializeSchemaWithRows(schema, rows, opts);
  return encoder.encode(text);
}

export function unmarshal<T>(
  source: string | Buffer | ArrayBuffer | Uint8Array,
  schema: Schema,
  factory: () => T,
): T[] {
  const text = normalizeInput(source);
  const doc = parseSCRT(text, schema.name);
  const decode = createRowDecoder(schema, factory);
  const rows = doc.records(schema.name);
  return rows.map((row) => decode(row));
}

export function createRowEncoder<T>(schema: Schema): (record: T) => Row {
  const accessors = schema.fields.map((field) => ({
    field,
    read(record: any) {
      if (record == null) {
        return undefined;
      }
      return record[field.name];
    },
  }));
  return (record: T): Row => {
    const out: Row = {};
    for (const { field, read } of accessors) {
      const raw = read(record as any);
      if (raw === undefined || raw === null) {
        continue;
      }
      out[field.name] = coerceValueToField(raw, field);
    }
    return out;
  };
}

export function createRowDecoder<T>(schema: Schema, factory: () => T): (row: Row) => T {
  const fieldList = [...schema.fields];
  return (row: Row): T => {
    const target = factory();
    for (const field of fieldList) {
      if (!(field.name in row)) {
        continue;
      }
      (target as any)[field.name] = cloneRowValue(row[field.name], field.kind);
    }
    return target;
  };
}

export async function loadSCRT(path: string): Promise<Document> {
  const buf = await fs.readFile(path);
  return parseSCRT(buf, path);
}

export function parseSCRT(input: string | Buffer, source?: string): Document {
  const text = typeof input === "string" ? input : input.toString("utf8");
  const doc = new Document(undefined, undefined, source);
  let currentSchema: Schema | undefined;
  let awaitingName = false;
  let currentDataSchema = "";

  const finishCurrent = (): void => {
    if (!currentSchema) {
      return;
    }
    if (doc.schemas.has(currentSchema.name)) {
      throw new Error(`duplicate schema ${currentSchema.name}`);
    }
    doc.schemas.set(currentSchema.name, currentSchema);
    currentSchema = undefined;
  };

  const startSchema = (name: string): void => {
    if (!name.trim()) {
      throw new Error("schema name cannot be empty");
    }
    finishCurrent();
    currentSchema = new Schema(name, []);
  };

  const lines = text.split(/\r?\n/);
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) {
      continue;
    }

    if (awaitingName) {
      startSchema(line);
      awaitingName = false;
      continue;
    }

    if (line.startsWith("@schema")) {
      currentDataSchema = "";
      const rest = line.slice("@schema".length).trim();
      if (!rest || rest === ":") {
        awaitingName = true;
      } else if (rest.startsWith(":")) {
        startSchema(rest.slice(1).trim());
      } else {
        startSchema(rest);
      }
      continue;
    }

    if (line.startsWith("@field")) {
      currentDataSchema = "";
      if (!currentSchema) {
        throw new Error("@field outside of schema");
      }
      const field = parseField(line.slice("@field".length).trim());
      currentSchema.fields.push(field);
      continue;
    }

    if (line.startsWith("@")) {
      awaitingName = false;
      finishCurrent();

      const containsAssignment = line.includes("=");
      if (containsAssignment && currentDataSchema) {
        const schema = doc.schemas.get(currentDataSchema);
        if (schema) {
          const row = parseDataRow(line, schema);
          pushRecord(doc, currentDataSchema, row);
        }
        continue;
      }

      currentDataSchema = line.slice(1).trim();
      continue;
    }

    if (currentDataSchema) {
      const schema = doc.schemas.get(currentDataSchema);
      if (!schema) {
        continue;
      }
      const row = parseDataRow(line, schema);
      pushRecord(doc, currentDataSchema, row);
    }
  }

  if (awaitingName) {
    throw new Error("schema name expected after @schema");
  }
  finishCurrent();
  return doc;
}

export function recordsAs<T>(rows: Row[], factory: () => T): T[] {
  return rows.map((row) => Object.assign(factory(), row));
}

function parseField(body: string): Field {
  const { name, type, attrs } = splitFieldParts(body);
  const base: Field = {
    name,
    rawType: type,
    kind: determineKind(type),
    attributes: [],
  };
  if (type.startsWith("ref:")) {
    const [, targetSchema, targetField] = type.split(":");
    if (!targetSchema || !targetField) {
      throw new Error(`invalid ref declaration: ${type}`);
    }
    base.targetSchema = targetSchema;
    base.targetField = targetField;
  }
  if (!attrs) {
    return base;
  }
  for (const attr of splitAttributes(attrs)) {
    const normalized = attr.toLowerCase();
    base.attributes.push(normalized);
    if (
      normalized === "auto_increment" ||
      normalized === "autoincrement" ||
      normalized === "serial"
    ) {
      base.autoIncrement = true;
      continue;
    }
    if (normalized.startsWith("default=") || normalized.startsWith("default:")) {
      const lowerAttr = attr.toLowerCase();
      const needle = normalized.startsWith("default=") ? "default=" : "default:";
      const idx = lowerAttr.indexOf(needle);
      const literal = attr.slice(idx + needle.length).trim();
      base.defaultValue = parseDefaultLiteral(base.kind, literal);
      continue;
    }
  }
  return base;
}

function splitFieldParts(body: string): { name: string; type: string; attrs: string } {
  const trimmed = body.trim();
  const firstSep = trimmed.search(/[ \t]/);
  if (firstSep === -1) {
    throw new Error(`invalid @field declaration: ${body}`);
  }
  const name = trimmed.slice(0, firstSep).trim();
  let remaining = trimmed.slice(firstSep + 1).trim();
  if (!name || !remaining) {
    throw new Error(`invalid @field declaration: ${body}`);
  }
  const secondSep = remaining.search(/[ \t]/);
  if (secondSep === -1) {
    return { name, type: remaining, attrs: "" };
  }
  const type = remaining.slice(0, secondSep).trim();
  const attrs = remaining.slice(secondSep + 1).trim();
  return { name, type, attrs };
}

function determineKind(typ: string): FieldKind {
  switch (typ) {
    case "uint64":
      return FieldKind.Uint64;
    case "string":
      return FieldKind.String;
    case "bool":
      return FieldKind.Bool;
    case "int64":
      return FieldKind.Int64;
    case "float64":
      return FieldKind.Float64;
    case "bytes":
      return FieldKind.Bytes;
    default:
      if (typ.startsWith("ref:")) {
        return FieldKind.Ref;
      }
      throw new Error(`unsupported field type ${typ}`);
  }
}

function splitAttributes(input: string): string[] {
  const attrs: string[] = [];
  let buffer = "";
  let quote: string | undefined;
  const flush = () => {
    const trimmed = buffer.trim();
    if (trimmed) {
      attrs.push(trimmed);
    }
    buffer = "";
  };
  for (const char of input) {
    if (char === "\"" || char === "'" || char === "`") {
      if (!quote) {
        quote = char;
      } else if (quote === char) {
        quote = undefined;
      }
      buffer += char;
      continue;
    }
    if ((char === "|" || char === "," || char === " " || char === "\t") && !quote) {
      flush();
      continue;
    }
    buffer += char;
  }
  flush();
  return attrs;
}

function parseDataRow(line: string, schema: Schema): Row {
  const row: Row = {};
  const fields = parseCSVLine(line);
  let fieldIdx = 0;
  for (const rawField of fields) {
    let fieldValue = rawField.trim();
    if (!fieldValue) {
      fieldIdx++;
      continue;
    }
    if (fieldValue.startsWith("@")) {
      const [designation, value] = fieldValue.slice(1).split(/=/, 2);
      if (value !== undefined) {
        const parts = designation.split(":");
        const fieldName = parts.length >= 2 ? parts[1] : parts[0];
        const field = schema.fields.find((f) => f.name === fieldName);
        if (!field) {
          throw new Error(`field ${fieldName} not found in schema ${schema.name}`);
        }
        row[fieldName] = parseValue(value, field);
        const idx = schema.fieldIndexFor(fieldName);
        if (idx !== undefined && idx >= fieldIdx) {
          fieldIdx = idx + 1;
        }
        continue;
      }
    }
    if (fieldIdx >= schema.fields.length) {
      throw new Error("too many fields in data row");
    }
    let field = schema.fields[fieldIdx];
    if (field.autoIncrement && !isNumeric(fieldValue)) {
      fieldIdx++;
      if (fieldIdx >= schema.fields.length) {
        throw new Error("too many fields in data row");
      }
      field = schema.fields[fieldIdx];
    }
    row[field.name] = parseValue(fieldValue, field);
    fieldIdx++;
  }
  return row;
}

function parseValue(raw: string, field: Field): unknown {
  const trimmed = raw.trim();
  switch (field.kind) {
    case FieldKind.Uint64:
    case FieldKind.Ref:
      return parseInt(trimmed, 10);
    case FieldKind.Int64:
      return parseInt(trimmed, 10);
    case FieldKind.Float64:
      return parseFloat(trimmed);
    case FieldKind.Bool: {
      const lowered = trimmed.toLowerCase();
      if (lowered === "true" || lowered === "1") {
        return true;
      }
      if (lowered === "false" || lowered === "0") {
        return false;
      }
      throw new Error(`invalid bool literal ${trimmed}`);
    }
    case FieldKind.String:
      return parseStringLiteral(trimmed);
    case FieldKind.Bytes:
      return parseBytesLiteral(trimmed);
    default:
      return trimmed;
  }
}

function parseStringLiteral(raw: string): string {
  if (!raw) {
    return "";
  }
  if (raw.length >= 2) {
    const first = raw[0];
    const last = raw[raw.length - 1];
    if ((first === '"' || first === "'" || first === "`") && first === last) {
      const inner = raw.slice(1, -1);
      if (first === "`") {
        return inner;
      }
      return unescapeString(inner);
    }
  }
  return raw;
}

function parseBytesLiteral(raw: string): Uint8Array {
  if (raw.startsWith("0x") || raw.startsWith("0X")) {
    const clean = raw.slice(2);
    if (clean.length % 2 !== 0) {
      throw new Error(`invalid hex literal ${raw}`);
    }
    const bytes = new Uint8Array(clean.length / 2);
    for (let i = 0; i < clean.length; i += 2) {
      bytes[i / 2] = parseInt(clean.slice(i, i + 2), 16);
    }
    return bytes;
  }
  return encoder.encode(parseStringLiteral(raw));
}

function parseDefaultLiteral(kind: FieldKind, raw: string): DefaultValue {
  switch (kind) {
    case FieldKind.Bool:
      if (raw === "1" || raw.toLowerCase() === "true") {
        return { kind, bool: true };
      }
      if (raw === "0" || raw.toLowerCase() === "false") {
        return { kind, bool: false };
      }
      throw new Error(`invalid bool default ${raw}`);
    case FieldKind.Int64:
      return { kind, int: parseInt(raw, 10) };
    case FieldKind.Uint64:
    case FieldKind.Ref:
      return { kind, uint: parseInt(raw, 10) };
    case FieldKind.Float64:
      return { kind, float: parseFloat(raw) };
    case FieldKind.String:
      return { kind, string: parseStringLiteral(raw) };
    case FieldKind.Bytes:
      return { kind, bytes: parseBytesLiteral(raw) };
    default:
      throw new Error(`defaults not supported for kind ${kind}`);
  }
}

function parseCSVLine(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let quote: string | undefined;
  for (const char of line) {
    if ((char === "\"" || char === "'" || char === "`") && !quote) {
      quote = char;
      current += char;
      continue;
    }
    if (char === quote) {
      quote = undefined;
      current += char;
      continue;
    }
    if (char === "," && !quote) {
      fields.push(current.trim());
      current = "";
      continue;
    }
    current += char;
  }
  if (current) {
    fields.push(current.trim());
  }
  return fields;
}

function isNumeric(value: string): boolean {
  return /^[-+]?\d+$/.test(value.trim());
}

function pushRecord(doc: Document, name: string, row: Row): void {
  const bucket = doc.data.get(name);
  if (bucket) {
    bucket.push(row);
    return;
  }
  doc.data.set(name, [row]);
}

function serializeSchemaWithRows(schema: Schema, rows: Iterable<Row>, opts?: MarshalOptions): string {
  const newline = opts?.newline ?? "\n";
  const includeSchema = opts?.includeSchema ?? true;
  const lines: string[] = [];
  if (includeSchema) {
    lines.push(...schemaDefinitionLines(schema));
    lines.push("");
  }
  lines.push(`@${schema.name}`);
  for (const row of rows) {
    lines.push(formatRow(schema, row));
  }
  lines.push("");
  return lines.join(newline);
}

function schemaDefinitionLines(schema: Schema): string[] {
  const lines = ["@schema", schema.name];
  for (const field of schema.fields) {
    const attrs = fieldAttributesToString(field);
    const suffix = attrs ? ` ${attrs}` : "";
    lines.push(`@field ${field.name} ${field.rawType}${suffix}`);
  }
  return lines;
}

function fieldAttributesToString(field: Field): string {
  const attrs: string[] = [];
  if (field.autoIncrement) {
    attrs.push("auto_increment");
  }
  for (const attr of field.attributes) {
    if (attr === "auto_increment" || attr === "autoincrement" || attr === "serial") {
      continue;
    }
    if (attr.startsWith("default=") || attr.startsWith("default:")) {
      continue;
    }
    attrs.push(attr);
  }
  if (field.defaultValue) {
    attrs.push(`default=${stringifyDefaultLiteral(field.defaultValue)}`);
  }
  return attrs.join("|");
}

function stringifyDefaultLiteral(value: DefaultValue): string {
  switch (value.kind) {
    case FieldKind.Bool:
      return value.bool ? "true" : "false";
    case FieldKind.Int64:
      return `${value.int ?? 0}`;
    case FieldKind.Uint64:
    case FieldKind.Ref:
      return `${value.uint ?? 0}`;
    case FieldKind.Float64:
      return `${value.float ?? 0}`;
    case FieldKind.String:
      return quoteString(value.string ?? "");
    case FieldKind.Bytes:
      return `0x${bytesToHex(value.bytes ?? new Uint8Array())}`;
    default:
      return "";
  }
}

function formatRow(schema: Schema, row: Row): string {
  const cells: string[] = [];
  for (const field of schema.fields) {
    const value = row[field.name];
    if (value === undefined || value === null) {
      cells.push("");
      continue;
    }
    cells.push(formatValueForField(value, field));
  }
  return cells.join(", ");
}

function formatValueForField(value: unknown, field: Field): string {
  switch (field.kind) {
    case FieldKind.Uint64:
    case FieldKind.Ref:
    case FieldKind.Int64:
      return `${Math.trunc(Number(value))}`;
    case FieldKind.Float64: {
      const num = Number(value);
      if (!Number.isFinite(num)) {
        throw new Error(`scrt: field ${field.name} produced non-finite float`);
      }
      return `${num}`;
    }
    case FieldKind.Bool:
      return value ? "true" : "false";
    case FieldKind.String:
      return quoteString(String(value));
    case FieldKind.Bytes:
      return `0x${bytesToHex(asUint8Array(value))}`;
    default:
      return String(value);
  }
}

function quoteString(value: string): string {
  let out = '"';
  for (const ch of value) {
    switch (ch) {
      case '"':
      case '\\':
        out += `\\${ch}`;
        break;
      case "\n":
        out += "\\n";
        break;
      case "\r":
        out += "\\r";
        break;
      case "\t":
        out += "\\t";
        break;
      default:
        out += ch;
        break;
    }
  }
  out += '"';
  return out;
}

function coerceValueToField(value: unknown, field: Field): unknown {
  switch (field.kind) {
    case FieldKind.Uint64:
    case FieldKind.Ref:
      return coerceInteger(value, field.name, false);
    case FieldKind.Int64:
      return coerceInteger(value, field.name, true);
    case FieldKind.Float64:
      return coerceFloat(value, field.name);
    case FieldKind.Bool:
      return coerceBool(value, field.name);
    case FieldKind.String:
      return String(value);
    case FieldKind.Bytes:
      return asUint8Array(value);
    default:
      return value;
  }
}

function cloneRowValue(value: unknown, kind: FieldKind): unknown {
  if (kind === FieldKind.Bytes && value instanceof Uint8Array) {
    return value.slice();
  }
  return value;
}

function coerceInteger(value: unknown, fieldName: string, signed: boolean): number {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(`scrt: field ${fieldName} received non-finite number`);
    }
    const truncated = Math.trunc(value);
    if (!signed && truncated < 0) {
      throw new Error(`scrt: field ${fieldName} expects unsigned integer`);
    }
    return truncated;
  }
  if (typeof value === "bigint") {
    if (!signed && value < 0n) {
      throw new Error(`scrt: field ${fieldName} expects unsigned integer`);
    }
    const asNumber = Number(value);
    if (!Number.isFinite(asNumber)) {
      throw new Error(`scrt: field ${fieldName} bigint out of range`);
    }
    return Math.trunc(asNumber);
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isNaN(parsed)) {
      throw new Error(`scrt: field ${fieldName} expects integer literal`);
    }
    if (!signed && parsed < 0) {
      throw new Error(`scrt: field ${fieldName} expects unsigned integer`);
    }
    return parsed;
  }
  if (typeof value === "boolean") {
    if (!signed && value === false) {
      return 0;
    }
    return value ? 1 : 0;
  }
  throw new Error(`scrt: field ${fieldName} expects ${signed ? "integer" : "unsigned integer"}`);
}

function coerceFloat(value: unknown, fieldName: string): number {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(`scrt: field ${fieldName} received non-finite float`);
    }
    return value;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number.parseFloat(value);
    if (Number.isNaN(parsed)) {
      throw new Error(`scrt: field ${fieldName} expects float literal`);
    }
    return parsed;
  }
  throw new Error(`scrt: field ${fieldName} expects float`);
}

function coerceBool(value: unknown, fieldName: string): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(`scrt: field ${fieldName} received non-finite number`);
    }
    return value !== 0;
  }
  if (typeof value === "string") {
    const lowered = value.trim().toLowerCase();
    if (lowered === "true" || lowered === "1") {
      return true;
    }
    if (lowered === "false" || lowered === "0") {
      return false;
    }
  }
  throw new Error(`scrt: field ${fieldName} expects boolean`);
}

function asUint8Array(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (typeof Buffer !== "undefined" && typeof Buffer.isBuffer === "function" && Buffer.isBuffer(value)) {
    return new Uint8Array(value);
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
  }
  if (Array.isArray(value)) {
    return Uint8Array.from(value);
  }
  if (typeof value === "string") {
    return encoder.encode(value);
  }
  throw new Error("scrt: unsupported bytes source");
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function bytesToBase64(bytes?: Uint8Array): string {
  if (!bytes || bytes.length === 0) {
    return "";
  }
  if (typeof Buffer !== "undefined") {
    return Buffer.from(bytes).toString("base64");
  }
  let binary = "";
  bytes.forEach((b) => {
    binary += String.fromCharCode(b);
  });
  if (typeof btoa === "function") {
    return btoa(binary);
  }
  return manualBase64Encode(binary);
}

function manualBase64Encode(binary: string): string {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  let result = "";
  for (let i = 0; i < binary.length; ) {
    const b1 = binary.charCodeAt(i++);
    const hasB2 = i < binary.length;
    const b2 = hasB2 ? binary.charCodeAt(i++) : 0;
    const hasB3 = i < binary.length;
    const b3 = hasB3 ? binary.charCodeAt(i++) : 0;
    const triplet = (b1 << 16) | (b2 << 8) | b3;
    result += alphabet[(triplet >> 18) & 63];
    result += alphabet[(triplet >> 12) & 63];
    result += hasB2 ? alphabet[(triplet >> 6) & 63] : "=";
    result += hasB2 && hasB3 ? alphabet[triplet & 63] : "=";
  }
  return result;
}

function normalizeInput(source: string | Buffer | ArrayBuffer | Uint8Array): string {
  if (typeof source === "string") {
    return source;
  }
  if (source instanceof Uint8Array) {
    return textDecoder.decode(source);
  }
  if (source instanceof ArrayBuffer) {
    return textDecoder.decode(new Uint8Array(source));
  }
  if (typeof Buffer !== "undefined" && typeof Buffer.isBuffer === "function" && Buffer.isBuffer(source)) {
    return source.toString("utf8");
  }
  throw new Error("scrt: unsupported input type for unmarshal");
}

const encoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8");

function fnv1a64() {
  let hash = 0xcbf29ce484222325n;
  const prime = 0x100000001b3n;
  return {
    update(chunk: string) {
      const bytes = encoder.encode(chunk);
      for (const byte of bytes) {
        hash ^= BigInt(byte);
        hash = (hash * prime) & 0xffffffffffffffffn;
      }
    },
    digest() {
      return hash;
    },
  };
}

function defaultHashKey(value: DefaultValue): string {
  switch (value.kind) {
    case FieldKind.Bool:
      return value.bool ? "bool:1" : "bool:0";
    case FieldKind.Int64:
      return `int:${value.int ?? 0}`;
    case FieldKind.Uint64:
    case FieldKind.Ref:
      return `uint:${value.uint ?? 0}`;
    case FieldKind.Float64:
      return `float:${value.float ?? 0}`;
    case FieldKind.String:
      return `str:${value.string ?? ""}`;
    case FieldKind.Bytes:
      return `bytes:${bytesToBase64(value.bytes)}`;
    default:
      return "";
  }
}

function unescapeString(input: string): string {
  let result = "";
  let escaping = false;
  for (let i = 0; i < input.length; i++) {
    const ch = input[i];
    if (!escaping && ch === "\\") {
      escaping = true;
      continue;
    }
    if (escaping) {
      switch (ch) {
        case "n":
          result += "\n";
          break;
        case "r":
          result += "\r";
          break;
        case "t":
          result += "\t";
          break;
        case "\\":
          result += "\\";
          break;
        case "\"":
          result += "\"";
          break;
        case "'":
          result += "'";
          break;
        case "u": {
          const hex = input.slice(i + 1, i + 5);
          if (hex.length === 4 && /^[0-9a-fA-F]{4}$/.test(hex)) {
            result += String.fromCharCode(parseInt(hex, 16));
            i += 4;
            break;
          }
          result += "u";
          break;
        }
        default:
          result += ch;
          break;
      }
      escaping = false;
      continue;
    }
    result += ch;
  }
  if (escaping) {
    result += "\\";
  }
  return result;
}
