import { ByteBuffer, bufferToUint8Array, createBuffer, pushBytes, writeUvarint } from "./binary";
import { PageBuilder } from "./page";
import { Field, FieldKind, Schema } from "./schema";
import { readUvarint, readVarint } from "./binary";

const MAGIC = "SCRT";
const VERSION = 2;
const textDecoder = new TextDecoder();

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

export class Row {
  private readonly values: CodecValue[];

  constructor(public readonly schema: Schema) {
    this.values = new Array(schema.fields.length).fill(null).map(() => ({ set: false }));
  }

  reset(): void {
    for (const value of this.values) {
      value.set = false;
      value.uint = undefined;
      value.int = undefined;
      value.float = undefined;
      value.str = undefined;
      value.bytes = undefined;
      value.bool = undefined;
      value.borrowed = undefined;
    }
  }

  setByIndex(idx: number, value: CodecValue): void {
    this.values[idx] = { ...value, set: true };
  }

  valuesSlice(): CodecValue[] {
    return this.values;
  }
}

export class Writer {
  private readonly builder: PageBuilder;
  private readonly output: ByteBuffer = createBuffer();
  private headerWritten = false;

  constructor(private readonly schema: Schema, rowsPerPage = 1024) {
    this.builder = new PageBuilder(schema, rowsPerPage);
  }

  writeRow(row: Row): void {
    if (row.schema !== this.schema) {
      throw new Error("scrt: schema mismatch for row");
    }
    this.ensureHeader();
    const values = row.valuesSlice();
    this.schema.fields.forEach((field, idx) => {
      const value = values[idx]!;
      if (!value.set) {
        this.builder.recordPresence(idx, false);
        return;
      }
      this.builder.recordPresence(idx, true);
      switch (field.valueKind()) {
        case FieldKind.Uint64:
        case FieldKind.Ref:
          this.builder.appendUint(idx, value.uint ?? 0n);
          break;
        case FieldKind.String:
        case FieldKind.TimestampTZ:
          this.builder.appendString(idx, value.str ?? "");
          break;
        case FieldKind.Bool:
          this.builder.appendBool(idx, value.bool ?? false);
          break;
        case FieldKind.Int64:
        case FieldKind.Date:
        case FieldKind.DateTime:
        case FieldKind.Timestamp:
        case FieldKind.Duration:
          this.builder.appendInt(idx, value.int ?? 0n);
          break;
        case FieldKind.Float64:
          this.builder.appendFloat(idx, value.float ?? 0);
          break;
        case FieldKind.Bytes:
          this.builder.appendBytes(idx, value.bytes ?? new Uint8Array());
          break;
        default:
          throw new Error(`scrt: unsupported field kind ${field.valueKind()}`);
      }
    });
    this.builder.sealRow();
    if (this.builder.full()) {
      this.flushPage();
    }
  }

  finish(): Uint8Array {
    this.flushPage();
    return bufferToUint8Array(this.output);
  }

  private ensureHeader(): void {
    if (this.headerWritten) {
      return;
    }
    for (const ch of MAGIC) {
      this.output.push(ch.charCodeAt(0));
    }
    this.output.push(VERSION);
    const fp = this.schema.fingerprint();
    const header = new Uint8Array(8);
    let temp = fp;
    for (let i = 0; i < 8; i += 1) {
      header[i] = Number(temp & 0xffn);
      temp >>= 8n;
    }
    pushBytes(this.output, header);
    this.headerWritten = true;
  }

  private flushPage(): void {
    if (this.builder.rowCount() === 0) {
      return;
    }
    const pageBuf = createBuffer();
    this.builder.encode(pageBuf);
    const lenBuf = createBuffer();
    writeUvarint(lenBuf, pageBuf.length);
    pushBytes(this.output, lenBuf);
    pushBytes(this.output, pageBuf);
    this.builder.reset();
  }
}

interface ReaderOptions {
  zeroCopyBytes?: boolean;
}

interface DecodedColumn {
  kind: FieldKind;
  rowIndexes: number[];
  uints: bigint[];
  stringTable: string[];
  stringIndexes: number[];
  bools: boolean[];
  ints: bigint[];
  floats: number[];
  bytes: Uint8Array[];
}

class DecodedPage {
  rows = 0;
  cursor = 0;
  columns: DecodedColumn[];

  constructor(fieldCount: number) {
    this.columns = new Array(fieldCount).fill(null).map(() => ({
      kind: FieldKind.Invalid,
      rowIndexes: [],
      uints: [],
      stringTable: [],
      stringIndexes: [],
      bools: [],
      ints: [],
      floats: [],
      bytes: [],
    }));
  }
}

export class Reader {
  private readonly state: DecodedPage;
  private offset = 0;
  private headerRead = false;

  constructor(private readonly data: Uint8Array, private readonly schema: Schema, private readonly options: ReaderOptions = {}) {
    this.state = new DecodedPage(schema.fields.length);
  }

  readRow(row: Row): boolean {
    if (!this.headerRead && !this.consumeHeader()) {
      return false;
    }
    if (this.state.cursor >= this.state.rows) {
      if (!this.loadPage()) {
        return false;
      }
    }
    const rowIdx = this.state.cursor;
    const values = row.valuesSlice();
    for (let fieldIdx = 0; fieldIdx < this.schema.fields.length; fieldIdx += 1) {
      const field = this.schema.fields[fieldIdx]!;
      const column = this.state.columns[fieldIdx]!;
      const valueSlot = values[fieldIdx]!;
      const valueIdx = column.rowIndexes[rowIdx] ?? -1;
      if (valueIdx < 0) {
        assignDefaultValue(field, valueSlot);
        continue;
      }
      valueSlot.set = true;
      switch (field.valueKind()) {
        case FieldKind.Uint64:
        case FieldKind.Ref:
          valueSlot.uint = column.uints[valueIdx];
          break;
        case FieldKind.String:
        case FieldKind.TimestampTZ:
          valueSlot.str = column.stringTable[column.stringIndexes[valueIdx] ?? 0] ?? "";
          break;
        case FieldKind.Bool:
          valueSlot.bool = column.bools[valueIdx];
          break;
        case FieldKind.Int64:
        case FieldKind.Date:
        case FieldKind.DateTime:
        case FieldKind.Timestamp:
        case FieldKind.Duration:
          valueSlot.int = column.ints[valueIdx];
          break;
        case FieldKind.Float64:
          valueSlot.float = column.floats[valueIdx];
          break;
        case FieldKind.Bytes:
          valueSlot.bytes = column.bytes[valueIdx];
          valueSlot.borrowed = this.options.zeroCopyBytes ?? false;
          break;
        default:
          throw new Error(`scrt: unsupported field kind ${field.valueKind()}`);
      }
    }
    this.state.cursor += 1;
    return true;
  }

  private consumeHeader(): boolean {
    if (this.data.length < MAGIC.length + 1 + 8) {
      return false;
    }
    const magic = textDecoder.decode(this.data.subarray(0, MAGIC.length));
    if (magic !== MAGIC) {
      throw new Error("scrt: invalid stream header");
    }
    const version = this.data[MAGIC.length]!;
    if (version !== VERSION) {
      throw new Error(`scrt: unsupported version ${version}`);
    }
    const fpBytes = this.data.subarray(MAGIC.length + 1, MAGIC.length + 9);
    let fp = 0n;
    for (let i = 7; i >= 0; i -= 1) {
      fp = (fp << 8n) | BigInt(fpBytes[i]!);
    }
    if (fp !== this.schema.fingerprint()) {
      throw new Error("scrt: schema fingerprint mismatch");
    }
    this.offset = MAGIC.length + 9;
    this.headerRead = true;
    return true;
  }

  private loadPage(): boolean {
    if (this.offset >= this.data.length) {
      return false;
    }
    const { value: length, bytesRead } = readUvarint(this.data, this.offset);
    this.offset += bytesRead;
    const pageLength = Number(length);
    if (pageLength === 0 || this.offset + pageLength > this.data.length) {
      return false;
    }
    const raw = this.data.subarray(this.offset, this.offset + pageLength);
    this.offset += pageLength;
    this.decodePage(raw);
    return true;
  }

  private decodePage(raw: Uint8Array): void {
    let cursor = 0;
    const { value: rows, bytesRead: rowsRead } = readUvarint(raw, cursor);
    cursor += rowsRead;
    const rowCount = Number(rows);
    const { value: columns, bytesRead: columnsRead } = readUvarint(raw, cursor);
    cursor += columnsRead;
    const columnCount = Number(columns);
    if (columnCount !== this.schema.fields.length) {
      throw new Error("scrt: column count mismatch");
    }
    this.state.rows = rowCount;
    this.state.cursor = 0;
    for (let i = 0; i < columnCount; i += 1) {
      const { value: fieldIdxBig, bytesRead: fieldIdxRead } = readUvarint(raw, cursor);
      cursor += fieldIdxRead;
      const fieldIdx = Number(fieldIdxBig);
      const kind = raw[cursor]! as FieldKind;
      cursor += 1;
      const { value: payloadLen, bytesRead: payloadLenRead } = readUvarint(raw, cursor);
      cursor += payloadLenRead;
      const payload = raw.subarray(cursor, cursor + Number(payloadLen));
      cursor += Number(payloadLen);
      this.decodeColumn(fieldIdx, kind, payload, rowCount);
    }
  }

  private decodeColumn(idx: number, kind: FieldKind, payload: Uint8Array, rows: number): void {
    const column = this.state.columns[idx]!;
    column.kind = kind;
    const presence = decodePresence(payload, rows);
    column.rowIndexes = presence.indexes;
    const buffer = payload.subarray(presence.bytesRead);
    switch (kind) {
      case FieldKind.Uint64:
      case FieldKind.Ref: {
        const decoded = decodeUintColumn(buffer, presence.setCount);
        column.uints = decoded.values;
        break;
      }
      case FieldKind.String:
      case FieldKind.TimestampTZ: {
        const decoded = decodeStringColumn(buffer, presence.setCount);
        column.stringTable = decoded.table;
        column.stringIndexes = decoded.indexes;
        break;
      }
      case FieldKind.Bool: {
        const decoded = decodeBoolColumn(buffer, presence.setCount);
        column.bools = decoded.values;
        break;
      }
      case FieldKind.Int64:
      case FieldKind.Date:
      case FieldKind.DateTime:
      case FieldKind.Timestamp:
      case FieldKind.Duration: {
        const decoded = decodeIntColumn(buffer, presence.setCount);
        column.ints = decoded.values;
        break;
      }
      case FieldKind.Float64: {
        const decoded = decodeFloatColumn(buffer, presence.setCount);
        column.floats = decoded.values;
        break;
      }
      case FieldKind.Bytes: {
        const decoded = decodeBytesColumn(buffer, presence.setCount, this.options.zeroCopyBytes ?? false);
        column.bytes = decoded.values;
        break;
      }
      default:
        throw new Error(`scrt: unsupported field kind ${kind}`);
    }
  }
}

function decodePresence(data: Uint8Array, rows: number): { indexes: number[]; setCount: number; bytesRead: number } {
  const { value: byteLenBig, bytesRead } = readUvarint(data, 0);
  const byteLen = Number(byteLenBig);
  let cursor = bytesRead;
  const indexes = new Array(rows).fill(-1);
  let setCount = 0;
  for (let row = 0; row < rows; row += 1) {
    const byteIdx = Math.floor(row / 8);
    const bit = row % 8;
    if (byteIdx < byteLen) {
      const present = (data[cursor + byteIdx]! & (1 << bit)) !== 0;
      if (present) {
        indexes[row] = setCount;
        setCount += 1;
      }
    }
  }
  cursor += byteLen;
  return { indexes, setCount, bytesRead: cursor };
}

function decodeUintColumn(data: Uint8Array, expected: number): { values: bigint[] } {
  let cursor = 0;
  const { value: header, bytesRead } = readUvarint(data, cursor);
  cursor += bytesRead;
  const mode = header & 1n;
  const count = Number(header >> 1n);
  if (count !== expected) {
    throw new Error("scrt: uint column count mismatch");
  }
  const values = new Array<bigint>(count).fill(0n);
  if (count === 0) {
    return { values };
  }
  if (mode === 0n) {
    for (let i = 0; i < count; i += 1) {
      const result = readUvarint(data, cursor);
      cursor += result.bytesRead;
      values[i] = result.value;
    }
  } else {
    let result = readUvarint(data, cursor);
    cursor += result.bytesRead;
    values[0] = result.value;
    for (let i = 1; i < count; i += 1) {
      result = readUvarint(data, cursor);
      cursor += result.bytesRead;
      values[i] = values[i - 1]! + result.value;
    }
  }
  return { values };
}

function decodeIntColumn(data: Uint8Array, expected: number): { values: bigint[] } {
  let cursor = 0;
  const { value: header, bytesRead } = readUvarint(data, cursor);
  cursor += bytesRead;
  const mode = header & 1n;
  const count = Number(header >> 1n);
  if (count !== expected) {
    throw new Error("scrt: int column count mismatch");
  }
  const values = new Array<bigint>(count).fill(0n);
  if (count === 0) {
    return { values };
  }
  if (mode === 0n) {
    for (let i = 0; i < count; i += 1) {
      const result = readVarint(data, cursor);
      cursor += result.bytesRead;
      values[i] = result.value;
    }
  } else {
    let result = readVarint(data, cursor);
    cursor += result.bytesRead;
    values[0] = result.value;
    for (let i = 1; i < count; i += 1) {
      result = readVarint(data, cursor);
      cursor += result.bytesRead;
      values[i] = values[i - 1]! + result.value;
    }
  }
  return { values };
}

function decodeFloatColumn(data: Uint8Array, expected: number): { values: number[] } {
  let cursor = 0;
  const { value: countBig, bytesRead } = readUvarint(data, cursor);
  cursor += bytesRead;
  const count = Number(countBig);
  if (count !== expected) {
    throw new Error("scrt: float column count mismatch");
  }
  const values = new Array<number>(count).fill(0);
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  for (let i = 0; i < count; i += 1) {
    if (cursor + 8 > data.length) {
      throw new Error("scrt: float column truncated");
    }
    values[i] = view.getFloat64(cursor, true);
    cursor += 8;
  }
  return { values };
}

function decodeBoolColumn(data: Uint8Array, expected: number): { values: boolean[] } {
  let cursor = 0;
  const { value: countBig, bytesRead } = readUvarint(data, cursor);
  cursor += bytesRead;
  const count = Number(countBig);
  if (count !== expected) {
    throw new Error("scrt: bool column count mismatch");
  }
  const values = new Array<boolean>(count);
  for (let i = 0; i < count; i += 1) {
    values[i] = data[cursor + i]! !== 0;
  }
  return { values };
}

function decodeStringColumn(data: Uint8Array, expected: number): { table: string[]; indexes: number[] } {
  let cursor = 0;
  const { value: dictLenBig, bytesRead } = readUvarint(data, cursor);
  cursor += bytesRead;
  const dictLen = Number(dictLenBig);
  const table = new Array<string>(dictLen);
  for (let i = 0; i < dictLen; i += 1) {
    const lengthInfo = readUvarint(data, cursor);
    cursor += lengthInfo.bytesRead;
    const length = Number(lengthInfo.value);
    const slice = data.subarray(cursor, cursor + length);
    table[i] = textDecoder.decode(slice);
    cursor += length;
  }
  const { value: indexLenBig, bytesRead: indexRead } = readUvarint(data, cursor);
  cursor += indexRead;
  const indexLen = Number(indexLenBig);
  if (indexLen !== expected) {
    throw new Error("scrt: string index count mismatch");
  }
  const indexes = new Array<number>(indexLen);
  for (let i = 0; i < indexLen; i += 1) {
    const idxInfo = readUvarint(data, cursor);
    cursor += idxInfo.bytesRead;
    indexes[i] = Number(idxInfo.value);
  }
  return { table, indexes };
}

function decodeBytesColumn(data: Uint8Array, expected: number, zeroCopy: boolean): { values: Uint8Array[] } {
  let cursor = 0;
  const { value: countBig, bytesRead } = readUvarint(data, cursor);
  cursor += bytesRead;
  const count = Number(countBig);
  if (count !== expected) {
    throw new Error("scrt: bytes column count mismatch");
  }
  const values = new Array<Uint8Array>(count);
  for (let i = 0; i < count; i += 1) {
    const lengthInfo = readUvarint(data, cursor);
    cursor += lengthInfo.bytesRead;
    const length = Number(lengthInfo.value);
    const slice = data.subarray(cursor, cursor + length);
    values[i] = zeroCopy ? slice : cloneBytes(slice);
    cursor += length;
  }
  return { values };
}

function cloneBytes(src: Uint8Array): Uint8Array {
  const copy = new Uint8Array(src.length);
  copy.set(src);
  return copy;
}

function assignDefaultValue(field: Field, slot: CodecValue): void {
  slot.set = false;
  const def = field.defaultValue;
  if (!def) {
    return;
  }
  slot.set = true;
  switch (def.kind) {
    case FieldKind.Uint64:
    case FieldKind.Ref:
      slot.uint = def.uintValue ?? 0n;
      break;
    case FieldKind.Int64:
    case FieldKind.Date:
    case FieldKind.DateTime:
    case FieldKind.Timestamp:
    case FieldKind.Duration:
      slot.int = def.intValue ?? 0n;
      break;
    case FieldKind.Float64:
      slot.float = def.floatValue ?? 0;
      break;
    case FieldKind.Bool:
      slot.bool = def.boolValue ?? false;
      break;
    case FieldKind.String:
    case FieldKind.TimestampTZ:
      slot.str = def.stringValue ?? "";
      break;
    case FieldKind.Bytes:
      slot.bytes = def.bytesValue ? cloneBytes(def.bytesValue) : new Uint8Array();
      break;
    default:
      slot.set = false;
  }
}
