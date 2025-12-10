import { ByteBuffer, createBuffer, pushByte, pushBytes, writeUvarint } from "./binary";
import { BoolColumn, BytesColumn, Float64Column, Int64Column, StringColumn, Uint64Column } from "./column";
import { FieldKind, Schema } from "./schema";

interface ColumnHandle {
  kind: FieldKind;
  uints?: Uint64Column;
  strings?: StringColumn;
  bools?: BoolColumn;
  ints?: Int64Column;
  floats?: Float64Column;
  bytes?: BytesColumn;
  presence: boolean[];
}

export class PageBuilder {
  private readonly columns: ColumnHandle[];
  private readonly columnBuf: ByteBuffer = createBuffer();
  private rows = 0;

  constructor(private readonly schema: Schema, private readonly rowLimit: number = 1024) {
    this.columns = schema.fields.map((field) => {
      const kind = field.valueKind();
      const handle: ColumnHandle = { kind, presence: [] };
      switch (kind) {
        case FieldKind.Uint64:
        case FieldKind.Ref:
          handle.uints = new Uint64Column();
          break;
        case FieldKind.String:
        case FieldKind.TimestampTZ:
          handle.strings = new StringColumn();
          break;
        case FieldKind.Bool:
          handle.bools = new BoolColumn();
          break;
        case FieldKind.Int64:
        case FieldKind.Date:
        case FieldKind.DateTime:
        case FieldKind.Timestamp:
        case FieldKind.Duration:
          handle.ints = new Int64Column();
          break;
        case FieldKind.Float64:
          handle.floats = new Float64Column();
          break;
        case FieldKind.Bytes:
          handle.bytes = new BytesColumn();
          break;
        default:
          throw new Error(`scrt: unsupported field kind ${kind}`);
      }
      return handle;
    });
  }

  appendUint(idx: number, value: bigint | number): void {
    this.columns[idx]!.uints?.append(value);
  }

  appendString(idx: number, value: string): void {
    this.columns[idx]!.strings?.append(value);
  }

  appendBool(idx: number, value: boolean): void {
    this.columns[idx]!.bools?.append(value);
  }

  appendInt(idx: number, value: bigint | number): void {
    this.columns[idx]!.ints?.append(value);
  }

  appendFloat(idx: number, value: number): void {
    this.columns[idx]!.floats?.append(value);
  }

  appendBytes(idx: number, value: Uint8Array): void {
    this.columns[idx]!.bytes?.append(value);
  }

  recordPresence(idx: number, present: boolean): void {
    this.columns[idx]!.presence.push(present);
  }

  sealRow(): void {
    this.rows += 1;
    if (this.rows > this.rowLimit) {
      throw new Error("scrt: page builder capacity exceeded");
    }
  }

  full(): boolean {
    return this.rows >= this.rowLimit;
  }

  rowCount(): number {
    return this.rows;
  }

  reset(): void {
    this.rows = 0;
    for (const column of this.columns) {
      column.presence.length = 0;
      column.uints?.reset();
      column.strings?.reset();
      column.bools?.reset();
      column.ints?.reset();
      column.floats?.reset();
      column.bytes?.reset();
    }
  }

  encode(dst: ByteBuffer): void {
    if (this.rows === 0) {
      return;
    }
    writeUvarint(dst, this.rows);
    writeUvarint(dst, this.columns.length);
    for (let idx = 0; idx < this.columns.length; idx += 1) {
      const column = this.columns[idx]!;
      this.columnBuf.length = 0;
      writePresence(this.columnBuf, column.presence, this.rows);
      switch (column.kind) {
        case FieldKind.Uint64:
        case FieldKind.Ref:
          column.uints?.encode(this.columnBuf);
          break;
        case FieldKind.String:
        case FieldKind.TimestampTZ:
          column.strings?.encode(this.columnBuf);
          break;
        case FieldKind.Bool:
          column.bools?.encode(this.columnBuf);
          break;
        case FieldKind.Int64:
        case FieldKind.Date:
        case FieldKind.DateTime:
        case FieldKind.Timestamp:
        case FieldKind.Duration:
          column.ints?.encode(this.columnBuf);
          break;
        case FieldKind.Float64:
          column.floats?.encode(this.columnBuf);
          break;
        case FieldKind.Bytes:
          column.bytes?.encode(this.columnBuf);
          break;
        default:
          throw new Error(`scrt: unsupported field kind ${column.kind}`);
      }
      writeUvarint(dst, idx);
      pushByte(dst, column.kind);
      writeUvarint(dst, this.columnBuf.length);
      pushBytes(dst, this.columnBuf);
    }
  }
}

function writePresence(dst: ByteBuffer, presence: boolean[], rows: number): void {
  const byteLen = Math.floor((rows + 7) / 8);
  writeUvarint(dst, byteLen);
  if (byteLen === 0) {
    return;
  }
  let current = 0;
  let shift = 0;
  let written = 0;
  for (let row = 0; row < rows; row += 1) {
    if (presence[row]) {
      current |= 1 << shift;
    }
    shift += 1;
    if (shift === 8) {
      pushByte(dst, current);
      written += 1;
      current = 0;
      shift = 0;
    }
  }
  if (shift !== 0) {
    pushByte(dst, current);
    written += 1;
  }
  while (written < byteLen) {
    pushByte(dst, 0);
    written += 1;
  }
}
