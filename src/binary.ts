export type ByteBuffer = number[];

export function createBuffer(): ByteBuffer {
  return [];
}

export function bufferLength(buf: ByteBuffer): number {
  return buf.length;
}

export function pushByte(buf: ByteBuffer, byte: number): void {
  buf.push(byte & 0xff);
}

export function pushBytes(buf: ByteBuffer, bytes: ArrayLike<number>): void {
  for (let i = 0; i < bytes.length; i += 1) {
    buf.push(bytes[i]! & 0xff);
  }
}

export function bufferToUint8Array(buf: ByteBuffer): Uint8Array {
  return Uint8Array.from(buf);
}

export function concatByteBuffers(buffers: ByteBuffer[]): Uint8Array {
  const total = buffers.reduce((acc, cur) => acc + cur.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const buf of buffers) {
    out.set(Uint8Array.from(buf), offset);
    offset += buf.length;
  }
  return out;
}

const MAX_VARINT_BYTES = 10;

export function writeUvarint(buf: ByteBuffer, value: bigint | number): void {
  let v = BigInt(value);
  if (v < 0n) {
    throw new RangeError("uvarint cannot be negative");
  }
  while (v >= 0x80n) {
    pushByte(buf, Number((v & 0x7fn) | 0x80n));
    v >>= 7n;
  }
  pushByte(buf, Number(v));
}

export function writeVarint(buf: ByteBuffer, value: bigint | number): void {
  let uv = zigZagEncode(BigInt(value));
  while (uv >= 0x80n) {
    pushByte(buf, Number((uv & 0x7fn) | 0x80n));
    uv >>= 7n;
  }
  pushByte(buf, Number(uv));
}

export function readUvarint(data: Uint8Array, offset: number): { value: bigint; bytesRead: number } {
  let x = 0n;
  let s = 0n;
  for (let i = 0; i < MAX_VARINT_BYTES; i += 1) {
    if (offset + i >= data.length) {
      throw new RangeError("uvarint exceeds buffer");
    }
    const b = BigInt(data[offset + i]!);
    if ((b & 0x80n) === 0n) {
      x |= (b & 0x7fn) << s;
      return { value: x, bytesRead: i + 1 };
    }
    x |= (b & 0x7fn) << s;
    s += 7n;
  }
  throw new RangeError("uvarint too large");
}

export function readVarint(data: Uint8Array, offset: number): { value: bigint; bytesRead: number } {
  const { value, bytesRead } = readUvarint(data, offset);
  return { value: zigZagDecode(value), bytesRead };
}

function zigZagEncode(value: bigint): bigint {
  return (value << 1n) ^ (value >> 63n);
}

function zigZagDecode(value: bigint): bigint {
  return (value >> 1n) ^ -(value & 1n);
}

export class BinaryWriter {
  private readonly chunks: ByteBuffer[] = [];
  private current: ByteBuffer = createBuffer();

  writeByte(byte: number): void {
    pushByte(this.current, byte);
  }

  writeBytes(bytes: ArrayLike<number>): void {
    pushBytes(this.current, bytes);
  }

  writeBuffer(buffer: ByteBuffer): void {
    this.flushCurrent();
    this.chunks.push(buffer.slice());
    this.current = createBuffer();
  }

  writeUint8Array(arr: Uint8Array): void {
    this.flushCurrent();
    this.chunks.push(Array.from(arr));
    this.current = createBuffer();
  }

  writeUvarint(value: bigint | number): void {
    writeUvarint(this.current, value);
  }

  toUint8Array(): Uint8Array {
    this.flushCurrent();
    return concatByteBuffers(this.chunks);
  }

  reset(): void {
    this.chunks.length = 0;
    this.current = createBuffer();
  }

  private flushCurrent(): void {
    if (this.current.length > 0) {
      this.chunks.push(this.current);
      this.current = createBuffer();
    }
  }
}

export class BinaryReader {
  constructor(private readonly data: Uint8Array, public offset = 0) {}

  ensure(size: number): void {
    if (this.offset + size > this.data.length) {
      throw new RangeError("buffer underrun");
    }
  }

  readByte(): number {
    this.ensure(1);
    return this.data[this.offset++]!;
  }

  readBytes(length: number): Uint8Array {
    this.ensure(length);
    const slice = this.data.subarray(this.offset, this.offset + length);
    this.offset += length;
    return slice;
  }

  readUvarint(): bigint {
    const { value, bytesRead } = readUvarint(this.data, this.offset);
    this.offset += bytesRead;
    return value;
  }

  readVarint(): bigint {
    const { value, bytesRead } = readVarint(this.data, this.offset);
    this.offset += bytesRead;
    return value;
  }

  remaining(): number {
    return this.data.length - this.offset;
  }
}

type NumberLike = number | bigint;

export function toSafeNumber(value: NumberLike, label: string): number {
  const v = Number(value);
  if (!Number.isFinite(v) || v > Number.MAX_SAFE_INTEGER) {
    throw new RangeError(`${label} exceeds safe number range`);
  }
  return v;
}

export function encodeUint64LE(value: bigint | number): Uint8Array {
  const v = BigInt(value);
  const out = new Uint8Array(8);
  let temp = v;
  for (let i = 0; i < 8; i += 1) {
    out[i] = Number(temp & 0xffn);
    temp >>= 8n;
  }
  return out;
}
