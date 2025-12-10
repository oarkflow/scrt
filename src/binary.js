export function createBuffer() {
    return [];
}
export function bufferLength(buf) {
    return buf.length;
}
export function pushByte(buf, byte) {
    buf.push(byte & 0xff);
}
export function pushBytes(buf, bytes) {
    for (let i = 0; i < bytes.length; i += 1) {
        buf.push(bytes[i] & 0xff);
    }
}
export function bufferToUint8Array(buf) {
    return Uint8Array.from(buf);
}
export function concatByteBuffers(buffers) {
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
export function writeUvarint(buf, value) {
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
export function writeVarint(buf, value) {
    let uv = zigZagEncode(BigInt(value));
    while (uv >= 0x80n) {
        pushByte(buf, Number((uv & 0x7fn) | 0x80n));
        uv >>= 7n;
    }
    pushByte(buf, Number(uv));
}
export function readUvarint(data, offset) {
    let x = 0n;
    let s = 0n;
    for (let i = 0; i < MAX_VARINT_BYTES; i += 1) {
        if (offset + i >= data.length) {
            throw new RangeError("uvarint exceeds buffer");
        }
        const b = BigInt(data[offset + i]);
        if ((b & 0x80n) === 0n) {
            x |= (b & 0x7fn) << s;
            return { value: x, bytesRead: i + 1 };
        }
        x |= (b & 0x7fn) << s;
        s += 7n;
    }
    throw new RangeError("uvarint too large");
}
export function readVarint(data, offset) {
    const { value, bytesRead } = readUvarint(data, offset);
    return { value: zigZagDecode(value), bytesRead };
}
function zigZagEncode(value) {
    return (value << 1n) ^ (value >> 63n);
}
function zigZagDecode(value) {
    return (value >> 1n) ^ -(value & 1n);
}
export class BinaryWriter {
    chunks = [];
    current = createBuffer();
    writeByte(byte) {
        pushByte(this.current, byte);
    }
    writeBytes(bytes) {
        pushBytes(this.current, bytes);
    }
    writeBuffer(buffer) {
        this.flushCurrent();
        this.chunks.push(buffer.slice());
        this.current = createBuffer();
    }
    writeUint8Array(arr) {
        this.flushCurrent();
        this.chunks.push(Array.from(arr));
        this.current = createBuffer();
    }
    writeUvarint(value) {
        writeUvarint(this.current, value);
    }
    toUint8Array() {
        this.flushCurrent();
        return concatByteBuffers(this.chunks);
    }
    reset() {
        this.chunks.length = 0;
        this.current = createBuffer();
    }
    flushCurrent() {
        if (this.current.length > 0) {
            this.chunks.push(this.current);
            this.current = createBuffer();
        }
    }
}
export class BinaryReader {
    data;
    offset;
    constructor(data, offset = 0) {
        this.data = data;
        this.offset = offset;
    }
    ensure(size) {
        if (this.offset + size > this.data.length) {
            throw new RangeError("buffer underrun");
        }
    }
    readByte() {
        this.ensure(1);
        return this.data[this.offset++];
    }
    readBytes(length) {
        this.ensure(length);
        const slice = this.data.subarray(this.offset, this.offset + length);
        this.offset += length;
        return slice;
    }
    readUvarint() {
        const { value, bytesRead } = readUvarint(this.data, this.offset);
        this.offset += bytesRead;
        return value;
    }
    readVarint() {
        const { value, bytesRead } = readVarint(this.data, this.offset);
        this.offset += bytesRead;
        return value;
    }
    remaining() {
        return this.data.length - this.offset;
    }
}
export function toSafeNumber(value, label) {
    const v = Number(value);
    if (!Number.isFinite(v) || v > Number.MAX_SAFE_INTEGER) {
        throw new RangeError(`${label} exceeds safe number range`);
    }
    return v;
}
export function encodeUint64LE(value) {
    const v = BigInt(value);
    const out = new Uint8Array(8);
    let temp = v;
    for (let i = 0; i < 8; i += 1) {
        out[i] = Number(temp & 0xffn);
        temp >>= 8n;
    }
    return out;
}
