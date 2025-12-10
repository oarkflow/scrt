import { pushByte, pushBytes, writeUvarint, writeVarint } from "./binary";
const encoder = new TextEncoder();
export class Uint64Column {
    values = [];
    append(value) {
        this.values.push(BigInt(value));
    }
    encode(dst) {
        const count = this.values.length;
        const mode = count >= 2 && isMonotonic(this.values) ? 1n : 0n;
        const header = (BigInt(count) << 1n) | mode;
        writeUvarint(dst, header);
        if (count === 0) {
            return;
        }
        if (mode === 0n) {
            for (const value of this.values) {
                writeUvarint(dst, value);
            }
            return;
        }
        writeUvarint(dst, this.values[0]);
        let prev = this.values[0];
        for (let i = 1; i < count; i += 1) {
            const delta = this.values[i] - prev;
            writeUvarint(dst, delta);
            prev = this.values[i];
        }
    }
    reset() {
        this.values.length = 0;
    }
}
export class Int64Column {
    values = [];
    append(value) {
        this.values.push(BigInt(value));
    }
    encode(dst) {
        const count = this.values.length;
        const mode = count > 1 ? 1n : 0n;
        const header = (BigInt(count) << 1n) | mode;
        writeUvarint(dst, header);
        if (count === 0) {
            return;
        }
        if (mode === 0n) {
            for (const value of this.values) {
                writeVarint(dst, value);
            }
            return;
        }
        writeVarint(dst, this.values[0]);
        let prev = this.values[0];
        for (let i = 1; i < count; i += 1) {
            const delta = this.values[i] - prev;
            writeVarint(dst, delta);
            prev = this.values[i];
        }
    }
    reset() {
        this.values.length = 0;
    }
}
export class Float64Column {
    values = [];
    append(value) {
        this.values.push(value);
    }
    encode(dst) {
        writeUvarint(dst, this.values.length);
        for (const value of this.values) {
            writeFloat64(dst, value);
        }
    }
    reset() {
        this.values.length = 0;
    }
}
export class BoolColumn {
    values = [];
    append(value) {
        this.values.push(value ? 1 : 0);
    }
    encode(dst) {
        writeUvarint(dst, this.values.length);
        for (const value of this.values) {
            pushByte(dst, value);
        }
    }
    reset() {
        this.values.length = 0;
    }
}
export class StringColumn {
    dict = new Map();
    entries = [];
    indexes = [];
    append(value) {
        if (!this.dict.has(value)) {
            const bytes = encoder.encode(value);
            this.dict.set(value, this.entries.length);
            this.entries.push(bytes);
        }
        this.indexes.push(this.dict.get(value));
    }
    encode(dst) {
        writeUvarint(dst, this.entries.length);
        for (const entry of this.entries) {
            writeUvarint(dst, entry.length);
            pushBytes(dst, entry);
        }
        writeUvarint(dst, this.indexes.length);
        for (const idx of this.indexes) {
            writeUvarint(dst, BigInt(idx));
        }
    }
    reset() {
        this.dict.clear();
        this.entries.length = 0;
        this.indexes.length = 0;
    }
}
export class BytesColumn {
    values = [];
    append(value) {
        const copy = new Uint8Array(value.length);
        copy.set(value);
        this.values.push(copy);
    }
    encode(dst) {
        writeUvarint(dst, this.values.length);
        for (const value of this.values) {
            writeUvarint(dst, value.length);
            pushBytes(dst, value);
        }
    }
    reset() {
        this.values.length = 0;
    }
}
function isMonotonic(values) {
    for (let i = 1; i < values.length; i += 1) {
        if (values[i] < values[i - 1]) {
            return false;
        }
    }
    return true;
}
function writeFloat64(dst, value) {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setFloat64(0, value, true);
    for (let i = 0; i < 8; i += 1) {
        pushByte(dst, view.getUint8(i));
    }
}
