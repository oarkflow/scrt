var le = Object.defineProperty;
var de = (t, e, n) => e in t ? le(t, e, { enumerable: !0, configurable: !0, writable: !0, value: n }) : t[e] = n;
var f = (t, e, n) => de(t, typeof e != "symbol" ? e + "" : e, n);
function I() {
  return [];
}
function Wt(t) {
  return t.length;
}
function y(t, e) {
  t.push(e & 255);
}
function E(t, e) {
  for (let n = 0; n < e.length; n += 1)
    t.push(e[n] & 255);
}
function he(t) {
  return Uint8Array.from(t);
}
function me(t) {
  const e = t.reduce((s, o) => s + o.length, 0), n = new Uint8Array(e);
  let r = 0;
  for (const s of t)
    n.set(Uint8Array.from(s), r), r += s.length;
  return n;
}
const ge = 10;
function g(t, e) {
  let n = BigInt(e);
  if (n < 0n)
    throw new RangeError("uvarint cannot be negative");
  for (; n >= 0x80n; )
    y(t, Number(n & 0x7fn | 0x80n)), n >>= 7n;
  y(t, Number(n));
}
function C(t, e) {
  let n = we(BigInt(e));
  for (; n >= 0x80n; )
    y(t, Number(n & 0x7fn | 0x80n)), n >>= 7n;
  y(t, Number(n));
}
function m(t, e) {
  let n = 0n, r = 0n;
  for (let s = 0; s < ge; s += 1) {
    if (e + s >= t.length)
      throw new RangeError("uvarint exceeds buffer");
    const o = BigInt(t[e + s]);
    if ((o & 0x80n) === 0n)
      return n |= (o & 0x7fn) << r, { value: n, bytesRead: s + 1 };
    n |= (o & 0x7fn) << r, r += 7n;
  }
  throw new RangeError("uvarint too large");
}
function R(t, e) {
  const { value: n, bytesRead: r } = m(t, e);
  return { value: pe(n), bytesRead: r };
}
function we(t) {
  return t << 1n ^ t >> 63n;
}
function pe(t) {
  return t >> 1n ^ -(t & 1n);
}
class _t {
  constructor() {
    f(this, "chunks", []);
    f(this, "current", I());
  }
  writeByte(e) {
    y(this.current, e);
  }
  writeBytes(e) {
    E(this.current, e);
  }
  writeBuffer(e) {
    this.flushCurrent(), this.chunks.push(e.slice()), this.current = I();
  }
  writeUint8Array(e) {
    this.flushCurrent(), this.chunks.push(Array.from(e)), this.current = I();
  }
  writeUvarint(e) {
    g(this.current, e);
  }
  toUint8Array() {
    return this.flushCurrent(), me(this.chunks);
  }
  reset() {
    this.chunks.length = 0, this.current = I();
  }
  flushCurrent() {
    this.current.length > 0 && (this.chunks.push(this.current), this.current = I());
  }
}
class zt {
  constructor(e, n = 0) {
    f(this, "data");
    f(this, "offset");
    this.data = e, this.offset = n;
  }
  ensure(e) {
    if (this.offset + e > this.data.length)
      throw new RangeError("buffer underrun");
  }
  readByte() {
    return this.ensure(1), this.data[this.offset++];
  }
  readBytes(e) {
    this.ensure(e);
    const n = this.data.subarray(this.offset, this.offset + e);
    return this.offset += e, n;
  }
  readUvarint() {
    const { value: e, bytesRead: n } = m(this.data, this.offset);
    return this.offset += n, e;
  }
  readVarint() {
    const { value: e, bytesRead: n } = R(this.data, this.offset);
    return this.offset += n, e;
  }
  remaining() {
    return this.data.length - this.offset;
  }
}
function Pt(t, e) {
  const n = Number(t);
  if (!Number.isFinite(n) || n > Number.MAX_SAFE_INTEGER)
    throw new RangeError(`${e} exceeds safe number range`);
  return n;
}
function Zt(t) {
  const e = BigInt(t), n = new Uint8Array(8);
  let r = e;
  for (let s = 0; s < 8; s += 1)
    n[s] = Number(r & 0xffn), r >>= 8n;
  return n;
}
const be = new TextEncoder();
class ye {
  constructor() {
    f(this, "values", []);
  }
  append(e) {
    this.values.push(BigInt(e));
  }
  encode(e) {
    const n = this.values.length, r = n >= 2 && De(this.values) ? 1n : 0n, s = BigInt(n) << 1n | r;
    if (g(e, s), n === 0)
      return;
    if (r === 0n) {
      for (const a of this.values)
        g(e, a);
      return;
    }
    g(e, this.values[0]);
    let o = this.values[0];
    for (let a = 1; a < n; a += 1) {
      const c = this.values[a] - o;
      g(e, c), o = this.values[a];
    }
  }
  reset() {
    this.values.length = 0;
  }
}
class Ie {
  constructor() {
    f(this, "values", []);
  }
  append(e) {
    this.values.push(BigInt(e));
  }
  encode(e) {
    const n = this.values.length, r = n > 1 ? 1n : 0n, s = BigInt(n) << 1n | r;
    if (g(e, s), n === 0)
      return;
    if (r === 0n) {
      for (const a of this.values)
        C(e, a);
      return;
    }
    C(e, this.values[0]);
    let o = this.values[0];
    for (let a = 1; a < n; a += 1) {
      const c = this.values[a] - o;
      C(e, c), o = this.values[a];
    }
  }
  reset() {
    this.values.length = 0;
  }
}
class Be {
  constructor() {
    f(this, "values", []);
  }
  append(e) {
    this.values.push(e);
  }
  encode(e) {
    g(e, this.values.length);
    for (const n of this.values)
      Ne(e, n);
  }
  reset() {
    this.values.length = 0;
  }
}
class Te {
  constructor() {
    f(this, "values", []);
  }
  append(e) {
    this.values.push(e ? 1 : 0);
  }
  encode(e) {
    g(e, this.values.length);
    for (const n of this.values)
      y(e, n);
  }
  reset() {
    this.values.length = 0;
  }
}
class Ee {
  constructor() {
    f(this, "dict", /* @__PURE__ */ new Map());
    f(this, "entries", []);
    f(this, "indexes", []);
  }
  append(e) {
    if (!this.dict.has(e)) {
      const n = be.encode(e);
      this.dict.set(e, this.entries.length), this.entries.push(n);
    }
    this.indexes.push(this.dict.get(e));
  }
  encode(e) {
    g(e, this.entries.length);
    for (const n of this.entries)
      g(e, n.length), E(e, n);
    g(e, this.indexes.length);
    for (const n of this.indexes)
      g(e, BigInt(n));
  }
  reset() {
    this.dict.clear(), this.entries.length = 0, this.indexes.length = 0;
  }
}
class xe {
  constructor() {
    f(this, "values", []);
  }
  append(e) {
    const n = new Uint8Array(e.length);
    n.set(e), this.values.push(n);
  }
  encode(e) {
    g(e, this.values.length);
    for (const n of this.values)
      g(e, n.length), E(e, n);
  }
  reset() {
    this.values.length = 0;
  }
}
function De(t) {
  for (let e = 1; e < t.length; e += 1)
    if (t[e] < t[e - 1])
      return !1;
  return !0;
}
function Ne(t, e) {
  const n = new ArrayBuffer(8), r = new DataView(n);
  r.setFloat64(0, e, !0);
  for (let s = 0; s < 8; s += 1)
    y(t, r.getUint8(s));
}
const Se = /^(\d{4})-(\d{2})-(\d{2})$/, Re = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?$/, Y = /^[-+]?\d+(?:\.\d+)?$/, $e = /(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h|d)/gi, Ue = {
  ns: 1n,
  us: 1000n,
  µs: 1000n,
  ms: 1000000n,
  s: 1000000000n,
  m: 60n * 1000000000n,
  h: 60n * 60n * 1000000000n,
  d: 24n * 60n * 60n * 1000000000n
};
function U(t) {
  const e = t.trim(), n = Se.exec(e);
  if (!n) {
    const a = new Date(e);
    if (Number.isNaN(a.getTime()))
      throw new Error(`temporal: invalid date ${t}`);
    return F(a.getUTCFullYear(), a.getUTCMonth(), a.getUTCDate());
  }
  const [, r, s, o] = n;
  return F(Number(r), Number(s) - 1, Number(o));
}
function k(t) {
  const e = t.trim(), n = Re.exec(e);
  if (!n) {
    const w = new Date(e);
    if (Number.isNaN(w.getTime()))
      throw new Error(`temporal: invalid datetime ${t}`);
    return new Date(w.toISOString());
  }
  const [, r, s, o, a, c, u = "0", l = "0"] = n, d = Me(l), h = Date.UTC(Number(r), Number(s) - 1, Number(o), Number(a), Number(c), Number(u));
  return new Date(h + Number(d / 1000000n));
}
function $(t) {
  const e = t.trim();
  if (Y.test(e))
    return K(e);
  const n = new Date(e);
  return Number.isNaN(n.getTime()) ? k(e) : new Date(n.toISOString());
}
function A(t) {
  const e = t.trim();
  if (Y.test(e))
    return K(e);
  const n = new Date(e);
  if (Number.isNaN(n.getTime()))
    throw new Error(`temporal: invalid timestamptz ${t}`);
  return n;
}
function O(t) {
  const e = t.trim();
  if (!e)
    throw new Error("temporal: empty duration");
  let n = 0n, r = !1;
  for (const s of e.matchAll($e)) {
    r = !0;
    const [, o, a] = s, c = a.toLowerCase(), u = Ue[c];
    if (!u)
      throw new Error(`temporal: unsupported duration unit ${a}`);
    const [l, d = "0"] = o.split(".");
    let h = BigInt(l) * u;
    if (d) {
      const w = u / Q(d.length);
      h += BigInt(d) * w;
    }
    n += h;
  }
  if (!r)
    throw new Error(`temporal: malformed duration ${t}`);
  return n;
}
function v(t) {
  return t instanceof Date ? BigInt(t.getTime()) * 1000000n : typeof t == "number" ? BigInt(Math.trunc(t)) * 1000000n : t;
}
function J(t) {
  if (t instanceof Date) {
    const n = F(t.getUTCFullYear(), t.getUTCMonth(), t.getUTCDate());
    return BigInt(n.getTime()) * 1000000n;
  }
  const e = U(t);
  return BigInt(e.getTime()) * 1000000n;
}
function x(t) {
  return t.toISOString();
}
function ke(t) {
  if (!t.trim())
    return "";
  const e = A(t);
  return x(e);
}
function D(t) {
  const e = BigInt(t);
  if (e === 0n)
    return /* @__PURE__ */ new Date(0);
  const n = Number(e / 1000000n);
  return new Date(n);
}
function Ae(t) {
  return D(t);
}
function Ce(t) {
  return Number.isFinite(t.getTime()) ? t.toISOString().slice(0, 10) : "";
}
function ve(t) {
  return Number.isFinite(t.getTime()) ? t.toISOString() : "";
}
function K(t) {
  if (t.includes(".")) {
    const [r, s] = t.split("."), o = BigInt(r), a = BigInt(Q(s.length)), c = BigInt(s) * 1000000000n / a;
    return new Date(Number(o * 1000n + c / 1000000n));
  }
  const e = BigInt(t), n = W(e);
  return new Date(Number(n / 1000000n));
}
function W(t) {
  const e = t < 0n ? -t : t;
  return e < 100000000000n ? t * 1000000000n : e < 100000000000000n ? t * 1000000n : e < 100000000000000000n ? t * 1000n : t;
}
const Fe = [
  { nanos: 24n * 60n * 60n * 1000000000n, suffix: "d" },
  { nanos: 60n * 60n * 1000000000n, suffix: "h" },
  { nanos: 60n * 1000000000n, suffix: "m" },
  { nanos: 1000000000n, suffix: "s" },
  { nanos: 1000000n, suffix: "ms" },
  { nanos: 1000n, suffix: "µs" },
  { nanos: 1n, suffix: "ns" }
];
function Ve(t) {
  if (t === 0n)
    return "0s";
  const e = t < 0n;
  let n = e ? -t : t;
  const r = [];
  for (const s of Fe) {
    if (n < s.nanos)
      continue;
    const o = n / s.nanos;
    if (n -= o * s.nanos, r.push(`${o}${s.suffix}`), n === 0n)
      break;
  }
  return n > 0n && r.push(`${n}ns`), `${e ? "-" : ""}${r.join("")}`;
}
function F(t, e, n) {
  return new Date(Date.UTC(t, e, n, 0, 0, 0, 0));
}
function Me(t) {
  const e = t.padEnd(9, "0").slice(0, 9);
  return BigInt(e);
}
function Q(t) {
  let e = 1n;
  for (let n = 0; n < t; n += 1)
    e *= 10n;
  return e;
}
const Le = 0xcbf29ce484222325n, Oe = 0x100000001b3n;
var i;
(function(t) {
  t[t.Invalid = 0] = "Invalid", t[t.Uint64 = 1] = "Uint64", t[t.String = 2] = "String", t[t.Ref = 3] = "Ref", t[t.Bool = 4] = "Bool", t[t.Int64 = 5] = "Int64", t[t.Float64 = 6] = "Float64", t[t.Bytes = 7] = "Bytes", t[t.Date = 8] = "Date", t[t.DateTime = 9] = "DateTime", t[t.Timestamp = 10] = "Timestamp", t[t.TimestampTZ = 11] = "TimestampTZ", t[t.Duration = 12] = "Duration";
})(i || (i = {}));
class b {
  constructor(e, n, r, s, o, a, c) {
    f(this, "kind");
    f(this, "boolValue");
    f(this, "intValue");
    f(this, "uintValue");
    f(this, "floatValue");
    f(this, "stringValue");
    f(this, "bytesValue");
    this.kind = e, this.boolValue = n, this.intValue = r, this.uintValue = s, this.floatValue = o, this.stringValue = a, this.bytesValue = c;
  }
  hashKey() {
    switch (this.kind) {
      case i.Bool:
        return `bool:${this.boolValue ? 1 : 0}`;
      case i.Int64:
        return `int:${this.intValue ?? 0n}`;
      case i.Uint64:
      case i.Ref:
        return `uint:${this.uintValue ?? 0n}`;
      case i.Float64:
        return `float:${this.floatValue ?? 0}`;
      case i.String:
        return `string:${this.stringValue ?? ""}`;
      case i.Bytes:
        return `bytes:${tt(this.bytesValue ?? new Uint8Array())}`;
      case i.Date:
      case i.DateTime:
      case i.Timestamp:
      case i.Duration:
        return `int:${this.intValue ?? 0n}`;
      case i.TimestampTZ:
        return `timestamptz:${this.stringValue ?? ""}`;
      default:
        return "";
    }
  }
}
class We {
  constructor(e, n, r, s = "", o = "", a = !1, c = [], u) {
    f(this, "name");
    f(this, "kind");
    f(this, "rawType");
    f(this, "targetSchema");
    f(this, "targetField");
    f(this, "autoIncrement");
    f(this, "attributes");
    f(this, "defaultValue");
    f(this, "resolvedKind", i.Invalid);
    f(this, "pendingDefault", "");
    this.name = e, this.kind = n, this.rawType = r, this.targetSchema = s, this.targetField = o, this.autoIncrement = a, this.attributes = c, this.defaultValue = u;
  }
  valueKind() {
    return this.kind === i.Ref ? this.resolvedKind === i.Invalid ? i.Uint64 : this.resolvedKind : this.resolvedKind === i.Invalid ? this.kind : this.resolvedKind;
  }
  isReference() {
    return this.kind === i.Ref && !!this.targetSchema && !!this.targetField;
  }
}
class _e {
  constructor(e, n) {
    f(this, "name");
    f(this, "fields");
    f(this, "fingerprintCache");
    f(this, "fieldIndex");
    this.name = e, this.fields = n;
  }
  fingerprint() {
    if (this.fingerprintCache !== void 0)
      return this.fingerprintCache;
    let e = Le;
    const n = (r) => {
      for (let s = 0; s < r.length; s += 1)
        e ^= BigInt(r.charCodeAt(s)), e = BigInt.asUintN(64, e * Oe);
    };
    n(this.name);
    for (const r of this.fields) {
      if (n("|"), n(r.name), n(":"), n(r.rawType), r.targetSchema && (n("->"), n(`${r.targetSchema}.${r.targetField}`)), r.autoIncrement && n("+auto"), r.attributes.length) {
        const s = [...r.attributes].sort();
        for (const o of s)
          n(`@${o}`);
      }
      r.defaultValue && (n("=def:"), n(r.defaultValue.hashKey()));
    }
    return this.fingerprintCache = BigInt.asUintN(64, e), this.fingerprintCache;
  }
  fieldIndexByName(e) {
    this.fieldIndex || (this.fieldIndex = /* @__PURE__ */ new Map(), this.fields.forEach((r, s) => this.fieldIndex.set(r.name, s)));
    const n = this.fieldIndex.get(e);
    if (n === void 0)
      throw new Error(`scrt: field ${e} not found in schema ${this.name}`);
    return n;
  }
  tryFieldIndex(e) {
    return this.fieldIndex || (this.fieldIndex = /* @__PURE__ */ new Map(), this.fields.forEach((n, r) => this.fieldIndex.set(n.name, r))), this.fieldIndex.get(e);
  }
}
class ze {
  constructor(e, n, r) {
    f(this, "schemas");
    f(this, "data");
    f(this, "source");
    this.schemas = e, this.data = n, this.source = r;
  }
  schema(e) {
    return this.schemas.get(e);
  }
  records(e) {
    return this.data.get(e);
  }
  finalize() {
    for (const e of this.schemas.values())
      et(this, e);
  }
}
function Gt(t) {
  const e = t.split(/\r?\n/).map((d) => d.trim()), n = /* @__PURE__ */ new Map(), r = /* @__PURE__ */ new Map();
  let s, o = !1, a = "";
  const c = () => {
    if (s) {
      if (n.has(s.name))
        throw new Error(`scrt: duplicate schema ${s.name}`);
      n.set(s.name, s), s = void 0;
    }
  }, u = (d) => {
    if (c(), !d)
      throw new Error("scrt: schema name cannot be empty");
    s = new _e(d, []);
  };
  for (const d of e)
    if (d) {
      if (o) {
        u(d), o = !1;
        continue;
      }
      if (d.startsWith("@schema")) {
        a = "";
        let h = d.slice(7).trim();
        if (h.startsWith(":") && (h = h.slice(1).trim()), !h) {
          o = !0;
          continue;
        }
        u(h);
        continue;
      }
      if (d.startsWith("@field")) {
        if (a = "", !s)
          throw new Error("scrt: @field outside schema block");
        const h = Pe(d.slice(6).trim());
        s.fields.push(h);
        continue;
      }
      if (d.startsWith("@")) {
        if (o = !1, c(), d.includes("=") && a) {
          const h = n.get(a);
          if (h) {
            const w = P(d, h);
            z(r, a, w);
          }
          continue;
        }
        a = d.slice(1).trim();
        continue;
      }
      if (a) {
        const h = n.get(a);
        if (!h)
          continue;
        const w = P(d, h);
        z(r, a, w);
        continue;
      }
    }
  c();
  const l = new ze(n, r);
  return l.finalize(), l;
}
function z(t, e, n) {
  t.has(e) || t.set(e, []), t.get(e).push(n);
}
function Pe(t) {
  const [e, n, r] = Ze(t), { kind: s, targetSchema: o, targetField: a } = Ge(n), c = new We(e, s, n, o, a);
  if (r) {
    const u = je(r);
    for (const l of u) {
      const d = l.toLowerCase();
      switch (!0) {
        case (d === "auto_increment" || d === "autoincrement" || d === "serial"):
          c.autoIncrement = !0;
          break;
        case d.startsWith("default="):
        case d.startsWith("default:"):
          Xe(c, He(l));
          break;
      }
      c.attributes.push(d);
    }
  }
  return c;
}
function Ze(t) {
  const e = t.trim(), n = e.search(/[ \t]/);
  if (n === -1)
    throw new Error(`scrt: invalid @field declaration ${t}`);
  const r = e.slice(0, n).trim(), s = e.slice(n + 1).trim(), o = s.search(/[ \t]/);
  return o === -1 ? [r, s, ""] : [r, s.slice(0, o).trim(), s.slice(o + 1).trim()];
}
function Ge(t) {
  const e = t.toLowerCase();
  switch (!0) {
    case e === "uint64":
      return { kind: i.Uint64, targetSchema: "", targetField: "" };
    case e === "string":
      return { kind: i.String, targetSchema: "", targetField: "" };
    case e === "bool":
      return { kind: i.Bool, targetSchema: "", targetField: "" };
    case e === "int64":
      return { kind: i.Int64, targetSchema: "", targetField: "" };
    case e === "float64":
      return { kind: i.Float64, targetSchema: "", targetField: "" };
    case e === "bytes":
      return { kind: i.Bytes, targetSchema: "", targetField: "" };
    case e === "date":
      return { kind: i.Date, targetSchema: "", targetField: "" };
    case e === "datetime":
      return { kind: i.DateTime, targetSchema: "", targetField: "" };
    case e === "timestamp":
      return { kind: i.Timestamp, targetSchema: "", targetField: "" };
    case e === "timestamptz":
      return { kind: i.TimestampTZ, targetSchema: "", targetField: "" };
    case e === "duration":
      return { kind: i.Duration, targetSchema: "", targetField: "" };
    case e.startsWith("ref:"):
      const [, n, r] = t.split(":");
      return { kind: i.Ref, targetSchema: n ?? "", targetField: r ?? "" };
    default:
      throw new Error(`scrt: unsupported field type ${t}`);
  }
}
function je(t) {
  const e = [];
  let n = "", r = null;
  for (const s of t)
    (s === '"' || s === "'" || s === "`") && r === null ? (r = s, n += s) : r && s === r ? (r = null, n += s) : !r && s === "," ? (n.trim() && e.push(n.trim()), n = "") : n += s;
  return n.trim() && e.push(n.trim()), e;
}
function Xe(t, e) {
  const n = e.trim();
  if (n) {
    if (t.kind === i.Ref) {
      t.pendingDefault = n;
      return;
    }
    t.defaultValue = V(t.kind, n);
  }
}
function He(t) {
  const e = t.indexOf("=") >= 0 ? t.indexOf("=") : t.indexOf(":");
  return e === -1 ? t : t.slice(e + 1);
}
function V(t, e) {
  switch (t) {
    case i.Bool:
      return new b(t, e.toLowerCase() === "true" || e === "1");
    case i.Int64:
      return new b(t, void 0, BigInt(e));
    case i.Uint64:
    case i.Ref:
      return new b(t, void 0, void 0, BigInt(e));
    case i.Float64:
      return new b(t, void 0, void 0, void 0, Number(e));
    case i.String:
      return new b(t, void 0, void 0, void 0, void 0, qe(e));
    case i.Bytes:
      return new b(t, void 0, void 0, void 0, void 0, void 0, ee(e));
    case i.Date:
      return new b(t, void 0, J(U(p(e))));
    case i.DateTime:
      return new b(t, void 0, v(k(p(e))));
    case i.Timestamp:
      return new b(t, void 0, v($(p(e))));
    case i.TimestampTZ: {
      const n = A(p(e));
      return new b(t, void 0, void 0, void 0, void 0, x(n));
    }
    case i.Duration:
      return new b(t, void 0, O(p(e)));
    default:
      throw new Error(`scrt: defaults not supported for kind ${t}`);
  }
}
function qe(t) {
  const e = t.trim();
  return e ? e.startsWith('"') || e.startsWith("'") || e.startsWith("`") ? e.slice(1, -1) : e : "";
}
function p(t) {
  const e = t.trim();
  return e ? e.startsWith('"') && e.endsWith('"') || e.startsWith("'") && e.endsWith("'") || e.startsWith("`") && e.endsWith("`") ? e.slice(1, -1) : e : "";
}
function ee(t) {
  const e = t.trim();
  if (e.startsWith("0x") || e.startsWith("0X")) {
    const n = e.slice(2);
    if (n.length % 2 !== 0)
      throw new Error(`scrt: invalid hex literal ${t}`);
    const r = new Uint8Array(n.length / 2);
    for (let s = 0; s < n.length; s += 2)
      r[s / 2] = parseInt(n.slice(s, s + 2), 16);
    return r;
  }
  return new TextEncoder().encode(p(e));
}
function P(t, e) {
  const n = {}, r = t.split(",");
  let s = 0, o = Ye(r);
  const a = () => {
    for (; s < e.fields.length && e.fields[s].autoIncrement; ) {
      const c = Je(e.fields, s);
      if (o > c)
        return;
      s += 1;
    }
  };
  for (const c of r) {
    const u = c.trim();
    if (!u) {
      s += 1;
      continue;
    }
    if (u.startsWith("@")) {
      const { index: d, value: h } = Ke(e, u.slice(1));
      d >= 0 && (n[e.fields[d].name] = h, s = Math.max(s, d + 1));
      continue;
    }
    if (a(), s >= e.fields.length)
      throw new Error("scrt: too many values in row");
    const l = e.fields[s];
    n[l.name] = te(u, l), s += 1, o -= 1;
  }
  return n;
}
function Ye(t) {
  return t.reduce((e, n) => {
    const r = n.trim();
    return !r || r.startsWith("@") ? e : e + 1;
  }, 0);
}
function Je(t, e) {
  let n = 0;
  for (let r = e; r < t.length; r += 1)
    t[r].autoIncrement || (n += 1);
  return n;
}
function Ke(t, e) {
  const [n, r] = e.split("=", 2);
  if (!r)
    throw new Error(`scrt: invalid assignment ${e}`);
  const s = Qe(n), o = t.tryFieldIndex(s);
  if (o === void 0)
    throw new Error(`scrt: field ${s} not found`);
  const a = t.fields[o];
  return { index: o, value: te(r.trim(), a) };
}
function Qe(t) {
  const n = t.trim().split(":");
  return n.length >= 2 ? n[1] : n[0];
}
function te(t, e) {
  const n = e.valueKind(), r = t.trim();
  switch (n) {
    case i.Uint64:
      return BigInt(r);
    case i.Int64:
      return BigInt(r);
    case i.Float64:
      return Number(r);
    case i.Bool:
      return r.toLowerCase() === "true" || r === "1";
    case i.String:
      return p(r);
    case i.Bytes:
      return ee(r);
    case i.Date:
      return U(p(r));
    case i.DateTime:
      return k(p(r));
    case i.Timestamp:
      return $(p(r));
    case i.TimestampTZ:
      return A(p(r));
    case i.Duration:
      return O(p(r));
    default:
      return r;
  }
}
function et(t, e) {
  e.fields.forEach((n, r) => ne(t, e, r, /* @__PURE__ */ new Set()));
}
function ne(t, e, n, r) {
  const s = e.fields[n];
  if (s.resolvedKind !== i.Invalid)
    return s.resolvedKind;
  if (s.kind !== i.Ref)
    return s.resolvedKind = s.kind, s.pendingDefault && !s.defaultValue && (s.defaultValue = V(s.resolvedKind, s.pendingDefault), s.pendingDefault = ""), s.resolvedKind;
  const o = `${e.name}.${s.name}`;
  if (r.has(o))
    throw new Error(`scrt: circular reference detected for ${o}`);
  r.add(o);
  const a = t.schemas.get(s.targetSchema);
  if (!a)
    throw new Error(`scrt: schema ${e.name} references unknown schema ${s.targetSchema}`);
  const c = a.tryFieldIndex(s.targetField);
  if (c === void 0)
    throw new Error(`scrt: schema ${e.name} references unknown field ${s.targetSchema}.${s.targetField}`);
  const u = ne(t, a, c, r);
  return s.resolvedKind = u, r.delete(o), s.pendingDefault && !s.defaultValue && (s.defaultValue = V(u, s.pendingDefault), s.pendingDefault = ""), u;
}
function tt(t) {
  if (typeof Buffer < "u")
    return Buffer.from(t).toString("base64");
  let e = "";
  for (let n = 0; n < t.length; n += 1)
    e += String.fromCharCode(t[n]);
  if (typeof btoa == "function")
    return btoa(e);
  throw new Error("scrt: base64 encoding unavailable in this environment");
}
class nt {
  constructor(e, n = 1024) {
    f(this, "schema");
    f(this, "rowLimit");
    f(this, "columns");
    f(this, "columnBuf", I());
    f(this, "rows", 0);
    this.schema = e, this.rowLimit = n, this.columns = e.fields.map((r) => {
      const s = r.valueKind(), o = { kind: s, presence: [] };
      switch (s) {
        case i.Uint64:
        case i.Ref:
          o.uints = new ye();
          break;
        case i.String:
        case i.TimestampTZ:
          o.strings = new Ee();
          break;
        case i.Bool:
          o.bools = new Te();
          break;
        case i.Int64:
        case i.Date:
        case i.DateTime:
        case i.Timestamp:
        case i.Duration:
          o.ints = new Ie();
          break;
        case i.Float64:
          o.floats = new Be();
          break;
        case i.Bytes:
          o.bytes = new xe();
          break;
        default:
          throw new Error(`scrt: unsupported field kind ${s}`);
      }
      return o;
    });
  }
  appendUint(e, n) {
    var r;
    (r = this.columns[e].uints) == null || r.append(n);
  }
  appendString(e, n) {
    var r;
    (r = this.columns[e].strings) == null || r.append(n);
  }
  appendBool(e, n) {
    var r;
    (r = this.columns[e].bools) == null || r.append(n);
  }
  appendInt(e, n) {
    var r;
    (r = this.columns[e].ints) == null || r.append(n);
  }
  appendFloat(e, n) {
    var r;
    (r = this.columns[e].floats) == null || r.append(n);
  }
  appendBytes(e, n) {
    var r;
    (r = this.columns[e].bytes) == null || r.append(n);
  }
  recordPresence(e, n) {
    this.columns[e].presence.push(n);
  }
  sealRow() {
    if (this.rows += 1, this.rows > this.rowLimit)
      throw new Error("scrt: page builder capacity exceeded");
  }
  full() {
    return this.rows >= this.rowLimit;
  }
  rowCount() {
    return this.rows;
  }
  reset() {
    var e, n, r, s, o, a;
    this.rows = 0;
    for (const c of this.columns)
      c.presence.length = 0, (e = c.uints) == null || e.reset(), (n = c.strings) == null || n.reset(), (r = c.bools) == null || r.reset(), (s = c.ints) == null || s.reset(), (o = c.floats) == null || o.reset(), (a = c.bytes) == null || a.reset();
  }
  encode(e) {
    var n, r, s, o, a, c;
    if (this.rows !== 0) {
      g(e, this.rows), g(e, this.columns.length);
      for (let u = 0; u < this.columns.length; u += 1) {
        const l = this.columns[u];
        switch (this.columnBuf.length = 0, rt(this.columnBuf, l.presence, this.rows), l.kind) {
          case i.Uint64:
          case i.Ref:
            (n = l.uints) == null || n.encode(this.columnBuf);
            break;
          case i.String:
          case i.TimestampTZ:
            (r = l.strings) == null || r.encode(this.columnBuf);
            break;
          case i.Bool:
            (s = l.bools) == null || s.encode(this.columnBuf);
            break;
          case i.Int64:
          case i.Date:
          case i.DateTime:
          case i.Timestamp:
          case i.Duration:
            (o = l.ints) == null || o.encode(this.columnBuf);
            break;
          case i.Float64:
            (a = l.floats) == null || a.encode(this.columnBuf);
            break;
          case i.Bytes:
            (c = l.bytes) == null || c.encode(this.columnBuf);
            break;
          default:
            throw new Error(`scrt: unsupported field kind ${l.kind}`);
        }
        g(e, u), y(e, l.kind), g(e, this.columnBuf.length), E(e, this.columnBuf);
      }
    }
  }
}
function rt(t, e, n) {
  const r = Math.floor((n + 7) / 8);
  if (g(t, r), r === 0)
    return;
  let s = 0, o = 0, a = 0;
  for (let c = 0; c < n; c += 1)
    e[c] && (s |= 1 << o), o += 1, o === 8 && (y(t, s), a += 1, s = 0, o = 0);
  for (o !== 0 && (y(t, s), a += 1); a < r; )
    y(t, 0), a += 1;
}
const B = "SCRT", re = 2, se = new TextDecoder();
class M {
  constructor(e) {
    f(this, "schema");
    f(this, "values");
    this.schema = e, this.values = new Array(e.fields.length).fill(null).map(() => ({ set: !1 }));
  }
  reset() {
    for (const e of this.values)
      e.set = !1, e.uint = void 0, e.int = void 0, e.float = void 0, e.str = void 0, e.bytes = void 0, e.bool = void 0, e.borrowed = void 0;
  }
  setByIndex(e, n) {
    this.values[e] = { ...n, set: !0 };
  }
  valuesSlice() {
    return this.values;
  }
  fieldIndex(e) {
    return this.schema.fieldIndexByName(e);
  }
  setValue(e, n) {
    this.setByIndex(this.fieldIndex(e), n);
  }
  setUint(e, n) {
    const r = this.claimSlot(this.fieldIndex(e));
    r.uint = BigInt(n);
  }
  setInt(e, n) {
    const r = this.claimSlot(this.fieldIndex(e));
    r.int = BigInt(n);
  }
  setFloat(e, n) {
    const r = this.claimSlot(this.fieldIndex(e));
    r.float = n;
  }
  setBool(e, n) {
    const r = this.claimSlot(this.fieldIndex(e));
    r.bool = n;
  }
  setString(e, n) {
    const r = this.claimSlot(this.fieldIndex(e));
    r.str = n;
  }
  setBytes(e, n) {
    const r = this.claimSlot(this.fieldIndex(e));
    r.bytes = _(n);
  }
  claimSlot(e) {
    const n = this.values[e];
    return n.set = !0, n.uint = void 0, n.int = void 0, n.float = void 0, n.str = void 0, n.bytes = void 0, n.bool = void 0, n.borrowed = !1, n;
  }
}
class st {
  constructor(e, n = 1024) {
    f(this, "schema");
    f(this, "builder");
    f(this, "output", I());
    f(this, "headerWritten", !1);
    this.schema = e, this.builder = new nt(e, n);
  }
  writeRow(e) {
    if (e.schema !== this.schema)
      throw new Error("scrt: schema mismatch for row");
    this.ensureHeader();
    const n = e.valuesSlice();
    this.schema.fields.forEach((r, s) => {
      const o = n[s];
      if (!o.set) {
        this.builder.recordPresence(s, !1);
        return;
      }
      switch (this.builder.recordPresence(s, !0), r.valueKind()) {
        case i.Uint64:
        case i.Ref:
          this.builder.appendUint(s, o.uint ?? 0n);
          break;
        case i.String:
        case i.TimestampTZ:
          this.builder.appendString(s, o.str ?? "");
          break;
        case i.Bool:
          this.builder.appendBool(s, o.bool ?? !1);
          break;
        case i.Int64:
        case i.Date:
        case i.DateTime:
        case i.Timestamp:
        case i.Duration:
          this.builder.appendInt(s, o.int ?? 0n);
          break;
        case i.Float64:
          this.builder.appendFloat(s, o.float ?? 0);
          break;
        case i.Bytes:
          this.builder.appendBytes(s, o.bytes ?? new Uint8Array());
          break;
        default:
          throw new Error(`scrt: unsupported field kind ${r.valueKind()}`);
      }
    }), this.builder.sealRow(), this.builder.full() && this.flushPage();
  }
  finish() {
    return this.flushPage(), he(this.output);
  }
  ensureHeader() {
    if (this.headerWritten)
      return;
    for (const s of B)
      this.output.push(s.charCodeAt(0));
    this.output.push(re);
    const e = this.schema.fingerprint(), n = new Uint8Array(8);
    let r = e;
    for (let s = 0; s < 8; s += 1)
      n[s] = Number(r & 0xffn), r >>= 8n;
    E(this.output, n), this.headerWritten = !0;
  }
  flushPage() {
    if (this.builder.rowCount() === 0)
      return;
    const e = I();
    this.builder.encode(e);
    const n = I();
    g(n, e.length), E(this.output, n), E(this.output, e), this.builder.reset();
  }
}
class it {
  constructor(e) {
    f(this, "rows", 0);
    f(this, "cursor", 0);
    f(this, "columns");
    this.columns = new Array(e).fill(null).map(() => ({
      kind: i.Invalid,
      rowIndexes: [],
      uints: [],
      stringTable: [],
      stringIndexes: [],
      bools: [],
      ints: [],
      floats: [],
      bytes: []
    }));
  }
}
class ot {
  constructor(e, n, r = {}) {
    f(this, "data");
    f(this, "schema");
    f(this, "options");
    f(this, "state");
    f(this, "offset", 0);
    f(this, "headerRead", !1);
    this.data = e, this.schema = n, this.options = r, this.state = new it(n.fields.length);
  }
  readRow(e) {
    if (!this.headerRead && !this.consumeHeader() || this.state.cursor >= this.state.rows && !this.loadPage())
      return !1;
    const n = this.state.cursor, r = e.valuesSlice();
    for (let s = 0; s < this.schema.fields.length; s += 1) {
      const o = this.schema.fields[s], a = this.state.columns[s], c = r[s], u = a.rowIndexes[n] ?? -1;
      if (u < 0) {
        mt(o, c);
        continue;
      }
      switch (c.set = !0, o.valueKind()) {
        case i.Uint64:
        case i.Ref:
          c.uint = a.uints[u];
          break;
        case i.String:
        case i.TimestampTZ:
          c.str = a.stringTable[a.stringIndexes[u] ?? 0] ?? "";
          break;
        case i.Bool:
          c.bool = a.bools[u];
          break;
        case i.Int64:
        case i.Date:
        case i.DateTime:
        case i.Timestamp:
        case i.Duration:
          c.int = a.ints[u];
          break;
        case i.Float64:
          c.float = a.floats[u];
          break;
        case i.Bytes:
          c.bytes = a.bytes[u], c.borrowed = this.options.zeroCopyBytes ?? !1;
          break;
        default:
          throw new Error(`scrt: unsupported field kind ${o.valueKind()}`);
      }
    }
    return this.state.cursor += 1, !0;
  }
  consumeHeader() {
    if (this.data.length < B.length + 1 + 8)
      return !1;
    if (se.decode(this.data.subarray(0, B.length)) !== B)
      throw new Error("scrt: invalid stream header");
    const n = this.data[B.length];
    if (n !== re)
      throw new Error(`scrt: unsupported version ${n}`);
    const r = this.data.subarray(B.length + 1, B.length + 9);
    let s = 0n;
    for (let o = 7; o >= 0; o -= 1)
      s = s << 8n | BigInt(r[o]);
    if (s !== this.schema.fingerprint())
      throw new Error("scrt: schema fingerprint mismatch");
    return this.offset = B.length + 9, this.headerRead = !0, !0;
  }
  loadPage() {
    if (this.offset >= this.data.length)
      return !1;
    const { value: e, bytesRead: n } = m(this.data, this.offset);
    this.offset += n;
    const r = Number(e);
    if (r === 0 || this.offset + r > this.data.length)
      return !1;
    const s = this.data.subarray(this.offset, this.offset + r);
    return this.offset += r, this.decodePage(s), !0;
  }
  decodePage(e) {
    let n = 0;
    const { value: r, bytesRead: s } = m(e, n);
    n += s;
    const o = Number(r), { value: a, bytesRead: c } = m(e, n);
    n += c;
    const u = Number(a);
    if (u !== this.schema.fields.length)
      throw new Error("scrt: column count mismatch");
    this.state.rows = o, this.state.cursor = 0;
    for (let l = 0; l < u; l += 1) {
      const { value: d, bytesRead: h } = m(e, n);
      n += h;
      const w = Number(d), N = e[n];
      n += 1;
      const { value: S, bytesRead: ue } = m(e, n);
      n += ue;
      const fe = e.subarray(n, n + Number(S));
      n += Number(S), this.decodeColumn(w, N, fe, o);
    }
  }
  decodeColumn(e, n, r, s) {
    const o = this.state.columns[e];
    o.kind = n;
    const a = at(r, s);
    o.rowIndexes = a.indexes;
    const c = r.subarray(a.bytesRead);
    switch (n) {
      case i.Uint64:
      case i.Ref: {
        const u = ct(c, a.setCount);
        o.uints = u.values;
        break;
      }
      case i.String:
      case i.TimestampTZ: {
        const u = dt(c, a.setCount);
        o.stringTable = u.table, o.stringIndexes = u.indexes;
        break;
      }
      case i.Bool: {
        const u = lt(c, a.setCount);
        o.bools = u.values;
        break;
      }
      case i.Int64:
      case i.Date:
      case i.DateTime:
      case i.Timestamp:
      case i.Duration: {
        const u = ut(c, a.setCount);
        o.ints = u.values;
        break;
      }
      case i.Float64: {
        const u = ft(c, a.setCount);
        o.floats = u.values;
        break;
      }
      case i.Bytes: {
        const u = ht(c, a.setCount, this.options.zeroCopyBytes ?? !1);
        o.bytes = u.values;
        break;
      }
      default:
        throw new Error(`scrt: unsupported field kind ${n}`);
    }
  }
}
function at(t, e) {
  const { value: n, bytesRead: r } = m(t, 0), s = Number(n);
  let o = r;
  const a = new Array(e).fill(-1);
  let c = 0;
  for (let u = 0; u < e; u += 1) {
    const l = Math.floor(u / 8), d = u % 8;
    l < s && t[o + l] & 1 << d && (a[u] = c, c += 1);
  }
  return o += s, { indexes: a, setCount: c, bytesRead: o };
}
function ct(t, e) {
  let n = 0;
  const { value: r, bytesRead: s } = m(t, n);
  n += s;
  const o = r & 1n, a = Number(r >> 1n);
  if (a !== e)
    throw new Error("scrt: uint column count mismatch");
  const c = new Array(a).fill(0n);
  if (a === 0)
    return { values: c };
  if (o === 0n)
    for (let u = 0; u < a; u += 1) {
      const l = m(t, n);
      n += l.bytesRead, c[u] = l.value;
    }
  else {
    let u = m(t, n);
    n += u.bytesRead, c[0] = u.value;
    for (let l = 1; l < a; l += 1)
      u = m(t, n), n += u.bytesRead, c[l] = c[l - 1] + u.value;
  }
  return { values: c };
}
function ut(t, e) {
  let n = 0;
  const { value: r, bytesRead: s } = m(t, n);
  n += s;
  const o = r & 1n, a = Number(r >> 1n);
  if (a !== e)
    throw new Error("scrt: int column count mismatch");
  const c = new Array(a).fill(0n);
  if (a === 0)
    return { values: c };
  if (o === 0n)
    for (let u = 0; u < a; u += 1) {
      const l = R(t, n);
      n += l.bytesRead, c[u] = l.value;
    }
  else {
    let u = R(t, n);
    n += u.bytesRead, c[0] = u.value;
    for (let l = 1; l < a; l += 1)
      u = R(t, n), n += u.bytesRead, c[l] = c[l - 1] + u.value;
  }
  return { values: c };
}
function ft(t, e) {
  let n = 0;
  const { value: r, bytesRead: s } = m(t, n);
  n += s;
  const o = Number(r);
  if (o !== e)
    throw new Error("scrt: float column count mismatch");
  const a = new Array(o).fill(0), c = new DataView(t.buffer, t.byteOffset, t.byteLength);
  for (let u = 0; u < o; u += 1) {
    if (n + 8 > t.length)
      throw new Error("scrt: float column truncated");
    a[u] = c.getFloat64(n, !0), n += 8;
  }
  return { values: a };
}
function lt(t, e) {
  let n = 0;
  const { value: r, bytesRead: s } = m(t, n);
  n += s;
  const o = Number(r);
  if (o !== e)
    throw new Error("scrt: bool column count mismatch");
  const a = new Array(o);
  for (let c = 0; c < o; c += 1)
    a[c] = t[n + c] !== 0;
  return { values: a };
}
function dt(t, e) {
  let n = 0;
  const { value: r, bytesRead: s } = m(t, n);
  n += s;
  const o = Number(r), a = new Array(o);
  for (let h = 0; h < o; h += 1) {
    const w = m(t, n);
    n += w.bytesRead;
    const N = Number(w.value), S = t.subarray(n, n + N);
    a[h] = se.decode(S), n += N;
  }
  const { value: c, bytesRead: u } = m(t, n);
  n += u;
  const l = Number(c);
  if (l !== e)
    throw new Error("scrt: string index count mismatch");
  const d = new Array(l);
  for (let h = 0; h < l; h += 1) {
    const w = m(t, n);
    n += w.bytesRead, d[h] = Number(w.value);
  }
  return { table: a, indexes: d };
}
function ht(t, e, n) {
  let r = 0;
  const { value: s, bytesRead: o } = m(t, r);
  r += o;
  const a = Number(s);
  if (a !== e)
    throw new Error("scrt: bytes column count mismatch");
  const c = new Array(a);
  for (let u = 0; u < a; u += 1) {
    const l = m(t, r);
    r += l.bytesRead;
    const d = Number(l.value), h = t.subarray(r, r + d);
    c[u] = n ? h : _(h), r += d;
  }
  return { values: c };
}
function _(t) {
  const e = new Uint8Array(t.length);
  return e.set(t), e;
}
function mt(t, e) {
  e.set = !1;
  const n = t.defaultValue;
  if (n)
    switch (e.set = !0, n.kind) {
      case i.Uint64:
      case i.Ref:
        e.uint = n.uintValue ?? 0n;
        break;
      case i.Int64:
      case i.Date:
      case i.DateTime:
      case i.Timestamp:
      case i.Duration:
        e.int = n.intValue ?? 0n;
        break;
      case i.Float64:
        e.float = n.floatValue ?? 0;
        break;
      case i.Bool:
        e.bool = n.boolValue ?? !1;
        break;
      case i.String:
      case i.TimestampTZ:
        e.str = n.stringValue ?? "";
        break;
      case i.Bytes:
        e.bytes = n.bytesValue ? _(n.bytesValue) : new Uint8Array();
        break;
      default:
        e.set = !1;
    }
}
const gt = new TextEncoder(), wt = new TextDecoder();
function jt(t, e, n = {}) {
  if (!t)
    throw new Error("scrt: schema is required for marshal");
  const r = new st(t, n.rowsPerPage ?? 1024), s = new M(t);
  for (const o of bt(e)) {
    if (o instanceof M) {
      if (o.schema !== t)
        throw new Error("scrt: row schema mismatch during marshal");
      r.writeRow(o);
      continue;
    }
    if (!It(o))
      throw new Error("scrt: marshal expects plain objects, maps, or Row instances");
    Bt(s, t, o), r.writeRow(s);
  }
  return r.finish();
}
function Xt(t, e, n) {
  const r = [];
  for (const s of pt(t, e, n))
    r.push(s);
  return r;
}
function* pt(t, e, n) {
  if (!e)
    throw new Error("scrt: schema is required for unmarshal");
  const r = At(n), s = new ot(kt(t), e, { zeroCopyBytes: r.zeroCopyBytes }), o = new M(e);
  for (; s.readRow(o); )
    yield Ct(o, e, r), o.reset();
}
function bt(t) {
  return yt(t) ? t : [t];
}
function yt(t) {
  return t == null || typeof t == "string" ? !1 : typeof t[Symbol.iterator] == "function";
}
function It(t) {
  return t instanceof Map ? !0 : t instanceof Date ? !1 : t && typeof t == "object" ? !(Array.isArray(t) || ArrayBuffer.isView(t) || t instanceof ArrayBuffer) : !1;
}
function Bt(t, e, n) {
  t.reset();
  const r = Tt(n);
  e.fields.forEach((s, o) => {
    const a = r(s.name);
    if (a == null)
      return;
    const c = Et(s, a);
    c && t.setByIndex(o, c);
  });
}
function Tt(t) {
  return t instanceof Map ? (e) => t.get(e) : (e) => t[e];
}
function Et(t, e) {
  if (e == null)
    return null;
  const n = t.valueKind(), r = { set: !0 };
  switch (n) {
    case i.Uint64:
    case i.Ref:
      return r.uint = xt(e, t.name), r;
    case i.Int64:
      return r.int = ie(e, t.name), r;
    case i.Float64:
      return r.float = Dt(e, t.name), r;
    case i.Bool:
      return r.bool = Nt(e, t.name), r;
    case i.String:
      return r.str = St(e, t.name), r;
    case i.Bytes:
      return r.bytes = Rt(e, t.name), r;
    case i.Date:
      return r.int = J(Z(e, t.name, i.Date)), r;
    case i.DateTime:
    case i.Timestamp:
      return r.int = v(Z(e, t.name, n)), r;
    case i.TimestampTZ:
      return r.str = $t(e, t.name), r;
    case i.Duration:
      return r.int = Ut(e, t.name), r;
    default:
      throw new Error(`scrt: unsupported field kind ${n} for ${t.name}`);
  }
}
function xt(t, e) {
  const n = ie(t, e);
  if (n < 0n)
    throw new Error(`scrt: ${e} cannot be negative`);
  return n;
}
function ie(t, e) {
  if (typeof t == "bigint")
    return t;
  if (typeof t == "number") {
    if (!Number.isFinite(t) || !Number.isInteger(t))
      throw new Error(`scrt: ${e} must be a finite integer`);
    if (Math.abs(t) > Number.MAX_SAFE_INTEGER)
      throw new Error(`scrt: ${e} exceeds safe integer range`);
    return BigInt(t);
  }
  if (typeof t == "string") {
    const n = t.trim();
    if (!n)
      throw new Error(`scrt: ${e} cannot be empty`);
    return BigInt(n);
  }
  throw new Error(`scrt: ${e} expects an integer-compatible value`);
}
function Dt(t, e) {
  if (typeof t == "number") {
    if (!Number.isFinite(t))
      throw new Error(`scrt: ${e} must be finite`);
    return t;
  }
  if (typeof t == "bigint")
    return Number(t);
  if (typeof t == "string") {
    const n = Number(t.trim());
    if (Number.isNaN(n))
      throw new Error(`scrt: ${e} cannot parse float literal`);
    return n;
  }
  throw new Error(`scrt: ${e} expects a float-compatible value`);
}
function Nt(t, e) {
  if (typeof t == "boolean")
    return t;
  if (typeof t == "number") {
    if (!Number.isFinite(t))
      throw new Error(`scrt: ${e} must be finite`);
    return t !== 0;
  }
  if (typeof t == "string") {
    const n = t.trim().toLowerCase();
    if (n === "true" || n === "1")
      return !0;
    if (n === "false" || n === "0")
      return !1;
    throw new Error(`scrt: ${e} cannot parse boolean literal`);
  }
  throw new Error(`scrt: ${e} expects a boolean-compatible value`);
}
function St(t, e) {
  if (typeof t == "string")
    return t;
  if (typeof t == "number" || typeof t == "boolean" || typeof t == "bigint")
    return String(t);
  if (t instanceof Date) {
    if (!Number.isFinite(t.getTime()))
      throw new Error(`scrt: ${e} received invalid Date`);
    return t.toISOString();
  }
  if (t instanceof Uint8Array)
    return wt.decode(t);
  throw new Error(`scrt: ${e} expects a string-compatible value`);
}
function Rt(t, e) {
  if (t instanceof Uint8Array)
    return t.slice();
  if (ArrayBuffer.isView(t)) {
    const n = t;
    return new Uint8Array(n.buffer.slice(n.byteOffset, n.byteOffset + n.byteLength));
  }
  if (t instanceof ArrayBuffer)
    return new Uint8Array(t.slice(0));
  if (Array.isArray(t)) {
    const n = new Uint8Array(t.length);
    return t.forEach((r, s) => {
      if (typeof r != "number" || !Number.isFinite(r))
        throw new Error(`scrt: ${e} byte array contains non-number at index ${s}`);
      n[s] = r & 255;
    }), n;
  }
  if (typeof t == "string")
    return gt.encode(t);
  throw new Error(`scrt: ${e} expects bytes, ArrayBufferView, or string input`);
}
function Z(t, e, n) {
  if (t instanceof Date) {
    if (!Number.isFinite(t.getTime()))
      throw new Error(`scrt: ${e} received invalid Date`);
    return t;
  }
  if (typeof t == "string") {
    const r = t.trim();
    if (!r)
      throw new Error(`scrt: ${e} cannot parse empty temporal literal`);
    switch (n) {
      case i.Date:
        return U(r);
      case i.DateTime:
        return k(r);
      case i.Timestamp:
        return $(r);
      default:
        return $(r);
    }
  }
  if (typeof t == "number") {
    if (!Number.isFinite(t))
      throw new Error(`scrt: ${e} must be finite`);
    return oe(t);
  }
  if (typeof t == "bigint")
    return ae(t);
  throw new Error(`scrt: ${e} expects Date, number, bigint, or string`);
}
function $t(t, e) {
  if (t instanceof Date) {
    if (!Number.isFinite(t.getTime()))
      throw new Error(`scrt: ${e} received invalid Date`);
    return x(t);
  }
  if (typeof t == "string")
    return ke(t);
  if (typeof t == "number") {
    if (!Number.isFinite(t))
      throw new Error(`scrt: ${e} must be finite`);
    return x(oe(t));
  }
  if (typeof t == "bigint")
    return x(ae(t));
  throw new Error(`scrt: ${e} expects Date, number, bigint, or string`);
}
function Ut(t, e) {
  if (typeof t == "bigint")
    return t;
  if (typeof t == "number") {
    if (!Number.isFinite(t) || !Number.isInteger(t))
      throw new Error(`scrt: ${e} duration must be a finite integer`);
    if (Math.abs(t) > Number.MAX_SAFE_INTEGER)
      throw new Error(`scrt: ${e} duration exceeds safe integer range`);
    return BigInt(t);
  }
  if (typeof t == "string")
    return O(t);
  throw new Error(`scrt: ${e} expects bigint, number, or duration literal`);
}
function oe(t) {
  if (Number.isInteger(t))
    return D(W(BigInt(t)));
  const e = Math.trunc(t), n = t - e, r = BigInt(e) * 1000000000n + BigInt(Math.trunc(n * 1e9));
  return D(r);
}
function ae(t) {
  return D(W(t));
}
function kt(t) {
  if (t instanceof Uint8Array)
    return t;
  if (ArrayBuffer.isView(t)) {
    const e = t;
    return new Uint8Array(e.buffer, e.byteOffset, e.byteLength);
  }
  if (t instanceof ArrayBuffer)
    return new Uint8Array(t);
  throw new Error("scrt: unsupported binary source");
}
function At(t) {
  return {
    zeroCopyBytes: (t == null ? void 0 : t.zeroCopyBytes) ?? !1,
    numericMode: (t == null ? void 0 : t.numericMode) ?? "auto",
    temporalMode: (t == null ? void 0 : t.temporalMode) ?? "date",
    durationMode: (t == null ? void 0 : t.durationMode) ?? "bigint",
    objectFactory: (t == null ? void 0 : t.objectFactory) ?? (() => ({}))
  };
}
function Ct(t, e, n) {
  const r = n.objectFactory(), s = t.valuesSlice();
  return e.fields.forEach((o, a) => {
    const c = s[a];
    if (!c.set)
      return;
    const u = Ft(o, c, n);
    vt(r, o.name, u);
  }), r;
}
function vt(t, e, n) {
  if (t instanceof Map) {
    t.set(e, n);
    return;
  }
  t[e] = n;
}
function Ft(t, e, n) {
  const r = t.valueKind();
  switch (r) {
    case i.Uint64:
    case i.Ref:
      return G(e.uint ?? 0n, n.numericMode, t.name, !0);
    case i.Int64:
      return G(e.int ?? 0n, n.numericMode, t.name, !1);
    case i.Float64:
      return e.float ?? 0;
    case i.Bool:
      return e.bool ?? !1;
    case i.String:
      return e.str ?? "";
    case i.Bytes:
      return e.bytes ?? new Uint8Array();
    case i.Date: {
      const s = Ae(e.int ?? 0n);
      return n.temporalMode === "string" ? Ce(s) : s;
    }
    case i.DateTime:
    case i.Timestamp: {
      const s = D(e.int ?? 0n);
      return n.temporalMode === "string" ? ve(s) : s;
    }
    case i.TimestampTZ: {
      const s = e.str ?? "";
      return s ? n.temporalMode === "string" ? s : A(s) : n.temporalMode === "date" ? /* @__PURE__ */ new Date(0) : "";
    }
    case i.Duration:
      return Vt(e.int ?? 0n, n.durationMode, t.name);
    default:
      throw new Error(`scrt: unsupported field kind ${r}`);
  }
}
function G(t, e, n, r) {
  if (r && t < 0n)
    throw new Error(`scrt: ${n} stored value cannot be negative`);
  switch (e) {
    case "bigint":
      return t;
    case "number":
      if (!L(t))
        throw new Error(`scrt: ${n} exceeds JS safe integer range`);
      return Number(t);
    default:
      return L(t) ? Number(t) : t;
  }
}
function Vt(t, e, n) {
  switch (e) {
    case "bigint":
      return t;
    case "number":
      if (!L(t))
        throw new Error(`scrt: duration ${n} exceeds JS safe integer range`);
      return Number(t);
    case "string":
      return Ve(t);
    default:
      return t;
  }
}
function L(t) {
  return t <= BigInt(Number.MAX_SAFE_INTEGER) && t >= BigInt(Number.MIN_SAFE_INTEGER);
}
const ce = new TextDecoder(), j = "SCB1", Mt = 1;
function Lt(t) {
  const e = new DataView(t);
  let n = 0;
  for (let h = 0; h < j.length; h += 1) {
    if (e.getUint8(n) !== j.charCodeAt(h))
      throw new Error("scrt: invalid bundle magic");
    n += 1;
  }
  const r = e.getUint8(n);
  if (n += 1, r !== Mt)
    throw new Error(`scrt: unsupported bundle version ${r}`);
  const s = e.getBigUint64(n, !0);
  n += 8;
  const o = e.getBigUint64(n, !0);
  n += 8;
  const a = new Date(Number(e.getBigInt64(n, !0) / 1000000n));
  n += 8;
  const c = X(e, t, n);
  n += c.bytes;
  const u = X(e, t, n);
  n += u.bytes;
  const l = H(e, t, n);
  n += l.bytes;
  const d = H(e, t, n);
  return {
    documentName: c.value,
    schemaName: u.value,
    documentFingerprint: s,
    schemaFingerprint: o,
    updatedAt: a,
    schemaText: ce.decode(l.data),
    payload: d.data
  };
}
function X(t, e, n) {
  const r = t.getUint16(n, !0), s = n + 2, o = s + r;
  if (o > e.byteLength)
    throw new Error("scrt: bundle string exceeds buffer");
  return { value: ce.decode(e.slice(s, o)), bytes: 2 + r };
}
function H(t, e, n) {
  const r = t.getUint32(n, !0), s = n + 4, o = s + r;
  if (o > e.byteLength)
    throw new Error("scrt: bundle blob exceeds buffer");
  return { data: new Uint8Array(e.slice(s, o)), bytes: 4 + r };
}
function q(t) {
  const e = t.trim();
  return e ? e.endsWith("/") ? e.slice(0, -1) : e : "http://localhost:8080";
}
async function T(t) {
  if (t.ok)
    return t;
  const e = await t.text();
  throw new Error(e || t.statusText);
}
class Ht {
  constructor(e = "http://localhost:8080") {
    f(this, "baseUrl");
    this.baseUrl = q(e);
  }
  setBaseUrl(e) {
    this.baseUrl = q(e);
  }
  url(e) {
    return `${this.baseUrl}${e}`;
  }
  async listDocuments() {
    return (await (await T(await fetch(this.url("/documents"), {
      headers: { Accept: "text/plain" }
    }))).text()).split(/\r?\n/).map((r) => r.trim()).filter(Boolean);
  }
  async downloadDocument(e) {
    return (await T(await fetch(this.url(`/documents/${encodeURIComponent(e)}`), {
      headers: { Accept: "text/plain" }
    }))).text();
  }
  async saveDocument(e, n) {
    await T(await fetch(this.url(`/documents/${encodeURIComponent(e)}`), {
      method: "POST",
      headers: { "Content-Type": "text/plain; charset=utf-8" },
      body: n
    }));
  }
  async deleteDocument(e) {
    await T(await fetch(this.url(`/documents/${encodeURIComponent(e)}`), {
      method: "DELETE"
    }));
  }
  async uploadRecords(e, n, r) {
    await T(await fetch(this.url(`/records/${encodeURIComponent(e)}/${encodeURIComponent(n)}`), {
      method: "POST",
      headers: { "Content-Type": "application/x-scrt" },
      body: r
    }));
  }
  async fetchRecords(e, n) {
    const s = await (await T(await fetch(this.url(`/records/${encodeURIComponent(e)}/${encodeURIComponent(n)}`)))).arrayBuffer();
    return new Uint8Array(s);
  }
  async fetchBundle(e, n) {
    const s = await (await T(await fetch(this.url(`/bundle?document=${encodeURIComponent(e)}&schema=${encodeURIComponent(n)}`)))).arrayBuffer();
    return Lt(s);
  }
}
export {
  zt as BinaryReader,
  _t as BinaryWriter,
  b as DefaultValue,
  ze as Document,
  We as Field,
  i as FieldKind,
  ot as Reader,
  M as Row,
  _e as Schema,
  Ht as ScrtHttpClient,
  st as Writer,
  Wt as bufferLength,
  he as bufferToUint8Array,
  ke as canonicalTimestampTZ,
  me as concatByteBuffers,
  I as createBuffer,
  Lt as decodeBundle,
  Ae as decodeDate,
  D as decodeInstant,
  J as encodeDate,
  v as encodeInstant,
  Zt as encodeUint64LE,
  Ce as formatDate,
  Ve as formatDuration,
  ve as formatInstant,
  x as formatTimestampTZ,
  W as inferEpochNanoseconds,
  jt as marshalRecords,
  K as numericTimestamp,
  U as parseDate,
  k as parseDateTime,
  O as parseDuration,
  Gt as parseSchema,
  $ as parseTimestamp,
  A as parseTimestampTZ,
  y as pushByte,
  E as pushBytes,
  m as readUvarint,
  R as readVarint,
  pt as streamDecodedRows,
  Pt as toSafeNumber,
  Xt as unmarshalRecords,
  g as writeUvarint,
  C as writeVarint
};
//# sourceMappingURL=scrt.es.js.map
