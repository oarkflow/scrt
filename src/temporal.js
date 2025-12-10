const ISO_DATE = /^(\d{4})-(\d{2})-(\d{2})$/;
const ISO_DATE_TIME = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?$/;
const NUMERIC = /^[-+]?\d+(?:\.\d+)?$/;
const DURATION_TOKEN = /(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h|d)/gi;
const NANOS = {
    ns: 1n,
    us: 1000n,
    "µs": 1000n,
    ms: 1000000n,
    s: 1000000000n,
    m: 60n * 1000000000n,
    h: 60n * 60n * 1000000000n,
    d: 24n * 60n * 60n * 1000000000n,
};
export function parseDate(raw) {
    const trimmed = raw.trim();
    const iso = ISO_DATE.exec(trimmed);
    if (!iso) {
        const parsed = new Date(trimmed);
        if (Number.isNaN(parsed.getTime())) {
            throw new Error(`temporal: invalid date ${raw}`);
        }
        return utcDate(parsed.getUTCFullYear(), parsed.getUTCMonth(), parsed.getUTCDate());
    }
    const [, year, month, day] = iso;
    return utcDate(Number(year), Number(month) - 1, Number(day));
}
export function parseDateTime(raw) {
    const trimmed = raw.trim();
    const match = ISO_DATE_TIME.exec(trimmed);
    if (!match) {
        const parsed = new Date(trimmed);
        if (Number.isNaN(parsed.getTime())) {
            throw new Error(`temporal: invalid datetime ${raw}`);
        }
        return new Date(parsed.toISOString());
    }
    const [, y, m, d, hh, mm, ss = "0", frac = "0"] = match;
    const nanos = normalizeFraction(frac);
    const date = Date.UTC(Number(y), Number(m) - 1, Number(d), Number(hh), Number(mm), Number(ss));
    return new Date(date + Number(nanos / 1000000n));
}
export function parseTimestamp(raw) {
    const trimmed = raw.trim();
    if (NUMERIC.test(trimmed)) {
        return numericTimestamp(trimmed);
    }
    const parsed = new Date(trimmed);
    if (Number.isNaN(parsed.getTime())) {
        return parseDateTime(trimmed);
    }
    return new Date(parsed.toISOString());
}
export function parseTimestampTZ(raw) {
    const trimmed = raw.trim();
    if (NUMERIC.test(trimmed)) {
        return numericTimestamp(trimmed);
    }
    const parsed = new Date(trimmed);
    if (Number.isNaN(parsed.getTime())) {
        throw new Error(`temporal: invalid timestamptz ${raw}`);
    }
    return parsed;
}
export function parseDuration(raw) {
    const trimmed = raw.trim();
    if (!trimmed) {
        throw new Error("temporal: empty duration");
    }
    let total = 0n;
    let matched = false;
    for (const token of trimmed.matchAll(DURATION_TOKEN)) {
        matched = true;
        const [, value, unitRaw] = token;
        const unit = unitRaw.toLowerCase();
        const scale = NANOS[unit];
        if (!scale) {
            throw new Error(`temporal: unsupported duration unit ${unitRaw}`);
        }
        const [whole, frac = "0"] = value.split(".");
        let nanos = BigInt(whole) * scale;
        if (frac) {
            const fracScale = scale / powerOfTen(frac.length);
            nanos += BigInt(frac) * fracScale;
        }
        total += nanos;
    }
    if (!matched) {
        throw new Error(`temporal: malformed duration ${raw}`);
    }
    return total;
}
export function encodeInstant(value) {
    if (value instanceof Date) {
        return BigInt(value.getTime()) * 1000000n;
    }
    if (typeof value === "number") {
        return BigInt(Math.trunc(value)) * 1000000n;
    }
    return value;
}
export function encodeDate(value) {
    if (value instanceof Date) {
        const midnight = utcDate(value.getUTCFullYear(), value.getUTCMonth(), value.getUTCDate());
        return BigInt(midnight.getTime()) * 1000000n;
    }
    const parsed = parseDate(value);
    return BigInt(parsed.getTime()) * 1000000n;
}
export function formatTimestampTZ(value) {
    return value.toISOString();
}
export function canonicalTimestampTZ(raw) {
    if (!raw.trim()) {
        return "";
    }
    const parsed = parseTimestampTZ(raw);
    return formatTimestampTZ(parsed);
}
export function decodeInstant(value) {
    const ns = BigInt(value);
    if (ns === 0n) {
        return new Date(0);
    }
    const ms = Number(ns / 1000000n);
    return new Date(ms);
}
export function decodeDate(value) {
    return decodeInstant(value);
}
export function formatDate(value) {
    if (!Number.isFinite(value.getTime())) {
        return "";
    }
    return value.toISOString().slice(0, 10);
}
export function formatInstant(value) {
    if (!Number.isFinite(value.getTime())) {
        return "";
    }
    return value.toISOString();
}
export function numericTimestamp(raw) {
    if (raw.includes(".")) {
        const [whole, frac] = raw.split(".");
        const sec = BigInt(whole);
        const fracDigits = BigInt(powerOfTen(frac.length));
        const nanos = (BigInt(frac) * 1000000000n) / fracDigits;
        return new Date(Number(sec * 1000n + nanos / 1000000n));
    }
    const value = BigInt(raw);
    const nanos = inferEpochNanoseconds(value);
    return new Date(Number(nanos / 1000000n));
}
export function inferEpochNanoseconds(value) {
    const abs = value < 0n ? -value : value;
    if (abs < 100000000000n) {
        return value * 1000000000n;
    }
    if (abs < 100000000000000n) {
        return value * 1000000n;
    }
    if (abs < 100000000000000000n) {
        return value * 1000n;
    }
    return value;
}
const DURATION_UNITS = [
    { nanos: 24n * 60n * 60n * 1000000000n, suffix: "d" },
    { nanos: 60n * 60n * 1000000000n, suffix: "h" },
    { nanos: 60n * 1000000000n, suffix: "m" },
    { nanos: 1000000000n, suffix: "s" },
    { nanos: 1000000n, suffix: "ms" },
    { nanos: 1000n, suffix: "µs" },
    { nanos: 1n, suffix: "ns" },
];
export function formatDuration(nanos) {
    if (nanos === 0n) {
        return "0s";
    }
    const negative = nanos < 0n;
    let remaining = negative ? -nanos : nanos;
    const chunks = [];
    for (const unit of DURATION_UNITS) {
        if (remaining < unit.nanos) {
            continue;
        }
        const value = remaining / unit.nanos;
        remaining -= value * unit.nanos;
        chunks.push(`${value}${unit.suffix}`);
        if (remaining === 0n) {
            break;
        }
    }
    if (remaining > 0n) {
        chunks.push(`${remaining}ns`);
    }
    return `${negative ? "-" : ""}${chunks.join("")}`;
}
function utcDate(year, month, day) {
    return new Date(Date.UTC(year, month, day, 0, 0, 0, 0));
}
function normalizeFraction(input) {
    const digits = input.padEnd(9, "0").slice(0, 9);
    return BigInt(digits);
}
function powerOfTen(exp) {
    let result = 1n;
    for (let i = 0; i < exp; i += 1) {
        result *= 10n;
    }
    return result;
}
