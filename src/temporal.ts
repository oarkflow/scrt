const ISO_DATE = /^(\d{4})-(\d{2})-(\d{2})$/;
const ISO_DATE_TIME = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?$/;
const NUMERIC = /^[-+]?\d+(?:\.\d+)?$/;
const DURATION_TOKEN = /(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h|d)/gi;

const NANOS = {
  ns: 1n,
  us: 1_000n,
  "µs": 1_000n,
  ms: 1_000_000n,
  s: 1_000_000_000n,
  m: 60n * 1_000_000_000n,
  h: 60n * 60n * 1_000_000_000n,
  d: 24n * 60n * 60n * 1_000_000_000n,
} as const;

export function parseDate(raw: string): Date {
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

export function parseDateTime(raw: string): Date {
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
  return new Date(date + Number(nanos / 1_000_000n));
}

export function parseTimestamp(raw: string): Date {
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

export function parseTimestampTZ(raw: string): Date {
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

export function parseDuration(raw: string): bigint {
  const trimmed = raw.trim();
  if (!trimmed) {
    throw new Error("temporal: empty duration");
  }
  let total = 0n;
  let matched = false;
  for (const token of trimmed.matchAll(DURATION_TOKEN)) {
    matched = true;
    const [, value, unitRaw] = token;
    const unit = unitRaw.toLowerCase() as keyof typeof NANOS;
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

export function encodeInstant(value: Date | number | bigint): bigint {
  if (value instanceof Date) {
    return BigInt(value.getTime()) * 1_000_000n;
  }
  if (typeof value === "number") {
    return BigInt(Math.trunc(value)) * 1_000_000n;
  }
  return value;
}

export function encodeDate(value: Date | string): bigint {
  if (value instanceof Date) {
    const midnight = utcDate(value.getUTCFullYear(), value.getUTCMonth(), value.getUTCDate());
    return BigInt(midnight.getTime()) * 1_000_000n;
  }
  const parsed = parseDate(value);
  return BigInt(parsed.getTime()) * 1_000_000n;
}

export function formatTimestampTZ(value: Date): string {
  return value.toISOString();
}

export function numericTimestamp(raw: string): Date {
  if (raw.includes(".")) {
    const [whole, frac] = raw.split(".");
    const sec = BigInt(whole);
    const fracDigits = BigInt(powerOfTen(frac.length));
    const nanos = (BigInt(frac) * 1_000_000_000n) / fracDigits;
    return new Date(Number(sec * 1_000n + nanos / 1_000_000n));
  }
  const value = BigInt(raw);
  const nanos = inferEpochNanoseconds(value);
  return new Date(Number(nanos / 1_000_000n));
}

export function inferEpochNanoseconds(value: bigint): bigint {
  const abs = value < 0n ? -value : value;
  if (abs < 100_000_000_000n) {
    return value * 1_000_000_000n;
  }
  if (abs < 100_000_000_000_000n) {
    return value * 1_000_000n;
  }
  if (abs < 100_000_000_000_000_000n) {
    return value * 1_000n;
  }
  return value;
}

function utcDate(year: number, month: number, day: number): Date {
  return new Date(Date.UTC(year, month, day, 0, 0, 0, 0));
}

function normalizeFraction(input: string): bigint {
  const digits = input.padEnd(9, "0").slice(0, 9);
  return BigInt(digits);
}

function powerOfTen(exp: number): bigint {
  let result = 1n;
  for (let i = 0; i < exp; i += 1) {
    result *= 10n;
  }
  return result;
}
