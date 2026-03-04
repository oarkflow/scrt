import { encodeDate, encodeInstant, formatTimestampTZ, parseDate, parseDateTime, parseDuration, parseTimestamp, parseTimestampTZ } from "./temporal";

const FNV_OFFSET = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;

export enum FieldKind {
	Invalid = 0,
	Uint64 = 1,
	String = 2,
	Ref = 3,
	Bool = 4,
	Int64 = 5,
	Float64 = 6,
	Bytes = 7,
	Date = 8,
	DateTime = 9,
	Timestamp = 10,
	TimestampTZ = 11,
	Duration = 12,
}

export class DefaultValue {
	constructor(
		public kind: FieldKind,
		public boolValue?: boolean,
		public intValue?: bigint,
		public uintValue?: bigint,
		public floatValue?: number,
		public stringValue?: string,
		public bytesValue?: Uint8Array,
	) { }

	hashKey(): string {
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
	public resolvedKind: FieldKind = FieldKind.Invalid;
	public pendingDefault = "";

	constructor(
		public readonly name: string,
		public readonly kind: FieldKind,
		public readonly rawType: string,
		public targetSchema = "",
		public targetField = "",
		public autoIncrement = false,
		public primaryKey = false,
		public format = "",
		public pattern = "",
		public enumValues: string[] = [],
		public minLength?: number,
		public maxLength?: number,
		public minimum?: number,
		public maximum?: number,
		public description = "",
		public example = "",
		public isArray = false,
		public isMap = false,
		public isObject = false,
		public arrayElemType = "",
		public mapKeyType = "",
		public mapValueType = "",
		public objectSchema = "",
		public onDelete = "",
		public onUpdate = "",
		public attributes: string[] = [],
		public defaultValue?: DefaultValue,
	) { }

	valueKind(): FieldKind {
		if (this.kind === FieldKind.Ref) {
			return this.resolvedKind === FieldKind.Invalid ? FieldKind.Uint64 : this.resolvedKind;
		}
		return this.resolvedKind === FieldKind.Invalid ? this.kind : this.resolvedKind;
	}

	isReference(): boolean {
		return this.kind === FieldKind.Ref && !!this.targetSchema && !!this.targetField;
	}
}

export class RelationDef {
	constructor(
		public name: string,
		public field: string,
		public targetSchema: string,
		public targetField: string,
		public onDelete: string = "restrict",
		public onUpdate: string = "restrict",
	) { }
}

export class Schema {
	private fingerprintCache?: bigint;
	private fieldIndex?: Map<string, number>;

	constructor(public readonly name: string, public readonly fields: Field[], public readonly relations: RelationDef[] = []) { }

	fingerprint(): bigint {
		if (this.fingerprintCache !== undefined) {
			return this.fingerprintCache;
		}
		let hash = FNV_OFFSET;
		const write = (str: string): void => {
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
		for (const rel of this.relations) {
			write("|rel:");
			write(rel.name);
			write(":");
			write(rel.field);
			write("->");
			write(rel.targetSchema);
			write(".");
			write(rel.targetField);
			write("/d:");
			write(rel.onDelete);
			write("/u:");
			write(rel.onUpdate);
		}
		this.fingerprintCache = BigInt.asUintN(64, hash);
		return this.fingerprintCache;
	}

	fieldIndexByName(name: string): number {
		if (!this.fieldIndex) {
			this.fieldIndex = new Map();
			this.fields.forEach((field, idx) => this.fieldIndex!.set(field.name, idx));
		}
		const idx = this.fieldIndex.get(name);
		if (idx === undefined) {
			throw new Error(`scrt: field ${name} not found in schema ${this.name}`);
		}
		return idx;
	}

	tryFieldIndex(name: string): number | undefined {
		if (!this.fieldIndex) {
			this.fieldIndex = new Map();
			this.fields.forEach((field, idx) => this.fieldIndex!.set(field.name, idx));
		}
		return this.fieldIndex.get(name);
	}
}

export interface Argument {
	name: string;
	type: string;
}

export class FunctionDef {
	constructor(
		public name: string,
		public args: Argument[],
		public returnType: string,
		public body: string = ""
	) { }
}

export class QueryDef {
	constructor(
		public name: string,
		public args: Argument[],
		public sql: string = ""
	) { }
}

export class Document {
	constructor(
		public readonly schemas = new Map<string, Schema>(),
		public readonly data = new Map<string, Record<string, unknown>[]>(),
		public readonly functions = new Map<string, FunctionDef>(),
		public readonly queries = new Map<string, QueryDef>(),
		public source?: string,
	) { }

	schema(name: string): Schema | undefined {
		return this.schemas.get(name);
	}

	records(name: string): Record<string, unknown>[] | undefined {
		return this.data.get(name);
	}

	finalize(): void {
	for (const schema of this.schemas.values()) {
			resolveSchemaKinds(this, schema);
			synthesizeInlineRelations(schema);
		}
	}

	load(text: string): void {
		const lines = text.split(/\r?\n/).map((line) => line.trim());
		const schemas = this.schemas;
		const data = this.data;
		const functions = this.functions;
		const queries = this.queries;

		let current: Schema | undefined;
		let currentFunc: FunctionDef | undefined;
		let currentQuery: QueryDef | undefined;
		let awaitingName = false;
		let currentData = "";
		let fieldBlock = false;

		const finishCurrent = (): void => {
			if (current) {
				if (schemas.has(current.name)) {
					throw new Error(`scrt: duplicate schema ${current.name}`);
				}
				schemas.set(current.name, current);
				current = undefined;
			}
			if (currentFunc) {
				if (functions.has(currentFunc.name)) {
					throw new Error(`scrt: duplicate function ${currentFunc.name}`);
				}
				functions.set(currentFunc.name, currentFunc);
				currentFunc = undefined;
			}
			if (currentQuery) {
				if (queries.has(currentQuery.name)) {
					throw new Error(`scrt: duplicate query ${currentQuery.name}`);
				}
				queries.set(currentQuery.name, currentQuery);
				currentQuery = undefined;
			}
		};

		const startSchema = (name: string): void => {
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

			if (currentFunc && !line.startsWith("@")) {
				if (currentFunc.body) currentFunc.body += "\n";
				currentFunc.body += line;
				continue;
			}
			if (currentQuery && !line.startsWith("@")) {
				if (currentQuery.sql) currentQuery.sql += "\n";
				currentQuery.sql += line;
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

			if (line.startsWith("@function")) {
				finishCurrent();
				fieldBlock = false;
				currentData = "";
				let rest = line.slice("@function".length).trim();
				if (rest.startsWith(":")) {
					rest = rest.slice(1).trim();
				}
				currentFunc = parseFunctionHeader(rest);
				continue;
			}
			if (line.startsWith("@query")) {
				finishCurrent();
				fieldBlock = false;
				currentData = "";
				let rest = line.slice("@query".length).trim();
				if (rest.startsWith(":")) {
					rest = rest.slice(1).trim();
				}
				currentQuery = parseQueryHeader(rest);
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
			if (line.startsWith("@relation")) {
				fieldBlock = false;
				currentData = "";
				if (!current) {
					throw new Error("scrt: @relation outside schema block");
				}
				const rel = parseRelation(line.slice("@relation".length).trim());
				current.relations.push(rel);
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
	}
}

export function parseSchema(text: string): Document {
	const doc = new Document();
	doc.load(text);
	doc.finalize();
	return doc;
}

function pushDataRow(store: Map<string, Record<string, unknown>[]>, schemaName: string, row: Record<string, unknown>): void {
	if (!store.has(schemaName)) {
		store.set(schemaName, []);
	}
	store.get(schemaName)!.push(row);
}

function parseField(body: string): Field {
	const [name, typ, attrChunk] = splitFieldParts(body);
	const complexType = parseComplexType(typ);
	const { kind, targetSchema, targetField } = complexType ?? interpretFieldType(typ);
	const field = new Field(name, kind, typ, targetSchema, targetField);
	if (complexType) {
		field.isArray = !!complexType.isArray;
		field.isMap = !!complexType.isMap;
		field.isObject = !!complexType.isObject;
		field.arrayElemType = complexType.arrayElemType ?? "";
		field.mapKeyType = complexType.mapKeyType ?? "";
		field.mapValueType = complexType.mapValueType ?? "";
		field.objectSchema = complexType.objectSchema ?? "";
	}
	if (attrChunk) {
		const attrs = splitFieldAttributes(attrChunk);
		for (const attr of attrs) {
			const lower = attr.toLowerCase();
			switch (true) {
				case lower === "auto_increment" || lower === "autoincrement" || lower === "serial":
					field.autoIncrement = true;
					break;
				case lower === "primary_key" || lower === "pk" || lower === "primary":
					field.primaryKey = true;
					break;
				case lower.startsWith("default="):
				case lower.startsWith("default:"):
					assignFieldDefault(field, extractDefaultLiteral(attr));
					break;
				case hasAttrPrefix(lower, "format"):
					field.format = unquoteAttrValue(extractAttrValue(attr)).toLowerCase();
					break;
				case hasAttrPrefix(lower, "pattern"):
					field.pattern = unquoteAttrValue(extractAttrValue(attr));
					break;
				case hasAttrPrefix(lower, "enum"):
					field.enumValues = parseEnumValues(extractAttrValue(attr));
					break;
				case hasAttrPrefix(lower, "minlength") || hasAttrPrefix(lower, "min_len"):
					field.minLength = Number(extractAttrValue(attr));
					break;
				case hasAttrPrefix(lower, "maxlength") || hasAttrPrefix(lower, "max_len"):
					field.maxLength = Number(extractAttrValue(attr));
					break;
				case hasAttrPrefix(lower, "min"):
					field.minimum = Number(extractAttrValue(attr));
					break;
				case hasAttrPrefix(lower, "max"):
					field.maximum = Number(extractAttrValue(attr));
					break;
				case hasAttrPrefix(lower, "description") || hasAttrPrefix(lower, "desc"):
					field.description = unquoteAttrValue(extractAttrValue(attr));
					break;
				case hasAttrPrefix(lower, "example"):
					field.example = unquoteAttrValue(extractAttrValue(attr));
					break;
				case hasAttrPrefix(lower, "ondelete"):
					field.onDelete = unquoteAttrValue(extractAttrValue(attr)).toLowerCase();
					break;
				case hasAttrPrefix(lower, "onupdate"):
					field.onUpdate = unquoteAttrValue(extractAttrValue(attr)).toLowerCase();
					break;
				default:
					break;
			}
			field.attributes.push(lower);
		}
	}
	return field;
}

function parseRelation(body: string): RelationDef {
	let raw = body.trim();
	if (raw.startsWith(":")) {
		raw = raw.slice(1).trim();
	}
	const parts = raw.split(/\s+/).filter(Boolean);
	if (parts.length < 2) {
		throw new Error(`scrt: invalid relation declaration ${body}`);
	}
	const field = parts[0]!;
	let targetToken = parts[1]!;
	if (targetToken.includes("->")) {
		const [, right = ""] = targetToken.split("->", 2);
		targetToken = right.trim();
	}
	let targetSchema = "";
	let targetField = "";
	if (targetToken.includes(".")) {
		[targetSchema, targetField] = targetToken.split(".", 2);
	} else if (targetToken.includes(":")) {
		[targetSchema, targetField] = targetToken.split(":", 2);
	}
	if (!targetSchema || !targetField) {
		throw new Error(`scrt: invalid relation target ${targetToken}`);
	}
	let onDelete = "restrict";
	let onUpdate = "restrict";
	for (const token of parts.slice(2)) {
		const lower = token.toLowerCase();
		if (hasAttrPrefix(lower, "ondelete")) {
			onDelete = unquoteAttrValue(extractAttrValue(token)).toLowerCase();
		} else if (hasAttrPrefix(lower, "onupdate")) {
			onUpdate = unquoteAttrValue(extractAttrValue(token)).toLowerCase();
		}
	}
	return new RelationDef(field, field, targetSchema.trim(), targetField.trim(), onDelete, onUpdate);
}

function parseComplexType(raw: string): { kind: FieldKind; targetSchema: string; targetField: string; isArray?: boolean; isMap?: boolean; isObject?: boolean; arrayElemType?: string; mapKeyType?: string; mapValueType?: string; objectSchema?: string } | null {
	const typ = raw.trim();
	if (typ.startsWith("[]")) {
		const elem = typ.slice(2).trim();
		if (!elem) {
			throw new Error(`scrt: invalid array type ${raw}`);
		}
		return { kind: FieldKind.String, targetSchema: "", targetField: "", isArray: true, arrayElemType: elem };
	}
	if (typ.toLowerCase().startsWith("map[")) {
		const open = typ.indexOf("[");
		const close = typ.indexOf("]");
		if (open === -1 || close === -1 || close <= open + 1 || close >= typ.length - 1) {
			throw new Error(`scrt: invalid map type ${raw}`);
		}
		const key = typ.slice(open + 1, close).trim();
		const value = typ.slice(close + 1).trim();
		if (!key || !value) {
			throw new Error(`scrt: invalid map type ${raw}`);
		}
		return { kind: FieldKind.String, targetSchema: "", targetField: "", isMap: true, mapKeyType: key, mapValueType: value };
	}
	if (typ.toLowerCase().startsWith("object:")) {
		const schemaName = typ.slice("object:".length).trim();
		if (!schemaName) {
			throw new Error(`scrt: invalid object type ${raw}`);
		}
		return { kind: FieldKind.String, targetSchema: "", targetField: "", isObject: true, objectSchema: schemaName };
	}
	return null;
}

function synthesizeInlineRelations(schema: Schema): void {
	const seen = new Set(schema.relations.map((rel) => rel.field.toLowerCase()));
	for (const field of schema.fields) {
		if (field.kind !== FieldKind.Ref || !field.targetSchema || !field.targetField) {
			continue;
		}
		if (!field.onDelete && !field.onUpdate) {
			continue;
		}
		const key = field.name.toLowerCase();
		if (seen.has(key)) {
			continue;
		}
		schema.relations.push(
			new RelationDef(
				field.name,
				field.name,
				field.targetSchema,
				field.targetField,
				field.onDelete || "restrict",
				field.onUpdate || "restrict",
			),
		);
		seen.add(key);
	}
}

function splitFieldParts(body: string): [string, string, string] {
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

function interpretFieldType(raw: string): { kind: FieldKind; targetSchema: string; targetField: string } {
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
			const parts = raw.split(":");
			if (parts.length !== 3) {
				throw new Error(`scrt: invalid ref declaration ${raw}`);
			}
			const [, schemaName, fieldName] = parts;
			return { kind: FieldKind.Ref, targetSchema: schemaName ?? "", targetField: fieldName ?? "" };
		default:
			if (isIdentifier(raw)) {
				return { kind: FieldKind.Ref, targetSchema: raw, targetField: "" };
			}
			throw new Error(`scrt: unsupported field type ${raw}`);
	}
}

function isIdentifier(value: string): boolean {
	if (!value) {
		return false;
	}
	for (let i = 0; i < value.length; i += 1) {
		const ch = value[i]!;
		const isAlpha = (ch >= "a" && ch <= "z") || (ch >= "A" && ch <= "Z");
		const isNum = ch >= "0" && ch <= "9";
		if (ch === "_" || isAlpha || (i > 0 && isNum)) {
			continue;
		}
		return false;
	}
	return true;
}

function hasAttrPrefix(lowerAttr: string, key: string): boolean {
	return lowerAttr.startsWith(`${key}=`) || lowerAttr.startsWith(`${key}:`);
}

function extractAttrValue(attr: string): string {
	const eq = attr.indexOf("=");
	if (eq >= 0) {
		return attr.slice(eq + 1).trim();
	}
	const colon = attr.indexOf(":");
	if (colon >= 0) {
		return attr.slice(colon + 1).trim();
	}
	return "";
}

function unquoteAttrValue(raw: string): string {
	const trimmed = raw.trim();
	if (!trimmed) {
		return "";
	}
	const first = trimmed[0]!;
	const last = trimmed[trimmed.length - 1]!;
	if ((first === '"' && last === '"') || (first === "'" && last === "'") || (first === "`" && last === "`")) {
		return trimmed.slice(1, -1);
	}
	return trimmed;
}

function parseEnumValues(raw: string): string[] {
	const value = unquoteAttrValue(raw);
	if (!value) {
		return [];
	}
	return value.split("|").map((part) => part.trim()).filter(Boolean);
}

function splitFieldAttributes(attrChunk: string): string[] {
	const attrs: string[] = [];
	let current = "";
	let quote: string | null = null;
	for (const ch of attrChunk) {
		if ((ch === '"' || ch === "'" || ch === "`") && quote === null) {
			quote = ch;
			current += ch;
		} else if (quote && ch === quote) {
			quote = null;
			current += ch;
		} else if (!quote && ch === ',') {
			if (current.trim()) {
				attrs.push(current.trim());
			}
			current = "";
		} else {
			current += ch;
		}
	}
	if (current.trim()) {
		attrs.push(current.trim());
	}
	return attrs;
}

function assignFieldDefault(field: Field, literalRaw: string): void {
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

function extractDefaultLiteral(attr: string): string {
	const sepIdx = attr.indexOf("=") >= 0 ? attr.indexOf("=") : attr.indexOf(":");
	if (sepIdx === -1) {
		return attr;
	}
	return attr.slice(sepIdx + 1);
}

function parseDefaultLiteral(kind: FieldKind, literal: string): DefaultValue {
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

function parseStringLiteral(raw: string): string {
	const trimmed = raw.trim();
	if (!trimmed) {
		return "";
	}
	if (trimmed.startsWith("\"") || trimmed.startsWith("'") || trimmed.startsWith("`")) {
		return trimmed.slice(1, -1);
	}
	return trimmed;
}

function stripQuotes(raw: string): string {
	const trimmed = raw.trim();
	if (!trimmed) {
		return "";
	}
	if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) || (trimmed.startsWith("'") && trimmed.endsWith("'")) || (trimmed.startsWith("`") && trimmed.endsWith("`"))) {
		return trimmed.slice(1, -1);
	}
	return trimmed;
}

function parseBytesLiteral(raw: string): Uint8Array {
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

function parseDataRow(line: string, schema: Schema): Record<string, unknown> {
	const row: Record<string, unknown> = {};
	const tokens = line.split(',');
	let fieldIdx = 0;
	let remaining = countValueTokens(tokens);
	const skipAuto = (): void => {
		while (fieldIdx < schema.fields.length && schema.fields[fieldIdx]!.autoIncrement) {
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
				row[schema.fields[index]!.name] = value;
				fieldIdx = Math.max(fieldIdx, index + 1);
			}
			continue;
		}
		skipAuto();
		if (fieldIdx >= schema.fields.length) {
			throw new Error("scrt: too many values in row");
		}
		const field = schema.fields[fieldIdx]!;
		row[field.name] = parseValue(trimmed, field);
		fieldIdx += 1;
		remaining -= 1;
	}
	return row;
}

function countValueTokens(tokens: string[]): number {
	return tokens.reduce((acc, token) => {
		const trimmed = token.trim();
		if (!trimmed || trimmed.startsWith("@")) {
			return acc;
		}
		return acc + 1;
	}, 0);
}

function countNonAuto(fields: Field[], start: number): number {
	let count = 0;
	for (let i = start; i < fields.length; i += 1) {
		if (!fields[i]!.autoIncrement) {
			count += 1;
		}
	}
	return count;
}

function applyExplicitAssignment(schema: Schema, expr: string): { index: number; value: unknown } {
	const [fieldToken, rawValue] = expr.split("=", 2);
	if (!rawValue) {
		throw new Error(`scrt: invalid assignment ${expr}`);
	}
	const normalized = normalizeAssignmentTarget(fieldToken);
	const idx = schema.tryFieldIndex(normalized);
	if (idx === undefined) {
		throw new Error(`scrt: field ${normalized} not found`);
	}
	const field = schema.fields[idx]!;
	return { index: idx, value: parseValue(rawValue.trim(), field) };
}

function normalizeAssignmentTarget(token: string): string {
	const trimmed = token.trim();
	const parts = trimmed.split(":");
	if (parts.length >= 2) {
		return parts[1]!;
	}
	return parts[0]!;
}

function parseValue(raw: string, field: Field): unknown {
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

function resolveSchemaKinds(doc: Document, schema: Schema): void {
	schema.fields.forEach((field, idx) => resolveFieldKind(doc, schema, idx, new Set()));
}

function resolveFieldKind(doc: Document, schema: Schema, idx: number, stack: Set<string>): FieldKind {
	const field = schema.fields[idx]!;
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
	if (!field.targetField) {
		field.targetField = inferReferenceTargetField(targetSchema);
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

function inferReferenceTargetField(target: Schema): string {
	for (const field of target.fields) {
		if (field.primaryKey) {
			return field.name;
		}
	}
	for (const field of target.fields) {
		if (field.name.toLowerCase() === "id") {
			return field.name;
		}
	}
	throw new Error(`scrt: cannot infer reference field for schema ${target.name}; add a primary key, an ID field, or use explicit ref:${target.name}:<Field>`);
}

function bytesToBase64(bytes: Uint8Array): string {
	if (typeof Buffer !== "undefined") {
		return Buffer.from(bytes).toString("base64");
	}
	let binary = "";
	for (let i = 0; i < bytes.length; i += 1) {
		binary += String.fromCharCode(bytes[i]!);
	}
	if (typeof btoa === "function") {
		return btoa(binary);
	}
	throw new Error("scrt: base64 encoding unavailable in this environment");
}

function parseFunctionHeader(line: string): FunctionDef {
	const parenOpen = line.indexOf("(");
	const parenClose = line.lastIndexOf(")");
	if (parenOpen === -1 || parenClose === -1 || parenClose < parenOpen) {
		throw new Error("scrt: invalid function signature");
	}
	const name = line.slice(0, parenOpen).trim();
	const argsStr = line.slice(parenOpen + 1, parenClose);
	const ret = line.slice(parenClose + 1).trim();
	const args = parseArgs(argsStr);
	return new FunctionDef(name, args, ret);
}

function parseQueryHeader(line: string): QueryDef {
	const parenOpen = line.indexOf("(");
	if (parenOpen === -1) {
		return new QueryDef(line.trim(), []);
	}
	const parenClose = line.lastIndexOf(")");
	if (parenClose === -1 || parenClose < parenOpen) {
		throw new Error("scrt: invalid query signature");
	}
	const name = line.slice(0, parenOpen).trim();
	const argsStr = line.slice(parenOpen + 1, parenClose);
	const args = parseArgs(argsStr);
	return new QueryDef(name, args);
}

function parseArgs(str: string): Argument[] {
	const s = str.trim();
	if (!s) return [];
	const parts = s.split(",");
	const args: Argument[] = [];
	for (const p of parts) {
		const trimmed = p.trim();
		const idx = trimmed.lastIndexOf(" ");
		if (idx === -1) {
			throw new Error(`scrt: invalid argument format: ${p}`);
		}
		const name = trimmed.slice(0, idx).trim();
		const typ = trimmed.slice(idx + 1).trim();
		args.push({ name, type: typ });
	}
	return args;
}
