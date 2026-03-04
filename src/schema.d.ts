export declare enum FieldKind {
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
    Duration = 12
}
export declare class DefaultValue {
    kind: FieldKind;
    boolValue?: boolean | undefined;
    intValue?: bigint | undefined;
    uintValue?: bigint | undefined;
    floatValue?: number | undefined;
    stringValue?: string | undefined;
    bytesValue?: Uint8Array | undefined;
    constructor(kind: FieldKind, boolValue?: boolean | undefined, intValue?: bigint | undefined, uintValue?: bigint | undefined, floatValue?: number | undefined, stringValue?: string | undefined, bytesValue?: Uint8Array | undefined);
    hashKey(): string;
}
export declare class Field {
    readonly name: string;
    readonly kind: FieldKind;
    readonly rawType: string;
    targetSchema: string;
    targetField: string;
    autoIncrement: boolean;
    primaryKey: boolean;
    format: string;
    pattern: string;
    enumValues: string[];
    minLength?: number | undefined;
    maxLength?: number | undefined;
    minimum?: number | undefined;
    maximum?: number | undefined;
    description: string;
    example: string;
    isArray: boolean;
    isMap: boolean;
    isObject: boolean;
    arrayElemType: string;
    mapKeyType: string;
    mapValueType: string;
    objectSchema: string;
    onDelete: string;
    onUpdate: string;
    attributes: string[];
    defaultValue?: DefaultValue | undefined;
    resolvedKind: FieldKind;
    pendingDefault: string;
    constructor(name: string, kind: FieldKind, rawType: string, targetSchema?: string, targetField?: string, autoIncrement?: boolean, primaryKey?: boolean, format?: string, pattern?: string, enumValues?: string[], minLength?: number | undefined, maxLength?: number | undefined, minimum?: number | undefined, maximum?: number | undefined, description?: string, example?: string, isArray?: boolean, isMap?: boolean, isObject?: boolean, arrayElemType?: string, mapKeyType?: string, mapValueType?: string, objectSchema?: string, onDelete?: string, onUpdate?: string, attributes?: string[], defaultValue?: DefaultValue | undefined);
    valueKind(): FieldKind;
    isReference(): boolean;
}
export declare class RelationDef {
    name: string;
    field: string;
    targetSchema: string;
    targetField: string;
    onDelete: string;
    onUpdate: string;
    constructor(name: string, field: string, targetSchema: string, targetField: string, onDelete?: string, onUpdate?: string);
}
export declare class Schema {
    readonly name: string;
    readonly fields: Field[];
    readonly relations: RelationDef[];
    private fingerprintCache?;
    private fieldIndex?;
    constructor(name: string, fields: Field[], relations?: RelationDef[]);
    fingerprint(): bigint;
    fieldIndexByName(name: string): number;
    tryFieldIndex(name: string): number | undefined;
}
export interface Argument {
    name: string;
    type: string;
}
export declare class FunctionDef {
    name: string;
    args: Argument[];
    returnType: string;
    body: string;
    constructor(name: string, args: Argument[], returnType: string, body?: string);
}
export declare class QueryDef {
    name: string;
    args: Argument[];
    sql: string;
    constructor(name: string, args: Argument[], sql?: string);
}
export declare class Document {
    readonly schemas: Map<string, Schema>;
    readonly data: Map<string, Record<string, unknown>[]>;
    readonly functions: Map<string, FunctionDef>;
    readonly queries: Map<string, QueryDef>;
    source?: string | undefined;
    constructor(schemas?: Map<string, Schema>, data?: Map<string, Record<string, unknown>[]>, functions?: Map<string, FunctionDef>, queries?: Map<string, QueryDef>, source?: string | undefined);
    schema(name: string): Schema | undefined;
    records(name: string): Record<string, unknown>[] | undefined;
    finalize(): void;
    load(text: string): void;
}
export declare function parseSchema(text: string): Document;
