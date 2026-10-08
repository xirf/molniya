import { requireThat } from "./errors.ts";

export type DataType = "i32" | "i64" | "f64" | "bool" | "utf8";
export type Scalar = number | bigint | boolean | string | null;
export interface Field {
  readonly name: string;
  readonly type: DataType;
  readonly nullable: boolean;
}
export type Schema = readonly Field[];
export type SchemaSpec = Readonly<
  Record<string, DataType | { readonly type: DataType; readonly nullable?: boolean }>
>;
export type Row = Readonly<Record<string, Scalar>>;
const types = new Set(["i32", "i64", "f64", "bool", "utf8"]);

export function schema(spec: SchemaSpec | Schema): Schema {
  requireThat(
    spec !== null && typeof spec === "object",
    "SCHEMA",
    "Schema must be an object or field array",
  );
  const fields = Array.isArray(spec)
    ? spec
    : Object.entries(spec).map(([name, value]) => ({
        name,
        type: typeof value === "string" ? value : value?.type,
        nullable: typeof value === "string" ? false : (value?.nullable ?? false),
      }));
  const names = new Set<string>();
  requireThat(fields.length > 0, "SCHEMA", "A schema must contain at least one column");
  return Object.freeze(
    fields.map((field: Field) => {
      requireThat(
        field !== null && typeof field === "object",
        "SCHEMA",
        "Invalid field definition",
      );
      requireThat(
        typeof field.name === "string" && field.name.length > 0 && !names.has(field.name),
        "SCHEMA",
        `Invalid or duplicate column: ${field.name}`,
      );
      requireThat(
        types.has(field.type) && typeof field.nullable === "boolean",
        "SCHEMA",
        `Invalid type for ${field.name}`,
      );
      names.add(field.name);
      return Object.freeze({ name: field.name, type: field.type, nullable: field.nullable });
    }),
  );
}

export function indexOf(fields: Schema, name: string): number {
  const index = fields.findIndex((field) => field.name === name);
  requireThat(index >= 0, "SCHEMA", `Unknown column: ${name}`);
  return index;
}

export function validate(value: unknown, field: Field): asserts value is Scalar {
  if (value === null) {
    requireThat(field.nullable, "TYPE", `${field.name} is not nullable`);
    return;
  }
  const valid =
    field.type === "utf8"
      ? typeof value === "string" && value.isWellFormed()
      : field.type === "bool"
        ? typeof value === "boolean"
        : field.type === "i64"
          ? typeof value === "bigint" && BigInt.asIntN(64, value) === value
          : field.type === "i32"
            ? typeof value === "number" &&
              Number.isInteger(value) &&
              value >= -2147483648 &&
              value <= 2147483647
            : typeof value === "number" && Number.isFinite(value);
  requireThat(valid, "TYPE", `Invalid ${field.type} value for ${field.name}`);
}

export function valuesOf(fields: Schema, row: Row): Scalar[] {
  requireThat(
    row !== null && typeof row === "object" && !Array.isArray(row),
    "SCHEMA",
    "Row must be an object",
  );
  requireThat(
    Object.keys(row).length === fields.length,
    "SCHEMA",
    "Row columns must match schema exactly",
  );
  return fields.map((field) => {
    const value = Object.hasOwn(row, field.name) ? row[field.name] : undefined;
    validate(value, field);
    return value;
  });
}

export function rowOf(fields: Schema, values: readonly Scalar[]): Row {
  return Object.fromEntries(fields.map((field, i) => [field.name, values[i]!])) as Row;
}

export function keyOf(values: readonly Scalar[]): string {
  return JSON.stringify(
    values.map((value) =>
      typeof value === "bigint" ? ["i64", value.toString()] : [typeof value, value],
    ),
  );
}

export function compare(a: Scalar, b: Scalar): number {
  if (a === b) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  if (typeof a === "string" && typeof b === "string") {
    let i = 0;
    let j = 0;
    while (i < a.length && j < b.length) {
      const x = a.codePointAt(i)!;
      const y = b.codePointAt(j)!;
      if (x !== y) return x < y ? -1 : 1;
      i += x > 0xffff ? 2 : 1;
      j += y > 0xffff ? 2 : 1;
    }
    return i === a.length ? -1 : 1;
  }
  return a < b ? -1 : 1;
}

/** A conservative accounting model for JS row staging; not a measurement of RSS. */
export function rowBytes(values: readonly Scalar[]): number {
  return (
    32 +
    values.reduce<number>(
      (bytes, value) => bytes + (typeof value === "string" ? 16 + value.length * 2 : 16),
      0,
    )
  );
}
