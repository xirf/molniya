import { EngineError, requireThat } from "./errors.ts";
import {
  compare,
  indexOf,
  type DataType,
  type Field,
  type Scalar,
  type Schema,
  validate,
} from "./schema.ts";

export type Op =
  | "eq"
  | "ne"
  | "lt"
  | "le"
  | "gt"
  | "ge"
  | "add"
  | "sub"
  | "mul"
  | "div"
  | "and"
  | "or"
  | "not"
  | "isNull"
  | "coalesce"
  | "lower"
  | "upper"
  | "length"
  | "contains";
export type Expr = Readonly<
  | { kind: "column"; name: string }
  | { kind: "literal"; value: Scalar; type: DataType }
  | { kind: "call"; op: Op; args: readonly Expr[] }
>;
export const col = (name: string): Expr => Object.freeze({ kind: "column", name });
export function lit(value: Scalar, type?: DataType): Expr {
  const inferred =
    type ??
    (typeof value === "string"
      ? "utf8"
      : typeof value === "boolean"
        ? "bool"
        : typeof value === "bigint"
          ? "i64"
          : "f64");
  validate(value, { name: "literal", type: inferred, nullable: true });
  return Object.freeze({ kind: "literal", value, type: inferred });
}
export const call = (op: Op, ...args: Expr[]): Expr =>
  Object.freeze({ kind: "call", op, args: Object.freeze(args.slice()) });
export const eq = (a: Expr, b: Expr): Expr => call("eq", a, b);
export const ne = (a: Expr, b: Expr): Expr => call("ne", a, b);
export const lt = (a: Expr, b: Expr): Expr => call("lt", a, b);
export const le = (a: Expr, b: Expr): Expr => call("le", a, b);
export const gt = (a: Expr, b: Expr): Expr => call("gt", a, b);
export const ge = (a: Expr, b: Expr): Expr => call("ge", a, b);
export const add = (a: Expr, b: Expr): Expr => call("add", a, b);
export const sub = (a: Expr, b: Expr): Expr => call("sub", a, b);
export const mul = (a: Expr, b: Expr): Expr => call("mul", a, b);
export const div = (a: Expr, b: Expr): Expr => call("div", a, b);
export const and = (a: Expr, b: Expr): Expr => call("and", a, b);
export const or = (a: Expr, b: Expr): Expr => call("or", a, b);
export const not = (a: Expr): Expr => call("not", a);
export const isNull = (a: Expr): Expr => call("isNull", a);
export const coalesce = (a: Expr, b: Expr): Expr => call("coalesce", a, b);
export const lower = (a: Expr): Expr => call("lower", a);
export const upper = (a: Expr): Expr => call("upper", a);
export const length = (a: Expr): Expr => call("length", a);
export const contains = (a: Expr, b: Expr): Expr => call("contains", a, b);

export interface BoundExpr {
  readonly type: DataType;
  readonly nullable: boolean;
  readonly evaluate: (get: (column: number) => Scalar) => Scalar;
}
const numeric = (type: DataType) => type === "i32" || type === "f64" || type === "i64";
const compatible = (a: DataType, b: DataType) =>
  a === b || (a === "i32" && b === "f64") || (a === "f64" && b === "i32");

export function bind(expr: Expr, fields: Schema): BoundExpr {
  if (expr.kind === "column") {
    const index = indexOf(fields, expr.name);
    return { ...fields[index]!, evaluate: (get) => get(index) };
  }
  if (expr.kind === "literal") {
    validate(expr.value, { name: "literal", type: expr.type, nullable: true });
    return { type: expr.type, nullable: expr.value === null, evaluate: () => expr.value };
  }
  const unary = ["not", "isNull", "lower", "upper", "length"].includes(expr.op);
  requireThat(expr.args.length === (unary ? 1 : 2), "PLAN", `Wrong arity for ${expr.op}`);
  const args = expr.args.map((arg) => bind(arg, fields));
  const a = args[0]!;
  const b = args[1];
  let type: DataType;
  let nullable = args.some((arg) => arg.nullable);
  switch (expr.op) {
    case "and":
    case "or":
    case "not":
      requireThat(
        args.every((arg) => arg.type === "bool"),
        "TYPE",
        `${expr.op} requires Boolean operands`,
      );
      type = "bool";
      break;
    case "isNull":
      type = "bool";
      nullable = false;
      break;
    case "lower":
    case "upper":
    case "length":
    case "contains":
      requireThat(
        args.every((arg) => arg.type === "utf8"),
        "TYPE",
        `${expr.op} requires strings`,
      );
      type = expr.op === "length" ? "f64" : expr.op === "contains" ? "bool" : "utf8";
      break;
    case "coalesce":
      requireThat(compatible(a.type, b!.type), "TYPE", "coalesce types differ");
      type = a.type === b!.type ? a.type : "f64";
      nullable = a.nullable && b!.nullable;
      break;
    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge":
      requireThat(compatible(a.type, b!.type), "TYPE", "Comparison types differ");
      type = "bool";
      break;
    case "add":
    case "sub":
    case "mul":
    case "div":
      requireThat(
        args.every((arg) => numeric(arg.type)) && compatible(a.type, b!.type),
        "TYPE",
        "Arithmetic types differ or are not numeric",
      );
      type = a.type === "i64" ? "i64" : "f64";
      nullable ||= expr.op === "div";
      break;
    default:
      throw new EngineError("PLAN", `Unknown expression operation: ${expr.op}`);
  }
  const field: Field = { name: expr.op, type, nullable };
  return {
    type,
    nullable,
    evaluate(get) {
      const x = a.evaluate(get);
      if (expr.op === "isNull") return x === null;
      if (expr.op === "coalesce") return x === null ? b!.evaluate(get) : x;
      if (expr.op === "and" && x === false) return false;
      if (expr.op === "or" && x === true) return true;
      const y = b?.evaluate(get) ?? null;
      if (expr.op === "and") return y === false ? false : x === null || y === null ? null : true;
      if (expr.op === "or") return y === true ? true : x === null || y === null ? null : false;
      if (x === null || (!unary && y === null)) return null;
      let result: Scalar;
      switch (expr.op) {
        case "not":
          return !x;
        case "lower":
          return (x as string).toLowerCase();
        case "upper":
          return (x as string).toUpperCase();
        case "length":
          return Array.from(x as string).length;
        case "contains":
          return (x as string).includes(y as string);
        case "eq":
          return compare(x, y) === 0;
        case "ne":
          return compare(x, y) !== 0;
        case "lt":
          return compare(x, y) < 0;
        case "le":
          return compare(x, y) <= 0;
        case "gt":
          return compare(x, y) > 0;
        case "ge":
          return compare(x, y) >= 0;
        default: {
          if (expr.op === "div" && (y === 0 || y === 0n)) return null;
          if (typeof x === "bigint") {
            const right = y as bigint;
            result =
              expr.op === "add"
                ? x + right
                : expr.op === "sub"
                  ? x - right
                  : expr.op === "mul"
                    ? x * right
                    : x / right;
          } else {
            const left = x as number;
            const right = y as number;
            result =
              expr.op === "add"
                ? left + right
                : expr.op === "sub"
                  ? left - right
                  : expr.op === "mul"
                    ? left * right
                    : left / right;
          }
        }
      }
      validate(result, field);
      return result;
    },
  };
}

export function references(expr: Expr): readonly string[] {
  return expr.kind === "column"
    ? [expr.name]
    : expr.kind === "call"
      ? [...new Set(expr.args.flatMap((arg) => references(arg)))]
      : [];
}

/** Copy caller-supplied ASTs so a query can never be changed after construction. */
export function snapshotExpr(expr: Expr): Expr {
  return expr.kind === "column"
    ? col(expr.name)
    : expr.kind === "literal"
      ? lit(expr.value, expr.type)
      : call(expr.op, ...expr.args.map(snapshotExpr));
}
