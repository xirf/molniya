/**
 * High-level factory functions for creating DataFrames
 * Provides convenient APIs that hide low-level complexity
 */

import { enableNullTracking, setColumnValue } from '../core/column';
import { internString } from '../memory/dictionary';
import { DType } from '../types/dtypes';
import { type Result, err, ok } from '../types/result';
import { setNull } from '../utils/nulls';
import { type DataFrame, addColumn, createDataFrame, getColumn } from './dataframe';

/**
 * Column specification for factory function
 */
export interface ColumnSpec {
  /** Array of values for this column */
  data: Array<number | bigint | string | boolean | null | undefined>;
  /** Data type of the column */
  dtype: DType;
}

/**
 * Create a DataFrame from column specifications
 * This is a high-level convenience function that handles null tracking,
 * string interning, and type conversions automatically.
 *
 * @param columns - Object mapping column names to their specifications
 * @returns Result with DataFrame or error
 *
 * @example
 * ```typescript
 * const df = from({
 *   id: { data: [1, 2, 3], dtype: DType.Int32 },
 *   name: { data: ['Alice', 'Bob', null], dtype: DType.String },
 *   price: { data: [10.5, 20.0, 30.5], dtype: DType.Float64 },
 * });
 * ```
 */
export function from(columns: Record<string, ColumnSpec>): Result<DataFrame, Error> {
  const df = createDataFrame();

  // Handle empty input
  const columnEntries = Object.entries(columns);
  if (columnEntries.length === 0) {
    return ok(df);
  }

  // Get length from first column
  const firstCol = columnEntries[0]![1];
  const length = firstCol.data.length;

  // Validate all columns have same length
  for (const [colName, spec] of columnEntries) {
    if (spec.data.length !== length) {
      return err(
        new Error(
          `Column '${colName}' has length ${spec.data.length}, expected ${length} to match first column`,
        ),
      );
    }
  }

  // Add all columns and populate data
  for (const [colName, spec] of columnEntries) {
    const { data: values, dtype } = spec;

    // Add column to DataFrame
    const addResult = addColumn(df, colName, dtype, length);
    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }

    // Get the column we just added
    // @ts-ignore - We just added it
    const colResult = getColumn(df, colName);
    if (!colResult.ok) {
      return err(new Error(colResult.error));
    }

    const col = colResult.data;

    // Check if we have any null values
    const hasNulls = values.some((v) => v === null || v === undefined);
    if (hasNulls) {
      enableNullTracking(col);
    }

    // Populate column data
    for (let i = 0; i < length; i++) {
      const value = values[i];

      // Handle null/undefined
      if (value === null || value === undefined) {
        if (col.nullBitmap) {
          setNull(col.nullBitmap, i);
        }
        continue;
      }

      // Handle different data types
      try {
        if (dtype === DType.String) {
          if (typeof value !== 'string') {
            return err(
              new Error(
                `Column '${colName}' expects String type, got ${typeof value} at index ${i}`,
              ),
            );
          }
          const id = internString(df.dictionary!, value);
          setColumnValue(col, i, id);
        } else if (dtype === DType.Bool) {
          if (typeof value !== 'boolean') {
            return err(
              new Error(`Column '${colName}' expects Bool type, got ${typeof value} at index ${i}`),
            );
          }
          setColumnValue(col, i, value ? 1 : 0);
        } else if (dtype === DType.Int32) {
          if (typeof value !== 'number' && typeof value !== 'bigint') {
            return err(
              new Error(
                `Column '${colName}' expects Int32 type, got ${typeof value} at index ${i}`,
              ),
            );
          }
          const numValue = typeof value === 'bigint' ? Number(value) : value;
          if (!Number.isInteger(numValue)) {
            return err(
              new Error(`Column '${colName}' expects integer value, got ${value} at index ${i}`),
            );
          }
          setColumnValue(col, i, numValue);
        } else if (dtype === DType.Float64) {
          if (typeof value !== 'number' && typeof value !== 'bigint') {
            return err(
              new Error(
                `Column '${colName}' expects Float64 type, got ${typeof value} at index ${i}`,
              ),
            );
          }
          const numValue = typeof value === 'bigint' ? Number(value) : value;
          setColumnValue(col, i, numValue);
        } else if (dtype === DType.DateTime || dtype === DType.Date) {
          if (typeof value !== 'bigint' && typeof value !== 'number') {
            return err(
              new Error(
                `Column '${colName}' expects DateTime/Date type (bigint/number), got ${typeof value} at index ${i}`,
              ),
            );
          }
          setColumnValue(col, i, value);
        } else {
          return err(new Error(`Unsupported dtype: ${dtype}`));
        }
      } catch (error) {
        return err(
          new Error(
            `Failed to set value for column '${colName}' at index ${i}: ${error instanceof Error ? error.message : String(error)}`,
          ),
        );
      }
    }
  }

  return ok(df);
}

/**
 * Maps JavaScript types to DType string literals
 */
type InferDType<T> = T extends string
  ? 'string'
  : T extends boolean
    ? 'bool'
    : T extends number
      ? 'float64' | 'int32' // Default to float64 for numbers
      : T extends bigint
        ? 'datetime'
        : never;

/**
 * Infer schema type from array inputs
 * Maps to DType literals instead of TypeScript types
 */
export type InferSchemaType<T extends Record<string, Array<unknown>>> = {
  [K in keyof T]: T[K] extends Array<infer U>
    ? U extends number
      ? 'float64' // Always default to float64 for numbers
      : U extends string
        ? 'string'
        : U extends boolean
          ? 'bool'
          : U extends bigint
            ? 'int32'
            : never
    : never;
};

/**
 * Helper to infer the row type from array inputs (legacy)
 */
export type InferSchemaFromArrays<T extends Record<string, Array<unknown>>> = {
  [K in keyof T]: T[K] extends Array<infer U> ? U : never;
};

/**
 * Create a DataFrame from arrays with automatic type inference
 * Infers data types from the first non-null value in each column
 * Numbers default to Float64 unless all values are integers
 *
 * @param columns - Object mapping column names to value arrays
 * @returns DataFrame with inferred schema types
 * @throws Error if type inference fails or arrays have mismatched lengths
 *
 * @example
 * ```typescript
 * const df = fromArrays({
 *   id: [1, 2, 3],
 *   name: ['Alice', 'Bob', 'Charlie'],
 *   price: [10.5, 20.0, 30.5],
 *   active: [true, false, true],
 * });
 * // Type: DataFrame<InferSchemaType<{ id: "int32", name: "string", price: "float64", active: "bool" }>>
 * ```
 */
export function fromArrays<
  T extends Record<string, Array<number | bigint | string | boolean | null | undefined>>,
>(columns: T): DataFrame<InferSchemaType<T>> {
  const specs: Record<string, ColumnSpec> = {};

  for (const [colName, values] of Object.entries(columns)) {
    // Find first non-null value to infer type
    const firstValue = values.find((v) => v !== null && v !== undefined);

    if (firstValue === undefined) {
      // All nulls - default to Int32
      specs[colName] = { data: values, dtype: DType.Int32 };
      continue;
    }

    // Infer dtype from first value
    let dtype: DType;
    if (typeof firstValue === 'boolean') {
      dtype = DType.Bool;
    } else if (typeof firstValue === 'string') {
      dtype = DType.String;
    } else if (typeof firstValue === 'bigint') {
      dtype = DType.Int32;
    } else if (typeof firstValue === 'number') {
      // Check if all numbers are integers
      const allIntegers = values.every(
        (v) => v === null || v === undefined || (typeof v === 'number' && Number.isInteger(v)),
      );
      dtype = allIntegers ? DType.Int32 : DType.Float64;
    } else {
      throw new Error(`Cannot infer type for column '${colName}' from value: ${firstValue}`);
    }

    specs[colName] = { data: values, dtype };
  }

  // We cast to the inferred type because at runtime we validated and created the DF
  const result = from(specs);
  if (!result.ok) {
    throw result.error;
  }
  return result.data as DataFrame<InferSchemaType<T>>;
}
