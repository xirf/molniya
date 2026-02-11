/**
 * Project operator.
 *
 * Selects and reorders columns.
 * Can also rename columns.
 */

import { Chunk } from "../buffer/chunk.ts";
import type { ColumnBuffer } from "../buffer/column-buffer.ts";
import { EnhancedChunk } from "../buffer/chunk-ext.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import {
	createSchema,
	getColumnIndex,
	type Schema,
	type SchemaSpec,
} from "../types/schema.ts";
import {
	type OperatorResult,
	opEmpty,
	opResult,
	SimpleOperator,
} from "./operator.ts";

/** Column projection specification */
export interface ProjectSpec {
	/** Source column name */
	source: string;
	/** Target column name (defaults to source if not specified) */
	target?: string;
}

/**
 * Project operator that selects and optionally renames columns.
 */
export class ProjectOperator extends SimpleOperator {
	readonly name = "Project";
	readonly outputSchema: Schema;

	/** Mapping from input column index to output column index */
	private readonly columnMapping: readonly number[];

	private constructor(
		_inputSchema: Schema,
		outputSchema: Schema,
		columnMapping: number[],
	) {
		super();
		this.outputSchema = outputSchema;
		this.columnMapping = columnMapping;
	}

	/**
	 * Create a project operator from column names.
	 * Simple form: just select columns in order.
	 */
	static create(
		inputSchema: Schema,
		columns: string[],
	): Result<ProjectOperator> {
		const specs = columns.map((name) => ({ source: name }));
		return ProjectOperator.createWithSpecs(inputSchema, specs);
	}

	/**
	 * Create a project operator with rename support.
	 */
	static createWithSpecs(
		inputSchema: Schema,
		specs: ProjectSpec[],
	): Result<ProjectOperator> {
		if (specs.length === 0) {
			return err(ErrorCode.EmptySchema);
		}

		const columnMapping: number[] = [];
		const outputSpec: SchemaSpec = {};
		const seen = new Set<string>();

		for (const spec of specs) {
			// Get source column index
			const indexResult = getColumnIndex(inputSchema, spec.source);
			if (indexResult.error !== ErrorCode.None) {
				return err(ErrorCode.UnknownColumn);
			}

			const targetName = spec.target ?? spec.source;

			// Check for duplicate output names
			if (seen.has(targetName)) {
				return err(ErrorCode.DuplicateColumn);
			}
			seen.add(targetName);

			columnMapping.push(indexResult.value);
			// biome-ignore lint/style/noNonNullAssertion: Checked via result validation
			outputSpec[targetName] = inputSchema.columns[indexResult.value]!.dtype;
		}

		const outputSchemaResult = createSchema(outputSpec);
		if (outputSchemaResult.error !== ErrorCode.None) {
			return err(outputSchemaResult.error);
		}

		return ok(
			new ProjectOperator(inputSchema, outputSchemaResult.value, columnMapping),
		);
	}

	process(chunk: Chunk): Result<OperatorResult> {
		if (chunk.rowCount === 0) {
			return ok(opEmpty());
		}

		// Create new column array with selected columns in new order
		const newColumns: ColumnBuffer[] = [];

		for (const sourceIdx of this.columnMapping) {
			const col = chunk.getColumn(sourceIdx);
			if (col === undefined) {
				return err(ErrorCode.InvalidOffset);
			}
			newColumns.push(col);
		}

		// Create new chunk with projected columns
		const projectedChunk = new Chunk(
			this.outputSchema,
			newColumns,
			chunk.dictionary,
		);

		// Preserve and remap enhanced chunk metadata for StringView support
		const enhancedChunk = (chunk as any)._enhancedChunk;
		if (enhancedChunk) {
			// Remap StringView columns to new indices
			const remappedStringViews = new Map<number, any>();
			for (let newIdx = 0; newIdx < this.columnMapping.length; newIdx++) {
				const oldIdx = this.columnMapping[newIdx];
				const adapter = enhancedChunk.getColumnAdapter(oldIdx);
				// Check if this column has a StringView adapter
				if (adapter && adapter.isStringView()) {
					const storage = adapter.getStorage();
					remappedStringViews.set(newIdx, storage);
				}
			}
			
			// Create new EnhancedChunk with remapped columns
			if (remappedStringViews.size > 0) {
				const newEnhanced = new EnhancedChunk(
					this.outputSchema,
					newColumns,
					chunk.dictionary,
					remappedStringViews
				);
				(projectedChunk as any)._enhancedChunk = newEnhanced;
			}
		}

		// Preserve selection if any
		const existingSelection = chunk.getSelection();
		if (existingSelection !== null) {
			projectedChunk.applySelection(existingSelection, chunk.rowCount);
		}

		return ok(opResult(projectedChunk));
	}
}

/**
 * Create a project operator selecting specific columns.
 */
export function project(
	inputSchema: Schema,
	columns: string[],
): Result<ProjectOperator> {
	return ProjectOperator.create(inputSchema, columns);
}

/**
 * Create a project operator with rename support.
 */
export function projectWithRename(
	inputSchema: Schema,
	specs: ProjectSpec[],
): Result<ProjectOperator> {
	return ProjectOperator.createWithSpecs(inputSchema, specs);
}
