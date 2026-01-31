/* JOIN METHODS
/*-----------------------------------------------------
/* Combine DataFrames by key columns
/* ==================================================== */

import { crossProduct, hashJoin, JoinType } from "../ops/index.ts";
import { ErrorCode } from "../types/error.ts";
import type { DataFrame } from "./core.ts";

export function addJoinMethods(df: typeof DataFrame.prototype) {
	// ... helper for generic hash join wrapper ...
	const executeHashJoin = async (
		// biome-ignore lint/suspicious/noExplicitAny: generic
		left: DataFrame<any>,
		// biome-ignore lint/suspicious/noExplicitAny: generic
		right: DataFrame<any>,
		leftOn: string,
		rightOn: string,
		joinType: JoinType,
		suffix?: string,
	) => {
		const collectedLeft = await left.collect();
		const collectedRight = await right.collect();

		const result = hashJoin(
			collectedLeft.source as import("../buffer/chunk.ts").Chunk[],
			collectedLeft._schema,
			collectedRight.source as import("../buffer/chunk.ts").Chunk[],
			collectedRight._schema,
			{ leftKey: leftOn, rightKey: rightOn, joinType, suffix },
		);

		if (result.error !== ErrorCode.None) {
			throw new Error(`Join error (${joinType}): ${result.error}`);
		}

		return (left.constructor as typeof DataFrame).fromChunks(
			result.value.chunks,
			result.value.schema,
			collectedLeft._dictionary,
		);
	};

	df.innerJoin = function (other, leftOn, rightOn, suffix) {
		return executeHashJoin(
			this,
			other,
			leftOn as string,
			(rightOn ?? leftOn) as string,
			JoinType.Inner,
			suffix,
		);
	};

	df.leftJoin = function (other, leftOn, rightOn, suffix) {
		return executeHashJoin(
			this,
			other,
			leftOn as string,
			(rightOn ?? leftOn) as string,
			JoinType.Left,
			suffix,
		);
	};

	df.semiJoin = function (other, on) {
		const key = Array.isArray(on) ? on[0] : on;
		return executeHashJoin(
			this,
			other,
			key as string,
			key as string,
			JoinType.Semi,
		);
	};

	df.antiJoin = function (other, on) {
		const key = Array.isArray(on) ? on[0] : on;
		return executeHashJoin(
			this,
			other,
			key as string,
			key as string,
			JoinType.Anti,
		);
	};

	df.crossJoin = async function (other, suffix) {
		const collectedLeft = await this.collect();
		const collectedRight = await other.collect();

		const result = crossProduct(
			collectedLeft.source as import("../buffer/chunk.ts").Chunk[],
			collectedLeft._schema,
			collectedRight.source as import("../buffer/chunk.ts").Chunk[],
			collectedRight._schema,
			suffix,
		);

		if (result.error !== ErrorCode.None) {
			throw new Error(`CrossJoin error: ${result.error}`);
		}

		return (this.constructor as typeof DataFrame).fromChunks(
			result.value.chunks,
			result.value.schema,
			collectedLeft._dictionary,
		);
	};

	df.join = function (other, leftOn, rightOn, how, suffix) {
		if (how === "cross") return this.crossJoin(other, suffix);
		if (how === "anti") return this.antiJoin(other, leftOn);
		if (how === "semi") return this.semiJoin(other, leftOn);

		const actualRightOn = rightOn ?? leftOn;
		return how === "inner"
			? // @ts-expect-error - we know how is either "inner" or "left"
				this.innerJoin(other, leftOn, actualRightOn, suffix)
			: // @ts-expect-error - we know how is either "inner" or "left"
				this.leftJoin(other, leftOn, actualRightOn, suffix);
	};
}
