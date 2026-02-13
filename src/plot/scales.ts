/* SCALES
/*-----------------------------------------------------
/* Pure functions for mapping data domains to pixel ranges.
/* ==================================================== */

export interface LinearScale {
	(value: number): number;
	domain: [number, number];
	range: [number, number];
}

export interface OrdinalScale {
	(value: string): number;
	domain: string[];
	range: [number, number];
	bandwidth: number;
}

export function linearScale(
	domain: [number, number],
	range: [number, number],
): LinearScale {
	const [d0, d1] = domain;
	const [r0, r1] = range;
	const span = d1 - d0;

	const scale = ((value: number): number => {
		if (span === 0) return (r0 + r1) / 2;
		return r0 + ((value - d0) / span) * (r1 - r0);
	}) as LinearScale;

	scale.domain = domain;
	scale.range = range;
	return scale;
}

export function ordinalScale(
	domain: string[],
	range: [number, number],
): OrdinalScale {
	const [r0, r1] = range;
	const totalWidth = r1 - r0;
	const bandWidth = domain.length > 0 ? totalWidth / domain.length : 0;

	const lookup = new Map<string, number>();
	for (let i = 0; i < domain.length; i++) {
		lookup.set(domain[i]!, r0 + i * bandWidth + bandWidth / 2);
	}

	const scale = ((value: string): number => {
		return lookup.get(value) ?? r0;
	}) as OrdinalScale;

	scale.domain = domain;
	scale.range = range;
	scale.bandwidth = bandWidth;
	return scale;
}

// Compute human-readable tick values for a numeric axis
export function computeNiceTicks(
	min: number,
	max: number,
	targetCount: number,
): number[] {
	if (min === max) return [min];

	const range = max - min;
	const roughStep = range / targetCount;

	// Snap to a "nice" step: 1, 2, 5 × 10^n
	const magnitude = 10 ** Math.floor(Math.log10(roughStep));
	const normalized = roughStep / magnitude;

	let niceStep: number;
	if (normalized <= 1.5) niceStep = 1 * magnitude;
	else if (normalized <= 3.5) niceStep = 2 * magnitude;
	else if (normalized <= 7.5) niceStep = 5 * magnitude;
	else niceStep = 10 * magnitude;

	const niceMin = Math.floor(min / niceStep) * niceStep;
	const niceMax = Math.ceil(max / niceStep) * niceStep;

	const ticks: number[] = [];
	for (let v = niceMin; v <= niceMax + niceStep * 0.5; v += niceStep) {
		ticks.push(Math.round(v * 1e10) / 1e10);
	}

	return ticks;
}

// Compute domain [min, max] from data, with optional nice padding
export function computeDomain(
	values: number[],
	niceExtend = true,
): [number, number] {
	if (values.length === 0) return [0, 1];

	let min = values[0]!;
	let max = values[0]!;
	for (let i = 1; i < values.length; i++) {
		const v = values[i]!;
		if (v < min) min = v;
		if (v > max) max = v;
	}

	if (min === max) {
		return min === 0 ? [0, 1] : [min * 0.9, max * 1.1];
	}

	if (niceExtend) {
		const ticks = computeNiceTicks(min, max, 5);
		return [ticks[0]!, ticks[ticks.length - 1]!];
	}

	return [min, max];
}
