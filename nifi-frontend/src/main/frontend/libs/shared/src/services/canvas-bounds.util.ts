/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Numeric safety helpers for the NiFi canvas surfaces (flow-designer's
 * CanvasView, the reusable canvas component, the connector-canvas, etc.).
 *
 * Number.isFinite(value) is necessary but not sufficient to guarantee that
 * a number can be written into an SVG attribute or used as a divisor: the
 * browser's SVG parser treats coordinates as float32 internally (max ~3.4e38),
 * and downstream multiplication/division in canvas math can turn a ~9e307
 * finite double into Infinity on the first operation. We apply an explicit
 * magnitude bound everywhere these values cross a trust boundary -- component
 * DTOs from the server, viewport state from localStorage, d3 zoom events,
 * internal transform arithmetic.
 */

/**
 * Upper bound for any canvas-space coordinate value (a component's position.x
 * or position.y). 1e9 is roughly 10,000x larger than any realistic flow
 * extent, and nowhere near the float32 overflow boundary, so even chained
 * zoom math has ~29 orders of magnitude of headroom before re-overflowing.
 */
export const MAX_ABS_COORD = 1_000_000_000;

/**
 * Upper bound for any SVG transform translate component (the tx or ty of a
 * translate(...)). Legitimate translates compose roughly as MAX_ABS_COORD *
 * MAX_SCALE (a few times 1e9); we pick 1e12 to leave three orders of
 * magnitude of headroom while still rejecting catastrophic overflow values
 * (the incident driver was ~9e307 persisted in localStorage).
 */
export const MAX_ABS_TRANSLATE = 1_000_000_000_000;

/**
 * Minimum d3-zoom scale extent shared across every canvas in the workspace.
 */
export const MIN_SCALE = 0.2;

/**
 * Maximum d3-zoom scale extent shared across every canvas in the workspace.
 */
export const MAX_SCALE = 8;

/**
 * Number.isFinite AND magnitude-bound check. Use this everywhere we
 * previously used Number.isFinite(value) alone: a catastrophic-but-finite
 * value like Number.MAX_VALUE / 2 ~= 8.988e+307 is finite, but the browser
 * still rejects it as an SVG attribute and any downstream arithmetic overflows
 * to Infinity.
 *
 * Returns a type guard so call-sites that pass unknown (e.g. JSON parsed from
 * localStorage) get a typed number in the truthy branch.
 */
export function isFiniteInBound(value: unknown, bound: number): value is number {
    return typeof value === 'number' && Number.isFinite(value) && Math.abs(value) <= bound;
}

/**
 * Range check for d3 zoom scale. Returns true iff value is a finite number
 * within [MIN_SCALE, MAX_SCALE].
 *
 * Use this instead of isFiniteInBound(value, MAX_SCALE) at every
 * scale-validation site -- the latter only bounds the magnitude and silently
 * accepts values like k = 0.0001, which would cause division-by-zero in
 * birdseye math and any other code that divides by scale.
 */
export function isScaleInBound(value: unknown): value is number {
    return typeof value === 'number' && Number.isFinite(value) && value >= MIN_SCALE && value <= MAX_SCALE;
}

/**
 * Clamp scale into [MIN_SCALE, MAX_SCALE], returning fallback (default 1)
 * when the input is non-finite. Math.max / Math.min are NaN-poisoned so they
 * cannot be the sole line of defense -- if any upstream arithmetic produces
 * 0 / 0 = NaN, a raw clamp would let NaN escape and corrupt the d3 zoom state.
 */
export function clampScale(scale: number, fallback = 1): number {
    if (!Number.isFinite(scale)) {
        return fallback;
    }
    return Math.min(Math.max(scale, MIN_SCALE), MAX_SCALE);
}

export interface SanitizePositionOptions {
    /** Component id used for the warn message and warn-once dedupe. */
    componentId: string;
    /** Component kind/label used for the warn message (e.g. 'Processor'). */
    componentKind: string;
    /**
     * Optional warn-once dedupe state. When provided, the helper emits at most
     * one console.warn per componentId across the lifetime of the Set. Callers
     * that hold the Set on a long-lived service get one warn per id; callers
     * that omit it get one warn per call.
     */
    warnedIds?: Set<string>;
}

/**
 * Sanitize a position arriving from server data (or any other untrusted
 * source) before it is bound to a d3 datum.
 *
 * The canvas pipeline reads d.position.{x,y} from many call sites (rendering,
 * viewport math, bbox math). If a corrupted finite-but-extreme coordinate
 * (Number.MAX_VALUE / 2.4) reaches any of those, downstream
 * multiplication-heavy math can overflow into Infinity and corrupt the d3 zoom
 * state for the whole session.
 *
 * Returns a fresh object so the caller never mutates the position living in
 * the source DTO. When the value is out of range, the fallback is (0, 0) and
 * a (deduped) console.warn is emitted so support can identify which entities
 * need to be dragged-and-saved (or repaired via the REST API) to flush the
 * bad value out of the persisted flow.
 */
export function sanitizePosition(
    position: { x: number | null | undefined; y: number | null | undefined } | null | undefined,
    { componentId, componentKind, warnedIds }: SanitizePositionOptions
): { x: number; y: number } {
    const x = position?.x;
    const y = position?.y;
    if (!isFiniteInBound(x, MAX_ABS_COORD) || !isFiniteInBound(y, MAX_ABS_COORD)) {
        if (!warnedIds || !warnedIds.has(componentId)) {
            warnedIds?.add(componentId);
            console.warn(
                `Component ${componentKind} ${componentId} has an out-of-range position`,
                position,
                '\u2014 falling back to (0, 0). Drag the component to a new location and save to repair the persisted value.'
            );
        }
        return { x: 0, y: 0 };
    }
    return { x, y };
}
