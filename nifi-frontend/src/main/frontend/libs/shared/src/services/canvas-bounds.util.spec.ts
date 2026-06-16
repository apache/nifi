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

import {
    clampScale,
    isFiniteInBound,
    isScaleInBound,
    MAX_ABS_COORD,
    MAX_ABS_TRANSLATE,
    MAX_SCALE,
    MIN_SCALE,
    sanitizePosition
} from './canvas-bounds.util';

describe('canvas-bounds.util', () => {
    describe('constants', () => {
        it('exposes MAX_ABS_COORD = 1e9', () => {
            expect(MAX_ABS_COORD).toBe(1_000_000_000);
        });

        it('exposes the SVG translate bound with three orders of magnitude of headroom over MAX_ABS_COORD * MAX_SCALE', () => {
            expect(MAX_ABS_TRANSLATE).toBe(1_000_000_000_000);
            expect(MAX_ABS_TRANSLATE).toBeGreaterThan(MAX_ABS_COORD * MAX_SCALE);
        });

        it('exposes MIN_SCALE = 0.2 and MAX_SCALE = 8', () => {
            expect(MIN_SCALE).toBe(0.2);
            expect(MAX_SCALE).toBe(8);
        });
    });

    describe('isFiniteInBound', () => {
        it('accepts a finite value within bound', () => {
            expect(isFiniteInBound(0, MAX_ABS_COORD)).toBe(true);
            expect(isFiniteInBound(1e8, MAX_ABS_COORD)).toBe(true);
            expect(isFiniteInBound(-1e8, MAX_ABS_COORD)).toBe(true);
        });

        it('accepts a value exactly at the bound', () => {
            expect(isFiniteInBound(MAX_ABS_COORD, MAX_ABS_COORD)).toBe(true);
            expect(isFiniteInBound(-MAX_ABS_COORD, MAX_ABS_COORD)).toBe(true);
        });

        it('rejects a value just over the bound', () => {
            expect(isFiniteInBound(MAX_ABS_COORD + 1, MAX_ABS_COORD)).toBe(false);
        });

        it('rejects catastrophic-but-finite values (the ~7.49e+307 fingerprint)', () => {
            expect(isFiniteInBound(7.490388061926315e307, MAX_ABS_COORD)).toBe(false);
            expect(isFiniteInBound(Number.MAX_VALUE / 2, MAX_ABS_TRANSLATE)).toBe(false);
        });

        it('rejects Infinity and -Infinity', () => {
            expect(isFiniteInBound(Number.POSITIVE_INFINITY, MAX_ABS_COORD)).toBe(false);
            expect(isFiniteInBound(Number.NEGATIVE_INFINITY, MAX_ABS_COORD)).toBe(false);
        });

        it('rejects NaN', () => {
            expect(isFiniteInBound(NaN, MAX_ABS_COORD)).toBe(false);
        });

        it('rejects non-number types', () => {
            expect(isFiniteInBound('42', MAX_ABS_COORD)).toBe(false);
            expect(isFiniteInBound(null, MAX_ABS_COORD)).toBe(false);
            expect(isFiniteInBound(undefined, MAX_ABS_COORD)).toBe(false);
        });
    });

    describe('isScaleInBound', () => {
        it('accepts values within [MIN_SCALE, MAX_SCALE]', () => {
            expect(isScaleInBound(1)).toBe(true);
            expect(isScaleInBound(MIN_SCALE)).toBe(true);
            expect(isScaleInBound(MAX_SCALE)).toBe(true);
            expect(isScaleInBound(0.5)).toBe(true);
        });

        it('rejects values below MIN_SCALE', () => {
            expect(isScaleInBound(0.1)).toBe(false);
            expect(isScaleInBound(0.0001)).toBe(false);
            expect(isScaleInBound(0)).toBe(false);
        });

        it('rejects values above MAX_SCALE', () => {
            expect(isScaleInBound(8.0001)).toBe(false);
            expect(isScaleInBound(1e6)).toBe(false);
        });

        it('rejects Infinity, -Infinity, and NaN', () => {
            expect(isScaleInBound(Number.POSITIVE_INFINITY)).toBe(false);
            expect(isScaleInBound(Number.NEGATIVE_INFINITY)).toBe(false);
            expect(isScaleInBound(NaN)).toBe(false);
        });

        it('rejects non-number types', () => {
            expect(isScaleInBound('1')).toBe(false);
            expect(isScaleInBound(null)).toBe(false);
            expect(isScaleInBound(undefined)).toBe(false);
        });
    });

    describe('clampScale', () => {
        it('clamps a value below MIN_SCALE to MIN_SCALE', () => {
            expect(clampScale(0)).toBe(MIN_SCALE);
            expect(clampScale(0.1)).toBe(MIN_SCALE);
        });

        it('clamps a value above MAX_SCALE to MAX_SCALE', () => {
            expect(clampScale(100)).toBe(MAX_SCALE);
        });

        it('passes through an in-bounds value unchanged', () => {
            expect(clampScale(1)).toBe(1);
            expect(clampScale(MIN_SCALE)).toBe(MIN_SCALE);
            expect(clampScale(MAX_SCALE)).toBe(MAX_SCALE);
        });

        it('returns the default fallback (1) for NaN', () => {
            expect(clampScale(NaN)).toBe(1);
        });

        it('returns the default fallback (1) for Infinity', () => {
            expect(clampScale(Number.POSITIVE_INFINITY)).toBe(1);
            expect(clampScale(Number.NEGATIVE_INFINITY)).toBe(1);
        });

        it('returns a custom fallback for non-finite inputs', () => {
            expect(clampScale(NaN, 2)).toBe(2);
        });
    });

    describe('sanitizePosition', () => {
        let warnSpy: ReturnType<typeof vi.spyOn>;

        beforeEach(() => {
            warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
        });

        afterEach(() => {
            warnSpy.mockRestore();
        });

        it('returns the original coordinate when in bounds', () => {
            const result = sanitizePosition({ x: 100, y: 200 }, { componentId: 'id1', componentKind: 'Processor' });
            expect(result).toEqual({ x: 100, y: 200 });
            expect(warnSpy).not.toHaveBeenCalled();
        });

        it('returns (0, 0) and warns for catastrophic-but-finite coordinates (the NIFI-16025 fingerprint)', () => {
            const result = sanitizePosition(
                { x: 7.490388061926315e307, y: 0 },
                { componentId: 'id2', componentKind: 'Processor' }
            );
            expect(result).toEqual({ x: 0, y: 0 });
            expect(warnSpy).toHaveBeenCalledTimes(1);
        });

        it('returns (0, 0) and warns for Infinity', () => {
            const result = sanitizePosition(
                { x: Number.POSITIVE_INFINITY, y: 0 },
                { componentId: 'id3', componentKind: 'Funnel' }
            );
            expect(result).toEqual({ x: 0, y: 0 });
            expect(warnSpy).toHaveBeenCalledTimes(1);
        });

        it('returns (0, 0) and warns for null position', () => {
            const result = sanitizePosition(null, { componentId: 'id4', componentKind: 'Label' });
            expect(result).toEqual({ x: 0, y: 0 });
            expect(warnSpy).toHaveBeenCalledTimes(1);
        });

        it('accepts a coordinate exactly at MAX_ABS_COORD', () => {
            const result = sanitizePosition(
                { x: MAX_ABS_COORD, y: -MAX_ABS_COORD },
                { componentId: 'id5', componentKind: 'Processor' }
            );
            expect(result).toEqual({ x: MAX_ABS_COORD, y: -MAX_ABS_COORD });
            expect(warnSpy).not.toHaveBeenCalled();
        });

        it('rejects a coordinate just over MAX_ABS_COORD', () => {
            const result = sanitizePosition(
                { x: MAX_ABS_COORD + 1, y: 0 },
                { componentId: 'id6', componentKind: 'Processor' }
            );
            expect(result).toEqual({ x: 0, y: 0 });
            expect(warnSpy).toHaveBeenCalledTimes(1);
        });

        it('returns a new object (does not mutate the source)', () => {
            const original = { x: 100, y: 200 };
            const result = sanitizePosition(original, { componentId: 'id7', componentKind: 'Processor' });
            expect(result).not.toBe(original);
        });

        describe('warn-once dedupe via warnedIds', () => {
            it('emits at most one warn per componentId when a Set is provided', () => {
                const warnedIds = new Set<string>();
                sanitizePosition({ x: 7.49e307, y: 0 }, { componentId: 'dup', componentKind: 'Processor', warnedIds });
                sanitizePosition({ x: 7.49e307, y: 0 }, { componentId: 'dup', componentKind: 'Processor', warnedIds });
                expect(warnSpy).toHaveBeenCalledTimes(1);
            });

            it('warns for distinct component ids independently', () => {
                const warnedIds = new Set<string>();
                sanitizePosition({ x: 7.49e307, y: 0 }, { componentId: 'a', componentKind: 'Processor', warnedIds });
                sanitizePosition({ x: 7.49e307, y: 0 }, { componentId: 'b', componentKind: 'Processor', warnedIds });
                expect(warnSpy).toHaveBeenCalledTimes(2);
            });
        });
    });
});
