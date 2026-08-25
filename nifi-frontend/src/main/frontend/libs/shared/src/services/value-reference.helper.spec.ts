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

import { fromValueReference, toBooleanValue } from './value-reference.helper';

describe('Value Reference Helper', () => {
    describe('toBooleanValue', () => {
        it('coerces true values case-insensitively to match Boolean.parseBoolean', () => {
            expect(toBooleanValue(true)).toBe(true);
            expect(toBooleanValue('true')).toBe(true);
            expect(toBooleanValue('True')).toBe(true);
            expect(toBooleanValue('TRUE')).toBe(true);
        });

        it('coerces all other values to false', () => {
            expect(toBooleanValue(false)).toBe(false);
            expect(toBooleanValue('false')).toBe(false);
            expect(toBooleanValue('anything')).toBe(false);
            expect(toBooleanValue(null)).toBe(false);
            expect(toBooleanValue(undefined)).toBe(false);
        });
    });

    describe('fromValueReference', () => {
        it('normalizes BOOLEAN string values case-insensitively', () => {
            expect(fromValueReference({ value: 'TRUE', valueType: 'STRING_LITERAL' }, 'BOOLEAN')).toBe(true);
            expect(fromValueReference({ value: 'false', valueType: 'STRING_LITERAL' }, 'BOOLEAN')).toBe(false);
        });

        it('preserves a null BOOLEAN value so callers can use the descriptor default', () => {
            expect(fromValueReference({ value: null, valueType: 'STRING_LITERAL' }, 'BOOLEAN')).toBeNull();
        });
    });
});
