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

import { isDependencyValueSatisfied } from './dependency-value.utils';

describe('Dependency Value Utils', () => {
    it('matches native booleans against string dependent values', () => {
        expect(isDependencyValueSatisfied(true, ['true'])).toBe(true);
        expect(isDependencyValueSatisfied(false, ['true'])).toBe(false);
        expect(isDependencyValueSatisfied(false, ['false'])).toBe(true);
    });

    it('matches string values case-sensitively', () => {
        expect(isDependencyValueSatisfied('true', ['true'])).toBe(true);
        expect(isDependencyValueSatisfied('TRUE', ['true'])).toBe(false);
        expect(isDependencyValueSatisfied('value2', ['value1', 'value2'])).toBe(true);
    });

    it('treats any non-empty value as satisfying an empty dependent values list', () => {
        expect(isDependencyValueSatisfied('anything', [])).toBe(true);
        expect(isDependencyValueSatisfied(false, undefined)).toBe(true);
        expect(isDependencyValueSatisfied('', [])).toBe(false);
        expect(isDependencyValueSatisfied(null, [])).toBe(false);
        expect(isDependencyValueSatisfied(undefined, [])).toBe(false);
    });

    it('does not match null or undefined against a dependent values list', () => {
        expect(isDependencyValueSatisfied(null, ['true'])).toBe(false);
        expect(isDependencyValueSatisfied(undefined, ['undefined'])).toBe(false);
    });
});
