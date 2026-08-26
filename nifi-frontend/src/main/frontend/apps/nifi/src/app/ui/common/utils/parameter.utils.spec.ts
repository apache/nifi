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

import { extractParameterName, PARAM_REF_REGEX } from './parameter.utils';

describe('extractParameterName', () => {
    describe('valid unquoted references', () => {
        it('extracts a simple name', () => {
            expect(extractParameterName('#{my-param}')).toBe('my-param');
        });

        it('extracts a name with underscores and dots', () => {
            expect(extractParameterName('#{my_param.v2}')).toBe('my_param.v2');
        });

        it('extracts a name with spaces', () => {
            expect(extractParameterName('#{param with spaces}')).toBe('param with spaces');
        });

        it('trims surrounding whitespace inside an unquoted reference', () => {
            expect(extractParameterName('#{ my-param }')).toBe('my-param');
        });

        it('extracts a single-character name', () => {
            expect(extractParameterName('#{x}')).toBe('x');
        });
    });

    describe('valid single-quoted references', () => {
        it('extracts a name wrapped in single quotes', () => {
            expect(extractParameterName("#{'my-param'}")).toBe('my-param');
        });

        it('extracts a name with spaces wrapped in single quotes', () => {
            expect(extractParameterName("#{'param with spaces'}")).toBe('param with spaces');
        });
    });

    describe('valid double-quoted references', () => {
        it('extracts a name wrapped in double quotes', () => {
            expect(extractParameterName('#{"my-param"}')).toBe('my-param');
        });

        it('extracts a name with spaces wrapped in double quotes', () => {
            expect(extractParameterName('#{"param with spaces"}')).toBe('param with spaces');
        });
    });

    describe('non-matching values', () => {
        it('returns undefined for a plain string', () => {
            expect(extractParameterName('plain-value')).toBeUndefined();
        });

        it('returns undefined for an empty string', () => {
            expect(extractParameterName('')).toBeUndefined();
        });

        it('returns undefined for #{} with no name', () => {
            expect(extractParameterName('#{}')).toBeUndefined();
        });

        it('returns undefined for #{ } with only whitespace', () => {
            expect(extractParameterName('#{ }')).toBeUndefined();
        });

        it('returns undefined when quotes are mismatched', () => {
            expect(extractParameterName('#{\'mismatched"}')).toBeUndefined();
        });
    });

    describe('references embedded in surrounding content', () => {
        it('returns the parameter name when the reference has trailing content', () => {
            expect(extractParameterName('#{param} extra')).toBe('param');
        });

        it('returns the parameter name when the reference has leading content', () => {
            expect(extractParameterName('prefix #{param}')).toBe('param');
        });

        it('returns the first parameter name when the value contains multiple references', () => {
            expect(extractParameterName('#{p1}-#{p2}')).toBe('p1');
        });

        it('skips an invalid reference and extracts the first valid one', () => {
            expect(extractParameterName('#{bad:name} #{kafka.brokers}')).toBe('kafka.brokers');
        });
    });
});

describe('PARAM_REF_REGEX', () => {
    it('matches the same first valid reference that extractParameterName returns', () => {
        const value = '#{bad:name} #{kafka.brokers}';
        expect(PARAM_REF_REGEX.test(value)).toBe(true);
        expect(extractParameterName(value)).toBe('kafka.brokers');
    });
});
