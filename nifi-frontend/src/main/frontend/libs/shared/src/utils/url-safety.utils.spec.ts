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

import { isSameOriginTarget, safeApiPath, UnsafeApiPathError } from './url-safety.utils';

describe('safeApiPath', () => {
    describe('valid segments', () => {
        it('joins plain segments with /', () => {
            expect(safeApiPath('read', 'processors', 'abc-123')).toBe('read/processors/abc-123');
        });

        it('leaves already-safe values unchanged (preserves existing URL shapes)', () => {
            expect(safeApiPath('read', 'data', 'connectors')).toBe('read/data/connectors');
        });

        it('percent-encodes reserved characters within a segment', () => {
            expect(safeApiPath('step name')).toBe('step%20name');
            expect(safeApiPath('a?b&c=d')).toBe('a%3Fb%26c%3Dd');
        });

        it('encodes a single segment', () => {
            expect(safeApiPath('connector-1')).toBe('connector-1');
        });
    });

    describe('traversal and separator rejection', () => {
        it('rejects a literal .. segment', () => {
            expect(() => safeApiPath('..')).toThrow(UnsafeApiPathError);
        });

        it('rejects a literal . segment', () => {
            expect(() => safeApiPath('.')).toThrow(UnsafeApiPathError);
        });

        it('rejects a segment containing a forward slash', () => {
            expect(() => safeApiPath('a/b')).toThrow(UnsafeApiPathError);
            expect(() => safeApiPath('../etc/passwd')).toThrow(UnsafeApiPathError);
        });

        it('rejects a segment containing a backslash', () => {
            expect(() => safeApiPath('a\\b')).toThrow(UnsafeApiPathError);
        });

        it('rejects a segment that embeds a traversal sequence', () => {
            expect(() => safeApiPath('foo..bar')).toThrow(UnsafeApiPathError);
        });
    });

    describe('pre-encoded traversal rejection', () => {
        it('rejects url-encoded dot-dot (%2e%2e)', () => {
            expect(() => safeApiPath('%2e%2e')).toThrow(UnsafeApiPathError);
        });

        it('rejects mixed-case url-encoded dot-dot (%2E%2E)', () => {
            expect(() => safeApiPath('%2E%2E')).toThrow(UnsafeApiPathError);
        });

        it('rejects url-encoded forward slash (%2f)', () => {
            expect(() => safeApiPath('a%2fb')).toThrow(UnsafeApiPathError);
        });

        it('rejects url-encoded backslash (%5c)', () => {
            expect(() => safeApiPath('a%5cb')).toThrow(UnsafeApiPathError);
        });

        it('rejects url-encoded traversal path (%2e%2e%2f)', () => {
            expect(() => safeApiPath('%2e%2e%2fetc')).toThrow(UnsafeApiPathError);
        });
    });

    describe('malformed / empty input rejection', () => {
        it('rejects an empty segment', () => {
            expect(() => safeApiPath('')).toThrow(UnsafeApiPathError);
        });

        it('rejects when any of multiple segments is empty', () => {
            expect(() => safeApiPath('read', '', 'processors')).toThrow(UnsafeApiPathError);
        });

        it('rejects a segment containing a control character', () => {
            expect(() => safeApiPath('a\u0000b')).toThrow(UnsafeApiPathError);
        });

        it('rejects a segment that is not a decodable URI component', () => {
            expect(() => safeApiPath('%')).toThrow(UnsafeApiPathError);
            expect(() => safeApiPath('%zz')).toThrow(UnsafeApiPathError);
        });
    });
});

describe('isSameOriginTarget', () => {
    const base = 'https://nifi.example.com/nifi-api';

    it('returns true for a same-origin absolute URL', () => {
        expect(isSameOriginTarget('https://nifi.example.com/nifi-api/flowfile-queues/1/content', base)).toBe(true);
    });

    it('returns true for a same-origin URL on a different path', () => {
        expect(isSameOriginTarget('https://nifi.example.com/other', base)).toBe(true);
    });

    it('returns true for a relative reference resolved against the base', () => {
        expect(isSameOriginTarget('/nifi-api/flowfile-queues/1/content', base)).toBe(true);
    });

    it('returns false for a different host', () => {
        expect(isSameOriginTarget('https://evil.example.com/nifi-api/content', base)).toBe(false);
    });

    it('returns false for a prefix-spoofing host', () => {
        expect(isSameOriginTarget('https://nifi.example.com.evil.com/nifi-api/content', base)).toBe(false);
    });

    it('returns false for a different scheme', () => {
        expect(isSameOriginTarget('http://nifi.example.com/nifi-api/content', base)).toBe(false);
    });

    it('returns false for a different port', () => {
        expect(isSameOriginTarget('https://nifi.example.com:8443/nifi-api/content', base)).toBe(false);
    });

    it('returns false when the candidate is an invalid absolute URL', () => {
        expect(isSameOriginTarget('http://', base)).toBe(false);
    });

    it('returns false when the base cannot be parsed', () => {
        expect(isSameOriginTarget('https://nifi.example.com/x', 'not-an-absolute-url')).toBe(false);
    });

    describe('requireBasePathPrefix', () => {
        it('returns true for a same-origin ref that resolves under the base path', () => {
            expect(
                isSameOriginTarget('https://nifi.example.com/nifi-api/flowfile-queues/1/content', base, {
                    requireBasePathPrefix: true
                })
            ).toBe(true);
        });

        it('returns false for a same-origin ref outside the base path', () => {
            expect(
                isSameOriginTarget('https://nifi.example.com/other/content', base, { requireBasePathPrefix: true })
            ).toBe(false);
        });

        it('still returns false for a cross-origin ref', () => {
            expect(
                isSameOriginTarget('https://evil.example.com/nifi-api/content', base, { requireBasePathPrefix: true })
            ).toBe(false);
        });

        it('returns false for a same-origin ref that escapes the base path via ../ traversal', () => {
            expect(
                isSameOriginTarget('https://nifi.example.com/nifi-api/../internal-status', base, {
                    requireBasePathPrefix: true
                })
            ).toBe(false);
        });

        it('returns false for a sibling path that shares the base as a string prefix', () => {
            expect(
                isSameOriginTarget('https://nifi.example.com/nifi-api-evil/content', base, {
                    requireBasePathPrefix: true
                })
            ).toBe(false);
        });

        it('returns true when the ref resolves to exactly the base path', () => {
            expect(isSameOriginTarget('https://nifi.example.com/nifi-api', base, { requireBasePathPrefix: true })).toBe(
                true
            );
        });
    });
});
