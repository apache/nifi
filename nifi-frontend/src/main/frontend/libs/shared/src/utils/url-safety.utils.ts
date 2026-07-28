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
 * Error thrown by {@link safeApiPath} when an untrusted path segment fails
 * validation. Callers fail closed (no request is issued) rather than
 * interpolating a potentially malicious value into an authenticated API URL.
 */
export class UnsafeApiPathError extends Error {
    constructor(segment: string, reason: string) {
        super(`Unsafe API path segment [${segment}]: ${reason}`);
        this.name = 'UnsafeApiPathError';
    }
}

// Matches ASCII control characters (including NUL) which must never appear in a
// path segment.
// eslint-disable-next-line no-control-regex
const CONTROL_CHARS = /[\u0000-\u001f\u007f]/;

/**
 * Returns `true` when the given (already url-decoded) value contains a path
 * separator or traversal sequence and therefore must not be used as a single
 * path segment.
 *
 * Note: this rejects any value that merely *embeds* `..` (e.g. `foo..bar`), not
 * only a literal `..` segment. This is intentional and safe for the intended
 * callers, whose segments are identifiers (UUIDs / decimal ids) or enumerated
 * NiFi resource names -- none of which legitimately contain `..`.
 */
function containsTraversal(value: string): boolean {
    return value === '.' || value.includes('/') || value.includes('\\') || value.includes('..');
}

/**
 * Validate and percent-encode untrusted strings for use as path segments of an
 * authenticated API URL.
 *
 * Each argument is treated as a single, atomic path segment. Callers with a
 * composite value (for example a policy resource such as
 * `provenance-data/connectors`) must split it on `/` and pass each atom
 * individually so every atom is validated and encoded.
 *
 * A segment is rejected (via {@link UnsafeApiPathError}) when it is empty,
 * contains a `/` or `\`, embeds a `..` traversal sequence, contains an ASCII
 * control character, is not decodable, or decodes to a value that itself
 * contains a traversal sequence (defends against pre-encoded traversal such as
 * `%2e%2e` or `%2f`). Segments are assumed to be identifiers or enumerated
 * resource names.
 *
 * @returns the surviving segments `encodeURIComponent`-encoded and joined with `/`.
 */
export function safeApiPath(...segments: string[]): string {
    return segments
        .map((segment) => {
            if (segment === null || segment === undefined || segment.length === 0) {
                throw new UnsafeApiPathError(String(segment), 'segment is empty');
            }

            if (CONTROL_CHARS.test(segment)) {
                throw new UnsafeApiPathError(segment, 'segment contains control characters');
            }

            if (containsTraversal(segment)) {
                throw new UnsafeApiPathError(segment, 'segment contains a path separator or traversal sequence');
            }

            let decoded: string;
            try {
                decoded = decodeURIComponent(segment);
            } catch {
                throw new UnsafeApiPathError(segment, 'segment is not a valid URI component');
            }

            if (containsTraversal(decoded)) {
                throw new UnsafeApiPathError(segment, 'segment decodes to a path separator or traversal sequence');
            }

            return encodeURIComponent(segment);
        })
        .join('/');
}

/**
 * Determine whether `candidate` resolves to the same origin as `base`.
 *
 * Both values are canonicalized with the URL constructor (`candidate` is
 * resolved relative to `base`) and their origins compared. Returns `false`
 * when either value cannot be parsed, so callers fail closed on malformed
 * input rather than trusting a value that merely string-prefix-matches a
 * trusted URL.
 *
 * When `options.requireBasePathPrefix` is set, the candidate must additionally
 * resolve under `base`'s path. The check is segment-boundary aware -- the
 * candidate's `pathname` must equal `base`'s `pathname` or start with it
 * followed by a `/` -- so a base of `/nifi-api` does not match a sibling path
 * such as `/nifi-api-evil`. This preserves the scoping of a legacy
 * `startsWith(base)` guard while still closing look-alike-origin bypasses.
 */
export function isSameOriginTarget(
    candidate: string,
    base: string,
    options: { requireBasePathPrefix?: boolean } = {}
): boolean {
    try {
        const baseUrl = new URL(base);
        const candidateUrl = new URL(candidate, baseUrl);
        if (candidateUrl.origin !== baseUrl.origin) {
            return false;
        }
        if (options.requireBasePathPrefix && !isPathUnder(candidateUrl.pathname, baseUrl.pathname)) {
            return false;
        }
        return true;
    } catch {
        return false;
    }
}

/**
 * Returns `true` when `pathname` is the same as, or a descendant of, `basePath`.
 * The comparison respects path-segment boundaries so `/nifi-api` does not match
 * `/nifi-api-evil` (but does match `/nifi-api` and `/nifi-api/flow`).
 */
function isPathUnder(pathname: string, basePath: string): boolean {
    if (pathname === basePath) {
        return true;
    }
    const normalizedBase = basePath.endsWith('/') ? basePath : `${basePath}/`;
    return pathname.startsWith(normalizedBase);
}
