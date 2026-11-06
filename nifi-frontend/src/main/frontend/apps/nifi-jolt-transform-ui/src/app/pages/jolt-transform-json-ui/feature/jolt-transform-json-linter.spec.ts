/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { joltSpecJsonParseLinter } from './jolt-transform-json-linter';
import { removeJsonComments } from './jolt-transform-json-linter';
import { parser } from './jolt-transform-json-parser';

describe('Jolt JSON parser', () => {
    const getParserErrors = (json: string): string[] => {
        const tree = parser.parse(json);
        const errors: string[] = [];

        tree.iterate({
            enter: (node) => {
                if (node.type.isError) {
                    errors.push(`Error at ${node.from}-${node.to}`);
                }
            }
        });

        return errors;
    };

    it('should accept valid JSON without comments', () => {
        expect(getParserErrors('{"name": "person"}')).toEqual([]);
    });

    it('should accept single-line comments', () => {
        expect(
            getParserErrors(`{
            // comment
            "name": "person"
        }`)
        ).toEqual([]);
    });

    it('should accept multiline comments', () => {
        expect(
            getParserErrors(`{
            /* comment */
            "name": "person"
        }`)
        ).toEqual([]);
    });

    it('should accept comment-like text inside strings', () => {
        expect(getParserErrors('{"url": "https://example.com"}')).toEqual([]);
    });

    it('should accept block-comment-like text inside strings', () => {
        expect(getParserErrors('{"name": "customer/*profile*/"}')).toEqual([]);
    });

    it('should report an error for an unterminated multiline comment', () => {
        expect(
            getParserErrors(`{
            /* comment
            "name": "person"
        }`)
        ).not.toEqual([]);
    });

    it('should report an error for malformed JSON', () => {
        expect(getParserErrors('{"name": "person",}')).not.toEqual([]);
    });

    it('should accept a single-line comment before the top-level array', () => {
        const json = `
// Comment before the Jolt specification
[
    {
        "operation": "shift",
        "spec": {
            "name": "customer"
        }
    }
]`;

        expect(getParserErrors(json)).toEqual([]);
    });
});

describe('removeJsonComments', () => {
    it('should preserve valid JSON without comments', () => {
        const json = '{"operation": "shift"}';

        expect(removeJsonComments(json)).toBe(json);
    });

    it('should remove single-line comments', () => {
        const json = `{
            // This is a comment
            "operation": "shift"
        }`;

        expect(removeJsonComments(json)).not.toContain('// This is a comment');
    });

    it('should remove multiline comments', () => {
        const json = `{
            /* This is a
               multiline comment */
            "operation": "shift"
        }`;

        expect(removeJsonComments(json)).not.toContain('This is a');
    });

    it('should preserve comment-like text inside strings', () => {
        const json = `{
            "url": "https://example.com/api",
            "description": "This contains // and /* text */"
        }`;

        expect(removeJsonComments(json)).toBe(json);
    });

    it('should preserve line breaks when removing comments', () => {
        const json = `{
            // comment
            "operation": "shift"
        }`;

        const result = removeJsonComments(json);

        expect(result.split('\n').length).toBe(json.split('\n').length);
    });

    it('should preserve comment-like text inside strings with escaped quotation marks', () => {
        const json = `{
        "description": "This is a \\"quoted\\" string containing // and /* text */"
    }`;

        expect(removeJsonComments(json)).toBe(json);
    });

    it('should leave unterminated multiline comments unchanged', () => {
        const json = `{
        /* This comment is never closed
        "operation": "shift"
    }`;

        expect(removeJsonComments(json)).toBe(json);
    });

    it('should preserve character positions when removing comments containing Unicode characters', () => {
        const json = `{
        // Comment containing an emoji 😀
        "operation": "shift"
    }`;

        const result = removeJsonComments(json);

        expect(result).toHaveLength(json.length);
        expect(result.indexOf('"operation"')).toBe(json.indexOf('"operation"'));
    });

    describe('joltSpecJsonParseLinter', () => {
        const runLinter = (json: string) => {
            const doc = {
                length: json.length,
                toString: () => json,
                line: (lineNumber: number) => {
                    const lines = json.split('\n');
                    const from = lines.slice(0, lineNumber - 1).join('\n').length + (lineNumber > 1 ? 1 : 0);
                    return { from };
                }
            };

            return joltSpecJsonParseLinter()({
                state: {
                    doc
                }
            } as any);
        };

        it('should accept valid JSON containing single-line comments', () => {
            const json = `{
            // Jolt operation
            "operation": "shift"
        }`;

            expect(runLinter(json)).toEqual([]);
        });

        it('should accept valid JSON containing multiline comments', () => {
            const json = `{
            /* Jolt operation */
            "operation": "shift"
        }`;

            expect(runLinter(json)).toEqual([]);
        });

        it('should report invalid JSON', () => {
            const json = `{
            // Jolt operation
            "operation":
        }`;

            expect(runLinter(json)).toHaveLength(1);
        });

        it('should report unterminated multiline comments', () => {
            const json = `{
            /* Unterminated comment
            "operation": "shift"
        }`;

            expect(runLinter(json)).toHaveLength(1);
        });

        it('should accept a single-line comment before the top-level array', () => {
            const json = `
// Comment before the Jolt specification
[
    {
        "operation": "shift",
        "spec": {
            "name": "customer"
        }
    }
]`;

            expect(runLinter(json)).toEqual([]);
        });
    });
});
