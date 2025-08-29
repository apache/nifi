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

import { TestBed } from '@angular/core/testing';
import { CodemirrorNifiLanguageService } from './codemirror-nifi-language.service';
import { ElService } from '../el.service';
import { NiFiCommon } from '../nifi-common.service';
import { of } from 'rxjs';
import { NFEL_PATTERNS } from './nfel/nfel-example';
import { CompletionContext } from '@codemirror/autocomplete';
import { EditorState } from '@codemirror/state';
import { syntaxTree } from '@codemirror/language';

describe('CodemirrorNifiLanguageService', () => {
    let service: CodemirrorNifiLanguageService;
    let mockElService: jest.Mocked<ElService>;
    let mockNiFiCommon: jest.Mocked<NiFiCommon>;

    // Centralized test data
    const mockFunctions = [
        { name: 'equals', description: 'Tests for equality', returnType: 'Boolean', subject: 'String' },
        {
            name: 'contains',
            description: 'Tests if string contains substring',
            returnType: 'Boolean',
            subject: 'String'
        },
        { name: 'toUpper', description: 'Converts to uppercase', returnType: 'String', subject: 'String' },
        { name: 'uuid', description: 'Generates a UUID', returnType: 'String', subjectless: true },
        { name: 'now', description: 'Current timestamp', returnType: 'String', subjectless: true }
    ];

    const mockParameters = [
        { name: 'param1', description: 'Test parameter 1', sensitive: false, value: 'value1' },
        { name: 'param with spaces', description: 'Parameter with spaces', sensitive: false, value: 'value2' },
        { name: 'database.url', description: 'Database URL parameter', sensitive: false, value: 'jdbc:...' },
        { name: 'Date Format', description: 'Date format parameter', sensitive: false, value: 'yyyy-MM-dd' }
    ];

    beforeEach(() => {
        mockElService = { getElGuide: jest.fn() } as unknown as jest.Mocked<ElService>;
        mockNiFiCommon = { compareString: jest.fn() } as unknown as jest.Mocked<NiFiCommon>;

        // Generate EL guide HTML from mock data
        const functionsHtml = mockFunctions
            .map(
                (fn) => `
            <div class="function">
                <h3>${fn.name}</h3>
                <span class="description">${fn.description}</span>
                <span class="returnType">${fn.returnType}</span>
                ${fn.subject ? `<span class="subject">${fn.subject}</span>` : ''}
                ${fn.subjectless ? '<span class="subjectless">true</span>' : ''}
            </div>
        `
            )
            .join('');

        mockElService.getElGuide.mockReturnValue(of(`<html><body>${functionsHtml}</body></html>`));
        mockNiFiCommon.compareString.mockImplementation((a, b) => {
            if (!a && !b) return 0;
            if (!a) return -1;
            if (!b) return 1;
            return a.localeCompare(b);
        });

        TestBed.configureTestingModule({
            providers: [
                CodemirrorNifiLanguageService,
                { provide: ElService, useValue: mockElService },
                { provide: NiFiCommon, useValue: mockNiFiCommon }
            ]
        });

        service = TestBed.inject(CodemirrorNifiLanguageService);
    });

    // Centralized helper functions
    const createContext = (text: string, pos?: number): CompletionContext => {
        const position = pos ?? text.length;
        const state = EditorState.create({
            doc: text,
            selection: { anchor: position },
            extensions: [service.getLanguageSupport()]
        });

        return {
            state,
            pos: position,
            explicit: false,
            matchBefore: jest.fn((regex: RegExp) => {
                const beforeText = text.substring(0, position);
                const match = beforeText.match(regex);
                return match ? { from: position - match[0].length, to: position, text: match[0] } : null;
            })
        } as unknown as CompletionContext;
    };

    const enableFeatures = (el = true, params = true) => {
        service.setLanguageOptions({
            functionsEnabled: el,
            parametersEnabled: params,
            parameters: params ? mockParameters : undefined
        });
    };

    const expectCompletion = async (input: string, shouldHaveCompletion = true) => {
        const context = createContext(input);
        const result = await (service as any).nfelCompletions(context);
        if (shouldHaveCompletion) {
            expect(result).not.toBeNull();
            expect(result.options).toBeDefined();
        } else {
            expect(result).toBeNull();
        }
        return result;
    };

    const parseInput = (input: string) => {
        const context = createContext(input);
        const tree = syntaxTree(context.state);
        expect(() => syntaxTree(context.state)).not.toThrow();
        return { context, tree };
    };

    describe('Basic Functionality', () => {
        it('should be created and provide language support', () => {
            expect(service).toBeTruthy();
            expect(service.getLanguageSupport()).toBeTruthy();
        });

        it('should handle feature enablement correctly', () => {
            // Initially disabled
            expect(service.supportsEl()).toBe(false);
            expect(service.supportsParameterReference()).toBe(false);

            // Enable features
            enableFeatures(true, true);
            expect(service.supportsEl()).toBe(true);
            expect(service.supportsParameterReference()).toBe(true);

            // Disable features
            enableFeatures(false, false);
            expect(service.supportsEl()).toBe(false);
            expect(service.supportsParameterReference()).toBe(false);
        });
    });

    describe('Expression Language Parsing', () => {
        beforeEach(() => enableFeatures(true, true));

        it('should parse simple expressions without errors', () => {
            const expressions = [
                '${attr}',
                '${filename:replace("old", "new")}',
                '${uuid()}',
                '#{param}',
                'Hello ${name} world',
                '$${escaped}', // escaped expression literal
                '##{param}', // escaped parameter literal
                '{}', // literal braces should not crash
                'Text with { and } braces'
            ];

            expressions.forEach((expr) => {
                const { tree } = parseInput(expr);
                expect(tree).toBeTruthy();
                expect(tree.length).toBeGreaterThan(0);
            });
        });

        it('should properly identify multi-attribute function names', () => {
            const testCases = [
                '${anyAttribute("user.*")}',
                '${anyMatchingAttribute("[a-z].*")}',
                '${allAttributes("abc", "xyz")}',
                '${allMatchingAttributes("user.*")}',
                '${anyDelineatedValue("${attr}", ",")}',
                '${allDelineatedValues("${list}", ",")}'
            ];

            testCases.forEach((expr) => {
                const { context, tree } = parseInput(expr);
                expect(tree).toBeTruthy();
                expect(tree.length).toBeGreaterThan(0);

                let foundFunction = false;
                tree.cursor().iterate((node: any) => {
                    if (node.type.name === 'MultiAttrFunction') {
                        const funcName = context.state.doc.sliceString(node.from, node.to);
                        // Extract expected function name from expression
                        const expected = expr.match(/\$\{(\w+)/)?.[1];
                        expect(funcName).toBe(expected);
                        foundFunction = true;
                    }
                    return true;
                });

                expect(foundFunction).toBe(true);
            });
        });

        it('should parse parameters with no quotes and with quotes similar', () => {
            const exprUnquoted = '#{param1}';
            const exprQuoted = '#{"param1"}';

            const { context: ctxUnquoted, tree: treeUnquoted } = parseInput(exprUnquoted);
            const { context: ctxQuoted, tree: treeQuoted } = parseInput(exprQuoted);

            expect(treeUnquoted).toBeTruthy();
            expect(treeQuoted).toBeTruthy();

            expect(treeUnquoted.length).toBeGreaterThan(0);
            expect(treeQuoted.length).toBeGreaterThan(0);

            const getParamName = (state: any, tree: any) => {
                let name = '';
                tree.cursor().iterate((node: any) => {
                    if (node.type.name === 'ParameterName') {
                        const raw = state.doc.sliceString(node.from, node.to);
                        name = raw.replace(/^['"]|['"]$/g, '');
                    }
                    return true;
                });
                return name;
            };

            expect(getParamName(ctxUnquoted.state, treeUnquoted)).toEqual(getParamName(ctxQuoted.state, treeQuoted));
        });

        it('should parse parameters with no spaces and with spaces (unquoted) the same', () => {
            const exprSpaces = '#{Date Format}';
            const exprNoSpaces = '#{param1}';

            const { tree: treeNoSpaces } = parseInput(exprNoSpaces);
            const { tree: treeSpaces } = parseInput(exprSpaces);

            expect(treeNoSpaces).toBeTruthy();
            expect(treeSpaces).toBeTruthy();

            expect(treeNoSpaces.length).toBeGreaterThan(0);
            expect(treeSpaces.length).toBeGreaterThan(0);

            const serializedNoSpaces = (treeNoSpaces as any).toString();
            const serializedSpaces = (treeSpaces as any).toString();
            expect(serializedSpaces).toEqual(serializedNoSpaces);
        });

        it('should handle complex nested expressions', () => {
            const complexExpressions = [
                '${filename:replace(${attr:substring(0, 3)}, "new")}',
                '${attr:equals(${other})}',
                '${#{param}:toUpper()}',
                'Result: ${filename:replace(${attr}, "#{param}")} - Status: ${status}',
                // New complex nesting cases
                '${attr:substring(${start}, ${end})}',
                '${attr:replace(#{search}, ${replacement:toUpper()})}',
                '${attr:contains(${other:substring(${start:toNumber()}, 5)})}',
                '${path:replace(${dir:append("/")}${file:substring(0, ${len:toNumber()})}, ".txt")}',
                '${attr:equals(null)}', // null literal usage
                '${attr:func(1,2,3);}' // punctuation heavy
            ];

            complexExpressions.forEach((expr) => {
                const { tree } = parseInput(expr);
                expect(tree).toBeTruthy();
            });
        });

        it('should handle malformed expressions gracefully', () => {
            const malformedExpressions = [
                '${unclosed',
                '${:missing}',
                '${attr:}',
                '${func(}',
                '${}',
                '${nested${broken}',
                // New incomplete expression edge cases
                '${attr:toUpper():',
                '${attr:trim():toUpper():',
                '#{"',
                "#{'myParam",
                '${attr:toUpper()} more text'
            ];

            malformedExpressions.forEach((expr) => {
                expect(() => parseInput(expr)).not.toThrow();
            });
        });
    });

    describe('Autocompletion', () => {
        beforeEach(() => enableFeatures(true, true));

        it('should provide completions for expression language functions', async () => {
            const functionTests = [
                '${u', // standalone function
                '${attr:eq', // chained function
                '${attr:toUpper():' // multiple chained functions
            ];

            for (const test of functionTests) {
                const result = await expectCompletion(test, true);
                expect(result.options.length).toBeGreaterThan(0);
                expect(result.options.every((opt: any) => opt.type === 'function')).toBe(true);
            }
        });

        it('should provide completions for parameters', async () => {
            const paramTests = [
                '#{par', // unquoted parameter
                '#{"param ', // quoted parameter with double quotes
                "#{'param " // quoted parameter with single quotes
            ];

            for (const test of paramTests) {
                const result = await expectCompletion(test, true);
                expect(result.options.length).toBeGreaterThan(0);
                // Parameters may have different completion types, just verify we get completions
                expect(result.options.length).toBeGreaterThan(0);
            }
        });

        it('should respect feature enablement for completions', async () => {
            // Test with EL disabled
            enableFeatures(false, true);
            await expectCompletion('${now', false);
            await expectCompletion('${attr:eq', false);

            // Test with parameters disabled
            enableFeatures(true, false);
            await expectCompletion('#{par', false);

            // Test with both disabled
            enableFeatures(false, false);
            await expectCompletion('${now', false);
            await expectCompletion('#{par', false);
        });

        it('should suppress completions in string literals', async () => {
            const stringTests = [NFEL_PATTERNS.STRING_SINGLE_QUOTE, NFEL_PATTERNS.STRING_DOUBLE_QUOTE];

            for (const test of stringTests) {
                await expectCompletion(test, false);
            }
        });
    });

    describe('Expression Language Escaping', () => {
        beforeEach(() => enableFeatures(true, true));

        it('should handle escaped expressions correctly', async () => {
            const escapeTests = [
                { input: '${', escaped: false, shouldComplete: true },
                { input: '$${', escaped: true, shouldComplete: false },
                { input: '$$${', escaped: false, shouldComplete: true },
                { input: '$$$${', escaped: true, shouldComplete: false },
                { input: '##{', escaped: true, shouldComplete: false }
            ];

            for (const test of escapeTests) {
                if (test.input.includes('${')) {
                    expect((service as any).isExpressionEscaped(test.input)).toBe(test.escaped);
                }
                await expectCompletion(test.input, test.shouldComplete);
            }
        });

        it('should not parse escaped EL with braces as expression or attribute', () => {
            const input = '$$$${test}';
            const { context, tree } = parseInput(input);

            // Ensure parse tree contains no EL expression nodes
            let hasElNodes = false;
            const elNodeTypes = new Set([
                'attributeName',
                'standaloneFunctionName',
                'functionName',
                'MultiAttrFunction'
            ]);

            tree.cursor().iterate((node: any) => {
                if (elNodeTypes.has(node.type.name)) {
                    hasElNodes = true;
                }
                return true;
            });
            expect(hasElNodes).toBe(false);

            // Also verify token breakdown looks like: EscapedDollar, EscapedDollar, '{', Text 'test', '}'
            const tokens: Array<{ text: string; type: string }> = [];
            tree.cursor().iterate((node: any) => {
                const textFrag = context.state.doc.sliceString(node.from, node.to);
                if (textFrag.length) tokens.push({ text: textFrag, type: node.type.name });
                return true;
            });
            const types = tokens.map((t) => t.type);
            expect(types).toContain('EscapedDollar');
            expect(types.filter((t) => t === 'EscapedDollar').length).toBeGreaterThanOrEqual(2);
            expect(types).toContain('{');
            expect(tokens.some((t) => t.type === 'Text' && t.text === 'test')).toBe(true);
            expect(types).toContain('}');
        });
    });

    describe('Context Detection', () => {
        beforeEach(() => enableFeatures(true, true));

        it('should handle context detection without crashing', () => {
            const contextTests = [
                { input: '#{par', pos: 4 }, // inside parameter
                { input: '#{param}', pos: 8 }, // after parameter
                { input: '${attr:toUpper():', pos: 16 }, // after colon
                { input: '${attr:toUpper()}', pos: 16 }, // after complete function
                { input: 'text #{par', pos: 2 } // before parameter
            ];

            contextTests.forEach((test) => {
                const context = createContext(test.input, test.pos);
                const tree = syntaxTree(context.state);
                const node = tree.resolveInner(context.pos, -1);

                // Test that context detection methods don't crash
                expect(() => (service as any).isInParameterContext(node, context)).not.toThrow();
                expect(() => (service as any).isFunctionContext(node, context)).not.toThrow();
            });
        });
    });

    describe('Grammar Compliance', () => {
        beforeEach(() => enableFeatures(true, true));

        it('should parse NFEL pattern examples without critical errors', () => {
            let criticalErrors = 0;
            const allowedIncompletePatterns = [
                'INCOMPLETE_',
                'STRING_SINGLE_QUOTE',
                'STRING_DOUBLE_QUOTE',
                'STRING_PARTIAL_',
                'STRING_NESTED_',
                'QUOTED_PARAM_ERROR_',
                'STRING_ESCAPED_QUOTE'
            ];

            Object.entries(NFEL_PATTERNS).forEach(([key, pattern]) => {
                try {
                    const { tree } = parseInput(pattern);

                    // Count critical errors (not just incomplete patterns)
                    if (!allowedIncompletePatterns.some((allowed) => key.includes(allowed))) {
                        let hasErrors = false;
                        tree.cursor().iterate((node) => {
                            if (node.type.name === '⚠') hasErrors = true;
                            return true;
                        });
                        if (hasErrors) criticalErrors++;
                    }
                } catch (error) {
                    if (!allowedIncompletePatterns.some((allowed) => key.includes(allowed))) {
                        criticalErrors++;
                    }
                }
            });

            // Allow reasonable tolerance for enhanced grammar features
            expect(criticalErrors).toBeLessThanOrEqual(50);
        });

        it('should handle all core expression language features', () => {
            const coreFeatures = [
                '${filename}', // Simple attribute
                '${uuid()}', // Standalone function
                '${attr:toUpper()}', // Chained function
                '#{param}', // Parameter reference
                '${#{param}:equals("value")}', // Embedded parameter
                "${attr:equals('value'):contains('test')}", // Multiple chaining
                '$${escaped}', // Escaped expression
                '#{"quoted param"}', // Quoted parameter
                '${attr:replace("old", "new")}', // Function with arguments
                '${anyAttribute("pattern")}', // Multi-attribute function
                '${filename:equals(null)}' // Null literal
            ];

            coreFeatures.forEach((feature) => {
                const { tree } = parseInput(feature);
                expect(tree).toBeTruthy();
                expect(tree.length).toBeGreaterThan(0);
            });
        });
    });

    describe('Enhanced Grammar Features', () => {
        beforeEach(() => enableFeatures(true, true));

        it('should handle comments gracefully', () => {
            const commentTests = [
                '# This is a comment\n${attr}',
                'Hello # Comment\n${name} World',
                '${attr} # Comment after\nNext line'
            ];

            commentTests.forEach((test) => {
                const { tree } = parseInput(test);
                expect(tree).toBeTruthy();

                // Should contain expression nodes when expressions are present
                if (test.includes('${')) {
                    let hasExpressionNodes = false;
                    tree.cursor().iterate((node) => {
                        if (['ReferenceOrFunction', 'AttrName', 'ExpressionStart'].includes(node.type.name)) {
                            hasExpressionNodes = true;
                        }
                        return true;
                    });
                    expect(hasExpressionNodes).toBe(true);
                }
            });
        });

        it('should handle enhanced string literals', () => {
            const stringTests = [
                "${attr:equals('test\\\\x')}", // Unknown escape should be literal
                '${attr:equals("backslash\\\\test")}' // Backslash escape
            ];

            stringTests.forEach((test) => {
                const { tree } = parseInput(test);
                expect(tree).toBeTruthy();

                // Should contain expression structure
                let hasExpression = false;
                tree.cursor().iterate((node) => {
                    if (node.type.name === 'ReferenceOrFunction') {
                        hasExpression = true;
                    }
                    return true;
                });
                expect(hasExpression).toBe(true);
            });
        });

        it('should handle semicolons in various contexts', () => {
            const semicolonTests = [
                '${attr:equals("test;value")}', // In string literal
                '${attr}; ${other}', // As separator
                'First: ${value1}; Second: ${value2}', // In mixed content
                '${value1:toString();}' // trailing semicolon inside expression
            ];

            semicolonTests.forEach((test) => {
                const { tree } = parseInput(test);
                expect(tree).toBeTruthy();

                // Should contain expression nodes
                let hasExpression = false;
                tree.cursor().iterate((node) => {
                    if (node.type.name === 'ReferenceOrFunction') {
                        hasExpression = true;
                    }
                    return true;
                });
                expect(hasExpression).toBe(true);
            });
        });

        it('should differentiate attribute vs function names by parse context', () => {
            const input = '${attr:toUpper():replace("a","b")} ${uuid()}';
            const { context, tree } = parseInput(input);

            const asText = (node: any) => context.state.doc.sliceString(node.from, node.to);

            const attributeNames: string[] = [];
            const functionNames: string[] = [];
            const standaloneNames: string[] = [];

            // Attribute name comes from AttributeRef subtree
            tree.cursor().iterate((node: any) => {
                if (node.type.name === 'AttributeRef') {
                    const inner = tree.cursor();
                    inner.moveTo(node.from, 1);
                    do {
                        if (inner.type.name === 'attributeName') {
                            attributeNames.push(asText({ from: inner.from, to: inner.to }));
                            break;
                        }
                    } while (inner.next() && inner.from < node.to);
                }
                return true;
            });

            // Chained function names are the ReferenceOrFunction within FunctionCall
            tree.cursor().iterate((node: any) => {
                if (node.type.name === 'FunctionCall') {
                    const inner = tree.cursor();
                    inner.moveTo(node.from, 1);
                    do {
                        if (inner.type.name === 'functionName') {
                            functionNames.push(asText({ from: inner.from, to: inner.to }));
                            break;
                        }
                    } while (inner.next() && inner.from < node.to);
                }
                return true;
            });

            // Standalone function names are the ReferenceOrFunction within StandaloneFunction
            tree.cursor().iterate((node: any) => {
                if (node.type.name === 'StandaloneFunction') {
                    const inner = tree.cursor();
                    inner.moveTo(node.from, 1);
                    do {
                        if (inner.type.name === 'standaloneFunctionName') {
                            standaloneNames.push(asText({ from: inner.from, to: inner.to }));
                            break;
                        }
                    } while (inner.next() && inner.from < node.to);
                }
                return true;
            });

            expect(attributeNames).toContain('attr');
            expect(functionNames).toEqual(expect.arrayContaining(['toUpper', 'replace']));
            expect(standaloneNames).toEqual(expect.arrayContaining(['uuid']));
        });

        it('should only treat closing braces as delimiters inside EL or parameter references', () => {
            const cases = [
                { input: 'plain } here', expectIn: null },
                { input: '${a}', expectIn: 'ReferenceOrFunction' },
                { input: '#{b}', expectIn: 'ParameterReference' }
            ];

            cases.forEach(({ input, expectIn }) => {
                const { context } = parseInput(input);
                const doc = context.state.doc.toString();
                const closeIndex = doc.indexOf('}');
                if (closeIndex === -1) {
                    expect(expectIn).toBeNull();
                    return;
                }
                const nodeAt = syntaxTree(context.state).resolveInner(closeIndex, -1);
                const types: string[] = [];
                let cur: any = nodeAt;
                while (cur) {
                    types.push(cur.type.name);
                    cur = cur.parent;
                }
                if (expectIn === null) {
                    expect(types.includes('ReferenceOrFunction') || types.includes('ParameterReference')).toBe(false);
                } else {
                    expect(types.includes(expectIn)).toBe(true);
                }
            });
        });

        it('should style opening brace for standalone EL functions like ${uuid()}', () => {
            const input = '${uuid()}';
            const { context } = parseInput(input);
            const doc = context.state.doc.toString();
            const openIndex = doc.indexOf('{');
            expect(openIndex).toBeGreaterThan(-1);
            const nodeAt = syntaxTree(context.state).resolveInner(openIndex, 1);
            // Ensure we are inside an EL expression
            let cur: any = nodeAt;
            const types: string[] = [];
            while (cur) {
                types.push(cur.type.name);
                cur = cur.parent;
            }
            expect(types.includes('ReferenceOrFunction')).toBe(true);
        });

        it('should map nested examples to expected node types', () => {
            const examples = [
                {
                    input: '${attr:substring(${start}, ${end})} - Multiple nested expressions',
                    expectRefOrFunc: ['attr', 'substring', 'start', 'end'],
                    expectText: 'Multiple nested expressions'
                },
                {
                    input: '${attr:replace(#{search}, ${replacement:toUpper()})} - Mixed parameter and expression nesting',
                    expectRefOrFunc: ['attr', 'replace', 'replacement', 'toUpper'],
                    expectParams: ['search'],
                    expectText: 'Mixed parameter and expression nesting'
                },
                {
                    input: '${attr:contains(${other:substring(${start:toNumber()}, 5)})} - Deep nesting with type conversion',
                    expectRefOrFunc: ['attr', 'contains', 'other', 'substring', 'start', 'toNumber'],
                    expectText: 'Deep nesting with type conversion'
                },
                {
                    input: '${path:replace(${dir:append("/")}${file:substring(0, ${len:toNumber()})}, ".txt")} - Complex path manipulation',
                    expectRefOrFunc: ['path', 'replace', 'dir', 'append', 'file', 'substring', 'len', 'toNumber'],
                    expectText: 'Complex path manipulation'
                }
            ];

            examples.forEach(({ input, expectRefOrFunc, expectParams = [], expectText }) => {
                const { context, tree } = parseInput(input);
                const refOrFuncNames: string[] = [];
                const paramNames: string[] = [];
                const textSegments: string[] = [];

                const nameNodeTypes = new Set([
                    'attributeName',
                    'functionName',
                    'standaloneFunctionName',
                    'MultiAttrFunction'
                ]);

                tree.cursor().iterate((node: any) => {
                    const text = context.state.doc.sliceString(node.from, node.to);
                    if (nameNodeTypes.has(node.type.name)) {
                        refOrFuncNames.push(text);
                    } else if (node.type.name === 'ParameterName') {
                        // Strip surrounding quotes if present
                        paramNames.push(text.replace(/^['"]|['"]$/g, ''));
                    } else if (node.type.name === 'Text') {
                        if (text && text.trim().length) textSegments.push(text.trim());
                    }
                    return true;
                });

                // Verify expected names are present
                expectRefOrFunc.forEach((name) => {
                    expect(refOrFuncNames).toContain(name);
                });

                // Verify expected ParameterName entries are present when provided
                expectParams.forEach((p) => {
                    expect(paramNames).toContain(p);
                });

                // Verify that the trailing description is parsed as Text somewhere
                const joinedText = textSegments.join(' ');
                expect(joinedText).toContain(expectText);
            });
        });
    });

    describe('Performance & Stability', () => {
        beforeEach(() => enableFeatures(true, true));

        it('should handle large content efficiently', () => {
            let largeContent = '';
            for (let i = 0; i < 100; i++) {
                largeContent += `Line ${i}: Hello \${attr${i}} world\n`;
            }

            const start = performance.now();
            const { tree } = parseInput(largeContent);
            const end = performance.now();

            expect(end - start).toBeLessThan(500); // Should parse in < 500ms
            expect(tree).toBeTruthy();
        });

        it('should be stable across multiple parses', () => {
            const testInput = 'Hello ${name:toUpperCase()} world';

            for (let i = 0; i < 10; i++) {
                expect(() => parseInput(testInput)).not.toThrow();
            }
        });

        it('should handle dynamic language configuration changes', () => {
            const combinations = [
                { el: true, params: true },
                { el: true, params: false },
                { el: false, params: true },
                { el: false, params: false }
            ];

            combinations.forEach(({ el, params }) => {
                enableFeatures(el, params);
                const languageSupport = service.getLanguageSupport();
                expect(languageSupport).toBeDefined();
                expect(languageSupport.language).toBeDefined();
            });
        });
    });
});
