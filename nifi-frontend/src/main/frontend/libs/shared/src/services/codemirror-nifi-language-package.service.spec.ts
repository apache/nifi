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
import { of, throwError } from 'rxjs';

import { CodemirrorNifiLanguagePackage } from './codemirror-nifi-language-package.service';
import { ElService } from './el.service';

describe('CodemirrorNifiLanguagePackage', () => {
    let service: CodemirrorNifiLanguagePackage;
    let mockElService: Partial<ElService>;

    // Test data constants
    const MOCK_EL_HTML = `<html><body>
        <div><strong>uuid()</strong> - Generate UUID</div>
        <div><strong>now()</strong> - Current time</div>
        <div><strong>substring(value, start, end)</strong> - Extract substring</div>
    </body></html>`;

    const MOCK_PARAMETERS = [
        { name: 'param1', description: 'Test parameter 1', value: 'value1', sensitive: false },
        { name: 'param.with.dots', description: 'Dotted parameter', value: 'value2', sensitive: false },
        { name: 'param with spaces', description: 'Spaced parameter', value: 'value3', sensitive: false }
    ];

    // Shared test utilities
    const createMockCompletionContext = (text: string, pos: number, explicit: boolean = true) =>
        ({
            pos,
            explicit,
            state: {
                doc: {
                    toString: () => text,
                    lineAt: (position: number) => ({ text, from: 0, to: text.length })
                }
            },
            lineAt: (position: number) => ({ text, from: 0, to: text.length }),
            tokenBefore: () => null,
            matchBefore: () => null,
            aborted: false,
            addEventListener: () => {}
        }) as any;

    const createMockViewContainerRef = () =>
        ({
            createComponent: jest.fn().mockReturnValue({
                instance: {},
                location: { nativeElement: document.createElement('div') }
            }),
            clear: jest.fn(),
            element: { nativeElement: document.createElement('div') },
            length: 0
        }) as any;

    const createLanguageConfig = (supportsEl: boolean, parameterListing?: any[]) => ({
        supportsEl,
        parameterListing
    });

    beforeEach(() => {
        mockElService = {
            getElGuide: jest.fn().mockReturnValue(of(MOCK_EL_HTML))
        };

        TestBed.configureTestingModule({
            providers: [CodemirrorNifiLanguagePackage, { provide: ElService, useValue: mockElService }]
        });

        service = TestBed.inject(CodemirrorNifiLanguagePackage);
    });

    afterEach(() => {
        jest.clearAllMocks();
    });

    describe('Core Functionality', () => {
        it('should create service successfully', () => {
            expect(service).toBeTruthy();
        });

        it('should return correct language ID', () => {
            expect(service.getLanguageId()).toEqual('nf');
        });

        it('should handle EL service errors gracefully', () => {
            (mockElService.getElGuide as jest.Mock).mockReturnValue(throwError(() => new Error('Network error')));

            const result = service.getLanguageMode(createLanguageConfig(true, MOCK_PARAMETERS));

            expect(result).toBeDefined();
            expect(result.streamParser).toBeDefined();
        });
    });

    describe('Language Mode Configuration', () => {
        const testCases = [
            {
                name: 'EL and parameters supported',
                config: createLanguageConfig(true, MOCK_PARAMETERS),
                expectedEl: true,
                expectedParams: true
            },
            {
                name: 'EL only',
                config: createLanguageConfig(true, undefined),
                expectedEl: true,
                expectedParams: false
            },
            {
                name: 'Parameters only',
                config: createLanguageConfig(false, MOCK_PARAMETERS),
                expectedEl: false,
                expectedParams: true
            },
            {
                name: 'Neither supported',
                config: createLanguageConfig(false, undefined),
                expectedEl: false,
                expectedParams: false
            }
        ];

        testCases.forEach(({ name, config, expectedEl, expectedParams }) => {
            it(`should configure language mode: ${name}`, () => {
                const result = service.getLanguageMode(config);

                expect(result).toBeDefined();
                expect(result.streamParser).toBeDefined();
                expect(result.streamParser.startState).toBeDefined();
                expect(result.streamParser.token).toBeDefined();

                // Verify internal state configuration
                service['configureLanguageSupport'](config);
                expect(service['functionSupported']).toBe(expectedEl);
                expect(service['parametersSupported']).toBe(expectedParams);
            });
        });
    });

    describe('Validation Methods', () => {
        beforeEach(() => {
            // Setup test data
            service['parameters'] = ['param1', 'param.with.dots', 'param with spaces'];
            service['functions'] = ['uuid', 'substring', 'contains'];
            service['subjectlessFunctions'] = ['uuid', 'now'];
        });

        describe('Parameter Validation', () => {
            const parameterTestCases = [
                { input: 'param1', supported: true, expected: true },
                { input: 'param.with.dots', supported: true, expected: true },
                { input: 'param with spaces', supported: true, expected: true },
                { input: 'nonexistent', supported: true, expected: false },
                { input: 'param1', supported: false, expected: false }
            ];

            parameterTestCases.forEach(({ input, supported, expected }) => {
                it(`should validate parameter "${input}" when support is ${supported}`, () => {
                    service['parametersSupported'] = supported;
                    expect(service.isValidParameter(input)).toBe(expected);
                });
            });
        });

        describe('EL Function Validation', () => {
            const functionTestCases = [
                { input: 'uuid', supported: true, expected: true, description: 'subjectless function' },
                { input: 'substring', supported: true, expected: true, description: 'regular function' },
                { input: 'nonexistent', supported: true, expected: false, description: 'invalid function' },
                { input: 'uuid()', supported: true, expected: true, description: 'function with parentheses' },
                {
                    input: 'attribute:contains',
                    supported: true,
                    expected: true,
                    description: 'subject:function format'
                },
                { input: 'anyFunction', supported: false, expected: true, description: 'EL not supported' }
            ];

            functionTestCases.forEach(({ input, supported, expected, description }) => {
                it(`should validate EL function: ${description}`, () => {
                    service['functionSupported'] = supported;
                    expect(service.isValidElFunction(input)).toBe(expected);
                });
            });
        });
    });

    describe('Autocompletion', () => {
        let mockViewContainerRef: any;

        beforeEach(() => {
            mockViewContainerRef = createMockViewContainerRef();
            service['parameters'] = ['param1', 'parameter', 'test'];
            service['functions'] = ['uuid', 'substring', 'contains'];
            service['parametersSupported'] = true;
            service['functionSupported'] = true;
        });

        const autocompletionTestCases = [
            {
                name: 'parameter completion',
                context: '#{param}',
                pos: 7,
                explicit: true,
                expectResults: true
            },
            {
                name: 'EL function completion',
                context: '${uuid}',
                pos: 6,
                explicit: true,
                expectResults: true
            },
            {
                name: 'no completion for non-explicit context',
                context: '#{param}',
                pos: 7,
                explicit: false,
                expectResults: false
            },
            {
                name: 'no completion outside expressions',
                context: 'normal text',
                pos: 6,
                explicit: true,
                expectResults: false
            }
        ];

        autocompletionTestCases.forEach(({ name, context, pos, explicit, expectResults }) => {
            it(`should handle ${name}`, () => {
                const handler = service['createAutocompletionHandler'](mockViewContainerRef);
                const mockContext = createMockCompletionContext(context, pos, explicit);

                const result = handler(mockContext);

                if (expectResults) {
                    expect(result).toBeDefined();
                    if (result) {
                        expect(result.options).toBeDefined();
                    }
                } else {
                    expect(result).toBeNull();
                }
            });
        });
    });

    describe('Helper Methods', () => {
        describe('extractBaseFunctionName', () => {
            const extractionTestCases = [
                { input: 'attribute:contains', expected: 'contains' },
                { input: 'allAttributes()', expected: 'allAttributes' },
                { input: 'attribute:startsWith("prefix")', expected: 'startsWith' },
                { input: 'simpleFunction', expected: 'simpleFunction' },
                { input: ':functionName', expected: 'functionName' }
            ];

            extractionTestCases.forEach(({ input, expected }) => {
                it(`should extract "${expected}" from "${input}"`, () => {
                    expect(service['extractBaseFunctionName'](input)).toBe(expected);
                });
            });
        });

        describe('filterOptionsWithSpaceSupport', () => {
            const options = ['exact', 'exactMatch', 'param with spaces', 'MixedCase', 'param123'];

            const filterTestCases = [
                { input: 'exact', expectedCount: 2, description: 'exact matches' },
                { input: 'param', expectedCount: 2, description: 'partial matches' },
                { input: 'mixed', expectedCount: 1, description: 'case insensitive' },
                { input: '', expectedCount: 5, description: 'empty input returns all' },
                { input: 'nomatch', expectedCount: 0, description: 'no matches' }
            ];

            filterTestCases.forEach(({ input, expectedCount, description }) => {
                it(`should filter options: ${description}`, () => {
                    const result = service['filterOptionsWithSpaceSupport'](options, input);
                    expect(result.length).toBe(expectedCount);
                });
            });
        });
    });

    describe('Stream Parser Integration', () => {
        it('should create functional stream parser', () => {
            const config = createLanguageConfig(true, MOCK_PARAMETERS);
            const result = service.getLanguageMode(config);

            expect(result.streamParser.startState).toBeInstanceOf(Function);
            expect(result.streamParser.token).toBeInstanceOf(Function);

            // Test basic tokenization
            const mockStream = {
                string: 'test',
                pos: 0,
                start: 0,
                eol: () => false,
                sol: () => true,
                peek: () => 't',
                next: () => 't',
                eat: () => false,
                eatWhile: () => false,
                eatSpace: () => false,
                skipToEnd: () => {},
                skipTo: () => false,
                match: () => false,
                backUp: () => {},
                column: () => 0,
                indentation: () => 0,
                current: () => 'test'
            } as any;

            if (result.streamParser.startState) {
                const state = result.streamParser.startState(4); // Standard indent unit
                const token = result.streamParser.token(mockStream, state);
                expect(token).toBeDefined();
            }
        });
    });

    describe('Advanced Tokenization & Coverage', () => {
        let mockStream: any;
        let mockStates: any;

        beforeEach(() => {
            mockStream = {
                string: '',
                pos: 0,
                start: 0,
                eol: () => false,
                sol: () => true,
                peek: () => undefined,
                next: () => undefined,
                eat: () => false,
                eatWhile: () => false,
                eatSpace: () => false,
                skipToEnd: () => {},
                skipTo: () => false,
                match: () => false,
                backUp: () => {},
                column: () => 0,
                indentation: () => 0,
                current: () => ''
            };

            mockStates = {
                get: () => ({ context: 'start' }),
                copy: () => mockStates,
                push: jest.fn(),
                pop: jest.fn().mockReturnValue({ context: 'expression' })
            };
        });

        it('should handle complex tokenization paths', () => {
            expect(() => service['tokenize'](mockStream, mockStates)).not.toThrow();
        });

        it('should handle parameter context tokenization', () => {
            service['parametersSupported'] = true;
            mockStates.get = () => ({ context: 'parameter' });

            const result = service['handleParameterContext'](mockStream, mockStates, mockStates.get(), '#');
            expect(result).toBeDefined();
        });

        it('should handle expression context tokenization', () => {
            service['functionSupported'] = true;
            mockStates.get = () => ({ context: 'expression' });

            const result = service['handleExpressionContext'](mockStream, mockStates, mockStates.get(), '$');
            expect(result).toBeDefined();
        });

        it('should handle default context with various characters', () => {
            const testChars = ['$', '#', '}', '"', "'", 't'];
            testChars.forEach((char) => {
                const result = service['handleDefaultContext'](mockStream, mockStates, char);
                expect(result).toBeDefined();
            });
        });

        it('should handle string literal detection', () => {
            mockStream.peek = () => '"';
            const result = service['handleStringLiteral'](mockStream, mockStates);
            expect(result).toBeDefined();
        });

        it('should handle comment detection', () => {
            mockStream.peek = () => '/';
            mockStream.string = '// comment';
            const result = service['isCommentStart'](mockStream, '/');
            expect(typeof result).toBe('boolean');
        });
    });

    describe('Advanced Boundary Detection', () => {
        it('should handle findParameterBoundaries with complex scenarios', () => {
            const testCases = [
                { text: 'start #{param1} end', cursor: 10, expected: 'param1' },
                { text: 'nested #{outer #{inner} end}', cursor: 18, expected: 'inner' },
                { text: 'no parameters here', cursor: 10, expected: null }
            ];

            testCases.forEach((testCase) => {
                const result = service['findParameterBoundaries'](testCase.text, testCase.cursor, 0);
                if (testCase.expected === null) {
                    expect(result).toBeNull();
                } else {
                    expect(result).toBeDefined();
                }
            });
        });

        it('should not delete text after cursor when autocompletion boundaries are detected for parameters', () => {
            // Scenario: user types "#{" at the beginning of "example text"
            // Cursor is at position 2 (after "#{")
            const result = service['findParameterBoundaries']('#{example text', 2, 0);

            expect(result).toBeDefined();
            expect(result?.from).toBe(2); // Position after #{
            expect(result?.to).toBe(2); // Should only replace up to cursor, not the "example text"
            expect(result?.currentText).toBe(''); // Empty text between #{ and cursor
        });

        it('should not delete text after cursor when parameter has no closing brace', () => {
            // Scenario: user types "#{param" in the middle of "some example text"
            // Cursor is at position 8 (at "m" of "more")
            const result = service['findParameterBoundaries']('#{param more text', 8, 0);

            expect(result).toBeDefined();
            expect(result?.from).toBe(2); // Position after #{
            expect(result?.to).toBe(8); // Should only replace up to cursor
            expect(result?.currentText).toBe('param '); // Text between #{ and cursor (includes space)
        });

        it('should properly handle parameter boundaries when closing brace exists after cursor', () => {
            // Scenario: "#{param} more text" with cursor at position 7 (between 'm' and '}')
            const result = service['findParameterBoundaries']('#{param} more text', 7, 0);

            expect(result).toBeDefined();
            expect(result?.from).toBe(2); // Position after #{
            expect(result?.to).toBe(7); // Should only replace up to cursor, not to closing brace
            expect(result?.currentText).toBe('param'); // Text between #{ and cursor
        });

        it('should handle findElFunctionBoundaries with complex scenarios', () => {
            const testCases = [
                { text: 'start ${func1()} end', cursor: 10, expected: 'func1()' },
                { text: 'nested ${outer(${inner()})}', cursor: 18, expected: 'inner()' },
                { text: 'no functions here', cursor: 10, expected: null }
            ];

            testCases.forEach((testCase) => {
                const result = service['findElFunctionBoundaries'](testCase.text, testCase.cursor, 0);
                if (testCase.expected === null) {
                    expect(result).toBeNull();
                } else {
                    expect(result).toBeDefined();
                }
            });
        });

        it('should not delete text after cursor when autocompletion boundaries are detected for EL functions', () => {
            // Scenario: user types "${" at the beginning of "example text"
            // Cursor is at position 2 (after "${")
            const result = service['findElFunctionBoundaries']('${example text', 2, 0);

            expect(result).toBeDefined();
            expect(result?.from).toBe(2); // Position after ${
            expect(result?.to).toBe(2); // Should only replace up to cursor, not the "example text"
            expect(result?.currentText).toBe(''); // Empty text between ${ and cursor
        });

        it('should not delete text after cursor when EL function has no closing brace', () => {
            // Scenario: user types "${uuid" in the middle of "some example text"
            // Cursor is at position 6 (after "${uuid")
            const result = service['findElFunctionBoundaries']('${uuid more text', 6, 0);

            expect(result).toBeDefined();
            expect(result?.from).toBe(2); // Position after ${
            expect(result?.to).toBe(6); // Should only replace up to cursor
            expect(result?.currentText).toBe('uuid'); // Text between ${ and cursor
        });

        it('should properly handle EL function boundaries when closing brace exists after cursor', () => {
            // Scenario: "${uuid()} more text" with cursor at position 7 (at closing parenthesis ')')
            const result = service['findElFunctionBoundaries']('${uuid()} more text', 7, 0);

            expect(result).toBeDefined();
            expect(result?.from).toBe(2); // Position after ${
            expect(result?.to).toBe(7); // Should only replace up to cursor, not to closing brace
            expect(result?.currentText).toBe('uuid('); // Text between ${ and cursor (up to ')')
        });

        it('should handle analyzeCursorContext with nested expressions', () => {
            const testCases = [
                { text: 'Simple #{param} text', pos: 10, expectedInParam: true },
                { text: 'Simple ${func()} text', pos: 10, expectedInEl: true },
                { text: 'Nested ${outer(#{inner})} complex', pos: 18, expectedInParam: true }
            ];

            testCases.forEach((testCase) => {
                const mockContext = {
                    pos: testCase.pos,
                    state: {
                        doc: { lineAt: () => ({ text: testCase.text, from: 0, to: testCase.text.length }) }
                    }
                } as any;

                const result = service['analyzeCursorContext'](mockContext);
                expect(result).toBeDefined();
            });
        });
    });

    describe('Advanced Autocompletion Scenarios', () => {
        let mockViewContainerRef: any;

        beforeEach(() => {
            mockViewContainerRef = createMockViewContainerRef();
            service['parameters'] = ['param1', 'parameter', 'complex.nested.param'];
            service['functions'] = ['uuid', 'substring', 'contains'];
            service['functionDetails'] = {
                uuid: { description: 'Generate UUID', returnType: 'String', name: 'uuid', args: {} },
                substring: { description: 'Extract substring', returnType: 'String', name: 'substring', args: {} }
            };
        });

        it('should handle createCompletions with function details', () => {
            const options = ['uuid', 'substring'];
            const tokenValue = 'u';
            const cursorContext = { type: 'expression', useFunctionDetails: true };
            const mockContext = createMockCompletionContext('${u}', 3, true);

            const result = service['createCompletions'](
                options,
                tokenValue,
                cursorContext,
                mockViewContainerRef,
                mockContext
            );

            expect(result).toBeDefined();
            expect(Array.isArray(result)).toBe(true);
        });

        it('should handle getCompletionOptions for parameters', () => {
            service['parametersSupported'] = true;
            const cursorContext = {
                isInParameterExpression: true,
                isInElExpression: false,
                currentText: 'param',
                parameterSupported: true,
                functionSupported: false
            };

            const result = service['getCompletionOptions'](cursorContext);
            expect(Array.isArray(result)).toBe(true);
        });

        it('should handle getCompletionOptions for EL functions', () => {
            service['functionSupported'] = true;
            const cursorContext = {
                isInParameterExpression: false,
                isInElExpression: true,
                currentText: 'u',
                parameterSupported: false,
                functionSupported: true
            };

            const result = service['getCompletionOptions'](cursorContext);
            expect(Array.isArray(result)).toBe(true);
        });

        it('should position cursor at end of inserted text when completion is applied', () => {
            const options = ['uuid'];
            const completions = service['createCompletions'](
                options,
                'u',
                { useFunctionDetails: true },
                mockViewContainerRef,
                createMockCompletionContext('${u}', 3, true)
            );

            expect(completions).toHaveLength(1);
            expect(completions[0].apply).toBeDefined();

            // Mock view object with dispatch method
            const mockView = {
                dispatch: jest.fn()
            };

            // Apply the completion
            const from = 10;
            const to = 11;
            if (typeof completions[0].apply === 'function') {
                completions[0].apply(mockView as any, completions[0], from, to);
            }

            // Verify that dispatch was called with cursor positioned at end of inserted text
            expect(mockView.dispatch).toHaveBeenCalledWith({
                changes: {
                    from: from,
                    to: to,
                    insert: 'uuid'
                },
                selection: { anchor: from + 'uuid'.length } // Cursor at end of 'uuid'
            });
        });

        it('should position cursor correctly for auto-insertion with single match', () => {
            // This test verifies the auto-insertion logic in createAutocompletionHandler
            const mockView = {
                dispatch: jest.fn()
            };

            const mockContext = {
                ...createMockCompletionContext('${exact}', 7, true),
                view: mockView
            };

            // Mock analyzeCursorContext to return expression context
            jest.spyOn(service as any, 'analyzeCursorContext').mockReturnValue({
                type: CodemirrorNifiLanguagePackage['CONTEXT_EXPRESSION'],
                useFunctionDetails: true
            });

            // Mock getCompletionOptions to return single match
            jest.spyOn(service as any, 'getCompletionOptions').mockReturnValue(['exactMatch']);

            // Mock findExpressionBoundaries to return boundary info
            jest.spyOn(service as any, 'findExpressionBoundaries').mockReturnValue({
                from: 5,
                to: 10,
                currentText: 'exact'
            });

            // Create autocompletion handler
            const handler = service['createAutocompletionHandler'](mockViewContainerRef);

            // Call the handler
            const result = handler(mockContext);

            // Should return null because of auto-insertion
            expect(result).toBeNull();

            // Verify dispatch was called with cursor positioning
            expect(mockView.dispatch).toHaveBeenCalledWith({
                changes: {
                    from: 5,
                    to: 10,
                    insert: 'exactMatch'
                },
                selection: { anchor: 5 + 'exactMatch'.length } // Cursor at end
            });
        });
    });

    describe('Private Method Coverage', () => {
        it('should handle setParameters with complex parameter types', () => {
            const complexParams = [
                { name: 'simple', description: 'Simple param', sensitive: false, value: 'value1' },
                { name: 'param.with.dots', description: 'Dotted param', sensitive: false, value: 'value2' },
                { name: 'param with spaces', description: 'Spaced param', sensitive: false, value: 'value3' },
                { name: '', description: 'Empty name param', sensitive: false, value: 'value4' }
            ];

            service['setParameters'](complexParams);
            expect(service['parameters']).toContain('simple');
            expect(service['parameters']).toContain('param.with.dots');
            expect(service['parameters']).toContain('param with spaces');
        });

        it('should handle buildFunctionRegexes', () => {
            service['functions'] = ['uuid', 'now', 'substring'];
            service['subjectlessFunctions'] = ['uuid', 'now'];

            expect(() => service['buildFunctionRegexes']()).not.toThrow();
            expect(service['functionRegex']).toBeDefined();
            expect(service['subjectlessFunctionRegex']).toBeDefined();
        });

        it('should handle parseElGuide with rich HTML content', () => {
            const richHtml = `
                <div>
                    <h3>Expression Language Functions</h3>
                    <div><strong>uuid()</strong> - Generate UUID</div>
                    <div><strong>now()</strong> - Current time</div>
                    <div><strong>substring(value, start, end)</strong> - Extract substring</div>
                </div>
            `;

            expect(() => service['parseElGuide'](richHtml)).not.toThrow();
        });

        it('should handle extractFunctionData', () => {
            const mockElement = document.createElement('div');
            mockElement.innerHTML = '<strong>uuid()</strong> - Generates a UUID';

            const result = service['extractFunctionData'](mockElement);
            expect(result).toBeDefined();
        });

        it('should handle storeFunctionData', () => {
            const functionData = {
                functionName: 'testFunction',
                subjectless: true,
                description: 'Test description',
                usage: 'testFunction()',
                returnType: 'String'
            };

            expect(() => service['storeFunctionData'](functionData)).not.toThrow();
        });

        it('should handle buildStates functionality', () => {
            const initialStates = [{ name: 'start' }, { name: 'expression' }];
            const result = service['buildStates'](initialStates);

            expect(result).toBeDefined();
            expect(result.get).toBeDefined();
            expect(result.push).toBeDefined();
            expect(result.pop).toBeDefined();
            expect(result.copy).toBeDefined();
        });

        it('should handle cleanupTooltip', () => {
            const mockElement = document.createElement('div');
            mockElement.setAttribute('data-nifi-tooltip', 'true');

            service['cleanupTooltip'](mockElement);
            expect(mockElement.getAttribute('data-nifi-tooltip')).toBeNull();
        });
    });

    describe('Error Handling & Edge Cases', () => {
        it('should handle malformed HTML gracefully', () => {
            const malformedHtml = '<html><body><h1>Incomplete HTML';
            (mockElService.getElGuide as jest.Mock).mockReturnValue(of(malformedHtml));

            expect(() => {
                service.getLanguageMode(createLanguageConfig(true, MOCK_PARAMETERS));
            }).not.toThrow();
        });

        it('should handle empty parameter lists', () => {
            expect(() => {
                service.getLanguageMode(createLanguageConfig(true, []));
            }).not.toThrow();
        });

        it('should handle undefined parameter listing', () => {
            expect(() => {
                service.getLanguageMode(createLanguageConfig(false, undefined));
            }).not.toThrow();
        });

        it('should handle large function lists efficiently', () => {
            const largeFunctionList = Array.from({ length: 100 }, (_, i) => `function_${i}`);
            service['functions'] = largeFunctionList;
            service['subjectlessFunctions'] = largeFunctionList.slice(0, 50);

            expect(() => service['buildFunctionRegexes']()).not.toThrow();
        });
    });
});
