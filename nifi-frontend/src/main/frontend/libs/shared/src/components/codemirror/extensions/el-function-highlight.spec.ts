/*!
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

// Capture calls to mock decorations for verification
const capturedDecorations: any[] = [];

// Simple mocks for CodeMirror dependencies
jest.mock('@codemirror/view', () => ({
    ViewPlugin: {
        fromClass: jest.fn((pluginClass, options) => ({ pluginClass, options }))
    },
    Decoration: {
        mark: jest.fn((attrs) => ({
            range: jest.fn((from, to) => {
                const decoration = { type: 'mark', from, to, attrs };
                capturedDecorations.push(decoration);
                return decoration;
            })
        })),
        replace: jest.fn((attrs) => ({
            range: jest.fn((from, to) => {
                const decoration = { type: 'replace', from, to, attrs };
                capturedDecorations.push(decoration);
                return decoration;
            })
        })),
        set: jest.fn((decorations) => ({
            decorations: decorations || [],
            size: decorations ? decorations.length : 0
        }))
    },
    WidgetType: class MockWidgetType {
        constructor() {}
        eq(other: any) {
            return false;
        }
        toDOM() {
            return document.createElement('span');
        }
        ignoreEvent() {
            return false;
        }
    }
}));

import { elFunctionHighlightPlugin } from './el-function-highlight';

// Type assertion for accessing mocked plugin properties
const plugin = elFunctionHighlightPlugin as any;

describe('EL Function Highlight Extension', () => {
    let PluginClass: any;

    beforeEach(() => {
        jest.clearAllMocks();
        capturedDecorations.length = 0;
        PluginClass = plugin.pluginClass;
    });

    describe('Plugin Configuration', () => {
        it('should export a properly configured plugin', () => {
            expect(elFunctionHighlightPlugin).toBeDefined();
            expect(plugin).toHaveProperty('pluginClass');
            expect(plugin).toHaveProperty('options');
        });

        it('should have decorations configuration that returns instance decorations', () => {
            expect(plugin.options).toHaveProperty('decorations');
            expect(typeof plugin.options.decorations).toBe('function');

            // Test the decorations function
            const testDecorations = { test: 'decorations' };
            const instance = { decorations: testDecorations };
            const result = plugin.options.decorations(instance);
            expect(result).toBe(testDecorations);
        });
    });

    describe('Plugin Class Implementation', () => {
        describe('Constructor Behavior', () => {
            it('should initialize with decorations', () => {
                const instance = new PluginClass(createMockView(['Hello ${world}']));
                expect(instance).toHaveProperty('decorations');
            });

            it('should create proper decorations for EL functions', () => {
                const instance = new PluginClass(createMockView(['Text with ${function1} and ${function2}']));

                // Should create 8 decorations (4 per function: dollar, open brace, function name, close brace)
                expect(capturedDecorations.length).toBe(8);

                // Verify decoration types and counts
                const decorationCounts = getDecorationCounts();
                expect(decorationCounts.dollar).toBe(2); // One '$' for each function
                expect(decorationCounts.bracket).toBe(4); // Two braces per function
                expect(decorationCounts.replace).toBe(2); // One widget per function
            });

            it('should handle documents without EL functions', () => {
                new PluginClass(createMockView(['Plain text without EL functions']));
                expect(capturedDecorations).toHaveLength(0);

                new PluginClass(createMockView(['']));
                expect(capturedDecorations).toHaveLength(0);
            });

            it('should process multiple lines correctly', () => {
                new PluginClass(
                    createMockView(['Line 1 has ${function1}', 'Line 2 has ${function2}', 'Line 3 has no functions'])
                );

                const replaceDecorations = capturedDecorations.filter((d) => d.type === 'replace');
                expect(replaceDecorations.length).toBe(2);
            });
        });

        describe('Update Method', () => {
            it('should update decorations when document or viewport changes', () => {
                const instance = new PluginClass(createMockView(['${oldFunction}']));

                // Test document change
                capturedDecorations.length = 0;
                instance.update({
                    docChanged: true,
                    viewportChanged: false,
                    view: createMockView(['${newFunction}'])
                });
                expect(capturedDecorations.length).toBeGreaterThan(0);

                // Test viewport change
                capturedDecorations.length = 0;
                instance.update({
                    docChanged: false,
                    viewportChanged: true,
                    view: createMockView(['${function}'])
                });
                expect(capturedDecorations.length).toBeGreaterThan(0);
            });

            it('should not update when nothing changes', () => {
                const mockView = createMockView(['${function}']);
                const instance = new PluginClass(mockView);

                capturedDecorations.length = 0;
                instance.update({
                    docChanged: false,
                    viewportChanged: false,
                    view: mockView
                });

                expect(capturedDecorations).toHaveLength(0);
            });
        });

        describe('Position Calculations', () => {
            it('should calculate correct positions for decorations', () => {
                new PluginClass(createMockView(['Start ${middle} end']));

                // Sort decorations by position to verify correct ordering
                capturedDecorations.sort((a, b) => a.from - b.from);

                expect(capturedDecorations[0].from).toBe(6); // '$' position
                expect(capturedDecorations[1].from).toBe(7); // '{' position
                expect(capturedDecorations[2].from).toBe(8); // start of function name
                expect(capturedDecorations[3].from).toBe(14); // '}' position
            });

            it('should handle adjacent functions and maintain sorted order', () => {
                new PluginClass(createMockView(['${function1}${function2}']));

                // Should create 8 decorations (4 per function)
                expect(capturedDecorations).toHaveLength(8);

                // Verify they're in correct order
                capturedDecorations.sort((a, b) => a.from - b.from);
                expect(capturedDecorations[0].from).toBe(0); // First '$'
                expect(capturedDecorations[4].from).toBe(12); // Second '$'

                // Verify all decorations are properly sorted
                const positions = capturedDecorations.map((d) => d.from);
                const sortedPositions = [...positions].sort((a, b) => a - b);
                expect(positions).toEqual(sortedPositions);
            });
        });
    });

    describe('EL Function Recognition Patterns', () => {
        it('should recognize valid EL function patterns', () => {
            const validCases = [
                { input: '${simple}', expectedFunctions: 1 },
                { input: '${function_with_underscore}', expectedFunctions: 1 },
                { input: '${function-with-dash}', expectedFunctions: 1 },
                { input: '${function123}', expectedFunctions: 1 },
                { input: '${function.with.dots}', expectedFunctions: 1 },
                { input: '${function1} ${function2}', expectedFunctions: 2 },
                { input: '${fn:length(value)}', expectedFunctions: 1 },
                { input: '${math:add(1, 2)}', expectedFunctions: 1 },
                { input: '${functioné} ${函数} ${функция}', expectedFunctions: 3 }
            ];

            validCases.forEach(({ input, expectedFunctions }) => {
                const functionCount = testFunctionRecognition(input);
                expect(functionCount).toBe(expectedFunctions);
            });
        });

        it('should handle malformed and edge case patterns', () => {
            const testCases = [
                { input: '${}', expectedFunctions: 0, description: 'empty functions' },
                { input: '${ }', expectedFunctions: 1, description: 'space-only functions (matches current regex)' },
                { input: '{function}', expectedFunctions: 0, description: 'missing dollar sign' },
                { input: '$function', expectedFunctions: 0, description: 'missing braces' },
                { input: '${function', expectedFunctions: 0, description: 'missing closing brace' },
                { input: 'function}', expectedFunctions: 0, description: 'missing dollar and opening brace' },
                {
                    input: '${function{with}braces}',
                    expectedFunctions: 1,
                    description: 'nested braces (stops at first closing)'
                }
            ];

            testCases.forEach(({ input, expectedFunctions, description }) => {
                const functionCount = testFunctionRecognition(input);
                expect(functionCount).toBe(expectedFunctions);
            });
        });

        it('should handle extreme cases', () => {
            // Very long function name
            const longFunction = 'a'.repeat(1000);
            expect(() => testFunctionRecognition(`\${${longFunction}}`)).not.toThrow();
            expect(testFunctionRecognition(`\${${longFunction}}`)).toBe(1);

            // Many functions
            const manyFunctions = Array.from({ length: 50 }, (_, i) => `\${function${i}}`).join(' ');
            expect(testFunctionRecognition(manyFunctions)).toBe(50);
        });

        it('should handle complex function calls with parentheses', () => {
            const complexCases = [
                { input: '${fn:length()}', expectedFunctions: 1 },
                { input: '${fn:substring(value, 0, 5)}', expectedFunctions: 1 },
                { input: '${math:add(${var1}, ${var2})}', expectedFunctions: 2 }, // Nested functions (stops at first closing brace)
                { input: '${fn:contains("hello (world)", "(")}', expectedFunctions: 1 },
                { input: '${fn:escapeXml("test")} ${fn:length("hello")}', expectedFunctions: 2 }
            ];

            complexCases.forEach(({ input, expectedFunctions }) => {
                const functionCount = testFunctionRecognition(input);
                expect(functionCount).toBe(expectedFunctions);
            });
        });
    });

    describe('ELFunctionNameWidget Functionality', () => {
        // Since we're mocking WidgetType, we'll test the widget indirectly through the plugin
        it('should create widgets for function names', () => {
            new PluginClass(createMockView(['${testFunction}']));

            const replaceDecorations = capturedDecorations.filter((d) => d.type === 'replace');
            expect(replaceDecorations).toHaveLength(1);
            expect(replaceDecorations[0].attrs).toHaveProperty('widget');
        });

        it('should handle functions with parentheses', () => {
            new PluginClass(createMockView(['${fn:length(value)}']));

            const replaceDecorations = capturedDecorations.filter((d) => d.type === 'replace');
            expect(replaceDecorations).toHaveLength(1);
        });

        it('should handle multiple complex functions', () => {
            new PluginClass(createMockView(['${fn:length(str)} and ${math:add(1, 2)}']));

            const replaceDecorations = capturedDecorations.filter((d) => d.type === 'replace');
            expect(replaceDecorations).toHaveLength(2);
        });
    });
});

// Helper functions
function createMockView(lines: string[]) {
    return {
        state: {
            doc: {
                lines: lines.length,
                line: jest.fn().mockImplementation((lineNumber: number) => {
                    const lineIndex = lineNumber - 1;
                    if (lineIndex >= 0 && lineIndex < lines.length) {
                        const lineText = lines[lineIndex];
                        const from = lines.slice(0, lineIndex).join('\n').length + (lineIndex > 0 ? 1 : 0); // Add 1 for newline characters
                        return {
                            from,
                            to: from + lineText.length,
                            text: lineText,
                            length: lineText.length,
                            number: lineNumber
                        };
                    }
                    return { from: 0, to: 0, text: '', length: 0, number: lineNumber };
                })
            }
        }
    };
}

function getDecorationCounts() {
    return {
        dollar: capturedDecorations.filter((d) => d.attrs?.class === 'cm-el-function-dollar-sign').length,
        bracket: capturedDecorations.filter((d) => d.attrs?.class === 'cm-bracket').length,
        replace: capturedDecorations.filter((d) => d.type === 'replace').length
    };
}

function testFunctionRecognition(input: string): number {
    capturedDecorations.length = 0;
    const mockView = createMockView([input]);
    new (elFunctionHighlightPlugin as any).pluginClass(mockView);
    return capturedDecorations.filter((d) => d.type === 'replace').length;
}
