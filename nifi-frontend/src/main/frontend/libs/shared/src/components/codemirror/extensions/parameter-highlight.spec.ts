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

import { parameterHighlightPlugin } from './parameter-highlight';

// Type assertion for accessing mocked plugin properties
const plugin = parameterHighlightPlugin as any;

describe('Parameter Highlight Extension', () => {
    let PluginClass: any;

    beforeEach(() => {
        jest.clearAllMocks();
        capturedDecorations.length = 0;
        PluginClass = plugin.pluginClass;
    });

    describe('Plugin Configuration', () => {
        it('should export a properly configured plugin', () => {
            expect(parameterHighlightPlugin).toBeDefined();
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
                const instance = new PluginClass(createMockView(['Hello #{world}']));
                expect(instance).toHaveProperty('decorations');
            });

            it('should create proper decorations for parameters', () => {
                const instance = new PluginClass(createMockView(['Text with #{param1} and #{param2}']));

                // Should create 8 decorations (4 per parameter: hash, open brace, param name, close brace)
                expect(capturedDecorations.length).toBe(8);

                // Verify decoration types and counts
                const decorationCounts = getDecorationCounts();
                expect(decorationCounts.hash).toBe(2); // One '#' for each parameter
                expect(decorationCounts.bracket).toBe(4); // Two braces per parameter
                expect(decorationCounts.replace).toBe(2); // One widget per parameter
            });

            it('should handle documents without parameters', () => {
                new PluginClass(createMockView(['Plain text without parameters']));
                expect(capturedDecorations).toHaveLength(0);

                new PluginClass(createMockView(['']));
                expect(capturedDecorations).toHaveLength(0);
            });

            it('should process multiple lines correctly', () => {
                new PluginClass(
                    createMockView(['Line 1 has #{param1}', 'Line 2 has #{param2}', 'Line 3 has no params'])
                );

                const replaceDecorations = capturedDecorations.filter((d) => d.type === 'replace');
                expect(replaceDecorations.length).toBe(2);
            });
        });

        describe('Update Method', () => {
            it('should update decorations when document or viewport changes', () => {
                const instance = new PluginClass(createMockView(['#{oldParam}']));

                // Test document change
                capturedDecorations.length = 0;
                instance.update({
                    docChanged: true,
                    viewportChanged: false,
                    view: createMockView(['#{newParam}'])
                });
                expect(capturedDecorations.length).toBeGreaterThan(0);

                // Test viewport change
                capturedDecorations.length = 0;
                instance.update({
                    docChanged: false,
                    viewportChanged: true,
                    view: createMockView(['#{param}'])
                });
                expect(capturedDecorations.length).toBeGreaterThan(0);
            });

            it('should not update when nothing changes', () => {
                const mockView = createMockView(['#{param}']);
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
                new PluginClass(createMockView(['Start #{middle} end']));

                // Sort decorations by position to verify correct ordering
                capturedDecorations.sort((a, b) => a.from - b.from);

                expect(capturedDecorations[0].from).toBe(6); // '#' position
                expect(capturedDecorations[1].from).toBe(7); // '{' position
                expect(capturedDecorations[2].from).toBe(8); // start of parameter name
                expect(capturedDecorations[3].from).toBe(14); // '}' position
            });

            it('should handle adjacent parameters and maintain sorted order', () => {
                new PluginClass(createMockView(['#{param1}#{param2}']));

                // Should create 8 decorations (4 per parameter)
                expect(capturedDecorations).toHaveLength(8);

                // Verify they're in correct order
                capturedDecorations.sort((a, b) => a.from - b.from);
                expect(capturedDecorations[0].from).toBe(0); // First '#'
                expect(capturedDecorations[4].from).toBe(9); // Second '#'

                // Verify all decorations are properly sorted
                const positions = capturedDecorations.map((d) => d.from);
                const sortedPositions = [...positions].sort((a, b) => a - b);
                expect(positions).toEqual(sortedPositions);
            });
        });
    });

    describe('Parameter Recognition Patterns', () => {
        it('should recognize valid parameter patterns', () => {
            const validCases = [
                { input: '#{simple}', expectedParams: 1 },
                { input: '#{param_with_underscore}', expectedParams: 1 },
                { input: '#{param-with-dash}', expectedParams: 1 },
                { input: '#{param123}', expectedParams: 1 },
                { input: '#{param.with.dots}', expectedParams: 1 },
                { input: '#{param1} #{param2}', expectedParams: 2 },
                { input: '#{param@domain.com} #{param:value}', expectedParams: 2 },
                { input: '#{paramété} #{参数} #{параметр}', expectedParams: 3 }
            ];

            validCases.forEach(({ input, expectedParams }) => {
                const paramCount = testParameterRecognition(input);
                expect(paramCount).toBe(expectedParams);
            });
        });

        it('should handle malformed and edge case patterns', () => {
            const testCases = [
                { input: '#{}', expectedParams: 0, description: 'empty parameters' },
                { input: '#{ }', expectedParams: 1, description: 'space-only parameters (matches current regex)' },
                { input: '{param}', expectedParams: 0, description: 'missing hash' },
                { input: '#param', expectedParams: 0, description: 'missing braces' },
                { input: '#{param', expectedParams: 0, description: 'missing closing brace' },
                { input: 'param}', expectedParams: 0, description: 'missing hash and opening brace' },
                {
                    input: '#{param{with}braces}',
                    expectedParams: 1,
                    description: 'nested braces (stops at first closing)'
                }
            ];

            testCases.forEach(({ input, expectedParams, description }) => {
                const paramCount = testParameterRecognition(input);
                expect(paramCount).toBe(expectedParams);
            });
        });

        it('should handle extreme cases', () => {
            // Very long parameter name
            const longParam = 'a'.repeat(1000);
            expect(() => testParameterRecognition(`#{${longParam}}`)).not.toThrow();
            expect(testParameterRecognition(`#{${longParam}}`)).toBe(1);

            // Many parameters
            const manyParams = Array.from({ length: 50 }, (_, i) => `#{param${i}}`).join(' ');
            expect(testParameterRecognition(manyParams)).toBe(50);
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
        hash: capturedDecorations.filter((d) => d.attrs?.class === 'cm-parameter-hash').length,
        bracket: capturedDecorations.filter((d) => d.attrs?.class === 'cm-bracket').length,
        replace: capturedDecorations.filter((d) => d.type === 'replace').length
    };
}

function testParameterRecognition(input: string): number {
    capturedDecorations.length = 0;
    const mockView = createMockView([input]);
    new (parameterHighlightPlugin as any).pluginClass(mockView);
    return capturedDecorations.filter((d) => d.type === 'replace').length;
}
