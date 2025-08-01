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

import { EditorView, ViewPlugin, Decoration } from '@codemirror/view';
import { Prec } from '@codemirror/state';
import { elFunctionHighlightPlugin } from './el-function-highlight';

// Mock CodeMirror dependencies at the top level
jest.mock('@codemirror/state', () => ({
    Prec: {
        highest: jest.fn((plugin) => ({ highestPrecedence: true, plugin }))
    },
    Range: jest.fn()
}));

jest.mock('@codemirror/view', () => ({
    ViewPlugin: {
        fromClass: jest.fn((pluginClass, config) => ({ pluginClass, config }))
    },
    Decoration: {
        mark: jest.fn((spec) => ({
            type: 'mark',
            spec,
            range: (from: number, to: number) => ({
                from,
                to,
                value: { type: 'mark', spec },
                eq: jest.fn(),
                map: jest.fn()
            })
        })),
        set: jest.fn((decorations) => ({
            decorations,
            size: decorations.length,
            map: jest.fn(),
            eq: jest.fn(),
            update: jest.fn(),
            iter: jest.fn()
        }))
    }
}));

describe('EL Function Highlight Plugin', () => {
    let mockValidationService: any;
    let mockEditorView: any;
    let mockEditorState: any;
    let mockDocument: any;
    let mockedViewPlugin: jest.Mocked<typeof ViewPlugin>;
    let mockedDecoration: jest.Mocked<typeof Decoration>;
    let mockedPrec: jest.Mocked<typeof Prec>;

    beforeEach(() => {
        jest.clearAllMocks();

        // Get the mocked modules
        mockedViewPlugin = jest.mocked(ViewPlugin);
        mockedDecoration = jest.mocked(Decoration);
        mockedPrec = jest.mocked(Prec);

        mockValidationService = {
            isValidElFunction: jest.fn()
        };

        // Create comprehensive mocks for CodeMirror
        mockDocument = {
            lines: 1,
            line: jest.fn(),
            toString: jest.fn()
        };

        mockEditorState = {
            doc: mockDocument
        };

        mockEditorView = {
            state: mockEditorState
        };
    });

    describe('Basic Functionality', () => {
        it('should export a properly configured plugin function', () => {
            expect(elFunctionHighlightPlugin).toBeDefined();
            expect(typeof elFunctionHighlightPlugin).toBe('function');
        });

        it('should return a valid extension when called without config', () => {
            const extension = elFunctionHighlightPlugin();
            expect(extension).toBeDefined();
        });

        it('should return a valid extension when called with validation service', () => {
            const extension = elFunctionHighlightPlugin({ validationService: mockValidationService });
            expect(extension).toBeDefined();
        });

        it('should accept undefined config gracefully', () => {
            const extension = elFunctionHighlightPlugin(undefined);
            expect(extension).toBeDefined();
        });
    });

    describe('Configuration Interface', () => {
        it('should work with validation service that returns true', () => {
            mockValidationService.isValidElFunction.mockReturnValue(true);
            expect(() => elFunctionHighlightPlugin({ validationService: mockValidationService })).not.toThrow();
        });

        it('should work with validation service that returns false', () => {
            mockValidationService.isValidElFunction.mockReturnValue(false);
            expect(() => elFunctionHighlightPlugin({ validationService: mockValidationService })).not.toThrow();
        });
    });

    describe('ViewPlugin Integration', () => {
        it('should create ViewPlugin with highest precedence', () => {
            const config = { validationService: mockValidationService };
            const plugin = elFunctionHighlightPlugin(config);

            expect(mockedPrec.highest).toHaveBeenCalled();
            expect(mockedViewPlugin.fromClass).toHaveBeenCalled();
        });

        it('should pass decorations configuration to ViewPlugin', () => {
            elFunctionHighlightPlugin();

            const [, viewConfig] = mockedViewPlugin.fromClass.mock.calls[0];
            expect(viewConfig).toBeDefined();
            expect(viewConfig).toHaveProperty('decorations');
            expect(typeof viewConfig?.decorations).toBe('function');
        });
    });

    describe('EL Function Detection and Decoration Creation', () => {
        const mockLine = (text: string, lineNumber: number = 1) => ({
            from: (lineNumber - 1) * 50, // Mock line positions
            to: (lineNumber - 1) * 50 + text.length,
            text,
            number: lineNumber
        });

        beforeEach(() => {
            mockDocument.line.mockImplementation((lineNum: number) => {
                // Default to empty line if not specifically mocked
                return mockLine('', lineNum);
            });
        });

        describe('Single EL Function Detection', () => {
            it('should detect simple EL function pattern', () => {
                const text = 'Hello ${uuid} test';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;

                const plugin = elFunctionHighlightPlugin();

                // Get the plugin class constructor
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                const instance = new PluginClass(mockEditorView) as any;

                // Verify decorations were created
                expect(instance.decorations).toBeDefined();
                expect(mockedDecoration.set).toHaveBeenCalled();
            });

            it('should create decorations for dollar sign, braces, and function name', () => {
                const text = 'Value: ${functionName}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidElFunction.mockReturnValue(true);

                const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });

                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                // Should create 4 decorations: $, {, functionName, }
                const markCalls = mockedDecoration.mark.mock.calls;
                expect(markCalls.length).toBeGreaterThan(0);

                // Check for different CSS classes
                const classes = markCalls.map((call) => call[0].class);
                expect(classes).toContain('cm-el-function-dollar-sign');
                expect(classes).toContain('cm-bracket');
                expect(classes).toContain('cm-el-function-name');
            });

            it('should apply error styling to invalid EL functions', () => {
                const text = 'Invalid: ${badFunction}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidElFunction.mockReturnValue(false);

                const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });

                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                const markCalls = mockedDecoration.mark.mock.calls;
                const errorClasses = markCalls
                    .map((call) => call[0].class)
                    .filter((cls) => cls && cls.includes('cm-el-function-error'));

                expect(errorClasses.length).toBeGreaterThan(0);
                expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('badFunction');
            });

            it('should apply normal styling to valid EL functions', () => {
                const text = 'Valid: ${goodFunction}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidElFunction.mockReturnValue(true);

                const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });

                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                const markCalls = mockedDecoration.mark.mock.calls;
                const normalClasses = markCalls
                    .map((call) => call[0].class)
                    .filter((cls) => cls === 'cm-el-function-name');

                expect(normalClasses.length).toBeGreaterThan(0);
                expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('goodFunction');
            });
        });

        describe('Multiple EL Functions', () => {
            it('should detect multiple EL functions on same line', () => {
                const text = 'Start ${function1} middle ${function2} end';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidElFunction.mockReturnValue(true);

                const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('function1');
                expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('function2');
                expect(mockValidationService.isValidElFunction).toHaveBeenCalledTimes(2);
            });

            it('should handle mixed valid and invalid EL functions', () => {
                const text = 'Mixed ${uuid} and ${invalidFunc} functions';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidElFunction.mockImplementation((func: string) => func === 'uuid');

                const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });

                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                const markCalls = mockedDecoration.mark.mock.calls;
                const classes = markCalls.map((call) => call[0].class);

                expect(classes).toContain('cm-el-function-name'); // valid function
                expect(classes.some((cls) => cls && cls.includes('cm-el-function-error'))).toBe(true); // invalid function
            });
        });

        describe('Multi-line EL Function Detection', () => {
            it('should detect EL functions across multiple lines', () => {
                const line1 = 'First line ${function1}';
                const line2 = 'Second line ${function2}';

                mockDocument.lines = 2;
                mockDocument.line.mockImplementation((lineNum: number) => {
                    if (lineNum === 1) return mockLine(line1, 1);
                    if (lineNum === 2) return mockLine(line2, 2);
                    return mockLine('', lineNum);
                });

                mockValidationService.isValidElFunction.mockReturnValue(true);

                const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('function1');
                expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('function2');
            });

            it('should handle empty lines correctly', () => {
                mockDocument.lines = 3;
                mockDocument.line.mockImplementation((lineNum: number) => {
                    if (lineNum === 1) return mockLine('First ${function1}', 1);
                    if (lineNum === 2) return mockLine('', 2); // Empty line
                    if (lineNum === 3) return mockLine('Third ${function3}', 3);
                    return mockLine('', lineNum);
                });

                mockValidationService.isValidElFunction.mockReturnValue(true);

                const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('function1');
                expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('function3');
                expect(mockValidationService.isValidElFunction).toHaveBeenCalledTimes(2);
            });
        });

        describe('EL Function Name Edge Cases', () => {
            it('should handle EL functions with parentheses', () => {
                const text = 'Functions: ${uuid()} and ${substring(0, 5)}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidElFunction.mockReturnValue(true);

                const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('uuid()');
                expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('substring(0, 5)');
            });

            it('should handle EL functions with colons (subject functions)', () => {
                const text = 'Subject: ${attribute:contains("test")}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidElFunction.mockReturnValue(true);

                const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('attribute:contains("test")');
            });

            it('should handle EL functions with complex arguments', () => {
                const text = 'Complex: ${substring(${attribute:startsWith("prefix")}, 0, 10)}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidElFunction.mockReturnValue(true);

                const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                // The regex stops at the first '}' character, so it captures the first inner expression
                expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith(
                    'substring(${attribute:startsWith("prefix")'
                );
            });

            it('should handle EL functions with special characters', () => {
                const text = 'Special: ${function-with_dash} and ${function.with.dots}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidElFunction.mockReturnValue(true);

                const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('function-with_dash');
                expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('function.with.dots');
            });

            it('should handle empty EL function names', () => {
                const text = 'Empty: ${} function';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidElFunction.mockReturnValue(false);

                const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                // Empty EL function names ${} don't match the regex /\$\{([^}]+)\}/g
                // which requires at least one character, so validation won't be called
                expect(mockValidationService.isValidElFunction).not.toHaveBeenCalled();
            });
        });

        describe('Without Validation Service', () => {
            it('should create decorations without validation when no service provided', () => {
                const text = 'No validation: ${someFunction}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;

                const plugin = elFunctionHighlightPlugin(); // No validation service

                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                // Should still create decorations
                expect(mockedDecoration.set).toHaveBeenCalled();
                expect(mockedDecoration.mark).toHaveBeenCalled();

                // Should use normal styling (not error) by default
                const markCalls = mockedDecoration.mark.mock.calls;
                const classes = markCalls.map((call) => call[0].class);
                expect(classes).toContain('cm-el-function-name');
                expect(classes.every((cls) => !cls || !cls.includes('cm-el-function-error'))).toBe(true);
            });
        });
    });

    describe('ViewPlugin Lifecycle', () => {
        beforeEach(() => {
            mockDocument.line.mockReturnValue({
                from: 0,
                to: 20,
                text: 'Test ${function} text',
                number: 1
            });
            mockDocument.lines = 1;
        });

        it('should initialize decorations in constructor', () => {
            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];

            const instance = new PluginClass(mockEditorView) as any;
            expect(instance.decorations).toBeDefined();
        });

        it('should update decorations when document changes', () => {
            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];

            const instance = new PluginClass(mockEditorView) as any;
            const initialDecorations = instance.decorations;

            // Mock ViewUpdate with required properties
            const mockUpdate = {
                docChanged: true,
                viewportChanged: false,
                view: mockEditorView,
                state: mockEditorState,
                transactions: [],
                changes: {},
                startState: mockEditorState,
                focusChanged: false,
                geometryChanged: false,
                heightChanged: false
            } as any;

            if (instance.update) {
                instance.update(mockUpdate);
            }

            // Should have recreated decorations
            expect(instance.decorations).toBeDefined();
        });

        it('should update decorations when viewport changes', () => {
            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];

            const instance = new PluginClass(mockEditorView) as any;

            // Mock ViewUpdate with required properties
            const mockUpdate = {
                docChanged: false,
                viewportChanged: true,
                view: mockEditorView,
                state: mockEditorState,
                transactions: [],
                changes: {},
                startState: mockEditorState,
                focusChanged: false,
                geometryChanged: false,
                heightChanged: false
            } as any;

            if (instance.update) {
                instance.update(mockUpdate);
            }

            expect(instance.decorations).toBeDefined();
        });

        it('should not update decorations when no relevant changes occur', () => {
            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];

            const instance = new PluginClass(mockEditorView) as any;

            // Clear previous calls
            mockedDecoration.set.mockClear();

            // Mock ViewUpdate with no relevant changes
            const mockUpdate = {
                docChanged: false,
                viewportChanged: false,
                view: mockEditorView,
                state: mockEditorState,
                transactions: [],
                changes: {},
                startState: mockEditorState,
                focusChanged: false,
                geometryChanged: false,
                heightChanged: false
            } as any;

            if (instance.update) {
                instance.update(mockUpdate);
            }

            // Should not have called Decoration.set again
            expect(mockedDecoration.set).not.toHaveBeenCalled();
        });
    });

    describe('Decoration Sorting and Positioning', () => {
        it('should handle adjacent EL functions correctly', () => {
            const text = '${function1}${function2}';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidElFunction.mockReturnValue(true);

            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });

            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            // Should handle both functions
            expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('function1');
            expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('function2');
            expect(mockedDecoration.set).toHaveBeenCalled();
        });

        it('should handle EL functions at line boundaries', () => {
            const text = 'Start${function}End';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidElFunction.mockReturnValue(true);

            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('function');
        });
    });

    describe('Real-world EL Function Scenarios', () => {
        it('should handle common NiFi EL functions', () => {
            const text = 'Process ${uuid()}, ${now():format("yyyy-MM-dd")}, ${allAttributes()}';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidElFunction.mockImplementation((func: string) =>
                ['uuid()', 'now():format("yyyy-MM-dd")', 'allAttributes()'].includes(func)
            );

            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('uuid()');
            expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('now():format("yyyy-MM-dd")');
            expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('allAttributes()');
        });

        it('should handle attribute functions with various operations', () => {
            const text = 'Attributes: ${attribute:startsWith("prefix")} and ${attribute:contains("text")}';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidElFunction.mockReturnValue(true);

            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('attribute:startsWith("prefix")');
            expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('attribute:contains("text")');
        });

        it('should handle nested EL function expressions', () => {
            const text = 'Nested: ${substring(${uuid()}, 0, 8)}';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidElFunction.mockReturnValue(true);

            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            // The regex stops at the first '}' character, so it captures the first inner expression
            expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('substring(${uuid()');
        });

        it('should handle EL functions with regex-sensitive characters', () => {
            const text = 'Regex: ${function[0]} and ${function^test}';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidElFunction.mockReturnValue(true);

            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('function[0]');
            expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('function^test');
        });
    });

    describe('Edge Cases and Error Conditions', () => {
        it('should handle malformed EL function syntax gracefully', () => {
            const text = 'Malformed: ${unclosed and ${nested${inside}} and ${}';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidElFunction.mockReturnValue(false);

            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];

            // Should not throw error
            expect(() => new PluginClass(mockEditorView)).not.toThrow();
        });

        it('should handle very long EL function names', () => {
            const longFunctionName = 'very'.repeat(100) + 'LongFunctionName()';
            const text = `Long: \${${longFunctionName}}`;
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidElFunction.mockReturnValue(true);

            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith(longFunctionName);
        });

        it('should handle documents with many lines efficiently', () => {
            mockDocument.lines = 100;
            mockDocument.line.mockImplementation((lineNum: number) => ({
                from: (lineNum - 1) * 50,
                to: lineNum * 50,
                text: lineNum % 10 === 0 ? `Line ${lineNum} \${function${lineNum}}` : `Line ${lineNum}`,
                number: lineNum
            }));

            mockValidationService.isValidElFunction.mockReturnValue(true);

            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];

            // Should handle many lines without issues
            expect(() => new PluginClass(mockEditorView)).not.toThrow();

            // Should have called validation for functions on lines 10, 20, 30, etc.
            expect(mockValidationService.isValidElFunction).toHaveBeenCalledTimes(10);
        });

        it('should handle mixed EL functions and parameters in same text', () => {
            const text = 'Mixed: ${uuid()} and #{parameter} together';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidElFunction.mockReturnValue(true);

            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            // Should only detect EL functions, not parameters
            expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('uuid()');
            expect(mockValidationService.isValidElFunction).toHaveBeenCalledTimes(1);
        });

        it('should handle complex real-world NiFi expressions', () => {
            const text = 'Complex: ${attribute:matches("^[0-9]{3}-[0-9]{2}-[0-9]{4}$"):ifElse("valid", "invalid")}';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidElFunction.mockReturnValue(true);

            const plugin = elFunctionHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            // The regex stops at the first '}' which truncates this complex expression
            expect(mockValidationService.isValidElFunction).toHaveBeenCalledWith('attribute:matches("^[0-9]{3');
        });
    });
});
