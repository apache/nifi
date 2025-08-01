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
import { parameterHighlightPlugin } from './parameter-highlight';

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

describe('Parameter Highlight Plugin', () => {
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
            isValidParameter: jest.fn()
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
            expect(parameterHighlightPlugin).toBeDefined();
            expect(typeof parameterHighlightPlugin).toBe('function');
        });

        it('should return a valid extension when called without config', () => {
            const extension = parameterHighlightPlugin();
            expect(extension).toBeDefined();
        });

        it('should return a valid extension when called with validation service', () => {
            const extension = parameterHighlightPlugin({ validationService: mockValidationService });
            expect(extension).toBeDefined();
        });

        it('should accept undefined config gracefully', () => {
            const extension = parameterHighlightPlugin(undefined);
            expect(extension).toBeDefined();
        });
    });

    describe('Configuration Interface', () => {
        it('should work with validation service that returns true', () => {
            mockValidationService.isValidParameter.mockReturnValue(true);
            expect(() => parameterHighlightPlugin({ validationService: mockValidationService })).not.toThrow();
        });

        it('should work with validation service that returns false', () => {
            mockValidationService.isValidParameter.mockReturnValue(false);
            expect(() => parameterHighlightPlugin({ validationService: mockValidationService })).not.toThrow();
        });
    });

    describe('ViewPlugin Integration', () => {
        it('should create ViewPlugin with highest precedence', () => {
            const config = { validationService: mockValidationService };
            const plugin = parameterHighlightPlugin(config);

            expect(mockedPrec.highest).toHaveBeenCalled();
            expect(mockedViewPlugin.fromClass).toHaveBeenCalled();
        });

        it('should pass decorations configuration to ViewPlugin', () => {
            parameterHighlightPlugin();

            const [, viewConfig] = mockedViewPlugin.fromClass.mock.calls[0];
            expect(viewConfig).toBeDefined();
            expect(viewConfig).toHaveProperty('decorations');
            expect(typeof viewConfig?.decorations).toBe('function');
        });
    });

    describe('Parameter Detection and Decoration Creation', () => {
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

        describe('Single Parameter Detection', () => {
            it('should detect simple parameter pattern', () => {
                const text = 'Hello #{world} test';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;

                const plugin = parameterHighlightPlugin();

                // Get the plugin class constructor
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                const instance = new PluginClass(mockEditorView) as any;

                // Verify decorations were created
                expect(instance.decorations).toBeDefined();
                expect(mockedDecoration.set).toHaveBeenCalled();
            });

            it('should create decorations for hash, braces, and parameter name', () => {
                const text = 'Value: #{paramName}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidParameter.mockReturnValue(true);

                const plugin = parameterHighlightPlugin({ validationService: mockValidationService });

                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                // Should create 4 decorations: #, {, paramName, }
                const markCalls = mockedDecoration.mark.mock.calls;
                expect(markCalls.length).toBeGreaterThan(0);

                // Check for different CSS classes
                const classes = markCalls.map((call) => call[0].class);
                expect(classes).toContain('cm-parameter-hash');
                expect(classes).toContain('cm-bracket');
                expect(classes).toContain('cm-parameter-name');
            });

            it('should apply error styling to invalid parameters', () => {
                const text = 'Invalid: #{badParam}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidParameter.mockReturnValue(false);

                const plugin = parameterHighlightPlugin({ validationService: mockValidationService });

                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                const markCalls = mockedDecoration.mark.mock.calls;
                const errorClasses = markCalls
                    .map((call) => call[0].class)
                    .filter((cls) => cls && cls.includes('cm-parameter-error'));

                expect(errorClasses.length).toBeGreaterThan(0);
                expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('badParam');
            });

            it('should apply normal styling to valid parameters', () => {
                const text = 'Valid: #{goodParam}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidParameter.mockReturnValue(true);

                const plugin = parameterHighlightPlugin({ validationService: mockValidationService });

                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                const markCalls = mockedDecoration.mark.mock.calls;
                const normalClasses = markCalls
                    .map((call) => call[0].class)
                    .filter((cls) => cls === 'cm-parameter-name');

                expect(normalClasses.length).toBeGreaterThan(0);
                expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('goodParam');
            });
        });

        describe('Multiple Parameters', () => {
            it('should detect multiple parameters on same line', () => {
                const text = 'Start #{param1} middle #{param2} end';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidParameter.mockReturnValue(true);

                const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('param1');
                expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('param2');
                expect(mockValidationService.isValidParameter).toHaveBeenCalledTimes(2);
            });

            it('should handle mixed valid and invalid parameters', () => {
                const text = 'Mixed #{valid} and #{invalid} params';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidParameter.mockImplementation((param: string) => param === 'valid');

                const plugin = parameterHighlightPlugin({ validationService: mockValidationService });

                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                const markCalls = mockedDecoration.mark.mock.calls;
                const classes = markCalls.map((call) => call[0].class);

                expect(classes).toContain('cm-parameter-name'); // valid param
                expect(classes.some((cls) => cls && cls.includes('cm-parameter-error'))).toBe(true); // invalid param
            });
        });

        describe('Multi-line Parameter Detection', () => {
            it('should detect parameters across multiple lines', () => {
                const line1 = 'First line #{param1}';
                const line2 = 'Second line #{param2}';

                mockDocument.lines = 2;
                mockDocument.line.mockImplementation((lineNum: number) => {
                    if (lineNum === 1) return mockLine(line1, 1);
                    if (lineNum === 2) return mockLine(line2, 2);
                    return mockLine('', lineNum);
                });

                mockValidationService.isValidParameter.mockReturnValue(true);

                const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('param1');
                expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('param2');
            });

            it('should handle empty lines correctly', () => {
                mockDocument.lines = 3;
                mockDocument.line.mockImplementation((lineNum: number) => {
                    if (lineNum === 1) return mockLine('First #{param1}', 1);
                    if (lineNum === 2) return mockLine('', 2); // Empty line
                    if (lineNum === 3) return mockLine('Third #{param3}', 3);
                    return mockLine('', lineNum);
                });

                mockValidationService.isValidParameter.mockReturnValue(true);

                const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('param1');
                expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('param3');
                expect(mockValidationService.isValidParameter).toHaveBeenCalledTimes(2);
            });
        });

        describe('Parameter Name Edge Cases', () => {
            it('should handle parameters with spaces', () => {
                const text = 'Spaced: #{param with spaces}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidParameter.mockReturnValue(true);

                const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('param with spaces');
            });

            it('should handle parameters with special characters', () => {
                const text = 'Special: #{param-with.special_chars}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidParameter.mockReturnValue(true);

                const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('param-with.special_chars');
            });

            it('should handle parameters with numbers', () => {
                const text = 'Numbers: #{param123} and #{123param}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidParameter.mockReturnValue(true);

                const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('param123');
                expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('123param');
            });

            it('should handle empty parameter names', () => {
                const text = 'Empty: #{} parameter';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;
                mockValidationService.isValidParameter.mockReturnValue(false);

                const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                // Empty parameter names #{} don't match the regex /#\{([^}]+)\}/g
                // which requires at least one character, so validation won't be called
                expect(mockValidationService.isValidParameter).not.toHaveBeenCalled();
            });
        });

        describe('Without Validation Service', () => {
            it('should create decorations without validation when no service provided', () => {
                const text = 'No validation: #{someParam}';
                mockDocument.line.mockReturnValue(mockLine(text));
                mockDocument.lines = 1;

                const plugin = parameterHighlightPlugin(); // No validation service

                const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
                new PluginClass(mockEditorView);

                // Should still create decorations
                expect(mockedDecoration.set).toHaveBeenCalled();
                expect(mockedDecoration.mark).toHaveBeenCalled();

                // Should use normal styling (not error) by default
                const markCalls = mockedDecoration.mark.mock.calls;
                const classes = markCalls.map((call) => call[0].class);
                expect(classes).toContain('cm-parameter-name');
                expect(classes.every((cls) => !cls || !cls.includes('cm-parameter-error'))).toBe(true);
            });
        });
    });

    describe('ViewPlugin Lifecycle', () => {
        beforeEach(() => {
            mockDocument.line.mockReturnValue({
                from: 0,
                to: 20,
                text: 'Test #{param} text',
                number: 1
            });
            mockDocument.lines = 1;
        });

        it('should initialize decorations in constructor', () => {
            const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];

            const instance = new PluginClass(mockEditorView) as any;
            expect(instance.decorations).toBeDefined();
        });

        it('should update decorations when document changes', () => {
            const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
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
            const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
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
            const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
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
        it('should handle adjacent parameters correctly', () => {
            const text = '#{param1}#{param2}';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidParameter.mockReturnValue(true);

            const plugin = parameterHighlightPlugin({ validationService: mockValidationService });

            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            // Should handle both parameters
            expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('param1');
            expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('param2');
            expect(mockedDecoration.set).toHaveBeenCalled();
        });

        it('should handle parameters at line boundaries', () => {
            const text = 'Start#{param}End';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidParameter.mockReturnValue(true);

            const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('param');
        });
    });

    describe('Real-world Parameter Scenarios', () => {
        it('should handle configuration file with multiple parameters', () => {
            const text = 'database.url=#{db.host}:#{db.port}/#{db.name}';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidParameter.mockImplementation((param: string) =>
                ['db.host', 'db.port', 'db.name'].includes(param)
            );

            const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('db.host');
            expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('db.port');
            expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('db.name');
        });

        it('should handle JSON-like parameter names', () => {
            const text = 'Config: #{app.config.timeout} and #{app.config.retries}';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidParameter.mockReturnValue(true);

            const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('app.config.timeout');
            expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('app.config.retries');
        });

        it('should handle parameters with regex-sensitive characters', () => {
            const text = 'Regex: #{param[0]} and #{param^test}';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidParameter.mockReturnValue(true);

            const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('param[0]');
            expect(mockValidationService.isValidParameter).toHaveBeenCalledWith('param^test');
        });
    });

    describe('Edge Cases and Error Conditions', () => {
        it('should handle malformed parameter syntax gracefully', () => {
            const text = 'Malformed: #{unclosed and #{nested#{inside}} and #{}';
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidParameter.mockReturnValue(false);

            const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];

            // Should not throw error
            expect(() => new PluginClass(mockEditorView)).not.toThrow();
        });

        it('should handle very long parameter names', () => {
            const longParamName = 'very'.repeat(100) + 'LongParameterName';
            const text = `Long: #{${longParamName}}`;
            mockDocument.line.mockReturnValue({
                from: 0,
                to: text.length,
                text,
                number: 1
            });
            mockDocument.lines = 1;
            mockValidationService.isValidParameter.mockReturnValue(true);

            const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];
            new PluginClass(mockEditorView);

            expect(mockValidationService.isValidParameter).toHaveBeenCalledWith(longParamName);
        });

        it('should handle documents with many lines efficiently', () => {
            mockDocument.lines = 100;
            mockDocument.line.mockImplementation((lineNum: number) => ({
                from: (lineNum - 1) * 50,
                to: lineNum * 50,
                text: lineNum % 10 === 0 ? `Line ${lineNum} #{param${lineNum}}` : `Line ${lineNum}`,
                number: lineNum
            }));

            mockValidationService.isValidParameter.mockReturnValue(true);

            const plugin = parameterHighlightPlugin({ validationService: mockValidationService });
            const [PluginClass] = mockedViewPlugin.fromClass.mock.calls[0];

            // Should handle many lines without issues
            expect(() => new PluginClass(mockEditorView)).not.toThrow();

            // Should have called validation for parameters on lines 10, 20, 30, etc.
            expect(mockValidationService.isValidParameter).toHaveBeenCalledTimes(10);
        });
    });
});
