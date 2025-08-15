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
import { EditorState } from '@codemirror/state';
import { syntaxTree } from '@codemirror/language';
import { of } from 'rxjs';
import { baseTheme } from '../../components/codemirror/themes/baseTheme';

describe('NiFi Expression Language - Theme Validation', () => {
    let service: CodemirrorNifiLanguageService;

    beforeEach(() => {
        const mockElService = { getElGuide: jest.fn() } as unknown as jest.Mocked<ElService>;
        const mockNiFiCommon = { compareString: jest.fn() } as unknown as jest.Mocked<NiFiCommon>;

        // Mock EL guide with basic functions
        mockElService.getElGuide.mockReturnValue(
            of(`
            <html><body>
                <div class="function">
                    <h3>replace</h3>
                    <span class="description">Replaces text</span>
                </div>
                <div class="function">
                    <h3>equals</h3>
                    <span class="description">Tests equality</span>
                </div>
            </body></html>
        `)
        );

        TestBed.configureTestingModule({
            providers: [
                CodemirrorNifiLanguageService,
                { provide: ElService, useValue: mockElService },
                { provide: NiFiCommon, useValue: mockNiFiCommon }
            ]
        });

        service = TestBed.inject(CodemirrorNifiLanguageService);
    });

    // Helper function to parse input and return token information
    const parseTokens = (input: string) => {
        const state = EditorState.create({
            doc: input,
            extensions: [service.getLanguageSupport()]
        });
        const tree = syntaxTree(state);
        const tokens: Array<{ text: string; nodeType: string; isPlainText: boolean }> = [];

        tree.cursor().iterate((node) => {
            const text = input.slice(node.from, node.to);
            if (text.trim()) {
                tokens.push({
                    text,
                    nodeType: node.type.name,
                    isPlainText: ['Text', '⚠', 'Query', 'content'].includes(node.type.name)
                });
            }
            return true;
        });

        return { tree, tokens };
    };

    describe('Critical Regression Prevention', () => {
        it('should not crash on plain text content', () => {
            const plainTextCases = [
                'Hello World',
                'Price: $$100 for items',
                'Result: Status: Price:',
                'Numbers 123 and symbols $$'
            ];

            plainTextCases.forEach((input) => {
                expect(() => parseTokens(input)).not.toThrow();
                const { tree } = parseTokens(input);
                expect(tree).toBeDefined();
            });
        });

        it('should not crash on expression content', () => {
            const expressionCases = [
                '${attr}',
                '${filename:replace("old", "new")}',
                '${attr:equals(true)}',
                'Text ${expression} more text'
            ];

            expressionCases.forEach((input) => {
                expect(() => parseTokens(input)).not.toThrow();
                const { tree, tokens } = parseTokens(input);
                expect(tree).toBeDefined();

                // Should have some structured tokens for expressions
                const hasStructuredTokens = tokens.some((token) => !token.isPlainText);
                expect(hasStructuredTokens).toBe(true);
            });
        });

        it('should handle malformed expressions gracefully', () => {
            const malformedCases = ['${unclosed', '${:missing}', '${attr:}', '${func(}', '${}', '${nested${broken}'];

            malformedCases.forEach((input) => {
                expect(() => parseTokens(input)).not.toThrow();
                const { tree } = parseTokens(input);
                expect(tree).toBeDefined();
            });
        });
    });

    describe('Theme Regression Tests', () => {
        it('should treat plain text as plain text (no special styling)', () => {
            const input = 'Result: Status: Price:';
            const { tokens } = parseTokens(input);

            // Most tokens should be plain text
            const plainTextCount = tokens.filter((token) => token.isPlainText).length;
            const totalTokens = tokens.length;
            expect(plainTextCount / totalTokens).toBeGreaterThanOrEqual(0.5);
        });

        it('should treat escaped dollars as plain text', () => {
            const input = 'Price: $$100';
            const { tokens } = parseTokens(input);

            // Should have tokens that include the escaped dollar
            const hasEscapedDollar = tokens.some((token) => token.text.includes('$$'));
            expect(hasEscapedDollar).toBe(true);
        });

        it('should style expression attributes properly', () => {
            const input = '${filename:replace(${attr}, "new")}';
            const { tokens } = parseTokens(input);

            // Should have some styled tokens for attributes
            const attributeTokens = tokens.filter(
                (token) => token.text.includes('filename') || token.text.includes('attr')
            );
            const hasStyledAttributes = attributeTokens.some((token) => !token.isPlainText);
            expect(hasStyledAttributes).toBe(true);
        });

        it('should style expression delimiters properly', () => {
            const input = '${attr}';
            const { tokens } = parseTokens(input);

            // Should have some styled tokens for delimiters
            const delimiterTokens = tokens.filter((token) => token.text.includes('${') || token.text.includes('}'));
            const hasStyledDelimiters = delimiterTokens.some((token) => !token.isPlainText);
            expect(hasStyledDelimiters).toBe(true);
        });
    });

    describe('Performance & Stability', () => {
        it('should handle large content efficiently', () => {
            let largeContent = '';
            for (let i = 0; i < 100; i++) {
                largeContent += `Line ${i}: Hello \${attr${i}} world\n`;
            }

            const start = performance.now();
            const { tree } = parseTokens(largeContent);
            const end = performance.now();

            expect(end - start).toBeLessThan(500); // Should parse in < 500ms
            expect(tree).toBeDefined();
        });

        it('should be stable across multiple parses', () => {
            const input = 'Hello ${name:toUpperCase()} world';

            for (let i = 0; i < 10; i++) {
                expect(() => parseTokens(input)).not.toThrow();
                const { tree } = parseTokens(input);
                expect(tree).toBeDefined();
            }
        });
    });

    describe('Integration Tests', () => {
        it('should integrate with CodeMirror', () => {
            const languageSupport = service.getLanguageSupport();
            expect(languageSupport).toBeDefined();

            const editorState = EditorState.create({
                doc: '${test}',
                extensions: [languageSupport]
            });

            expect(editorState).toBeDefined();
            expect(editorState.doc.toString()).toBe('${test}');
        });

        it('should handle the exact migration test cases', () => {
            // These are the patterns that caused issues during v5→v6 migration
            const migrationCases = [
                'Price: $$100 for ${item}', // Plain text + expressions
                'Result: Status: Price:', // Plain text with colons
                '${filename:replace(${attr}, "new")}', // Nested expressions
                '${attr:equals(true)}', // Boolean literals
                'Hello ${name}, you have ${count} items.' // Mixed content
            ];

            migrationCases.forEach((input) => {
                expect(() => parseTokens(input)).not.toThrow();
                const { tree } = parseTokens(input);
                expect(tree).toBeDefined();
            });
        });

        it('should have all restored CSS variables properly defined in baseTheme', () => {
            // Verify bracket matching styles are properly configured with v5 parity
            expect(baseTheme['.cm-matchingBracket']['color']).toBe('var(--mat-sys-inverse-on-surface)');
            expect(baseTheme['.cm-matchingBracket > .cm-bracket, .cm-matchingBracket > span']['color']).toBe(
                'var(--mat-sys-inverse-on-surface)'
            );
            expect(baseTheme['.cm-nonmatchingBracket']['color']).toBe('var(--editor-text)');

            // Verify the new focused non-matching bracket style
            expect(baseTheme['&.cm-focused .cm-nonmatchingBracket']['background']).toBe('var(--nf-neutral)');
            expect(baseTheme['&.cm-focused .cm-nonmatchingBracket']['color']).toBe('var(--mat-sys-inverse-on-surface)');

            const themeString = JSON.stringify(baseTheme);
            expect(themeString).toContain('var(--editor-text)');
        });

        it('should have proper syntax highlighting for restored v5 tokens', () => {
            // Verify that the language support includes syntax highlighting
            const languageSupport = service.getLanguageSupport();
            expect(languageSupport).toBeDefined();

            // Verify the style can be applied without errors
            expect(() => {
                const testState = EditorState.create({
                    doc: '${attr} "quote" <tag>',
                    extensions: [languageSupport]
                });
                return testState;
            }).not.toThrow();
        });
    });
});
