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

import { Decoration, DecorationSet, EditorView, ViewPlugin, ViewUpdate } from '@codemirror/view';
import { Range, Prec } from '@codemirror/state';
import { CodemirrorNifiLanguagePackage } from '../../../services/codemirror-nifi-language-package.service';

/**
 * Configuration for EL function highlighting
 */
export interface ELFunctionHighlightConfig {
    validationService?: CodemirrorNifiLanguagePackage;
}

/**
 * Creates EL function mark decorations with validation support
 */
function createELFunctionMarkDecorations(view: EditorView, config?: ELFunctionHighlightConfig): DecorationSet {
    const decorations: Range<Decoration>[] = [];
    const doc = view.state.doc;

    for (let i = 1; i <= doc.lines; i++) {
        const line = doc.line(i);
        const text = line.text;

        // Find all EL expressions in the line
        const elExpressions = findELExpressions(text);

        for (const expr of elExpressions) {
            const fullStart = line.from + expr.start;
            const fullEnd = line.from + expr.end;

            // Add decorations for the EL expression structure
            // 1. Dollar sign '$'
            decorations.push(
                Decoration.mark({
                    class: 'cm-el-function-dollar-sign'
                }).range(fullStart, fullStart + 1)
            );

            // 2. Opening brace '{'
            decorations.push(
                Decoration.mark({
                    class: 'cm-bracket'
                }).range(fullStart + 1, fullStart + 2)
            );

            // 3. Parse and style function names within the expression
            const contentStart = fullStart + 2; // After '${'
            const contentEnd = fullEnd - 1; // Before '}'
            const content = text.substring(expr.start + 2, expr.end - 1);

            const functionNames = extractFunctionNames(content);
            for (const funcName of functionNames) {
                const funcStart = contentStart + funcName.start;
                const funcEnd = contentStart + funcName.end;

                // Validate function if validation service is provided
                let isValid = true;
                if (config?.validationService) {
                    isValid = config.validationService.isValidElFunction(funcName.name);
                }

                // Apply styling to function name only
                decorations.push(
                    Decoration.mark({
                        class: isValid ? 'cm-el-function-name' : 'cm-el-function-name cm-el-function-error'
                    }).range(funcStart, funcEnd)
                );
            }

            // 4. Closing brace '}'
            decorations.push(
                Decoration.mark({
                    class: 'cm-bracket'
                }).range(fullEnd - 1, fullEnd)
            );
        }
    }

    // Sort decorations by position to ensure they're in the correct order
    decorations.sort((a, b) => a.from - b.from);

    return Decoration.set(decorations);
}

/**
 * Finds all EL expressions in a text, handling nested braces properly
 */
function findELExpressions(text: string): Array<{ start: number; end: number }> {
    const expressions: Array<{ start: number; end: number }> = [];
    let i = 0;

    while (i < text.length - 1) {
        if (text[i] === '$' && text[i + 1] === '{') {
            const start = i;
            let braceCount = 1;
            let j = i + 2;

            // Find matching closing brace, handling nesting
            while (j < text.length && braceCount > 0) {
                if (text[j] === '{') {
                    braceCount++;
                } else if (text[j] === '}') {
                    braceCount--;
                }
                j++;
            }

            if (braceCount === 0) {
                expressions.push({ start, end: j });
                i = j;
            } else {
                i++;
            }
        } else {
            i++;
        }
    }

    return expressions;
}

/**
 * Extracts function names and their positions from EL expression content
 */
function extractFunctionNames(content: string): Array<{ name: string; start: number; end: number }> {
    const functionNames: Array<{ name: string; start: number; end: number }> = [];

    // Handle nested expressions recursively
    let i = 0;
    while (i < content.length) {
        // Skip nested ${...} expressions
        if (content[i] === '$' && i + 1 < content.length && content[i + 1] === '{') {
            let braceCount = 1;
            let j = i + 2;

            while (j < content.length && braceCount > 0) {
                if (content[j] === '{') {
                    braceCount++;
                } else if (content[j] === '}') {
                    braceCount--;
                }
                j++;
            }

            // Recursively process nested content
            if (braceCount === 0) {
                const nestedContent = content.substring(i + 2, j - 1);
                const nestedFunctions = extractFunctionNames(nestedContent);
                for (const nested of nestedFunctions) {
                    functionNames.push({
                        name: nested.name,
                        start: i + 2 + nested.start,
                        end: i + 2 + nested.end
                    });
                }
            }

            i = j;
            continue;
        }

        // Look for function name patterns
        if (/[a-zA-Z_]/.test(content[i])) {
            const start = i;

            // Read the identifier
            while (i < content.length && /[a-zA-Z0-9_]/.test(content[i])) {
                i++;
            }

            const identifier = content.substring(start, i);

            // Check if this looks like a function name
            // It could be:
            // 1. A standalone function: functionName
            // 2. After a colon: subject:functionName
            // 3. Before parentheses: functionName()

            let isFunction = false;

            // Check if followed by parentheses (function call)
            if (i < content.length && content[i] === '(') {
                isFunction = true;
            }

            // Check if preceded by colon (subject:function pattern)
            if (start > 0 && content[start - 1] === ':') {
                isFunction = true;
            }

            // If no clear indicators, assume it's a function if it's not clearly something else
            if (!isFunction) {
                // Simple heuristic: if it's not followed by operators or whitespace suggesting it's a value,
                // treat it as a function name
                const nextChar = i < content.length ? content[i] : '';
                if (
                    nextChar === '' ||
                    nextChar === ',' ||
                    nextChar === ')' ||
                    nextChar === '}' ||
                    /\s/.test(nextChar)
                ) {
                    isFunction = true;
                }
            }

            if (isFunction) {
                functionNames.push({
                    name: identifier,
                    start: start,
                    end: i
                });
            }
        } else {
            i++;
        }
    }

    return functionNames;
}

/**
 * Creates EL function highlight plugin with optional configuration
 */
export function elFunctionHighlightPlugin(config?: ELFunctionHighlightConfig) {
    return Prec.highest(
        ViewPlugin.fromClass(
            class {
                decorations: DecorationSet;

                constructor(view: EditorView) {
                    this.decorations = createELFunctionMarkDecorations(view, config);
                }

                update(update: ViewUpdate) {
                    if (update.docChanged || update.viewportChanged) {
                        this.decorations = createELFunctionMarkDecorations(update.view, config);
                    }
                }
            },
            {
                decorations: (v) => v.decorations
            }
        )
    );
}
