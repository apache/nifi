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
 * Configuration for parameter highlighting
 */
export interface ParameterHighlightConfig {
    validationService?: CodemirrorNifiLanguagePackage;
}

/**
 * Creates parameter mark decorations with validation support
 */
function createParameterMarkDecorations(view: EditorView, config?: ParameterHighlightConfig): DecorationSet {
    const decorations: Range<Decoration>[] = [];
    const doc = view.state.doc;

    // Regular expression to match #{paramName} patterns
    const parameterRegex = /#\{([^}]+)\}/g;

    for (let i = 1; i <= doc.lines; i++) {
        const line = doc.line(i);
        const text = line.text;
        let match;

        // Reset regex for each line
        parameterRegex.lastIndex = 0;

        while ((match = parameterRegex.exec(text)) !== null) {
            const fullStart = line.from + match.index;
            const fullEnd = line.from + match.index + match[0].length;
            const hashPos = fullStart; // Position of '#'
            const openBracePos = fullStart + 1; // Position of '{'
            const paramStart = line.from + match.index + 2; // Skip '#{'
            const paramEnd = fullEnd - 1; // Skip '}'
            const closeBracePos = fullEnd - 1; // Position of '}'
            const parameterContent = match[1];

            // Parse parameter content to separate name from parentheses
            const baseParamName = parameterContent.replace(/\(\)$/, '');
            const hasParens = parameterContent.endsWith('()');

            // Validate parameter using the base name (without parentheses)
            let isValid = true;
            if (config?.validationService) {
                isValid = config.validationService.isValidParameter(baseParamName);
            }

            // Add decorations in the correct order (sorted by position)
            // 1. Hash character '#' - always normal styling
            decorations.push(
                Decoration.mark({
                    class: 'cm-parameter-hash'
                }).range(hashPos, hashPos + 1)
            );

            // 2. Opening brace '{' - always normal styling
            decorations.push(
                Decoration.mark({
                    class: 'cm-bracket'
                }).range(openBracePos, openBracePos + 1)
            );

            // 3. Parameter name (without parentheses) - apply error styling only if invalid
            const paramNameEnd = hasParens ? paramStart + baseParamName.length : paramEnd;
            if (!isValid) {
                decorations.push(
                    Decoration.mark({
                        class: 'cm-parameter-name cm-parameter-error'
                    }).range(paramStart, paramNameEnd)
                );
            } else {
                decorations.push(
                    Decoration.mark({
                        class: 'cm-parameter-name'
                    }).range(paramStart, paramNameEnd)
                );
            }

            // 4. Parentheses (if present) - separate styling
            if (hasParens) {
                decorations.push(
                    Decoration.mark({
                        class: 'cm-parameter-parens'
                    }).range(paramNameEnd, paramEnd)
                );
            }

            // 5. Closing brace '}' - always normal styling
            decorations.push(
                Decoration.mark({
                    class: 'cm-bracket'
                }).range(closeBracePos, closeBracePos + 1)
            );
        }
    }

    // Sort decorations by position to ensure they're in the correct order
    decorations.sort((a, b) => a.from - b.from);

    return Decoration.set(decorations);
}

/**
 * Creates parameter highlight plugin with optional configuration
 */
export function parameterHighlightPlugin(config?: ParameterHighlightConfig) {
    return Prec.highest(
        ViewPlugin.fromClass(
            class {
                decorations: DecorationSet;

                constructor(view: EditorView) {
                    this.decorations = createParameterMarkDecorations(view, config);
                }

                update(update: ViewUpdate) {
                    if (update.docChanged || update.viewportChanged) {
                        this.decorations = createParameterMarkDecorations(update.view, config);
                    }
                }
            },
            {
                decorations: (v) => v.decorations
            }
        )
    );
}
