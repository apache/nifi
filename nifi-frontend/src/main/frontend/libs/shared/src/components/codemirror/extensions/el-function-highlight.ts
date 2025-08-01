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

    // Regular expression to match ${functionName} patterns
    const elFunctionRegex = /\$\{([^}]+)\}/g;

    for (let i = 1; i <= doc.lines; i++) {
        const line = doc.line(i);
        const text = line.text;
        let match;

        // Reset regex for each line
        elFunctionRegex.lastIndex = 0;

        while ((match = elFunctionRegex.exec(text)) !== null) {
            const fullStart = line.from + match.index;
            const fullEnd = line.from + match.index + match[0].length;
            const dollarPos = fullStart; // Position of '$'
            const openBracePos = fullStart + 1; // Position of '{'
            const functionStart = line.from + match.index + 2; // Skip '${'
            const functionEnd = fullEnd - 1; // Skip '}'
            const closeBracePos = fullEnd - 1; // Position of '}'
            const functionName = match[1];

            // Validate function if validation service is provided
            let isValid = true;
            if (config?.validationService) {
                isValid = config.validationService.isValidElFunction(functionName);
            }

            // Add decorations in the correct order (sorted by position)
            // 1. Dollar sign '$' - always normal styling
            decorations.push(
                Decoration.mark({
                    class: 'cm-el-function-dollar-sign'
                }).range(dollarPos, dollarPos + 1)
            );

            // 2. Opening brace '{' - always normal styling
            decorations.push(
                Decoration.mark({
                    class: 'cm-bracket'
                }).range(openBracePos, openBracePos + 1)
            );

            // 3. Function name - apply error styling only if invalid
            if (!isValid) {
                decorations.push(
                    Decoration.mark({
                        class: 'cm-el-function-name cm-el-function-error'
                    }).range(functionStart, functionEnd)
                );
            } else {
                decorations.push(
                    Decoration.mark({
                        class: 'cm-el-function-name'
                    }).range(functionStart, functionEnd)
                );
            }

            // 4. Closing brace '}' - always normal styling
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
