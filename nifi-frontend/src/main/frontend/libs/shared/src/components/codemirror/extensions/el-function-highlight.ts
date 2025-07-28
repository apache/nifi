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

import { Decoration, DecorationSet, EditorView, ViewPlugin, ViewUpdate, WidgetType } from '@codemirror/view';
import { Range } from '@codemirror/state';

/**
 * Widget for EL function names that prevents built-in syntax highlighting
 */
class ELFunctionNameWidget extends WidgetType {
    constructor(private readonly functionName: string) {
        super();
    }

    override eq(other: ELFunctionNameWidget): boolean {
        return other.functionName === this.functionName;
    }

    toDOM(): HTMLElement {
        const container = document.createElement('span');
        container.className = 'cm-el-function-name';

        // Parse the function name to identify parentheses and style them separately
        const parts = this.parseFunction(this.functionName);

        parts.forEach((part) => {
            const span = document.createElement('span');
            if (part.type === 'parenthesis') {
                span.className = 'cm-matchingBracket';
            } else {
                span.className = 'cm-el-function-name';
            }
            span.textContent = part.text;
            container.appendChild(span);
        });

        return container;
    }

    private parseFunction(functionName: string): Array<{ type: 'text' | 'parenthesis'; text: string }> {
        const parts: Array<{ type: 'text' | 'parenthesis'; text: string }> = [];
        let currentText = '';

        for (let i = 0; i < functionName.length; i++) {
            const char = functionName[i];

            if (char === '(' || char === ')') {
                // If we have accumulated text, add it as a text part
                if (currentText) {
                    parts.push({ type: 'text', text: currentText });
                    currentText = '';
                }
                // Add the parenthesis as a separate part
                parts.push({ type: 'parenthesis', text: char });
            } else {
                currentText += char;
            }
        }

        // Add any remaining text
        if (currentText) {
            parts.push({ type: 'text', text: currentText });
        }

        return parts;
    }

    override ignoreEvent(): boolean {
        return false;
    }
}

/**
 * Alternative approach using mark decorations instead of widgets
 */
function createELFunctionMarkDecorations(view: EditorView): DecorationSet {
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

            // Add decorations in the correct order (sorted by position)
            // 1. Dollar sign '$'
            decorations.push(
                Decoration.mark({
                    class: 'cm-el-function-dollar-sign'
                }).range(dollarPos, dollarPos + 1)
            );

            // 2. Opening brace '{'
            decorations.push(
                Decoration.mark({
                    class: 'cm-bracket'
                }).range(openBracePos, openBracePos + 1)
            );

            // 3. Function name (use replace decoration to override built-in highlighting)
            decorations.push(
                Decoration.replace({
                    widget: new ELFunctionNameWidget(functionName),
                    inclusive: false
                }).range(functionStart, functionEnd)
            );

            // 4. Closing brace '}'
            decorations.push(
                Decoration.mark({
                    class: 'cm-bracket' // Reuse parameter brace styling
                }).range(closeBracePos, closeBracePos + 1)
            );
        }
    }

    // Sort decorations by position to ensure they're in the correct order
    decorations.sort((a, b) => a.from - b.from);

    return Decoration.set(decorations);
}

/**
 * View plugin that manages EL function highlighting decorations
 */
export const elFunctionHighlightPlugin = ViewPlugin.fromClass(
    class {
        decorations: DecorationSet;

        constructor(view: EditorView) {
            this.decorations = createELFunctionMarkDecorations(view);
        }

        update(update: ViewUpdate) {
            if (update.docChanged || update.viewportChanged) {
                this.decorations = createELFunctionMarkDecorations(update.view);
            }
        }
    },
    {
        decorations: (v) => v.decorations
    }
);
