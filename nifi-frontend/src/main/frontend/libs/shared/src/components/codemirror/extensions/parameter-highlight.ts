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
 * Widget for parameter names that prevents built-in syntax highlighting
 */
class ParameterNameWidget extends WidgetType {
    constructor(private readonly paramName: string) {
        super();
    }

    override eq(other: ParameterNameWidget): boolean {
        return other.paramName === this.paramName;
    }

    toDOM(): HTMLElement {
        const span = document.createElement('span');
        span.className = 'cm-parameter-name';
        span.textContent = this.paramName;
        return span;
    }

    override ignoreEvent(): boolean {
        return false;
    }
}

/**
 * Alternative approach using mark decorations instead of widgets
 */
function createParameterMarkDecorations(view: EditorView): DecorationSet {
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

            // Add decorations in the correct order (sorted by position)
            // 1. Hash character '#'
            decorations.push(
                Decoration.mark({
                    class: 'cm-parameter-hash'
                }).range(hashPos, hashPos + 1)
            );

            // 2. Opening brace '{'
            decorations.push(
                Decoration.mark({
                    class: 'cm-bracket'
                }).range(openBracePos, openBracePos + 1)
            );

            // 3. Parameter name (comes after opening brace)
            decorations.push(
                Decoration.replace({
                    widget: new ParameterNameWidget(match[1]),
                    inclusive: false
                }).range(paramStart, paramEnd)
            );

            // 4. Closing brace '}'
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
 * View plugin that manages parameter highlighting decorations
 */
export const parameterHighlightPlugin = ViewPlugin.fromClass(
    class {
        decorations: DecorationSet;

        constructor(view: EditorView) {
            this.decorations = createParameterMarkDecorations(view);
        }

        update(update: ViewUpdate) {
            if (update.docChanged || update.viewportChanged) {
                this.decorations = createParameterMarkDecorations(update.view);
            }
        }
    },
    {
        decorations: (v) => v.decorations
    }
);
