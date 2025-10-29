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

import { baseTheme } from './baseTheme';
import { EditorView } from '@codemirror/view';
import { tags as t } from '@lezer/highlight';
import { HighlightStyle } from '@codemirror/language';

export const xmlHighlightStyle = HighlightStyle.define([
    { tag: t.tagName, color: 'var(--editor-keyword)' },
    { tag: t.attributeName, color: 'var(--editor-attribute-name)' },
    { tag: t.attributeValue, color: 'var(--editor-string)' },
    { tag: t.angleBracket, color: 'var(--editor-bracket)' },
    { tag: t.keyword, color: 'var(--editor-variable-name)' },
    { tag: t.typeName, color: 'var(--editor-variable-name) !important' },
    { tag: t.string, color: 'var(--editor-function)' },
    { tag: t.number, color: 'var(--editor-constant)' },
    { tag: t.squareBracket, color: 'var(--editor-special)' },
    { tag: t.comment, color: 'var(--editor-comment)' },
    { tag: t.separator, color: 'var(--editor-special)' },
    { tag: t.content, color: 'var(--editor-text)' },
    { tag: t.punctuation, color: 'var(--editor-variable-name)' },
    { tag: t.meta, color: 'var(--editor-function-name-variable-name)' }
]);

export const jsonHighlightStyle = HighlightStyle.define([
    { tag: t.keyword, color: 'var(--editor-variable-name)' },
    { tag: [t.atom, t.bool, t.special(t.variableName)], color: 'var(--editor-attribute-name)' },
    { tag: [t.meta, t.comment], color: 'var(--editor-comment)' },
    {
        tag: [t.typeName, t.className, t.number, t.changed, t.annotation, t.modifier, t.self, t.namespace],
        color: 'var(--editor-number)'
    },
    {
        tag: [t.operator, t.operatorKeyword, t.url, t.escape, t.regexp, t.link, t.special(t.string)],
        color: 'var(--editor-variable-name)'
    },
    { tag: t.definition(t.name), color: 'var(--editor-special)' },
    { tag: t.separator, color: 'var(--editor-text)' },
    { tag: [t.processingInstruction, t.string, t.inserted], color: 'var(--editor-string)' }
]);

export const yamlHighlightStyle = HighlightStyle.define([
    { tag: t.keyword, color: 'var(--editor-attribute-name)' },
    { tag: t.comment, color: 'var(--editor-comment)' },
    { tag: t.separator, color: 'var(--editor-special)' },
    { tag: t.punctuation, color: 'var(--editor-special)' },
    { tag: t.squareBracket, color: 'var(--editor-special)' },
    { tag: t.brace, color: 'var(--editor-special)' },
    { tag: t.content, color: 'var(--editor-text)' },
    { tag: t.attributeValue, color: 'var(--editor-special)' },
    { tag: t.string, color: 'var(--editor-function)' },
    { tag: t.definition(t.propertyName), color: 'var(--editor-keyword)' }
]);

export const defaultTheme = EditorView.theme({
    ...baseTheme,
    '.tok-comment': { color: 'var(--editor-comment)' },
    '.tok-controlKeyword': { color: 'var(--editor-control-keyword)' },
    '.tok-definitionKeyword.tok-keyword': { color: 'var(--editor-keyword)' },
    '.tok-variableName': { color: 'var(--editor-variable-name)' },
    '.tok-keyword': { color: 'var(--editor-el-function)' },
    '.tok-name': { color: 'var(--editor-text)' },
    '.tok-functionName.tok-variableName': { color: 'var(--editor-function-name-variable-name)' },
    '.tok-number': { color: 'var(--editor-number)' },
    '.tok-string2': { color: 'var(--editor-special)' },
    '.tok-string': { color: 'var(--editor-string)' },
    '.tok-typeName': { color: 'var(--editor-type-name)' },
    '.typetag': { color: 'var(--editor-type)' },
    '.darkMode & .typetag': { color: 'var(--md-ref-palette-neutral-60)' }
});
