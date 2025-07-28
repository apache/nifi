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

export const defaultTheme = EditorView.theme({
    ...baseTheme,
    '.tok-comment': { color: 'var(--nf-codemirror-comment)' },
    '.tok-controlKeyword': { color: 'var(--nf-codemirror-keyword)' },
    '.tok-definitionKeyword.tok-keyword': { color: 'var(--nf-codemirror-keyword)' },
    '.tok-variableName': { color: 'var(--nf-codemirror-variable)' },
    '.tok-keyword': { color: 'var(--nf-codemirror-keyword)' },
    '.tok-name': { color: 'var(--nf-codemirror-text)' },
    '.tok-functionName.tok-variableName': { color: 'var(--nf-codemirror-variable)' },
    '.tok-number': { color: 'var(--nf-codemirror-number)' },
    '.tok-string2': { color: 'var(--nf-codemirror-string2)' },
    '.tok-string': { color: 'var(--nf-codemirror-string)' },
    '.tok-typeName': { color: 'var(--nf-codemirror-number)' },
    '.typetag': { color: 'var(--nf-codemirror-tag)' },
    '.darkMode & .typetag': { color: 'var(--md-ref-palette-neutral-60)' }
});
