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
