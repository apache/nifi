/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { EditorView } from '@codemirror/view';

export function removeJsonComments(json: string): string {
    const characters = json.split('');
    let inString = false;
    let escaped = false;

    for (let i = 0; i < characters.length; i++) {
        const character = characters[i];

        if (inString) {
            if (escaped) {
                escaped = false;
            } else if (character === '\\') {
                escaped = true;
            } else if (character === '"') {
                inString = false;
            }

            continue;
        }

        if (character === '"') {
            inString = true;
            continue;
        }

        if (character !== '/' || i + 1 >= characters.length) {
            continue;
        }

        const nextCharacter = characters[i + 1];

        if (nextCharacter === '/') {
            while (i < characters.length && characters[i] !== '\n' && characters[i] !== '\r') {
                characters[i] = ' ';
                i++;
            }
        } else if (nextCharacter === '*') {
            const commentStart = i;
            let commentEnd = -1;

            for (let j = i + 2; j < characters.length - 1; j++) {
                if (characters[j] === '*' && characters[j + 1] === '/') {
                    commentEnd = j + 1;
                    break;
                }
            }

            // Leave an unterminated comment unchanged so JSON parsing reports an error.
            if (commentEnd === -1) {
                continue;
            }

            for (let j = commentStart; j <= commentEnd; j++) {
                if (characters[j] !== '\n' && characters[j] !== '\r') {
                    characters[j] = ' ';
                }
            }

            i = commentEnd;
        }
    }

    return characters.join('');
}

export function joltSpecJsonParseLinter() {
    return (view: EditorView) => {
        const json = removeJsonComments(view.state.doc.toString());

        try {
            JSON.parse(json);
        } catch (error) {
            if (!(error instanceof SyntaxError)) {
                throw error;
            }

            const message = error.message;
            let position = 0;
            let match = message.match(/at position (\d+)/);

            if (match) {
                position = Math.min(Number(match[1]), view.state.doc.length);
            } else {
                match = message.match(/at line (\d+) column (\d+)/);

                if (match) {
                    const line = view.state.doc.line(Number(match[1]));
                    position = Math.min(line.from + Number(match[2]) - 1, view.state.doc.length);
                }
            }

            return [
                {
                    from: position,
                    message,
                    severity: 'error' as const,
                    to: position
                }
            ];
        }

        return [];
    };
}
