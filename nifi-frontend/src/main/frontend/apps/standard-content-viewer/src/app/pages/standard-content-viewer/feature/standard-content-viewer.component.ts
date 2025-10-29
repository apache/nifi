/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the 'License'); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, inject } from '@angular/core';
import { Store } from '@ngrx/store';
import { StandardContentViewerState } from '../../../state';
import { FormBuilder, FormGroup } from '@angular/forms';
import {
    isDefinedAndNotNull,
    selectQueryParams,
    CodeMirrorConfig,
    jsonHighlightStyle,
    xmlHighlightStyle,
    yamlHighlightStyle
} from '@nifi/shared';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ContentViewerService } from '../service/content-viewer.service';
import { HttpErrorResponse } from '@angular/common/http';
import { EditorState, Extension, Prec } from '@codemirror/state';
import {
    bracketMatching,
    foldGutter,
    foldKeymap,
    indentOnInput,
    indentUnit,
    syntaxHighlighting
} from '@codemirror/language';
import {
    crosshairCursor,
    EditorView,
    highlightActiveLine,
    highlightActiveLineGutter,
    keymap,
    lineNumbers,
    rectangularSelection
} from '@codemirror/view';
import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
import { markdown } from '@codemirror/lang-markdown';
import { xml } from '@codemirror/lang-xml';
import { yaml } from '@codemirror/lang-yaml';
import { json } from '@codemirror/lang-json';

@Component({
    selector: 'standard-content-viewer',
    templateUrl: './standard-content-viewer.component.html',
    styleUrls: ['./standard-content-viewer.component.scss'],
    standalone: false
})
export class StandardContentViewer {
    private formBuilder = inject(FormBuilder);
    private store = inject<Store<StandardContentViewerState>>(Store);
    private contentViewerService = inject(ContentViewerService);

    contentFormGroup: FormGroup;

    private _codemirrorConfig: CodeMirrorConfig = {
        plugins: [],
        focusOnInit: true
    };

    // Dynamic config getter that includes runtime state
    get codemirrorConfig(): CodeMirrorConfig {
        return {
            ...this._codemirrorConfig,
            disabled: true,
            readOnly: true
        };
    }

    // Remove the unused languageConfig object
    private ref: string | null = null;
    private mimeTypeDisplayName: string | null = null;
    private clientId: string | undefined = undefined;

    error: string | null = null;
    contentLoaded = false;

    constructor() {
        this.contentFormGroup = this.formBuilder.group({
            value: '',
            formatted: 'true'
        });

        this.store
            .select(selectQueryParams)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((queryParams) => {
                const dataRef: string | undefined = queryParams['ref'];
                const mimeTypeDisplayName: string | undefined = queryParams['mimeTypeDisplayName'];
                if (dataRef && mimeTypeDisplayName) {
                    this.ref = dataRef;
                    this.mimeTypeDisplayName = mimeTypeDisplayName;
                    this.clientId = queryParams['clientId'];

                    this.loadContent();
                }
            });
    }

    loadContent(): void {
        if (this.ref && this.mimeTypeDisplayName) {
            // Base extensions that are always included
            const baseExtensions: Extension[] = [
                lineNumbers(),
                history(),
                indentUnit.of('    '),
                EditorView.lineWrapping,
                rectangularSelection(),
                crosshairCursor(),
                EditorState.allowMultipleSelections.of(true),
                indentOnInput(),
                highlightActiveLine(),
                [highlightActiveLineGutter(), Prec.highest(lineNumbers())],
                bracketMatching(),
                EditorView.contentAttributes.of({ 'aria-label': 'Code Editor' })
            ];

            // Add language-specific extensions based on mimeTypeDisplayName
            const languageExtensions: Extension[] = [];
            switch (this.mimeTypeDisplayName) {
                case 'json':
                case 'avro':
                    languageExtensions.push(
                        json(),
                        syntaxHighlighting(jsonHighlightStyle),
                        foldGutter(),
                        keymap.of([...defaultKeymap, ...historyKeymap, ...foldKeymap])
                    );
                    break;
                case 'xml':
                    languageExtensions.push(
                        xml(),
                        syntaxHighlighting(xmlHighlightStyle),
                        foldGutter(),
                        keymap.of([...defaultKeymap, ...historyKeymap, ...foldKeymap])
                    );
                    break;
                case 'yaml':
                    languageExtensions.push(
                        yaml(),
                        syntaxHighlighting(yamlHighlightStyle),
                        foldGutter(),
                        keymap.of([...defaultKeymap, ...historyKeymap, ...foldKeymap])
                    );
                    break;
                case 'markdown':
                    languageExtensions.push(markdown(), keymap.of([...defaultKeymap, ...historyKeymap]));
                    break;
                // For text, csv, and other cases, no specific language extension is needed
                case 'text':
                case 'csv':
                default:
                    // No specific language extension, will use plain text
                    languageExtensions.push(keymap.of([...defaultKeymap, ...historyKeymap]));
                    break;
            }

            // Combine base extensions with language-specific extensions
            this._codemirrorConfig.plugins = [...baseExtensions, ...languageExtensions];

            this.contentLoaded = false;

            const formatted: string = this.contentFormGroup.get('formatted')?.value;
            this.contentViewerService
                .getContent(this.ref, this.mimeTypeDisplayName, formatted, this.clientId)
                .subscribe({
                    error: (errorResponse: HttpErrorResponse) => {
                        const errorBodyString = errorResponse.error;
                        if (typeof errorBodyString === 'string') {
                            try {
                                const errorBody = JSON.parse(errorBodyString);
                                this.error = errorBody.message;
                            } catch (e) {
                                this.error = 'Unable to load content.';
                            }
                        } else {
                            this.error = 'Unable to load content.';
                        }
                        this.contentLoaded = true;

                        this.contentFormGroup.get('value')?.setValue('');
                    },
                    next: (content) => {
                        this.error = null;
                        this.contentLoaded = true;

                        this.contentFormGroup.get('value')?.setValue(content);
                    }
                });
        }
    }
}
