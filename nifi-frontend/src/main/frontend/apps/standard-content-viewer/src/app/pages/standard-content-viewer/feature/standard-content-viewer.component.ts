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

import { Component } from '@angular/core';
import { Store } from '@ngrx/store';
import { StandardContentViewerState } from '../../../state';
import { FormBuilder, FormGroup } from '@angular/forms';
import { isDefinedAndNotNull, selectQueryParams } from '@nifi/shared';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ContentViewerService } from '../service/content-viewer.service';
import { HttpErrorResponse } from '@angular/common/http';

@Component({
    selector: 'standard-content-viewer',
    templateUrl: './standard-content-viewer.component.html',
    styleUrls: ['./standard-content-viewer.component.scss'],
    standalone: false
})
export class StandardContentViewer {
    contentFormGroup: FormGroup;

    private mode = 'text/plain';
    private ref: string | null = null;
    private mimeTypeDisplayName: string | null = null;
    private clientId: string | undefined = undefined;

    error: string | null = null;
    contentLoaded = false;

    constructor(
        private formBuilder: FormBuilder,
        private store: Store<StandardContentViewerState>,
        private contentViewerService: ContentViewerService
    ) {
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
            this.setMode(this.mimeTypeDisplayName);

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

    private setMode(mimeTypeDisplayName: string): void {
        switch (mimeTypeDisplayName) {
            case 'json':
            case 'avro':
                this.mode = 'application/json';
                break;
            case 'xml':
                this.mode = 'application/xml';
                break;
            case 'yaml':
                this.mode = 'text/x-yaml';
                break;
            case 'text':
                this.mode = 'text/plain';
                break;
            case 'csv':
                this.mode = 'text/csv';
                break;
        }
    }

    getOptions(): any {
        return {
            theme: 'nifi',
            mode: this.mode,
            lineNumbers: true,
            matchBrackets: true,
            foldGutter: true,
            gutters: ['CodeMirror-linenumbers', 'CodeMirror-foldgutter'],
            readOnly: true
        };
    }
}
