/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, EventEmitter, Input, OnDestroy, Output, Renderer2, ViewContainerRef } from '@angular/core';
import { CdkDrag } from '@angular/cdk/drag-drop';
import { AbstractControl, FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatDialogModule } from '@angular/material/dialog';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { Resizable, PropertyHint } from '@nifi/shared';
import { A11yModule } from '@angular/cdk/a11y';
import { CodemirrorModule } from '@ctrl/ngx-codemirror';
import { Editor } from 'codemirror';
import { NfEl } from './modes/nfel';

@Component({
    selector: 'ua-editor',
    templateUrl: './ua-editor.component.html',
    imports: [
        CdkDrag,
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatButtonModule,
        MatCheckboxModule,
        A11yModule,
        CodemirrorModule,
        Resizable,
        PropertyHint
    ],
    styleUrls: ['./ua-editor.component.scss']
})
export class UaEditor implements OnDestroy {
    @Input() set value(value: string) {
        this.uaEditorForm.get('value')?.setValue(value);
    }
    @Input() supportsEl: boolean = false;
    @Input() set required(required: boolean) {
        this.isRequired = required;

        if (this.isRequired) {
            this.uaEditorForm.get('value')?.setValidators([Validators.required]);
        } else {
            this.uaEditorForm.get('value')?.clearValidators();
        }
    }

    @Input() width!: number;
    @Input() readonly: boolean = false;

    @Output() ok: EventEmitter<string> = new EventEmitter<string>();
    @Output() close: EventEmitter<void> = new EventEmitter<void>();

    isRequired: boolean = true;

    uaEditorForm: FormGroup;
    editor!: Editor;

    constructor(
        private formBuilder: FormBuilder,
        private viewContainerRef: ViewContainerRef,
        private renderer: Renderer2,
        private nfel: NfEl
    ) {
        this.uaEditorForm = this.formBuilder.group({
            value: new FormControl('')
        });

        if (this.isRequired) {
            this.uaEditorForm.get('value')?.setValidators([Validators.required]);
        }

        this.nfel.setViewContainerRef(this.viewContainerRef, this.renderer);
        this.nfel.configureAutocomplete();
    }

    codeMirrorLoaded(codeEditor: any): void {
        this.editor = codeEditor.codeMirror;

        if (!this.readonly) {
            this.editor.focus();
            this.editor.execCommand('selectAll');
        }
    }

    getOptions(): { [p: string]: any } {
        const options: { [p: string]: any } = {
            readOnly: this.readonly,
            lineNumbers: true,
            theme: 'nifi',
            matchBrackets: true,
            extraKeys: {
                Enter: () => {
                    if (this.uaEditorForm.dirty && this.uaEditorForm.valid) {
                        this.okClicked();
                    }
                }
            }
        };

        if (this.supportsEl) {
            options['mode'] = this.nfel.getLanguageId();
            options['extraKeys'] = {
                'Ctrl-Space': 'autocomplete',
                ...options['extraKeys']
            };
        }

        return options;
    }

    resized(event: any): void {
        // Note: We calculate the height of the codemirror to fit into an `.ua-editor` overlay. The
        // height of the codemirror needs to be set in order to handle large amounts of text in the codemirror editor.
        // The height of the codemirror should be the height of the `.ua-editor` overlay minus the 132px of spacing
        // needed to display the EL and Param tooltips, the 'Set Empty String' checkbox, the action buttons,
        // and the resize handle. If the amount of spacing needed for additional UX is needed for the `.ua-editor` is
        // changed then this value should also be updated.
        this.editor.setSize('100%', event.height - 132);
    }

    preventDrag(event: MouseEvent): void {
        event.stopPropagation();
    }

    okClicked(): void {
        const valueControl: AbstractControl | null = this.uaEditorForm.get('value');
        if (valueControl) {
            this.ok.next(valueControl.value);
        }
    }

    cancelClicked(): void {
        this.close.next();
    }

    ngOnDestroy(): void {
        this.nfel.disableParameters();
    }
}
