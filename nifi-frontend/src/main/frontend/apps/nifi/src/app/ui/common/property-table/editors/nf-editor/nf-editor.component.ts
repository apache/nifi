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
import { PropertyItem } from '../../property-table.component';
import { CdkDrag } from '@angular/cdk/drag-drop';
import { AbstractControl, FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatDialogModule } from '@angular/material/dialog';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { Resizable, Parameter, PropertyHint } from '@nifi/shared';
import { ParameterConfig } from '../../../../../state/shared';
import { A11yModule } from '@angular/cdk/a11y';
import { CodemirrorModule } from '@ctrl/ngx-codemirror';
import { Editor } from 'codemirror';
import { NfEl } from './modes/nfel';
import { NfPr } from './modes/nfpr';

@Component({
    selector: 'nf-editor',
    templateUrl: './nf-editor.component.html',
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
    styleUrls: ['./nf-editor.component.scss']
})
export class NfEditor implements OnDestroy {
    @Input() set item(item: PropertyItem) {
        if (item.descriptor.sensitive && item.value !== null) {
            this.nfEditorForm.get('value')?.setValue('Sensitive value set');
            this.showSensitiveHelperText = true;
        } else {
            this.nfEditorForm.get('value')?.setValue(item.value);
        }

        if (item.descriptor.required) {
            this.nfEditorForm.get('value')?.addValidators(Validators.required);
        } else {
            this.nfEditorForm.get('value')?.removeValidators(Validators.required);
        }

        const isEmptyString: boolean = item.value === '';
        this.nfEditorForm.get('setEmptyString')?.setValue(isEmptyString);
        this.setEmptyStringChanged();

        this.supportsEl = item.descriptor.supportsEl;
        this.mode = this.supportsEl ? this.nfel.getLanguageId() : this.nfpr.getLanguageId();

        this.itemSet = true;
        this.loadParameters();
    }

    @Input() set parameterConfig(parameterConfig: ParameterConfig) {
        this.parameters = parameterConfig.parameters;
        this.supportsParameters = parameterConfig.supportsParameters;

        this.getParametersSet = true;
        this.loadParameters();
    }
    @Input() width!: number;
    @Input() readonly: boolean = false;

    @Output() ok: EventEmitter<string | null> = new EventEmitter<string | null>();
    @Output() close: EventEmitter<void> = new EventEmitter<void>();

    itemSet = false;
    getParametersSet = false;

    nfEditorForm: FormGroup;
    showSensitiveHelperText = false;
    supportsEl = false;
    supportsParameters = false;
    blank = false;

    mode!: string;
    parameters: Parameter[] | null = null;

    editor!: Editor;

    constructor(
        private formBuilder: FormBuilder,
        private viewContainerRef: ViewContainerRef,
        private renderer: Renderer2,
        private nfel: NfEl,
        private nfpr: NfPr
    ) {
        this.nfEditorForm = this.formBuilder.group({
            value: new FormControl(''),
            setEmptyString: new FormControl(false)
        });
    }

    codeMirrorLoaded(codeEditor: any): void {
        this.editor = codeEditor.codeMirror;

        if (!this.readonly) {
            this.editor.focus();
            this.editor.execCommand('selectAll');
        }

        if (this.showSensitiveHelperText) {
            const clearSensitiveHelperText = () => {
                if (this.showSensitiveHelperText) {
                    this.nfEditorForm.get('value')?.setValue('');
                    this.nfEditorForm.get('value')?.markAsDirty();
                    this.showSensitiveHelperText = false;
                }
            };

            this.editor.on('keydown', clearSensitiveHelperText);
        }

        // disabling of the input through the form isn't supported until codemirror
        // has loaded so we must disable again if the value is an empty string
        if (this.nfEditorForm.get('setEmptyString')?.value) {
            this.nfEditorForm.get('value')?.disable();
            this.editor.setOption('readOnly', 'nocursor');
        }
    }

    loadParameters(): void {
        if (this.itemSet) {
            this.nfel.setViewContainerRef(this.viewContainerRef, this.renderer);
            this.nfpr.setViewContainerRef(this.viewContainerRef, this.renderer);

            if (this.getParametersSet) {
                if (this.parameters) {
                    const parameters: Parameter[] = this.parameters;
                    if (this.supportsEl) {
                        this.nfel.enableParameters();
                        this.nfel.setParameters(parameters);
                        this.nfel.configureAutocomplete();
                    } else {
                        this.nfpr.enableParameters();
                        this.nfpr.setParameters(parameters);
                        this.nfpr.configureAutocomplete();
                    }
                } else {
                    this.nfel.disableParameters();
                    this.nfpr.disableParameters();

                    if (this.supportsEl) {
                        this.nfel.configureAutocomplete();
                    } else {
                        this.nfpr.configureAutocomplete();
                    }
                }
            }
        }
    }

    getOptions(): any {
        return {
            mode: this.mode,
            readOnly: this.readonly,
            lineNumbers: true,
            theme: 'nifi',
            matchBrackets: true,
            extraKeys: {
                'Ctrl-Space': 'autocomplete',
                Enter: () => {
                    if (this.nfEditorForm.dirty && this.nfEditorForm.valid) {
                        this.okClicked();
                    }
                }
            }
        };
    }

    resized(event: any): void {
        // Note: We calculate the height of the codemirror to fit into an `.nf-editor` overlay. The
        // height of the codemirror needs to be set in order to handle large amounts of text in the codemirror editor.
        // The height of the codemirror should be the height of the `.nf-editor` overlay minus the 132px of spacing
        // needed to display the EL and Param tooltips, the 'Set Empty String' checkbox, the action buttons,
        // and the resize handle. If the amount of spacing needed for additional UX is needed for the `.nf-editor` is
        // changed then this value should also be updated.
        this.editor.setSize('100%', event.height - 132);
    }

    preventDrag(event: MouseEvent): void {
        event.stopPropagation();
    }

    setEmptyStringChanged(): void {
        const emptyStringChecked: AbstractControl | null = this.nfEditorForm.get('setEmptyString');
        if (emptyStringChecked) {
            this.blank = emptyStringChecked.value;

            if (emptyStringChecked.value) {
                this.nfEditorForm.get('value')?.setValue('');
                this.nfEditorForm.get('value')?.disable();

                if (this.editor) {
                    this.editor.setOption('readOnly', 'nocursor');
                }
            } else {
                this.nfEditorForm.get('value')?.enable();

                if (this.editor) {
                    this.editor.setOption('readOnly', false);
                }
            }
        }
    }

    okClicked(): void {
        const valueControl: AbstractControl | null = this.nfEditorForm.get('value');
        const emptyStringChecked: AbstractControl | null = this.nfEditorForm.get('setEmptyString');
        if (valueControl && emptyStringChecked) {
            const value = valueControl.value;
            if (value === '') {
                if (emptyStringChecked.value) {
                    this.ok.next('');
                } else {
                    this.ok.next(null);
                }
            } else {
                this.ok.next(value);
            }
        }
    }

    cancelClicked(): void {
        this.close.next();
    }

    ngOnDestroy(): void {
        this.nfpr.disableParameters();
        this.nfel.disableParameters();
    }
}
