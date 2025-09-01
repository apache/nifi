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

import { Component, EventEmitter, Input, Output } from '@angular/core';
import { PropertyItem } from '../../property-item';
import { CdkDrag } from '@angular/cdk/drag-drop';
import { AbstractControl, FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatDialogModule } from '@angular/material/dialog';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { ParameterConfig } from '../../../../../state/shared';
import {
    CodemirrorNifiLanguageService,
    Parameter,
    PropertyHintTipInput,
    Codemirror,
    CodeMirrorConfig,
    PropertyHint,
    Resizable
} from '@nifi/shared';
import { A11yModule } from '@angular/cdk/a11y';
import { Extension, EditorState, Prec } from '@codemirror/state';
import {
    keymap,
    highlightActiveLine,
    lineNumbers,
    highlightActiveLineGutter,
    EditorView,
    rectangularSelection,
    crosshairCursor
} from '@codemirror/view';
import { defaultKeymap, deleteLine, history, historyKeymap, redoSelection } from '@codemirror/commands';
import { indentOnInput, bracketMatching, indentUnit } from '@codemirror/language';
import { completionKeymap } from '@codemirror/autocomplete';

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
        Codemirror,
        PropertyHint,
        Resizable
    ],
    styleUrls: ['./nf-editor.component.scss']
})
export class NfEditor {
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
        this.itemSet = true;

        this.initializeCodeMirror();
    }

    @Input() set parameterConfig(parameterConfig: ParameterConfig) {
        this.parameters = parameterConfig.parameters;
        this.supportsParameters = parameterConfig.supportsParameters;

        this.parameterConfigSet = true;

        this.initializeCodeMirror();
    }
    @Input() width!: number;
    @Input() readonly: boolean = false;

    @Output() ok: EventEmitter<string | null> = new EventEmitter<string | null>();
    @Output() exit: EventEmitter<void> = new EventEmitter<void>();

    itemSet = false;
    parameterConfigSet = false;
    nfEditorForm: FormGroup;
    showSensitiveHelperText = false;
    supportsEl = false;
    supportsParameters = false;
    blank = false;

    parameters: Parameter[] | null = null;
    private _codemirrorConfig: CodeMirrorConfig = {
        plugins: [],
        focusOnInit: true
    };

    // Styling configuration
    editorStyling = {
        width: '100%',
        height: '108px'
    };

    // Dynamic config getter that includes disabled state
    get codemirrorConfig(): CodeMirrorConfig {
        return {
            ...this._codemirrorConfig,
            disabled: this.readonly || this.blank,
            readOnly: this.readonly || this.blank
        };
    }

    constructor(
        private formBuilder: FormBuilder,
        private nifiLanguageService: CodemirrorNifiLanguageService
    ) {
        this.nfEditorForm = this.formBuilder.group({
            value: new FormControl(''),
            setEmptyString: new FormControl(false)
        });
    }

    initializeCodeMirror(): void {
        if (this.itemSet && this.parameterConfigSet) {
            const setup: Extension[] = [
                lineNumbers(),
                history(),
                indentUnit.of('    '),
                EditorView.lineWrapping,
                rectangularSelection(),
                crosshairCursor(),
                EditorState.allowMultipleSelections.of(true),
                indentOnInput(),
                bracketMatching(),
                highlightActiveLine(),
                [highlightActiveLineGutter(), Prec.highest(lineNumbers())],
                EditorView.contentAttributes.of({ 'aria-label': 'Code Editor' }),
                keymap.of([
                    { key: 'Mod-Enter', run: () => true }, // ignore Mod-Enter in `defaultKeymap` which is handled by `QueryShortcuts.ts`
                    { key: 'Mod-y', run: redoSelection },
                    { key: 'Shift-Mod-k', run: deleteLine },
                    {
                        key: 'Enter',
                        run: () => {
                            if (this.nfEditorForm.dirty && this.nfEditorForm.valid) {
                                this.okClicked();
                                return true;
                            }
                            return false;
                        }
                    },
                    ...defaultKeymap,
                    ...historyKeymap,
                    ...completionKeymap
                ])
            ];

            if (this.supportsEl || this.parameters) {
                this.nifiLanguageService.setLanguageOptions({
                    functionsEnabled: this.supportsEl,
                    parametersEnabled: this.supportsParameters,
                    parameters: this.parameters || []
                });

                this._codemirrorConfig.plugins = [this.nifiLanguageService.getLanguageSupport(), ...setup];
            } else {
                this._codemirrorConfig.plugins = setup;
            }
        }
    }

    codeMirrorLoaded(codemirror: Codemirror): void {
        if (this.showSensitiveHelperText) {
            const clearSensitiveHelperText = () => {
                if (this.showSensitiveHelperText) {
                    this.nfEditorForm.get('value')?.setValue('');
                    this.nfEditorForm.get('value')?.markAsDirty();
                    this.showSensitiveHelperText = false;
                }
            };

            codemirror.addEventListener('keydown', clearSensitiveHelperText);
        }

        // disabling of the input through the form isn't supported until codemirror
        // has loaded so we must disable again if the value is an empty string
        if (this.nfEditorForm.get('setEmptyString')?.value) {
            this.nfEditorForm.get('value')?.disable();
        }
    }

    getPropertyHintTipData(): PropertyHintTipInput {
        return {
            supportsEl: this.supportsEl,
            showParameters: true,
            supportsParameters: this.supportsParameters,
            hasParameterContext: this.parameters !== null
        };
    }

    resized(event: any): void {
        // Note: We calculate the height of the codemirror to fit into an `.nf-editor` overlay. The
        // height of the codemirror needs to be set in order to handle large amounts of text in the codemirror editor.
        // The height of the codemirror should be the height of the `.nf-editor` overlay minus the 132px of spacing
        // needed to display the EL and Param tooltips, the 'Set Empty String' checkbox, the action buttons,
        // and the resize handle. If the amount of spacing needed for additional UX is needed for the `.nf-editor` is
        // changed then this value should also be updated.
        this.editorStyling.width = '100%';
        this.editorStyling.height = `${event.height - 152}px`;
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
            } else {
                this.nfEditorForm.get('value')?.enable();
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
        this.exit.next();
    }
}
