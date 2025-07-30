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

import { Component, EventEmitter, Input, Output, ViewContainerRef } from '@angular/core';
import { PropertyItem } from '../../property-item';
import { CdkDrag } from '@angular/cdk/drag-drop';
import { AbstractControl, FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatDialogModule } from '@angular/material/dialog';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { ParameterConfig } from '../../../../../state/shared';
import { Parameter, PropertyHintTipInput } from '@nifi/shared';
import { A11yModule } from '@angular/cdk/a11y';
import { Extension, EditorState, Prec } from '@codemirror/state';
import {
    keymap,
    highlightActiveLine,
    lineNumbers,
    highlightSpecialChars,
    highlightActiveLineGutter,
    EditorView,
    rectangularSelection,
    crosshairCursor
} from '@codemirror/view';
import {
    acceptCompletion,
    autocompletion,
    closeBrackets,
    closeBracketsKeymap,
    completionKeymap
} from '@codemirror/autocomplete';
import { defaultKeymap, deleteLine, history, historyKeymap, redoSelection } from '@codemirror/commands';
import {
    defaultHighlightStyle,
    syntaxHighlighting,
    indentOnInput,
    bracketMatching,
    foldKeymap,
    StreamLanguage,
    indentUnit,
    LanguageDescription
} from '@codemirror/language';
import { searchKeymap, highlightSelectionMatches } from '@codemirror/search';
import {
    Codemirror,
    CodeMirrorConfig,
    PropertyHint,
    Resizable,
    elFunctionHighlightPlugin,
    parameterHighlightPlugin,
    highlightStyle,
    CodemirrorNifiLanguagePackage,
    NfLanguageConfig,
    NfLanguageDefinition
} from '@nifi/shared';
import { markdown } from '@codemirror/lang-markdown';
import { xml } from '@codemirror/lang-xml';
import { foldGutter } from '@codemirror/language';

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
    nfLanguageDefinition: NfLanguageDefinition | null = null;
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

    // Language configuration
    languageConfig = {
        language: this.nifiLanguagePackage.getLanguageId(),
        languages: [] as LanguageDescription[]
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
        private viewContainerRef: ViewContainerRef,
        private nifiLanguagePackage: CodemirrorNifiLanguagePackage
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
                parameterHighlightPlugin,
                elFunctionHighlightPlugin,
                EditorView.lineWrapping,
                rectangularSelection(),
                crosshairCursor(),
                EditorState.allowMultipleSelections.of(true),
                indentOnInput(),
                highlightSpecialChars(),
                syntaxHighlighting(highlightStyle),
                syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
                bracketMatching(),
                closeBrackets(),
                highlightActiveLine(),
                highlightSelectionMatches(),
                [highlightActiveLineGutter(), Prec.highest(lineNumbers())],
                foldGutter(),
                autocompletion(),
                markdown(),
                xml(),
                EditorView.contentAttributes.of({ 'aria-label': 'Code Editor' }),
                keymap.of([
                    { key: 'Mod-Enter', run: () => true }, // ignore Mod-Enter in `defaultKeymap` which is handled by `QueryShortcuts.ts`
                    { key: 'Ctrl-Enter', run: () => true }, // ignore Ctrl-Enter in `defaultKeymap` which is handled by `QueryShortcuts.ts`
                    { key: 'Mod-y', run: redoSelection },
                    { key: 'Shift-Ctrl-k', run: deleteLine },
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
                    ...closeBracketsKeymap,
                    ...defaultKeymap,
                    ...historyKeymap,
                    ...foldKeymap,
                    ...searchKeymap,
                    ...completionKeymap
                ])
            ];

            if (this.supportsEl || this.parameters) {
                this.nfLanguageDefinition = {
                    supportsEl: this.supportsEl
                };

                if (this.parameters) {
                    this.nfLanguageDefinition.parameterListing = this.parameters;
                }

                const nfLanguageConfig: NfLanguageConfig = this.nifiLanguagePackage.getLanguageMode(
                    this.nfLanguageDefinition
                );

                this._codemirrorConfig.plugins = [
                    StreamLanguage.define(nfLanguageConfig.streamParser),
                    autocompletion({
                        override: [nfLanguageConfig.getAutocompletions(this.viewContainerRef)]
                    }),
                    Prec.highest(keymap.of([{ key: 'Tab', run: acceptCompletion }])),
                    setup
                ];
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
