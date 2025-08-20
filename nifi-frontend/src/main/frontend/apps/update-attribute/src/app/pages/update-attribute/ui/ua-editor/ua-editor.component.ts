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
import { CdkDrag } from '@angular/cdk/drag-drop';
import { AbstractControl, FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatDialogModule } from '@angular/material/dialog';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { Codemirror, CodeMirrorConfig, Resizable, PropertyHint } from '@nifi/shared';
import { A11yModule } from '@angular/cdk/a11y';
import { CodemirrorNifiLanguageService } from '@nifi/shared';
import { EditorState, Extension, Prec } from '@codemirror/state';
import { bracketMatching, indentOnInput, indentUnit } from '@codemirror/language';
import {
    crosshairCursor,
    EditorView,
    highlightActiveLine,
    highlightActiveLineGutter,
    keymap,
    lineNumbers,
    rectangularSelection
} from '@codemirror/view';
import { defaultKeymap, deleteLine, history, historyKeymap, redoSelection } from '@codemirror/commands';
import { completionKeymap } from '@codemirror/autocomplete';

@Component({
    selector: 'ua-editor',
    templateUrl: './ua-editor.component.html',
    standalone: true,
    imports: [
        CdkDrag,
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatButtonModule,
        MatCheckboxModule,
        A11yModule,
        Resizable,
        Codemirror,
        PropertyHint
    ],
    styleUrls: ['./ua-editor.component.scss']
})
export class UaEditor {
    @Input() set value(value: string) {
        this.uaEditorForm.get('value')?.setValue(value);

        this.valueSet = true;

        this.initializeCodeMirror();
    }
    @Input() supportsEl: boolean = false;
    @Input() set required(required: boolean) {
        this.isRequired = required;

        if (this.isRequired) {
            this.uaEditorForm.get('value')?.setValidators([Validators.required]);
        } else {
            this.uaEditorForm.get('value')?.clearValidators();
        }

        this.requiredSet = true;

        this.initializeCodeMirror();
    }

    @Input() width!: number;
    @Input() readonly: boolean = false;

    @Output() ok: EventEmitter<string> = new EventEmitter<string>();
    @Output() exit: EventEmitter<void> = new EventEmitter<void>();

    isRequired: boolean = true;
    valueSet = false;
    requiredSet = false;

    uaEditorForm: FormGroup;
    private _codemirrorConfig: CodeMirrorConfig = {
        plugins: [],
        focusOnInit: true
    };

    // Styling configuration
    editorStyling = {
        width: '100%',
        height: '108px'
    };

    // Dynamic config getter that includes readonly state
    get codemirrorConfig(): CodeMirrorConfig {
        return {
            ...this._codemirrorConfig,
            disabled: this.readonly,
            readOnly: this.readonly
        };
    }

    constructor(
        private formBuilder: FormBuilder,
        private nifiLanguageService: CodemirrorNifiLanguageService
    ) {
        this.uaEditorForm = this.formBuilder.group({
            value: new FormControl('')
        });

        if (this.isRequired) {
            this.uaEditorForm.get('value')?.setValidators([Validators.required]);
        }
    }

    initializeCodeMirror(): void {
        if (this.valueSet && this.requiredSet) {
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
                            if (this.uaEditorForm.dirty && this.uaEditorForm.valid) {
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

            if (this.supportsEl) {
                this.nifiLanguageService.setLanguageOptions({
                    functionsEnabled: this.supportsEl,
                    parametersEnabled: false,
                    parameters: []
                });

                this._codemirrorConfig.plugins = [this.nifiLanguageService.getLanguageSupport(), ...setup];
            } else {
                this._codemirrorConfig.plugins = setup;
            }
        }
    }

    resized(event: any): void {
        // Note: We calculate the height of the codemirror to fit into an `.ua-editor` overlay. The
        // height of the codemirror needs to be set in order to handle large amounts of text in the codemirror editor.
        // The height of the codemirror should be the height of the `.ua-editor` overlay minus the 132px of spacing
        // needed to display the EL and Param tooltips, the 'Set Empty String' checkbox, the action buttons,
        // and the resize handle. If the amount of spacing needed for additional UX is needed for the `.ua-editor` is
        // changed then this value should also be updated.
        this.editorStyling.width = '100%';
        this.editorStyling.height = `${event.height - 152}px`;
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
        this.exit.next();
    }
}
