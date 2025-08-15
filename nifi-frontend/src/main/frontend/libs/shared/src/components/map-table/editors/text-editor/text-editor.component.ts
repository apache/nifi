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

import { Component, EventEmitter, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { AbstractControl, FormBuilder, FormControl, FormGroup, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { CdkDrag } from '@angular/cdk/drag-drop';
import { CdkTrapFocus } from '@angular/cdk/a11y';
import { MatButton } from '@angular/material/button';
import { MatCheckbox } from '@angular/material/checkbox';
import { Resizable } from '../../../resizable/resizable.component';
import { Codemirror, CodeMirrorConfig } from '../../../codemirror/codemirror.component';
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
import { indentOnInput, indentUnit } from '@codemirror/language';
import { MatLabel } from '@angular/material/form-field';
import { MapTableItem } from '../../../../types';

@Component({
    selector: 'text-editor',
    imports: [
        CommonModule,
        CdkDrag,
        CdkTrapFocus,
        MatButton,
        MatCheckbox,
        ReactiveFormsModule,
        Resizable,
        MatLabel,
        FormsModule,
        Codemirror
    ],
    templateUrl: './text-editor.component.html',
    styleUrl: './text-editor.component.scss'
})
export class TextEditor {
    @Input() set item(item: MapTableItem) {
        this.textEditorForm.get('value')?.setValue(item.entry.value);
        const isEmptyString: boolean = item.entry.value === '';
        this.textEditorForm.get('setEmptyString')?.setValue(isEmptyString);
        this.setEmptyStringChanged();
        this.itemSet = true;
    }

    @Input() width!: number;
    @Input() readonly: boolean = false;

    @Output() ok: EventEmitter<string | null> = new EventEmitter<string | null>();
    @Output() exit: EventEmitter<void> = new EventEmitter<void>();

    textEditorForm: FormGroup;
    blank = false;
    itemSet = false;
    codemirrorConfig: CodeMirrorConfig = {
        plugins: [],
        focusOnInit: true
    };

    // Styling configuration
    editorStyling = {
        width: '100%',
        height: '108px'
    };

    // Dynamic config getter that includes readonly state
    get codemirrorConfigWithState(): CodeMirrorConfig {
        return {
            ...this.codemirrorConfig,
            plugins: this.getExtensions(),
            disabled: this.readonly,
            readOnly: this.readonly
        };
    }

    constructor(private formBuilder: FormBuilder) {
        this.textEditorForm = this.formBuilder.group({
            value: new FormControl(''),
            setEmptyString: new FormControl(false)
        });
    }

    cancelClicked(): void {
        this.exit.next();
    }

    okClicked(): void {
        const valueControl: AbstractControl | null = this.textEditorForm.get('value');
        const emptyStringChecked: AbstractControl | null = this.textEditorForm.get('setEmptyString');
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

    setEmptyStringChanged(): void {
        const emptyStringChecked: AbstractControl | null = this.textEditorForm.get('setEmptyString');
        if (emptyStringChecked) {
            this.blank = emptyStringChecked.value;

            if (emptyStringChecked.value) {
                this.textEditorForm.get('value')?.setValue('');
                this.textEditorForm.get('value')?.disable();
            } else {
                this.textEditorForm.get('value')?.enable();
            }
        }
    }

    resized(event: any): void {
        // Note: We calculate the height of the codemirror to fit into an `.editor` overlay. The
        // height of the codemirror needs to be set in order to handle large amounts of text in the codemirror editor.
        // The height of the codemirror should be the height of the `.editor` overlay minus the 112px of spacing
        // needed to display the 'Set Empty String' checkbox, the action buttons,
        // and the resize handle. If the amount of spacing needed for additional UX is needed for the `.editor` is
        // changed then this value should also be updated.
        this.editorStyling.width = '100%';
        this.editorStyling.height = `${event.height - 152}px`;
    }

    preventDrag(event: MouseEvent): void {
        event.stopPropagation();
    }

    codeMirrorLoaded(): void {
        // disabling of the input through the form isn't supported until codemirror
        // has loaded so we must disable again if the value is an empty string
        if (this.textEditorForm.get('setEmptyString')?.value) {
            this.textEditorForm.get('value')?.disable();
        }
    }

    getExtensions(): Extension[] {
        const setup: Extension[] = [
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
            EditorView.contentAttributes.of({ 'aria-label': 'Code Editor' }),
            keymap.of([
                { key: 'Mod-Enter', run: () => true }, // ignore Mod-Enter in `defaultKeymap` which is handled by `QueryShortcuts.ts`
                { key: 'Mod-y', run: redoSelection },
                { key: 'Shift-Mod-k', run: deleteLine },
                {
                    key: 'Enter',
                    run: () => {
                        if (this.textEditorForm.dirty && this.textEditorForm.valid) {
                            this.okClicked();
                            return true;
                        }
                        return false;
                    }
                },
                ...defaultKeymap,
                ...historyKeymap
            ])
        ];

        return setup;
    }
}
