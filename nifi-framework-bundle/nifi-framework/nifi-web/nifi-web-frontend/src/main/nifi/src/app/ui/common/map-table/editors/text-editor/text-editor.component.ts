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

import { Component, EventEmitter, Input, Output, Renderer2, ViewContainerRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MapTableItem } from '../../map-table.component';
import { AbstractControl, FormBuilder, FormControl, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { Editor } from 'codemirror';
import { CdkDrag } from '@angular/cdk/drag-drop';
import { CdkTrapFocus } from '@angular/cdk/a11y';
import { CodemirrorModule } from '@ctrl/ngx-codemirror';
import { MatButton } from '@angular/material/button';
import { MatCheckbox } from '@angular/material/checkbox';
import { NifiTooltipDirective } from '../../../tooltips/nifi-tooltip.directive';
import { Resizable } from '../../../resizable/resizable.component';

@Component({
    selector: 'text-editor',
    standalone: true,
    imports: [
        CommonModule,
        CdkDrag,
        CdkTrapFocus,
        CodemirrorModule,
        MatButton,
        MatCheckbox,
        NifiTooltipDirective,
        ReactiveFormsModule,
        Resizable
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
    @Output() cancel: EventEmitter<void> = new EventEmitter<void>();

    textEditorForm: FormGroup;
    editor!: Editor;
    blank = false;
    itemSet = false;

    constructor(
        private formBuilder: FormBuilder,
        private viewContainerRef: ViewContainerRef,
        private renderer: Renderer2
    ) {
        this.textEditorForm = this.formBuilder.group({
            value: new FormControl(''),
            setEmptyString: new FormControl(false)
        });
    }

    cancelClicked(): void {
        this.cancel.next();
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

                if (this.editor) {
                    this.editor.setOption('readOnly', 'nocursor');
                }
            } else {
                this.textEditorForm.get('value')?.enable();

                if (this.editor) {
                    this.editor.setOption('readOnly', false);
                }
            }
        }
    }

    resized(): void {
        this.editor.setSize('100%', '100%');
    }

    preventDrag(event: MouseEvent): void {
        event.stopPropagation();
    }

    codeMirrorLoaded(codeEditor: any): void {
        this.editor = codeEditor.codeMirror;
        this.editor.setSize('100%', '100%');

        if (!this.readonly) {
            this.editor.execCommand('selectAll');
        }

        // disabling of the input through the form isn't supported until codemirror
        // has loaded so we must disable again if the value is an empty string
        if (this.textEditorForm.get('setEmptyString')?.value) {
            this.textEditorForm.get('value')?.disable();
            this.editor.setOption('readOnly', 'nocursor');
        }
    }

    getOptions(): any {
        return {
            readOnly: this.readonly,
            lineNumbers: true,
            matchBrackets: true,
            extraKeys: {
                Enter: () => {
                    if (this.textEditorForm.dirty && this.textEditorForm.valid) {
                        this.okClicked();
                    }
                }
            }
        };
    }
}
