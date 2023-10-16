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
import { PropertyItem } from '../../property-table.component';
import { CdkDrag, CdkDragHandle } from '@angular/cdk/drag-drop';
import { AbstractControl, FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatDialogModule } from '@angular/material/dialog';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { NgTemplateOutlet } from '@angular/common';
import { NifiTooltipDirective } from '../../../nifi-tooltip.directive';
import { PropertyHintTip } from '../../../tooltips/property-hint-tip/property-hint-tip.component';
import { PropertyHintTipInput } from '../../../../../state/shared';

@Component({
    selector: 'nf-editor',
    standalone: true,
    templateUrl: './nf-editor.component.html',
    imports: [
        CdkDrag,
        CdkDragHandle,
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatButtonModule,
        MatCheckboxModule,
        NgTemplateOutlet,
        NifiTooltipDirective
    ],
    styleUrls: ['./nf-editor.component.scss']
})
export class NfEditor {
    @Input() set item(item: PropertyItem) {
        this.nfEditorForm.get('value')?.setValue(item.value);

        const isEmptyString: boolean = item.value == '';
        this.nfEditorForm.get('setEmptyString')?.setValue(isEmptyString);
        if (isEmptyString) {
            this.nfEditorForm.get('value')?.disable();
        } else {
            this.nfEditorForm.get('value')?.enable();
        }

        this.supportsEl = item.descriptor.supportsEl;
    }
    @Input() supportsParameters: boolean = false;

    @Output() ok: EventEmitter<string> = new EventEmitter<string>();
    @Output() cancel: EventEmitter<void> = new EventEmitter<void>();

    protected readonly PropertyHintTip = PropertyHintTip;

    nfEditorForm: FormGroup;
    supportsEl: boolean = false;

    constructor(private formBuilder: FormBuilder) {
        this.nfEditorForm = this.formBuilder.group({
            value: new FormControl('', Validators.required),
            setEmptyString: new FormControl(false)
        });
    }

    getPropertyHintTipData(): PropertyHintTipInput {
        return {
            supportsEl: this.supportsEl,
            supportsParameters: this.supportsParameters
        };
    }

    preventDrag(event: MouseEvent): void {
        event.stopPropagation();
    }

    setEmptyStringChanged(): void {
        const emptyStringChecked: AbstractControl | null = this.nfEditorForm.get('setEmptyString');
        if (emptyStringChecked) {
            if (emptyStringChecked.value) {
                this.nfEditorForm.get('value')?.setValue('');
                this.nfEditorForm.get('value')?.disable();
            } else {
                this.nfEditorForm.get('value')?.enable();
            }
        }
    }

    okClick(): void {
        const valueControl: AbstractControl | null = this.nfEditorForm.get('value');
        if (valueControl) {
            this.ok.next(valueControl.value);
        }
    }

    cancelClicked(): void {
        this.cancel.next();
    }
}
