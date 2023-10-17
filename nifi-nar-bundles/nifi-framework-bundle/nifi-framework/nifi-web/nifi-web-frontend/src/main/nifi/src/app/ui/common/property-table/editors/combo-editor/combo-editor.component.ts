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
import { NgForOf, NgIf, NgTemplateOutlet } from '@angular/common';
import { NifiTooltipDirective } from '../../../nifi-tooltip.directive';
import { PropertyDescriptor, AllowableValue, TextTipInput } from '../../../../../state/shared';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { MatTooltipModule } from '@angular/material/tooltip';
import { TextTip } from '../../../tooltips/text-tip/text-tip.component';

export interface AllowableValueItem extends AllowableValue {
    id: number;
}

@Component({
    selector: 'combo-editor',
    standalone: true,
    templateUrl: './combo-editor.component.html',
    imports: [
        CdkDrag,
        CdkDragHandle,
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatButtonModule,
        MatCheckboxModule,
        NgTemplateOutlet,
        NifiTooltipDirective,
        MatOptionModule,
        MatSelectModule,
        NgForOf,
        MatTooltipModule,
        NgIf
    ],
    styleUrls: ['./combo-editor.component.scss']
})
export class ComboEditor {
    @Input() set item(item: PropertyItem) {
        this.itemLookup.clear();
        this.descriptor = item.descriptor;
        this.allowableValues = [];

        let i: number = 0;
        let selectedItem: AllowableValueItem | null = null;

        if (!this.descriptor.required) {
            const noValue: AllowableValueItem = {
                id: i++,
                displayName: 'No value',
                value: null
            };
            this.itemLookup.set(noValue.id, noValue);
            this.allowableValues.push(noValue);

            if (noValue.value == item.value) {
                selectedItem = noValue;
            }
        }

        const allowableValueItems: AllowableValueItem[] = this.descriptor.allowableValues.map(
            (allowableValueEntity) => {
                const allowableValue: AllowableValueItem = {
                    ...allowableValueEntity.allowableValue,
                    id: i++
                };
                this.itemLookup.set(allowableValue.id, allowableValue);

                if (allowableValue.value == item.value) {
                    selectedItem = allowableValue;
                }

                return allowableValue;
            }
        );
        this.allowableValues.push(...allowableValueItems);

        if (selectedItem) {
            // mat-select does not have good support for options with null value so we've
            // introduced a mapping to work around the shortcoming
            this.comboEditorForm.get('value')?.setValue(selectedItem.id);
        }
    }
    @Input() supportsParameters: boolean = false;

    @Output() ok: EventEmitter<any> = new EventEmitter<any>();
    @Output() cancel: EventEmitter<void> = new EventEmitter<void>();

    protected readonly TextTip = TextTip;

    itemLookup: Map<number, AllowableValueItem> = new Map<number, AllowableValueItem>();

    comboEditorForm: FormGroup;
    descriptor!: PropertyDescriptor;
    allowableValues!: AllowableValueItem[];

    constructor(private formBuilder: FormBuilder) {
        this.comboEditorForm = this.formBuilder.group({
            value: new FormControl(null, Validators.required)
        });
    }

    preventDrag(event: MouseEvent): void {
        event.stopPropagation();
    }

    getComboPlaceholder(): string {
        const valueControl: AbstractControl | null = this.comboEditorForm.get('value');
        if (valueControl) {
            if (!this.descriptor.required && valueControl.value == null) {
                return 'No value';
            }
        }
        return '';
    }

    getAllowableValueOptionTipData(allowableValue: AllowableValue): TextTipInput {
        return {
            // @ts-ignore
            text: allowableValue.description
        };
    }

    okClicked(): void {
        const valueControl: AbstractControl | null = this.comboEditorForm.get('value');
        if (valueControl) {
            const selectedItem: AllowableValueItem | undefined = this.itemLookup.get(valueControl.value);
            if (selectedItem) {
                this.ok.next(selectedItem.value);
            }
        }
    }

    cancelClicked(): void {
        this.cancel.next();
    }
}
