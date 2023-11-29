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
import { NifiTooltipDirective } from '../../../tooltips/nifi-tooltip.directive';
import { AllowableValue, Parameter, PropertyDescriptor, TextTipInput } from '../../../../../state/shared';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { MatTooltipModule } from '@angular/material/tooltip';
import { TextTip } from '../../../tooltips/text-tip/text-tip.component';
import { A11yModule } from '@angular/cdk/a11y';
import { Observable, take } from 'rxjs';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';

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
        NgIf,
        A11yModule,
        NgxSkeletonLoaderModule
    ],
    styleUrls: ['./combo-editor.component.scss']
})
export class ComboEditor {
    @Input() set item(item: PropertyItem) {
        if (item.value != null) {
            this.configuredValue = item.value;
        } else if (item.descriptor.defaultValue != null) {
            this.configuredValue = item.descriptor.defaultValue;
        }

        this.descriptor = item.descriptor;
        this.sensitive = item.descriptor.sensitive;

        this.itemSet = true;
        this.initialAllowableValues();
    }

    @Input() set getParameters(getParameters: (sensitive: boolean) => Observable<Parameter[]>) {
        this._getParameters = getParameters;

        this.supportsParameters = getParameters != null;
        this.initialAllowableValues();
    }

    @Output() ok: EventEmitter<any> = new EventEmitter<any>();
    @Output() cancel: EventEmitter<void> = new EventEmitter<void>();

    protected readonly TextTip = TextTip;

    itemLookup: Map<number, AllowableValueItem> = new Map<number, AllowableValueItem>();
    referencesParametersId: number = -1;
    configuredParameterId: number = -1;

    comboEditorForm: FormGroup;
    descriptor!: PropertyDescriptor;
    allowableValues!: AllowableValueItem[];

    showParameterAllowableValues: boolean = false;
    parameterAllowableValues!: AllowableValueItem[];

    sensitive: boolean = false;
    supportsParameters: boolean = false;
    parametersLoaded: boolean = false;

    itemSet: boolean = false;
    configuredValue: string | null = null;
    _getParameters!: (sensitive: boolean) => Observable<Parameter[]>;

    constructor(private formBuilder: FormBuilder) {
        this.comboEditorForm = this.formBuilder.group({
            value: new FormControl(null, Validators.required)
        });
    }

    initialAllowableValues(): void {
        if (this.itemSet) {
            this.itemLookup.clear();
            this.allowableValues = [];
            this.referencesParametersId = -1;

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

                if (noValue.value == this.configuredValue) {
                    selectedItem = noValue;
                }
            }

            if (this.descriptor.allowableValues) {
                const allowableValueItems: AllowableValueItem[] = this.descriptor.allowableValues.map(
                    (allowableValueEntity) => {
                        const allowableValue: AllowableValueItem = {
                            ...allowableValueEntity.allowableValue,
                            id: i++
                        };
                        this.itemLookup.set(allowableValue.id, allowableValue);

                        if (allowableValue.value == this.configuredValue) {
                            selectedItem = allowableValue;
                        }

                        return allowableValue;
                    }
                );
                this.allowableValues.push(...allowableValueItems);
            }

            if (this.supportsParameters) {
                this.parametersLoaded = false;

                // parameters are supported so add the item to support showing
                // and hiding the parameter options select
                const referencesParameterOption: AllowableValueItem = {
                    id: i++,
                    displayName: 'Reference Parameter...',
                    value: null
                };
                this.allowableValues.push(referencesParameterOption);
                this.itemLookup.set(referencesParameterOption.id, referencesParameterOption);

                // record the item of the item to more easily identify this item
                this.referencesParametersId = referencesParameterOption.id;

                // if the current value references a parameter auto select the
                // references parameter item
                if (this.referencesParameter(this.configuredValue)) {
                    selectedItem = referencesParameterOption;

                    // trigger allowable value changed to show the parameters
                    this.allowableValueChanged(this.referencesParametersId);
                }

                this._getParameters(this.sensitive)
                    .pipe(take(1))
                    .subscribe((parameters) => {
                        if (parameters.length > 0) {
                            // capture the value of i which will be the id of the first
                            // parameter
                            this.configuredParameterId = i;

                            // create allowable values for each parameter
                            parameters.forEach((parameter) => {
                                const parameterItem: AllowableValueItem = {
                                    id: i++,
                                    displayName: parameter.name,
                                    value: '#{' + parameter.name + '}',
                                    description: parameter.description
                                };
                                this.parameterAllowableValues.push(parameterItem);
                                this.itemLookup.set(parameterItem.id, parameterItem);

                                // if the configured parameter is still available,
                                // capture the id, so we can auto select it
                                if (parameterItem.value === this.configuredValue) {
                                    this.configuredParameterId = parameterItem.id;
                                }
                            });

                            // if combo still set to reference a parameter, set the default value
                            if (this.comboEditorForm.get('value')?.value == this.referencesParametersId) {
                                this.comboEditorForm.get('parameterReference')?.setValue(this.configuredParameterId);
                            }
                        }

                        this.parametersLoaded = true;
                    });
            } else {
                this.parameterAllowableValues = [];
            }

            if (selectedItem) {
                // mat-select does not have good support for options with null value so we've
                // introduced a mapping to work around the shortcoming
                this.comboEditorForm.get('value')?.setValue(selectedItem.id);
            }
        }
    }

    referencesParameter(value: string | null): boolean {
        if (value) {
            return value.startsWith('#{') && value.endsWith('}');
        }

        return false;
    }

    preventDrag(event: MouseEvent): void {
        event.stopPropagation();
    }

    allowableValueChanged(value: number): void {
        this.showParameterAllowableValues = value === this.referencesParametersId;

        if (this.showParameterAllowableValues) {
            if (this.configuredParameterId === -1) {
                this.comboEditorForm.addControl('parameterReference', new FormControl(null, Validators.required));
            } else {
                this.comboEditorForm.addControl(
                    'parameterReference',
                    new FormControl(this.configuredParameterId, Validators.required)
                );
            }
        } else {
            this.comboEditorForm.removeControl('parameterReference');
        }
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
                // if the value currently references a parameter emit the parameter, get the parameter reference control and emit that value
                if (selectedItem.id == this.referencesParametersId) {
                    const parameterReferenceControl: AbstractControl | null =
                        this.comboEditorForm.get('parameterReference');
                    if (parameterReferenceControl) {
                        const selectedParameterItem: AllowableValueItem | undefined = this.itemLookup.get(
                            parameterReferenceControl.value
                        );
                        if (selectedParameterItem) {
                            this.ok.next(selectedParameterItem.value);
                        }
                    }
                } else {
                    this.ok.next(selectedItem.value);
                }
            }
        }
    }

    cancelClicked(): void {
        this.cancel.next();
    }
}
