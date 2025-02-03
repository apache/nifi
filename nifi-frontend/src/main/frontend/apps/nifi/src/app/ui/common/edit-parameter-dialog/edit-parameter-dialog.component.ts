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

import { Component, EventEmitter, Inject, Input, Output } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { EditParameterRequest, EditParameterResponse } from '../../../state/shared';
import { MatButtonModule } from '@angular/material/button';
import {
    AbstractControl,
    FormBuilder,
    FormControl,
    FormGroup,
    FormsModule,
    ReactiveFormsModule,
    ValidationErrors,
    ValidatorFn,
    Validators
} from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatRadioModule } from '@angular/material/radio';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { NifiSpinnerDirective } from '../spinner/nifi-spinner.directive';
import { AsyncPipe } from '@angular/common';
import { Observable } from 'rxjs';
import { NifiTooltipDirective, TextTip, CloseOnEscapeDialog, Parameter } from '@nifi/shared';

@Component({
    selector: 'edit-parameter-dialog',
    imports: [
        MatDialogModule,
        MatButtonModule,
        FormsModule,
        MatFormFieldModule,
        MatInputModule,
        ReactiveFormsModule,
        MatRadioModule,
        MatCheckboxModule,
        NifiSpinnerDirective,
        AsyncPipe,
        NifiTooltipDirective
    ],
    templateUrl: './edit-parameter-dialog.component.html',
    styleUrls: ['./edit-parameter-dialog.component.scss']
})
export class EditParameterDialog extends CloseOnEscapeDialog {
    @Input() saving$!: Observable<boolean>;
    @Output() editParameter: EventEmitter<EditParameterResponse> = new EventEmitter<EditParameterResponse>();
    @Output() close: EventEmitter<void> = new EventEmitter<void>();

    name: FormControl;
    sensitive: FormControl;
    editParameterForm: FormGroup;
    isNew: boolean;
    showSensitiveHelperText: boolean = false;
    valueInputTriggered: boolean = false;

    private originalParameter: Parameter | undefined = undefined;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditParameterRequest,
        private formBuilder: FormBuilder
    ) {
        super();
        // get the optional parameter. when existingParameters are specified this parameter is used to
        // seed the form for the new parameter. when existingParameters are not specified, this is the
        // existing parameter that populates the form
        const parameter: Parameter | undefined = request.parameter;
        this.originalParameter = parameter;

        let value: string | null;

        const validators: any[] = [Validators.required];
        if (request.existingParameters) {
            this.isNew = true;

            value = parameter ? parameter.value : null;

            // since there were existing parameters in the request, add the existing parameters validator because
            // parameters names must be unique
            validators.push(this.existingParameterValidator(request.existingParameters));

            this.name = new FormControl(parameter ? parameter.name : '', validators);

            // when seeding a new parameter with a sensitivity flag do not allow it to be changed
            const disableSensitive: boolean = parameter != null;
            this.sensitive = new FormControl(
                { value: parameter ? parameter.sensitive : false, disabled: disableSensitive },
                Validators.required
            );
        } else {
            this.isNew = false;

            value = parameter ? parameter.value : null;

            const sensitive = parameter ? parameter.sensitive : false;
            if (sensitive && value !== null) {
                value = 'Sensitive value set';
                this.showSensitiveHelperText = true;
            }

            // without existingParameters, we are editing an existing parameter. in this case the name and sensitivity cannot be modified
            this.name = new FormControl(
                { value: parameter ? parameter.name : '', disabled: true },
                Validators.required
            );
            this.sensitive = new FormControl({ value: sensitive, disabled: true }, Validators.required);
        }

        this.editParameterForm = this.formBuilder.group({
            name: this.name,
            value: new FormControl(value),
            empty: new FormControl(parameter ? parameter.value == '' : false),
            sensitive: this.sensitive,
            description: new FormControl(parameter ? parameter.description : '')
        });

        // ensure the value input is enabled/disabled according to the empty value check box state
        this.setEmptyStringChanged();
    }

    private existingParameterValidator(existingParameters: string[]): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const value = control.value;
            if (value === '') {
                return null;
            }
            if (existingParameters.includes(value)) {
                return {
                    existingParameter: true
                };
            }
            return null;
        };
    }

    getNameErrorMessage(): string {
        if (this.name.hasError('required')) {
            return 'Property name is required.';
        }

        return this.name.hasError('existingParameter') ? 'A parameter with this name already exists.' : '';
    }

    clearSensitiveHelperText(): void {
        if (this.showSensitiveHelperText) {
            this.editParameterForm.get('value')?.setValue('');
            this.showSensitiveHelperText = false;
        }

        this.valueInputTriggered = true;
    }

    setEmptyStringChanged(): void {
        const emptyStringChecked: AbstractControl | null = this.editParameterForm.get('empty');
        if (emptyStringChecked) {
            if (emptyStringChecked.value) {
                this.editParameterForm.get('value')?.setValue('');
                this.editParameterForm.get('value')?.disable();

                this.valueInputTriggered = true;
            } else {
                this.editParameterForm.get('value')?.enable();
            }
        }
    }

    cancelClicked(): void {
        this.close.next();
    }

    private valueChanged(enteredValue: string | null): boolean {
        let valueChanged = true;
        if (this.originalParameter) {
            if (this.originalParameter.sensitive) {
                valueChanged = this.valueInputTriggered;
            } else {
                valueChanged = enteredValue !== this.originalParameter.value;
            }
        }
        return valueChanged;
    }

    okClicked(): void {
        const value: string | null = this.editParameterForm.get('value')?.value;
        const empty: boolean = this.editParameterForm.get('empty')?.value;

        const parameter: Parameter = {
            name: this.editParameterForm.get('name')?.value,
            value: null,
            sensitive: this.editParameterForm.get('sensitive')?.value,
            description: this.editParameterForm.get('description')?.value
        };

        // update the parameter value
        let valueChanged = this.valueChanged(value);
        if (valueChanged || this.isNew) {
            parameter.value = value;
        }

        // indicate if the value has been removed
        const valueRemoved = value === '' && !empty;
        if (valueRemoved) {
            valueChanged = true;
            parameter.value = null;

            // if this is a new parameter there is no need to indicate that the value is removed
            if (!this.isNew) {
                parameter.valueRemoved = true;
            }
        }

        this.editParameter.next({
            parameter,
            valueChanged
        });
    }

    protected readonly TextTip = TextTip;

    override isDirty(): boolean {
        return this.editParameterForm.dirty;
    }
}
