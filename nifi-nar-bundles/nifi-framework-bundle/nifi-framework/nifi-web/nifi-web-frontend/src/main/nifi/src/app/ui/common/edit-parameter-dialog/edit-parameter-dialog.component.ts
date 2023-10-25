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
import { EditParameterRequest, EditParameterResponse, ParameterDetails } from '../../../state/shared';
import { MatButtonModule } from '@angular/material/button';
import {
    AbstractControl,
    FormBuilder,
    FormControl,
    FormGroup,
    FormsModule,
    ReactiveFormsModule,
    Validators
} from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatRadioModule } from '@angular/material/radio';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { NifiSpinnerDirective } from '../spinner/nifi-spinner.directive';

@Component({
    selector: 'edit-parameter-dialog',
    standalone: true,
    imports: [
        MatDialogModule,
        MatButtonModule,
        FormsModule,
        MatFormFieldModule,
        MatInputModule,
        ReactiveFormsModule,
        MatRadioModule,
        MatCheckboxModule,
        NifiSpinnerDirective
    ],
    templateUrl: './edit-parameter-dialog.component.html',
    styleUrls: ['./edit-parameter-dialog.component.scss']
})
export class EditParameterDialog {
    @Input() saving!: boolean;
    @Output() editParameter: EventEmitter<EditParameterResponse> = new EventEmitter<EditParameterResponse>();

    editParameterForm: FormGroup;
    isNew: boolean;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditParameterRequest,
        private formBuilder: FormBuilder
    ) {
        const parameter: ParameterDetails | undefined = request.parameter;

        if (parameter) {
            this.isNew = false;

            this.editParameterForm = this.formBuilder.group({
                name: new FormControl({ value: parameter.name, disabled: true }, Validators.required),
                value: new FormControl(parameter.value),
                empty: new FormControl(parameter.value == ''),
                sensitive: new FormControl({ value: parameter.sensitive, disabled: true }, Validators.required),
                description: new FormControl(parameter.description)
            });
        } else {
            this.isNew = true;

            this.editParameterForm = this.formBuilder.group({
                name: new FormControl('', Validators.required),
                value: new FormControl(''),
                empty: new FormControl(false),
                sensitive: new FormControl({ value: false, disabled: false }, Validators.required),
                description: new FormControl('')
            });
        }
    }

    setEmptyStringChanged(): void {
        const emptyStringChecked: AbstractControl | null = this.editParameterForm.get('empty');
        if (emptyStringChecked) {
            if (emptyStringChecked.value) {
                this.editParameterForm.get('value')?.setValue('');
                this.editParameterForm.get('value')?.disable();
            } else {
                this.editParameterForm.get('value')?.enable();
            }
        }
    }

    addProperty(): void {
        const value: string = this.editParameterForm.get('value')?.value;
        const empty: boolean = this.editParameterForm.get('empty')?.value;

        this.editParameter.next({
            parameter: {
                name: this.editParameterForm.get('name')?.value,
                value,
                valueRemoved: value == '' && !empty,
                sensitive: this.editParameterForm.get('sensitive')?.value,
                description: this.editParameterForm.get('description')?.value
            }
        });
    }
}
