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

import { Component, EventEmitter, Inject, Output } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { NewPropertyDialogRequest, NewPropertyDialogResponse } from '../../../state/shared';
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
import { CloseOnEscapeDialog } from '@nifi/shared';

@Component({
    selector: 'new-property-dialog',
    imports: [
        MatDialogModule,
        MatButtonModule,
        FormsModule,
        MatFormFieldModule,
        MatInputModule,
        ReactiveFormsModule,
        MatRadioModule
    ],
    templateUrl: './new-property-dialog.component.html',
    styleUrls: ['./new-property-dialog.component.scss']
})
export class NewPropertyDialog extends CloseOnEscapeDialog {
    @Output() newProperty: EventEmitter<NewPropertyDialogResponse> = new EventEmitter<NewPropertyDialogResponse>();

    newPropertyForm: FormGroup;
    name: FormControl;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: NewPropertyDialogRequest,
        private formBuilder: FormBuilder
    ) {
        super();
        this.name = new FormControl('', [
            Validators.required,
            this.existingPropertyValidator(request.existingProperties)
        ]);

        this.newPropertyForm = this.formBuilder.group({
            name: this.name,
            sensitive: new FormControl({ value: false, disabled: !request.allowsSensitive }, Validators.required)
        });
    }

    private existingPropertyValidator(existingProperties: string[]): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const value = control.value;
            if (value === '') {
                return null;
            }
            if (existingProperties.includes(value)) {
                return {
                    existingProperty: true
                };
            }
            return null;
        };
    }

    getNameErrorMessage(): string {
        if (this.name.hasError('required')) {
            return 'Property name is required.';
        }

        return this.name.hasError('existingProperty') ? 'A property with this name already exists.' : '';
    }

    addProperty(): void {
        this.newProperty.next({
            name: this.newPropertyForm.get('name')?.value,
            sensitive: this.newPropertyForm.get('sensitive')?.value
        });
    }

    override isDirty(): boolean {
        return this.newPropertyForm.dirty;
    }
}
