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

import { Component, EventEmitter, Inject, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import {
    AbstractControl,
    FormBuilder,
    FormControl,
    FormGroup,
    ReactiveFormsModule,
    ValidationErrors,
    ValidatorFn,
    Validators
} from '@angular/forms';
import {
    MAT_DIALOG_DATA,
    MatDialogActions,
    MatDialogClose,
    MatDialogContent,
    MatDialogTitle
} from '@angular/material/dialog';
import { CloseOnEscapeDialog } from '../close-on-escape-dialog/close-on-escape-dialog.component';
import { MatButton } from '@angular/material/button';
import { MatError, MatFormField, MatLabel } from '@angular/material/form-field';
import { MatInput } from '@angular/material/input';
import { MapTableEntryData } from '../../types';

@Component({
    selector: 'new-map-table-entry-dialog',
    imports: [
        CommonModule,
        MatButton,
        MatDialogActions,
        MatDialogClose,
        MatDialogContent,
        MatDialogTitle,
        MatError,
        MatFormField,
        MatInput,
        MatLabel,
        ReactiveFormsModule
    ],
    templateUrl: './new-map-table-entry-dialog.component.html',
    styleUrl: './new-map-table-entry-dialog.component.scss'
})
export class NewMapTableEntryDialog extends CloseOnEscapeDialog {
    @Output() newEntry: EventEmitter<string> = new EventEmitter<string>();

    newEntryForm: FormGroup;
    name: FormControl;

    constructor(
        private formBuilder: FormBuilder,
        @Inject(MAT_DIALOG_DATA) public data: MapTableEntryData
    ) {
        super();
        this.name = new FormControl(null, [
            Validators.required,
            this.existingEntryValidator(this.data.existingEntries)
        ]);
        this.newEntryForm = formBuilder.group({
            name: this.name
        });
    }

    private existingEntryValidator(existingEntries: string[]): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const value = control.value;
            if (value === '') {
                return null;
            }
            if (existingEntries.includes(value)) {
                return {
                    existingEntry: true
                };
            }
            return null;
        };
    }

    getNameErrorMessage(): string {
        if (this.name) {
            if (this.name.hasError('required')) {
                return 'Name is required.';
            }

            return this.name.hasError('existingEntry') ? 'Name already exists.' : '';
        }
        return '';
    }

    addClicked() {
        this.newEntry.next(this.name.value);
    }
}
