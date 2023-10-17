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

import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { NewPropertyDialogRequest } from '../../../state/shared';
import { MatButtonModule } from '@angular/material/button';
import { FormBuilder, FormControl, FormGroup, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatRadioModule } from '@angular/material/radio';

@Component({
    selector: 'new-property-dialog',
    standalone: true,
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
export class NewPropertyDialog {
    newPropertyForm: FormGroup;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: NewPropertyDialogRequest,
        private dialogRef: MatDialogRef<NewPropertyDialog>,
        private formBuilder: FormBuilder
    ) {
        this.newPropertyForm = this.formBuilder.group({
            name: new FormControl('', Validators.required),
            sensitive: new FormControl({ value: false, disabled: !request.allowsSensitive }, Validators.required)
        });
    }

    addProperty(): void {
        this.dialogRef.close({
            name: this.newPropertyForm.get('name')?.value,
            sensitive: this.newPropertyForm.get('sensitive')?.value
        });
    }
}
