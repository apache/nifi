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

import { Component, EventEmitter, Output } from '@angular/core';
import { MatDialogModule } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { FormBuilder, FormControl, FormGroup, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatRadioModule } from '@angular/material/radio';
import { CloseOnEscapeDialog } from '@nifi/shared';

@Component({
    selector: 'override-policy-dialog',
    imports: [MatDialogModule, MatButtonModule, FormsModule, ReactiveFormsModule, MatRadioModule],
    templateUrl: './override-policy-dialog.component.html',
    styleUrls: ['./override-policy-dialog.component.scss']
})
export class OverridePolicyDialog extends CloseOnEscapeDialog {
    @Output() copyInheritedPolicy: EventEmitter<boolean> = new EventEmitter<boolean>();

    overridePolicyForm: FormGroup;

    constructor(private formBuilder: FormBuilder) {
        super();
        this.overridePolicyForm = this.formBuilder.group({
            override: new FormControl('copy')
        });
    }

    overrideClicked(): void {
        const override: string = this.overridePolicyForm.get('override')?.value;
        this.copyInheritedPolicy.next(override === 'copy');
    }
}
