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

import { Component, EventEmitter, Output, inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { MatButton } from '@angular/material/button';
import { ReactiveFormsModule, FormBuilder, Validators } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { CreateBranchDialogRequest } from '../../../../../state/flow';
import { CloseOnEscapeDialog } from '@nifi/shared';
import { CommonModule } from '@angular/common';

@Component({
    selector: 'create-branch-dialog',
    imports: [CommonModule, MatDialogModule, MatFormFieldModule, MatInputModule, MatButton, ReactiveFormsModule],
    templateUrl: './create-branch-dialog.html',
    styleUrl: './create-branch-dialog.scss'
})
export class CreateBranchDialog extends CloseOnEscapeDialog {
    private dialogRequest = inject<CreateBranchDialogRequest>(MAT_DIALOG_DATA);
    private formBuilder = inject(FormBuilder);

    createBranchForm = this.formBuilder.group({
        branch: ['', [Validators.required, Validators.pattern(/^(?!\s).*$/), this.branchNotCurrentValidator.bind(this)]]
    });

    currentBranch = this.dialogRequest.versionControlInformation.branch;

    @Output() createBranch: EventEmitter<string> = new EventEmitter<string>();

    branchNotCurrentValidator(control: { value: string }) {
        if (!control.value) {
            return null;
        }

        const trimmedValue = control.value.trim();

        if (this.currentBranch && trimmedValue === this.currentBranch) {
            return {
                branchConflicts: true
            };
        }

        return null;
    }

    submit() {
        if (this.createBranchForm.valid) {
            const branch = this.createBranchForm.controls.branch.value?.trim();
            if (branch) {
                this.createBranch.emit(branch);
            }
        } else {
            this.createBranchForm.markAllAsTouched();
        }
    }
}
