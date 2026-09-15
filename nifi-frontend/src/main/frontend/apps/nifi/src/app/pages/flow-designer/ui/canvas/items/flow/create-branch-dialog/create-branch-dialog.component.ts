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

import { Component, DestroyRef, EventEmitter, Input, Output, Signal, inject } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import {
    MAT_DIALOG_DATA,
    MatDialogActions,
    MatDialogClose,
    MatDialogContent,
    MatDialogTitle
} from '@angular/material/dialog';
import {
    AbstractControl,
    FormBuilder,
    FormGroup,
    ReactiveFormsModule,
    ValidationErrors,
    Validators
} from '@angular/forms';
import { MatButton } from '@angular/material/button';
import { MatError, MatFormField, MatLabel } from '@angular/material/form-field';
import { MatInput } from '@angular/material/input';
import { CloseOnEscapeDialog, NifiSpinnerDirective } from '@nifi/shared';
import { CreateBranchDialogRequest } from '../../../../../state/flow';
import { ErrorContextKey } from '../../../../../../../state/error';
import { ContextErrorBanner } from '../../../../../../../ui/common/context-error-banner/context-error-banner.component';

@Component({
    selector: 'create-branch-dialog',
    imports: [
        MatDialogTitle,
        MatDialogContent,
        MatDialogActions,
        MatDialogClose,
        ReactiveFormsModule,
        MatButton,
        MatFormField,
        MatLabel,
        MatError,
        MatInput,
        ContextErrorBanner,
        NifiSpinnerDirective
    ],
    templateUrl: './create-branch-dialog.component.html',
    styleUrl: './create-branch-dialog.component.scss'
})
export class CreateBranchDialog extends CloseOnEscapeDialog {
    private dialogRequest = inject<CreateBranchDialogRequest>(MAT_DIALOG_DATA);
    private formBuilder = inject(FormBuilder);
    private destroyRef = inject(DestroyRef);

    @Output() createBranch: EventEmitter<string> = new EventEmitter<string>();

    @Input({ required: true }) saving!: Signal<boolean>;

    protected readonly ErrorContextKey = ErrorContextKey;

    currentBranch = this.dialogRequest.versionControlInformation.branch;

    createBranchForm: FormGroup;

    constructor() {
        super();
        this.createBranchForm = this.formBuilder.group({
            branch: [
                '',
                [Validators.required, Validators.pattern(/^(?!\s).*$/), this.branchNotCurrentValidator.bind(this)]
            ]
        });

        const branchControl = this.createBranchForm.controls['branch'];
        branchControl.valueChanges.pipe(takeUntilDestroyed(this.destroyRef)).subscribe(() => {
            if (branchControl.hasError('branchConflicts')) {
                branchControl.markAsTouched({ onlySelf: true });
            }
        });
    }

    submitForm(): void {
        if (this.createBranchForm.invalid) {
            this.createBranchForm.markAllAsTouched();
            return;
        }

        this.createBranch.emit(this.createBranchForm.controls['branch'].value.trim());
    }

    private branchNotCurrentValidator(control: AbstractControl): ValidationErrors | null {
        const value = control.value;
        if (!value) {
            return null;
        }

        if (this.currentBranch && value.trim() === this.currentBranch) {
            return { branchConflicts: true };
        }

        return null;
    }
}