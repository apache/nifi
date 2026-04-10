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

import { Component, EventEmitter, Input, Output, inject } from '@angular/core';
import { AsyncPipe } from '@angular/common';
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
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { Observable } from 'rxjs';
import { CloseOnEscapeDialog, ConnectorEntity, NifiSpinnerDirective } from '@nifi/shared';
import { RenameConnectorRequest } from '../../state';
import { ContextErrorBanner } from '../../../../ui/common/context-error-banner/context-error-banner.component';
import { ErrorContextKey } from '../../../../state/error';

export interface RenameConnectorDialogData {
    connector: ConnectorEntity;
}

@Component({
    selector: 'rename-connector-dialog',
    imports: [
        AsyncPipe,
        MatDialogModule,
        MatButtonModule,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatInputModule,
        NifiSpinnerDirective,
        ContextErrorBanner
    ],
    templateUrl: './rename-connector-dialog.component.html',
    styleUrls: ['./rename-connector-dialog.component.scss']
})
export class RenameConnectorDialog extends CloseOnEscapeDialog {
    private formBuilder = inject(FormBuilder);
    private data = inject<RenameConnectorDialogData>(MAT_DIALOG_DATA);

    @Input() saving$!: Observable<boolean>;
    @Output() rename = new EventEmitter<RenameConnectorRequest>();
    @Output() exit = new EventEmitter<void>();

    protected readonly ErrorContextKey = ErrorContextKey;

    renameForm: FormGroup;
    currentName: string;

    constructor() {
        super();
        this.currentName = this.data.connector.component.name;

        this.renameForm = this.formBuilder.group({
            name: new FormControl(this.currentName, [
                Validators.required,
                this.notBlankValidator(),
                this.notSameNameValidator(this.currentName)
            ])
        });
    }

    private notBlankValidator(): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const value = control.value;
            if (value && value.trim().length === 0) {
                return { notBlank: true };
            }
            return null;
        };
    }

    private notSameNameValidator(currentName: string): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const value = control.value;
            if (value && value.trim() === currentName) {
                return { sameName: true };
            }
            return null;
        };
    }

    getNameErrorMessage(): string {
        const nameControl = this.renameForm.get('name');
        if (nameControl?.hasError('required')) {
            return 'Name is required.';
        }
        if (nameControl?.hasError('notBlank')) {
            return 'Name cannot be blank.';
        }
        if (nameControl?.hasError('sameName')) {
            return 'Name must be different from the current name.';
        }
        return '';
    }

    renameClicked(): void {
        if (this.renameForm.invalid) {
            return;
        }

        const newName = this.renameForm.get('name')?.value?.trim();
        this.rename.emit({
            connector: this.data.connector,
            newName
        });
    }

    cancelClicked(): void {
        this.exit.emit();
    }

    override isDirty(): boolean {
        return this.renameForm.dirty;
    }
}
