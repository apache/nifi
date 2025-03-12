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
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import {
    FlowComparisonEntity,
    LocalChangesDialogRequest,
    NavigateToComponentRequest,
    VersionControlInformationEntity
} from '../../../../../state/flow';
import { MatButton } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { ReactiveFormsModule } from '@angular/forms';
import { LocalChangesTable } from './local-changes-table/local-changes-table';
import { CloseOnEscapeDialog } from '@nifi/shared';

@Component({
    selector: 'local-changes-dialog',
    imports: [MatDialogModule, MatButton, MatFormFieldModule, ReactiveFormsModule, LocalChangesTable],
    templateUrl: './local-changes-dialog.html',
    styleUrl: './local-changes-dialog.scss'
})
export class LocalChangesDialog extends CloseOnEscapeDialog {
    mode: 'SHOW' | 'REVERT' = 'SHOW';
    versionControlInformation: VersionControlInformationEntity;
    localModifications: FlowComparisonEntity;

    private readonly _request: LocalChangesDialogRequest;

    @Output()
    revert: EventEmitter<LocalChangesDialogRequest> = new EventEmitter<LocalChangesDialogRequest>();
    @Output() goToChange: EventEmitter<NavigateToComponentRequest> = new EventEmitter<NavigateToComponentRequest>();

    constructor(@Inject(MAT_DIALOG_DATA) private dialogRequest: LocalChangesDialogRequest) {
        super();
        this.mode = dialogRequest.mode;
        this.versionControlInformation = dialogRequest.versionControlInformation;
        this.localModifications = dialogRequest.localModifications;
        this._request = dialogRequest;
    }

    revertChanges() {
        this.revert.next(this._request);
    }
}
