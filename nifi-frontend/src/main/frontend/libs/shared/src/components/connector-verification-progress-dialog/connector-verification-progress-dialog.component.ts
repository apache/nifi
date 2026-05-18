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

import { Component, EventEmitter, Input, Output, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatDialogContent, MatDialogRef, MatDialogTitle } from '@angular/material/dialog';
import { MatProgressBar } from '@angular/material/progress-bar';
import { MatLabel } from '@angular/material/form-field';
import { Observable, of } from 'rxjs';
import { ConnectorConfigStepVerificationRequest } from '../../types';

@Component({
    selector: 'shared-connector-verification-progress-dialog',
    standalone: true,
    imports: [CommonModule, MatDialogContent, MatDialogTitle, MatProgressBar, MatLabel],
    templateUrl: './connector-verification-progress-dialog.component.html',
    styleUrl: './connector-verification-progress-dialog.component.scss'
})
export class ConnectorVerificationProgressDialog {
    dialogRef = inject<MatDialogRef<ConnectorVerificationProgressDialog>>(MatDialogRef);

    @Input() stepName = '';
    @Input() verificationRequest$: Observable<ConnectorConfigStepVerificationRequest | null> = of(null);
    @Output() stopVerification = new EventEmitter<ConnectorConfigStepVerificationRequest>();

    stop(request: ConnectorConfigStepVerificationRequest) {
        this.stopVerification.next(request);
        this.dialogRef.close();
    }
}
