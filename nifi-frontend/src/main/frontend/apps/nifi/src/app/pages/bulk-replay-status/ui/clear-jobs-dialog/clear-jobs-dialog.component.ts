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

import { Component, inject } from '@angular/core';
import { MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MatRadioModule } from '@angular/material/radio';
import { FormsModule } from '@angular/forms';

export type ClearJobsOption = 'successful' | 'finished' | 'all';

@Component({
    selector: 'clear-jobs-dialog',
    templateUrl: './clear-jobs-dialog.component.html',
    styleUrls: ['./clear-jobs-dialog.component.scss'],
    imports: [MatDialogModule, MatButtonModule, MatRadioModule, FormsModule]
})
export class ClearJobsDialog {
    private dialogRef = inject<MatDialogRef<ClearJobsDialog>>(MatDialogRef);

    selectedOption: ClearJobsOption = 'successful';

    get isAllSelected(): boolean {
        return this.selectedOption === 'all';
    }

    submit(): void {
        this.dialogRef.close(this.selectedOption);
    }
}
