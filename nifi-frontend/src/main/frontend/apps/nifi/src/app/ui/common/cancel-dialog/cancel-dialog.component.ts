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
import { MatButtonModule } from '@angular/material/button';
import { CancelDialogRequest } from '../../../state/shared';

@Component({
    selector: 'cancel-dialog',
    imports: [MatDialogModule, MatButtonModule],
    templateUrl: './cancel-dialog.component.html',
    styleUrls: ['./cancel-dialog.component.scss']
})
export class CancelDialog {
    request = inject<CancelDialogRequest>(MAT_DIALOG_DATA);

    @Output() exit: EventEmitter<void> = new EventEmitter<void>();

    cancelClicked(): void {
        this.exit.next();
    }
}
