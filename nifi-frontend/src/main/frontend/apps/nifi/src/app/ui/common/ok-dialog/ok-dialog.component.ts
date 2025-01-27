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

import { Component, EventEmitter, Inject, Output } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { OkDialogRequest } from '../../../state/shared';
import { MatButtonModule } from '@angular/material/button';
import { CloseOnEscapeDialog } from '@nifi/shared';

@Component({
    selector: 'ok-dialog',
    imports: [MatDialogModule, MatButtonModule],
    templateUrl: './ok-dialog.component.html',
    styleUrls: ['./ok-dialog.component.scss']
})
export class OkDialog extends CloseOnEscapeDialog {
    @Output() ok: EventEmitter<void> = new EventEmitter<void>();

    constructor(@Inject(MAT_DIALOG_DATA) public request: OkDialogRequest) {
        super();
    }

    okClicked(): void {
        this.ok.next();
    }
}
