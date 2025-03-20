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
import { MatButtonModule } from '@angular/material/button';
import { CloseOnEscapeDialog } from '../close-on-escape-dialog/close-on-escape-dialog.component';
import { YesNoDialogRequest } from '../../types';

@Component({
    selector: 'yes-no-dialog',
    imports: [MatDialogModule, MatButtonModule],
    templateUrl: './yes-no-dialog.component.html',
    styleUrls: ['./yes-no-dialog.component.scss']
})
export class YesNoDialog extends CloseOnEscapeDialog {
    @Output() yes: EventEmitter<void> = new EventEmitter<void>();
    @Output() no: EventEmitter<void> = new EventEmitter<void>();

    constructor(@Inject(MAT_DIALOG_DATA) public request: YesNoDialogRequest) {
        super();
    }

    yesClicked(): void {
        this.yes.next();
    }

    noClicked(): void {
        this.no.next();
    }
}
