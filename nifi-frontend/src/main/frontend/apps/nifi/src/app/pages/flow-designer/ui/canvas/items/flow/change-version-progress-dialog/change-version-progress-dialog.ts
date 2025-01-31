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

import { Component, EventEmitter, Input, Output } from '@angular/core';
import { MatDialogModule, MatDialogTitle } from '@angular/material/dialog';
import { MatButton } from '@angular/material/button';
import { FlowUpdateRequestEntity } from '../../../../../state/flow';
import { Observable, of } from 'rxjs';
import { AsyncPipe } from '@angular/common';
import { MatProgressBar } from '@angular/material/progress-bar';

@Component({
    selector: 'change-version-progress-dialog',
    imports: [MatDialogTitle, MatDialogModule, MatButton, AsyncPipe, MatProgressBar],
    templateUrl: './change-version-progress-dialog.html',
    styleUrl: './change-version-progress-dialog.scss'
})
export class ChangeVersionProgressDialog {
    @Input() flowUpdateRequest$: Observable<FlowUpdateRequestEntity | null> = of(null);
    @Output() changeVersionComplete: EventEmitter<FlowUpdateRequestEntity> =
        new EventEmitter<FlowUpdateRequestEntity>();

    close(entity: FlowUpdateRequestEntity) {
        this.changeVersionComplete.next(entity);
    }
}
