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

import { Component, EventEmitter, Inject, Input, Output } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { KeyValuePipe, NgForOf, NgIf, NgTemplateOutlet } from '@angular/common';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { MatTabsModule } from '@angular/material/tabs';
import { FlowFileDialogRequest } from '../../../state/queue-listing';
import { CopyDirective, NiFiCommon } from '@nifi/shared';
import { TabbedDialog } from '../../../../../ui/common/tabbed-dialog/tabbed-dialog.component';

@Component({
    selector: 'flowfile-dialog',
    templateUrl: './flowfile-dialog.component.html',
    styleUrls: ['./flowfile-dialog.component.scss'],
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        NgIf,
        NgForOf,
        MatDatepickerModule,
        MatTabsModule,
        NgTemplateOutlet,
        FormsModule,
        KeyValuePipe,
        CopyDirective
    ]
})
export class FlowFileDialog extends TabbedDialog {
    @Input() contentViewerAvailable!: boolean;

    @Output() downloadContent: EventEmitter<void> = new EventEmitter<void>();
    @Output() viewContent: EventEmitter<void> = new EventEmitter<void>();

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: FlowFileDialogRequest,
        private nifiCommon: NiFiCommon
    ) {
        super('flowfile-dialog-selected-index');
    }

    formatDurationValue(duration: number): string {
        if (duration === 0) {
            return '< 1 sec';
        }

        return this.nifiCommon.formatDuration(duration);
    }

    downloadContentClicked(): void {
        this.downloadContent.next();
    }

    viewContentClicked(): void {
        this.viewContent.next();
    }
}
