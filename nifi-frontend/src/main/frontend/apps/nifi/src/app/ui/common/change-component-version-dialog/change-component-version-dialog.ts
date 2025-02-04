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
import { MatButton } from '@angular/material/button';
import { Bundle, DocumentedType, OpenChangeComponentVersionDialogRequest } from '../../../state/shared';
import { MatFormField, MatLabel, MatOption, MatSelect } from '@angular/material/select';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { TextTip, NiFiCommon, CloseOnEscapeDialog } from '@nifi/shared';
import { ControllerServiceApi } from '../controller-service/controller-service-api/controller-service-api.component';

@Component({
    selector: 'change-component-version-dialog',
    imports: [
        MatDialogModule,
        MatButton,
        MatSelect,
        MatLabel,
        MatOption,
        MatFormField,
        ReactiveFormsModule,
        ControllerServiceApi
    ],
    templateUrl: './change-component-version-dialog.html',
    styleUrl: './change-component-version-dialog.scss'
})
export class ChangeComponentVersionDialog extends CloseOnEscapeDialog {
    versions: DocumentedType[];
    selected: DocumentedType | null = null;
    changeComponentVersionForm: FormGroup;
    private currentBundle: Bundle;

    @Output() changeVersion: EventEmitter<DocumentedType> = new EventEmitter<DocumentedType>();

    constructor(
        @Inject(MAT_DIALOG_DATA) private dialogRequest: OpenChangeComponentVersionDialogRequest,
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon
    ) {
        super();
        this.versions = dialogRequest.componentVersions;
        this.currentBundle = dialogRequest.fetchRequest.bundle;
        const idx = this.versions.findIndex(
            (version: DocumentedType) => version.bundle.version === this.currentBundle.version
        );
        this.selected = this.versions[idx > 0 ? idx : 0];
        this.changeComponentVersionForm = this.formBuilder.group({
            bundle: new FormControl(this.selected, [Validators.required])
        });
    }

    apply(): void {
        if (this.selected) {
            this.changeVersion.next(this.selected);
        }
    }

    isCurrent(selection: DocumentedType | null): boolean {
        return selection?.bundle.version === this.currentBundle.version;
    }

    getName(selected: DocumentedType | null): string {
        return this.nifiCommon.getComponentTypeLabel(selected?.type || '');
    }

    protected readonly TextTip = TextTip;

    override isDirty(): boolean {
        return this.changeComponentVersionForm.dirty;
    }
}
