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

import { Component, ElementRef, ViewChild, inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { CloseOnEscapeDialog, NiFiCommon } from '@nifi/shared';
import { FormBuilder, FormControl, FormGroup, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { Store } from '@ngrx/store';
import { importNewDropletVersion } from 'apps/nifi-registry/src/app/state/droplets/droplets.actions';
import { Droplet } from 'apps/nifi-registry/src/app/state/droplets';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { ErrorContextKey } from 'apps/nifi-registry/src/app/state/error';
import { ContextErrorBanner } from 'apps/nifi-registry/src/app/ui/common/context-error-banner/context-error-banner.component';

export interface ImportNewFlowVersionDialogData {
    droplet: Droplet;
}

@Component({
    selector: 'app-import-new-flow-version-dialog',
    imports: [
        MatDialogModule,
        FormsModule,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatSelectModule,
        MatInputModule,
        MatButtonModule,
        ContextErrorBanner
    ],
    templateUrl: './import-new-droplet-version-dialog.component.html',
    styleUrl: './import-new-droplet-version-dialog.component.scss'
})
export class ImportNewDropletVersionDialogComponent extends CloseOnEscapeDialog {
    data = inject<ImportNewFlowVersionDialogData>(MAT_DIALOG_DATA);
    private formBuilder = inject(FormBuilder);
    private store = inject(Store);
    private nifiCommon = inject(NiFiCommon);

    @ViewChild('flowUploadControl') flowUploadControl!: ElementRef;

    protected readonly ErrorContextKey = ErrorContextKey;
    extensions = 'application/json';
    fileName: string | null = null;
    fileToUpload: File | null = null;
    name = '';
    description = '';
    droplet: Droplet;
    comments = '';
    importNewFlowVersionForm: FormGroup;
    fileNameAttached: string | null = '';

    constructor() {
        super();
        const data = this.data;

        this.droplet = data.droplet;

        this.importNewFlowVersionForm = this.formBuilder.group({
            newFlowVersionDefinition: new FormControl('', Validators.required),
            newFlowVersionComments: new FormControl(null)
        });
    }

    attachFlowDefinition(event: Event): void {
        const target = event.target as HTMLInputElement;
        const files = target.files as FileList;
        const file = files.item(0);
        if (file) {
            this.importNewFlowVersionForm
                .get('newFlowVersionDefinition')
                ?.setValue(this.nifiCommon.substringBeforeLast(file.name, '.'));
            this.importNewFlowVersionForm.get('newFlowVersionDefinition')?.markAsDirty();
            this.importNewFlowVersionForm.get('newFlowVersionComments')?.setValue(null);
            this.fileNameAttached = file.name;
            this.fileToUpload = file;
        }
    }

    removeAttachedFlowDefinition() {
        this.importNewFlowVersionForm.get('newFlowVersionDefinition')?.setValue('');
        this.flowUploadControl.nativeElement.value = '';
        this.fileNameAttached = null;
        this.fileToUpload = null;
    }

    importNewFlowVersion() {
        this.store.dispatch(
            importNewDropletVersion({
                request: {
                    href: this.droplet.link.href,
                    file: this.fileToUpload!,
                    description: this.importNewFlowVersionForm.get('newFlowVersionComments')?.value || null
                }
            })
        );
    }
}
