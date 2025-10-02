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

import { Component, ElementRef, OnInit, ViewChild, inject } from '@angular/core';
import { CloseOnEscapeDialog, NiFiCommon } from '@nifi/shared';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FormBuilder, FormControl, FormGroup, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { Bucket } from 'apps/nifi-registry/src/app/state/buckets';
import { MatInputModule } from '@angular/material/input';
import { Store } from '@ngrx/store';
import { createNewDroplet } from 'apps/nifi-registry/src/app/state/droplets/droplets.actions';
import { MatButtonModule } from '@angular/material/button';
import { ErrorContextKey } from 'apps/nifi-registry/src/app/state/error';
import { ContextErrorBanner } from 'apps/nifi-registry/src/app/ui/common/context-error-banner/context-error-banner.component';

export interface ImportNewFlowDialogData {
    buckets: Bucket[];
}

@Component({
    selector: 'app-import-new-flow-dialog',
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
    templateUrl: './import-new-droplet-dialog.component.html',
    styleUrl: './import-new-droplet-dialog.component.scss'
})
export class ImportNewDropletDialogComponent extends CloseOnEscapeDialog implements OnInit {
    data = inject<ImportNewFlowDialogData>(MAT_DIALOG_DATA);
    private store = inject(Store);
    private formBuilder = inject(FormBuilder);
    private nifiCommon = inject(NiFiCommon);

    @ViewChild('flowUploadControl') flowUploadControl!: ElementRef;

    protected readonly ErrorContextKey = ErrorContextKey;

    fileToUpload: File | null = null;
    writableBuckets: Bucket[] = [];
    buckets: Bucket[] = [];
    importNewFlowForm: FormGroup;
    fileNameAttached: string | null = '';

    constructor() {
        super();
        const data = this.data;

        this.buckets = data.buckets;
        this.importNewFlowForm = this.formBuilder.group({
            name: new FormControl('', Validators.required),
            description: new FormControl(null),
            bucket: new FormControl('', Validators.required),
            definition: new FormControl('', Validators.required)
        });
    }

    ngOnInit() {
        this.writableBuckets = this.filterWritableBuckets(this.buckets);

        // if there's only 1 writable bucket, always set as the initial value in the bucket dropdown
        if (this.writableBuckets.length === 1) {
            const autoSelectedBucket = this.buckets.find((b) => b.identifier === this.writableBuckets[0].identifier);
            this.importNewFlowForm.get('bucket')?.setValue(autoSelectedBucket?.identifier);
        }
    }

    filterWritableBuckets(buckets: Bucket[]) {
        const filteredWritableBuckets: Bucket[] = [];
        buckets.forEach(function (b: Bucket) {
            if (b.permissions.canWrite) {
                filteredWritableBuckets.push(b);
            }
        });
        return filteredWritableBuckets;
    }

    importNewFlow() {
        const selectedBucket: Bucket = this.writableBuckets.find((b) => {
            return b.identifier === this.importNewFlowForm.get('bucket')?.value;
        })!;

        this.store.dispatch(
            createNewDroplet({
                request: {
                    bucket: selectedBucket,
                    file: this.fileToUpload!,
                    name: this.importNewFlowForm.get('name')?.value,
                    description: this.importNewFlowForm.get('description')?.value || null
                }
            })
        );
    }

    attachFlowDefinition(event: Event): void {
        const target = event.target as HTMLInputElement;
        const files = target.files as FileList;
        const file = files.item(0);
        if (file) {
            this.importNewFlowForm.get('definition')?.setValue(this.nifiCommon.substringBeforeLast(file.name, '.'));
            this.importNewFlowForm.get('definition')?.markAsDirty();
            this.importNewFlowForm.get('description')?.setValue(null);
            this.fileNameAttached = file.name;
            this.fileToUpload = file;
        }
    }

    removeAttachedFlowDefinition() {
        this.importNewFlowForm.get('definition')?.setValue('');
        this.flowUploadControl.nativeElement.value = '';
        this.fileNameAttached = null;
        this.fileToUpload = null;
    }
}
