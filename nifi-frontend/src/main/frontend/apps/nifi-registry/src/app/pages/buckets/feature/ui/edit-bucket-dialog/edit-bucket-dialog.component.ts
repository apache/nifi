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
import { MatDialogModule, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { Bucket } from 'apps/nifi-registry/src/app/state/buckets';
import { CopyDirective } from '@nifi/shared';
import { MatTooltipModule } from '@angular/material/tooltip';
import { Store } from '@ngrx/store';
import { updateBucket } from '../../../../../state/buckets/buckets.actions';
import { ContextErrorBanner } from '../../../../../ui/common/context-error-banner/context-error-banner.component';
import { ErrorContextKey } from '../../../../../state/error';

export interface EditBucketDialogData {
    bucket: Bucket;
}

@Component({
    selector: 'edit-bucket-dialog',
    templateUrl: './edit-bucket-dialog.component.html',
    styleUrl: './edit-bucket-dialog.component.scss',
    standalone: true,
    imports: [
        MatDialogModule,
        MatFormFieldModule,
        MatInputModule,
        MatButtonModule,
        MatCheckboxModule,
        ReactiveFormsModule,
        CopyDirective,
        MatTooltipModule,
        ContextErrorBanner
    ]
})
export class EditBucketDialogComponent {
    bucketForm: FormGroup;
    protected data = inject<EditBucketDialogData>(MAT_DIALOG_DATA);
    protected formBuilder = inject(FormBuilder);
    private store = inject(Store);

    constructor() {
        this.bucketForm = this.formBuilder.group({
            name: [this.data.bucket.name, [Validators.required, Validators.maxLength(255)]],
            description: [this.data.bucket.description || '', [Validators.maxLength(1000)]],
            allowPublicRead: [this.data.bucket.allowPublicRead],
            allowBundleRedeploy: [this.data.bucket.allowBundleRedeploy]
        });
    }

    onSaveBucket(): void {
        if (this.data.bucket && this.bucketForm.valid) {
            const bucket: Bucket = {
                ...this.data.bucket,
                name: this.bucketForm.value.name,
                description: this.bucketForm.value.description,
                allowPublicRead: this.bucketForm.value.allowPublicRead,
                allowBundleRedeploy: this.bucketForm.value.allowBundleRedeploy
            };

            this.store.dispatch(
                updateBucket({
                    request: {
                        bucket
                    }
                })
            );
        }
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
