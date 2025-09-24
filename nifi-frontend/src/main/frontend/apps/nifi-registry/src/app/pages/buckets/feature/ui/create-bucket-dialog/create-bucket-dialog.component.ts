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
import { MatDialogModule } from '@angular/material/dialog';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatIconModule } from '@angular/material/icon';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { createBucket, CreateBucketRequest } from '../../../../../state/buckets/buckets.actions';
import { MatTooltipModule } from '@angular/material/tooltip';
import { Store } from '@ngrx/store';
import { ContextErrorBanner } from '../../../../../ui/common/context-error-banner/context-error-banner.component';
import { ErrorContextKey } from '../../../../../state/error';

@Component({
    selector: 'create-bucket-dialog',
    templateUrl: './create-bucket-dialog.component.html',
    styleUrl: './create-bucket-dialog.component.scss',
    standalone: true,
    imports: [
        MatDialogModule,
        MatFormFieldModule,
        MatInputModule,
        MatButtonModule,
        MatCheckboxModule,
        MatIconModule,
        ReactiveFormsModule,
        MatTooltipModule,
        ContextErrorBanner
    ]
})
export class CreateBucketDialogComponent {
    private formBuilder = inject(FormBuilder);
    bucketForm: FormGroup;
    readonly supportsPublicRead = window?.location?.protocol === 'https:';
    private store = inject(Store);

    constructor() {
        this.bucketForm = this.formBuilder.group({
            name: ['', [Validators.required, Validators.maxLength(255)]],
            description: ['', [Validators.maxLength(1000)]],
            allowPublicRead: [{ value: false, disabled: !this.supportsPublicRead }],
            keepDialogOpen: [false]
        });
    }

    onSubmit(): void {
        if (this.bucketForm.valid) {
            const rawValue = this.bucketForm.getRawValue();
            const { name, description, allowPublicRead, keepDialogOpen } = rawValue;

            const request: CreateBucketRequest = {
                name,
                description,
                allowPublicRead
            };

            this.store.dispatch(
                createBucket({
                    request: request,
                    keepDialogOpen
                })
            );
        }
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
