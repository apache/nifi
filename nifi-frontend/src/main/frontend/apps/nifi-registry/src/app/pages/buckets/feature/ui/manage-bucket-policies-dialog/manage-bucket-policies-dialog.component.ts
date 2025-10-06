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
import { MatButtonModule } from '@angular/material/button';
import { Bucket } from 'apps/nifi-registry/src/app/state/buckets';

export interface ManageBucketPoliciesDialogData {
    bucket: Bucket;
}

@Component({
    selector: 'manage-bucket-policies-dialog',
    templateUrl: './manage-bucket-policies-dialog.component.html',
    styleUrl: './manage-bucket-policies-dialog.component.scss',
    standalone: true,
    imports: [MatDialogModule, MatButtonModule]
})
export class ManageBucketPoliciesDialogComponent {
    protected data = inject<ManageBucketPoliciesDialogData>(MAT_DIALOG_DATA);
}
