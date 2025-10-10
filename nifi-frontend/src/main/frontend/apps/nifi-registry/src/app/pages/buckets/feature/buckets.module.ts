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

import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormsModule } from '@angular/forms';
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { BucketsRoutingModule } from './buckets-routing.module';
import { BucketsComponent } from './buckets.component';
import { BucketsEffects } from '../../../state/buckets/buckets.effects';
import { PoliciesEffects } from '../../../state/policies/policies.effects';
import { reducers, resourcesFeatureKey } from '../../../state';
import { MatTableModule } from '@angular/material/table';
import { MatSortModule } from '@angular/material/sort';
import { MatMenuModule } from '@angular/material/menu';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatDialogModule } from '@angular/material/dialog';
import { BucketTableFilterComponent } from './ui/bucket-table-filter/bucket-table-filter.component';
import { BucketTableComponent } from './ui/bucket-table/bucket-table.component';
import { CreateBucketDialogComponent } from './ui/create-bucket-dialog/create-bucket-dialog.component';
import { EditBucketDialogComponent } from './ui/edit-bucket-dialog/edit-bucket-dialog.component';
import { ManageBucketPoliciesDialogComponent } from './ui/manage-bucket-policies-dialog/manage-bucket-policies-dialog.component';
import { ContextErrorBanner } from '../../../ui/common/context-error-banner/context-error-banner.component';
import { HeaderComponent } from '../../../ui/header/header.component';

@NgModule({
    declarations: [BucketsComponent],
    exports: [BucketsComponent],
    imports: [
        BucketTableFilterComponent,
        BucketTableComponent,
        CreateBucketDialogComponent,
        EditBucketDialogComponent,
        ManageBucketPoliciesDialogComponent,
        ContextErrorBanner,
        CommonModule,
        ReactiveFormsModule,
        FormsModule,
        MatTableModule,
        MatSortModule,
        MatMenuModule,
        MatButtonModule,
        MatIconModule,
        MatFormFieldModule,
        MatInputModule,
        MatSelectModule,
        MatCheckboxModule,
        MatDialogModule,
        BucketsRoutingModule,
        StoreModule.forFeature(resourcesFeatureKey, reducers),
        EffectsModule.forFeature([BucketsEffects, PoliciesEffects]),
        HeaderComponent
    ]
})
export class BucketsModule {}
