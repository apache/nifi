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
import { ComponentAccessPolicies } from './component-access-policies.component';
import { CommonModule, AsyncPipe } from '@angular/common';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { MatTableModule } from '@angular/material/table';
import { MatSortModule } from '@angular/material/sort';
import { MatInputModule } from '@angular/material/input';
import { MatFormFieldModule } from '@angular/material/form-field';
import { ReactiveFormsModule } from '@angular/forms';
import { MatSelectModule } from '@angular/material/select';
import { ComponentAccessPoliciesRoutingModule } from './component-access-policies-routing.module';
import { RouterModule } from '@angular/router';
import { PolicyTable } from '../common/policy-table/policy-table.component';
import { MatButtonModule } from '@angular/material/button';
import { ComponentContext, NifiTooltipDirective, ErrorBanner } from '@nifi/shared';
import { ContextErrorBanner } from '../../../../ui/common/context-error-banner/context-error-banner.component';

@NgModule({
    declarations: [ComponentAccessPolicies],
    exports: [ComponentAccessPolicies],
    imports: [
        CommonModule,
        AsyncPipe,
        ComponentAccessPoliciesRoutingModule,
        NgxSkeletonLoaderModule,
        MatTableModule,
        MatSortModule,
        MatInputModule,
        MatFormFieldModule,
        ReactiveFormsModule,
        MatSelectModule,
        RouterModule,
        NifiTooltipDirective,
        PolicyTable,
        MatButtonModule,
        ErrorBanner,
        ComponentContext,
        ContextErrorBanner
    ]
})
export class ComponentAccessPoliciesModule {}
