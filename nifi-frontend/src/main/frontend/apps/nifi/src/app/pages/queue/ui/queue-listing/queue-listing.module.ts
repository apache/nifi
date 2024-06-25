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

import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { QueueListing } from './queue-listing.component';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { NifiTooltipDirective } from '@nifi/shared';
import { QueueListingRoutingModule } from './queue-listing-routing.module';
import { FlowFileTable } from './flowfile-table/flowfile-table.component';
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { queueFeatureKey, reducers } from '../../state';
import { QueueListingEffects } from '../../state/queue-listing/queue-listing.effects';
import { ErrorBanner } from '../../../../ui/common/error-banner/error-banner.component';
import { MatButtonModule } from '@angular/material/button';

@NgModule({
    declarations: [QueueListing],
    exports: [QueueListing],
    imports: [
        CommonModule,
        QueueListingRoutingModule,
        NgxSkeletonLoaderModule,
        NifiTooltipDirective,
        FlowFileTable,
        StoreModule.forFeature(queueFeatureKey, reducers),
        EffectsModule.forFeature(QueueListingEffects),
        ErrorBanner,
        MatButtonModule
    ]
})
export class QueueListingModule {}
