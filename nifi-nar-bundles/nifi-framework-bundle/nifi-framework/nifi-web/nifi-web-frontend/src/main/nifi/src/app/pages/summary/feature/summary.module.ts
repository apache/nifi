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
import { Summary } from './summary.component';
import { CommonModule } from '@angular/common';
import { SummaryRoutingModule } from './summary-routing.module';
import { MatTabsModule } from '@angular/material/tabs';
import { StoreModule } from '@ngrx/store';
import { reducers, summaryFeatureKey } from '../state';
import { EffectsModule } from '@ngrx/effects';
import { ProcessorStatusListingModule } from '../ui/processor-status-listing/processor-status-listing.module';
import { ProcessGroupStatusListingModule } from '../ui/process-group-status-listing/process-group-status-listing.module';
import { ConnectionStatusListingModule } from '../ui/connection-status-listing/connection-status-listing.module';
import { RemoteProcessGroupStatusListingModule } from '../ui/remote-process-group-status-listing/remote-process-group-status-listing.module';
import { OutputPortStatusListingModule } from '../ui/output-port-status-listing/output-port-status-listing.module';
import { InputPortStatusListingModule } from '../ui/input-port-status-listing/input-port-status-listing.module';
import { SummaryListingEffects } from '../state/summary-listing/summary-listing.effects';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { ComponentClusterStatusEffects } from '../state/component-cluster-status/component-cluster-status.effects';

@NgModule({
    declarations: [Summary],
    exports: [Summary],
    imports: [
        CommonModule,
        SummaryRoutingModule,
        MatTabsModule,
        ProcessorStatusListingModule,
        ProcessGroupStatusListingModule,
        ConnectionStatusListingModule,
        RemoteProcessGroupStatusListingModule,
        ConnectionStatusListingModule,
        OutputPortStatusListingModule,
        InputPortStatusListingModule,
        StoreModule.forFeature(summaryFeatureKey, reducers),
        EffectsModule.forFeature(SummaryListingEffects, ComponentClusterStatusEffects),
        NgxSkeletonLoaderModule,
        Navigation
    ]
})
export class SummaryModule {}
