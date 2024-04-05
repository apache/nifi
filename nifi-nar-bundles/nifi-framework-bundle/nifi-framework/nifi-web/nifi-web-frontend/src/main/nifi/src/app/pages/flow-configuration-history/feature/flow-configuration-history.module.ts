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
import { FlowConfigurationHistory } from './flow-configuration-history.component';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { FlowConfigurationHistoryRoutingModule } from './flow-configuration-history-routing.module';
import { FlowConfigurationHistoryListing } from '../ui/flow-configuration-history-listing/flow-configuration-history-listing.component';
import { StoreModule } from '@ngrx/store';
import { flowConfigurationHistoryFeatureKey, reducers } from '../state';
import { EffectsModule } from '@ngrx/effects';
import { FlowConfigurationHistoryListingEffects } from '../state/flow-configuration-history-listing/flow-configuration-history-listing.effects';

@NgModule({
    imports: [
        CommonModule,
        Navigation,
        FlowConfigurationHistoryRoutingModule,
        FlowConfigurationHistoryListing,
        StoreModule.forFeature(flowConfigurationHistoryFeatureKey, reducers),
        EffectsModule.forFeature(FlowConfigurationHistoryListingEffects)
    ],
    declarations: [FlowConfigurationHistory],
    exports: [FlowConfigurationHistory]
})
export class FlowConfigurationHistoryModule {}
