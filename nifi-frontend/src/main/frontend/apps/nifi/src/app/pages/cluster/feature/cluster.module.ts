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
import { Cluster } from './cluster.component';
import { CommonModule } from '@angular/common';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { StoreModule } from '@ngrx/store';
import { clusterFeatureKey, reducers } from '../state';
import { EffectsModule } from '@ngrx/effects';
import { ClusterListingEffects } from '../state/cluster-listing/cluster-listing.effects';
import { ClusterRoutingModule } from './cluster-routing.module';
import { MatTabsModule } from '@angular/material/tabs';
import { MatIconButton } from '@angular/material/button';
import { ErrorBanner } from '../../../ui/common/error-banner/error-banner.component';
import { BannerText } from '../../../ui/common/banner-text/banner-text.component';
import { ContextErrorBanner } from '../../../ui/common/context-error-banner/context-error-banner.component';

@NgModule({
    declarations: [Cluster],
    exports: [Cluster],
    imports: [
        CommonModule,
        Navigation,
        ClusterRoutingModule,
        StoreModule.forFeature(clusterFeatureKey, reducers),
        EffectsModule.forFeature(ClusterListingEffects),
        MatTabsModule,
        MatIconButton,
        ErrorBanner,
        BannerText,
        ContextErrorBanner
    ]
})
export class ClusterModule {}
