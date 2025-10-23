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
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { ResourcesRoutingModule } from './resources-routing.module';
import { ResourcesComponent } from './resources.component';
import { DropletsEffects } from '../../../state/droplets/droplets.effects';
import { BucketsEffects } from '../../../state/buckets/buckets.effects';
import { reducers, resourcesFeatureKey } from '../../../state';
import { MatTableModule } from '@angular/material/table';
import { MatSortModule } from '@angular/material/sort';
import { MatMenuModule } from '@angular/material/menu';
import { DropletTableFilterComponent } from './ui/droplet-table-filter/droplet-table-filter.component';
import { MatButtonModule } from '@angular/material/button';
import { MatTooltipModule } from '@angular/material/tooltip';
import { DropletTableComponent } from './ui/droplet-table/droplet-table.component';
import { ContextErrorBanner } from '../../../ui/common/context-error-banner/context-error-banner.component';
import { HeaderComponent } from '../../../ui/header/header.component';

@NgModule({
    declarations: [ResourcesComponent],
    exports: [ResourcesComponent],
    imports: [
        CommonModule,
        MatTableModule,
        MatSortModule,
        MatMenuModule,
        MatButtonModule,
        MatTooltipModule,
        ResourcesRoutingModule,
        StoreModule.forFeature(resourcesFeatureKey, reducers),
        EffectsModule.forFeature([DropletsEffects, BucketsEffects]),
        HeaderComponent,
        DropletTableFilterComponent,
        DropletTableComponent,
        ContextErrorBanner
    ]
})
export class ResourcesModule {}
