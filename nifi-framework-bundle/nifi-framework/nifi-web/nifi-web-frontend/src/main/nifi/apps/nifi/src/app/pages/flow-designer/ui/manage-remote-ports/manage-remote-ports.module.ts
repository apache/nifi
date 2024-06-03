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
import { ManageRemotePorts } from './manage-remote-ports.component';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { ControllerServiceTable } from '../../../../ui/common/controller-service/controller-service-table/controller-service-table.component';
import { ManageRemotePortsRoutingModule } from './manage-remote-ports-routing.module';
import { Breadcrumbs } from '../common/breadcrumbs/breadcrumbs.component';
import { Navigation } from '../../../../ui/common/navigation/navigation.component';
import { MatTableModule } from '@angular/material/table';
import { MatSortModule } from '@angular/material/sort';
import { NifiTooltipDirective } from '../../../../ui/common/tooltips/nifi-tooltip.directive';
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { ManageRemotePortsEffects } from '../../state/manage-remote-ports/manage-remote-ports.effects';
import { remotePortsFeatureKey } from '../../state/manage-remote-ports';
import { manageRemotePortsReducer } from '../../state/manage-remote-ports/manage-remote-ports.reducer';
import { MatButtonModule } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';

@NgModule({
    declarations: [ManageRemotePorts],
    exports: [ManageRemotePorts],
    imports: [
        CommonModule,
        NgxSkeletonLoaderModule,
        ManageRemotePortsRoutingModule,
        StoreModule.forFeature(remotePortsFeatureKey, manageRemotePortsReducer),
        EffectsModule.forFeature(ManageRemotePortsEffects),
        ControllerServiceTable,
        Breadcrumbs,
        Navigation,
        MatTableModule,
        MatSortModule,
        NifiTooltipDirective,
        MatButtonModule,
        MatMenu,
        MatMenuItem,
        MatMenuTrigger
    ]
})
export class ManageRemotePortsModule {}
