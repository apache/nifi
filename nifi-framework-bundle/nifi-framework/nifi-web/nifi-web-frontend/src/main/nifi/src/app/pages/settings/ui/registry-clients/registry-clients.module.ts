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
import { RegistryClients } from './registry-clients.component';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { RegistryClientTable } from './registry-client-table/registry-client-table.component';
import { MatTableModule } from '@angular/material/table';
import { MatSortModule } from '@angular/material/sort';
import { NifiTooltipDirective } from '../../../../ui/common/tooltips/nifi-tooltip.directive';
import { MatButtonModule } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';

@NgModule({
    declarations: [RegistryClients, RegistryClientTable],
    exports: [RegistryClients],
    imports: [
        CommonModule,
        NgxSkeletonLoaderModule,
        MatTableModule,
        MatSortModule,
        NifiTooltipDirective,
        MatButtonModule,
        MatMenu,
        MatMenuItem,
        MatMenuTrigger
    ]
})
export class RegistryClientsModule {}
