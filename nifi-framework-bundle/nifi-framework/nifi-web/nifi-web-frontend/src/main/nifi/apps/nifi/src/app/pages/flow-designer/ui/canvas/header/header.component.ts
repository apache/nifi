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

import { Component } from '@angular/core';
import { ComponentType } from '../../../../../state/shared';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../state';
import {
    selectCanvasPermissions,
    selectControllerBulletins,
    selectControllerStatus,
    selectCurrentProcessGroupId,
    selectLastRefreshed
} from '../../../state/flow/flow.selectors';
import { LoadingService } from '../../../../../service/loading.service';
import { NewCanvasItem } from './new-canvas-item/new-canvas-item.component';
import { MatButtonModule } from '@angular/material/button';
import { MatMenuModule } from '@angular/material/menu';
import { AsyncPipe, NgOptimizedImage } from '@angular/common';
import { MatDividerModule } from '@angular/material/divider';
import { RouterLink } from '@angular/router';
import { FlowStatus } from './flow-status/flow-status.component';
import { Navigation } from '../../../../../ui/common/navigation/navigation.component';
import { selectClusterSummary } from '../../../../../state/cluster-summary/cluster-summary.selectors';

@Component({
    selector: 'fd-header',
    standalone: true,
    templateUrl: './header.component.html',
    imports: [
        NewCanvasItem,
        MatButtonModule,
        MatMenuModule,
        AsyncPipe,
        MatDividerModule,
        RouterLink,
        FlowStatus,
        NgOptimizedImage,
        Navigation
    ],
    styleUrls: ['./header.component.scss']
})
export class HeaderComponent {
    protected readonly ComponentType = ComponentType;

    controllerStatus$ = this.store.select(selectControllerStatus);
    lastRefreshed$ = this.store.select(selectLastRefreshed);
    clusterSummary$ = this.store.select(selectClusterSummary);
    controllerBulletins$ = this.store.select(selectControllerBulletins);
    currentProcessGroupId$ = this.store.select(selectCurrentProcessGroupId);
    canvasPermissions$ = this.store.select(selectCanvasPermissions);

    constructor(
        private store: Store<CanvasState>,
        public loadingService: LoadingService
    ) {}
}
