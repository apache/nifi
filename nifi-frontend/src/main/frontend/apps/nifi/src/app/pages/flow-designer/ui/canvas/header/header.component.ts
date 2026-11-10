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

import { Component, inject } from '@angular/core';
import { CdkDragEnd } from '@angular/cdk/drag-drop';
import { ComponentType } from '@nifi/shared';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../state';
import {
    selectCanvasPermissions,
    selectControllerBulletins,
    selectControllerStatus,
    selectCurrentProcessGroupId,
    selectDragging,
    selectFlowAnalysisOpen,
    selectLastRefreshed
} from '../../../state/flow/flow.selectors';
import { createComponentRequest, setDragging } from '../../../state/flow/flow.actions';
import { LoadingService } from '../../../../../service/loading.service';
import { Client } from '../../../../../service/client.service';
import { CanvasView } from '../../../service/canvas-view.service';
import { NewCanvasItem } from '../../../../../ui/common/canvas/new-canvas-item/new-canvas-item.component';
import { MatButtonModule } from '@angular/material/button';
import { MatMenuModule } from '@angular/material/menu';
import { AsyncPipe } from '@angular/common';
import { MatDividerModule } from '@angular/material/divider';
import { FlowStatus } from './flow-status/flow-status.component';
import { Navigation } from '../../../../../ui/common/navigation/navigation.component';
import { selectClusterSummary } from '../../../../../state/cluster-summary/cluster-summary.selectors';
import { selectFlowAnalysisState } from '../../../state/flow-analysis/flow-analysis.selectors';

@Component({
    selector: 'fd-header',
    templateUrl: './header.component.html',
    imports: [NewCanvasItem, MatButtonModule, MatMenuModule, AsyncPipe, MatDividerModule, FlowStatus, Navigation],
    styleUrls: ['./header.component.scss']
})
export class HeaderComponent {
    private store = inject<Store<CanvasState>>(Store);
    private client = inject(Client);
    private canvasView = inject(CanvasView);
    loadingService = inject(LoadingService);

    protected readonly ComponentType = ComponentType;

    controllerStatus$ = this.store.select(selectControllerStatus);
    lastRefreshed$ = this.store.select(selectLastRefreshed);
    clusterSummary$ = this.store.select(selectClusterSummary);
    controllerBulletins$ = this.store.select(selectControllerBulletins);
    currentProcessGroupId$ = this.store.select(selectCurrentProcessGroupId);
    canvasPermissions$ = this.store.select(selectCanvasPermissions);
    flowAnalysisState$ = this.store.select(selectFlowAnalysisState);
    flowAnalysisOpen$ = this.store.select(selectFlowAnalysisOpen);
    dragging$ = this.store.select(selectDragging);

    onDragStarted(): void {
        this.store.dispatch(setDragging({ dragging: true }));
    }

    onDragEnded(type: ComponentType, event: CdkDragEnd): void {
        const position = this.canvasView.getCanvasPosition(event.dropPoint);
        if (position) {
            this.store.dispatch(
                createComponentRequest({
                    request: {
                        revision: {
                            clientId: this.client.getClientId(),
                            version: 0
                        },
                        type,
                        position
                    }
                })
            );
        } else {
            this.store.dispatch(setDragging({ dragging: false }));
        }
    }
}
