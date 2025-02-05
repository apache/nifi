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

import { Component, Inject } from '@angular/core';
import {
    MAT_DIALOG_DATA,
    MatDialogActions,
    MatDialogClose,
    MatDialogContent,
    MatDialogTitle
} from '@angular/material/dialog';
import { MatButton, MatButtonModule } from '@angular/material/button';
import { ComponentType, isDefinedAndNotNull, ComponentContext, CloseOnEscapeDialog } from '@nifi/shared';
import {
    ClusterStatusEntity,
    ComponentClusterStatusRequest,
    ComponentClusterStatusState
} from '../../../state/component-cluster-status';
import { map, Observable } from 'rxjs';
import { AsyncPipe } from '@angular/common';
import {
    selectComponentClusterStatusEntity,
    selectComponentClusterStatusLoadedTimestamp,
    selectComponentClusterStatusLoadingStatus
} from '../../../state/component-cluster-status/component-cluster-status.selectors';
import { Store } from '@ngrx/store';
import * as ClusterStatusActions from '../../../state/component-cluster-status/component-cluster-status.actions';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ProcessorClusterTable } from './processor-cluster-table/processor-cluster-table.component';
import { PortClusterTable } from './port-cluster-table/port-cluster-table.component';
import { RemoteProcessGroupClusterTable } from './remote-process-group-cluster-table/remote-process-group-cluster-table.component';
import { ConnectionClusterTable } from './connection-cluster-table/connection-cluster-table.component';
import { ProcessGroupClusterTable } from './process-group-cluster-table/process-group-cluster-table.component';

interface Helper {
    getName: () => string;
}

@Component({
    selector: 'cluster-summary-dialog',
    imports: [
        MatDialogTitle,
        MatDialogContent,
        MatButton,
        MatDialogActions,
        MatDialogClose,
        ComponentContext,
        AsyncPipe,
        ProcessorClusterTable,
        PortClusterTable,
        RemoteProcessGroupClusterTable,
        ConnectionClusterTable,
        ProcessGroupClusterTable,
        MatButtonModule
    ],
    templateUrl: './cluster-summary-dialog.component.html',
    styleUrl: './cluster-summary-dialog.component.scss'
})
export class ClusterSummaryDialog extends CloseOnEscapeDialog {
    loading$: Observable<boolean> = this.store
        .select(selectComponentClusterStatusLoadingStatus)
        .pipe(map((status) => status === 'loading'));
    loadedTimestamp$: Observable<string> = this.store.select(selectComponentClusterStatusLoadedTimestamp);
    clusterStatusEntity$: Observable<ClusterStatusEntity | null> = this.store.select(
        selectComponentClusterStatusEntity
    );
    clusterStatusEntity: ClusterStatusEntity | null = null;
    componentId!: string;
    componentType!: ComponentType;

    componentHelper: Helper = {
        getName() {
            return '';
        }
    };

    constructor(
        private store: Store<ComponentClusterStatusState>,
        @Inject(MAT_DIALOG_DATA) private clusterStatusRequest: ComponentClusterStatusRequest
    ) {
        super();
        this.componentId = clusterStatusRequest.id;
        this.componentType = clusterStatusRequest.componentType;

        this.clusterStatusEntity$.pipe(takeUntilDestroyed(), isDefinedAndNotNull()).subscribe((entity) => {
            this.clusterStatusEntity = entity;

            switch (this.componentType) {
                case ComponentType.Processor:
                    this.componentHelper.getName = () => this.clusterStatusEntity?.processorStatus?.name || '';
                    break;
                case ComponentType.RemoteProcessGroup:
                    this.componentHelper.getName = () => this.clusterStatusEntity?.remoteProcessGroupStatus?.name || '';
                    break;
                case ComponentType.ProcessGroup:
                    this.componentHelper.getName = () => this.clusterStatusEntity?.processGroupStatus?.name || '';
                    break;
                case ComponentType.InputPort:
                case ComponentType.OutputPort:
                    this.componentHelper.getName = () => this.clusterStatusEntity?.portStatus?.name || '';
                    break;
                case ComponentType.Connection:
                    this.componentHelper.getName = () => this.clusterStatusEntity?.connectionStatus?.name || '';
                    break;
                default:
                    throw 'Unsupported Component Type';
            }
        });
    }

    refresh() {
        this.store.dispatch(
            ClusterStatusActions.loadComponentClusterStatus({
                request: {
                    id: this.componentId,
                    componentType: this.componentType
                }
            })
        );
    }

    protected readonly ComponentType = ComponentType;
}
