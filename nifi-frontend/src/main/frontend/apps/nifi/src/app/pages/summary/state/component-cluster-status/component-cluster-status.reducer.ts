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

import { createReducer, on } from '@ngrx/store';
import { ComponentClusterStatusState } from './index';
import * as ClusterStatusActions from './component-cluster-status.actions';
import { ComponentType } from '@nifi/shared';

export const initialComponentClusterStatusState: ComponentClusterStatusState = {
    clusterStatus: null,
    status: 'pending',
    loadedTimestamp: '',
    latestRequest: null
};

export const componentClusterStatusReducer = createReducer(
    initialComponentClusterStatusState,

    on(
        ClusterStatusActions.loadComponentClusterStatusAndOpenDialog,
        ClusterStatusActions.loadComponentClusterStatus,
        (state, { request }) => ({
            ...state,
            status: 'loading' as const,
            latestRequest: request
        })
    ),

    on(
        ClusterStatusActions.loadComponentClusterStatusSuccess,
        ClusterStatusActions.openComponentClusterStatusDialog,
        (state, { response }) => {
            let loadedTimestamp = '';
            switch (response.componentType) {
                case ComponentType.Processor:
                    loadedTimestamp = response.clusterStatusEntity.processorStatus?.statsLastRefreshed || '';
                    break;
                case ComponentType.RemoteProcessGroup:
                    loadedTimestamp = response.clusterStatusEntity.remoteProcessGroupStatus?.statsLastRefreshed || '';
                    break;
                case ComponentType.ProcessGroup:
                    loadedTimestamp = response.clusterStatusEntity.processGroupStatus?.statsLastRefreshed || '';
                    break;
                case ComponentType.InputPort:
                case ComponentType.OutputPort:
                    loadedTimestamp = response.clusterStatusEntity.portStatus?.statsLastRefreshed || '';
                    break;
                case ComponentType.Connection:
                    loadedTimestamp = response.clusterStatusEntity.connectionStatus?.statsLastRefreshed || '';
                    break;
                default:
                    loadedTimestamp = '';
            }
            return {
                ...state,
                status: 'success' as const,
                clusterStatus: response.clusterStatusEntity,
                loadedTimestamp: loadedTimestamp
            };
        }
    ),

    on(ClusterStatusActions.resetComponentClusterStatusState, () => ({
        ...initialComponentClusterStatusState
    }))
);
