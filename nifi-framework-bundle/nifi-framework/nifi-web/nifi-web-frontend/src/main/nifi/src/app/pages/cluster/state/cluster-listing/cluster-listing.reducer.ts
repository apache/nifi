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

import { ClusterListingState } from './index';
import { createReducer, on } from '@ngrx/store';
import {
    connectNode,
    disconnectNode,
    loadClusterListing,
    loadClusterListingSuccess,
    removeNode,
    removeNodeSuccess,
    resetClusterState,
    updateNodeSuccess
} from './cluster-listing.actions';
import { produce } from 'immer';

export const initialClusterState: ClusterListingState = {
    nodes: [],
    loadedTimestamp: '',
    status: 'pending',
    saving: false
};

export const clusterListingReducer = createReducer(
    initialClusterState,

    on(loadClusterListing, (state) => ({
        ...state,
        status: 'loading' as const
    })),

    on(resetClusterState, () => ({
        ...initialClusterState
    })),

    on(loadClusterListingSuccess, (state, { response }) => ({
        ...state,
        loadedTimestamp: response.generated,
        nodes: response.nodes,
        status: 'success' as const
    })),

    on(disconnectNode, connectNode, removeNode, (state) => ({
        ...state,
        saving: true
    })),

    on(updateNodeSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const index = draftState.nodes.findIndex((node) => response.node.nodeId === node.nodeId);
            if (index > -1) {
                draftState.nodes[index] = response.node;
            }
            draftState.saving = false;
        });
    }),

    on(removeNodeSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const index = draftState.nodes.findIndex((node) => response.nodeId === node.nodeId);
            if (index > -1) {
                draftState.nodes.splice(index, 1);
            }
            draftState.saving = false;
        });
    })
);
