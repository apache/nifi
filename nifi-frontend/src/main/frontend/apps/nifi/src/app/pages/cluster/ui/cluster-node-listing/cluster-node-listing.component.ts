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

import { Component } from '@angular/core';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { initialClusterState } from '../../state/cluster-listing/cluster-listing.reducer';
import {
    selectClusterListingLoadedTimestamp,
    selectClusterListingNodes,
    selectClusterListingStatus,
    selectClusterNodeIdFromRoute
} from '../../state/cluster-listing/cluster-listing.selectors';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { ClusterNodeTable } from './cluster-node-table/cluster-node-table.component';
import { ClusterNode } from '../../state/cluster-listing';
import {
    clearClusterNodeSelection,
    confirmAndConnectNode,
    confirmAndDisconnectNode,
    confirmAndOffloadNode,
    confirmAndRemoveNode,
    selectClusterNode,
    showClusterNodeDetails
} from '../../state/cluster-listing/cluster-listing.actions';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';

@Component({
    selector: 'cluster-node-listing',
    imports: [NgxSkeletonLoaderModule, ClusterNodeTable],
    templateUrl: './cluster-node-listing.component.html',
    styleUrl: './cluster-node-listing.component.scss'
})
export class ClusterNodeListing {
    loadedTimestamp = this.store.selectSignal(selectClusterListingLoadedTimestamp);
    listingStatus = this.store.selectSignal(selectClusterListingStatus);
    nodes = this.store.selectSignal(selectClusterListingNodes);
    selectedClusterNodeId = this.store.selectSignal(selectClusterNodeIdFromRoute);
    currentUser = this.store.selectSignal(selectCurrentUser);

    constructor(private store: Store<NiFiState>) {}

    isInitialLoading(loadedTimestamp: string): boolean {
        return loadedTimestamp == initialClusterState.loadedTimestamp;
    }

    disconnectNode(node: ClusterNode): void {
        this.store.dispatch(confirmAndDisconnectNode({ request: node }));
    }

    connectNode(node: ClusterNode): void {
        this.store.dispatch(confirmAndConnectNode({ request: node }));
    }

    removeNode(node: ClusterNode): void {
        this.store.dispatch(confirmAndRemoveNode({ request: node }));
    }

    offloadNode(node: ClusterNode): void {
        this.store.dispatch(confirmAndOffloadNode({ request: node }));
    }

    showDetail(node: ClusterNode): void {
        this.store.dispatch(showClusterNodeDetails({ request: node }));
    }

    selectNode(node: ClusterNode): void {
        this.store.dispatch(selectClusterNode({ request: { id: node.nodeId } }));
    }

    clearSelection(): void {
        this.store.dispatch(clearClusterNodeSelection());
    }
}
