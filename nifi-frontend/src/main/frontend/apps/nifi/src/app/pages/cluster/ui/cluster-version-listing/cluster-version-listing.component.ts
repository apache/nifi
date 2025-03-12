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
import {
    selectClusterListingLoadedTimestamp,
    selectClusterListingStatus,
    selectClusterNodeIdFromRoute
} from '../../state/cluster-listing/cluster-listing.selectors';
import { selectSystemNodeSnapshots } from '../../../../state/system-diagnostics/system-diagnostics.selectors';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { initialClusterState } from '../../state/cluster-listing/cluster-listing.reducer';
import { NodeSnapshot } from '../../../../state/system-diagnostics';
import { clearVersionsNodeSelection, selectVersionNode } from '../../state/cluster-listing/cluster-listing.actions';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { ClusterVersionTable } from './cluster-version-table/cluster-version-table.component';

@Component({
    selector: 'cluster-version-listing',
    imports: [NgxSkeletonLoaderModule, ClusterVersionTable],
    templateUrl: './cluster-version-listing.component.html',
    styleUrl: './cluster-version-listing.component.scss'
})
export class ClusterVersionListing {
    loadedTimestamp = this.store.selectSignal(selectClusterListingLoadedTimestamp);
    listingStatus = this.store.selectSignal(selectClusterListingStatus);
    selectedClusterNodeId = this.store.selectSignal(selectClusterNodeIdFromRoute);
    nodes = this.store.selectSignal(selectSystemNodeSnapshots);

    constructor(private store: Store<NiFiState>) {}

    isInitialLoading(loadedTimestamp: string): boolean {
        return loadedTimestamp == initialClusterState.loadedTimestamp;
    }

    selectNode(node: NodeSnapshot): void {
        this.store.dispatch(selectVersionNode({ request: { id: node.nodeId } }));
    }

    clearSelection(): void {
        this.store.dispatch(clearVersionsNodeSelection());
    }
}
