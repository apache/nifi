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
import { AsyncPipe } from '@angular/common';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { RepositoryStorageTable } from '../common/repository-storage-table/repository-storage-table.component';
import {
    selectClusterListingLoadedTimestamp,
    selectClusterListingStatus,
    selectClusterNodeIdFromRoute,
    selectClusterStorageRepositoryIdFromRoute
} from '../../state/cluster-listing/cluster-listing.selectors';
import { selectSystemNodeSnapshots } from '../../../../state/system-diagnostics/system-diagnostics.selectors';
import { isDefinedAndNotNull } from '@nifi/shared';
import { map } from 'rxjs';
import { ClusterNodeRepositoryStorageUsage } from '../../../../state/system-diagnostics';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { initialClusterState } from '../../state/cluster-listing/cluster-listing.reducer';
import {
    clearContentStorageNodeSelection,
    selectContentStorageNode
} from '../../state/cluster-listing/cluster-listing.actions';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
    selector: 'cluster-content-storage-listing',
    imports: [AsyncPipe, NgxSkeletonLoaderModule, RepositoryStorageTable],
    templateUrl: './cluster-content-storage-listing.component.html',
    styleUrl: './cluster-content-storage-listing.component.scss'
})
export class ClusterContentStorageListing {
    loadedTimestamp = this.store.selectSignal(selectClusterListingLoadedTimestamp);
    listingStatus = this.store.selectSignal(selectClusterListingStatus);
    selectedClusterNodeId = this.store.selectSignal(selectClusterNodeIdFromRoute);
    selectedClusterRepoId = this.store.selectSignal(selectClusterStorageRepositoryIdFromRoute);
    components$ = this.store.select(selectSystemNodeSnapshots).pipe(
        takeUntilDestroyed(),
        isDefinedAndNotNull(),
        map((clusterNodes) => {
            const expanded: ClusterNodeRepositoryStorageUsage[] = [];
            return clusterNodes.reduce((acc, node) => {
                const repos = node.snapshot.contentRepositoryStorageUsage.map((storage) => {
                    return {
                        address: node.address,
                        apiPort: node.apiPort,
                        nodeId: node.nodeId,
                        repositoryStorageUsage: storage
                    };
                });
                return [...acc, ...repos];
            }, expanded);
        })
    );

    constructor(private store: Store<NiFiState>) {}

    isInitialLoading(loadedTimestamp: string): boolean {
        return loadedTimestamp == initialClusterState.loadedTimestamp;
    }

    selectStorageNode(node: ClusterNodeRepositoryStorageUsage): void {
        this.store.dispatch(
            selectContentStorageNode({
                request: {
                    id: node.nodeId,
                    repository: node.repositoryStorageUsage.identifier
                }
            })
        );
    }

    clearSelection(): void {
        this.store.dispatch(clearContentStorageNodeSelection());
    }
}
