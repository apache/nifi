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

import { Component, OnDestroy, OnInit } from '@angular/core';
import { Store } from '@ngrx/store';
import { startCurrentUserPolling, stopCurrentUserPolling } from '../../../state/current-user/current-user.actions';
import { NiFiState } from '../../../state';
import { loadClusterListing } from '../state/cluster-listing/cluster-listing.actions';
import {
    selectClusterListingLoadedTimestamp,
    selectClusterListingStatus
} from '../state/cluster-listing/cluster-listing.selectors';

interface TabLink {
    label: string;
    link: string;
}

@Component({
    selector: 'cluster',
    templateUrl: './cluster.component.html',
    styleUrls: ['./cluster.component.scss']
})
export class Cluster implements OnInit, OnDestroy {
    tabLinks: TabLink[] = [
        { label: 'Nodes', link: 'nodes' },
        { label: 'System', link: 'system' },
        { label: 'JVM', link: 'jvm' },
        { label: 'FlowFile Storage', link: 'flowfile-storage' },
        { label: 'Content Storage', link: 'content-storage' },
        { label: 'Provenance Storage', link: 'provenance-storage' },
        { label: 'Versions', link: 'versions' }
    ];

    listingStatus = this.store.selectSignal(selectClusterListingStatus);
    loadedTimestamp = this.store.selectSignal(selectClusterListingLoadedTimestamp);

    constructor(private store: Store<NiFiState>) {}

    ngOnInit(): void {
        this.store.dispatch(startCurrentUserPolling());
        this.store.dispatch(loadClusterListing());
    }

    ngOnDestroy(): void {
        this.store.dispatch(stopCurrentUserPolling());
    }

    refresh() {
        this.store.dispatch(loadClusterListing());
    }
}
