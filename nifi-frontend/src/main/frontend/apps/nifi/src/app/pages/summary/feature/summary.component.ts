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
import { NiFiState } from '../../../state';
import { loadSummaryListing, resetSummaryState } from '../state/summary-listing/summary-listing.actions';
import { loadClusterSummary, searchCluster } from '../../../state/cluster-summary/cluster-summary.actions';
import { selectClusterSummary } from '../../../state/cluster-summary/cluster-summary.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { isDefinedAndNotNull } from '@nifi/shared';
import { selectSelectedClusterNode } from '../state/summary-listing/summary-listing.selectors';

interface TabLink {
    label: string;
    link: string;
}

@Component({
    selector: 'summary',
    templateUrl: './summary.component.html',
    styleUrls: ['./summary.component.scss'],
    standalone: false
})
export class Summary implements OnInit, OnDestroy {
    tabLinks: TabLink[] = [
        { label: 'Processors', link: 'processors' },
        { label: 'Input Ports', link: 'input-ports' },
        { label: 'Output Ports', link: 'output-ports' },
        { label: 'Remote Process Groups', link: 'remote-process-groups' },
        { label: 'Connections', link: 'connections' },
        { label: 'Process Groups', link: 'process-groups' }
    ];

    clusterSummary$ = this.store.select(selectClusterSummary);
    selectedClusterNode$ = this.store.select(selectSelectedClusterNode).pipe(isDefinedAndNotNull());

    constructor(private store: Store<NiFiState>) {
        this.clusterSummary$.pipe(takeUntilDestroyed(), isDefinedAndNotNull()).subscribe((clusterSummary) => {
            if (clusterSummary.connectedToCluster) {
                this.store.dispatch(searchCluster({ request: {} }));
            }
        });
        this.selectedClusterNode$.pipe(isDefinedAndNotNull(), takeUntilDestroyed()).subscribe(() => {
            this.store.dispatch(loadSummaryListing({ recursive: true }));
        });
    }

    ngOnInit(): void {
        this.store.dispatch(loadSummaryListing({ recursive: true }));
        this.store.dispatch(loadClusterSummary());
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetSummaryState());
    }
}
