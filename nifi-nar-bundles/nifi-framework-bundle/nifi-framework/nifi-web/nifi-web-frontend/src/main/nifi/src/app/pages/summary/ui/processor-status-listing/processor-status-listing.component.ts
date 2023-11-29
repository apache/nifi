/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { Component } from '@angular/core';
import { Store } from '@ngrx/store';
import {
    selectClusterSummary,
    selectProcessorIdFromRoute,
    selectProcessorStatus,
    selectProcessorStatusSnapshots,
    selectSummaryListingLoadedTimestamp,
    selectSummaryListingStatus,
    selectViewStatusHistory
} from '../../state/summary-listing/summary-listing.selectors';
import { ProcessorStatusSnapshotEntity, SummaryListingState } from '../../state/summary-listing';
import { selectUser } from '../../../../state/user/user.selectors';
import { initialState } from '../../state/summary-listing/summary-listing.reducer';
import { Router } from '@angular/router';
import { openStatusHistoryDialog } from '../../../../state/status-history/status-history.actions';
import { ComponentType } from '../../../../state/shared';
import { filter, switchMap, take } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import * as SummaryListingActions from '../../state/summary-listing/summary-listing.actions';

@Component({
    selector: 'processor-status-listing',
    templateUrl: './processor-status-listing.component.html',
    styleUrls: ['./processor-status-listing.component.scss']
})
export class ProcessorStatusListing {
    clusterSummary$ = this.store.select(selectClusterSummary);
    processorStatusSnapshots$ = this.store.select(selectProcessorStatusSnapshots);
    loadedTimestamp$ = this.store.select(selectSummaryListingLoadedTimestamp);
    summaryListingStatus$ = this.store.select(selectSummaryListingStatus);
    selectedProcessorId$ = this.store.select(selectProcessorIdFromRoute);

    currentUser$ = this.store.select(selectUser);

    constructor(
        private store: Store<SummaryListingState>,
        private router: Router
    ) {
        this.store
            .select(selectViewStatusHistory)
            .pipe(
                filter((id: string) => !!id),
                switchMap((id: string) =>
                    this.store.select(selectProcessorStatus(id)).pipe(
                        filter((processor) => !!processor),
                        take(1)
                    )
                ),
                takeUntilDestroyed()
            )
            .subscribe((processor) => {
                if (processor) {
                    this.store.dispatch(
                        openStatusHistoryDialog({
                            request: {
                                source: 'summary',
                                componentType: ComponentType.Processor,
                                componentId: processor.id
                            }
                        })
                    );
                }
            });
    }

    isInitialLoading(loadedTimestamp: string): boolean {
        return loadedTimestamp == initialState.loadedTimestamp;
    }

    refreshSummaryListing() {
        this.store.dispatch(SummaryListingActions.loadSummaryListing({ recursive: true }));
    }

    gotoProcessor(processor: ProcessorStatusSnapshotEntity): void {
        this.router.navigate([
            '/process-groups',
            processor.processorStatusSnapshot.groupId,
            'processors',
            processor.id
        ]);
    }

    viewStatusHistory(processor: ProcessorStatusSnapshotEntity): void {
        this.store.dispatch(
            SummaryListingActions.navigateToViewProcessorStatusHistory({
                id: processor.id
            })
        );
    }

    selectProcessor(processor: ProcessorStatusSnapshotEntity): void {
        this.store.dispatch(
            SummaryListingActions.selectProcessorStatus({
                request: {
                    id: processor.id
                }
            })
        );
    }
}
