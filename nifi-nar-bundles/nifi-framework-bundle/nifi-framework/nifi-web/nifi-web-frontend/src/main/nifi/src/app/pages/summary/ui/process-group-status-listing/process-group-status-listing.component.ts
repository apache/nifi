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
import { initialState } from '../../state/summary-listing/summary-listing.reducer';
import * as SummaryListingActions from '../../state/summary-listing/summary-listing.actions';
import { ProcessGroupStatusSnapshotEntity, SummaryListingState } from '../../state/summary-listing';
import { Store } from '@ngrx/store';
import {
    selectProcessGroupIdFromRoute,
    selectProcessGroupStatus,
    selectProcessGroupStatusItem,
    selectProcessGroupStatusSnapshots,
    selectSummaryListingLoadedTimestamp,
    selectSummaryListingStatus,
    selectViewStatusHistory
} from '../../state/summary-listing/summary-listing.selectors';
import { filter, switchMap, take } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { getStatusHistoryAndOpenDialog } from '../../../../state/status-history/status-history.actions';
import { ComponentType } from '../../../../state/shared';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';

@Component({
    selector: 'process-group-status-listing',
    templateUrl: './process-group-status-listing.component.html',
    styleUrls: ['./process-group-status-listing.component.scss']
})
export class ProcessGroupStatusListing {
    processGroupStatusSnapshots$ = this.store.select(selectProcessGroupStatusSnapshots);
    loadedTimestamp$ = this.store.select(selectSummaryListingLoadedTimestamp);
    summaryListingStatus$ = this.store.select(selectSummaryListingStatus);
    currentUser$ = this.store.select(selectCurrentUser);
    selectedProcessGroupId$ = this.store.select(selectProcessGroupIdFromRoute);
    processGroupStatus$ = this.store.select(selectProcessGroupStatus);

    constructor(private store: Store<SummaryListingState>) {
        this.store
            .select(selectViewStatusHistory)
            .pipe(
                filter((id: string) => !!id),
                switchMap((id: string) =>
                    this.store.select(selectProcessGroupStatusItem(id)).pipe(
                        filter((pg) => !!pg),
                        take(1)
                    )
                ),
                takeUntilDestroyed()
            )
            .subscribe((pg) => {
                if (pg) {
                    this.store.dispatch(
                        getStatusHistoryAndOpenDialog({
                            request: {
                                source: 'summary',
                                componentType: ComponentType.ProcessGroup,
                                componentId: pg.id
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

    viewStatusHistory(pg: ProcessGroupStatusSnapshotEntity): void {
        this.store.dispatch(
            SummaryListingActions.navigateToViewProcessGroupStatusHistory({
                id: pg.id
            })
        );
    }

    selectProcessGroup(pg: ProcessGroupStatusSnapshotEntity): void {
        this.store.dispatch(
            SummaryListingActions.selectProcessGroupStatus({
                request: {
                    id: pg.id
                }
            })
        );
    }

    clearSelection() {
        this.store.dispatch(SummaryListingActions.clearProcessGroupStatusSelection());
    }
}
