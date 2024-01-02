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
import { ConnectionStatusSnapshotEntity, SummaryListingState } from '../../state/summary-listing';
import { initialState } from '../../state/summary-listing/summary-listing.reducer';
import * as SummaryListingActions from '../../state/summary-listing/summary-listing.actions';
import {
    selectConnectionIdFromRoute,
    selectConnectionStatus,
    selectConnectionStatusSnapshots,
    selectSummaryListingLoadedTimestamp,
    selectSummaryListingStatus,
    selectViewStatusHistory
} from '../../state/summary-listing/summary-listing.selectors';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { filter, switchMap, take } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { getStatusHistoryAndOpenDialog } from '../../../../state/status-history/status-history.actions';
import { ComponentType } from '../../../../state/shared';

@Component({
    selector: 'connection-status-listing',
    templateUrl: './connection-status-listing.component.html',
    styleUrls: ['./connection-status-listing.component.scss']
})
export class ConnectionStatusListing {
    loadedTimestamp$ = this.store.select(selectSummaryListingLoadedTimestamp);
    summaryListingStatus$ = this.store.select(selectSummaryListingStatus);
    currentUser$ = this.store.select(selectCurrentUser);
    connectionStatusSnapshots$ = this.store.select(selectConnectionStatusSnapshots);
    selectedConnectionId$ = this.store.select(selectConnectionIdFromRoute);

    constructor(private store: Store<SummaryListingState>) {
        this.store
            .select(selectViewStatusHistory)
            .pipe(
                filter((id: string) => !!id),
                switchMap((id: string) =>
                    this.store.select(selectConnectionStatus(id)).pipe(
                        filter((connection) => !!connection),
                        take(1)
                    )
                ),
                takeUntilDestroyed()
            )
            .subscribe((connection) => {
                if (connection) {
                    this.store.dispatch(
                        getStatusHistoryAndOpenDialog({
                            request: {
                                source: 'summary',
                                componentType: ComponentType.Connection,
                                componentId: connection.id
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

    selectConnection(connection: ConnectionStatusSnapshotEntity | null): void {
        this.store.dispatch(
            SummaryListingActions.selectConnectionStatus({
                request: {
                    id: connection ? connection.id : null
                }
            })
        );
    }

    viewStatusHistory(connection: ConnectionStatusSnapshotEntity): void {
        this.store.dispatch(
            SummaryListingActions.navigateToViewConnectionStatusHistory({
                id: connection.id
            })
        );
    }
}
