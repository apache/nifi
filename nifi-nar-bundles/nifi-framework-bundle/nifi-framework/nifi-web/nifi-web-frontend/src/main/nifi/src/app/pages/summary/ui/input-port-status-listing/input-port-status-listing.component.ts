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
import {
    selectInputPortIdFromRoute,
    selectInputPortStatusSnapshots,
    selectSummaryListingLoadedTimestamp,
    selectSummaryListingStatus
} from '../../state/summary-listing/summary-listing.selectors';
import { selectUser } from '../../../../state/user/user.selectors';
import { PortStatusSnapshotEntity, SummaryListingState } from '../../state/summary-listing';
import { Store } from '@ngrx/store';
import { initialState } from '../../state/summary-listing/summary-listing.reducer';
import * as SummaryListingActions from '../../state/summary-listing/summary-listing.actions';
import { getSystemDiagnosticsAndOpenDialog } from '../../../../state/system-diagnostics/system-diagnostics.actions';

@Component({
    selector: 'input-port-status-listing',
    templateUrl: './input-port-status-listing.component.html',
    styleUrls: ['./input-port-status-listing.component.scss']
})
export class InputPortStatusListing {
    portStatusSnapshots$ = this.store.select(selectInputPortStatusSnapshots);
    loadedTimestamp$ = this.store.select(selectSummaryListingLoadedTimestamp);
    summaryListingStatus$ = this.store.select(selectSummaryListingStatus);
    currentUser$ = this.store.select(selectUser);
    selectedPortId$ = this.store.select(selectInputPortIdFromRoute);

    constructor(private store: Store<SummaryListingState>) {}

    isInitialLoading(loadedTimestamp: string): boolean {
        return loadedTimestamp == initialState.loadedTimestamp;
    }

    refreshSummaryListing() {
        this.store.dispatch(SummaryListingActions.loadSummaryListing({ recursive: true }));
    }

    selectPort(port: PortStatusSnapshotEntity): void {
        this.store.dispatch(
            SummaryListingActions.selectInputPortStatus({
                request: {
                    id: port.id
                }
            })
        );
    }

    openSystemDiagnostics() {
        this.store.dispatch(
            getSystemDiagnosticsAndOpenDialog({
                request: {
                    nodewise: false
                }
            })
        );
    }
}
