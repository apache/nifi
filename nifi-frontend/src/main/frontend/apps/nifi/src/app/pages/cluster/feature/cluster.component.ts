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
import {
    loadClusterListing,
    navigateHome,
    navigateToClusterNodeListing,
    resetClusterState
} from '../state/cluster-listing/cluster-listing.actions';
import {
    selectClusterListingLoadedTimestamp,
    selectClusterListingStatus
} from '../state/cluster-listing/cluster-listing.selectors';
import { selectCurrentUser } from '../../../state/current-user/current-user.selectors';
import { CurrentUser } from '../../../state/current-user';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { selectCurrentRoute } from '@nifi/shared';
import { resetSystemDiagnostics } from '../../../state/system-diagnostics/system-diagnostics.actions';
import { ErrorContextKey } from '../../../state/error';

interface TabLink {
    label: string;
    link: string;
    restricted: boolean;
}

@Component({
    selector: 'cluster',
    templateUrl: './cluster.component.html',
    styleUrls: ['./cluster.component.scss'],
    standalone: false
})
export class Cluster implements OnInit, OnDestroy {
    private _currentUser!: CurrentUser;
    private _tabLinks: TabLink[] = [
        { label: 'Nodes', link: 'nodes', restricted: false },
        { label: 'System', link: 'system', restricted: true },
        { label: 'JVM', link: 'jvm', restricted: true },
        { label: 'FlowFile Storage', link: 'flowfile-storage', restricted: true },
        { label: 'Content Storage', link: 'content-storage', restricted: true },
        { label: 'Provenance Storage', link: 'provenance-storage', restricted: true },
        { label: 'Versions', link: 'versions', restricted: true }
    ];

    listingStatus = this.store.selectSignal(selectClusterListingStatus);
    loadedTimestamp = this.store.selectSignal(selectClusterListingLoadedTimestamp);
    currentUser$ = this.store.select(selectCurrentUser);
    currentRoute = this.store.selectSignal(selectCurrentRoute);

    private _userHasSystemReadAccess: boolean | null = null;

    constructor(private store: Store<NiFiState>) {
        this.currentUser$.pipe(takeUntilDestroyed()).subscribe((currentUser) => {
            this._currentUser = currentUser;
            if (!currentUser.controllerPermissions.canRead) {
                this.store.dispatch(navigateHome());
            } else {
                this.evaluateCurrentTabForPermissions();
            }
        });
    }

    ngOnInit(): void {
        this.store.dispatch(loadClusterListing());
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetClusterState());
        this.store.dispatch(resetSystemDiagnostics());
    }

    refresh() {
        this.store.dispatch(loadClusterListing());
    }

    getTabLinks() {
        const canRead = this._userHasSystemReadAccess;
        return this._tabLinks.filter((tabLink) => !tabLink.restricted || (tabLink.restricted && canRead));
    }

    private evaluateCurrentTabForPermissions() {
        if (this._userHasSystemReadAccess !== null) {
            // If the user is on a tab that requires system read permissions, but they have lost said permission while
            // on that tab, route them to the nodes tab
            if (!this._currentUser.systemPermissions.canRead && this._userHasSystemReadAccess) {
                const link = this.getActiveTabLink();
                if (!link || link.restricted) {
                    this.store.dispatch(navigateToClusterNodeListing());
                    this.store.dispatch(resetSystemDiagnostics());
                }
            } else if (this._currentUser.systemPermissions.canRead && !this._userHasSystemReadAccess) {
                // the user has gained permission to see the system info. reload the data to make system info available.
                this.refresh();
            }
        }
        this._userHasSystemReadAccess = this._currentUser.systemPermissions.canRead;
    }

    private getActiveTabLink(): TabLink | undefined {
        const route = this.currentRoute();
        const path = route.routeConfig.path;
        return this._tabLinks.find((tabLink) => tabLink.link === path);
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
