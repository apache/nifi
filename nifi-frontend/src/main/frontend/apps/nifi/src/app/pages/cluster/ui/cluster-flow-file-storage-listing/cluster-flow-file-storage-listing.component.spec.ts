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

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ClusterFlowFileStorageListing } from './cluster-flow-file-storage-listing.component';
import { clusterListingFeatureKey } from '../../state/cluster-listing';
import { initialClusterState } from '../../state/cluster-listing/cluster-listing.reducer';
import { clusterFeatureKey } from '../../state';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import {
    selectClusterListing,
    selectClusterListingLoadedTimestamp
} from '../../state/cluster-listing/cluster-listing.selectors';
import { selectSystemDiagnosticsLoadedTimestamp } from '../../../../state/system-diagnostics/system-diagnostics.selectors';
import { initialState as initialErrorState } from '../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../state/current-user';
import { initialSystemDiagnosticsState } from '../../../../state/system-diagnostics/system-diagnostics.reducer';
import { systemDiagnosticsFeatureKey } from '../../../../state/system-diagnostics';

describe('ClusterFlowFileStorageListing', () => {
    let component: ClusterFlowFileStorageListing;
    let fixture: ComponentFixture<ClusterFlowFileStorageListing>;
    let store: MockStore;

    beforeEach(async () => {
        const initialState = {
            [errorFeatureKey]: initialErrorState,
            [currentUserFeatureKey]: initialCurrentUserState,
            [clusterFeatureKey]: {
                [clusterListingFeatureKey]: initialClusterState
            },
            [systemDiagnosticsFeatureKey]: initialSystemDiagnosticsState
        };
        await TestBed.configureTestingModule({
            imports: [ClusterFlowFileStorageListing],
            providers: [
                provideMockStore({
                    initialState,
                    selectors: [
                        {
                            selector: selectClusterListing,
                            value: initialState[clusterFeatureKey][clusterListingFeatureKey]
                        }
                    ]
                })
            ]
        }).compileComponents();

        store = TestBed.inject(MockStore);
        fixture = TestBed.createComponent(ClusterFlowFileStorageListing);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('isInitialLoading', () => {
        it('should return true when both timestamps are empty (initial state)', () => {
            expect(component.isInitialLoading()).toBe(true);
        });

        it('should return true when cluster timestamp is empty but system diagnostics has loaded', () => {
            store.overrideSelector(selectClusterListingLoadedTimestamp, '');
            store.overrideSelector(selectSystemDiagnosticsLoadedTimestamp, '10:30:00 UTC');
            store.refreshState();
            fixture.detectChanges();

            expect(component.isInitialLoading()).toBe(true);
        });

        it('should return true when system diagnostics timestamp is empty but cluster has loaded', () => {
            store.overrideSelector(selectClusterListingLoadedTimestamp, '10:30:00 UTC');
            store.overrideSelector(selectSystemDiagnosticsLoadedTimestamp, '');
            store.refreshState();
            fixture.detectChanges();

            expect(component.isInitialLoading()).toBe(true);
        });

        it('should return false when both timestamps are populated', () => {
            store.overrideSelector(selectClusterListingLoadedTimestamp, '10:30:00 UTC');
            store.overrideSelector(selectSystemDiagnosticsLoadedTimestamp, '10:30:05 UTC');
            store.refreshState();
            fixture.detectChanges();

            expect(component.isInitialLoading()).toBe(false);
        });

        it('should return false when data has been loaded and user triggers a refresh', () => {
            store.overrideSelector(selectClusterListingLoadedTimestamp, '10:30:00 UTC');
            store.overrideSelector(selectSystemDiagnosticsLoadedTimestamp, '10:30:05 UTC');
            store.refreshState();
            fixture.detectChanges();

            expect(component.isInitialLoading()).toBe(false);
        });
    });
});
