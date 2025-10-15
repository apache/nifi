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

import { ClusterProvenanceStorageListing } from './cluster-provenance-storage-listing.component';
import { clusterListingFeatureKey } from '../../state/cluster-listing';
import { initialClusterState } from '../../state/cluster-listing/cluster-listing.reducer';
import { clusterFeatureKey } from '../../state';
import { provideMockStore } from '@ngrx/store/testing';
import { selectClusterListing } from '../../state/cluster-listing/cluster-listing.selectors';
import { initialState as initialErrorState } from '../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../state/current-user';
import { initialSystemDiagnosticsState } from '../../../../state/system-diagnostics/system-diagnostics.reducer';
import { systemDiagnosticsFeatureKey } from '../../../../state/system-diagnostics';

describe('ClusterProvenanceStorageListing', () => {
    let component: ClusterProvenanceStorageListing;
    let fixture: ComponentFixture<ClusterProvenanceStorageListing>;

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
            imports: [ClusterProvenanceStorageListing],
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

        fixture = TestBed.createComponent(ClusterProvenanceStorageListing);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('isInitialLoading', () => {
        it('should return true when both timestamps are empty (initial state)', () => {
            const result = component.isInitialLoading(
                initialClusterState.loadedTimestamp,
                initialSystemDiagnosticsState.loadedTimestamp
            );
            expect(result).toBe(true);
        });

        it('should return true when cluster timestamp is empty but system diagnostics has loaded', () => {
            const result = component.isInitialLoading(initialClusterState.loadedTimestamp, '10:30:00 UTC');
            expect(result).toBe(true);
        });

        it('should return true when system diagnostics timestamp is empty but cluster has loaded', () => {
            const result = component.isInitialLoading('10:30:00 UTC', initialSystemDiagnosticsState.loadedTimestamp);
            expect(result).toBe(true);
        });

        it('should return false when both timestamps are populated', () => {
            const result = component.isInitialLoading('10:30:00 UTC', '10:30:05 UTC');
            expect(result).toBe(false);
        });
    });
});
