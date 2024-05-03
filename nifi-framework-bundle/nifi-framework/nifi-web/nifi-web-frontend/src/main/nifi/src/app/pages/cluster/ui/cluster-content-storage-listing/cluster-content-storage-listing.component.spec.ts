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

import { ClusterContentStorageListing } from './cluster-content-storage-listing.component';
import { ClusterState } from '../../state';
import { clusterListingFeatureKey } from '../../state/cluster-listing';
import { initialClusterState } from '../../state/cluster-listing/cluster-listing.reducer';
import { provideMockStore } from '@ngrx/store/testing';
import { selectClusterListing } from '../../state/cluster-listing/cluster-listing.selectors';

describe('ClusterContentStorageListing', () => {
    let component: ClusterContentStorageListing;
    let fixture: ComponentFixture<ClusterContentStorageListing>;

    beforeEach(async () => {
        const initialState: ClusterState = {
            [clusterListingFeatureKey]: initialClusterState
        };
        await TestBed.configureTestingModule({
            imports: [ClusterContentStorageListing],
            providers: [
                provideMockStore({
                    initialState,
                    selectors: [
                        {
                            selector: selectClusterListing,
                            value: initialState[clusterListingFeatureKey]
                        }
                    ]
                })
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(ClusterContentStorageListing);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
