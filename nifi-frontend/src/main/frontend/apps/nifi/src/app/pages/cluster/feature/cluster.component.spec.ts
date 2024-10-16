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
import { RouterTestingModule } from '@angular/router/testing';
import { provideMockStore } from '@ngrx/store/testing';
import { Cluster } from './cluster.component';
import { initialClusterState } from '../state/cluster-listing/cluster-listing.reducer';
import { ClusterNodeListing } from '../ui/cluster-node-listing/cluster-node-listing.component';
import { MatTabsModule } from '@angular/material/tabs';
import { selectClusterListing } from '../state/cluster-listing/cluster-listing.selectors';
import { clusterListingFeatureKey } from '../state/cluster-listing';
import { ClusterState } from '../state';
import { MockComponent } from 'ng-mocks';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { BannerText } from '../../../ui/common/banner-text/banner-text.component';
import { ContextErrorBanner } from '../../../ui/common/context-error-banner/context-error-banner.component';

describe('Cluster', () => {
    let component: Cluster;
    let fixture: ComponentFixture<Cluster>;

    beforeEach(() => {
        const initialState: ClusterState = {
            [clusterListingFeatureKey]: initialClusterState
        };
        TestBed.configureTestingModule({
            declarations: [Cluster],
            imports: [
                ClusterNodeListing,
                MatTabsModule,
                RouterTestingModule,
                MockComponent(BannerText),
                MockComponent(Navigation),
                MockComponent(ContextErrorBanner)
            ],
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
        });

        fixture = TestBed.createComponent(Cluster);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
