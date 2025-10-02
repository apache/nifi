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

import { FlowConfigurationHistoryListing } from './flow-configuration-history-listing.component';
import { provideMockStore } from '@ngrx/store/testing';
import { initialHistoryState } from '../../state/flow-configuration-history-listing/flow-configuration-history-listing.reducer';
import { flowConfigurationHistoryListingFeatureKey } from '../../state/flow-configuration-history-listing';
import { flowConfigurationHistoryFeatureKey } from '../../state';
import { initialState as initialErrorState } from '../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../state/current-user';

describe('FlowConfigurationHistoryListing', () => {
    let component: FlowConfigurationHistoryListing;
    let fixture: ComponentFixture<FlowConfigurationHistoryListing>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [FlowConfigurationHistoryListing],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [flowConfigurationHistoryFeatureKey]: {
                            [flowConfigurationHistoryListingFeatureKey]: initialHistoryState
                        }
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(FlowConfigurationHistoryListing);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
