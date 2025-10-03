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
import { MatTabsModule } from '@angular/material/tabs';
import { RouterModule } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { provideMockStore } from '@ngrx/store/testing';
import { Summary } from './summary.component';
import { initialState } from '../state/summary-listing/summary-listing.reducer';
import { MockComponent } from 'ng-mocks';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { summaryFeatureKey } from '../state';
import { summaryListingFeatureKey } from '../state/summary-listing';
import { BannerText } from '../../../ui/common/banner-text/banner-text.component';
import { initialState as initialClusterSummaryState } from '../../../state/cluster-summary/cluster-summary.reducer';
import { clusterSummaryFeatureKey } from '../../../state/cluster-summary';

describe('Summary', () => {
    let component: Summary;
    let fixture: ComponentFixture<Summary>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [Summary],
            imports: [
                MatTabsModule,
                RouterModule,
                RouterTestingModule,
                MockComponent(BannerText),
                MockComponent(Navigation)
            ],
            providers: [
                provideMockStore({
                    initialState: {
                        [clusterSummaryFeatureKey]: initialClusterSummaryState,
                        [summaryFeatureKey]: {
                            [summaryListingFeatureKey]: initialState
                        }
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(Summary);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
