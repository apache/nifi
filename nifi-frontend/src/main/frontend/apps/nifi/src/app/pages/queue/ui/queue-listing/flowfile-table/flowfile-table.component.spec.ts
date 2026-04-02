/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { FlowFileTable } from './flowfile-table.component';
import { MatTableModule } from '@angular/material/table';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MockComponent } from 'ng-mocks';
import { ContextErrorBanner } from '../../../../../ui/common/context-error-banner/context-error-banner.component';
import { provideMockStore } from '@ngrx/store/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { errorFeatureKey } from '../../../../../state/error';
import { initialState as errorInitialState } from '../../../../../state/error/error.reducer';
import { clusterSummaryFeatureKey } from '../../../../../state/cluster-summary';
import { initialState as clusterSummaryInitialState } from '../../../../../state/cluster-summary/cluster-summary.reducer';

describe('FlowFileTable', () => {
    let component: FlowFileTable;
    let fixture: ComponentFixture<FlowFileTable>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [
                FlowFileTable,
                MockComponent(ContextErrorBanner),
                MatTableModule,
                NoopAnimationsModule,
                RouterTestingModule
            ],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: errorInitialState,
                        [clusterSummaryFeatureKey]: clusterSummaryInitialState
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(FlowFileTable);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
