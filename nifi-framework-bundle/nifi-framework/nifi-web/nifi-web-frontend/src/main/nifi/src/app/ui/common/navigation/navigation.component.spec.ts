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

import { Navigation } from './navigation.component';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../state/current-user/current-user.reducer';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { selectCurrentUser } from '../../../state/current-user/current-user.selectors';
import * as fromUser from '../../../state/current-user/current-user.reducer';
import { selectClusterSummary } from '../../../state/cluster-summary/cluster-summary.selectors';
import * as fromClusterSummary from '../../../state/cluster-summary/cluster-summary.reducer';
import { selectFlowConfiguration } from '../../../state/flow-configuration/flow-configuration.selectors';
import * as fromFlowConfiguration from '../../../state/flow-configuration/flow-configuration.reducer';

describe('Navigation', () => {
    let component: Navigation;
    let fixture: ComponentFixture<Navigation>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [Navigation, HttpClientTestingModule, RouterTestingModule],
            providers: [
                provideMockStore({
                    initialState,
                    selectors: [
                        {
                            selector: selectCurrentUser,
                            value: fromUser.initialState.user
                        },
                        {
                            selector: selectClusterSummary,
                            value: fromClusterSummary.initialState
                        },
                        {
                            selector: selectFlowConfiguration,
                            value: fromFlowConfiguration.initialState.flowConfiguration
                        }
                    ]
                })
            ]
        });
        fixture = TestBed.createComponent(Navigation);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
