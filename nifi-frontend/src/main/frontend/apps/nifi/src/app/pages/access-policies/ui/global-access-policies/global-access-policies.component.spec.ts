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

import { GlobalAccessPolicies } from './global-access-policies.component';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../state/access-policy/access-policy.reducer';
import { initialState as initialErrorState } from '../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../state/error';
import { accessPoliciesFeatureKey } from '../../state';
import { accessPolicyFeatureKey } from '../../state/access-policy';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';

describe('GlobalAccessPolicies', () => {
    let component: GlobalAccessPolicies;
    let fixture: ComponentFixture<GlobalAccessPolicies>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [GlobalAccessPolicies],
            imports: [NgxSkeletonLoaderComponent],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [accessPoliciesFeatureKey]: {
                            [accessPolicyFeatureKey]: initialState
                        }
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(GlobalAccessPolicies);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
