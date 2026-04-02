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

import { ParameterProviderDefinition } from './parameter-provider-definition.component';
import { provideMockStore } from '@ngrx/store/testing';
import { documentationFeatureKey } from '../../state';
import { parameterProviderDefinitionFeatureKey } from '../../state/parameter-provider-definition';
import { initialState } from '../../state/parameter-provider-definition/parameter-provider-definition.reducer';
import { initialState as initialErrorState } from '../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../state/current-user';
import { flowConfigurationFeatureKey } from '../../../../state/flow-configuration';
import * as fromFlowConfiguration from '../../../../state/flow-configuration/flow-configuration.reducer';
import { selectCurrentRoute } from '@nifi/shared';

describe('ParameterProviderDefinition', () => {
    let component: ParameterProviderDefinition;
    let fixture: ComponentFixture<ParameterProviderDefinition>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [ParameterProviderDefinition],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [flowConfigurationFeatureKey]: fromFlowConfiguration.initialState,
                        [documentationFeatureKey]: {
                            [parameterProviderDefinitionFeatureKey]: initialState
                        }
                    },
                    selectors: [
                        {
                            selector: selectCurrentRoute,
                            value: {
                                params: {
                                    group: 'g',
                                    artifact: 'a',
                                    version: '1',
                                    type: 't'
                                },
                                routeConfig: { path: 'ParameterProvider' },
                                url: [{ path: 'ParameterProvider' }]
                            }
                        }
                    ]
                })
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(ParameterProviderDefinition);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
