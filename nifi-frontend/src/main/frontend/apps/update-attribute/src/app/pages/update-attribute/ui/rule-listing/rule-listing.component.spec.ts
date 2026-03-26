/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RuleListing } from './rule-listing.component';
import { provideMockStore } from '@ngrx/store/testing';
import { MatSlideToggleChange } from '@angular/material/slide-toggle';
import { initialState as rulesInitialState } from '../../state/rules/rules.reducer';
import { initialState as evaluationContextInitialState } from '../../state/evaluation-context/evaluation-context.reducer';
import { initialState as advancedUiParametersInitialState } from '../../state/advanced-ui-parameters/advanced-ui-parameters.reducer';
import { updateAttributeFeatureKey } from '../../state';
import { advancedUiParametersFeatureKey } from '../../state/advanced-ui-parameters';
import { evaluationContextFeatureKey } from '../../state/evaluation-context';
import { rulesFeatureKey } from '../../state/rules';

describe('RuleListing', () => {
    let component: RuleListing;
    let fixture: ComponentFixture<RuleListing>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [RuleListing],
            providers: [
                provideMockStore({
                    initialState: {
                        [updateAttributeFeatureKey]: {
                            [rulesFeatureKey]: rulesInitialState,
                            [evaluationContextFeatureKey]: evaluationContextInitialState,
                            [advancedUiParametersFeatureKey]: advancedUiParametersInitialState
                        }
                    }
                })
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(RuleListing);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should disable rule reordering when switching to clone policy', () => {
        component.setAllowRuleReordering({ checked: true } as MatSlideToggleChange);

        component.flowFilePolicyChanged('USE_CLONE');

        expect(component.allowRuleReordering).toBeFalse();
    });

    it('should disable rule reordering when clone policy is loaded', () => {
        component.setAllowRuleReordering({ checked: true } as MatSlideToggleChange);

        component.evaluationContext = {
            ruleOrder: [],
            flowFilePolicy: 'USE_CLONE'
        };

        expect(component.allowRuleReordering).toBeFalse();
    });
});
