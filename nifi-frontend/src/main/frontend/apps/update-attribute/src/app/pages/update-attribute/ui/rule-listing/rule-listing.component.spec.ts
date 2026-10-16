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

    const createRule = (id: string, name: string): Rule =>
        ({
            id,
            name,
            comments: '',
            conditions: [],
            actions: []
        }) as Rule;

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
    });

    it('should create', () => {
        fixture.detectChanges();
        expect(component).toBeTruthy();
    });

    it('should display the number of filtered rules and total rules', () => {
        component.rules = [createRule('1', 'Rule One'), createRule('2', 'Rule Two')];

        fixture.detectChanges();

        expect(fixture.nativeElement.textContent).toContain('Displaying 2 of 2');
    });

    it('should display one matching rule out of four when filtered', () => {
        component.rules = [
            createRule('1', 'Apple'),
            createRule('2', 'Apricot'),
            createRule('3', 'Banana'),
            createRule('4', 'Cherry')
        ];

        component.searchForm.get('searchRules')?.setValue('Ba');
        component.filterRules();

        fixture.detectChanges();

        expect(fixture.nativeElement.textContent).toContain('Displaying 1 of 4');
    });

    it('should display two matching rules out of four when filtered', () => {
        component.rules = [
            createRule('1', 'Apple'),
            createRule('2', 'Apricot'),
            createRule('3', 'Banana'),
            createRule('4', 'Cherry')
        ];

        component.searchForm.get('searchRules')?.setValue('Ap');
        component.filterRules();

        fixture.detectChanges();

        expect(fixture.nativeElement.textContent).toContain('Displaying 2 of 4');
    });

    it('should display zero matching rules when no rules match the search', () => {
        component.rules = [
            createRule('1', 'Apple'),
            createRule('2', 'Apricot'),
            createRule('3', 'Banana'),
            createRule('4', 'Cherry')
        ];

        component.searchForm.get('searchRules')?.setValue('Plum');
        component.filterRules();

        fixture.detectChanges();

        expect(fixture.nativeElement.textContent).toContain('Displaying 0 of 4');
        expect(fixture.nativeElement.textContent).not.toContain('NO Matches');
    });

    it('should display nothing when there are no rules', () => {
        component.rules = [];

        fixture.detectChanges();

        // The empty-list template may not display the count at all.
        expect(fixture.nativeElement.textContent).not.toContain('Displaying');
    });
});
