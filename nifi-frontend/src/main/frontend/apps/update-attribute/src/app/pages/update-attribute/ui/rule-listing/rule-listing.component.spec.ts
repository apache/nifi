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

import { provideZoneChangeDetection } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RuleListing } from './rule-listing.component';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState as rulesInitialState } from '../../state/rules/rules.reducer';
import { initialState as evaluationContextInitialState } from '../../state/evaluation-context/evaluation-context.reducer';
import { initialState as advancedUiParametersInitialState } from '../../state/advanced-ui-parameters/advanced-ui-parameters.reducer';
import { updateAttributeFeatureKey } from '../../state';
import { advancedUiParametersFeatureKey } from '../../state/advanced-ui-parameters';
import { evaluationContextFeatureKey } from '../../state/evaluation-context';
import { Rule, rulesFeatureKey } from '../../state/rules';
import { fakeAsync, tick } from '@angular/core/testing';

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
                // match the change detection strategy used when the app is bootstrapped (see main.ts)
                provideZoneChangeDetection(),
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

    it('should display one matching rule out of four when filtered', fakeAsync(() => {
        component.rules = [
            createRule('1', 'Apple'),
            createRule('2', 'Apricot'),
            createRule('3', 'Banana'),
            createRule('4', 'Cherry')
        ];

        component.searchForm.get('searchRules')?.setValue('Ba');
        tick(500);
        fixture.detectChanges();

        expect(fixture.nativeElement.textContent).toContain('Displaying 1 of 4');
    }));

    it('should display two matching rules out of four when filtered', fakeAsync(() => {
        component.rules = [
            createRule('1', 'Apple'),
            createRule('2', 'Apricot'),
            createRule('3', 'Banana'),
            createRule('4', 'Cherry')
        ];

        component.searchForm.get('searchRules')?.setValue('Ap');
        tick(500);
        fixture.detectChanges();

        expect(fixture.nativeElement.textContent).toContain('Displaying 2 of 4');
    }));

    it('should display zero matching rules when no rules match the search', fakeAsync(() => {
        component.rules = [
            createRule('1', 'Apple'),
            createRule('2', 'Apricot'),
            createRule('3', 'Banana'),
            createRule('4', 'Cherry')
        ];

        component.searchForm.get('searchRules')?.setValue('Plum');
        tick(500);
        fixture.detectChanges();

        expect(fixture.nativeElement.textContent).toContain('Displaying 0 of 4');
        expect(fixture.nativeElement.textContent).toContain('No matching rules');
    }));

    it('should display nothing when there are no rules', () => {
        component.rules = [];

        fixture.detectChanges();

        // The empty-list template may not display the count at all.
        expect(fixture.nativeElement.textContent).not.toContain('Displaying');
    });

    describe('Expand All / Collapse All', () => {
        const getLink = (text: string): HTMLAnchorElement | undefined =>
            Array.from<HTMLAnchorElement>(fixture.nativeElement.querySelectorAll('a')).find(
                (a) => a.textContent?.trim() === text
            );

        const getPanels = (): HTMLElement[] =>
            Array.from<HTMLElement>(fixture.nativeElement.querySelectorAll('mat-expansion-panel'));

        const isExpanded = (panel: HTMLElement): boolean => panel.classList.contains('mat-expanded');

        const getPanelByRuleName = (name: string): HTMLElement | undefined =>
            getPanels().find((panel) => panel.querySelector('mat-panel-title')?.textContent?.trim() === name);

        // open a panel the same way a user would, by clicking its header
        const openPanelByClickingHeader = (panel: HTMLElement): void => {
            (panel.querySelector('mat-expansion-panel-header') as HTMLElement).click();
            fixture.detectChanges();
        };

        it('should expand every rule when Expand All is clicked', () => {
            component.rules = [createRule('1', 'Rule One'), createRule('2', 'Rule Two'), createRule('3', 'Rule Three')];
            fixture.detectChanges();

            const panels = getPanels();
            expect(panels.length).toBe(3);
            panels.forEach((panel) => expect(isExpanded(panel)).toBe(false));

            getLink('Expand All')!.click();
            fixture.detectChanges();

            getPanels().forEach((panel) => expect(isExpanded(panel)).toBe(true));
        });

        it('should collapse every rule when Collapse All is clicked', () => {
            component.rules = [createRule('1', 'Rule One'), createRule('2', 'Rule Two'), createRule('3', 'Rule Three')];
            fixture.detectChanges();

            // open the panels individually so this test does not depend on Expand All working
            getPanels().forEach((panel) => openPanelByClickingHeader(panel));
            getPanels().forEach((panel) => expect(isExpanded(panel)).toBe(true));

            getLink('Collapse All')!.click();
            fixture.detectChanges();

            getPanels().forEach((panel) => expect(isExpanded(panel)).toBe(false));
        });

        it('should keep a rule with unsaved changes expanded when Collapse All is clicked', () => {
            component.rules = [createRule('1', 'Rule One'), createRule('2', 'Rule Two')];
            fixture.detectChanges();

            getPanels().forEach((panel) => openPanelByClickingHeader(panel));

            // simulate the user editing Rule One without saving
            component.setDirty('1', true);
            fixture.detectChanges();

            getLink('Collapse All')!.click();
            fixture.detectChanges();

            expect(isExpanded(getPanelByRuleName('Rule One')!)).toBe(true);
            expect(isExpanded(getPanelByRuleName('Rule Two')!)).toBe(false);
        });

        it('should not display Expand All / Collapse All when there are no rules to show', fakeAsync(() => {
            component.rules = [createRule('1', 'Apple'), createRule('2', 'Banana')];
            fixture.detectChanges();

            expect(getLink('Expand All')).toBeDefined();
            expect(getLink('Collapse All')).toBeDefined();

            // filter out every rule
            component.searchForm.get('searchRules')?.setValue('Plum');
            tick(500);
            fixture.detectChanges();

            expect(getLink('Expand All')).toBeUndefined();
            expect(getLink('Collapse All')).toBeUndefined();
        }));

        it('should only expand the rules matching the current search', fakeAsync(() => {
            component.rules = [
                createRule('1', 'Apple'),
                createRule('2', 'Apricot'),
                createRule('3', 'Banana'),
                createRule('4', 'Cherry')
            ];
            fixture.detectChanges();

            component.searchForm.get('searchRules')?.setValue('Ap');
            tick(500);
            fixture.detectChanges();

            getLink('Expand All')!.click();
            fixture.detectChanges();

            // clear the search so all rules are shown again
            component.searchForm.get('searchRules')?.setValue('');
            tick(500);
            fixture.detectChanges();

            expect(getPanels().length).toBe(4);
            expect(isExpanded(getPanelByRuleName('Apple')!)).toBe(true);
            expect(isExpanded(getPanelByRuleName('Apricot')!)).toBe(true);
            expect(isExpanded(getPanelByRuleName('Banana')!)).toBe(false);
            expect(isExpanded(getPanelByRuleName('Cherry')!)).toBe(false);
        }));
    });
});
