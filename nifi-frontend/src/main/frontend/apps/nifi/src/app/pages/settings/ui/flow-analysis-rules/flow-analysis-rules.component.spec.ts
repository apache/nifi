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

import { TestBed } from '@angular/core/testing';
import { provideMockStore, MockStore } from '@ngrx/store/testing';

import { FlowAnalysisRules } from './flow-analysis-rules.component';
import { initialState } from '../../state/flow-analysis-rules/flow-analysis-rules.reducer';
import { FlowAnalysisRuleEntity, flowAnalysisRulesFeatureKey } from '../../state/flow-analysis-rules';
import { settingsFeatureKey } from '../../state';
import { ComponentType } from '@nifi/shared';
import * as FlowAnalysisRulesActions from '../../state/flow-analysis-rules/flow-analysis-rules.actions';
import { navigateToComponentDocumentation } from '../../../../state/documentation/documentation.actions';
import { getComponentStateAndOpenDialog } from '../../../../state/component-state/component-state.actions';
import { currentUserFeatureKey } from '../../../../state/current-user';
import * as fromCurrentUser from '../../../../state/current-user/current-user.reducer';

describe('FlowAnalysisRules', () => {
    // Mock data factories
    const mockTimestampIso = '2023-10-08T12:00:00.000Z';

    function createMockFlowAnalysisRuleEntity(): FlowAnalysisRuleEntity {
        return {
            id: 'test-flow-analysis-rule-id',
            uri: 'test-uri',
            bulletins: [
                {
                    id: 1,
                    sourceId: 'test-flow-analysis-rule-id',
                    groupId: 'test-group-id',
                    timestamp: '12:00:00 UTC',
                    timestampIso: mockTimestampIso,
                    canRead: true,
                    bulletin: {
                        id: 1,
                        category: 'INFO',
                        level: 'INFO',
                        message: 'Test bulletin',
                        timestamp: '12:00:00 UTC',
                        timestampIso: mockTimestampIso,
                        sourceId: 'test-flow-analysis-rule-id',
                        groupId: 'test-group-id',
                        sourceType: 'FLOW_ANALYSIS_RULE',
                        sourceName: 'Test Flow Analysis Rule'
                    }
                }
            ],
            permissions: {
                canRead: true,
                canWrite: true
            },
            operatePermissions: {
                canRead: true,
                canWrite: true
            },
            revision: {
                version: 1,
                clientId: 'test-client'
            },
            component: {
                id: 'test-flow-analysis-rule-id',
                name: 'Test Flow Analysis Rule',
                type: 'org.apache.nifi.TestFlowAnalysisRule',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
                state: 'DISABLED',
                comments: 'Test comments',
                persistsState: true,
                restricted: false,
                deprecated: false,
                multipleVersionsAvailable: false,
                supportsSensitiveDynamicProperties: false,
                properties: {},
                descriptors: {},
                customUiUrl: '',
                annotationData: '',
                validationErrors: [],
                validationStatus: 'VALID'
            },
            status: {
                runStatus: 'DISABLED',
                validationStatus: 'VALID'
            }
        };
    }

    // Setup function for component configuration
    async function setup() {
        await TestBed.configureTestingModule({
            imports: [FlowAnalysisRules],
            providers: [
                provideMockStore({
                    initialState: {
                        [settingsFeatureKey]: {
                            [flowAnalysisRulesFeatureKey]: {
                                ...initialState
                            }
                        },
                        [currentUserFeatureKey]: fromCurrentUser.initialState
                    }
                })
            ]
        }).compileComponents();

        const fixture = TestBed.createComponent(FlowAnalysisRules);
        const component = fixture.componentInstance;
        const store = TestBed.inject(MockStore);

        fixture.detectChanges();

        return { component, fixture, store };
    }

    it('should create', async () => {
        const { component } = await setup();
        expect(component).toBeTruthy();
    });

    describe('Component Initialization', () => {
        it('should dispatch loadFlowAnalysisRules on ngOnInit', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.ngOnInit();

            expect(store.dispatch).toHaveBeenCalledWith(FlowAnalysisRulesActions.loadFlowAnalysisRules());
        });

        it('should dispatch resetFlowAnalysisRulesState on ngOnDestroy', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.ngOnDestroy();

            expect(store.dispatch).toHaveBeenCalledWith(FlowAnalysisRulesActions.resetFlowAnalysisRulesState());
        });

        it('should return true for isInitialLoading when timestamp matches initial state', async () => {
            const { component } = await setup();
            const state = { ...initialState };

            expect(component.isInitialLoading(state)).toBe(true);
        });

        it('should return false for isInitialLoading when timestamp differs from initial state', async () => {
            const { component } = await setup();
            const state = {
                ...initialState,
                loadedTimestamp: '2023-01-01 12:00:00 EST'
            };

            expect(component.isInitialLoading(state)).toBe(false);
        });
    });

    describe('Action dispatching', () => {
        it('should dispatch loadFlowAnalysisRules action when refreshFlowAnalysisRuleListing is called', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.refreshFlowAnalysisRuleListing();

            expect(store.dispatch).toHaveBeenCalledWith(FlowAnalysisRulesActions.loadFlowAnalysisRules());
        });

        it('should dispatch openNewFlowAnalysisRuleDialog action when openNewFlowAnalysisRuleDialog is called', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.openNewFlowAnalysisRuleDialog();

            expect(store.dispatch).toHaveBeenCalledWith(FlowAnalysisRulesActions.openNewFlowAnalysisRuleDialog());
        });

        it('should dispatch selectFlowAnalysisRule action when selectFlowAnalysisRule is called', async () => {
            const { component, store } = await setup();
            const mockFlowAnalysisRuleEntity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(store, 'dispatch');

            component.selectFlowAnalysisRule(mockFlowAnalysisRuleEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                FlowAnalysisRulesActions.selectFlowAnalysisRule({
                    request: {
                        id: mockFlowAnalysisRuleEntity.id
                    }
                })
            );
        });

        it('should dispatch navigateToComponentDocumentation action when viewFlowAnalysisRuleDocumentation is called', async () => {
            const { component, store } = await setup();
            const mockFlowAnalysisRuleEntity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(store, 'dispatch');

            component.viewFlowAnalysisRuleDocumentation(mockFlowAnalysisRuleEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                navigateToComponentDocumentation({
                    request: {
                        backNavigation: {
                            route: ['/settings', 'flow-analysis-rules', mockFlowAnalysisRuleEntity.id],
                            routeBoundary: ['/documentation'],
                            context: 'Flow Analysis Rule'
                        },
                        parameters: {
                            componentType: ComponentType.FlowAnalysisRule,
                            type: mockFlowAnalysisRuleEntity.component.type,
                            group: mockFlowAnalysisRuleEntity.component.bundle.group,
                            artifact: mockFlowAnalysisRuleEntity.component.bundle.artifact,
                            version: mockFlowAnalysisRuleEntity.component.bundle.version
                        }
                    }
                })
            );
        });

        it('should dispatch enableFlowAnalysisRule action when enableFlowAnalysisRule is called', async () => {
            const { component, store } = await setup();
            const mockFlowAnalysisRuleEntity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(store, 'dispatch');

            component.enableFlowAnalysisRule(mockFlowAnalysisRuleEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                FlowAnalysisRulesActions.enableFlowAnalysisRule({
                    request: {
                        id: mockFlowAnalysisRuleEntity.id,
                        flowAnalysisRule: mockFlowAnalysisRuleEntity
                    }
                })
            );
        });

        it('should dispatch disableFlowAnalysisRule action when disableFlowAnalysisRule is called', async () => {
            const { component, store } = await setup();
            const mockFlowAnalysisRuleEntity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(store, 'dispatch');

            component.disableFlowAnalysisRule(mockFlowAnalysisRuleEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                FlowAnalysisRulesActions.disableFlowAnalysisRule({
                    request: {
                        id: mockFlowAnalysisRuleEntity.id,
                        flowAnalysisRule: mockFlowAnalysisRuleEntity
                    }
                })
            );
        });

        it('should dispatch getComponentStateAndOpenDialog action when viewStateFlowAnalysisRule is called with disabled rule', async () => {
            const { component, store } = await setup();
            const mockFlowAnalysisRuleEntity = createMockFlowAnalysisRuleEntity();
            mockFlowAnalysisRuleEntity.status.runStatus = 'DISABLED';
            jest.spyOn(store, 'dispatch');

            component.viewStateFlowAnalysisRule(mockFlowAnalysisRuleEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                getComponentStateAndOpenDialog({
                    request: {
                        componentType: ComponentType.FlowAnalysisRule,
                        componentId: mockFlowAnalysisRuleEntity.id,
                        componentName: mockFlowAnalysisRuleEntity.component.name,
                        canClear: true
                    }
                })
            );
        });

        it('should dispatch getComponentStateAndOpenDialog action with canClear false when rule is enabled', async () => {
            const { component, store } = await setup();
            const mockFlowAnalysisRuleEntity = createMockFlowAnalysisRuleEntity();
            mockFlowAnalysisRuleEntity.status.runStatus = 'ENABLED';
            jest.spyOn(store, 'dispatch');

            component.viewStateFlowAnalysisRule(mockFlowAnalysisRuleEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                getComponentStateAndOpenDialog({
                    request: {
                        componentType: ComponentType.FlowAnalysisRule,
                        componentId: mockFlowAnalysisRuleEntity.id,
                        componentName: mockFlowAnalysisRuleEntity.component.name,
                        canClear: false
                    }
                })
            );
        });

        it('should dispatch openChangeFlowAnalysisRuleVersionDialog action when changeFlowAnalysisRuleVersion is called', async () => {
            const { component, store } = await setup();
            const mockFlowAnalysisRuleEntity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(store, 'dispatch');

            component.changeFlowAnalysisRuleVersion(mockFlowAnalysisRuleEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                FlowAnalysisRulesActions.openChangeFlowAnalysisRuleVersionDialog({
                    request: {
                        id: mockFlowAnalysisRuleEntity.id,
                        bundle: mockFlowAnalysisRuleEntity.component.bundle,
                        uri: mockFlowAnalysisRuleEntity.uri,
                        type: mockFlowAnalysisRuleEntity.component.type,
                        revision: mockFlowAnalysisRuleEntity.revision
                    }
                })
            );
        });

        it('should dispatch promptFlowAnalysisRuleDeletion action when deleteFlowAnalysisRule is called', async () => {
            const { component, store } = await setup();
            const mockFlowAnalysisRuleEntity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(store, 'dispatch');

            component.deleteFlowAnalysisRule(mockFlowAnalysisRuleEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                FlowAnalysisRulesActions.promptFlowAnalysisRuleDeletion({
                    request: {
                        flowAnalysisRule: mockFlowAnalysisRuleEntity
                    }
                })
            );
        });

        it('should dispatch navigateToEditFlowAnalysisRule action when configureFlowAnalysisRule is called', async () => {
            const { component, store } = await setup();
            const mockFlowAnalysisRuleEntity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(store, 'dispatch');

            component.configureFlowAnalysisRule(mockFlowAnalysisRuleEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                FlowAnalysisRulesActions.navigateToEditFlowAnalysisRule({
                    id: mockFlowAnalysisRuleEntity.id
                })
            );
        });

        it('should dispatch clearFlowAnalysisRuleBulletins action when clearBulletinsFlowAnalysisRule is called', async () => {
            const { component, store } = await setup();

            const mockFlowAnalysisRuleEntity = createMockFlowAnalysisRuleEntity();

            jest.spyOn(store, 'dispatch');

            component.clearBulletinsFlowAnalysisRule(mockFlowAnalysisRuleEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                FlowAnalysisRulesActions.clearFlowAnalysisRuleBulletins({
                    request: {
                        uri: mockFlowAnalysisRuleEntity.uri,
                        fromTimestamp: mockTimestampIso,
                        componentId: mockFlowAnalysisRuleEntity.id,
                        componentType: ComponentType.FlowAnalysisRule
                    }
                })
            );
        });

        it('should not dispatch when no bulletins exist', async () => {
            const { component, store } = await setup();

            const mockFlowAnalysisRuleEntity = {
                ...createMockFlowAnalysisRuleEntity(),
                bulletins: [] // No bulletins
            };
            jest.spyOn(store, 'dispatch');

            component.clearBulletinsFlowAnalysisRule(mockFlowAnalysisRuleEntity);

            expect(store.dispatch).not.toHaveBeenCalled();
        });
    });
});
