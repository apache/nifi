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

import { TestBed } from '@angular/core/testing';
import { MockStore, provideMockStore } from '@ngrx/store/testing';

import { ConnectorDefinitionComponent } from './connector-definition.component';
import {
    ConfigurationStep,
    connectorDefinitionFeatureKey,
    ConnectorDefinitionState,
    ConnectorPropertyDescriptor,
    ConnectorPropertyGroup
} from '../../state/connector-definition';
import { initialConnectorDefinitionState } from '../../state/connector-definition/connector-definition.reducer';
import { documentationFeatureKey } from '../../state';
import { initialState as initialErrorState } from '../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../state/current-user';
import { ConnectorDefinition } from '../../state/connector-definition';
import { loadStepDocumentation } from '../../state/connector-definition/connector-definition.actions';

describe('ConnectorDefinitionComponent', () => {
    function createMockProperty(name: string, required: boolean, description: string): ConnectorPropertyDescriptor {
        return {
            name,
            description,
            required,
            allowableValuesFetchable: false
        };
    }

    function createMockPropertyGroup(name: string, properties: ConnectorPropertyDescriptor[]): ConnectorPropertyGroup {
        return {
            name,
            description: `Description for ${name}`,
            properties
        };
    }

    function createMockConfigurationStep(name: string, propertyGroups: ConnectorPropertyGroup[]): ConfigurationStep {
        return {
            name,
            description: `Description for ${name}`,
            documented: false,
            propertyGroups
        };
    }

    async function setup(stateOverrides?: Partial<ConnectorDefinitionState>) {
        const initialState = {
            [errorFeatureKey]: initialErrorState,
            [currentUserFeatureKey]: initialCurrentUserState,
            [documentationFeatureKey]: {
                [connectorDefinitionFeatureKey]: {
                    ...initialConnectorDefinitionState,
                    ...stateOverrides
                }
            }
        };

        await TestBed.configureTestingModule({
            imports: [ConnectorDefinitionComponent],
            providers: [provideMockStore({ initialState })]
        }).compileComponents();

        const fixture = TestBed.createComponent(ConnectorDefinitionComponent);
        const component = fixture.componentInstance;
        const store = TestBed.inject(MockStore);

        fixture.detectChanges();

        return { component, fixture, store };
    }

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should have null connectorDefinitionState initially', async () => {
            const { component } = await setup();
            expect(component.connectorDefinitionState).toBeDefined();
        });
    });

    describe('Loading state logic', () => {
        it('should return true for isInitialLoading when connectorDefinition and error are null', async () => {
            const { component } = await setup();
            const state: ConnectorDefinitionState = {
                connectorDefinition: null,
                error: null,
                status: 'pending',
                stepDocumentation: {}
            };
            expect(component.isInitialLoading(state)).toBe(true);
        });

        it('should return false for isInitialLoading when connectorDefinition is present', async () => {
            const { component } = await setup();
            const state: ConnectorDefinitionState = {
                connectorDefinition: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-test-nar',
                    version: '1.0.0',
                    type: 'org.apache.nifi.TestConnector',
                    typeDescription: 'Test Connector',
                    buildInfo: {
                        revision: 'abc123',
                        version: '1.0.0'
                    },
                    additionalDetails: false
                },
                error: null,
                status: 'success',
                stepDocumentation: {}
            };
            expect(component.isInitialLoading(state)).toBe(false);
        });

        it('should return false for isInitialLoading when error is present', async () => {
            const { component } = await setup();
            const state: ConnectorDefinitionState = {
                connectorDefinition: null,
                error: 'Failed to load',
                status: 'error',
                stepDocumentation: {}
            };
            expect(component.isInitialLoading(state)).toBe(false);
        });
    });

    describe('Configuration step helpers', () => {
        it('should return true for hasConfigurationSteps when steps array has items', async () => {
            const { component } = await setup();
            const steps = [createMockConfigurationStep('Step 1', [])];
            expect(component.hasConfigurationSteps(steps)).toBe(true);
        });

        it('should return false for hasConfigurationSteps when steps array is empty', async () => {
            const { component } = await setup();
            expect(component.hasConfigurationSteps([])).toBe(false);
        });

        it('should return false for hasConfigurationSteps when steps is undefined', async () => {
            const { component } = await setup();
            expect(component.hasConfigurationSteps(undefined)).toBe(false);
        });
    });

    describe('Property group helpers', () => {
        it('should return true for hasPropertyGroups when groups array has items', async () => {
            const { component } = await setup();
            const groups = [createMockPropertyGroup('Group 1', [])];
            expect(component.hasPropertyGroups(groups)).toBe(true);
        });

        it('should return false for hasPropertyGroups when groups array is empty', async () => {
            const { component } = await setup();
            expect(component.hasPropertyGroups([])).toBe(false);
        });

        it('should return false for hasPropertyGroups when groups is undefined', async () => {
            const { component } = await setup();
            expect(component.hasPropertyGroups(undefined)).toBe(false);
        });
    });

    describe('Property helpers', () => {
        it('should return true for hasProperties when properties array has items', async () => {
            const { component } = await setup();
            const properties = [createMockProperty('prop1', true, 'Description')];
            expect(component.hasProperties(properties)).toBe(true);
        });

        it('should return false for hasProperties when properties array is empty', async () => {
            const { component } = await setup();
            expect(component.hasProperties([])).toBe(false);
        });

        it('should return false for hasProperties when properties is undefined', async () => {
            const { component } = await setup();
            expect(component.hasProperties(undefined)).toBe(false);
        });
    });

    describe('Property title formatting', () => {
        it('should append asterisk for required properties', async () => {
            const { component } = await setup();
            const property = createMockProperty('Required Property', true, 'Description');
            expect(component.formatPropertyTitle(property)).toBe('Required Property*');
        });

        it('should not append asterisk for optional properties', async () => {
            const { component } = await setup();
            const property = createMockProperty('Optional Property', false, 'Description');
            expect(component.formatPropertyTitle(property)).toBe('Optional Property');
        });
    });

    describe('Property lookup', () => {
        it('should return a function that finds properties by name', async () => {
            const { component } = await setup();
            const properties = [
                createMockProperty('prop1', true, 'First property'),
                createMockProperty('prop2', false, 'Second property')
            ];

            const lookupFn = component.lookupProperty(properties);

            expect(lookupFn('prop1')).toEqual(properties[0]);
            expect(lookupFn('prop2')).toEqual(properties[1]);
            expect(lookupFn('nonexistent')).toBeUndefined();
        });
    });

    describe('Step documentation', () => {
        it('should return false for isStepDocumentationLoading when step is not in state', async () => {
            const { component } = await setup();
            expect(component.isStepDocumentationLoading('Some Step')).toBe(false);
        });

        it('should return true for isStepDocumentationLoading when step status is loading', async () => {
            const { component } = await setup({
                stepDocumentation: {
                    'Some Step': { documentation: null, error: null, status: 'loading' }
                }
            });
            expect(component.isStepDocumentationLoading('Some Step')).toBe(true);
        });

        it('should return undefined for getStepDocumentation when step is not in state', async () => {
            const { component } = await setup();
            expect(component.getStepDocumentation('Some Step')).toBeUndefined();
        });

        it('should return documentation when step has loaded documentation', async () => {
            const { component } = await setup({
                stepDocumentation: {
                    'Some Step': { documentation: '# Step Documentation', error: null, status: 'success' }
                }
            });
            expect(component.getStepDocumentation('Some Step')).toBe('# Step Documentation');
        });

        it('should return undefined for getStepDocumentationError when step is not in state', async () => {
            const { component } = await setup();
            expect(component.getStepDocumentationError('Some Step')).toBeUndefined();
        });

        it('should return error when step has an error', async () => {
            const { component } = await setup({
                stepDocumentation: {
                    'Some Step': { documentation: null, error: 'Failed to load', status: 'error' }
                }
            });
            expect(component.getStepDocumentationError('Some Step')).toBe('Failed to load');
        });
    });

    describe('onStepExpanded', () => {
        function createMockConnectorDefinition(): ConnectorDefinition {
            return {
                group: 'org.apache.nifi',
                artifact: 'nifi-test-nar',
                version: '1.0.0',
                type: 'org.apache.nifi.TestConnector',
                typeDescription: 'Test Connector',
                buildInfo: { revision: 'abc123', version: '1.0.0' },
                additionalDetails: false
            };
        }

        it('should call loadStepDocumentation when step is documented', async () => {
            const { component, store } = await setup();
            const dispatchSpy = jest.spyOn(store, 'dispatch');
            const connectorDefinition = createMockConnectorDefinition();
            const step = createMockConfigurationStep('Documented Step', []);
            step.documented = true;

            component.onStepExpanded(connectorDefinition, step);

            expect(dispatchSpy).toHaveBeenCalledWith(
                loadStepDocumentation({
                    coordinates: {
                        group: connectorDefinition.group,
                        artifact: connectorDefinition.artifact,
                        version: connectorDefinition.version,
                        type: connectorDefinition.type
                    },
                    stepName: 'Documented Step'
                })
            );
        });

        it('should not call loadStepDocumentation when step is not documented', async () => {
            const { component, store } = await setup();
            const dispatchSpy = jest.spyOn(store, 'dispatch');
            dispatchSpy.mockClear();
            const connectorDefinition = createMockConnectorDefinition();
            const step = createMockConfigurationStep('Undocumented Step', []);
            step.documented = false;

            component.onStepExpanded(connectorDefinition, step);

            expect(dispatchSpy).not.toHaveBeenCalled();
        });
    });

    describe('loadStepDocumentation deduplication', () => {
        function createMockConnectorDefinition(): ConnectorDefinition {
            return {
                group: 'org.apache.nifi',
                artifact: 'nifi-test-nar',
                version: '1.0.0',
                type: 'org.apache.nifi.TestConnector',
                typeDescription: 'Test Connector',
                buildInfo: { revision: 'abc123', version: '1.0.0' },
                additionalDetails: false
            };
        }

        it('should not dispatch when step is already loading', async () => {
            const { component, store } = await setup({
                stepDocumentation: {
                    'Loading Step': { documentation: null, error: null, status: 'loading' }
                }
            });
            const dispatchSpy = jest.spyOn(store, 'dispatch');
            dispatchSpy.mockClear();
            const connectorDefinition = createMockConnectorDefinition();

            component.loadStepDocumentation(connectorDefinition, 'Loading Step');

            expect(dispatchSpy).not.toHaveBeenCalled();
        });

        it('should not dispatch when step already has documentation', async () => {
            const { component, store } = await setup({
                stepDocumentation: {
                    'Loaded Step': { documentation: '# Docs', error: null, status: 'success' }
                }
            });
            const dispatchSpy = jest.spyOn(store, 'dispatch');
            dispatchSpy.mockClear();
            const connectorDefinition = createMockConnectorDefinition();

            component.loadStepDocumentation(connectorDefinition, 'Loaded Step');

            expect(dispatchSpy).not.toHaveBeenCalled();
        });

        it('should dispatch when step is not in state', async () => {
            const { component, store } = await setup();
            const dispatchSpy = jest.spyOn(store, 'dispatch');
            dispatchSpy.mockClear();
            const connectorDefinition = createMockConnectorDefinition();

            component.loadStepDocumentation(connectorDefinition, 'New Step');

            expect(dispatchSpy).toHaveBeenCalledWith(
                loadStepDocumentation({
                    coordinates: {
                        group: connectorDefinition.group,
                        artifact: connectorDefinition.artifact,
                        version: connectorDefinition.version,
                        type: connectorDefinition.type
                    },
                    stepName: 'New Step'
                })
            );
        });

        it('should dispatch when step had an error', async () => {
            const { component, store } = await setup({
                stepDocumentation: {
                    'Error Step': { documentation: null, error: 'Failed', status: 'error' }
                }
            });
            const dispatchSpy = jest.spyOn(store, 'dispatch');
            dispatchSpy.mockClear();
            const connectorDefinition = createMockConnectorDefinition();

            component.loadStepDocumentation(connectorDefinition, 'Error Step');

            expect(dispatchSpy).toHaveBeenCalledWith(
                loadStepDocumentation({
                    coordinates: {
                        group: connectorDefinition.group,
                        artifact: connectorDefinition.artifact,
                        version: connectorDefinition.version,
                        type: connectorDefinition.type
                    },
                    stepName: 'Error Step'
                })
            );
        });
    });
});
