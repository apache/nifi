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

import { TemplateRef } from '@angular/core';
import { GlobalAccessPolicies } from './global-access-policies.component';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../state/access-policy/access-policy.reducer';
import { initialState as initialErrorState } from '../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../state/error';
import { accessPoliciesFeatureKey } from '../../state';
import { accessPolicyFeatureKey } from '../../state/access-policy';
import { tenantsFeatureKey } from '../../state/tenants';
import { initialState as initialTenantsState } from '../../state/tenants/tenants.reducer';
import { policyComponentFeatureKey } from '../../state/policy-component';
import { initialState as initialPolicyComponentState } from '../../state/policy-component/policy-component.reducer';
import { currentUserFeatureKey } from '../../../../state/current-user';
import { initialState as initialCurrentUserState } from '../../../../state/current-user/current-user.reducer';
import { extensionTypesFeatureKey } from '../../../../state/extension-types';
import { initialExtensionsTypesState } from '../../../../state/extension-types/extension-types.reducer';
import { flowConfigurationFeatureKey } from '../../../../state/flow-configuration';
import * as fromFlowConfiguration from '../../../../state/flow-configuration/flow-configuration.reducer';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';
import { selectCurrentRoute } from '@nifi/shared';
import { Action } from '../../state/shared';
import { selectGlobalAccessPolicy } from '../../state/access-policy/access-policy.actions';
import type { MockInstance } from 'vitest';

describe('GlobalAccessPolicies', () => {
    let component: GlobalAccessPolicies;
    let fixture: ComponentFixture<GlobalAccessPolicies>;
    let store: MockStore;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [GlobalAccessPolicies],
            imports: [NgxSkeletonLoaderComponent],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [extensionTypesFeatureKey]: initialExtensionsTypesState,
                        [flowConfigurationFeatureKey]: fromFlowConfiguration.initialState,
                        [accessPoliciesFeatureKey]: {
                            [accessPolicyFeatureKey]: initialState,
                            [tenantsFeatureKey]: initialTenantsState,
                            [policyComponentFeatureKey]: initialPolicyComponentState
                        }
                    },
                    selectors: [
                        {
                            selector: selectCurrentRoute,
                            value: {
                                params: {},
                                routeConfig: { path: 'global' },
                                url: []
                            }
                        }
                    ]
                })
            ]
        });
        fixture = TestBed.createComponent(GlobalAccessPolicies);
        component = fixture.componentInstance;
        store = TestBed.inject(MockStore);
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('connectorActionOptions', () => {
        it('should have extended action options for connectors', () => {
            expect(component.connectorActionOptions).toBeDefined();
            expect(component.connectorActionOptions.length).toBe(5);

            const values = component.connectorActionOptions.map((o) => o.value);
            expect(values).toContain('read');
            expect(values).toContain('write');
            expect(values).toContain('read-data');
            expect(values).toContain('write-data');
            expect(values).toContain('read-provenance-data');
        });
    });

    describe('resourceChanged', () => {
        it('should set supportsConnectorActions to true when connectors is selected', () => {
            component.resourceChanged('connectors');
            expect(component.supportsConnectorActions).toBe(true);
            expect(component.supportsReadWriteAction).toBe(true);
        });

        it('should set supportsConnectorActions to false for non-connector resources', () => {
            component.resourceChanged('controller');
            expect(component.supportsConnectorActions).toBe(false);
            expect(component.supportsReadWriteAction).toBe(true);
        });

        it('should reset action to read when connectors is selected', () => {
            component.resourceChanged('connectors');
            expect(component.policyForm.get('action')?.value).toBe('read');
        });
    });

    describe('actionChanged with connector extended actions', () => {
        let dispatchSpy: MockInstance;

        beforeEach(() => {
            dispatchSpy = vi.spyOn(store, 'dispatch');
        });

        it('should map read-data action to connectors with data resourceIdentifier', () => {
            component.policyForm.get('resource')?.setValue('connectors');
            component.policyForm.get('action')?.setValue('read-data');
            component.actionChanged();

            expect(dispatchSpy).toHaveBeenCalledWith(
                selectGlobalAccessPolicy({
                    request: {
                        resourceAction: {
                            resource: 'connectors',
                            action: Action.Read,
                            resourceIdentifier: 'data'
                        }
                    }
                })
            );
        });

        it('should map write-data action to connectors with data resourceIdentifier', () => {
            component.policyForm.get('resource')?.setValue('connectors');
            component.policyForm.get('action')?.setValue('write-data');
            component.actionChanged();

            expect(dispatchSpy).toHaveBeenCalledWith(
                selectGlobalAccessPolicy({
                    request: {
                        resourceAction: {
                            resource: 'connectors',
                            action: Action.Write,
                            resourceIdentifier: 'data'
                        }
                    }
                })
            );
        });

        it('should map read-provenance-data action to connectors with provenance-data resourceIdentifier', () => {
            component.policyForm.get('resource')?.setValue('connectors');
            component.policyForm.get('action')?.setValue('read-provenance-data');
            component.actionChanged();

            expect(dispatchSpy).toHaveBeenCalledWith(
                selectGlobalAccessPolicy({
                    request: {
                        resourceAction: {
                            resource: 'connectors',
                            action: Action.Read,
                            resourceIdentifier: 'provenance-data'
                        }
                    }
                })
            );
        });

        it('should map read action to connectors resource with read action', () => {
            component.policyForm.get('resource')?.setValue('connectors');
            component.policyForm.get('action')?.setValue('read');
            component.actionChanged();

            expect(dispatchSpy).toHaveBeenCalledWith(
                selectGlobalAccessPolicy({
                    request: {
                        resourceAction: {
                            resource: 'connectors',
                            action: Action.Read,
                            resourceIdentifier: undefined
                        }
                    }
                })
            );
        });

        it('should map write action to connectors resource with write action', () => {
            component.policyForm.get('resource')?.setValue('connectors');
            component.policyForm.get('action')?.setValue('write');
            component.actionChanged();

            expect(dispatchSpy).toHaveBeenCalledWith(
                selectGlobalAccessPolicy({
                    request: {
                        resourceAction: {
                            resource: 'connectors',
                            action: Action.Write,
                            resourceIdentifier: undefined
                        }
                    }
                })
            );
        });
    });

    describe('mapRouteResourceToFormState', () => {
        const callMapRouteResourceToFormState = (
            comp: GlobalAccessPolicies,
            resourceAction: { resource: string; action: string; resourceIdentifier?: string }
        ) => {
            return (comp as any).mapRouteResourceToFormState(resourceAction);
        };

        it('should map connectors with data resourceIdentifier and read action to read-data', () => {
            const result = callMapRouteResourceToFormState(component, {
                resource: 'connectors',
                action: Action.Read,
                resourceIdentifier: 'data'
            });

            expect(result).toEqual({
                resource: 'connectors',
                action: 'read-data',
                supportsConnectorActions: true
            });
        });

        it('should map connectors with data resourceIdentifier and write action to write-data', () => {
            const result = callMapRouteResourceToFormState(component, {
                resource: 'connectors',
                action: Action.Write,
                resourceIdentifier: 'data'
            });

            expect(result).toEqual({
                resource: 'connectors',
                action: 'write-data',
                supportsConnectorActions: true
            });
        });

        it('should map connectors with provenance-data resourceIdentifier to read-provenance-data', () => {
            const result = callMapRouteResourceToFormState(component, {
                resource: 'connectors',
                action: Action.Read,
                resourceIdentifier: 'provenance-data'
            });

            expect(result).toEqual({
                resource: 'connectors',
                action: 'read-provenance-data',
                supportsConnectorActions: true
            });
        });

        it('should map connectors with read action to connectors with read', () => {
            const result = callMapRouteResourceToFormState(component, {
                resource: 'connectors',
                action: Action.Read
            });

            expect(result).toEqual({
                resource: 'connectors',
                action: 'read',
                supportsConnectorActions: true
            });
        });

        it('should map connectors with write action to connectors with write', () => {
            const result = callMapRouteResourceToFormState(component, {
                resource: 'connectors',
                action: Action.Write
            });

            expect(result).toEqual({
                resource: 'connectors',
                action: 'write',
                supportsConnectorActions: true
            });
        });

        it('should pass through non-connector resources unchanged', () => {
            const result = callMapRouteResourceToFormState(component, {
                resource: 'controller',
                action: Action.Read
            });

            expect(result).toEqual({
                resource: 'controller',
                action: Action.Read,
                supportsConnectorActions: false
            });
        });
    });

    describe('getTemplateForInheritedPolicy', () => {
        const templateRef = {} as TemplateRef<unknown>;

        beforeEach(() => {
            component.inheritedFromConnectors = templateRef;
            component.inheritedFromConnectorData = templateRef;
            component.inheritedFromConnectorProvenance = templateRef;
            component.inheritedFromNoRestrictions = {} as TemplateRef<unknown>;
        });

        it('should use connector data template for /data/connectors', () => {
            expect(
                component.getTemplateForInheritedPolicy({
                    component: { resource: '/data/connectors' }
                } as any)
            ).toBe(component.inheritedFromConnectorData);
        });

        it('should use connector provenance template for /provenance-data/connectors', () => {
            expect(
                component.getTemplateForInheritedPolicy({
                    component: { resource: '/provenance-data/connectors' }
                } as any)
            ).toBe(component.inheritedFromConnectorProvenance);
        });

        it('should use connectors template for /connectors', () => {
            expect(
                component.getTemplateForInheritedPolicy({
                    component: { resource: '/connectors' }
                } as any)
            ).toBe(component.inheritedFromConnectors);
        });
    });
});
