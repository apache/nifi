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
import { ComponentAccessPolicies } from './component-access-policies.component';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ComponentType } from '@nifi/shared';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../state/access-policy/access-policy.reducer';
import { initialState as initialErrorState } from '../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../state/error';
import { accessPoliciesFeatureKey } from '../../state';
import { accessPolicyFeatureKey } from '../../state/access-policy';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';

describe('ComponentAccessPolicies', () => {
    let component: ComponentAccessPolicies;
    let fixture: ComponentFixture<ComponentAccessPolicies>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [ComponentAccessPolicies],
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
        fixture = TestBed.createComponent(ComponentAccessPolicies);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('isComponentPolicy', () => {
        const createPolicyComponentState = (resource: string, allowRemoteAccess = false) => ({
            resource,
            allowRemoteAccess,
            label: 'Test Component'
        });

        const createOption = (value: string) => ({
            text: 'Test Option',
            value,
            description: 'Test description'
        });

        describe('connectors', () => {
            it('should enable read-data policy for connectors', () => {
                const state = createPolicyComponentState('connectors');
                const option = createOption('read-data');
                expect(component.isComponentPolicy(option, state as any)).toBe(true);
            });

            it('should enable write-data policy for connectors', () => {
                const state = createPolicyComponentState('connectors');
                const option = createOption('write-data');
                expect(component.isComponentPolicy(option, state as any)).toBe(true);
            });

            it('should enable read-provenance-data policy for connectors', () => {
                const state = createPolicyComponentState('connectors');
                const option = createOption('read-provenance-data');
                expect(component.isComponentPolicy(option, state as any)).toBe(true);
            });

            it('should disable write-send-data policy for connectors', () => {
                const state = createPolicyComponentState('connectors');
                const option = createOption('write-send-data');
                expect(component.isComponentPolicy(option, state as any)).toBe(false);
            });

            it('should disable write-receive-data policy for connectors', () => {
                const state = createPolicyComponentState('connectors');
                const option = createOption('write-receive-data');
                expect(component.isComponentPolicy(option, state as any)).toBe(false);
            });

            it('should enable component policies for connectors', () => {
                const state = createPolicyComponentState('connectors');
                expect(component.isComponentPolicy(createOption('read-component'), state as any)).toBe(true);
                expect(component.isComponentPolicy(createOption('write-component'), state as any)).toBe(true);
                expect(component.isComponentPolicy(createOption('write-operation'), state as any)).toBe(true);
            });
        });

        describe('parameter-contexts', () => {
            it('should disable data policies for parameter-contexts', () => {
                const state = createPolicyComponentState('parameter-contexts');
                expect(component.isComponentPolicy(createOption('read-data'), state as any)).toBe(false);
                expect(component.isComponentPolicy(createOption('write-operation'), state as any)).toBe(false);
            });
        });
    });

    describe('getContextType', () => {
        it('should return ComponentType.Connector for connectors resource', () => {
            component.resource = 'connectors';
            expect(component.getContextType()).toBe(ComponentType.Connector);
        });
    });

    describe('getTemplateForInheritedPolicy', () => {
        const templateRef = {} as TemplateRef<unknown>;

        beforeEach(() => {
            component.inheritedFromConnectors = templateRef;
            component.inheritedFromConnectorData = templateRef;
            component.inheritedFromConnectorProvenance = templateRef;
            component.inheritedFromProcessGroup = {} as TemplateRef<unknown>;
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
