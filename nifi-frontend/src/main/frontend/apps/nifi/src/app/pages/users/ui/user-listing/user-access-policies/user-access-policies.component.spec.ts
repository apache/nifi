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

import { UserAccessPolicies } from './user-access-policies.component';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { UserAccessPoliciesDialogRequest } from '../../../state/user-listing';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';

describe('UserAccessPolicies', () => {
    let component: UserAccessPolicies;
    let fixture: ComponentFixture<UserAccessPolicies>;

    const data: UserAccessPoliciesDialogRequest = {
        id: 'acfbfa2c-018c-1000-0311-47b83e34c9c3',
        identity: 'group 1',
        accessPolicies: [
            {
                revision: {
                    clientId: 'b09bd713-018c-1000-e5b8-14855e466f1b',
                    version: 4
                },
                id: 'b0c3148d-018c-1000-2cfe-8fab902c11f7',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: 'b0c3148d-018c-1000-2cfe-8fab902c11f7',
                    resource: '/system',
                    action: 'read',
                    configurable: true
                }
            }
        ]
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [UserAccessPolicies, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(UserAccessPolicies);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('formatPolicy', () => {
        it('should label per-instance connector policies', () => {
            const label = component.formatPolicy({
                permissions: { canRead: true, canWrite: true },
                component: {
                    resource: '/connectors/connector-1',
                    action: 'read',
                    componentReference: {
                        id: 'connector-1',
                        permissions: { canRead: true, canWrite: true },
                        component: { name: 'My Connector' }
                    }
                }
            } as any);

            expect(label).toBe('Component policy for connector My Connector');
        });

        it('should label connector data policies', () => {
            const label = component.formatPolicy({
                permissions: { canRead: true, canWrite: true },
                component: {
                    resource: '/data/connectors/connector-1',
                    action: 'read',
                    componentReference: {
                        id: 'connector-1',
                        permissions: { canRead: true, canWrite: true },
                        component: { name: 'My Connector' }
                    }
                }
            } as any);

            expect(label).toBe('Data policy for connector My Connector');
        });

        it('should label connector provenance policies', () => {
            const label = component.formatPolicy({
                permissions: { canRead: true, canWrite: true },
                component: {
                    resource: '/provenance-data/connectors/connector-1',
                    action: 'read',
                    componentReference: {
                        id: 'connector-1',
                        permissions: { canRead: true, canWrite: true },
                        component: { name: 'My Connector' }
                    }
                }
            } as any);

            expect(label).toBe('Provenance policy for connector My Connector');
        });

        it('should label global connector data read policy', () => {
            const label = component.formatPolicy({
                permissions: { canRead: true, canWrite: true },
                component: {
                    resource: '/data/connectors',
                    action: 'read',
                    configurable: true
                }
            } as any);

            expect(label).toBe('Global policy to view the data for connectors');
        });

        it('should label global connector data write policy', () => {
            const label = component.formatPolicy({
                permissions: { canRead: true, canWrite: true },
                component: {
                    resource: '/data/connectors',
                    action: 'write',
                    configurable: true
                }
            } as any);

            expect(label).toBe('Global policy to modify the data for connectors');
        });

        it('should label global connector provenance policy', () => {
            const label = component.formatPolicy({
                permissions: { canRead: true, canWrite: true },
                component: {
                    resource: '/provenance-data/connectors',
                    action: 'read',
                    configurable: true
                }
            } as any);

            expect(label).toBe('Global policy to view provenance for connectors');
        });

        it('should label global connectors policy via policy type listing', () => {
            const label = component.formatPolicy({
                permissions: { canRead: true, canWrite: true },
                component: {
                    resource: '/connectors',
                    action: 'read',
                    configurable: true
                }
            } as any);

            expect(label).toBe('Global policy to access connectors');
        });
    });

    describe('getPolicyTargetLink', () => {
        it('should link to the connectors listing for connector policies', () => {
            const link = component.getPolicyTargetLink({
                permissions: { canRead: true, canWrite: true },
                component: {
                    resource: '/connectors/connector-1',
                    action: 'read',
                    componentReference: {
                        id: 'connector-1',
                        permissions: { canRead: true, canWrite: true }
                    }
                }
            } as any);

            expect(link).toEqual(['/connectors', 'connector-1']);
        });
    });
});
