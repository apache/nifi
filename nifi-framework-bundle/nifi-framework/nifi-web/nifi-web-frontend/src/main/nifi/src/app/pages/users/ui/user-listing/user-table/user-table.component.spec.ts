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

import { UserTable } from './user-table.component';
import { MatTableModule } from '@angular/material/table';
import { MatSortModule } from '@angular/material/sort';
import { MatInputModule } from '@angular/material/input';
import { ReactiveFormsModule } from '@angular/forms';
import { MatSelectModule } from '@angular/material/select';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { CurrentUser } from '../../../../../state/current-user';

describe('UserTable', () => {
    let component: UserTable;
    let fixture: ComponentFixture<UserTable>;

    const currentUser: CurrentUser = {
        identity: 'admin',
        anonymous: false,
        provenancePermissions: {
            canRead: false,
            canWrite: false
        },
        countersPermissions: {
            canRead: false,
            canWrite: false
        },
        tenantsPermissions: {
            canRead: true,
            canWrite: true
        },
        controllerPermissions: {
            canRead: true,
            canWrite: true
        },
        policiesPermissions: {
            canRead: true,
            canWrite: true
        },
        systemPermissions: {
            canRead: true,
            canWrite: false
        },
        parameterContextPermissions: {
            canRead: true,
            canWrite: true
        },
        restrictedComponentsPermissions: {
            canRead: false,
            canWrite: true
        },
        componentRestrictionPermissions: [
            {
                requiredPermission: {
                    id: 'read-distributed-filesystem',
                    label: 'read distributed filesystem'
                },
                permissions: {
                    canRead: false,
                    canWrite: true
                }
            },
            {
                requiredPermission: {
                    id: 'access-keytab',
                    label: 'access keytab'
                },
                permissions: {
                    canRead: false,
                    canWrite: true
                }
            },
            {
                requiredPermission: {
                    id: 'export-nifi-details',
                    label: 'export nifi details'
                },
                permissions: {
                    canRead: false,
                    canWrite: true
                }
            },
            {
                requiredPermission: {
                    id: 'read-filesystem',
                    label: 'read filesystem'
                },
                permissions: {
                    canRead: false,
                    canWrite: true
                }
            },
            {
                requiredPermission: {
                    id: 'access-environment-credentials',
                    label: 'access environment credentials'
                },
                permissions: {
                    canRead: false,
                    canWrite: true
                }
            },
            {
                requiredPermission: {
                    id: 'reference-remote-resources',
                    label: 'reference remote resources'
                },
                permissions: {
                    canRead: false,
                    canWrite: true
                }
            },
            {
                requiredPermission: {
                    id: 'execute-code',
                    label: 'execute code'
                },
                permissions: {
                    canRead: false,
                    canWrite: true
                }
            },
            {
                requiredPermission: {
                    id: 'access-ticket-cache',
                    label: 'access ticket cache'
                },
                permissions: {
                    canRead: false,
                    canWrite: true
                }
            },
            {
                requiredPermission: {
                    id: 'write-filesystem',
                    label: 'write filesystem'
                },
                permissions: {
                    canRead: false,
                    canWrite: true
                }
            },
            {
                requiredPermission: {
                    id: 'write-distributed-filesystem',
                    label: 'write distributed filesystem'
                },
                permissions: {
                    canRead: false,
                    canWrite: true
                }
            }
        ],
        canVersionFlows: false
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [
                UserTable,
                MatTableModule,
                MatSortModule,
                MatInputModule,
                ReactiveFormsModule,
                MatSelectModule,
                NoopAnimationsModule
            ]
        });
        fixture = TestBed.createComponent(UserTable);
        component = fixture.componentInstance;
        component.currentUser = currentUser;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
