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

import { AddTenantToPolicyDialog } from './add-tenant-to-policy-dialog.component';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { AddTenantToPolicyDialogRequest } from '../../../state/access-policy';

describe('AddTenantToPolicyDialog', () => {
    let component: AddTenantToPolicyDialog;
    let fixture: ComponentFixture<AddTenantToPolicyDialog>;

    const data: AddTenantToPolicyDialogRequest = {
        accessPolicy: {
            revision: {
                clientId: '311032c3-f210-42f9-8a31-862c88b5fbd4',
                version: 4
            },
            id: 'f99bccd1-a30e-3e4a-98a2-dbc708edc67f',
            uri: 'https://localhost:4200/nifi-api/policies/f99bccd1-a30e-3e4a-98a2-dbc708edc67f',
            permissions: {
                canRead: true,
                canWrite: true
            },
            generated: '15:48:06 EST',
            component: {
                id: 'f99bccd1-a30e-3e4a-98a2-dbc708edc67f',
                resource: '/flow',
                action: 'read',
                configurable: true,
                users: [
                    {
                        revision: {
                            version: 0
                        },
                        id: 'bc646be3-146f-3cf2-bfd6-3a9f687ee7ab',
                        permissions: {
                            canRead: true,
                            canWrite: true
                        },
                        component: {
                            id: 'bc646be3-146f-3cf2-bfd6-3a9f687ee7ab',
                            identity: 'identify',
                            configurable: false
                        }
                    }
                ],
                userGroups: []
            }
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [AddTenantToPolicyDialog, NoopAnimationsModule],
            providers: [{ provide: MAT_DIALOG_DATA, useValue: data }]
        });
        fixture = TestBed.createComponent(AddTenantToPolicyDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
