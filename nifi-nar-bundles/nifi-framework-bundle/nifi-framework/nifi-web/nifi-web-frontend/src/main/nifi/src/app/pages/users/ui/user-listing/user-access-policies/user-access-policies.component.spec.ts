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
import { MAT_DIALOG_DATA } from '@angular/material/dialog';

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
            providers: [{ provide: MAT_DIALOG_DATA, useValue: data }]
        });
        fixture = TestBed.createComponent(UserAccessPolicies);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
