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

import { EditRemoteProcessGroup } from './edit-remote-process-group.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ComponentType } from '@nifi/shared';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/flow/flow.reducer';

describe('EditRemoteProcessGroup', () => {
    let component: EditRemoteProcessGroup;
    let fixture: ComponentFixture<EditRemoteProcessGroup>;

    const data: any = {
        type: ComponentType.RemoteProcessGroup,
        uri: 'https://localhost:4200/nifi-api/remote-process-groups/abd5a02c-018b-1000-c602-fe83979f1997',
        entity: {
            revision: {
                clientId: 'a6482293-7fe8-43b4-8ab4-ee95b3b27721',
                version: 0
            },
            position: {
                x: -4,
                y: -698.5
            },
            permissions: {
                canRead: true,
                canWrite: false
            },
            component: {
                activeRemoteInputPortCount: 0,
                activeRemoteOutputPortCount: 0,
                comments: '',
                communicationsTimeout: '30 sec',
                flowRefreshed: '02/10/2024 15:20:58 EST',
                id: '868228e2-018d-1000-00e2-92a25d9cb363',
                inactiveRemoteInputPortCount: 0,
                inactiveRemoteOutputPortCount: 0,
                inputPortCount: 0,
                name: 'NiFi Flow',
                outputPortCount: 0,
                parentGroupId: '7be4b23a-018d-1000-d059-ca023539b044',
                proxyHost: '',
                proxyUser: '',
                targetSecure: true,
                targetUri: 'https://localhost:8443/nifi',
                targetUris: 'https://localhost:8443/nifi',
                transmitting: false,
                transportProtocol: 'HTTP',
                yieldDuration: '10 sec'
            }
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [EditRemoteProcessGroup, NoopAnimationsModule],
            providers: [
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: data
                },
                provideMockStore({ initialState }),
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(EditRemoteProcessGroup);
        component = fixture.componentInstance;

        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
