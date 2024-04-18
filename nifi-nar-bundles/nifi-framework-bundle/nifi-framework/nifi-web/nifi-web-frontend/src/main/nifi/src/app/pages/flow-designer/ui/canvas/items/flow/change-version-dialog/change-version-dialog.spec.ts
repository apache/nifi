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

import { ChangeVersionDialog } from './change-version-dialog';
import { ChangeVersionDialogRequest } from '../../../../../state/flow';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/flow/flow.reducer';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { selectTimeOffset } from '../../../../../../../state/flow-configuration/flow-configuration.selectors';

describe('ChangeVersionDialog', () => {
    let component: ChangeVersionDialog;
    let fixture: ComponentFixture<ChangeVersionDialog>;
    const data: ChangeVersionDialogRequest = {
        processGroupId: '80f86c6f-018e-1000-68e5-17d68c402f4c',
        revision: {
            clientId: '8f36f627-d927-4a37-a1f2-a6deb9b91e50',
            version: 31
        },
        versionControlInformation: {
            groupId: '80f86c6f-018e-1000-68e5-17d68c402f4c',
            registryId: '80441509-018e-1000-12b2-d70361a7f661',
            registryName: 'Local Registry',
            bucketId: 'bd6fa6cc-da95-4a12-92cc-9b38b3d48266',
            bucketName: 'RAG',
            flowId: 'e884a53c-cbc2-4cb6-9ebd-d9e5d5bb7d05',
            flowName: 'sdaf',
            flowDescription: '',
            version: 2,
            state: 'UP_TO_DATE',
            stateExplanation: 'Flow version is current'
        },
        versions: [
            {
                versionedFlowSnapshotMetadata: {
                    bucketIdentifier: 'bd6fa6cc-da95-4a12-92cc-9b38b3d48266',
                    flowIdentifier: 'e884a53c-cbc2-4cb6-9ebd-d9e5d5bb7d05',
                    version: 2,
                    timestamp: 1712171233843,
                    author: 'anonymous',
                    comments: ''
                },
                registryId: '80441509-018e-1000-12b2-d70361a7f661'
            },
            {
                versionedFlowSnapshotMetadata: {
                    bucketIdentifier: 'bd6fa6cc-da95-4a12-92cc-9b38b3d48266',
                    flowIdentifier: 'e884a53c-cbc2-4cb6-9ebd-d9e5d5bb7d05',
                    version: 1,
                    timestamp: 1712076498414,
                    author: 'anonymous',
                    comments: ''
                },
                registryId: '80441509-018e-1000-12b2-d70361a7f661'
            }
        ]
    };

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [ChangeVersionDialog, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                provideMockStore({
                    initialState,
                    selectors: [
                        {
                            selector: selectTimeOffset,
                            value: 0
                        }
                    ]
                })
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(ChangeVersionDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
