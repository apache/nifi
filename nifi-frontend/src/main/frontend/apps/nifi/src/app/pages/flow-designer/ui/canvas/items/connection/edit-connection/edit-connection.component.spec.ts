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

import { EditConnectionComponent } from './edit-connection.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { EditConnectionDialogRequest } from '../../../../../state/flow';
import { ComponentType } from '@nifi/shared';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/flow/flow.reducer';
import { selectPrioritizerTypes } from '../../../../../../../state/extension-types/extension-types.selectors';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

describe('EditConnectionComponent', () => {
    let store: MockStore;
    let component: EditConnectionComponent;
    let fixture: ComponentFixture<EditConnectionComponent>;

    const data: EditConnectionDialogRequest = {
        type: ComponentType.Connection,
        uri: 'https://localhost:4200/nifi-api/connections/abd5a02c-018b-1000-c602-fe83979f1997',
        entity: {
            revision: {
                version: 0
            },
            id: 'abd5a02c-018b-1000-c602-fe83979f1997',
            uri: 'https://localhost:4200/nifi-api/connections/abd5a02c-018b-1000-c602-fe83979f1997',
            permissions: {
                canRead: true,
                canWrite: true
            },
            component: {
                id: 'abd5a02c-018b-1000-c602-fe83979f1997',
                versionedComponentId: '8ae63ec1-bf33-3af4-a1c1-bd19d41c19ec',
                parentGroupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                source: {
                    id: 'a67bf99d-018b-1000-611d-2993eb2f64b8',
                    versionedComponentId: '77458ab4-8e53-3855-a682-c787a2705b9d',
                    type: 'INPUT_PORT',
                    groupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                    name: 'in',
                    running: false
                },
                destination: {
                    id: 'a687e30e-018b-1000-f904-849a9f8e6bdb',
                    versionedComponentId: '56cf65da-e2cd-3ec5-9d69-d73c382a9049',
                    type: 'OUTPUT_PORT',
                    groupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                    name: 'out',
                    running: false
                },
                name: '',
                labelIndex: 1,
                zIndex: 0,
                backPressureObjectThreshold: 10000,
                backPressureDataSizeThreshold: '1 GB',
                flowFileExpiration: '0 sec',
                prioritizers: [],
                bends: [],
                loadBalanceStrategy: 'DO_NOT_LOAD_BALANCE',
                loadBalancePartitionAttribute: '',
                loadBalanceCompression: 'DO_NOT_COMPRESS',
                loadBalanceStatus: 'LOAD_BALANCE_NOT_CONFIGURED'
            },
            status: {
                id: 'abd5a02c-018b-1000-c602-fe83979f1997',
                groupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                name: '',
                statsLastRefreshed: '09:06:47 EST',
                sourceId: 'a67bf99d-018b-1000-611d-2993eb2f64b8',
                sourceName: 'in',
                destinationId: 'a687e30e-018b-1000-f904-849a9f8e6bdb',
                destinationName: 'out',
                aggregateSnapshot: {
                    id: 'abd5a02c-018b-1000-c602-fe83979f1997',
                    groupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                    name: '',
                    sourceName: 'in',
                    destinationName: 'out',
                    flowFilesIn: 0,
                    bytesIn: 0,
                    input: '0 (0 bytes)',
                    flowFilesOut: 0,
                    bytesOut: 0,
                    output: '0 (0 bytes)',
                    flowFilesQueued: 0,
                    bytesQueued: 0,
                    queued: '0 (0 bytes)',
                    queuedSize: '0 bytes',
                    queuedCount: '0',
                    percentUseCount: 0,
                    percentUseBytes: 0,
                    flowFileAvailability: 'ACTIVE_QUEUE_EMPTY'
                }
            },
            bends: [],
            labelIndex: 1,
            zIndex: 0,
            sourceId: 'a67bf99d-018b-1000-611d-2993eb2f64b8',
            sourceGroupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
            sourceType: 'INPUT_PORT',
            destinationId: 'a687e30e-018b-1000-f904-849a9f8e6bdb',
            destinationGroupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
            destinationType: 'OUTPUT_PORT'
        }
        // TODO - create separate test for existing different scenario edit scenarios
        // newDestination?: {
        //     type: ComponentType | null;
        //     id?: string;
        //     groupId: string;
        //     name: string;
        // }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [EditConnectionComponent, NoopAnimationsModule],
            providers: [
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: data
                },
                provideMockStore({ initialState }),
                { provide: MatDialogRef, useValue: null }
            ]
        });

        store = TestBed.inject(MockStore);
        store.overrideSelector(selectPrioritizerTypes, [
            {
                type: 'org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-framework-nar',
                    version: '2.0.0-SNAPSHOT'
                },
                restricted: false,
                tags: []
            }
        ]);

        fixture = TestBed.createComponent(EditConnectionComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
