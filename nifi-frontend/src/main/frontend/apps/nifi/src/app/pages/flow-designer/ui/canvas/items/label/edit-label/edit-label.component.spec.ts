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

import { EditLabel } from './edit-label.component';
import { EditComponentDialogRequest } from '../../../../../state/flow';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { ComponentType } from '@nifi/shared';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/flow/flow.reducer';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ClusterConnectionService } from '../../../../../../../service/cluster-connection.service';

describe('EditLabel', () => {
    let component: EditLabel;
    let fixture: ComponentFixture<EditLabel>;

    const data: EditComponentDialogRequest = {
        type: ComponentType.Label,
        uri: 'https://localhost:4200/nifi-api/labels/d7c98831-018e-1000-6a26-6d74b57897e5',
        entity: {
            revision: {
                version: 0
            },
            id: 'd7c98831-018e-1000-6a26-6d74b57897e5',
            uri: 'https://localhost:4200/nifi-api/labels/d7c98831-018e-1000-6a26-6d74b57897e5',
            position: {
                x: 424,
                y: -432
            },
            permissions: {
                canRead: true,
                canWrite: true
            },
            dimensions: {
                width: 150,
                height: 150
            },
            zIndex: 4,
            component: {
                id: 'd7c98831-018e-1000-6a26-6d74b57897e5',
                versionedComponentId: 'c99e7867-73c6-3687-8f27-9b1ee5d761c4',
                parentGroupId: 'f588fccd-018d-1000-e6dd-56d56dead186',
                position: {
                    x: 424,
                    y: -432
                },
                width: 150,
                height: 150,
                zIndex: 4,
                style: {}
            }
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [EditLabel, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                provideMockStore({ initialState }),
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: jest.fn()
                    }
                },
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(EditLabel);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
