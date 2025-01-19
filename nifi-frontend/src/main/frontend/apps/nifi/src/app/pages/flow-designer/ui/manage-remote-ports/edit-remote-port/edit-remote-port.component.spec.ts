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

import { EditRemotePortComponent } from './edit-remote-port.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { EditComponentDialogRequest } from '../../../state/flow';
import { ComponentType } from '@nifi/shared';
import { initialState } from '../../../state/manage-remote-ports/manage-remote-ports.reducer';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';

describe('EditRemotePortComponent', () => {
    let component: EditRemotePortComponent;
    let fixture: ComponentFixture<EditRemotePortComponent>;

    const data: EditComponentDialogRequest = {
        type: ComponentType.OutputPort,
        uri: 'https://localhost:4200/nifi-api/remote-process-groups/95a4b210-018b-1000-772a-5a9ebfa03287',
        entity: {
            id: 'a687e30e-018b-1000-f904-849a9f8e6bdb',
            groupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
            name: 'out',
            transmitting: false,
            concurrentlySchedulableTaskCount: 1,
            useCompression: true,
            batchSettings: {
                count: '',
                size: '',
                duration: ''
            }
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [EditRemotePortComponent, NoopAnimationsModule],
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
        fixture = TestBed.createComponent(EditRemotePortComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
