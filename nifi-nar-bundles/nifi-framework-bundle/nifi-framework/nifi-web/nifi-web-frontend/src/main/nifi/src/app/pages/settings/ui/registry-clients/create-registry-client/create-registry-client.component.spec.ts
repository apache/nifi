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

import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { CreateRegistryClient } from './create-registry-client.component';
import { CreateRegistryClientDialogRequest } from '../../../state/registry-clients';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';

describe('CreateRegistryClient', () => {
    let component: CreateRegistryClient;
    let fixture: ComponentFixture<CreateRegistryClient>;

    const data: CreateRegistryClientDialogRequest = {
        registryClientTypes: [
            {
                type: 'org.apache.nifi.registry.flow.NifiRegistryFlowRegistryClient',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-flow-registry-client-nar',
                    version: '2.0.0-SNAPSHOT'
                },
                restricted: false,
                tags: []
            }
        ]
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [CreateRegistryClient, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: jest.fn()
                    }
                }
            ]
        });
        fixture = TestBed.createComponent(CreateRegistryClient);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
