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

import { MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { CreateRegistryClient } from './create-registry-client.component';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';
import { DocumentedType } from '../../../../../state/shared';
import { MatIconTestingModule } from '@angular/material/icon/testing';
import { MockComponent } from 'ng-mocks';
import { ExtensionCreation } from '../../../../../ui/common/extension-creation/extension-creation.component';
import { of } from 'rxjs';

describe('CreateRegistryClient', () => {
    let component: CreateRegistryClient;
    let fixture: ComponentFixture<CreateRegistryClient>;

    const registryClientTypes: DocumentedType[] = [
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
    ];

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [
                CreateRegistryClient,
                NoopAnimationsModule,
                MatIconTestingModule,
                MockComponent(ExtensionCreation)
            ],
            providers: [
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: jest.fn()
                    }
                },
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(CreateRegistryClient);
        component = fixture.componentInstance;
        component.registryClientTypes$ = of(registryClientTypes);
        component.registryClientTypesLoadingStatus$ = of('success');
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
