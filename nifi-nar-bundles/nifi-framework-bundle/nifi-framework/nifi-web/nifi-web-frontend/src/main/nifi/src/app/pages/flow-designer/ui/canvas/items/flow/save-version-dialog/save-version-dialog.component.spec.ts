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

import { SaveVersionDialog } from './save-version-dialog.component';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { SaveVersionDialogRequest } from '../../../../../state/flow';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/flow/flow.reducer';
import { EMPTY } from 'rxjs';
import { Signal } from '@angular/core';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

describe('SaveVersionDialog', () => {
    let component: SaveVersionDialog;
    let fixture: ComponentFixture<SaveVersionDialog>;

    const data: SaveVersionDialogRequest = {
        processGroupId: '5752a5ae-018d-1000-0990-c3709f5466f3',
        revision: {
            version: 0
        },
        registryClients: [
            {
                revision: {
                    version: 0
                },
                id: '80441509-018e-1000-12b2-d70361a7f661',
                uri: 'https://localhost:4200/nifi-api/controller/registry-clients/80441509-018e-1000-12b2-d70361a7f661',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: '80441509-018e-1000-12b2-d70361a7f661',
                    name: 'Local Registry',
                    description: '',
                    type: 'org.apache.nifi.registry.flow.NifiRegistryFlowRegistryClient',
                    bundle: {
                        group: 'org.apache.nifi',
                        artifact: 'nifi-flow-registry-client-nar',
                        version: '2.0.0-SNAPSHOT'
                    },
                    properties: {
                        url: 'http://localhost:18080/nifi-registry',
                        'ssl-context-service': null
                    },
                    descriptors: {
                        url: {
                            name: 'url',
                            displayName: 'URL',
                            description: 'URL of the NiFi Registry',
                            required: true,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: false,
                            expressionLanguageScope: 'Not Supported',
                            dependencies: []
                        },
                        'ssl-context-service': {
                            name: 'ssl-context-service',
                            displayName: 'SSL Context Service',
                            description: 'Specifies the SSL Context Service to use for communicating with NiFiRegistry',
                            allowableValues: [
                                {
                                    allowableValue: {
                                        displayName: 'StandardSSLContextService',
                                        value: '5c272e23-018d-1000-72ef-f31b82cda378'
                                    },
                                    canRead: true
                                }
                            ],
                            required: false,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: false,
                            expressionLanguageScope: 'Not Supported',
                            identifiesControllerService: 'org.apache.nifi.ssl.SSLContextService',
                            identifiesControllerServiceBundle: {
                                group: 'org.apache.nifi',
                                artifact: 'nifi-standard-services-api-nar',
                                version: '2.0.0-SNAPSHOT'
                            },
                            dependencies: []
                        }
                    },
                    supportsSensitiveDynamicProperties: false,
                    restricted: false,
                    deprecated: false,
                    validationStatus: 'VALID',
                    multipleVersionsAvailable: false,
                    extensionMissing: false
                }
            }
        ]
    };

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [SaveVersionDialog, MatDialogModule, NoopAnimationsModule],
            providers: [{ provide: MAT_DIALOG_DATA, useValue: data }, provideMockStore({ initialState })]
        }).compileComponents();

        fixture = TestBed.createComponent(SaveVersionDialog);
        component = fixture.componentInstance;
        component.getBuckets = () => {
            return EMPTY;
        };
        component.saving = (() => false) as Signal<boolean>;

        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
