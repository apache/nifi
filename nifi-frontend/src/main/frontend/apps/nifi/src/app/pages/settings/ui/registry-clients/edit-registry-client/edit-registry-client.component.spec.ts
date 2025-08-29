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

import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { EditRegistryClient } from './edit-registry-client.component';
import { EditRegistryClientDialogRequest } from '../../../state/registry-clients';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';

import { MockComponent } from 'ng-mocks';
import { ContextErrorBanner } from '../../../../../ui/common/context-error-banner/context-error-banner.component';

describe('EditRegistryClient', () => {
    let component: EditRegistryClient;
    let fixture: ComponentFixture<EditRegistryClient>;

    const data: EditRegistryClientDialogRequest = {
        registryClient: {
            revision: {
                clientId: 'fdbbc975-5fd5-4dbf-9308-432d75d20c04',
                version: 2
            },
            id: '454cab42-018c-1000-6f9f-3603643c504c',
            uri: 'https://localhost:4200/nifi-api/controller/registry-clients/454cab42-018c-1000-6f9f-3603643c504c',
            permissions: {
                canRead: true,
                canWrite: true
            },
            component: {
                id: '454cab42-018c-1000-6f9f-3603643c504c',
                name: 'registry',
                description: '',
                type: 'org.apache.nifi.registry.flow.NifiRegistryFlowRegistryClient',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-flow-registry-client-nar',
                    version: '2.0.0-SNAPSHOT'
                },
                properties: {
                    url: null,
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
                                    displayName: 'StandardRestrictedSSLContextService',
                                    value: '45b3d2bf-018c-1000-f99a-fd441220c9d7'
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
                validationErrors: ["'URL' is invalid because URL is required"],
                validationStatus: 'INVALID',
                multipleVersionsAvailable: false,
                extensionMissing: false
            }
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [EditRegistryClient, MockComponent(ContextErrorBanner), NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: jest.fn()
                    }
                },
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(EditRegistryClient);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
