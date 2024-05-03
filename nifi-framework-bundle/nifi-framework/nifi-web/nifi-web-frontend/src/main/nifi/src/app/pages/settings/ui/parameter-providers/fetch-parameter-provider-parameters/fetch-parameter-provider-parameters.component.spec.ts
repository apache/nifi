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

import { FetchParameterProviderParameters } from './fetch-parameter-provider-parameters.component';
import { FetchParameterProviderDialogRequest } from '../../../state/parameter-providers';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { initialParameterProvidersState } from '../../../state/parameter-providers/parameter-providers.reducer';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';

describe('FetchParameterProviderParameters', () => {
    let component: FetchParameterProviderParameters;
    let fixture: ComponentFixture<FetchParameterProviderParameters>;

    const data: FetchParameterProviderDialogRequest = {
        id: 'id',
        parameterProvider: {
            revision: {
                clientId: '36ba1cc1-018d-1000-bc2c-787bc552d63d',
                version: 6
            },
            id: '369487d7-018d-1000-817a-1d8d9a8f4a91',
            uri: 'https://localhost:8443/nifi-api/parameter-providers/369487d7-018d-1000-817a-1d8d9a8f4a91',
            permissions: {
                canRead: true,
                canWrite: true
            },
            bulletins: [],
            component: {
                id: '369487d7-018d-1000-817a-1d8d9a8f4a91',
                name: 'Group 1 - FileParameterProvider',
                type: 'org.apache.nifi.parameter.FileParameterProvider',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-standard-nar',
                    version: '2.0.0-SNAPSHOT'
                },
                comments: '',
                persistsState: false,
                restricted: true,
                deprecated: false,
                multipleVersionsAvailable: false,
                properties: {
                    'parameter-group-directories': '/Users/rfellows/tmp/parameterProviders/group1',
                    'parameter-value-byte-limit': '256 B',
                    'parameter-value-encoding': 'plaintext'
                },
                affectedComponents: [],
                descriptors: {
                    'parameter-group-directories': {
                        name: 'parameter-group-directories',
                        displayName: 'Parameter Group Directories',
                        description:
                            'A comma-separated list of directory absolute paths that will map to named parameter groups.  Each directory that contains files will map to a parameter group, named after the innermost directory in the path.  Files inside the directory will map to parameter names, whose values are the content of each respective file.',
                        required: true,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    'parameter-value-byte-limit': {
                        name: 'parameter-value-byte-limit',
                        displayName: 'Parameter Value Byte Limit',
                        description:
                            'The maximum byte size of a parameter value.  Since parameter values are pulled from the contents of files, this is a safeguard that can prevent memory issues if large files are included.',
                        defaultValue: '256 B',
                        required: true,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    'parameter-value-encoding': {
                        name: 'parameter-value-encoding',
                        displayName: 'Parameter Value Encoding',
                        description: 'Indicates how parameter values are encoded inside Parameter files.',
                        defaultValue: 'base64',
                        allowableValues: [
                            {
                                allowableValue: {
                                    displayName: 'Base64',
                                    value: 'base64',
                                    description:
                                        'File content is Base64-encoded, and will be decoded before providing the value as a Parameter.'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Plain text',
                                    value: 'plaintext',
                                    description:
                                        'File content is not encoded, and will be provided directly as a Parameter value.'
                                },
                                canRead: true
                            }
                        ],
                        required: true,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    }
                },
                parameterGroupConfigurations: [
                    {
                        groupName: 'group1',
                        parameterContextName: 'group1',
                        parameterSensitivities: {
                            bytes: 'NON_SENSITIVE',
                            password: 'SENSITIVE',
                            username: 'NON_SENSITIVE'
                        },
                        synchronized: true
                    }
                ],
                referencingParameterContexts: [
                    {
                        id: '3716e18d-018d-1000-f203-4f6d571d572e',
                        permissions: {
                            canRead: true,
                            canWrite: true
                        },
                        bulletins: [],
                        component: {
                            id: '3716e18d-018d-1000-f203-4f6d571d572e',
                            name: 'group1'
                        }
                    }
                ],
                validationStatus: 'VALID',
                extensionMissing: false
            }
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [FetchParameterProviderParameters, NoopAnimationsModule],
            providers: [
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: data
                },
                provideMockStore({
                    initialState: initialParameterProvidersState
                }),
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: jest.fn()
                    }
                }
            ]
        });
        fixture = TestBed.createComponent(FetchParameterProviderParameters);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
