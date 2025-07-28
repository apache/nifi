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

import { EditControllerService } from './edit-controller-service.component';
import { EditControllerServiceDialogRequest } from '../../../../state/shared';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';

import { MockComponent } from 'ng-mocks';
import { ContextErrorBanner } from '../../context-error-banner/context-error-banner.component';
import { Client } from '../../../../service/client.service';
import { Revision } from '@nifi/shared';

describe('EditControllerService', () => {
    let component: EditControllerService;
    let fixture: ComponentFixture<EditControllerService>;

    const data: EditControllerServiceDialogRequest = {
        id: 'ca577df7-018b-1000-6182-90ea0c5d3337',
        controllerService: {
            revision: {
                version: 0
            },
            id: 'ca577df7-018b-1000-6182-90ea0c5d3337',
            uri: 'https://localhost:4200/nifi-api/controller-services/ca577df7-018b-1000-6182-90ea0c5d3337',
            permissions: {
                canRead: true,
                canWrite: true
            },
            bulletins: [],
            component: {
                id: 'ca577df7-018b-1000-6182-90ea0c5d3337',
                name: 'AmazonGlueSchemaRegistry',
                type: 'org.apache.nifi.aws.schemaregistry.AmazonGlueSchemaRegistry',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-aws-nar',
                    version: '2.0.0-SNAPSHOT'
                },
                controllerServiceApis: [
                    {
                        type: 'org.apache.nifi.schemaregistry.services.SchemaRegistry',
                        bundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-standard-services-api-nar',
                            version: '2.0.0-SNAPSHOT'
                        }
                    }
                ],
                state: 'DISABLED',
                persistsState: false,
                restricted: false,
                deprecated: false,
                multipleVersionsAvailable: false,
                supportsSensitiveDynamicProperties: false,
                properties: {
                    'schema-registry-name': null,
                    region: 'us-west-2',
                    'communications-timeout': '30 secs',
                    'cache-size': '1000',
                    'cache-expiration': '1 hour',
                    'aws-credentials-provider-service': null,
                    'proxy-configuration-service': null,
                    'ssl-context-service': null
                },
                descriptors: {
                    'schema-registry-name': {
                        name: 'schema-registry-name',
                        displayName: 'Schema Registry Name',
                        description: 'The name of the Schema Registry',
                        required: true,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    region: {
                        name: 'region',
                        displayName: 'Region',
                        description: 'The region of the cloud resources',
                        defaultValue: 'us-west-2',
                        allowableValues: [
                            {
                                allowableValue: {
                                    displayName: 'AWS GovCloud (US-East)',
                                    value: 'us-gov-east-1',
                                    description: 'AWS Region Code : us-gov-east-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'AWS GovCloud (US-West)',
                                    value: 'us-gov-west-1',
                                    description: 'AWS Region Code : us-gov-west-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Africa (Cape Town)',
                                    value: 'af-south-1',
                                    description: 'AWS Region Code : af-south-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Asia Pacific (Hong Kong)',
                                    value: 'ap-east-1',
                                    description: 'AWS Region Code : ap-east-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Asia Pacific (Hyderabad)',
                                    value: 'ap-south-2',
                                    description: 'AWS Region Code : ap-south-2'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Asia Pacific (Jakarta)',
                                    value: 'ap-southeast-3',
                                    description: 'AWS Region Code : ap-southeast-3'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Asia Pacific (Melbourne)',
                                    value: 'ap-southeast-4',
                                    description: 'AWS Region Code : ap-southeast-4'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Asia Pacific (Mumbai)',
                                    value: 'ap-south-1',
                                    description: 'AWS Region Code : ap-south-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Asia Pacific (Osaka)',
                                    value: 'ap-northeast-3',
                                    description: 'AWS Region Code : ap-northeast-3'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Asia Pacific (Seoul)',
                                    value: 'ap-northeast-2',
                                    description: 'AWS Region Code : ap-northeast-2'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Asia Pacific (Singapore)',
                                    value: 'ap-southeast-1',
                                    description: 'AWS Region Code : ap-southeast-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Asia Pacific (Sydney)',
                                    value: 'ap-southeast-2',
                                    description: 'AWS Region Code : ap-southeast-2'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Asia Pacific (Tokyo)',
                                    value: 'ap-northeast-1',
                                    description: 'AWS Region Code : ap-northeast-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Canada (Central)',
                                    value: 'ca-central-1',
                                    description: 'AWS Region Code : ca-central-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'China (Beijing)',
                                    value: 'cn-north-1',
                                    description: 'AWS Region Code : cn-north-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'China (Ningxia)',
                                    value: 'cn-northwest-1',
                                    description: 'AWS Region Code : cn-northwest-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Europe (Frankfurt)',
                                    value: 'eu-central-1',
                                    description: 'AWS Region Code : eu-central-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Europe (Ireland)',
                                    value: 'eu-west-1',
                                    description: 'AWS Region Code : eu-west-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Europe (London)',
                                    value: 'eu-west-2',
                                    description: 'AWS Region Code : eu-west-2'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Europe (Milan)',
                                    value: 'eu-south-1',
                                    description: 'AWS Region Code : eu-south-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Europe (Paris)',
                                    value: 'eu-west-3',
                                    description: 'AWS Region Code : eu-west-3'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Europe (Spain)',
                                    value: 'eu-south-2',
                                    description: 'AWS Region Code : eu-south-2'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Europe (Stockholm)',
                                    value: 'eu-north-1',
                                    description: 'AWS Region Code : eu-north-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Europe (Zurich)',
                                    value: 'eu-central-2',
                                    description: 'AWS Region Code : eu-central-2'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Israel (Tel Aviv)',
                                    value: 'il-central-1',
                                    description: 'AWS Region Code : il-central-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Middle East (Bahrain)',
                                    value: 'me-south-1',
                                    description: 'AWS Region Code : me-south-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Middle East (UAE)',
                                    value: 'me-central-1',
                                    description: 'AWS Region Code : me-central-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'South America (Sao Paulo)',
                                    value: 'sa-east-1',
                                    description: 'AWS Region Code : sa-east-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'US East (N. Virginia)',
                                    value: 'us-east-1',
                                    description: 'AWS Region Code : us-east-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'US East (Ohio)',
                                    value: 'us-east-2',
                                    description: 'AWS Region Code : us-east-2'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'US ISO East',
                                    value: 'us-iso-east-1',
                                    description: 'AWS Region Code : us-iso-east-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'US ISO WEST',
                                    value: 'us-iso-west-1',
                                    description: 'AWS Region Code : us-iso-west-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'US ISOB East (Ohio)',
                                    value: 'us-isob-east-1',
                                    description: 'AWS Region Code : us-isob-east-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'US West (N. California)',
                                    value: 'us-west-1',
                                    description: 'AWS Region Code : us-west-1'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'US West (Oregon)',
                                    value: 'us-west-2',
                                    description: 'AWS Region Code : us-west-2'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'aws-cn-global',
                                    value: 'aws-cn-global',
                                    description: 'AWS Region Code : aws-cn-global'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'aws-global',
                                    value: 'aws-global',
                                    description: 'AWS Region Code : aws-global'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'aws-iso-b-global',
                                    value: 'aws-iso-b-global',
                                    description: 'AWS Region Code : aws-iso-b-global'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'aws-iso-global',
                                    value: 'aws-iso-global',
                                    description: 'AWS Region Code : aws-iso-global'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'aws-us-gov-global',
                                    value: 'aws-us-gov-global',
                                    description: 'AWS Region Code : aws-us-gov-global'
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
                    },
                    'communications-timeout': {
                        name: 'communications-timeout',
                        displayName: 'Communications Timeout',
                        description:
                            'Specifies how long to wait to receive data from the Schema Registry before considering the communications a failure',
                        defaultValue: '30 secs',
                        required: true,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    'cache-size': {
                        name: 'cache-size',
                        displayName: 'Cache Size',
                        description: 'Specifies how many Schemas should be cached from the Schema Registry',
                        defaultValue: '1000',
                        required: true,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    'cache-expiration': {
                        name: 'cache-expiration',
                        displayName: 'Cache Expiration',
                        description:
                            'Specifies how long a Schema that is cached should remain in the cache. Once this time period elapses, a cached version of a schema will no longer be used, and the service will have to communicate with the Schema Registry again in order to obtain the schema.',
                        defaultValue: '1 hour',
                        required: true,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    'aws-credentials-provider-service': {
                        name: 'aws-credentials-provider-service',
                        displayName: 'AWS Credentials Provider Service',
                        description: 'The Controller Service that is used to obtain AWS credentials provider',
                        allowableValues: [],
                        required: false,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        identifiesControllerService:
                            'org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService',
                        identifiesControllerServiceBundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-aws-service-api-nar',
                            version: '2.0.0-SNAPSHOT'
                        },
                        dependencies: []
                    },
                    'proxy-configuration-service': {
                        name: 'proxy-configuration-service',
                        displayName: 'Proxy Configuration Service',
                        description:
                            'Specifies the Proxy Configuration Controller Service to proxy network requests. If set, it supersedes proxy settings configured per component. Supported proxies: HTTP + AuthN',
                        allowableValues: [],
                        required: false,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        identifiesControllerService: 'org.apache.nifi.proxy.ProxyConfigurationService',
                        identifiesControllerServiceBundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-standard-services-api-nar',
                            version: '2.0.0-SNAPSHOT'
                        },
                        dependencies: []
                    },
                    'ssl-context-service': {
                        name: 'ssl-context-service',
                        displayName: 'SSL Context Service',
                        description:
                            'Specifies an optional SSL Context Service that, if provided, will be used to create connections',
                        allowableValues: [
                            {
                                allowableValue: {
                                    displayName: 'StandardRestrictedSSLContextService',
                                    value: 'd84c4e53-018b-1000-38ff-b2f1002be8a0'
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
                referencingComponents: [],
                validationErrors: ["'Schema Registry Name' is invalid because Schema Registry Name is required"],
                validationStatus: 'INVALID',
                bulletinLevel: 'WARN',
                extensionMissing: false
            },
            operatePermissions: {
                canRead: false,
                canWrite: false
            },
            status: {
                runStatus: 'DISABLED',
                validationStatus: 'INVALID'
            }
        }
    };

    const mockRevision: Revision = {
        clientId: 'client-id',
        version: 1
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [EditControllerService, MockComponent(ContextErrorBanner), NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: jest.fn()
                    }
                },
                { provide: MatDialogRef, useValue: null },
                {
                    provide: Client,
                    useValue: {
                        getRevision: () => mockRevision
                    }
                },
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: () => true
                    }
                }
            ]
        });
        fixture = TestBed.createComponent(EditControllerService);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should dispatch edit action on form submission', () => {
        const mockFormData = {
            name: 'service-name',
            bulletinLevel: 'DEBUG',
            comments: 'service-comments',
            properties: []
        };

        jest.spyOn(component.editControllerService, 'next');
        component.editControllerServiceForm.setValue(mockFormData);
        component.submitForm();

        expect(component.editControllerService.next).toHaveBeenCalledWith({
            payload: {
                revision: mockRevision,
                disconnectedNodeAcknowledged: true,
                component: {
                    id: data.id,
                    name: mockFormData.name,
                    bulletinLevel: mockFormData.bulletinLevel,
                    comments: mockFormData.comments
                }
            }
        });
    });
});
