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

import { EditReportingTask } from './edit-reporting-task.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { EditReportingTaskDialogRequest } from '../../../state/reporting-tasks';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';

import { MockComponent } from 'ng-mocks';
import { ContextErrorBanner } from '../../../../../ui/common/context-error-banner/context-error-banner.component';

describe('EditReportingTask', () => {
    let component: EditReportingTask;
    let fixture: ComponentFixture<EditReportingTask>;

    const data: EditReportingTaskDialogRequest = {
        id: 'd5142be7-018c-1000-7105-2b1163fe0355',
        reportingTask: {
            revision: {
                clientId: 'da266443-018c-1000-7dd0-9fee5271e3e1',
                version: 22
            },
            id: 'd5142be7-018c-1000-7105-2b1163fe0355',
            uri: 'https://localhost:8443/nifi-api/reporting-tasks/d5142be7-018c-1000-7105-2b1163fe0355',
            permissions: {
                canRead: true,
                canWrite: true
            },
            bulletins: [],
            component: {
                id: 'd5142be7-018c-1000-7105-2b1163fe0355',
                name: 'SiteToSiteMetricsReportingTask',
                type: 'org.apache.nifi.reporting.SiteToSiteMetricsReportingTask',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-site-to-site-reporting-nar',
                    version: '2.0.0-SNAPSHOT'
                },
                state: 'DISABLED',
                comments: 'iutfiugviugv',
                persistsState: false,
                restricted: false,
                deprecated: false,
                multipleVersionsAvailable: false,
                supportsSensitiveDynamicProperties: false,
                schedulingPeriod: '5 mins',
                schedulingStrategy: 'TIMER_DRIVEN',
                defaultSchedulingPeriod: {
                    TIMER_DRIVEN: '0 sec',
                    CRON_DRIVEN: '* * * * * ?'
                },
                properties: {
                    'Destination URL': null,
                    'Input Port Name': null,
                    'SSL Context Service': null,
                    'Instance URL': 'http://${hostname(true)}:8080/nifi',
                    'Compress Events': 'true',
                    'Communications Timeout': '30 secs',
                    's2s-transport-protocol': 'RAW',
                    's2s-http-proxy-hostname': null,
                    's2s-http-proxy-port': null,
                    's2s-http-proxy-username': null,
                    's2s-http-proxy-password': null,
                    'record-writer': null,
                    'include-null-values': 'false',
                    's2s-metrics-hostname': '${hostname(true)}',
                    's2s-metrics-application-id': 'nifi',
                    's2s-metrics-format': 'ambari-format'
                },
                descriptors: {
                    'Destination URL': {
                        name: 'Destination URL',
                        displayName: 'Destination URL',
                        description:
                            'The URL of the destination NiFi instance or, if clustered, a comma-separated list of address in the format of http(s)://host:port/nifi. This destination URL will only be used to initiate the Site-to-Site connection. The data sent by this reporting task will be load-balanced on all the nodes of the destination (if clustered).',
                        required: true,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: true,
                        expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                        dependencies: []
                    },
                    'Input Port Name': {
                        name: 'Input Port Name',
                        displayName: 'Input Port Name',
                        description: 'The name of the Input Port to deliver data to.',
                        required: true,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: true,
                        expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                        dependencies: []
                    },
                    'SSL Context Service': {
                        name: 'SSL Context Service',
                        displayName: 'SSL Context Service',
                        description:
                            'The SSL Context Service to use when communicating with the destination. If not specified, communications will not be secure.',
                        allowableValues: [
                            {
                                allowableValue: {
                                    displayName: 'StandardRestrictedSSLContextService',
                                    value: 'd636f255-018c-1000-42a8-822c72a22795'
                                },
                                canRead: true
                            }
                        ],
                        required: false,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        identifiesControllerService: 'org.apache.nifi.ssl.RestrictedSSLContextService',
                        identifiesControllerServiceBundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-standard-services-api-nar',
                            version: '2.0.0-SNAPSHOT'
                        },
                        dependencies: []
                    },
                    'Instance URL': {
                        name: 'Instance URL',
                        displayName: 'Instance URL',
                        description: 'The URL of this instance to use in the Content URI of each event.',
                        defaultValue: 'http://${hostname(true)}:8080/nifi',
                        required: true,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: true,
                        expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                        dependencies: []
                    },
                    'Compress Events': {
                        name: 'Compress Events',
                        displayName: 'Compress Events',
                        description: 'Indicates whether or not to compress the data being sent.',
                        defaultValue: 'true',
                        allowableValues: [
                            {
                                allowableValue: {
                                    displayName: 'true',
                                    value: 'true'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'false',
                                    value: 'false'
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
                    'Communications Timeout': {
                        name: 'Communications Timeout',
                        displayName: 'Communications Timeout',
                        description:
                            'Specifies how long to wait to a response from the destination before deciding that an error has occurred and canceling the transaction',
                        defaultValue: '30 secs',
                        required: true,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    's2s-transport-protocol': {
                        name: 's2s-transport-protocol',
                        displayName: 'Transport Protocol',
                        description: 'Specifies which transport protocol to use for Site-to-Site communication.',
                        defaultValue: 'RAW',
                        allowableValues: [
                            {
                                allowableValue: {
                                    displayName: 'RAW',
                                    value: 'RAW'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'HTTP',
                                    value: 'HTTP'
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
                    's2s-http-proxy-hostname': {
                        name: 's2s-http-proxy-hostname',
                        displayName: 'HTTP Proxy hostname',
                        description:
                            "Specify the proxy server's hostname to use. If not specified, HTTP traffics are sent directly to the target NiFi instance.",
                        required: false,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    's2s-http-proxy-port': {
                        name: 's2s-http-proxy-port',
                        displayName: 'HTTP Proxy port',
                        description:
                            "Specify the proxy server's port number, optional. If not specified, default port 80 will be used.",
                        required: false,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    's2s-http-proxy-username': {
                        name: 's2s-http-proxy-username',
                        displayName: 'HTTP Proxy username',
                        description: 'Specify an user name to connect to the proxy server, optional.',
                        required: false,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    's2s-http-proxy-password': {
                        name: 's2s-http-proxy-password',
                        displayName: 'HTTP Proxy password',
                        description: 'Specify an user password to connect to the proxy server, optional.',
                        required: false,
                        sensitive: true,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    'record-writer': {
                        name: 'record-writer',
                        displayName: 'Record Writer',
                        description: 'Specifies the Controller Service to use for writing out the records.',
                        allowableValues: [],
                        required: false,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        identifiesControllerService: 'org.apache.nifi.serialization.RecordSetWriterFactory',
                        identifiesControllerServiceBundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-standard-services-api-nar',
                            version: '2.0.0-SNAPSHOT'
                        },
                        dependencies: []
                    },
                    'include-null-values': {
                        name: 'include-null-values',
                        displayName: 'Include Null Values',
                        description: 'Indicate if null values should be included in records. Default will be false',
                        defaultValue: 'false',
                        allowableValues: [
                            {
                                allowableValue: {
                                    displayName: 'true',
                                    value: 'true'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'false',
                                    value: 'false'
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
                    's2s-metrics-hostname': {
                        name: 's2s-metrics-hostname',
                        displayName: 'Hostname',
                        description: 'The Hostname of this NiFi instance to be included in the metrics',
                        defaultValue: '${hostname(true)}',
                        required: true,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: true,
                        expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                        dependencies: []
                    },
                    's2s-metrics-application-id': {
                        name: 's2s-metrics-application-id',
                        displayName: 'Application ID',
                        description: 'The Application ID to be included in the metrics',
                        defaultValue: 'nifi',
                        required: true,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: true,
                        expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                        dependencies: []
                    },
                    's2s-metrics-format': {
                        name: 's2s-metrics-format',
                        displayName: 'Output Format',
                        description:
                            'The output format that will be used for the metrics. If Record Format is selected, a Record Writer must be provided. If Ambari Format is selected, the Record Writer property should be empty.',
                        defaultValue: 'ambari-format',
                        allowableValues: [
                            {
                                allowableValue: {
                                    displayName: 'Ambari Format',
                                    value: 'ambari-format',
                                    description:
                                        'Metrics will be formatted according to the Ambari Metrics API. See Additional Details in Usage documentation.'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'Record Format',
                                    value: 'record-format',
                                    description:
                                        'Metrics will be formatted using the Record Writer property of this reporting task. See Additional Details in Usage documentation to have the description of the default schema.'
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
                validationErrors: [
                    "'Input Port Name' is invalid because Input Port Name is required",
                    "'Destination URL' is invalid because Destination URL is required"
                ],
                validationStatus: 'INVALID',
                activeThreadCount: 0,
                extensionMissing: false
            },
            operatePermissions: {
                canRead: true,
                canWrite: true
            },
            status: {
                runStatus: 'DISABLED',
                validationStatus: 'INVALID',
                activeThreadCount: 0
            }
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [EditReportingTask, MockComponent(ContextErrorBanner), NoopAnimationsModule],
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
        fixture = TestBed.createComponent(EditReportingTask);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
