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

import { EditProcessor } from './edit-processor.component';
import { EditComponentDialogRequest } from '../../../../../state/flow';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { ComponentType } from '@nifi/shared';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ClusterConnectionService } from '../../../../../../../service/cluster-connection.service';

import { MockComponent } from 'ng-mocks';
import { CanvasUtils } from '../../../../../service/canvas-utils.service';
import { ContextErrorBanner } from '../../../../../../../ui/common/context-error-banner/context-error-banner.component';

describe('EditProcessor', () => {
    let component: EditProcessor;
    let fixture: ComponentFixture<EditProcessor>;

    const data: EditComponentDialogRequest = {
        type: ComponentType.Processor,
        uri: 'https://localhost:4200/nifi-api/processors/d90ac264-018b-1000-1827-a86c8156fd9e',
        entity: {
            revision: {
                clientId: 'd8e8a955-018b-1000-915e-a59d0e7933ef',
                version: 1
            },
            id: 'd90ac264-018b-1000-1827-a86c8156fd9e',
            uri: 'https://localhost:4200/nifi-api/processors/d90ac264-018b-1000-1827-a86c8156fd9e',
            position: {
                x: 554.8456153681711,
                y: -690.0400701011749
            },
            permissions: {
                canRead: true,
                canWrite: true
            },
            bulletins: [],
            component: {
                id: 'd90ac264-018b-1000-1827-a86c8156fd9e',
                parentGroupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                position: {
                    x: 554.8456153681711,
                    y: -690.0400701011749
                },
                name: 'ConsumeKafka_2_6',
                type: 'org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-kafka-2-6-nar',
                    version: '2.0.0-SNAPSHOT'
                },
                state: 'STOPPED',
                style: {},
                relationships: [
                    {
                        name: 'success',
                        description:
                            'FlowFiles received from Kafka. Depending on demarcation strategy it is a flow file per message or a bundle of messages grouped by topic and partition.',
                        autoTerminate: false,
                        retry: false
                    }
                ],
                supportsParallelProcessing: true,
                supportsBatching: false,
                supportsSensitiveDynamicProperties: false,
                persistsState: false,
                restricted: false,
                deprecated: false,
                executionNodeRestricted: false,
                multipleVersionsAvailable: false,
                inputRequirement: 'INPUT_FORBIDDEN',
                config: {
                    properties: {
                        'bootstrap.servers': 'localhost:9092',
                        topic: null,
                        topic_type: 'names',
                        'group.id': null,
                        'Commit Offsets': 'true',
                        'max-uncommit-offset-wait': '1 secs',
                        'honor-transactions': 'true',
                        'message-demarcator': null,
                        'separate-by-key': 'false',
                        'security.protocol': 'PLAINTEXT',
                        'sasl.mechanism': 'GSSAPI',
                        'kerberos-user-service': null,
                        'sasl.kerberos.service.name': null,
                        'sasl.username': null,
                        'sasl.password': null,
                        'sasl.token.auth': 'false',
                        'aws.profile.name': null,
                        'ssl.context.service': null,
                        'key-attribute-encoding': 'utf-8',
                        'auto.offset.reset': 'latest',
                        'message-header-encoding': 'UTF-8',
                        'header-name-regex': null,
                        'max.poll.records': '10000',
                        'Communications Timeout': '60 secs'
                    },
                    descriptors: {
                        'bootstrap.servers': {
                            name: 'bootstrap.servers',
                            displayName: 'Kafka Brokers',
                            description: 'Comma-separated list of Kafka Brokers in the format host:port',
                            defaultValue: 'localhost:9092',
                            required: true,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: true,
                            expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                            dependencies: []
                        },
                        topic: {
                            name: 'topic',
                            displayName: 'Topic Name(s)',
                            description:
                                'The name of the Kafka Topic(s) to pull from. More than one can be supplied if comma separated.',
                            required: true,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: true,
                            expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                            dependencies: []
                        },
                        topic_type: {
                            name: 'topic_type',
                            displayName: 'Topic Name Format',
                            description:
                                'Specifies whether the Topic(s) provided are a comma separated list of names or a single regular expression',
                            defaultValue: 'names',
                            allowableValues: [
                                {
                                    allowableValue: {
                                        displayName: 'names',
                                        value: 'names',
                                        description: 'Topic is a full topic name or comma separated list of names'
                                    },
                                    canRead: true
                                },
                                {
                                    allowableValue: {
                                        displayName: 'pattern',
                                        value: 'pattern',
                                        description: 'Topic is a regex using the Java Pattern syntax'
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
                        'group.id': {
                            name: 'group.id',
                            displayName: 'Group ID',
                            description:
                                "A Group ID is used to identify consumers that are within the same consumer group. Corresponds to Kafka's 'group.id' property.",
                            required: true,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: true,
                            expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                            dependencies: []
                        },
                        'Commit Offsets': {
                            name: 'Commit Offsets',
                            displayName: 'Commit Offsets',
                            description:
                                "Specifies whether or not this Processor should commit the offsets to Kafka after receiving messages. Typically, we want this value set to true so that messages that are received are not duplicated. However, in certain scenarios, we may want to avoid committing the offsets, that the data can be processed and later acknowledged by PublishKafkaRecord in order to provide Exactly Once semantics. See Processor's Usage / Additional Details for more information.",
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
                            required: false,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: false,
                            expressionLanguageScope: 'Not Supported',
                            dependencies: []
                        },
                        'max-uncommit-offset-wait': {
                            name: 'max-uncommit-offset-wait',
                            displayName: 'Max Uncommitted Time',
                            description:
                                "Specifies the maximum amount of time allowed to pass before offsets must be committed. This value impacts how often offsets will be committed.  Committing offsets less often increases throughput but also increases the window of potential data duplication in the event of a rebalance or JVM restart between commits.  This value is also related to maximum poll records and the use of a message demarcator.  When using a message demarcator we can have far more uncommitted messages than when we're not as there is much less for us to keep track of in memory.",
                            defaultValue: '1 secs',
                            required: false,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: false,
                            expressionLanguageScope: 'Not Supported',
                            dependencies: [
                                {
                                    propertyName: 'Commit Offsets',
                                    dependentValues: ['true']
                                }
                            ]
                        },
                        'honor-transactions': {
                            name: 'honor-transactions',
                            displayName: 'Honor Transactions',
                            description:
                                'Specifies whether or not NiFi should honor transactional guarantees when communicating with Kafka. If false, the Processor will use an "isolation level" of read_uncomitted. This means that messages will be received as soon as they are written to Kafka but will be pulled, even if the producer cancels the transactions. If this value is true, NiFi will not receive any messages for which the producer\'s transaction was canceled, but this can result in some latency since the consumer must wait for the producer to finish its entire transaction instead of pulling as the messages become available.',
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
                        'message-demarcator': {
                            name: 'message-demarcator',
                            displayName: 'Message Demarcator',
                            description:
                                "Since KafkaConsumer receives messages in batches, you have an option to output FlowFiles which contains all Kafka messages in a single batch for a given topic and partition and this property allows you to provide a string (interpreted as UTF-8) to use for demarcating apart multiple Kafka messages. This is an optional property and if not provided each Kafka message received will result in a single FlowFile which  time it is triggered. To enter special character such as 'new line' use CTRL+Enter or Shift+Enter depending on the OS",
                            required: false,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: true,
                            expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                            dependencies: []
                        },
                        'separate-by-key': {
                            name: 'separate-by-key',
                            displayName: 'Separate By Key',
                            description:
                                'If true, and the <Message Demarcator> property is set, two messages will only be added to the same FlowFile if both of the Kafka Messages have identical keys.',
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
                            required: false,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: false,
                            expressionLanguageScope: 'Not Supported',
                            dependencies: []
                        },
                        'security.protocol': {
                            name: 'security.protocol',
                            displayName: 'Security Protocol',
                            description:
                                'Security protocol used to communicate with brokers. Corresponds to Kafka Client security.protocol property',
                            defaultValue: 'PLAINTEXT',
                            allowableValues: [
                                {
                                    allowableValue: {
                                        displayName: 'PLAINTEXT',
                                        value: 'PLAINTEXT'
                                    },
                                    canRead: true
                                },
                                {
                                    allowableValue: {
                                        displayName: 'SSL',
                                        value: 'SSL'
                                    },
                                    canRead: true
                                },
                                {
                                    allowableValue: {
                                        displayName: 'SASL_PLAINTEXT',
                                        value: 'SASL_PLAINTEXT'
                                    },
                                    canRead: true
                                },
                                {
                                    allowableValue: {
                                        displayName: 'SASL_SSL',
                                        value: 'SASL_SSL'
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
                        'sasl.mechanism': {
                            name: 'sasl.mechanism',
                            displayName: 'SASL Mechanism',
                            description:
                                'SASL mechanism used for authentication. Corresponds to Kafka Client sasl.mechanism property',
                            defaultValue: 'GSSAPI',
                            allowableValues: [
                                {
                                    allowableValue: {
                                        displayName: 'GSSAPI',
                                        value: 'GSSAPI',
                                        description: 'General Security Services API for Kerberos authentication'
                                    },
                                    canRead: true
                                },
                                {
                                    allowableValue: {
                                        displayName: 'PLAIN',
                                        value: 'PLAIN',
                                        description: 'Plain username and password authentication'
                                    },
                                    canRead: true
                                },
                                {
                                    allowableValue: {
                                        displayName: 'SCRAM-SHA-256',
                                        value: 'SCRAM-SHA-256',
                                        description:
                                            'Salted Challenge Response Authentication Mechanism using SHA-512 with username and password'
                                    },
                                    canRead: true
                                },
                                {
                                    allowableValue: {
                                        displayName: 'SCRAM-SHA-512',
                                        value: 'SCRAM-SHA-512',
                                        description:
                                            'Salted Challenge Response Authentication Mechanism using SHA-256 with username and password'
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
                        'kerberos-user-service': {
                            name: 'kerberos-user-service',
                            displayName: 'Kerberos User Service',
                            description: 'Service supporting user authentication with Kerberos',
                            allowableValues: [],
                            required: false,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: false,
                            expressionLanguageScope: 'Not Supported',
                            identifiesControllerService: 'org.apache.nifi.kerberos.SelfContainedKerberosUserService',
                            identifiesControllerServiceBundle: {
                                group: 'org.apache.nifi',
                                artifact: 'nifi-standard-services-api-nar',
                                version: '2.0.0-SNAPSHOT'
                            },
                            dependencies: []
                        },
                        'sasl.kerberos.service.name': {
                            name: 'sasl.kerberos.service.name',
                            displayName: 'Kerberos Service Name',
                            description:
                                'The service name that matches the primary name of the Kafka server configured in the broker JAAS configuration',
                            required: false,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: true,
                            expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                            dependencies: []
                        },
                        'sasl.username': {
                            name: 'sasl.username',
                            displayName: 'Username',
                            description:
                                'Username provided with configured password when using PLAIN or SCRAM SASL Mechanisms',
                            required: false,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: true,
                            expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                            dependencies: [
                                {
                                    propertyName: 'sasl.mechanism',
                                    dependentValues: ['PLAIN', 'SCRAM-SHA-512', 'SCRAM-SHA-256']
                                }
                            ]
                        },
                        'sasl.password': {
                            name: 'sasl.password',
                            displayName: 'Password',
                            description:
                                'Password provided with configured username when using PLAIN or SCRAM SASL Mechanisms',
                            required: false,
                            sensitive: true,
                            dynamic: false,
                            supportsEl: true,
                            expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                            dependencies: [
                                {
                                    propertyName: 'sasl.mechanism',
                                    dependentValues: ['PLAIN', 'SCRAM-SHA-512', 'SCRAM-SHA-256']
                                }
                            ]
                        },
                        'sasl.token.auth': {
                            name: 'sasl.token.auth',
                            displayName: 'Token Authentication',
                            description: 'Enables or disables Token authentication when using SCRAM SASL Mechanisms',
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
                            required: false,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: false,
                            expressionLanguageScope: 'Not Supported',
                            dependencies: [
                                {
                                    propertyName: 'sasl.mechanism',
                                    dependentValues: ['SCRAM-SHA-512', 'SCRAM-SHA-256']
                                }
                            ]
                        },
                        'aws.profile.name': {
                            name: 'aws.profile.name',
                            displayName: 'AWS Profile Name',
                            description:
                                'The Amazon Web Services Profile to select when multiple profiles are available.',
                            required: false,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: true,
                            expressionLanguageScope: 'Environment variables and FlowFile Attributes',
                            dependencies: [
                                {
                                    propertyName: 'sasl.mechanism',
                                    dependentValues: ['AWS_MSK_IAM']
                                }
                            ]
                        },
                        'ssl.context.service': {
                            name: 'ssl.context.service',
                            displayName: 'SSL Context Service',
                            description: 'Service supporting SSL communication with Kafka brokers',
                            allowableValues: [],
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
                        },
                        'key-attribute-encoding': {
                            name: 'key-attribute-encoding',
                            displayName: 'Key Attribute Encoding',
                            description:
                                "FlowFiles that are emitted have an attribute named 'kafka.key'. This property dictates how the value of the attribute should be encoded.",
                            defaultValue: 'utf-8',
                            allowableValues: [
                                {
                                    allowableValue: {
                                        displayName: 'UTF-8 Encoded',
                                        value: 'utf-8',
                                        description: 'The key is interpreted as a UTF-8 Encoded string.'
                                    },
                                    canRead: true
                                },
                                {
                                    allowableValue: {
                                        displayName: 'Hex Encoded',
                                        value: 'hex',
                                        description:
                                            'The key is interpreted as arbitrary binary data and is encoded using hexadecimal characters with uppercase letters'
                                    },
                                    canRead: true
                                },
                                {
                                    allowableValue: {
                                        displayName: 'Do Not Add Key as Attribute',
                                        value: 'do-not-add',
                                        description: 'The key will not be added as an Attribute'
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
                        'auto.offset.reset': {
                            name: 'auto.offset.reset',
                            displayName: 'Offset Reset',
                            description:
                                "Allows you to manage the condition when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted). Corresponds to Kafka's 'auto.offset.reset' property.",
                            defaultValue: 'latest',
                            allowableValues: [
                                {
                                    allowableValue: {
                                        displayName: 'earliest',
                                        value: 'earliest',
                                        description: 'Automatically reset the offset to the earliest offset'
                                    },
                                    canRead: true
                                },
                                {
                                    allowableValue: {
                                        displayName: 'latest',
                                        value: 'latest',
                                        description: 'Automatically reset the offset to the latest offset'
                                    },
                                    canRead: true
                                },
                                {
                                    allowableValue: {
                                        displayName: 'none',
                                        value: 'none',
                                        description:
                                            "Throw exception to the consumer if no previous offset is found for the consumer's group"
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
                        'message-header-encoding': {
                            name: 'message-header-encoding',
                            displayName: 'Message Header Encoding',
                            description:
                                'Any message header that is found on a Kafka message will be added to the outbound FlowFile as an attribute. This property indicates the Character Encoding to use for deserializing the headers.',
                            defaultValue: 'UTF-8',
                            required: false,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: false,
                            expressionLanguageScope: 'Not Supported',
                            dependencies: []
                        },
                        'header-name-regex': {
                            name: 'header-name-regex',
                            displayName: 'Headers to Add as Attributes (Regex)',
                            description:
                                'A Regular Expression that is matched against all message headers. Any message header whose name matches the regex will be added to the FlowFile as an Attribute. If not specified, no Header values will be added as FlowFile attributes. If two messages have a different value for the same header and that header is selected by the provided regex, then those two messages must be added to different FlowFiles. As a result, users should be cautious about using a regex like ".*" if messages are expected to have header values that are unique per message, such as an identifier or timestamp, because it will prevent NiFi from bundling the messages together efficiently.',
                            required: false,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: false,
                            expressionLanguageScope: 'Not Supported',
                            dependencies: []
                        },
                        'max.poll.records': {
                            name: 'max.poll.records',
                            displayName: 'Max Poll Records',
                            description:
                                'Specifies the maximum number of records Kafka should return in a single poll.',
                            defaultValue: '10000',
                            required: false,
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
                                'Specifies the timeout that the consumer should use when communicating with the Kafka Broker',
                            defaultValue: '60 secs',
                            required: true,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: false,
                            expressionLanguageScope: 'Not Supported',
                            dependencies: []
                        }
                    },
                    schedulingPeriod: '0 sec',
                    schedulingStrategy: 'TIMER_DRIVEN',
                    executionNode: 'ALL',
                    penaltyDuration: '30 sec',
                    yieldDuration: '1 sec',
                    bulletinLevel: 'WARN',
                    runDurationMillis: 0,
                    concurrentlySchedulableTaskCount: 1,
                    autoTerminatedRelationships: [],
                    comments: '',
                    lossTolerant: false,
                    defaultConcurrentTasks: {
                        TIMER_DRIVEN: '1',
                        CRON_DRIVEN: '1'
                    },
                    defaultSchedulingPeriod: {
                        TIMER_DRIVEN: '0 sec',
                        CRON_DRIVEN: '* * * * * ?'
                    },
                    retryCount: 10,
                    retriedRelationships: [],
                    backoffMechanism: 'PENALIZE_FLOWFILE',
                    maxBackoffPeriod: '10 mins'
                },
                validationErrors: [
                    "'Topic Name(s)' is invalid because Topic Name(s) is required",
                    "'Group ID' is invalid because Group ID is required",
                    "'Relationship success' is invalid because Relationship 'success' is not connected to any component and is not auto-terminated"
                ],
                validationStatus: 'INVALID',
                extensionMissing: false
            },
            inputRequirement: 'INPUT_FORBIDDEN',
            status: {
                groupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                id: 'd90ac264-018b-1000-1827-a86c8156fd9e',
                name: 'ConsumeKafka_2_6',
                runStatus: 'Invalid',
                statsLastRefreshed: '14:54:21 EST',
                aggregateSnapshot: {
                    id: 'd90ac264-018b-1000-1827-a86c8156fd9e',
                    groupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                    name: 'ConsumeKafka_2_6',
                    type: 'ConsumeKafka_2_6',
                    runStatus: 'Invalid',
                    executionNode: 'ALL',
                    bytesRead: 0,
                    bytesWritten: 0,
                    read: '0 bytes',
                    written: '0 bytes',
                    flowFilesIn: 0,
                    bytesIn: 0,
                    input: '0 (0 bytes)',
                    flowFilesOut: 0,
                    bytesOut: 0,
                    output: '0 (0 bytes)',
                    taskCount: 0,
                    tasksDurationNanos: 0,
                    tasks: '0',
                    tasksDuration: '00:00:00.000',
                    activeThreadCount: 0,
                    terminatedThreadCount: 0
                }
            },
            operatePermissions: {
                canRead: false,
                canWrite: false
            }
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [EditProcessor, MockComponent(ContextErrorBanner), NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: jest.fn()
                    }
                },
                {
                    provide: CanvasUtils,
                    useValue: {
                        runnableSupportsModification: jest.fn()
                    }
                },
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(EditProcessor);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
