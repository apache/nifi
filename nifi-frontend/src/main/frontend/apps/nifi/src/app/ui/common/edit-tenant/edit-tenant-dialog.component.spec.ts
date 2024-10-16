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

import { EditTenantDialog } from './edit-tenant-dialog.component';
import { EditTenantRequest } from '../../../state/shared';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MockComponent } from 'ng-mocks';
import { ContextErrorBanner } from '../context-error-banner/context-error-banner.component';

describe('EditTenantDialog', () => {
    let component: EditTenantDialog;
    let fixture: ComponentFixture<EditTenantDialog>;

    const data: EditTenantRequest = {
        user: {
            revision: {
                version: 0
            },
            id: 'acfc1479-018c-1000-1025-6cc5b4adefb8',
            uri: 'https://localhost:4200/nifi-api/tenants/users/acfc1479-018c-1000-1025-6cc5b4adefb8',
            permissions: {
                canRead: true,
                canWrite: true
            },
            component: {
                id: 'acfc1479-018c-1000-1025-6cc5b4adefb8',
                identity: 'Group 2',
                configurable: true,
                userGroups: [],
                accessPolicies: []
            }
        },
        existingUsers: [
            {
                revision: {
                    version: 0
                },
                id: 'ad0ddd93-018c-1000-4e40-8e3b207abdcd',
                uri: 'https://localhost:4200/nifi-api/tenants/users/ad0ddd93-018c-1000-4e40-8e3b207abdcd',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: 'ad0ddd93-018c-1000-4e40-8e3b207abdcd',
                    identity: 'user 1',
                    configurable: true,
                    userGroups: [
                        {
                            revision: {
                                version: 0
                            },
                            id: 'acfbfa2c-018c-1000-0311-47b83e34c9c3',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'acfbfa2c-018c-1000-0311-47b83e34c9c3',
                                identity: 'group 1',
                                configurable: true
                            }
                        }
                    ],
                    accessPolicies: [
                        {
                            revision: {
                                clientId: 'b09bd713-018c-1000-e5b8-14855e466f1b',
                                version: 4
                            },
                            id: 'b0c3148d-018c-1000-2cfe-8fab902c11f7',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'b0c3148d-018c-1000-2cfe-8fab902c11f7',
                                resource: '/system',
                                action: 'read',
                                configurable: true
                            }
                        }
                    ]
                }
            },
            {
                revision: {
                    version: 0
                },
                id: 'ad0875a3-018c-1000-1877-3f4ebf93f91e',
                uri: 'https://localhost:4200/nifi-api/tenants/users/ad0875a3-018c-1000-1877-3f4ebf93f91e',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: 'ad0875a3-018c-1000-1877-3f4ebf93f91e',
                    identity: 'user 2',
                    configurable: true,
                    userGroups: [
                        {
                            revision: {
                                version: 0
                            },
                            id: 'acfbfa2c-018c-1000-0311-47b83e34c9c3',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'acfbfa2c-018c-1000-0311-47b83e34c9c3',
                                identity: 'group 1',
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: 'acfcdcf6-018c-1000-604f-84da632fdbd5',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'acfcdcf6-018c-1000-604f-84da632fdbd5',
                                identity: 'group 9',
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: 'a69482a2-018c-1000-2c02-fac31f4b102b',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'a69482a2-018c-1000-2c02-fac31f4b102b',
                                identity: 'Group',
                                configurable: true
                            }
                        }
                    ],
                    accessPolicies: [
                        {
                            revision: {
                                clientId: 'b09bd713-018c-1000-e5b8-14855e466f1b',
                                version: 4
                            },
                            id: 'b0c3148d-018c-1000-2cfe-8fab902c11f7',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'b0c3148d-018c-1000-2cfe-8fab902c11f7',
                                resource: '/system',
                                action: 'read',
                                configurable: true
                            }
                        }
                    ]
                }
            },
            {
                revision: {
                    version: 0
                },
                id: 'acfc1479-018c-1000-1025-6cc5b4adefb8',
                uri: 'https://localhost:4200/nifi-api/tenants/users/acfc1479-018c-1000-1025-6cc5b4adefb8',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: 'acfc1479-018c-1000-1025-6cc5b4adefb8',
                    identity: 'Group 2',
                    configurable: true,
                    userGroups: [],
                    accessPolicies: []
                }
            },
            {
                revision: {
                    version: 0
                },
                id: 'acfc879d-018c-1000-c93e-21350df0f5bf',
                uri: 'https://localhost:4200/nifi-api/tenants/users/acfc879d-018c-1000-c93e-21350df0f5bf',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: 'acfc879d-018c-1000-c93e-21350df0f5bf',
                    identity: 'group 7',
                    configurable: true,
                    userGroups: [],
                    accessPolicies: []
                }
            },
            {
                revision: {
                    version: 0
                },
                id: '21232f29-7a57-35a7-8389-4a0e4a801fc3',
                uri: 'https://localhost:4200/nifi-api/tenants/users/21232f29-7a57-35a7-8389-4a0e4a801fc3',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: '21232f29-7a57-35a7-8389-4a0e4a801fc3',
                    identity: 'admin',
                    configurable: true,
                    userGroups: [
                        {
                            revision: {
                                version: 0
                            },
                            id: 'a69482a2-018c-1000-2c02-fac31f4b102b',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'a69482a2-018c-1000-2c02-fac31f4b102b',
                                identity: 'Group',
                                configurable: true
                            }
                        }
                    ],
                    accessPolicies: [
                        {
                            revision: {
                                clientId: 'b09bd713-018c-1000-e5b8-14855e466f1b',
                                version: 4
                            },
                            id: 'b0c3148d-018c-1000-2cfe-8fab902c11f7',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'b0c3148d-018c-1000-2cfe-8fab902c11f7',
                                resource: '/system',
                                action: 'read',
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: '15e4e0bd-cb28-34fd-8587-f8d15162cba5',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: '15e4e0bd-cb28-34fd-8587-f8d15162cba5',
                                resource: '/tenants',
                                action: 'write',
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: 'c6322e6c-4cc1-3bcc-91b3-2ed2111674cf',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'c6322e6c-4cc1-3bcc-91b3-2ed2111674cf',
                                resource: '/controller',
                                action: 'write',
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: 'f99bccd1-a30e-3e4a-98a2-dbc708edc67f',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'f99bccd1-a30e-3e4a-98a2-dbc708edc67f',
                                resource: '/flow',
                                action: 'read',
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: '627410be-1717-35b4-a06f-e9362b89e0b7',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: '627410be-1717-35b4-a06f-e9362b89e0b7',
                                resource: '/tenants',
                                action: 'read',
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: '2e1015cb-0fed-3005-8e0d-722311f21a03',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: '2e1015cb-0fed-3005-8e0d-722311f21a03',
                                resource: '/controller',
                                action: 'read',
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: 'b8775bd4-704a-34c6-987b-84f2daf7a515',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'b8775bd4-704a-34c6-987b-84f2daf7a515',
                                resource: '/restricted-components',
                                action: 'write',
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: '92db2c23-018c-1000-8885-b650a16dcb32',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: '92db2c23-018c-1000-8885-b650a16dcb32',
                                resource: '/process-groups/92dade11-018c-1000-91a9-ad537020d5cb',
                                action: 'read',
                                componentReference: {
                                    revision: {
                                        version: 0
                                    },
                                    id: '92dade11-018c-1000-91a9-ad537020d5cb',
                                    permissions: {
                                        canRead: true,
                                        canWrite: true
                                    },
                                    component: {
                                        id: '92dade11-018c-1000-91a9-ad537020d5cb',
                                        name: 'NiFi Flow'
                                    }
                                },
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: '92db4367-018c-1000-e220-2dbec57c389b',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: '92db4367-018c-1000-e220-2dbec57c389b',
                                resource: '/process-groups/92dade11-018c-1000-91a9-ad537020d5cb',
                                action: 'write',
                                componentReference: {
                                    revision: {
                                        version: 0
                                    },
                                    id: '92dade11-018c-1000-91a9-ad537020d5cb',
                                    permissions: {
                                        canRead: true,
                                        canWrite: true
                                    },
                                    component: {
                                        id: '92dade11-018c-1000-91a9-ad537020d5cb',
                                        name: 'NiFi Flow'
                                    }
                                },
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: 'ad99ea98-3af6-3561-ae27-5bf09e1d969d',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'ad99ea98-3af6-3561-ae27-5bf09e1d969d',
                                resource: '/policies',
                                action: 'write',
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: 'ff96062a-fa99-36dc-9942-0f6442ae7212',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'ff96062a-fa99-36dc-9942-0f6442ae7212',
                                resource: '/policies',
                                action: 'read',
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: 'b1d4bb80-018c-1000-6cb7-a1e977f8382b',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'b1d4bb80-018c-1000-6cb7-a1e977f8382b',
                                resource: '/processors/ad55f343-018c-1000-1291-1992288346e5',
                                action: 'read',
                                componentReference: {
                                    revision: {
                                        version: 0
                                    },
                                    id: 'ad55f343-018c-1000-1291-1992288346e5',
                                    permissions: {
                                        canRead: true,
                                        canWrite: true
                                    },
                                    parentGroupId: '92dade11-018c-1000-91a9-ad537020d5cb',
                                    component: {
                                        id: 'ad55f343-018c-1000-1291-1992288346e5',
                                        parentGroupId: '92dade11-018c-1000-91a9-ad537020d5cb',
                                        name: 'InvokeHTTP'
                                    }
                                },
                                configurable: true
                            }
                        }
                    ]
                }
            },
            {
                revision: {
                    version: 0
                },
                id: 'abf8bb43-018c-1000-3e0c-a4405f39c934',
                uri: 'https://localhost:4200/nifi-api/tenants/users/abf8bb43-018c-1000-3e0c-a4405f39c934',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: 'abf8bb43-018c-1000-3e0c-a4405f39c934',
                    identity: 'test',
                    configurable: true,
                    userGroups: [
                        {
                            revision: {
                                version: 0
                            },
                            id: 'a69482a2-018c-1000-2c02-fac31f4b102b',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'a69482a2-018c-1000-2c02-fac31f4b102b',
                                identity: 'Group',
                                configurable: true
                            }
                        }
                    ],
                    accessPolicies: []
                }
            },
            {
                revision: {
                    version: 0
                },
                id: 'acfc259e-018c-1000-2e1e-01919d4b0546',
                uri: 'https://localhost:4200/nifi-api/tenants/users/acfc259e-018c-1000-2e1e-01919d4b0546',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: 'acfc259e-018c-1000-2e1e-01919d4b0546',
                    identity: 'group 3',
                    configurable: true,
                    userGroups: [],
                    accessPolicies: []
                }
            },
            {
                revision: {
                    version: 0
                },
                id: 'acfca1a8-018c-1000-6852-0dd013e20ee7',
                uri: 'https://localhost:4200/nifi-api/tenants/users/acfca1a8-018c-1000-6852-0dd013e20ee7',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: 'acfca1a8-018c-1000-6852-0dd013e20ee7',
                    identity: 'group 8',
                    configurable: true,
                    userGroups: [],
                    accessPolicies: []
                }
            },
            {
                revision: {
                    version: 0
                },
                id: 'acfc5a70-018c-1000-a702-b52236aaacea',
                uri: 'https://localhost:4200/nifi-api/tenants/users/acfc5a70-018c-1000-a702-b52236aaacea',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: 'acfc5a70-018c-1000-a702-b52236aaacea',
                    identity: 'group 5',
                    configurable: true,
                    userGroups: [],
                    accessPolicies: []
                }
            },
            {
                revision: {
                    version: 0
                },
                id: 'acfc6ee7-018c-1000-feed-fb1b89482663',
                uri: 'https://localhost:4200/nifi-api/tenants/users/acfc6ee7-018c-1000-feed-fb1b89482663',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: 'acfc6ee7-018c-1000-feed-fb1b89482663',
                    identity: 'group 6',
                    configurable: true,
                    userGroups: [],
                    accessPolicies: []
                }
            },
            {
                revision: {
                    version: 0
                },
                id: 'acfc418b-018c-1000-bd42-50d38a5f8ec4',
                uri: 'https://localhost:4200/nifi-api/tenants/users/acfc418b-018c-1000-bd42-50d38a5f8ec4',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: 'acfc418b-018c-1000-bd42-50d38a5f8ec4',
                    identity: 'group 4',
                    configurable: true,
                    userGroups: [],
                    accessPolicies: []
                }
            }
        ],
        existingUserGroups: [
            {
                revision: {
                    version: 0
                },
                id: 'acfbfa2c-018c-1000-0311-47b83e34c9c3',
                uri: 'https://localhost:4200/nifi-api/tenants/user-groups/acfbfa2c-018c-1000-0311-47b83e34c9c3',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: 'acfbfa2c-018c-1000-0311-47b83e34c9c3',
                    identity: 'group 1',
                    configurable: true,
                    users: [
                        {
                            revision: {
                                version: 0
                            },
                            id: 'ad0ddd93-018c-1000-4e40-8e3b207abdcd',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'ad0ddd93-018c-1000-4e40-8e3b207abdcd',
                                identity: 'user 1',
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: 'ad0875a3-018c-1000-1877-3f4ebf93f91e',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'ad0875a3-018c-1000-1877-3f4ebf93f91e',
                                identity: 'user 2',
                                configurable: true
                            }
                        }
                    ],
                    accessPolicies: [
                        {
                            revision: {
                                clientId: 'b09bd713-018c-1000-e5b8-14855e466f1b',
                                version: 4
                            },
                            id: 'b0c3148d-018c-1000-2cfe-8fab902c11f7',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'b0c3148d-018c-1000-2cfe-8fab902c11f7',
                                resource: '/system',
                                action: 'read',
                                configurable: true
                            }
                        }
                    ]
                }
            },
            {
                revision: {
                    version: 0
                },
                id: 'acfcdcf6-018c-1000-604f-84da632fdbd5',
                uri: 'https://localhost:4200/nifi-api/tenants/user-groups/acfcdcf6-018c-1000-604f-84da632fdbd5',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: 'acfcdcf6-018c-1000-604f-84da632fdbd5',
                    identity: 'group 9',
                    configurable: true,
                    users: [
                        {
                            revision: {
                                version: 0
                            },
                            id: 'ad0875a3-018c-1000-1877-3f4ebf93f91e',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'ad0875a3-018c-1000-1877-3f4ebf93f91e',
                                identity: 'user 2',
                                configurable: true
                            }
                        }
                    ],
                    accessPolicies: []
                }
            },
            {
                revision: {
                    version: 0
                },
                id: 'a69482a2-018c-1000-2c02-fac31f4b102b',
                uri: 'https://localhost:4200/nifi-api/tenants/user-groups/a69482a2-018c-1000-2c02-fac31f4b102b',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: 'a69482a2-018c-1000-2c02-fac31f4b102b',
                    identity: 'Group',
                    configurable: true,
                    users: [
                        {
                            revision: {
                                version: 0
                            },
                            id: 'ad0875a3-018c-1000-1877-3f4ebf93f91e',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'ad0875a3-018c-1000-1877-3f4ebf93f91e',
                                identity: 'user 2',
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: '21232f29-7a57-35a7-8389-4a0e4a801fc3',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: '21232f29-7a57-35a7-8389-4a0e4a801fc3',
                                identity: 'admin',
                                configurable: true
                            }
                        },
                        {
                            revision: {
                                version: 0
                            },
                            id: 'abf8bb43-018c-1000-3e0c-a4405f39c934',
                            permissions: {
                                canRead: true,
                                canWrite: true
                            },
                            component: {
                                id: 'abf8bb43-018c-1000-3e0c-a4405f39c934',
                                identity: 'test',
                                configurable: true
                            }
                        }
                    ],
                    accessPolicies: []
                }
            }
        ]
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [EditTenantDialog, MockComponent(ContextErrorBanner), NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(EditTenantDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
