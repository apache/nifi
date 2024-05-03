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

import { CreateProcessGroup } from './create-process-group.component';
import { CreateProcessGroupDialogRequest } from '../../../../../state/flow';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { ComponentType } from '../../../../../../../state/shared';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/flow/flow.reducer';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

describe('CreateProcessGroup', () => {
    let component: CreateProcessGroup;
    let fixture: ComponentFixture<CreateProcessGroup>;

    const data: CreateProcessGroupDialogRequest = {
        request: {
            revision: {
                clientId: 'a6482293-7fe8-43b4-8ab4-ee95b3b27721',
                version: 0
            },
            type: ComponentType.ProcessGroup,
            position: {
                x: -4,
                y: -698.5
            }
        },
        parameterContexts: [
            {
                revision: {
                    version: 1
                },
                id: '95d4f3d2-018b-1000-b7c7-b830c49a8026',
                uri: '',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    name: 'params 1',
                    description: '',
                    parameters: [
                        {
                            canWrite: true,
                            parameter: {
                                name: 'one',
                                description: 'Description for one.',
                                sensitive: false,
                                value: 'value',
                                provided: false,
                                referencingComponents: [],
                                parameterContext: {
                                    id: '95d4f3d2-018b-1000-b7c7-b830c49a8026',
                                    permissions: {
                                        canRead: true,
                                        canWrite: true
                                    },
                                    component: {
                                        id: '95d4f3d2-018b-1000-b7c7-b830c49a8026',
                                        name: 'params 1'
                                    }
                                },
                                inherited: false
                            }
                        },
                        {
                            canWrite: true,
                            parameter: {
                                name: 'two',
                                description: 'Description for two.',
                                sensitive: false,
                                value: 'value',
                                provided: false,
                                referencingComponents: [],
                                parameterContext: {
                                    id: '95d4f3d2-018b-1000-b7c7-b830c49a8026',
                                    permissions: {
                                        canRead: true,
                                        canWrite: true
                                    },
                                    component: {
                                        id: '95d4f3d2-018b-1000-b7c7-b830c49a8026',
                                        name: 'params 1'
                                    }
                                },
                                inherited: false
                            }
                        },
                        {
                            canWrite: true,
                            parameter: {
                                name: 'Group ID',
                                description: '',
                                sensitive: false,
                                value: 'asdf',
                                provided: false,
                                referencingComponents: [],
                                parameterContext: {
                                    id: '95d4f3d2-018b-1000-b7c7-b830c49a8026',
                                    permissions: {
                                        canRead: true,
                                        canWrite: true
                                    },
                                    component: {
                                        id: '95d4f3d2-018b-1000-b7c7-b830c49a8026',
                                        name: 'params 1'
                                    }
                                },
                                inherited: false
                            }
                        }
                    ],
                    boundProcessGroups: [],
                    inheritedParameterContexts: [],
                    id: '95d4f3d2-018b-1000-b7c7-b830c49a8026'
                }
            },
            {
                revision: {
                    version: 0
                },
                id: '95d509b9-018b-1000-daff-b7957ea7934f',
                uri: '',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    name: 'params 2',
                    description: '',
                    parameters: [],
                    boundProcessGroups: [],
                    inheritedParameterContexts: [],
                    id: '95d509b9-018b-1000-daff-b7957ea7934f'
                }
            }
        ]
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [CreateProcessGroup, NoopAnimationsModule],
            providers: [{ provide: MAT_DIALOG_DATA, useValue: data }, provideMockStore({ initialState })]
        });
        fixture = TestBed.createComponent(CreateProcessGroup);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
