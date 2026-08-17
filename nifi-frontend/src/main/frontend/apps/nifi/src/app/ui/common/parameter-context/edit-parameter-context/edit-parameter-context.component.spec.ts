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
import { EventEmitter } from '@angular/core';

import { EditParameterContext } from './edit-parameter-context.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { of } from 'rxjs';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../pages/parameter-contexts/state/parameter-context-listing/parameter-context-listing.reducer';
import { parameterContextListingFeatureKey } from '../../../../pages/parameter-contexts/state/parameter-context-listing';
import { parameterContextsFeatureKey } from '../../../../pages/parameter-contexts/state';
import { initialState as initialErrorState } from '../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../state/current-user';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { ParameterContextEntity, ParameterContextUpdateRequestEntity, ParameterEntity } from '../../../../state/shared';

import { EditParameterContextRequest } from '../index';
import { By } from '@angular/platform-browser';

describe('EditParameterContext', () => {
    let component: EditParameterContext;
    let fixture: ComponentFixture<EditParameterContext>;

    const data: EditParameterContextRequest = {
        parameterContext: {
            revision: {
                version: 1
            },
            id: '95d4f3d2-018b-1000-b7c7-b830c49a8026',
            uri: 'https://localhost:4200/nifi-api/parameter-contexts/95d4f3d2-018b-1000-b7c7-b830c49a8026',
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
        }
    };

    const parameterContexts: ParameterContextEntity[] = [
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
    ];

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [EditParameterContext, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [parameterContextsFeatureKey]: {
                            [parameterContextListingFeatureKey]: initialState
                        }
                    }
                }),
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: vi.fn()
                    }
                },
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(EditParameterContext);
        component = fixture.componentInstance;
        component.availableParameterContexts$ = of(parameterContexts);
        component.hasPendingPostUpdateNavigation$ = of(false);
        component.updateRequest = of(null);
        component.saving$ = of(false);
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should have cancelUpdateRequest EventEmitter', () => {
        expect(component.cancelUpdateRequest).toBeDefined();
        expect(component.cancelUpdateRequest).toBeInstanceOf(EventEmitter);
    });

    it('should have continuePostUpdateNavigation EventEmitter', () => {
        expect(component.continuePostUpdateNavigation).toBeDefined();
        expect(component.continuePostUpdateNavigation).toBeInstanceOf(EventEmitter);
    });

    it('should emit cancelUpdateRequest when called', () => {
        const spy = vi.spyOn(component.cancelUpdateRequest, 'emit');

        component.cancelUpdateRequest.emit();

        expect(spy).toHaveBeenCalledTimes(1);
    });

    describe('submitForm', () => {
        it('should include postUpdateNavigation fields on editParameterContext emit', () => {
            const spy = vi.spyOn(component.editParameterContext, 'next');
            component.editParameterContextForm.markAsDirty();

            component.submitForm(['/parameter-contexts', 'inherited-id', 'edit'], ['/parameter-contexts'], {
                highlightedParameterName: 'inherited-param'
            });

            expect(spy).toHaveBeenCalledWith(
                expect.objectContaining({
                    payload: expect.objectContaining({
                        id: data.parameterContext!.id
                    }),
                    postUpdateNavigation: ['/parameter-contexts', 'inherited-id', 'edit'],
                    postUpdateNavigationBoundary: ['/parameter-contexts'],
                    postUpdateNavigationState: { highlightedParameterName: 'inherited-param' }
                })
            );
        });

        it('should omit postUpdateNavigation fields when not provided', () => {
            const spy = vi.spyOn(component.editParameterContext, 'next');
            component.editParameterContextForm.markAsDirty();

            component.submitForm();

            expect(spy).toHaveBeenCalledWith(
                expect.objectContaining({
                    payload: expect.objectContaining({
                        id: data.parameterContext!.id
                    }),
                    postUpdateNavigation: undefined,
                    postUpdateNavigationBoundary: undefined,
                    postUpdateNavigationState: undefined
                })
            );
        });
    });

    describe('post-update review actions', () => {
        const completeUpdateRequest: ParameterContextUpdateRequestEntity = {
            parameterContextRevision: { version: 1 },
            request: {
                complete: true,
                lastUpdated: '2024-01-01T00:00:00.000Z',
                percentComponent: 100,
                referencingComponents: [],
                requestId: 'request-1',
                state: 'COMPLETE',
                updateSteps: [],
                uri: '/nifi-api/parameter-contexts/update-requests/request-1'
            }
        };

        async function createReviewFixture(hasPendingNavigation: boolean): Promise<void> {
            TestBed.resetTestingModule();
            await TestBed.configureTestingModule({
                imports: [EditParameterContext, NoopAnimationsModule],
                providers: [
                    { provide: MAT_DIALOG_DATA, useValue: data },
                    provideMockStore({
                        initialState: {
                            [errorFeatureKey]: initialErrorState,
                            [currentUserFeatureKey]: initialCurrentUserState,
                            [parameterContextsFeatureKey]: {
                                [parameterContextListingFeatureKey]: initialState
                            }
                        }
                    }),
                    {
                        provide: ClusterConnectionService,
                        useValue: {
                            isDisconnectionAcknowledged: vi.fn()
                        }
                    },
                    { provide: MatDialogRef, useValue: null }
                ]
            }).compileComponents();

            fixture = TestBed.createComponent(EditParameterContext);
            component = fixture.componentInstance;
            component.availableParameterContexts$ = of(parameterContexts);
            component.hasPendingPostUpdateNavigation$ = of(hasPendingNavigation);
            component.updateRequest = of(completeUpdateRequest);
            component.saving$ = of(false);
            fixture.detectChanges();
        }

        it('should show Close as secondary and Go to Parameter as primary when navigation is pending', async () => {
            await createReviewFixture(true);

            const closeButton = fixture.debugElement.query(By.css('button[data-qa="edit-parameter-context-close"]'));
            const goToButton = fixture.debugElement.query(
                By.css('button[data-qa="edit-parameter-context-go-to-parameter"]')
            );

            expect(closeButton).toBeTruthy();
            expect(closeButton.attributes['mat-button']).toBeDefined();
            expect(goToButton).toBeTruthy();
            expect(goToButton.attributes['mat-flat-button']).toBeDefined();
        });

        it('should emit continuePostUpdateNavigation when Go to Parameter is clicked', async () => {
            await createReviewFixture(true);

            const emitSpy = vi.spyOn(component.continuePostUpdateNavigation, 'emit');
            const goToButton = fixture.debugElement.query(
                By.css('button[data-qa="edit-parameter-context-go-to-parameter"]')
            );

            goToButton.nativeElement.click();

            expect(emitSpy).toHaveBeenCalledTimes(1);
        });

        it('should show only Close as primary when no post-update navigation is pending', async () => {
            await createReviewFixture(false);

            const closeButton = fixture.debugElement.query(By.css('button[data-qa="edit-parameter-context-close"]'));
            const goToButton = fixture.debugElement.query(
                By.css('button[data-qa="edit-parameter-context-go-to-parameter"]')
            );

            expect(closeButton).toBeTruthy();
            expect(closeButton.attributes['mat-flat-button']).toBeDefined();
            expect(goToButton).toBeNull();
        });
    });

    describe('inheritsParameters', () => {
        it('should return true if parameters are inherited', () => {
            const parameters: ParameterEntity[] = [
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
                        inherited: true
                    }
                }
            ];
            expect(component.inheritsParameters(parameters)).toBe(true);
        });

        it('should return false if parameters are not inherited', () => {
            const parameters: ParameterEntity[] = [
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
                }
            ];
            expect(component.inheritsParameters(parameters)).toBe(false);
        });

        it('should return false if parameters undefined', () => {
            expect(component.inheritsParameters(undefined)).toBe(false);
        });
    });
});
