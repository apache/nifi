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

import { TestBed } from '@angular/core/testing';
import { provideMockActions } from '@ngrx/effects/testing';
import { Action } from '@ngrx/store';
import { provideMockStore } from '@ngrx/store/testing';
import { ReplaySubject, of } from 'rxjs';
import { take, switchMap } from 'rxjs/operators';
import { ControllerServicesEffects } from './controller-services.effects';
import { ControllerServiceService } from '../../service/controller-service.service';
import {
    clearControllerServiceBulletins,
    clearControllerServiceBulletinsSuccess,
    createControllerService,
    createControllerServiceSuccess,
    deleteControllerService,
    deleteControllerServiceSuccess,
    loadControllerServices
} from './controller-services.actions';
import { ComponentType } from '@nifi/shared';
import { HttpErrorResponse } from '@angular/common/http';
import * as ErrorActions from '../../../../state/error/error.actions';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { Client } from '../../../../service/client.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { ParameterHelperService } from '../../service/parameter-helper.service';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { ParameterContextService } from '../../../parameter-contexts/service/parameter-contexts.service';
import { Storage } from '@nifi/shared';
import { ClearBulletinsRequest, ClearBulletinsResponse } from '../../../../state/shared';

describe('ControllerServicesEffects', () => {
    let actions$: ReplaySubject<Action>;
    let effects: ControllerServicesEffects;
    let mockControllerServiceService: jest.Mocked<ControllerServiceService>;

    beforeEach(() => {
        const controllerServiceServiceSpy = {
            getControllerServices: jest.fn(),
            getControllerService: jest.fn(),
            getFlow: jest.fn(),
            createControllerService: jest.fn(),
            updateControllerService: jest.fn(),
            deleteControllerService: jest.fn(),
            clearBulletins: jest.fn()
        } as unknown as jest.Mocked<ControllerServiceService>;

        TestBed.configureTestingModule({
            providers: [
                ControllerServicesEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    initialState: {
                        controllerServices: {
                            status: 'pending',
                            processGroupId: 'test-process-group-id'
                        }
                    }
                }),
                {
                    provide: ControllerServiceService,
                    useValue: controllerServiceServiceSpy
                },
                {
                    provide: MatDialog,
                    useValue: {
                        open: jest.fn(),
                        closeAll: jest.fn()
                    }
                },
                {
                    provide: Router,
                    useValue: {
                        navigate: jest.fn()
                    }
                },
                {
                    provide: Client,
                    useValue: {
                        getClientId: jest.fn().mockReturnValue('test-client-id')
                    }
                },
                {
                    provide: ErrorHelper,
                    useValue: {
                        handleLoadingError: jest.fn(),
                        getErrorString: jest.fn(),
                        showErrorInContext: jest.fn(),
                        fullScreenError: jest.fn()
                    }
                },
                {
                    provide: PropertyTableHelperService,
                    useValue: {
                        getComponentHistory: jest.fn(),
                        createNewProperty: jest.fn(),
                        createNewService: jest.fn()
                    }
                },
                {
                    provide: ParameterHelperService,
                    useValue: {
                        convertToParameter: jest.fn()
                    }
                },
                {
                    provide: ExtensionTypesService,
                    useValue: {
                        getControllerServiceVersionsForType: jest.fn()
                    }
                },
                {
                    provide: ParameterContextService,
                    useValue: {
                        getParameterContext: jest.fn()
                    }
                },
                {
                    provide: Storage,
                    useValue: {
                        setItem: jest.fn(),
                        getItem: jest.fn()
                    }
                }
            ]
        });

        effects = TestBed.inject(ControllerServicesEffects);
        mockControllerServiceService = TestBed.inject(
            ControllerServiceService
        ) as jest.Mocked<ControllerServiceService>;
        actions$ = new ReplaySubject(1);
    });

    describe('clearControllerServiceBulletins$', () => {
        it('should dispatch clearControllerServiceBulletinsSuccess action when bulletins are cleared successfully', () => {
            const request: ClearBulletinsRequest = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'test-controller-service-id',
                componentType: ComponentType.ControllerService
            };

            const serviceResponse = {
                bulletinsCleared: 5,
                bulletins: []
            };

            const expectedResponse: ClearBulletinsResponse = {
                componentId: request.componentId,
                bulletinsCleared: 5,
                bulletins: [],
                componentType: request.componentType
            };

            mockControllerServiceService.clearBulletins.mockReturnValue(of(serviceResponse));

            actions$.next(clearControllerServiceBulletins({ request }));

            effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe((action) => {
                expect(action).toEqual(clearControllerServiceBulletinsSuccess({ response: expectedResponse }));
            });

            expect(mockControllerServiceService.clearBulletins).toHaveBeenCalledWith({
                uri: request.uri,
                fromTimestamp: request.fromTimestamp
            });
        });

        it('should handle bulletinsCleared being undefined and default to 0', () => {
            const request: ClearBulletinsRequest = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'test-controller-service-id',
                componentType: ComponentType.ControllerService
            };

            const serviceResponse = {
                // bulletinsCleared is undefined
                bulletins: []
            };

            const expectedResponse: ClearBulletinsResponse = {
                componentId: request.componentId,
                bulletinsCleared: 0, // Should default to 0
                bulletins: [],
                componentType: request.componentType
            };

            mockControllerServiceService.clearBulletins.mockReturnValue(of(serviceResponse));

            actions$.next(clearControllerServiceBulletins({ request }));

            effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe((action) => {
                expect(action).toEqual(clearControllerServiceBulletinsSuccess({ response: expectedResponse }));
            });
        });

        it('should preserve request fields in the success response', () => {
            const request: ClearBulletinsRequest = {
                uri: 'different-test-uri',
                fromTimestamp: '2023-06-15T08:30:00.000Z',
                componentId: 'different-controller-service-id',
                componentType: ComponentType.ControllerService
            };

            const serviceResponse = {
                bulletinsCleared: 3,
                bulletins: []
            };

            mockControllerServiceService.clearBulletins.mockReturnValue(of(serviceResponse));

            actions$.next(clearControllerServiceBulletins({ request }));

            effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe((action) => {
                expect(action.response.componentId).toBe(request.componentId);
                expect(action.response.componentType).toBe(request.componentType);
                expect(action.response.bulletinsCleared).toBe(3);
                expect(action.response.bulletins).toEqual([]);
            });
        });

        it('should call service with correct parameters', () => {
            const request: ClearBulletinsRequest = {
                uri: 'test-controller-service-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'test-controller-service-id',
                componentType: ComponentType.ControllerService
            };

            const serviceResponse = {
                bulletinsCleared: 2,
                bulletins: []
            };

            mockControllerServiceService.clearBulletins.mockReturnValue(of(serviceResponse));

            actions$.next(clearControllerServiceBulletins({ request }));

            effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe(() => {
                expect(mockControllerServiceService.clearBulletins).toHaveBeenCalledWith({
                    uri: request.uri,
                    fromTimestamp: request.fromTimestamp
                });
                expect(mockControllerServiceService.clearBulletins).toHaveBeenCalledTimes(1);
            });
        });

        it('should handle zero bulletins cleared', () => {
            const request: ClearBulletinsRequest = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'test-controller-service-id',
                componentType: ComponentType.ControllerService
            };

            const serviceResponse = {
                bulletinsCleared: 0,
                bulletins: []
            };

            const expectedResponse: ClearBulletinsResponse = {
                componentId: request.componentId,
                bulletinsCleared: 0,
                bulletins: [],
                componentType: request.componentType
            };

            mockControllerServiceService.clearBulletins.mockReturnValue(of(serviceResponse));

            actions$.next(clearControllerServiceBulletins({ request }));

            effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe((action) => {
                expect(action).toEqual(clearControllerServiceBulletinsSuccess({ response: expectedResponse }));
            });
        });

        it('should handle large number of bulletins cleared', () => {
            const request: ClearBulletinsRequest = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'test-controller-service-id',
                componentType: ComponentType.ControllerService
            };

            const serviceResponse = {
                bulletinsCleared: 1000,
                bulletins: []
            };

            const expectedResponse: ClearBulletinsResponse = {
                componentId: request.componentId,
                bulletinsCleared: 1000,
                bulletins: [],
                componentType: request.componentType
            };

            mockControllerServiceService.clearBulletins.mockReturnValue(of(serviceResponse));

            actions$.next(clearControllerServiceBulletins({ request }));

            effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe((action) => {
                expect(action).toEqual(clearControllerServiceBulletinsSuccess({ response: expectedResponse }));
            });
        });
    });

    describe('createControllerService$', () => {
        it('should dispatch createControllerServiceSuccess when service is created successfully', () => {
            const request = {
                revision: { clientId: 'test-client', version: 0 },
                processGroupId: 'test-process-group-id',
                controllerServiceType: 'org.apache.nifi.TestService',
                controllerServiceBundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' }
            };
            const serviceResponse = {
                id: 'new-service-id',
                component: { name: 'New Test Service' }
            } as any;

            mockControllerServiceService.createControllerService.mockReturnValue(of(serviceResponse));

            actions$.next(createControllerService({ request }));

            effects.createControllerService$.pipe(take(1)).subscribe((action) => {
                expect(action).toEqual(
                    createControllerServiceSuccess({
                        response: { controllerService: serviceResponse }
                    })
                );
            });

            expect(mockControllerServiceService.createControllerService).toHaveBeenCalledWith(request);
        });

        it('should dispatch snackBarError and close dialogs when creation fails', () => {
            const request = {
                revision: { clientId: 'test-client', version: 0 },
                processGroupId: 'test-process-group-id',
                controllerServiceType: 'org.apache.nifi.TestService',
                controllerServiceBundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' }
            };
            const errorResponse = new HttpErrorResponse({
                error: 'Creation failed',
                status: 500,
                statusText: 'Internal Server Error'
            });

            const mockDialog = TestBed.inject(MatDialog);
            const mockErrorHelper = TestBed.inject(ErrorHelper);
            jest.spyOn(mockDialog, 'closeAll');
            jest.spyOn(mockErrorHelper, 'getErrorString').mockReturnValue('Creation failed');

            mockControllerServiceService.createControllerService.mockReturnValue(
                new ReplaySubject().pipe(
                    switchMap(() => {
                        throw errorResponse;
                    })
                )
            );

            actions$.next(createControllerService({ request }));

            effects.createControllerService$.pipe(take(1)).subscribe((action) => {
                expect(action).toEqual(ErrorActions.snackBarError({ error: 'Creation failed' }));
                expect(mockDialog.closeAll).toHaveBeenCalled();
            });
        });
    });

    describe('deleteControllerService$', () => {
        it('should dispatch deleteControllerServiceSuccess when service is deleted successfully', () => {
            const request = {
                controllerService: {
                    id: 'service-to-delete',
                    component: { name: 'Service to Delete' }
                } as any
            };
            const serviceResponse = {
                id: 'service-to-delete',
                component: { name: 'Service to Delete' }
            } as any;

            mockControllerServiceService.deleteControllerService.mockReturnValue(of(serviceResponse));

            actions$.next(deleteControllerService({ request }));

            effects.deleteControllerService$.pipe(take(1)).subscribe((action) => {
                expect(action).toEqual(
                    deleteControllerServiceSuccess({
                        response: { controllerService: serviceResponse }
                    })
                );
            });

            expect(mockControllerServiceService.deleteControllerService).toHaveBeenCalledWith(request);
        });

        it('should dispatch snackBarError when deletion fails', () => {
            const request = {
                controllerService: {
                    id: 'service-to-delete',
                    component: { name: 'Service to Delete' }
                } as any
            };
            const errorResponse = new HttpErrorResponse({
                error: 'Deletion failed',
                status: 500,
                statusText: 'Internal Server Error'
            });

            const mockErrorHelper = TestBed.inject(ErrorHelper);
            jest.spyOn(mockErrorHelper, 'getErrorString').mockReturnValue('Deletion failed');

            mockControllerServiceService.deleteControllerService.mockReturnValue(
                new ReplaySubject().pipe(
                    switchMap(() => {
                        throw errorResponse;
                    })
                )
            );

            actions$.next(deleteControllerService({ request }));

            effects.deleteControllerService$.pipe(take(1)).subscribe((action) => {
                expect(action).toEqual(ErrorActions.snackBarError({ error: 'Deletion failed' }));
            });
        });
    });

    describe('deleteControllerServiceSuccess$', () => {
        it('should dispatch loadControllerServices after successful deletion', () => {
            const response = {
                controllerService: {
                    id: 'deleted-service',
                    component: { name: 'Deleted Service' }
                } as any
            };

            actions$.next(deleteControllerServiceSuccess({ response }));

            effects.deleteControllerServiceSuccess$.pipe(take(1)).subscribe((action) => {
                expect(action).toEqual(
                    loadControllerServices({
                        request: { processGroupId: undefined as any }
                    })
                );
            });
        });
    });
});
