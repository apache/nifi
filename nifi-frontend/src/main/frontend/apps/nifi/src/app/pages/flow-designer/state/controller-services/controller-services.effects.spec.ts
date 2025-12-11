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
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { Action } from '@ngrx/store';
import { ReplaySubject, of, take, throwError } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';

import * as ControllerServicesActions from './controller-services.actions';
import {
    clearControllerServiceBulletins,
    clearControllerServiceBulletinsSuccess,
    createControllerService,
    createControllerServiceSuccess,
    deleteControllerService,
    deleteControllerServiceSuccess,
    loadControllerServices
} from './controller-services.actions';
import { ControllerServicesEffects } from './controller-services.effects';
import { ControllerServiceService } from '../../service/controller-service.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { ParameterHelperService } from '../../service/parameter-helper.service';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { Client } from '../../../../service/client.service';
import { ComponentType, Storage } from '@nifi/shared';
import { ParameterContextService } from '../../../parameter-contexts/service/parameter-contexts.service';
import { controllerServicesFeatureKey } from './index';
import { initialState } from './controller-services.reducer';
import { ClearBulletinsRequest, ClearBulletinsResponse } from '../../../../state/shared';
import * as ErrorActions from '../../../../state/error/error.actions';
import { canvasFeatureKey } from '../index';
import { flowFeatureKey } from '../flow';

describe('ControllerServicesEffects', () => {
    interface SetupOptions {
        controllerServicesState?: any;
    }

    const mockControllerServicesResponse = {
        controllerServices: [],
        currentTime: '2023-01-01 12:00:00 EST'
    };
    const mockFlowResponse = {
        processGroupFlow: {
            id: 'pg-1',
            breadcrumb: { id: 'pg-1', name: 'PG', permissions: { canRead: true }, versionedFlowState: null },
            parameterContext: null
        }
    } as any;

    async function setup({ controllerServicesState = initialState }: SetupOptions = {}) {
        const controllerServiceServiceSpy = {
            getControllerServices: jest.fn(),
            getControllerService: jest.fn(),
            getFlow: jest.fn(),
            createControllerService: jest.fn(),
            updateControllerService: jest.fn(),
            deleteControllerService: jest.fn(),
            clearBulletins: jest.fn()
        } as unknown as jest.Mocked<ControllerServiceService>;

        await TestBed.configureTestingModule({
            providers: [
                ControllerServicesEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        [canvasFeatureKey]: {
                            [controllerServicesFeatureKey]: controllerServicesState,
                            [flowFeatureKey]: {
                                id: 'root'
                            }
                        }
                    }
                }),
                {
                    provide: ControllerServiceService,
                    useValue: controllerServiceServiceSpy
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
                    provide: Client,
                    useValue: {
                        getClientId: jest.fn().mockReturnValue('test-client-id')
                    }
                },
                {
                    provide: Storage,
                    useValue: {
                        setItem: jest.fn(),
                        getItem: jest.fn()
                    }
                },
                {
                    provide: ParameterContextService,
                    useValue: {
                        getParameterContext: jest.fn()
                    }
                }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const effects = TestBed.inject(ControllerServicesEffects);
        const controllerServiceService = TestBed.inject(
            ControllerServiceService
        ) as jest.Mocked<ControllerServiceService>;
        const errorHelper = TestBed.inject(ErrorHelper);

        return { effects, store, controllerServiceService, errorHelper };
    }

    let action$: ReplaySubject<Action>;
    beforeEach(() => {
        action$ = new ReplaySubject<Action>();
    });

    it('should create', async () => {
        const { effects } = await setup();
        expect(effects).toBeTruthy();
    });

    describe('Load Controller Services', () => {
        it('should load controller services successfully', async () => {
            const { effects, controllerServiceService } = await setup();

            action$.next(ControllerServicesActions.loadControllerServices({ request: { processGroupId: 'pg-1' } }));
            jest.spyOn(controllerServiceService, 'getControllerServices').mockReturnValueOnce(
                of(mockControllerServicesResponse) as never
            );
            jest.spyOn(controllerServiceService, 'getFlow').mockReturnValueOnce(of(mockFlowResponse) as never);

            const result = await new Promise((resolve) =>
                effects.loadControllerServices$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                ControllerServicesActions.loadControllerServicesSuccess({
                    response: {
                        processGroupId: mockFlowResponse.processGroupFlow.id,
                        controllerServices: mockControllerServicesResponse.controllerServices,
                        loadedTimestamp: mockControllerServicesResponse.currentTime,
                        breadcrumb: mockFlowResponse.processGroupFlow.breadcrumb,
                        parameterContext: mockFlowResponse.processGroupFlow.parameterContext
                    }
                })
            );
        });

        it('should fail to load controller services on initial load with hasExistingData=false', async () => {
            const { effects, controllerServiceService } = await setup();

            action$.next(ControllerServicesActions.loadControllerServices({ request: { processGroupId: 'pg-1' } }));
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(controllerServiceService, 'getControllerServices').mockImplementationOnce(() =>
                throwError(() => error)
            );
            jest.spyOn(controllerServiceService, 'getFlow').mockReturnValueOnce(of(mockFlowResponse) as never);

            const result = await new Promise((resolve) =>
                effects.loadControllerServices$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                ControllerServicesActions.loadControllerServicesError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });

        it('should fail to load controller services on refresh with hasExistingData=true', async () => {
            const stateWithData = { ...initialState, loadedTimestamp: '2023-01-01 11:00:00 EST' };
            const { effects, controllerServiceService } = await setup({
                controllerServicesState: stateWithData
            });

            action$.next(ControllerServicesActions.loadControllerServices({ request: { processGroupId: 'pg-1' } }));
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(controllerServiceService, 'getControllerServices').mockImplementationOnce(() =>
                throwError(() => error)
            );
            jest.spyOn(controllerServiceService, 'getFlow').mockReturnValueOnce(of(mockFlowResponse) as never);

            const result = await new Promise((resolve) =>
                effects.loadControllerServices$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                ControllerServicesActions.loadControllerServicesError({
                    errorResponse: error,
                    loadedTimestamp: stateWithData.loadedTimestamp,
                    status: 'success'
                })
            );
        });
    });

    describe('Controller Services Listing Error', () => {
        it('should handle controller services listing error for initial load', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = ControllerServicesActions.controllerServicesBannerApiError({ error: 'Error loading' });
            action$.next(
                ControllerServicesActions.loadControllerServicesError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.controllerServicesListingError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(false, error);
            expect(result).toEqual(errorAction);
        });

        it('should handle controller services listing error for refresh', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = ControllerServicesActions.controllerServicesBannerApiError({ error: 'Error loading' });
            action$.next(
                ControllerServicesActions.loadControllerServicesError({
                    errorResponse: error,
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.controllerServicesListingError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(true, error);
            expect(result).toEqual(errorAction);
        });
    });

    describe('clearControllerServiceBulletins$', () => {
        it('should dispatch clearControllerServiceBulletinsSuccess action when bulletins are cleared successfully', async () => {
            const { effects, controllerServiceService } = await setup();

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

            jest.spyOn(controllerServiceService, 'clearBulletins').mockReturnValue(of(serviceResponse));
            action$.next(clearControllerServiceBulletins({ request }));

            const result = await new Promise((resolve) =>
                effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe(resolve)
            );

            expect(controllerServiceService.clearBulletins).toHaveBeenCalledWith({
                id: request.componentId,
                fromTimestamp: request.fromTimestamp
            });
            expect(result).toEqual(clearControllerServiceBulletinsSuccess({ response: expectedResponse }));
        });

        it('should handle bulletinsCleared being undefined and default to 0', async () => {
            const { effects, controllerServiceService } = await setup();

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

            jest.spyOn(controllerServiceService, 'clearBulletins').mockReturnValue(of(serviceResponse));
            action$.next(clearControllerServiceBulletins({ request }));

            const result = await new Promise((resolve) =>
                effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(clearControllerServiceBulletinsSuccess({ response: expectedResponse }));
        });

        it('should preserve request fields in the success response', async () => {
            const { effects, controllerServiceService } = await setup();

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

            jest.spyOn(controllerServiceService, 'clearBulletins').mockReturnValue(of(serviceResponse));
            action$.next(clearControllerServiceBulletins({ request }));

            const result = await new Promise<any>((resolve) =>
                effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe(resolve)
            );

            expect(result.response.componentId).toBe(request.componentId);
            expect(result.response.componentType).toBe(request.componentType);
            expect(result.response.bulletinsCleared).toBe(3);
            expect(result.response.bulletins).toEqual([]);
        });

        it('should call service with correct parameters', async () => {
            const { effects, controllerServiceService } = await setup();

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

            jest.spyOn(controllerServiceService, 'clearBulletins').mockReturnValue(of(serviceResponse));
            action$.next(clearControllerServiceBulletins({ request }));

            await new Promise<void>((resolve) =>
                effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe(() => {
                    expect(controllerServiceService.clearBulletins).toHaveBeenCalledWith({
                        id: request.componentId,
                        fromTimestamp: request.fromTimestamp
                    });
                    expect(controllerServiceService.clearBulletins).toHaveBeenCalledTimes(1);
                    resolve();
                })
            );
        });

        it('should handle zero bulletins cleared', async () => {
            const { effects, controllerServiceService } = await setup();

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

            jest.spyOn(controllerServiceService, 'clearBulletins').mockReturnValue(of(serviceResponse));
            action$.next(clearControllerServiceBulletins({ request }));

            const result = await new Promise((resolve) =>
                effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(clearControllerServiceBulletinsSuccess({ response: expectedResponse }));
        });

        it('should handle large number of bulletins cleared', async () => {
            const { effects, controllerServiceService } = await setup();

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

            jest.spyOn(controllerServiceService, 'clearBulletins').mockReturnValue(of(serviceResponse));
            action$.next(clearControllerServiceBulletins({ request }));

            const result = await new Promise((resolve) =>
                effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(clearControllerServiceBulletinsSuccess({ response: expectedResponse }));
        });
    });

    describe('createControllerService$', () => {
        it('should dispatch createControllerServiceSuccess when service is created successfully', async () => {
            const { effects, controllerServiceService } = await setup();

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

            jest.spyOn(controllerServiceService, 'createControllerService').mockReturnValue(of(serviceResponse));
            action$.next(createControllerService({ request }));

            const result = await new Promise((resolve) =>
                effects.createControllerService$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                createControllerServiceSuccess({
                    response: { controllerService: serviceResponse }
                })
            );
            expect(controllerServiceService.createControllerService).toHaveBeenCalledWith(request);
        });

        it('should dispatch snackBarError and close dialogs when creation fails', async () => {
            const { effects, controllerServiceService, errorHelper } = await setup();

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
            jest.spyOn(mockDialog, 'closeAll');
            jest.spyOn(errorHelper as any, 'getErrorString').mockReturnValue('Creation failed');
            jest.spyOn(controllerServiceService, 'createControllerService').mockReturnValue(
                throwError(() => errorResponse)
            );

            action$.next(createControllerService({ request }));

            const result = await new Promise((resolve) =>
                effects.createControllerService$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(ErrorActions.snackBarError({ error: 'Creation failed' }));
            expect(mockDialog.closeAll).toHaveBeenCalled();
        });
    });

    describe('deleteControllerService$', () => {
        it('should dispatch deleteControllerServiceSuccess when service is deleted successfully', async () => {
            const { effects, controllerServiceService } = await setup();

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

            jest.spyOn(controllerServiceService, 'deleteControllerService').mockReturnValue(of(serviceResponse));
            action$.next(deleteControllerService({ request }));

            const result = await new Promise((resolve) =>
                effects.deleteControllerService$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                deleteControllerServiceSuccess({
                    response: { controllerService: serviceResponse }
                })
            );
            expect(controllerServiceService.deleteControllerService).toHaveBeenCalledWith(request);
        });

        it('should dispatch snackBarError when deletion fails', async () => {
            const { effects, controllerServiceService, errorHelper } = await setup();

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

            jest.spyOn(errorHelper as any, 'getErrorString').mockReturnValue('Deletion failed');
            jest.spyOn(controllerServiceService, 'deleteControllerService').mockReturnValue(
                throwError(() => errorResponse)
            );

            action$.next(deleteControllerService({ request }));

            const result = await new Promise((resolve) =>
                effects.deleteControllerService$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(ErrorActions.snackBarError({ error: 'Deletion failed' }));
        });
    });

    describe('deleteControllerServiceSuccess$', () => {
        it('should dispatch loadControllerServices after successful deletion', async () => {
            const { effects } = await setup();

            const response = {
                controllerService: {
                    id: 'deleted-service',
                    component: { name: 'Deleted Service' }
                } as any
            };

            action$.next(deleteControllerServiceSuccess({ response }));

            const result = await new Promise((resolve) =>
                effects.deleteControllerServiceSuccess$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                loadControllerServices({
                    request: { processGroupId: 'root' }
                })
            );
        });
    });
});
