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
import { provideMockStore } from '@ngrx/store/testing';
import { Action } from '@ngrx/store';
import { ReplaySubject, of, throwError, firstValueFrom } from 'rxjs';
import { take } from 'rxjs/operators';
import { Router } from '@angular/router';
import { MatDialog } from '@angular/material/dialog';
import { HttpErrorResponse } from '@angular/common/http';

import { ManagementControllerServicesEffects } from './management-controller-services.effects';
import * as ManagementControllerServicesActions from './management-controller-services.actions';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { Client } from '../../../../service/client.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { ComponentType } from '@nifi/shared';
import { managementControllerServicesFeatureKey } from './index';
import { initialState } from './management-controller-services.reducer';
import { settingsFeatureKey } from '../index';

describe('ManagementControllerServicesEffects', () => {
    let actions$: ReplaySubject<Action>;
    let effects: ManagementControllerServicesEffects;
    let mockManagementControllerServiceService: jest.Mocked<ManagementControllerServiceService>;
    let mockErrorHelper: jest.Mocked<ErrorHelper>;

    beforeEach(() => {
        const managementControllerServiceServiceSpy = {
            getControllerServices: jest.fn(),
            createControllerService: jest.fn(),
            getControllerService: jest.fn(),
            getPropertyDescriptor: jest.fn(),
            updateControllerService: jest.fn(),
            deleteControllerService: jest.fn(),
            clearBulletins: jest.fn()
        } as unknown as jest.Mocked<ManagementControllerServiceService>;

        const errorHelperSpy = {
            handleLoadingError: jest.fn().mockReturnValue({ type: '[Error] Loading Error' }),
            getErrorString: jest.fn(),
            showErrorInContext: jest.fn(),
            fullScreenError: jest.fn()
        } as unknown as jest.Mocked<ErrorHelper>;

        TestBed.configureTestingModule({
            providers: [
                ManagementControllerServicesEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    initialState: {
                        [settingsFeatureKey]: {
                            [managementControllerServicesFeatureKey]: initialState
                        }
                    }
                }),
                {
                    provide: ManagementControllerServiceService,
                    useValue: managementControllerServiceServiceSpy
                },
                {
                    provide: Client,
                    useValue: {
                        getClientId: jest.fn().mockReturnValue('test-client-id')
                    }
                },
                {
                    provide: ErrorHelper,
                    useValue: errorHelperSpy
                },
                {
                    provide: Router,
                    useValue: {
                        navigate: jest.fn()
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
                    provide: PropertyTableHelperService,
                    useValue: {
                        getComponentHistory: jest.fn(),
                        createNewProperty: jest.fn(),
                        createNewService: jest.fn()
                    }
                },
                {
                    provide: ExtensionTypesService,
                    useValue: {
                        getControllerServiceVersionsForType: jest.fn()
                    }
                }
            ]
        });

        effects = TestBed.inject(ManagementControllerServicesEffects);
        mockManagementControllerServiceService = TestBed.inject(
            ManagementControllerServiceService
        ) as jest.Mocked<ManagementControllerServiceService>;
        mockErrorHelper = TestBed.inject(ErrorHelper) as jest.Mocked<ErrorHelper>;
        actions$ = new ReplaySubject(1);
    });

    describe('loadManagementControllerServices$', () => {
        it('should dispatch loadManagementControllerServicesSuccess when services are loaded successfully', async () => {
            const mockResponse = {
                controllerServices: [],
                currentTime: '2023-01-01 12:00:00 EST'
            };
            mockManagementControllerServiceService.getControllerServices.mockReturnValue(of(mockResponse));

            actions$.next(ManagementControllerServicesActions.loadManagementControllerServices());

            const result = await firstValueFrom(effects.loadManagementControllerServices$);

            expect(result).toEqual(
                ManagementControllerServicesActions.loadManagementControllerServicesSuccess({
                    response: {
                        controllerServices: mockResponse.controllerServices,
                        loadedTimestamp: mockResponse.currentTime
                    }
                })
            );
            expect(mockManagementControllerServiceService.getControllerServices).toHaveBeenCalled();
        });

        it('should dispatch loadManagementControllerServicesError with status pending on initial load failure', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            mockManagementControllerServiceService.getControllerServices.mockReturnValue(
                throwError(() => errorResponse)
            );

            actions$.next(ManagementControllerServicesActions.loadManagementControllerServices());

            const result = await firstValueFrom(effects.loadManagementControllerServices$);

            expect(result).toEqual(
                ManagementControllerServicesActions.loadManagementControllerServicesError({
                    errorResponse,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });
    });

    describe('loadManagementControllerServicesError$', () => {
        it('should handle error on initial load (hasExistingData=false)', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500 });
            const errorAction = ManagementControllerServicesActions.managementControllerServicesBannerApiError({
                error: 'Error loading'
            });

            mockErrorHelper.handleLoadingError.mockReturnValue(errorAction);

            actions$.next(
                ManagementControllerServicesActions.loadManagementControllerServicesError({
                    errorResponse,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );

            const result = await firstValueFrom(effects.loadManagementControllerServicesError$);

            expect(mockErrorHelper.handleLoadingError).toHaveBeenCalledWith(false, errorResponse);
            expect(result).toEqual(errorAction);
        });

        it('should handle error on refresh (hasExistingData=true)', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500 });
            const errorAction = ManagementControllerServicesActions.managementControllerServicesBannerApiError({
                error: 'Error loading'
            });

            mockErrorHelper.handleLoadingError.mockReturnValue(errorAction);

            actions$.next(
                ManagementControllerServicesActions.loadManagementControllerServicesError({
                    errorResponse,
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );

            const result = await firstValueFrom(effects.loadManagementControllerServicesError$);

            expect(mockErrorHelper.handleLoadingError).toHaveBeenCalledWith(true, errorResponse);
            expect(result).toEqual(errorAction);
        });
    });

    describe('clearControllerServiceBulletins$', () => {
        it('should dispatch clearControllerServiceBulletinsSuccess action when bulletins are cleared successfully', () => {
            const request = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'controller-service-1',
                componentType: ComponentType.ControllerService
            };

            const mockResponse = {
                bulletinsCleared: 3,
                bulletins: []
            };

            mockManagementControllerServiceService.clearBulletins.mockReturnValue(of(mockResponse));

            const action = ManagementControllerServicesActions.clearControllerServiceBulletins({ request });
            actions$.next(action);

            effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe((result) => {
                expect(result).toEqual(
                    ManagementControllerServicesActions.clearControllerServiceBulletinsSuccess({
                        response: {
                            componentId: request.componentId,
                            bulletinsCleared: mockResponse.bulletinsCleared,
                            bulletins: mockResponse.bulletins || [],
                            componentType: request.componentType
                        }
                    })
                );
            });

            expect(mockManagementControllerServiceService.clearBulletins).toHaveBeenCalledWith({
                id: request.componentId,
                fromTimestamp: request.fromTimestamp
            });
        });

        it('should handle bulletinsCleared being undefined and default to 0', () => {
            const request = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'controller-service-2',
                componentType: ComponentType.ControllerService
            };

            const mockResponse = {
                // bulletinsCleared is undefined
                bulletins: []
            };

            mockManagementControllerServiceService.clearBulletins.mockReturnValue(of(mockResponse));

            const action = ManagementControllerServicesActions.clearControllerServiceBulletins({ request });
            actions$.next(action);

            effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe((result) => {
                expect(result).toEqual(
                    ManagementControllerServicesActions.clearControllerServiceBulletinsSuccess({
                        response: {
                            componentId: request.componentId,
                            bulletinsCleared: 0, // Should default to 0
                            bulletins: mockResponse.bulletins || [],
                            componentType: request.componentType
                        }
                    })
                );
            });
        });

        it('should preserve request fields in the success response', () => {
            const request = {
                uri: 'different-uri',
                fromTimestamp: '2023-06-15T08:30:00.000Z',
                componentId: 'another-controller-service',
                componentType: ComponentType.ControllerService
            };

            const mockResponse = {
                bulletinsCleared: 7,
                bulletins: []
            };

            mockManagementControllerServiceService.clearBulletins.mockReturnValue(of(mockResponse));

            const action = ManagementControllerServicesActions.clearControllerServiceBulletins({ request });
            actions$.next(action);

            effects.clearControllerServiceBulletins$.pipe(take(1)).subscribe((result) => {
                expect(result).toEqual(
                    ManagementControllerServicesActions.clearControllerServiceBulletinsSuccess({
                        response: {
                            componentId: request.componentId, // From request
                            bulletinsCleared: mockResponse.bulletinsCleared, // From response
                            bulletins: mockResponse.bulletins || [], // From response
                            componentType: request.componentType // From request
                        }
                    })
                );
            });
        });
    });
});
