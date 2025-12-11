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
import { Router } from '@angular/router';
import { MatDialog } from '@angular/material/dialog';
import { HttpErrorResponse } from '@angular/common/http';

import { ParameterProvidersEffects } from './parameter-providers.effects';
import * as ParameterProviderActions from './parameter-providers.actions';
import { ParameterProviderService } from '../../service/parameter-provider.service';
import { Client } from '../../../../service/client.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { ComponentType } from '@nifi/shared';
import { ParameterProviderEntity, parameterProvidersFeatureKey } from './index';
import { initialParameterProvidersState } from './parameter-providers.reducer';
import { settingsFeatureKey } from '../index';

describe('ParameterProvidersEffects', () => {
    let actions$: ReplaySubject<Action>;
    let effects: ParameterProvidersEffects;
    let mockParameterProviderService: jest.Mocked<ParameterProviderService>;
    let mockErrorHelper: jest.Mocked<ErrorHelper>;
    let mockDialog: jest.Mocked<MatDialog>;

    // Mock data factories
    function createMockParameterProviderEntity(): ParameterProviderEntity {
        return {
            id: 'test-parameter-provider-id',
            uri: 'test-uri',
            permissions: {
                canRead: true,
                canWrite: true
            },
            revision: {
                version: 1,
                clientId: 'test-client'
            },
            component: {
                id: 'test-parameter-provider-id',
                name: 'Test Parameter Provider',
                type: 'org.apache.nifi.TestParameterProvider',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
                properties: {},
                descriptors: {},
                validationErrors: [],
                affectedComponents: [],
                parameterGroupConfigurations: [],
                comments: '',
                deprecated: false,
                extensionMissing: false,
                multipleVersionsAvailable: false,
                persistsState: false,
                referencingParameterContexts: [],
                restricted: false,
                validationStatus: 'VALID'
            },
            bulletins: []
        } as ParameterProviderEntity;
    }

    beforeEach(() => {
        const parameterProviderServiceSpy = {
            getParameterProviders: jest.fn(),
            createParameterProvider: jest.fn(),
            updateParameterProvider: jest.fn(),
            deleteParameterProvider: jest.fn(),
            fetchParameters: jest.fn(),
            applyParameters: jest.fn(),
            pollParameterProviderParametersUpdateRequest: jest.fn(),
            deleteParameterProviderParametersUpdateRequest: jest.fn(),
            clearBulletins: jest.fn()
        } as unknown as jest.Mocked<ParameterProviderService>;

        const clientSpy = {
            getClientId: jest.fn().mockReturnValue('test-client-id')
        } as unknown as jest.Mocked<Client>;

        const errorHelperSpy = {
            handleLoadingError: jest.fn().mockReturnValue({ type: '[Error] Loading Error' }),
            getErrorString: jest.fn().mockReturnValue('Test error message'),
            showErrorInContext: jest.fn().mockReturnValue(true),
            fullScreenError: jest.fn()
        } as unknown as jest.Mocked<ErrorHelper>;

        const routerSpy = {
            navigate: jest.fn()
        } as unknown as jest.Mocked<Router>;

        const dialogSpy = {
            open: jest.fn(),
            closeAll: jest.fn()
        } as unknown as jest.Mocked<MatDialog>;

        TestBed.configureTestingModule({
            providers: [
                ParameterProvidersEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    initialState: {
                        [settingsFeatureKey]: {
                            [parameterProvidersFeatureKey]: initialParameterProvidersState
                        }
                    }
                }),
                {
                    provide: ParameterProviderService,
                    useValue: parameterProviderServiceSpy
                },
                {
                    provide: Client,
                    useValue: clientSpy
                },
                {
                    provide: ErrorHelper,
                    useValue: errorHelperSpy
                },
                {
                    provide: Router,
                    useValue: routerSpy
                },
                {
                    provide: MatDialog,
                    useValue: dialogSpy
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
                    provide: ManagementControllerServiceService,
                    useValue: {
                        getControllerServices: jest.fn(),
                        createControllerService: jest.fn(),
                        getControllerService: jest.fn(),
                        updateControllerService: jest.fn(),
                        deleteControllerService: jest.fn()
                    }
                }
            ]
        });

        effects = TestBed.inject(ParameterProvidersEffects);
        mockParameterProviderService = TestBed.inject(
            ParameterProviderService
        ) as jest.Mocked<ParameterProviderService>;
        mockErrorHelper = TestBed.inject(ErrorHelper) as jest.Mocked<ErrorHelper>;
        mockDialog = TestBed.inject(MatDialog) as jest.Mocked<MatDialog>;
        actions$ = new ReplaySubject(1);
    });

    describe('loadParameterProviders$', () => {
        it('should dispatch loadParameterProvidersSuccess action when parameter providers are loaded successfully', async () => {
            const mockResponse = {
                parameterProviders: [createMockParameterProviderEntity()],
                currentTime: '2023-01-01 12:00:00 EST'
            };

            mockParameterProviderService.getParameterProviders.mockReturnValue(of(mockResponse));

            actions$.next(ParameterProviderActions.loadParameterProviders());

            const action = await firstValueFrom(effects.loadParameterProviders$);

            expect(action).toEqual(
                ParameterProviderActions.loadParameterProvidersSuccess({
                    response: {
                        parameterProviders: mockResponse.parameterProviders,
                        loadedTimestamp: mockResponse.currentTime
                    }
                })
            );
            expect(mockParameterProviderService.getParameterProviders).toHaveBeenCalled();
        });

        it('should dispatch loadParameterProvidersError with status pending on initial load failure', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            mockParameterProviderService.getParameterProviders.mockReturnValue(throwError(() => errorResponse));

            actions$.next(ParameterProviderActions.loadParameterProviders());

            const action = await firstValueFrom(effects.loadParameterProviders$);

            expect(action).toEqual(
                ParameterProviderActions.loadParameterProvidersError({
                    errorResponse,
                    loadedTimestamp: initialParameterProvidersState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });
    });

    describe('loadParameterProvidersError$', () => {
        it('should handle error on initial load (hasExistingData=false)', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500 });
            const errorAction = ParameterProviderActions.parameterProvidersBannerApiError({ error: 'Error loading' });

            mockErrorHelper.handleLoadingError.mockReturnValue(errorAction);

            actions$.next(
                ParameterProviderActions.loadParameterProvidersError({
                    errorResponse,
                    loadedTimestamp: initialParameterProvidersState.loadedTimestamp,
                    status: 'pending'
                })
            );

            const action = await firstValueFrom(effects.loadParameterProvidersError$);

            expect(mockErrorHelper.handleLoadingError).toHaveBeenCalledWith(false, errorResponse);
            expect(action).toEqual(errorAction);
        });

        it('should handle error on refresh (hasExistingData=true)', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500 });
            const errorAction = ParameterProviderActions.parameterProvidersBannerApiError({ error: 'Error loading' });

            mockErrorHelper.handleLoadingError.mockReturnValue(errorAction);

            actions$.next(
                ParameterProviderActions.loadParameterProvidersError({
                    errorResponse,
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );

            const action = await firstValueFrom(effects.loadParameterProvidersError$);

            expect(mockErrorHelper.handleLoadingError).toHaveBeenCalledWith(true, errorResponse);
            expect(action).toEqual(errorAction);
        });
    });

    describe('createParameterProvider$', () => {
        it('should dispatch createParameterProviderSuccess action when parameter provider is created successfully', async () => {
            const mockRequest = {
                parameterProviderType: 'org.apache.nifi.TestParameterProvider',
                parameterProviderBundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
                revision: {
                    clientId: 'test-client-id',
                    version: 0
                }
            };
            const mockResponse = createMockParameterProviderEntity();

            mockParameterProviderService.createParameterProvider.mockReturnValue(of(mockResponse));

            actions$.next(ParameterProviderActions.createParameterProvider({ request: mockRequest }));

            const action = await firstValueFrom(effects.createParameterProvider$);

            expect(action).toEqual(
                ParameterProviderActions.createParameterProviderSuccess({
                    response: {
                        parameterProvider: mockResponse
                    }
                })
            );
            expect(mockParameterProviderService.createParameterProvider).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error and close dialog when creating parameter provider fails', async () => {
            const mockRequest = {
                parameterProviderType: 'org.apache.nifi.TestParameterProvider',
                parameterProviderBundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
                revision: {
                    clientId: 'test-client-id',
                    version: 0
                }
            };
            const mockError = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });

            mockParameterProviderService.createParameterProvider.mockReturnValue(throwError(() => mockError));

            actions$.next(ParameterProviderActions.createParameterProvider({ request: mockRequest }));

            const action = await firstValueFrom(effects.createParameterProvider$);

            expect(mockDialog.closeAll).toHaveBeenCalled();
            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(mockError);
            expect(action.type).toContain('Snackbar Error');
        });
    });

    describe('deleteParameterProvider$', () => {
        it('should dispatch deleteParameterProviderSuccess action when parameter provider is deleted successfully', async () => {
            const mockParameterProvider = createMockParameterProviderEntity();
            const mockRequest = { parameterProvider: mockParameterProvider };

            mockParameterProviderService.deleteParameterProvider.mockReturnValue(of(mockParameterProvider));

            actions$.next(ParameterProviderActions.deleteParameterProvider({ request: mockRequest }));

            const action = await firstValueFrom(effects.deleteParameterProvider);

            expect(action).toEqual(
                ParameterProviderActions.deleteParameterProviderSuccess({
                    response: {
                        parameterProvider: mockParameterProvider
                    }
                })
            );
            expect(mockParameterProviderService.deleteParameterProvider).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error when deleting parameter provider fails', async () => {
            const mockParameterProvider = createMockParameterProviderEntity();
            const mockRequest = { parameterProvider: mockParameterProvider };
            const mockError = new HttpErrorResponse({ status: 404, statusText: 'Not Found' });

            mockParameterProviderService.deleteParameterProvider.mockReturnValue(throwError(() => mockError));

            actions$.next(ParameterProviderActions.deleteParameterProvider({ request: mockRequest }));

            const action = await firstValueFrom(effects.deleteParameterProvider);

            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(mockError);
            expect(action.type).toContain('Snackbar Error');
        });
    });

    describe('clearParameterProviderBulletins$', () => {
        it('should dispatch clearParameterProviderBulletinsSuccess action when bulletins are cleared successfully', async () => {
            const mockRequest = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'test-parameter-provider-id',
                componentType: ComponentType.ParameterProvider
            };
            const mockResponse = {
                bulletinsCleared: 5,
                bulletins: []
            };

            mockParameterProviderService.clearBulletins.mockReturnValue(of(mockResponse));

            actions$.next(ParameterProviderActions.clearParameterProviderBulletins({ request: mockRequest }));

            const action = await firstValueFrom(effects.clearParameterProviderBulletins$);

            expect(action).toEqual(
                ParameterProviderActions.clearParameterProviderBulletinsSuccess({
                    response: {
                        componentId: mockRequest.componentId,
                        bulletinsCleared: mockResponse.bulletinsCleared,
                        bulletins: mockResponse.bulletins || [],
                        componentType: mockRequest.componentType
                    }
                })
            );
            expect(mockParameterProviderService.clearBulletins).toHaveBeenCalledWith({
                id: mockRequest.componentId,
                fromTimestamp: mockRequest.fromTimestamp
            });
        });

        it('should handle undefined bulletinsCleared response', async () => {
            const mockRequest = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'test-parameter-provider-id',
                componentType: ComponentType.ParameterProvider
            };
            const mockResponse = {
                bulletins: []
            }; // No bulletinsCleared property

            mockParameterProviderService.clearBulletins.mockReturnValue(of(mockResponse));

            actions$.next(ParameterProviderActions.clearParameterProviderBulletins({ request: mockRequest }));

            const action = await firstValueFrom(effects.clearParameterProviderBulletins$);

            expect(action).toEqual(
                ParameterProviderActions.clearParameterProviderBulletinsSuccess({
                    response: {
                        componentId: mockRequest.componentId,
                        bulletinsCleared: 0, // Should default to 0
                        bulletins: mockResponse.bulletins || [],
                        componentType: mockRequest.componentType
                    }
                })
            );
        });

        it('should pass through all required fields in the success response', async () => {
            const mockRequest = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'test-parameter-provider-id',
                componentType: ComponentType.ParameterProvider
            };
            const mockResponse = {
                bulletinsCleared: 3,
                bulletins: []
            };

            mockParameterProviderService.clearBulletins.mockReturnValue(of(mockResponse));

            actions$.next(ParameterProviderActions.clearParameterProviderBulletins({ request: mockRequest }));

            const action = await firstValueFrom(effects.clearParameterProviderBulletins$);

            const response = (action as any).response;
            expect(response.componentId).toBe(mockRequest.componentId);
            expect(response.bulletinsCleared).toBe(mockResponse.bulletinsCleared);
            expect(response.componentType).toBe(mockRequest.componentType);
            expect(response.bulletins).toEqual([]);
        });
    });

    describe('fetchParameterProviderParametersAndOpenDialog$', () => {
        it('should dispatch fetchParameterProviderParametersSuccess action when parameters are fetched successfully', async () => {
            const mockRequest = {
                id: 'test-parameter-provider-id',
                revision: {
                    version: 1,
                    clientId: 'test-client-id'
                }
            };
            const mockResponse = createMockParameterProviderEntity();

            mockParameterProviderService.fetchParameters.mockReturnValue(of(mockResponse));

            actions$.next(
                ParameterProviderActions.fetchParameterProviderParametersAndOpenDialog({ request: mockRequest })
            );

            const action = await firstValueFrom(effects.fetchParameterProviderParametersAndOpenDialog$);

            expect(action).toEqual(
                ParameterProviderActions.fetchParameterProviderParametersSuccess({
                    response: { parameterProvider: mockResponse }
                })
            );
            expect(mockParameterProviderService.fetchParameters).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error when fetching parameters fails', async () => {
            const mockRequest = {
                id: 'test-parameter-provider-id',
                revision: {
                    version: 1,
                    clientId: 'test-client-id'
                }
            };
            const mockError = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });

            mockParameterProviderService.fetchParameters.mockReturnValue(throwError(() => mockError));
            mockErrorHelper.showErrorInContext.mockReturnValue(true);

            actions$.next(
                ParameterProviderActions.fetchParameterProviderParametersAndOpenDialog({ request: mockRequest })
            );

            const action = await firstValueFrom(effects.fetchParameterProviderParametersAndOpenDialog$);

            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(mockError);
            expect(action.type).toContain('Snackbar Error');
        });

        it('should handle full screen error when showErrorInContext returns false', async () => {
            const mockRequest = {
                id: 'test-parameter-provider-id',
                revision: {
                    version: 1,
                    clientId: 'test-client-id'
                }
            };
            const mockError = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            const mockFullScreenErrorAction = { type: 'FULL_SCREEN_ERROR' };

            mockParameterProviderService.fetchParameters.mockReturnValue(throwError(() => mockError));
            mockErrorHelper.showErrorInContext.mockReturnValue(false);
            mockErrorHelper.fullScreenError.mockReturnValue(mockFullScreenErrorAction);

            actions$.next(
                ParameterProviderActions.fetchParameterProviderParametersAndOpenDialog({ request: mockRequest })
            );

            const action = await firstValueFrom(effects.fetchParameterProviderParametersAndOpenDialog$);

            expect(action).toEqual(mockFullScreenErrorAction);
            expect(mockErrorHelper.fullScreenError).toHaveBeenCalledWith(mockError);
        });
    });

    describe('configureParameterProvider$', () => {
        it('should dispatch configureParameterProviderSuccess action when parameter provider is configured successfully', async () => {
            const mockRequest = {
                id: 'test-parameter-provider-id',
                uri: 'test-uri',
                payload: {
                    revision: { version: 1, clientId: 'test-client-id' },
                    component: { name: 'Updated Parameter Provider' }
                },
                postUpdateNavigation: undefined,
                postUpdateNavigationBoundary: undefined
            };
            const mockResponse = createMockParameterProviderEntity();

            mockParameterProviderService.updateParameterProvider.mockReturnValue(of(mockResponse));

            actions$.next(ParameterProviderActions.configureParameterProvider({ request: mockRequest }));

            const action = await firstValueFrom(effects.configureParameterProvider$);

            expect(action).toEqual(
                ParameterProviderActions.configureParameterProviderSuccess({
                    response: {
                        id: mockRequest.id,
                        parameterProvider: mockResponse,
                        postUpdateNavigation: mockRequest.postUpdateNavigation,
                        postUpdateNavigationBoundary: mockRequest.postUpdateNavigationBoundary
                    }
                })
            );
            expect(mockParameterProviderService.updateParameterProvider).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error with banner when showErrorInContext returns true', async () => {
            const mockRequest = {
                id: 'test-parameter-provider-id',
                uri: 'test-uri',
                payload: {
                    revision: { version: 1, clientId: 'test-client-id' },
                    component: { name: 'Updated Parameter Provider' }
                },
                postUpdateNavigation: undefined,
                postUpdateNavigationBoundary: undefined
            };
            const mockError = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });

            mockParameterProviderService.updateParameterProvider.mockReturnValue(throwError(() => mockError));
            mockErrorHelper.showErrorInContext.mockReturnValue(true);

            actions$.next(ParameterProviderActions.configureParameterProvider({ request: mockRequest }));

            const action = await firstValueFrom(effects.configureParameterProvider$);

            expect(action).toEqual(
                ParameterProviderActions.parameterProvidersBannerApiError({
                    error: 'Test error message'
                })
            );
            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(mockError);
        });

        it('should handle full screen error when showErrorInContext returns false', async () => {
            const mockRequest = {
                id: 'test-parameter-provider-id',
                uri: 'test-uri',
                payload: {
                    revision: { version: 1, clientId: 'test-client-id' },
                    component: { name: 'Updated Parameter Provider' }
                },
                postUpdateNavigation: undefined,
                postUpdateNavigationBoundary: undefined
            };
            const mockError = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            const mockFullScreenErrorAction = { type: 'FULL_SCREEN_ERROR' };

            mockParameterProviderService.updateParameterProvider.mockReturnValue(throwError(() => mockError));
            mockErrorHelper.showErrorInContext.mockReturnValue(false);
            mockErrorHelper.fullScreenError.mockReturnValue(mockFullScreenErrorAction);

            actions$.next(ParameterProviderActions.configureParameterProvider({ request: mockRequest }));

            const action = await firstValueFrom(effects.configureParameterProvider$);

            expect(action).toEqual(mockFullScreenErrorAction);
            expect(mockErrorHelper.fullScreenError).toHaveBeenCalledWith(mockError);
        });
    });
});
