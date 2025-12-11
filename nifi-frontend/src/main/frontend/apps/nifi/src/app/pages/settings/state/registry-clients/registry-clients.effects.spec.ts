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

import { RegistryClientsEffects } from './registry-clients.effects';
import * as RegistryClientsActions from './registry-clients.actions';
import { RegistryClientService } from '../../service/registry-client.service';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { ComponentType } from '@nifi/shared';
import { RegistryClientEntity } from '../../../../state/shared';
import { registryClientsFeatureKey } from './index';
import { initialState } from './registry-clients.reducer';
import { settingsFeatureKey } from '../index';
import * as ErrorActions from '../../../../state/error/error.actions';

describe('RegistryClientsEffects', () => {
    let actions$: ReplaySubject<Action>;
    let effects: RegistryClientsEffects;
    let mockRegistryClientService: jest.Mocked<RegistryClientService>;
    let mockErrorHelper: jest.Mocked<ErrorHelper>;
    let mockDialog: jest.Mocked<MatDialog>;

    beforeEach(() => {
        const registryClientServiceSpy = {
            getRegistryClients: jest.fn(),
            createRegistryClient: jest.fn(),
            updateRegistryClient: jest.fn(),
            deleteRegistryClient: jest.fn(),
            clearBulletins: jest.fn()
        } as unknown as jest.Mocked<RegistryClientService>;

        const errorHelperSpy = {
            handleLoadingError: jest.fn().mockReturnValue({ type: '[Error] Loading Error' }),
            getErrorString: jest.fn().mockReturnValue('Test error message'),
            showErrorInContext: jest.fn(),
            fullScreenError: jest.fn().mockReturnValue({ type: 'FULL_SCREEN_ERROR' })
        } as unknown as jest.Mocked<ErrorHelper>;

        const dialogSpy = {
            open: jest.fn(),
            closeAll: jest.fn()
        } as unknown as jest.Mocked<MatDialog>;

        const routerSpy = {
            navigate: jest.fn()
        } as unknown as jest.Mocked<Router>;

        TestBed.configureTestingModule({
            providers: [
                RegistryClientsEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    initialState: {
                        [settingsFeatureKey]: {
                            [registryClientsFeatureKey]: initialState
                        }
                    }
                }),
                { provide: RegistryClientService, useValue: registryClientServiceSpy },
                { provide: ManagementControllerServiceService, useValue: {} },
                { provide: ErrorHelper, useValue: errorHelperSpy },
                { provide: Router, useValue: routerSpy },
                { provide: MatDialog, useValue: dialogSpy },
                { provide: PropertyTableHelperService, useValue: {} }
            ]
        });

        effects = TestBed.inject(RegistryClientsEffects);
        mockRegistryClientService = TestBed.inject(RegistryClientService) as jest.Mocked<RegistryClientService>;
        mockErrorHelper = TestBed.inject(ErrorHelper) as jest.Mocked<ErrorHelper>;
        mockDialog = TestBed.inject(MatDialog) as jest.Mocked<MatDialog>;
        actions$ = new ReplaySubject(1);
    });

    describe('loadRegistryClients$', () => {
        it('should dispatch loadRegistryClientsSuccess action when registry clients are loaded successfully', async () => {
            const mockResponse = {
                registries: [createMockRegistryClientEntity()],
                currentTime: '2023-01-01 12:00:00 EST'
            };
            mockRegistryClientService.getRegistryClients.mockReturnValue(of(mockResponse));

            actions$.next(RegistryClientsActions.loadRegistryClients());

            const result = await firstValueFrom(effects.loadRegistryClients$);

            expect(result).toEqual(
                RegistryClientsActions.loadRegistryClientsSuccess({
                    response: {
                        registryClients: mockResponse.registries,
                        loadedTimestamp: mockResponse.currentTime
                    }
                })
            );
            expect(mockRegistryClientService.getRegistryClients).toHaveBeenCalled();
        });

        it('should dispatch loadRegistryClientsError with status pending on initial load failure', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            mockRegistryClientService.getRegistryClients.mockReturnValue(throwError(() => errorResponse));

            actions$.next(RegistryClientsActions.loadRegistryClients());

            const result = await firstValueFrom(effects.loadRegistryClients$);

            expect(result).toEqual(
                RegistryClientsActions.loadRegistryClientsError({
                    errorResponse,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });
    });

    describe('loadRegistryClientsError$', () => {
        it('should handle error on initial load (hasExistingData=false)', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500 });
            const errorAction = RegistryClientsActions.registryClientsBannerApiError({ error: 'Error loading' });

            mockErrorHelper.handleLoadingError.mockReturnValue(errorAction);

            actions$.next(
                RegistryClientsActions.loadRegistryClientsError({
                    errorResponse,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );

            const result = await firstValueFrom(effects.loadRegistryClientsError$);

            expect(mockErrorHelper.handleLoadingError).toHaveBeenCalledWith(false, errorResponse);
            expect(result).toEqual(errorAction);
        });

        it('should handle error on refresh (hasExistingData=true)', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500 });
            const errorAction = RegistryClientsActions.registryClientsBannerApiError({ error: 'Error loading' });

            mockErrorHelper.handleLoadingError.mockReturnValue(errorAction);

            actions$.next(
                RegistryClientsActions.loadRegistryClientsError({
                    errorResponse,
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );

            const result = await firstValueFrom(effects.loadRegistryClientsError$);

            expect(mockErrorHelper.handleLoadingError).toHaveBeenCalledWith(true, errorResponse);
            expect(result).toEqual(errorAction);
        });
    });

    describe('createRegistryClient$', () => {
        it('should dispatch createRegistryClientSuccess action when registry client is created successfully', async () => {
            const mockRequest = {
                revision: { version: 0, clientId: 'test-client' },
                disconnectedNodeAcknowledged: false,
                component: {
                    name: 'Test Registry Client',
                    type: 'org.apache.nifi.TestRegistryClient',
                    bundle: {
                        group: 'org.apache.nifi',
                        artifact: 'test-bundle',
                        version: '1.0.0'
                    },
                    description: 'Test Description'
                }
            };
            const mockResponse = createMockRegistryClientEntity();
            mockRegistryClientService.createRegistryClient.mockReturnValue(of(mockResponse));

            actions$.next(RegistryClientsActions.createRegistryClient({ request: mockRequest }));

            const result = await firstValueFrom(effects.createRegistryClient$);

            expect(result).toEqual(
                RegistryClientsActions.createRegistryClientSuccess({
                    response: {
                        registryClient: mockResponse
                    }
                })
            );
            expect(mockRegistryClientService.createRegistryClient).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error and close dialog when creating registry client fails', async () => {
            const mockRequest = {
                revision: { version: 0, clientId: 'test-client' },
                disconnectedNodeAcknowledged: false,
                component: {
                    name: 'Test Registry Client',
                    type: 'org.apache.nifi.TestRegistryClient',
                    bundle: {
                        group: 'org.apache.nifi',
                        artifact: 'test-bundle',
                        version: '1.0.0'
                    },
                    description: 'Test Description'
                }
            };
            const errorResponse = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });
            mockRegistryClientService.createRegistryClient.mockReturnValue(throwError(() => errorResponse));

            actions$.next(RegistryClientsActions.createRegistryClient({ request: mockRequest }));

            const result = await firstValueFrom(effects.createRegistryClient$);

            expect(mockDialog.closeAll).toHaveBeenCalled();
            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(result).toEqual(
                RegistryClientsActions.registryClientsSnackbarApiError({
                    error: 'Test error message'
                })
            );
        });
    });

    describe('deleteRegistryClient$', () => {
        it('should dispatch deleteRegistryClientSuccess action when registry client is deleted successfully', async () => {
            const mockRequest = {
                registryClient: createMockRegistryClientEntity()
            };
            const mockResponse = createMockRegistryClientEntity();
            mockRegistryClientService.deleteRegistryClient.mockReturnValue(of(mockResponse));

            actions$.next(RegistryClientsActions.deleteRegistryClient({ request: mockRequest }));

            const result = await firstValueFrom(effects.deleteRegistryClient$);

            expect(result).toEqual(
                RegistryClientsActions.deleteRegistryClientSuccess({
                    response: {
                        registryClient: mockResponse
                    }
                })
            );
            expect(mockRegistryClientService.deleteRegistryClient).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error when deleting registry client fails', async () => {
            const mockRequest = {
                registryClient: createMockRegistryClientEntity()
            };
            const errorResponse = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            mockRegistryClientService.deleteRegistryClient.mockReturnValue(throwError(() => errorResponse));

            actions$.next(RegistryClientsActions.deleteRegistryClient({ request: mockRequest }));

            const result = await firstValueFrom(effects.deleteRegistryClient$);

            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(result).toEqual(
                RegistryClientsActions.registryClientsSnackbarApiError({
                    error: 'Test error message'
                })
            );
        });
    });

    describe('clearRegistryClientBulletins$', () => {
        it('should dispatch clearRegistryClientBulletinsSuccess action when bulletins are cleared successfully', async () => {
            const mockRequest = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'test-component-id',
                componentType: ComponentType.FlowRegistryClient
            };
            const mockResponse = {
                bulletinsCleared: 5,
                bulletins: []
            };
            mockRegistryClientService.clearBulletins.mockReturnValue(of(mockResponse));

            actions$.next(RegistryClientsActions.clearRegistryClientBulletins({ request: mockRequest }));

            const result = await firstValueFrom(effects.clearRegistryClientBulletins$);

            expect(result).toEqual(
                RegistryClientsActions.clearRegistryClientBulletinsSuccess({
                    response: {
                        componentId: mockRequest.componentId,
                        bulletinsCleared: 5,
                        bulletins: mockResponse.bulletins || [],
                        componentType: mockRequest.componentType
                    }
                })
            );
            expect(mockRegistryClientService.clearBulletins).toHaveBeenCalledWith({
                id: mockRequest.componentId,
                fromTimestamp: mockRequest.fromTimestamp
            });
        });

        it('should handle undefined bulletinsCleared response', async () => {
            const mockRequest = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'test-component-id',
                componentType: ComponentType.FlowRegistryClient
            };
            const mockResponse = {
                bulletins: []
            }; // No bulletinsCleared property
            mockRegistryClientService.clearBulletins.mockReturnValue(of(mockResponse));

            actions$.next(RegistryClientsActions.clearRegistryClientBulletins({ request: mockRequest }));

            const result = await firstValueFrom(effects.clearRegistryClientBulletins$);

            expect(result).toEqual(
                RegistryClientsActions.clearRegistryClientBulletinsSuccess({
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
                componentId: 'test-component-id',
                componentType: ComponentType.FlowRegistryClient
            };
            const mockResponse = {
                bulletinsCleared: 3,
                bulletins: []
            };
            mockRegistryClientService.clearBulletins.mockReturnValue(of(mockResponse));

            actions$.next(RegistryClientsActions.clearRegistryClientBulletins({ request: mockRequest }));

            const result = await firstValueFrom(effects.clearRegistryClientBulletins$);

            const successAction = result as any;
            expect(successAction.response.componentId).toBe(mockRequest.componentId);
            expect(successAction.response.componentType).toBe(mockRequest.componentType);
            expect(successAction.response.bulletinsCleared).toBe(3);
        });
    });

    describe('configureRegistryClient$', () => {
        it('should dispatch configureRegistryClientSuccess action when registry client is configured successfully', async () => {
            const mockRequest = {
                id: 'test-id',
                uri: 'test-uri',
                payload: {
                    revision: { version: 1, clientId: 'test-client' },
                    component: {
                        id: 'test-id',
                        name: 'Updated Registry Client'
                    }
                },
                postUpdateNavigation: undefined,
                postUpdateNavigationBoundary: undefined
            };
            const mockResponse = createMockRegistryClientEntity();
            mockRegistryClientService.updateRegistryClient.mockReturnValue(of(mockResponse));

            actions$.next(RegistryClientsActions.configureRegistryClient({ request: mockRequest }));

            const result = await firstValueFrom(effects.configureRegistryClient$);

            expect(result).toEqual(
                RegistryClientsActions.configureRegistryClientSuccess({
                    response: {
                        id: mockRequest.id,
                        registryClient: mockResponse,
                        postUpdateNavigation: mockRequest.postUpdateNavigation,
                        postUpdateNavigationBoundary: mockRequest.postUpdateNavigationBoundary
                    }
                })
            );
            expect(mockRegistryClientService.updateRegistryClient).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error with banner when showErrorInContext returns true', async () => {
            const mockRequest = {
                id: 'test-id',
                uri: 'test-uri',
                payload: {
                    revision: { version: 1, clientId: 'test-client' },
                    component: {
                        id: 'test-id',
                        name: 'Updated Registry Client'
                    }
                },
                postUpdateNavigation: undefined,
                postUpdateNavigationBoundary: undefined
            };
            const errorResponse = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });
            mockRegistryClientService.updateRegistryClient.mockReturnValue(throwError(() => errorResponse));

            actions$.next(RegistryClientsActions.configureRegistryClient({ request: mockRequest }));

            const result = await firstValueFrom(effects.configureRegistryClient$);

            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(result).toEqual(
                RegistryClientsActions.registryClientsBannerApiError({
                    error: 'Test error message'
                })
            );
        });
    });

    describe('createRegistryClientSuccess$', () => {
        it('should close dialog and dispatch selectClient action', async () => {
            const mockResponse = {
                registryClient: createMockRegistryClientEntity()
            };

            actions$.next(RegistryClientsActions.createRegistryClientSuccess({ response: mockResponse }));

            const result = await firstValueFrom(effects.createRegistryClientSuccess$);

            expect(mockDialog.closeAll).toHaveBeenCalled();
            expect(result).toEqual(
                RegistryClientsActions.selectClient({
                    request: {
                        id: mockResponse.registryClient.id
                    }
                })
            );
        });
    });

    describe('registryClientsBannerApiError$', () => {
        it('should dispatch addBannerError action', async () => {
            const errorMessage = 'Test error message';

            actions$.next(RegistryClientsActions.registryClientsBannerApiError({ error: errorMessage }));

            const result = await firstValueFrom(effects.registryClientsBannerApiError$);

            expect(result).toEqual(
                ErrorActions.addBannerError({
                    errorContext: {
                        errors: [errorMessage],
                        context: 'registry-clients' as any
                    }
                })
            );
        });
    });

    describe('registryClientsSnackbarApiError$', () => {
        it('should dispatch snackBarError action', async () => {
            const errorMessage = 'Test error message';

            actions$.next(RegistryClientsActions.registryClientsSnackbarApiError({ error: errorMessage }));

            const result = await firstValueFrom(effects.registryClientsSnackbarApiError$);

            expect(result).toEqual(ErrorActions.snackBarError({ error: errorMessage }));
        });
    });

    // Mock data factory
    function createMockRegistryClientEntity(): RegistryClientEntity {
        return {
            id: 'test-registry-client-id',
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
                id: 'test-registry-client-id',
                name: 'Test Registry Client',
                description: 'Test Description',
                type: 'org.apache.nifi.TestRegistryClient',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
                properties: {},
                descriptors: {},
                validationErrors: [],
                validationStatus: 'VALID'
            },
            bulletins: []
        };
    }
});
