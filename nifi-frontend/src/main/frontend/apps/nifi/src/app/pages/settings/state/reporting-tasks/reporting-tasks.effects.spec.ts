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

import { ReportingTasksEffects } from './reporting-tasks.effects';
import * as ReportingTasksActions from './reporting-tasks.actions';
import { ReportingTaskService } from '../../service/reporting-task.service';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { ComponentType } from '@nifi/shared';
import { ReportingTaskEntity, reportingTasksFeatureKey } from './index';
import { initialState } from './reporting-tasks.reducer';
import { settingsFeatureKey } from '../index';
import * as ErrorActions from '../../../../state/error/error.actions';

describe('ReportingTasksEffects', () => {
    let actions$: ReplaySubject<Action>;
    let effects: ReportingTasksEffects;
    let mockReportingTaskService: jest.Mocked<ReportingTaskService>;
    let mockErrorHelper: jest.Mocked<ErrorHelper>;
    let mockDialog: jest.Mocked<MatDialog>;

    beforeEach(() => {
        const reportingTaskServiceSpy = {
            getReportingTasks: jest.fn(),
            createReportingTask: jest.fn(),
            updateReportingTask: jest.fn(),
            deleteReportingTask: jest.fn(),
            startReportingTask: jest.fn(),
            stopReportingTask: jest.fn(),
            clearBulletins: jest.fn()
        } as unknown as jest.Mocked<ReportingTaskService>;

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

        const propertyTableHelperServiceSpy = {
            getComponentHistory: jest.fn(),
            createNewProperty: jest.fn(),
            createNewService: jest.fn()
        } as unknown as jest.Mocked<PropertyTableHelperService>;

        const extensionTypesServiceSpy = {
            getReportingTaskVersionsForType: jest.fn()
        } as unknown as jest.Mocked<ExtensionTypesService>;

        TestBed.configureTestingModule({
            providers: [
                ReportingTasksEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    initialState: {
                        [settingsFeatureKey]: {
                            [reportingTasksFeatureKey]: initialState
                        }
                    }
                }),
                { provide: ReportingTaskService, useValue: reportingTaskServiceSpy },
                { provide: ManagementControllerServiceService, useValue: {} },
                { provide: ErrorHelper, useValue: errorHelperSpy },
                { provide: Router, useValue: routerSpy },
                { provide: MatDialog, useValue: dialogSpy },
                { provide: PropertyTableHelperService, useValue: propertyTableHelperServiceSpy },
                { provide: ExtensionTypesService, useValue: extensionTypesServiceSpy }
            ]
        });

        effects = TestBed.inject(ReportingTasksEffects);
        mockReportingTaskService = TestBed.inject(ReportingTaskService) as jest.Mocked<ReportingTaskService>;
        mockErrorHelper = TestBed.inject(ErrorHelper) as jest.Mocked<ErrorHelper>;
        mockDialog = TestBed.inject(MatDialog) as jest.Mocked<MatDialog>;
        actions$ = new ReplaySubject(1);
    });

    describe('loadReportingTasks$', () => {
        it('should dispatch loadReportingTasksSuccess action when reporting tasks are loaded successfully', async () => {
            const mockResponse = {
                reportingTasks: [createMockReportingTaskEntity()],
                currentTime: '2023-01-01 12:00:00 EST'
            };
            mockReportingTaskService.getReportingTasks.mockReturnValue(of(mockResponse));

            actions$.next(ReportingTasksActions.loadReportingTasks());

            const result = await firstValueFrom(effects.loadReportingTasks$);

            expect(result).toEqual(
                ReportingTasksActions.loadReportingTasksSuccess({
                    response: {
                        reportingTasks: mockResponse.reportingTasks,
                        loadedTimestamp: mockResponse.currentTime
                    }
                })
            );
            expect(mockReportingTaskService.getReportingTasks).toHaveBeenCalled();
        });

        it('should dispatch loadReportingTasksError with status pending on initial load failure', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            mockReportingTaskService.getReportingTasks.mockReturnValue(throwError(() => errorResponse));

            actions$.next(ReportingTasksActions.loadReportingTasks());

            const result = await firstValueFrom(effects.loadReportingTasks$);

            // On initial load (no existing timestamp), status should be 'pending'
            expect(result).toEqual(
                ReportingTasksActions.loadReportingTasksError({
                    errorResponse,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });
    });

    describe('loadReportingTasksError$', () => {
        it('should handle error on initial load (hasExistingData=false)', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500 });
            const errorAction = ReportingTasksActions.reportingTasksBannerApiError({ error: 'Error loading' });

            mockErrorHelper.handleLoadingError.mockReturnValue(errorAction);

            actions$.next(
                ReportingTasksActions.loadReportingTasksError({
                    errorResponse,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );

            const result = await firstValueFrom(effects.loadReportingTasksError$);

            // Should call handleLoadingError with false for initial load
            expect(mockErrorHelper.handleLoadingError).toHaveBeenCalledWith(false, errorResponse);
            expect(result).toEqual(errorAction);
        });

        it('should handle error on refresh (hasExistingData=true)', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500 });
            const errorAction = ReportingTasksActions.reportingTasksBannerApiError({ error: 'Error loading' });

            mockErrorHelper.handleLoadingError.mockReturnValue(errorAction);

            actions$.next(
                ReportingTasksActions.loadReportingTasksError({
                    errorResponse,
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );

            const result = await firstValueFrom(effects.loadReportingTasksError$);

            // Should call handleLoadingError with true for refresh (existing data)
            expect(mockErrorHelper.handleLoadingError).toHaveBeenCalledWith(true, errorResponse);
            expect(result).toEqual(errorAction);
        });
    });

    describe('createReportingTask$', () => {
        it('should dispatch createReportingTaskSuccess action when reporting task is created successfully', async () => {
            const mockRequest = {
                reportingTaskType: 'org.apache.nifi.TestReportingTask',
                reportingTaskBundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
                revision: { version: 0, clientId: 'test-client' }
            };
            const mockResponse = createMockReportingTaskEntity();
            mockReportingTaskService.createReportingTask.mockReturnValue(of(mockResponse));

            actions$.next(ReportingTasksActions.createReportingTask({ request: mockRequest }));

            const result = await firstValueFrom(effects.createReportingTask$);

            expect(result).toEqual(
                ReportingTasksActions.createReportingTaskSuccess({
                    response: {
                        reportingTask: mockResponse
                    }
                })
            );
            expect(mockReportingTaskService.createReportingTask).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error and close dialog when creating reporting task fails', async () => {
            const mockRequest = {
                reportingTaskType: 'org.apache.nifi.TestReportingTask',
                reportingTaskBundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
                revision: { version: 0, clientId: 'test-client' }
            };
            const errorResponse = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });
            mockReportingTaskService.createReportingTask.mockReturnValue(throwError(() => errorResponse));

            actions$.next(ReportingTasksActions.createReportingTask({ request: mockRequest }));

            const result = await firstValueFrom(effects.createReportingTask$);

            expect(mockDialog.closeAll).toHaveBeenCalled();
            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(result).toEqual(
                ReportingTasksActions.reportingTasksSnackbarApiError({
                    error: 'Test error message'
                })
            );
        });
    });

    describe('deleteReportingTask$', () => {
        it('should dispatch deleteReportingTaskSuccess action when reporting task is deleted successfully', async () => {
            const mockRequest = {
                reportingTask: createMockReportingTaskEntity()
            };
            const mockResponse = createMockReportingTaskEntity();
            mockReportingTaskService.deleteReportingTask.mockReturnValue(of(mockResponse));

            actions$.next(ReportingTasksActions.deleteReportingTask({ request: mockRequest }));

            const result = await firstValueFrom(effects.deleteReportingTask$);

            expect(result).toEqual(
                ReportingTasksActions.deleteReportingTaskSuccess({
                    response: {
                        reportingTask: mockResponse
                    }
                })
            );
            expect(mockReportingTaskService.deleteReportingTask).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error when deleting reporting task fails', async () => {
            const mockRequest = {
                reportingTask: createMockReportingTaskEntity()
            };
            const errorResponse = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            mockReportingTaskService.deleteReportingTask.mockReturnValue(throwError(() => errorResponse));

            actions$.next(ReportingTasksActions.deleteReportingTask({ request: mockRequest }));

            const result = await firstValueFrom(effects.deleteReportingTask$);

            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(result).toEqual(
                ReportingTasksActions.reportingTasksSnackbarApiError({
                    error: 'Test error message'
                })
            );
        });
    });

    describe('startReportingTask$', () => {
        it('should dispatch startReportingTaskSuccess action when reporting task is started successfully', async () => {
            const mockRequest = {
                reportingTask: createMockReportingTaskEntity()
            };
            const mockResponse = createMockReportingTaskEntity();
            mockReportingTaskService.startReportingTask.mockReturnValue(of(mockResponse));

            actions$.next(ReportingTasksActions.startReportingTask({ request: mockRequest }));

            const result = await firstValueFrom(effects.startReportingTask$);

            expect(result).toEqual(
                ReportingTasksActions.startReportingTaskSuccess({
                    response: {
                        reportingTask: mockResponse
                    }
                })
            );
            expect(mockReportingTaskService.startReportingTask).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error when starting reporting task fails', async () => {
            const mockRequest = {
                reportingTask: createMockReportingTaskEntity()
            };
            const errorResponse = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            mockReportingTaskService.startReportingTask.mockReturnValue(throwError(() => errorResponse));

            actions$.next(ReportingTasksActions.startReportingTask({ request: mockRequest }));

            const result = await firstValueFrom(effects.startReportingTask$);

            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(result).toEqual(
                ReportingTasksActions.reportingTasksSnackbarApiError({
                    error: 'Test error message'
                })
            );
        });
    });

    describe('stopReportingTask$', () => {
        it('should dispatch stopReportingTaskSuccess action when reporting task is stopped successfully', async () => {
            const mockRequest = {
                reportingTask: createMockReportingTaskEntity()
            };
            const mockResponse = createMockReportingTaskEntity();
            mockReportingTaskService.stopReportingTask.mockReturnValue(of(mockResponse));

            actions$.next(ReportingTasksActions.stopReportingTask({ request: mockRequest }));

            const result = await firstValueFrom(effects.stopReportingTask$);

            expect(result).toEqual(
                ReportingTasksActions.stopReportingTaskSuccess({
                    response: {
                        reportingTask: mockResponse
                    }
                })
            );
            expect(mockReportingTaskService.stopReportingTask).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error when stopping reporting task fails', async () => {
            const mockRequest = {
                reportingTask: createMockReportingTaskEntity()
            };
            const errorResponse = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            mockReportingTaskService.stopReportingTask.mockReturnValue(throwError(() => errorResponse));

            actions$.next(ReportingTasksActions.stopReportingTask({ request: mockRequest }));

            const result = await firstValueFrom(effects.stopReportingTask$);

            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(result).toEqual(
                ReportingTasksActions.reportingTasksSnackbarApiError({
                    error: 'Test error message'
                })
            );
        });
    });

    describe('clearReportingTaskBulletins$', () => {
        it('should dispatch clearReportingTaskBulletinsSuccess action when bulletins are cleared successfully', async () => {
            const mockRequest = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'test-component-id',
                componentType: ComponentType.ReportingTask
            };
            const mockResponse = {
                bulletinsCleared: 5,
                bulletins: []
            };
            mockReportingTaskService.clearBulletins.mockReturnValue(of(mockResponse));

            actions$.next(ReportingTasksActions.clearReportingTaskBulletins({ request: mockRequest }));

            const result = await firstValueFrom(effects.clearReportingTaskBulletins$);

            expect(result).toEqual(
                ReportingTasksActions.clearReportingTaskBulletinsSuccess({
                    response: {
                        componentId: mockRequest.componentId,
                        bulletinsCleared: 5,
                        bulletins: mockResponse.bulletins || [],
                        componentType: mockRequest.componentType
                    }
                })
            );
            expect(mockReportingTaskService.clearBulletins).toHaveBeenCalledWith({
                id: mockRequest.componentId,
                fromTimestamp: mockRequest.fromTimestamp
            });
        });

        it('should handle undefined bulletinsCleared response', async () => {
            const mockRequest = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'test-component-id',
                componentType: ComponentType.ReportingTask
            };
            const mockResponse = {
                bulletins: []
            }; // No bulletinsCleared property
            mockReportingTaskService.clearBulletins.mockReturnValue(of(mockResponse));

            actions$.next(ReportingTasksActions.clearReportingTaskBulletins({ request: mockRequest }));

            const result = await firstValueFrom(effects.clearReportingTaskBulletins$);

            expect(result).toEqual(
                ReportingTasksActions.clearReportingTaskBulletinsSuccess({
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
                componentType: ComponentType.ReportingTask
            };
            const mockResponse = {
                bulletinsCleared: 3,
                bulletins: []
            };
            mockReportingTaskService.clearBulletins.mockReturnValue(of(mockResponse));

            actions$.next(ReportingTasksActions.clearReportingTaskBulletins({ request: mockRequest }));

            const result = await firstValueFrom(effects.clearReportingTaskBulletins$);

            const successAction = result as any;
            expect(successAction.response.componentId).toBe(mockRequest.componentId);
            expect(successAction.response.componentType).toBe(mockRequest.componentType);
            expect(successAction.response.bulletinsCleared).toBe(3);
            expect(successAction.response.bulletins).toEqual([]);
        });
    });

    describe('configureReportingTask$', () => {
        it('should dispatch configureReportingTaskSuccess action when reporting task is configured successfully', async () => {
            const mockRequest = {
                id: 'test-id',
                uri: 'test-uri',
                payload: {
                    revision: { version: 1, clientId: 'test-client' },
                    component: {
                        id: 'test-id',
                        name: 'Updated Reporting Task'
                    }
                },
                postUpdateNavigation: undefined,
                postUpdateNavigationBoundary: undefined
            };
            const mockResponse = createMockReportingTaskEntity();
            mockReportingTaskService.updateReportingTask.mockReturnValue(of(mockResponse));

            actions$.next(ReportingTasksActions.configureReportingTask({ request: mockRequest }));

            const result = await firstValueFrom(effects.configureReportingTask$);

            expect(result).toEqual(
                ReportingTasksActions.configureReportingTaskSuccess({
                    response: {
                        id: mockRequest.id,
                        reportingTask: mockResponse,
                        postUpdateNavigation: mockRequest.postUpdateNavigation,
                        postUpdateNavigationBoundary: mockRequest.postUpdateNavigationBoundary
                    }
                })
            );
            expect(mockReportingTaskService.updateReportingTask).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error with banner when configuring reporting task fails', async () => {
            const mockRequest = {
                id: 'test-id',
                uri: 'test-uri',
                payload: {
                    revision: { version: 1, clientId: 'test-client' },
                    component: {
                        id: 'test-id',
                        name: 'Updated Reporting Task'
                    }
                },
                postUpdateNavigation: undefined,
                postUpdateNavigationBoundary: undefined
            };
            const errorResponse = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });
            mockReportingTaskService.updateReportingTask.mockReturnValue(throwError(() => errorResponse));

            actions$.next(ReportingTasksActions.configureReportingTask({ request: mockRequest }));

            const result = await firstValueFrom(effects.configureReportingTask$);

            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(result).toEqual(
                ReportingTasksActions.reportingTasksBannerApiError({
                    error: 'Test error message'
                })
            );
        });
    });

    describe('createReportingTaskSuccess$', () => {
        it('should close dialog and dispatch selectReportingTask action', async () => {
            const mockResponse = {
                reportingTask: createMockReportingTaskEntity()
            };

            actions$.next(ReportingTasksActions.createReportingTaskSuccess({ response: mockResponse }));

            const result = await firstValueFrom(effects.createReportingTaskSuccess$);

            expect(mockDialog.closeAll).toHaveBeenCalled();
            expect(result).toEqual(
                ReportingTasksActions.selectReportingTask({
                    request: {
                        id: mockResponse.reportingTask.id
                    }
                })
            );
        });
    });

    describe('reportingTasksBannerApiError$', () => {
        it('should dispatch addBannerError action', async () => {
            const errorMessage = 'Test error message';

            actions$.next(ReportingTasksActions.reportingTasksBannerApiError({ error: errorMessage }));

            const result = await firstValueFrom(effects.reportingTasksBannerApiError$);

            expect(result).toEqual(
                ErrorActions.addBannerError({
                    errorContext: {
                        errors: [errorMessage],
                        context: 'report-tasks' as any
                    }
                })
            );
        });
    });

    describe('reportingTasksSnackbarApiError$', () => {
        it('should dispatch snackBarError action', async () => {
            const errorMessage = 'Test error message';

            actions$.next(ReportingTasksActions.reportingTasksSnackbarApiError({ error: errorMessage }));

            const result = await firstValueFrom(effects.reportingTasksSnackbarApiError$);

            expect(result).toEqual(ErrorActions.snackBarError({ error: errorMessage }));
        });
    });

    // Mock data factory
    function createMockReportingTaskEntity(): ReportingTaskEntity {
        return {
            id: 'test-reporting-task-id',
            uri: 'test-uri',
            permissions: {
                canRead: true,
                canWrite: true
            },
            operatePermissions: {
                canRead: true,
                canWrite: true
            },
            revision: {
                version: 1,
                clientId: 'test-client'
            },
            component: {
                id: 'test-reporting-task-id',
                name: 'Test Reporting Task',
                type: 'org.apache.nifi.TestReportingTask',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
                state: 'STOPPED',
                comments: 'Test comments',
                persistsState: true,
                restricted: false,
                deprecated: false,
                multipleVersionsAvailable: false,
                supportsSensitiveDynamicProperties: false,
                schedulingPeriod: '1 min',
                schedulingStrategy: 'TIMER_DRIVEN',
                defaultSchedulingPeriod: {},
                properties: {},
                descriptors: {},
                customUiUrl: '',
                annotationData: '',
                validationErrors: [],
                validationStatus: 'VALID',
                activeThreadCount: 0
            },
            status: {
                runStatus: 'STOPPED',
                validationStatus: 'VALID',
                activeThreadCount: 0
            },
            bulletins: []
        };
    }
});
