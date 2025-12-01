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

import { FlowAnalysisRulesEffects } from './flow-analysis-rules.effects';
import * as FlowAnalysisRulesActions from './flow-analysis-rules.actions';
import { FlowAnalysisRuleService } from '../../service/flow-analysis-rule.service';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { ComponentType } from '@nifi/shared';
import { FlowAnalysisRuleEntity, flowAnalysisRulesFeatureKey } from './index';
import { initialState } from './flow-analysis-rules.reducer';
import { settingsFeatureKey } from '../index';
import * as ErrorActions from '../../../../state/error/error.actions';

describe('FlowAnalysisRulesEffects', () => {
    let actions$: ReplaySubject<Action>;
    let effects: FlowAnalysisRulesEffects;
    let mockFlowAnalysisRuleService: jest.Mocked<FlowAnalysisRuleService>;
    let mockErrorHelper: jest.Mocked<ErrorHelper>;
    let mockDialog: jest.Mocked<MatDialog>;

    beforeEach(() => {
        const flowAnalysisRuleServiceSpy = {
            getFlowAnalysisRule: jest.fn(),
            createFlowAnalysisRule: jest.fn(),
            updateFlowAnalysisRule: jest.fn(),
            deleteFlowAnalysisRule: jest.fn(),
            setEnable: jest.fn(),
            clearBulletins: jest.fn()
        } as unknown as jest.Mocked<FlowAnalysisRuleService>;

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
            getFlowAnalysisRuleVersionsForType: jest.fn()
        } as unknown as jest.Mocked<ExtensionTypesService>;

        TestBed.configureTestingModule({
            providers: [
                FlowAnalysisRulesEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    initialState: {
                        [settingsFeatureKey]: {
                            [flowAnalysisRulesFeatureKey]: initialState
                        }
                    }
                }),
                { provide: FlowAnalysisRuleService, useValue: flowAnalysisRuleServiceSpy },
                { provide: ManagementControllerServiceService, useValue: {} },
                { provide: ErrorHelper, useValue: errorHelperSpy },
                { provide: Router, useValue: routerSpy },
                { provide: MatDialog, useValue: dialogSpy },
                { provide: PropertyTableHelperService, useValue: propertyTableHelperServiceSpy },
                { provide: ExtensionTypesService, useValue: extensionTypesServiceSpy }
            ]
        });

        effects = TestBed.inject(FlowAnalysisRulesEffects);
        mockFlowAnalysisRuleService = TestBed.inject(FlowAnalysisRuleService) as jest.Mocked<FlowAnalysisRuleService>;
        mockErrorHelper = TestBed.inject(ErrorHelper) as jest.Mocked<ErrorHelper>;
        mockDialog = TestBed.inject(MatDialog) as jest.Mocked<MatDialog>;
        actions$ = new ReplaySubject(1);
    });

    describe('loadFlowAnalysisRule$', () => {
        it('should dispatch loadFlowAnalysisRulesSuccess action when flow analysis rules are loaded successfully', async () => {
            const mockResponse = {
                flowAnalysisRules: [createMockFlowAnalysisRuleEntity()],
                currentTime: '2023-01-01 12:00:00 EST'
            };
            mockFlowAnalysisRuleService.getFlowAnalysisRule.mockReturnValue(of(mockResponse));

            actions$.next(FlowAnalysisRulesActions.loadFlowAnalysisRules());

            const result = await firstValueFrom(effects.loadFlowAnalysisRule$);

            expect(result).toEqual(
                FlowAnalysisRulesActions.loadFlowAnalysisRulesSuccess({
                    response: {
                        flowAnalysisRules: mockResponse.flowAnalysisRules,
                        loadedTimestamp: mockResponse.currentTime
                    }
                })
            );
            expect(mockFlowAnalysisRuleService.getFlowAnalysisRule).toHaveBeenCalled();
        });

        it('should dispatch loadFlowAnalysisRulesError with status pending on initial load failure', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            mockFlowAnalysisRuleService.getFlowAnalysisRule.mockReturnValue(throwError(() => errorResponse));

            actions$.next(FlowAnalysisRulesActions.loadFlowAnalysisRules());

            const result = await firstValueFrom(effects.loadFlowAnalysisRule$);

            expect(result).toEqual(
                FlowAnalysisRulesActions.loadFlowAnalysisRulesError({
                    errorResponse,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });
    });

    describe('loadFlowAnalysisRulesError$', () => {
        it('should handle error on initial load (hasExistingData=false)', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500 });
            const errorAction = FlowAnalysisRulesActions.flowAnalysisRuleBannerApiError({ error: 'Error loading' });

            mockErrorHelper.handleLoadingError.mockReturnValue(errorAction);

            actions$.next(
                FlowAnalysisRulesActions.loadFlowAnalysisRulesError({
                    errorResponse,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );

            const result = await firstValueFrom(effects.loadFlowAnalysisRulesError$);

            expect(mockErrorHelper.handleLoadingError).toHaveBeenCalledWith(false, errorResponse);
            expect(result).toEqual(errorAction);
        });

        it('should handle error on refresh (hasExistingData=true)', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500 });
            const errorAction = FlowAnalysisRulesActions.flowAnalysisRuleBannerApiError({ error: 'Error loading' });

            mockErrorHelper.handleLoadingError.mockReturnValue(errorAction);

            actions$.next(
                FlowAnalysisRulesActions.loadFlowAnalysisRulesError({
                    errorResponse,
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );

            const result = await firstValueFrom(effects.loadFlowAnalysisRulesError$);

            expect(mockErrorHelper.handleLoadingError).toHaveBeenCalledWith(true, errorResponse);
            expect(result).toEqual(errorAction);
        });
    });

    describe('createFlowAnalysisRule$', () => {
        it('should dispatch createFlowAnalysisRuleSuccess action when flow analysis rule is created successfully', async () => {
            const mockRequest = {
                flowAnalysisRuleType: 'org.apache.nifi.TestFlowAnalysisRule',
                flowAnalysisRuleBundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
                revision: { version: 0, clientId: 'test-client' }
            };
            const mockResponse = createMockFlowAnalysisRuleEntity();
            mockFlowAnalysisRuleService.createFlowAnalysisRule.mockReturnValue(of(mockResponse));

            actions$.next(FlowAnalysisRulesActions.createFlowAnalysisRule({ request: mockRequest }));

            const result = await firstValueFrom(effects.createFlowAnalysisRule$);

            expect(result).toEqual(
                FlowAnalysisRulesActions.createFlowAnalysisRuleSuccess({
                    response: {
                        flowAnalysisRule: mockResponse
                    }
                })
            );
            expect(mockFlowAnalysisRuleService.createFlowAnalysisRule).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error and close dialog when creating flow analysis rule fails', async () => {
            const mockRequest = {
                flowAnalysisRuleType: 'org.apache.nifi.TestFlowAnalysisRule',
                flowAnalysisRuleBundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
                revision: { version: 0, clientId: 'test-client' }
            };
            const errorResponse = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });
            mockFlowAnalysisRuleService.createFlowAnalysisRule.mockReturnValue(throwError(() => errorResponse));

            actions$.next(FlowAnalysisRulesActions.createFlowAnalysisRule({ request: mockRequest }));

            const result = await firstValueFrom(effects.createFlowAnalysisRule$);

            expect(mockDialog.closeAll).toHaveBeenCalled();
            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(result).toEqual(
                FlowAnalysisRulesActions.flowAnalysisRuleSnackbarApiError({
                    error: 'Test error message'
                })
            );
        });
    });

    describe('deleteFlowAnalysisRule$', () => {
        it('should dispatch deleteFlowAnalysisRuleSuccess action when flow analysis rule is deleted successfully', async () => {
            const mockRequest = {
                flowAnalysisRule: createMockFlowAnalysisRuleEntity()
            };
            const mockResponse = createMockFlowAnalysisRuleEntity();
            mockFlowAnalysisRuleService.deleteFlowAnalysisRule.mockReturnValue(of(mockResponse));

            actions$.next(FlowAnalysisRulesActions.deleteFlowAnalysisRule({ request: mockRequest }));

            const result = await firstValueFrom(effects.deleteFlowAnalysisRule$);

            expect(result).toEqual(
                FlowAnalysisRulesActions.deleteFlowAnalysisRuleSuccess({
                    response: {
                        flowAnalysisRule: mockResponse
                    }
                })
            );
            expect(mockFlowAnalysisRuleService.deleteFlowAnalysisRule).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error when deleting flow analysis rule fails', async () => {
            const mockRequest = {
                flowAnalysisRule: createMockFlowAnalysisRuleEntity()
            };
            const errorResponse = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            mockFlowAnalysisRuleService.deleteFlowAnalysisRule.mockReturnValue(throwError(() => errorResponse));

            actions$.next(FlowAnalysisRulesActions.deleteFlowAnalysisRule({ request: mockRequest }));

            const result = await firstValueFrom(effects.deleteFlowAnalysisRule$);

            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(result).toEqual(
                FlowAnalysisRulesActions.flowAnalysisRuleSnackbarApiError({
                    error: 'Test error message'
                })
            );
        });
    });

    describe('enableFlowAnalysisRule$', () => {
        it('should dispatch enableFlowAnalysisRuleSuccess action when flow analysis rule is enabled successfully', async () => {
            const mockRequest = {
                id: 'test-id',
                flowAnalysisRule: createMockFlowAnalysisRuleEntity()
            };
            const mockResponse = createMockFlowAnalysisRuleEntity();
            mockFlowAnalysisRuleService.setEnable.mockReturnValue(of(mockResponse));

            actions$.next(FlowAnalysisRulesActions.enableFlowAnalysisRule({ request: mockRequest }));

            const result = await firstValueFrom(effects.enableFlowAnalysisRule$);

            expect(result).toEqual(
                FlowAnalysisRulesActions.enableFlowAnalysisRuleSuccess({
                    response: {
                        id: mockRequest.id,
                        flowAnalysisRule: mockResponse
                    }
                })
            );
            expect(mockFlowAnalysisRuleService.setEnable).toHaveBeenCalledWith(mockRequest, true);
        });

        it('should handle error when enabling flow analysis rule fails', async () => {
            const mockRequest = {
                id: 'test-id',
                flowAnalysisRule: createMockFlowAnalysisRuleEntity()
            };
            const errorResponse = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            mockFlowAnalysisRuleService.setEnable.mockReturnValue(throwError(() => errorResponse));

            actions$.next(FlowAnalysisRulesActions.enableFlowAnalysisRule({ request: mockRequest }));

            const result = await firstValueFrom(effects.enableFlowAnalysisRule$);

            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(result).toEqual(
                FlowAnalysisRulesActions.flowAnalysisRuleSnackbarApiError({
                    error: 'Test error message'
                })
            );
        });
    });

    describe('disableFlowAnalysisRule$', () => {
        it('should dispatch disableFlowAnalysisRuleSuccess action when flow analysis rule is disabled successfully', async () => {
            const mockRequest = {
                id: 'test-id',
                flowAnalysisRule: createMockFlowAnalysisRuleEntity()
            };
            const mockResponse = createMockFlowAnalysisRuleEntity();
            mockFlowAnalysisRuleService.setEnable.mockReturnValue(of(mockResponse));

            actions$.next(FlowAnalysisRulesActions.disableFlowAnalysisRule({ request: mockRequest }));

            const result = await firstValueFrom(effects.disableFlowAnalysisRule$);

            expect(result).toEqual(
                FlowAnalysisRulesActions.disableFlowAnalysisRuleSuccess({
                    response: {
                        id: mockRequest.id,
                        flowAnalysisRule: mockResponse
                    }
                })
            );
            expect(mockFlowAnalysisRuleService.setEnable).toHaveBeenCalledWith(mockRequest, false);
        });

        it('should handle error when disabling flow analysis rule fails', async () => {
            const mockRequest = {
                id: 'test-id',
                flowAnalysisRule: createMockFlowAnalysisRuleEntity()
            };
            const errorResponse = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            mockFlowAnalysisRuleService.setEnable.mockReturnValue(throwError(() => errorResponse));

            actions$.next(FlowAnalysisRulesActions.disableFlowAnalysisRule({ request: mockRequest }));

            const result = await firstValueFrom(effects.disableFlowAnalysisRule$);

            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(result).toEqual(
                FlowAnalysisRulesActions.flowAnalysisRuleSnackbarApiError({
                    error: 'Test error message'
                })
            );
        });
    });

    describe('clearFlowAnalysisRuleBulletins$', () => {
        it('should dispatch clearFlowAnalysisRuleBulletinsSuccess action when bulletins are cleared successfully', async () => {
            const mockRequest = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'test-component-id',
                componentType: ComponentType.FlowAnalysisRule
            };
            const mockResponse = {
                bulletinsCleared: 5,
                bulletins: []
            };
            mockFlowAnalysisRuleService.clearBulletins.mockReturnValue(of(mockResponse));

            actions$.next(FlowAnalysisRulesActions.clearFlowAnalysisRuleBulletins({ request: mockRequest }));

            const result = await firstValueFrom(effects.clearFlowAnalysisRuleBulletins$);

            expect(result).toEqual(
                FlowAnalysisRulesActions.clearFlowAnalysisRuleBulletinsSuccess({
                    response: {
                        componentId: mockRequest.componentId,
                        bulletinsCleared: 5,
                        bulletins: mockResponse.bulletins || [],
                        componentType: mockRequest.componentType
                    }
                })
            );
            expect(mockFlowAnalysisRuleService.clearBulletins).toHaveBeenCalledWith({
                id: mockRequest.componentId,
                fromTimestamp: mockRequest.fromTimestamp
            });
        });

        it('should handle undefined bulletinsCleared response', async () => {
            const mockRequest = {
                uri: 'test-uri',
                fromTimestamp: '2023-01-01T12:00:00.000Z',
                componentId: 'test-component-id',
                componentType: ComponentType.FlowAnalysisRule
            };
            const mockResponse = {
                bulletins: []
            }; // No bulletinsCleared property
            mockFlowAnalysisRuleService.clearBulletins.mockReturnValue(of(mockResponse));

            actions$.next(FlowAnalysisRulesActions.clearFlowAnalysisRuleBulletins({ request: mockRequest }));

            const result = await firstValueFrom(effects.clearFlowAnalysisRuleBulletins$);

            expect(result).toEqual(
                FlowAnalysisRulesActions.clearFlowAnalysisRuleBulletinsSuccess({
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
                componentType: ComponentType.FlowAnalysisRule
            };
            const mockResponse = {
                bulletinsCleared: 3,
                bulletins: []
            };
            mockFlowAnalysisRuleService.clearBulletins.mockReturnValue(of(mockResponse));

            actions$.next(FlowAnalysisRulesActions.clearFlowAnalysisRuleBulletins({ request: mockRequest }));

            const result = await firstValueFrom(effects.clearFlowAnalysisRuleBulletins$);

            const successAction = result as any;
            expect(successAction.response.componentId).toBe(mockRequest.componentId);
            expect(successAction.response.componentType).toBe(mockRequest.componentType);
            expect(successAction.response.bulletinsCleared).toBe(3);
            expect(successAction.response.bulletins).toEqual([]);
        });
    });

    describe('configureFlowAnalysisRule$', () => {
        it('should dispatch configureFlowAnalysisRuleSuccess action when flow analysis rule is configured successfully', async () => {
            const mockRequest = {
                id: 'test-id',
                uri: 'test-uri',
                payload: {
                    revision: { version: 1, clientId: 'test-client' },
                    component: {
                        id: 'test-id',
                        name: 'Updated Flow Analysis Rule'
                    }
                },
                postUpdateNavigation: undefined,
                postUpdateNavigationBoundary: undefined
            };
            const mockResponse = createMockFlowAnalysisRuleEntity();
            mockFlowAnalysisRuleService.updateFlowAnalysisRule.mockReturnValue(of(mockResponse));

            actions$.next(FlowAnalysisRulesActions.configureFlowAnalysisRule({ request: mockRequest }));

            const result = await firstValueFrom(effects.configureFlowAnalysisRule$);

            expect(result).toEqual(
                FlowAnalysisRulesActions.configureFlowAnalysisRuleSuccess({
                    response: {
                        id: mockRequest.id,
                        flowAnalysisRule: mockResponse,
                        postUpdateNavigation: mockRequest.postUpdateNavigation,
                        postUpdateNavigationBoundary: mockRequest.postUpdateNavigationBoundary
                    }
                })
            );
            expect(mockFlowAnalysisRuleService.updateFlowAnalysisRule).toHaveBeenCalledWith(mockRequest);
        });

        it('should handle error with banner when configuring flow analysis rule fails', async () => {
            const mockRequest = {
                id: 'test-id',
                uri: 'test-uri',
                payload: {
                    revision: { version: 1, clientId: 'test-client' },
                    component: {
                        id: 'test-id',
                        name: 'Updated Flow Analysis Rule'
                    }
                },
                postUpdateNavigation: undefined,
                postUpdateNavigationBoundary: undefined
            };
            const errorResponse = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });
            mockFlowAnalysisRuleService.updateFlowAnalysisRule.mockReturnValue(throwError(() => errorResponse));

            actions$.next(FlowAnalysisRulesActions.configureFlowAnalysisRule({ request: mockRequest }));

            const result = await firstValueFrom(effects.configureFlowAnalysisRule$);

            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(result).toEqual(
                FlowAnalysisRulesActions.flowAnalysisRuleBannerApiError({
                    error: 'Test error message'
                })
            );
        });
    });

    describe('createFlowAnalysisRuleSuccess$', () => {
        it('should close dialog and dispatch selectFlowAnalysisRule action', async () => {
            const mockResponse = {
                flowAnalysisRule: createMockFlowAnalysisRuleEntity()
            };

            actions$.next(FlowAnalysisRulesActions.createFlowAnalysisRuleSuccess({ response: mockResponse }));

            const result = await firstValueFrom(effects.createFlowAnalysisRuleSuccess$);

            expect(mockDialog.closeAll).toHaveBeenCalled();
            expect(result).toEqual(
                FlowAnalysisRulesActions.selectFlowAnalysisRule({
                    request: {
                        id: mockResponse.flowAnalysisRule.id
                    }
                })
            );
        });
    });

    describe('flowAnalysisRuleBannerApiError$', () => {
        it('should dispatch addBannerError action', async () => {
            const errorMessage = 'Test error message';

            actions$.next(FlowAnalysisRulesActions.flowAnalysisRuleBannerApiError({ error: errorMessage }));

            const result = await firstValueFrom(effects.flowAnalysisRuleBannerApiError$);

            expect(result).toEqual(
                ErrorActions.addBannerError({
                    errorContext: {
                        errors: [errorMessage],
                        context: 'flow-analysis-rules' as any
                    }
                })
            );
        });
    });

    describe('flowAnalysisRuleSnackbarApiError$', () => {
        it('should dispatch snackBarError action', async () => {
            const errorMessage = 'Test error message';

            actions$.next(FlowAnalysisRulesActions.flowAnalysisRuleSnackbarApiError({ error: errorMessage }));

            const result = await firstValueFrom(effects.flowAnalysisRuleSnackbarApiError$);

            expect(result).toEqual(ErrorActions.snackBarError({ error: errorMessage }));
        });
    });

    // Mock data factory
    function createMockFlowAnalysisRuleEntity(): FlowAnalysisRuleEntity {
        return {
            id: 'test-flow-analysis-rule-id',
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
                id: 'test-flow-analysis-rule-id',
                name: 'Test Flow Analysis Rule',
                type: 'org.apache.nifi.TestFlowAnalysisRule',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
                state: 'DISABLED',
                comments: 'Test comments',
                persistsState: true,
                restricted: false,
                deprecated: false,
                multipleVersionsAvailable: false,
                supportsSensitiveDynamicProperties: false,
                properties: {},
                descriptors: {},
                customUiUrl: '',
                annotationData: '',
                validationErrors: [],
                validationStatus: 'VALID'
            },
            status: {
                runStatus: 'DISABLED',
                validationStatus: 'VALID'
            },
            bulletins: []
        };
    }
});
