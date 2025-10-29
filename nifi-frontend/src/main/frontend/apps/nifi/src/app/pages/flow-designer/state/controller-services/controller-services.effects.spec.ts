/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import { ControllerServicesEffects } from './controller-services.effects';
import { ControllerServiceService } from '../../service/controller-service.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { ParameterHelperService } from '../../service/parameter-helper.service';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { Client } from '../../../../service/client.service';
import { Storage } from '@nifi/shared';
import { ParameterContextService } from '../../../parameter-contexts/service/parameter-contexts.service';
import { initialState } from './controller-services.reducer';

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
        await TestBed.configureTestingModule({
            providers: [
                ControllerServicesEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        canvas: {
                            controllerServiceListing: controllerServicesState
                        }
                    }
                }),
                {
                    provide: ControllerServiceService,
                    useValue: { getControllerServices: jest.fn(), getFlow: jest.fn() }
                },
                { provide: ErrorHelper, useValue: { handleLoadingError: jest.fn(), getErrorString: jest.fn() } },
                { provide: MatDialog, useValue: { open: jest.fn(), closeAll: jest.fn() } },
                { provide: Router, useValue: { navigate: jest.fn() } },
                { provide: PropertyTableHelperService, useValue: {} },
                { provide: ParameterHelperService, useValue: {} },
                { provide: ExtensionTypesService, useValue: {} },
                { provide: Client, useValue: {} },
                { provide: Storage, useValue: { setItem: jest.fn(), getItem: jest.fn() } },
                { provide: ParameterContextService, useValue: { getParameterContext: jest.fn() } }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const effects = TestBed.inject(ControllerServicesEffects);
        const controllerServiceService = TestBed.inject(ControllerServiceService);
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
});
