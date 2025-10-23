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

import * as ManagementControllerServicesActions from './management-controller-services.actions';
import { ManagementControllerServicesEffects } from './management-controller-services.effects';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { Client } from '../../../../service/client.service';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { initialState } from './management-controller-services.reducer';

describe('ManagementControllerServicesEffects', () => {
    interface SetupOptions {
        managementControllerServicesState?: any;
    }

    const mockResponse = {
        controllerServices: [],
        currentTime: '2023-01-01 12:00:00 EST'
    };

    async function setup({ managementControllerServicesState = initialState }: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            providers: [
                ManagementControllerServicesEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        settings: {
                            managementControllerServices: managementControllerServicesState
                        }
                    }
                }),
                { provide: ManagementControllerServiceService, useValue: { getControllerServices: jest.fn() } },
                { provide: ErrorHelper, useValue: { handleLoadingError: jest.fn() } },
                { provide: MatDialog, useValue: { open: jest.fn(), closeAll: jest.fn() } },
                { provide: Router, useValue: { navigate: jest.fn() } },
                { provide: PropertyTableHelperService, useValue: {} },
                { provide: Client, useValue: {} },
                { provide: ExtensionTypesService, useValue: {} }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const effects = TestBed.inject(ManagementControllerServicesEffects);
        const managementControllerServiceService = TestBed.inject(ManagementControllerServiceService);
        const errorHelper = TestBed.inject(ErrorHelper);

        return { effects, store, managementControllerServiceService, errorHelper };
    }

    let action$: ReplaySubject<Action>;
    beforeEach(() => {
        action$ = new ReplaySubject<Action>();
    });

    it('should create', async () => {
        const { effects } = await setup();
        expect(effects).toBeTruthy();
    });

    describe('Load Management Controller Services', () => {
        it('should load management controller services successfully', async () => {
            const { effects, managementControllerServiceService } = await setup();

            action$.next(ManagementControllerServicesActions.loadManagementControllerServices());
            jest.spyOn(managementControllerServiceService, 'getControllerServices').mockReturnValueOnce(
                of(mockResponse) as never
            );

            const result = await new Promise((resolve) =>
                effects.loadManagementControllerServices$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                ManagementControllerServicesActions.loadManagementControllerServicesSuccess({
                    response: {
                        controllerServices: mockResponse.controllerServices,
                        loadedTimestamp: mockResponse.currentTime
                    }
                })
            );
        });

        it('should fail to load management controller services on initial load with hasExistingData=false', async () => {
            const { effects, managementControllerServiceService } = await setup();

            action$.next(ManagementControllerServicesActions.loadManagementControllerServices());
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(managementControllerServiceService, 'getControllerServices').mockImplementationOnce(() =>
                throwError(() => error)
            );

            const result = await new Promise((resolve) =>
                effects.loadManagementControllerServices$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                ManagementControllerServicesActions.loadManagementControllerServicesError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });

        it('should fail to load management controller services on refresh with hasExistingData=true', async () => {
            const stateWithData = { ...initialState, loadedTimestamp: '2023-01-01 11:00:00 EST' };
            const { effects, managementControllerServiceService } = await setup({
                managementControllerServicesState: stateWithData
            });

            action$.next(ManagementControllerServicesActions.loadManagementControllerServices());
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(managementControllerServiceService, 'getControllerServices').mockImplementationOnce(() =>
                throwError(() => error)
            );

            const result = await new Promise((resolve) =>
                effects.loadManagementControllerServices$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                ManagementControllerServicesActions.loadManagementControllerServicesError({
                    errorResponse: error,
                    loadedTimestamp: stateWithData.loadedTimestamp,
                    status: 'success'
                })
            );
        });
    });

    describe('Load Management Controller Services Error', () => {
        it('should handle management controller services error for initial load', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = ManagementControllerServicesActions.managementControllerServicesBannerApiError({
                error: 'Error message'
            });
            action$.next(
                ManagementControllerServicesActions.loadManagementControllerServicesError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadManagementControllerServicesError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(false, error);
            expect(result).toEqual(errorAction);
        });

        it('should handle management controller services error for refresh', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = ManagementControllerServicesActions.managementControllerServicesBannerApiError({
                error: 'Error message'
            });
            action$.next(
                ManagementControllerServicesActions.loadManagementControllerServicesError({
                    errorResponse: error,
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadManagementControllerServicesError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(true, error);
            expect(result).toEqual(errorAction);
        });
    });
});
