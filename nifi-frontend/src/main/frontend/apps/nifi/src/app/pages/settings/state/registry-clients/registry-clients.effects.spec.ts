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

import * as RegistryClientsActions from './registry-clients.actions';
import { RegistryClientsEffects } from './registry-clients.effects';
import { RegistryClientService } from '../../service/registry-client.service';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { Client } from '../../../../service/client.service';
import { NiFiCommon } from '@nifi/shared';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { initialState } from './registry-clients.reducer';

describe('RegistryClientsEffects', () => {
    interface SetupOptions {
        registryClientsState?: any;
    }

    const mockRegistryClientsResponse = {
        registries: [],
        currentTime: '2023-01-01 12:00:00 EST'
    };

    async function setup({ registryClientsState = initialState }: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            providers: [
                RegistryClientsEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        settings: {
                            registryClients: registryClientsState
                        }
                    }
                }),
                {
                    provide: RegistryClientService,
                    useValue: {
                        getRegistryClients: jest.fn()
                    }
                },
                {
                    provide: ManagementControllerServiceService,
                    useValue: {}
                },
                {
                    provide: ErrorHelper,
                    useValue: {
                        handleLoadingError: jest.fn()
                    }
                },
                {
                    provide: MatDialog,
                    useValue: { open: jest.fn(), closeAll: jest.fn() }
                },
                {
                    provide: Router,
                    useValue: { navigate: jest.fn() }
                },
                { provide: PropertyTableHelperService, useValue: {} },
                { provide: Client, useValue: {} },
                { provide: NiFiCommon, useValue: {} },
                { provide: ClusterConnectionService, useValue: {} },
                { provide: ExtensionTypesService, useValue: {} }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const effects = TestBed.inject(RegistryClientsEffects);
        const registryClientService = TestBed.inject(RegistryClientService);
        const errorHelper = TestBed.inject(ErrorHelper);

        return { effects, store, registryClientService, errorHelper };
    }

    let action$: ReplaySubject<Action>;
    beforeEach(() => {
        action$ = new ReplaySubject<Action>();
    });

    it('should create', async () => {
        const { effects } = await setup();
        expect(effects).toBeTruthy();
    });

    describe('Load Registry Clients', () => {
        it('should load registry clients successfully', async () => {
            const { effects, registryClientService } = await setup();

            action$.next(RegistryClientsActions.loadRegistryClients());
            jest.spyOn(registryClientService, 'getRegistryClients').mockReturnValueOnce(
                of(mockRegistryClientsResponse) as never
            );
            const result = await new Promise((resolve) =>
                effects.loadRegistryClients$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                RegistryClientsActions.loadRegistryClientsSuccess({
                    response: {
                        registryClients: mockRegistryClientsResponse.registries,
                        loadedTimestamp: mockRegistryClientsResponse.currentTime
                    }
                })
            );
        });

        it('should fail to load registry clients on initial load with hasExistingData=false', async () => {
            const { effects, registryClientService } = await setup();

            action$.next(RegistryClientsActions.loadRegistryClients());
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(registryClientService, 'getRegistryClients').mockImplementationOnce(() =>
                throwError(() => error)
            );

            const result = await new Promise((resolve) =>
                effects.loadRegistryClients$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                RegistryClientsActions.loadRegistryClientsError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });

        it('should fail to load registry clients on refresh with hasExistingData=true', async () => {
            const stateWithData = {
                ...initialState,
                loadedTimestamp: '2023-01-01 11:00:00 EST'
            };
            const { effects, registryClientService } = await setup({
                registryClientsState: stateWithData
            });

            action$.next(RegistryClientsActions.loadRegistryClients());
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(registryClientService, 'getRegistryClients').mockImplementationOnce(() =>
                throwError(() => error)
            );

            const result = await new Promise((resolve) =>
                effects.loadRegistryClients$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                RegistryClientsActions.loadRegistryClientsError({
                    errorResponse: error,
                    loadedTimestamp: stateWithData.loadedTimestamp,
                    status: 'success'
                })
            );
        });

        it('should handle loadRegistryClientsError with pending status (no existing data)', async () => {
            const { effects, errorHelper } = await setup();

            action$.next(
                RegistryClientsActions.loadRegistryClientsError({
                    errorResponse: new HttpErrorResponse({ status: 500 }),
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );

            const errorAction = RegistryClientsActions.registryClientsBannerApiError({ error: 'Error loading' });
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadRegistryClientsError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(false, expect.any(HttpErrorResponse));
            expect(result).toEqual(errorAction);
        });

        it('should handle loadRegistryClientsError with success status (existing data)', async () => {
            const { effects, errorHelper } = await setup();

            action$.next(
                RegistryClientsActions.loadRegistryClientsError({
                    errorResponse: new HttpErrorResponse({ status: 503 }),
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );

            const errorAction = RegistryClientsActions.registryClientsBannerApiError({ error: 'Error loading' });
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadRegistryClientsError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(true, expect.any(HttpErrorResponse));
            expect(result).toEqual(errorAction);
        });
    });
});
