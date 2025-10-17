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
import { ManageRemotePortsEffects } from './manage-remote-ports.effects';
import { provideMockActions } from '@ngrx/effects/testing';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { initialState } from './manage-remote-ports.reducer';
import { of, ReplaySubject, take, throwError } from 'rxjs';
import { Action } from '@ngrx/store';
import * as ManageRemotePortsActions from './manage-remote-ports.actions';
import { ManageRemotePortService } from '../../service/manage-remote-port.service';
import { MatDialog } from '@angular/material/dialog';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { Router } from '@angular/router';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { remotePortsFeatureKey } from './index';

describe('ManageRemotePortsEffects', () => {
    interface SetupOptions {
        remotePortsState?: any;
    }

    const mockRemotePortsResponse = {
        component: {
            contents: {
                inputPorts: [
                    {
                        id: 'port-1',
                        name: 'Test Input Port',
                        targetId: 'target-1',
                        targetRunning: false,
                        transmitting: false,
                        exists: true,
                        connected: true,
                        batchSettings: {
                            count: 1,
                            size: '1 KB',
                            duration: '0 sec'
                        },
                        comments: '',
                        concurrentlySchedulableTaskCount: 1,
                        groupId: 'rpg-1',
                        useCompression: false,
                        versionedComponentId: 'version-1'
                    }
                ],
                outputPorts: [
                    {
                        id: 'port-2',
                        name: 'Test Output Port',
                        targetId: 'target-2',
                        targetRunning: false,
                        transmitting: false,
                        exists: true,
                        connected: true,
                        batchSettings: {
                            count: 1,
                            size: '1 KB',
                            duration: '0 sec'
                        },
                        comments: '',
                        concurrentlySchedulableTaskCount: 1,
                        groupId: 'rpg-1',
                        useCompression: false,
                        versionedComponentId: 'version-1'
                    }
                ]
            }
        },
        id: 'rpg-1',
        name: 'Test Remote Process Group'
    };

    const mockTimeOffset = 0;
    const mockAbout = {
        timezone: 'EST'
    };

    let action$: ReplaySubject<Action>;

    async function setup({ remotePortsState = initialState }: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            providers: [
                ManageRemotePortsEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        [remotePortsFeatureKey]: remotePortsState || initialState,
                        flowConfiguration: {
                            flowConfiguration: {
                                timeOffset: mockTimeOffset
                            }
                        },
                        about: {
                            about: mockAbout
                        }
                    }
                }),
                {
                    provide: ManageRemotePortService,
                    useValue: {
                        getRemotePorts: jest.fn(),
                        configureRemotePort: jest.fn()
                    }
                },
                {
                    provide: MatDialog,
                    useValue: {
                        open: jest.fn()
                    }
                },
                {
                    provide: Router,
                    useValue: {
                        navigate: jest.fn()
                    }
                },
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: jest.fn().mockReturnValue(false)
                    }
                },
                {
                    provide: ErrorHelper,
                    useValue: {
                        handleLoadingError: jest.fn(),
                        getErrorString: jest.fn()
                    }
                }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const remotePortService = TestBed.inject(ManageRemotePortService) as jest.Mocked<ManageRemotePortService>;
        const errorHelper = TestBed.inject(ErrorHelper) as jest.Mocked<ErrorHelper>;
        const effects = TestBed.inject(ManageRemotePortsEffects);

        return { store, remotePortService, errorHelper, effects };
    }

    beforeEach(() => {
        action$ = new ReplaySubject<Action>(1);
    });

    afterEach(() => {
        action$.complete();
    });

    it('should be created', async () => {
        const { effects } = await setup();
        expect(effects).toBeTruthy();
    });

    describe('loadRemotePorts', () => {
        it('should load remote ports successfully', async () => {
            const { effects, remotePortService } = await setup();

            const request = { rpgId: 'rpg-1' };
            action$.next(ManageRemotePortsActions.loadRemotePorts({ request }));
            jest.spyOn(remotePortService, 'getRemotePorts').mockReturnValueOnce(of(mockRemotePortsResponse) as never);

            const result = await new Promise((resolve) => effects.loadRemotePorts$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                ManageRemotePortsActions.loadRemotePortsSuccess({
                    response: expect.objectContaining({
                        ports: expect.any(Array),
                        loadedTimestamp: expect.any(String),
                        rpg: expect.any(Object)
                    })
                })
            );
        });

        it('should fail to load remote ports on initial load with hasExistingData=false', async () => {
            const { effects, remotePortService } = await setup();

            const request = { rpgId: 'rpg-1' };
            action$.next(ManageRemotePortsActions.loadRemotePorts({ request }));
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(remotePortService, 'getRemotePorts').mockImplementationOnce(() => throwError(() => error));

            const result = await new Promise((resolve) => effects.loadRemotePorts$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                ManageRemotePortsActions.loadRemotePortsError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });

        it('should fail to load remote ports on refresh with hasExistingData=true', async () => {
            const stateWithData = {
                ...initialState,
                loadedTimestamp: '2023-01-01 11:00:00 EST'
            };
            const { effects, remotePortService } = await setup({ remotePortsState: stateWithData });

            const request = { rpgId: 'rpg-1' };
            action$.next(ManageRemotePortsActions.loadRemotePorts({ request }));
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(remotePortService, 'getRemotePorts').mockImplementationOnce(() => throwError(() => error));

            const result = await new Promise((resolve) => effects.loadRemotePorts$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                ManageRemotePortsActions.loadRemotePortsError({
                    errorResponse: error,
                    loadedTimestamp: stateWithData.loadedTimestamp,
                    status: 'success'
                })
            );
        });
    });

    describe('Load Remote Ports Error', () => {
        it('should handle remote ports error for initial load', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = ManageRemotePortsActions.remotePortsBannerApiError({ error: 'Error message' });
            action$.next(
                ManageRemotePortsActions.loadRemotePortsError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadRemotePortsError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(false, error);
            expect(result).toEqual(errorAction);
        });

        it('should handle remote ports error for refresh', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = ManageRemotePortsActions.remotePortsBannerApiError({ error: 'Error message' });
            action$.next(
                ManageRemotePortsActions.loadRemotePortsError({
                    errorResponse: error,
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadRemotePortsError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(true, error);
            expect(result).toEqual(errorAction);
        });
    });
});
