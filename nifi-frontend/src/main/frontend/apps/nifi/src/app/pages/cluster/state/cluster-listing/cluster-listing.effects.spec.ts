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
import { ClusterListingEffects } from './cluster-listing.effects';
import { provideMockActions } from '@ngrx/effects/testing';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { initialClusterState } from './cluster-listing.reducer';
import { of, ReplaySubject, take, throwError } from 'rxjs';
import { Action } from '@ngrx/store';
import * as ClusterListingActions from './cluster-listing.actions';
import { ClusterService } from '../../service/cluster.service';
import { MatDialog } from '@angular/material/dialog';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { Router } from '@angular/router';
import { clusterFeatureKey } from '../index';
import { clusterListingFeatureKey } from './index';

describe('ClusterListingEffects', () => {
    interface SetupOptions {
        clusterListingState?: any;
    }

    const mockClusterResponse = {
        cluster: {
            nodes: [
                {
                    nodeId: 'node-1',
                    address: '192.168.1.100',
                    apiPort: 8080,
                    status: 'CONNECTED',
                    heartbeat: '2023-01-01 12:00:00 EST',
                    roles: ['Primary Node'],
                    events: []
                }
            ],
            generated: '2023-01-01 12:00:00 EST'
        }
    };

    let action$: ReplaySubject<Action>;

    async function setup({ clusterListingState = initialClusterState }: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            providers: [
                ClusterListingEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        [clusterFeatureKey]: {
                            [clusterListingFeatureKey]: clusterListingState
                        }
                    }
                }),
                {
                    provide: ClusterService,
                    useValue: {
                        getClusterListing: jest.fn()
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
                    provide: ErrorHelper,
                    useValue: {
                        handleLoadingError: jest.fn(),
                        getErrorString: jest.fn()
                    }
                }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const clusterService = TestBed.inject(ClusterService) as jest.Mocked<ClusterService>;
        const errorHelper = TestBed.inject(ErrorHelper) as jest.Mocked<ErrorHelper>;
        const effects = TestBed.inject(ClusterListingEffects);

        return { store, clusterService, errorHelper, effects };
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

    describe('loadClusterListing', () => {
        it('should load cluster listing successfully', async () => {
            const { effects, clusterService } = await setup();

            action$.next(ClusterListingActions.loadClusterListing());
            jest.spyOn(clusterService, 'getClusterListing').mockReturnValueOnce(of(mockClusterResponse) as never);

            const result = await new Promise((resolve) => effects.loadClusterListing$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                ClusterListingActions.loadClusterListingSuccess({
                    response: mockClusterResponse.cluster
                })
            );
        });

        it('should fail to load cluster listing on initial load (no existing data)', async () => {
            const { effects, clusterService } = await setup();

            action$.next(ClusterListingActions.loadClusterListing());
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(clusterService, 'getClusterListing').mockImplementationOnce(() => throwError(() => error));

            const result = await new Promise((resolve) => effects.loadClusterListing$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                ClusterListingActions.loadClusterListingError({
                    errorResponse: error,
                    loadedTimestamp: initialClusterState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });

        it('should fail to load cluster listing on refresh (existing data)', async () => {
            const stateWithData = {
                ...initialClusterState,
                loadedTimestamp: '2023-01-01 11:00:00 EST'
            };
            const { effects, clusterService } = await setup({ clusterListingState: stateWithData });

            action$.next(ClusterListingActions.loadClusterListing());
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(clusterService, 'getClusterListing').mockImplementationOnce(() => throwError(() => error));

            const result = await new Promise((resolve) => effects.loadClusterListing$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                ClusterListingActions.loadClusterListingError({
                    errorResponse: error,
                    loadedTimestamp: stateWithData.loadedTimestamp,
                    status: 'success'
                })
            );
        });
    });

    describe('loadClusterListingError', () => {
        it('should handle loadClusterListingError with pending status', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = ClusterListingActions.loadClusterListingSuccess({
                response: mockClusterResponse.cluster
            });
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            action$.next(
                ClusterListingActions.loadClusterListingError({
                    errorResponse: error,
                    loadedTimestamp: '',
                    status: 'pending'
                })
            );

            const result = await new Promise((resolve) =>
                effects.loadClusterListingError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(false, error);
            expect(result).toEqual(errorAction);
        });

        it('should handle loadClusterListingError with success status', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = ClusterListingActions.loadClusterListingSuccess({
                response: mockClusterResponse.cluster
            });
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            action$.next(
                ClusterListingActions.loadClusterListingError({
                    errorResponse: error,
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );

            const result = await new Promise((resolve) =>
                effects.loadClusterListingError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(true, error);
            expect(result).toEqual(errorAction);
        });
    });
});
