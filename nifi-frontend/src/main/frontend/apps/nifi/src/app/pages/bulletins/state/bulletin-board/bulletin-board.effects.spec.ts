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

import * as BulletinBoardActions from './bulletin-board.actions';
import { BulletinBoardEffects } from './bulletin-board.effects';
import { BulletinBoardService } from '../../service/bulletin-board.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { initialBulletinBoardState } from './bulletin-board.reducer';

describe('BulletinBoardEffects', () => {
    interface SetupOptions {
        bulletinBoardState?: any;
    }

    const mockResponse = {
        bulletinBoard: {
            generated: '2023-01-01 12:00:00 EST',
            bulletins: []
        }
    };

    async function setup({ bulletinBoardState = initialBulletinBoardState }: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            providers: [
                BulletinBoardEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        bulletins: {
                            'bulletin-board': bulletinBoardState
                        }
                    }
                }),
                { provide: BulletinBoardService, useValue: { getBulletins: jest.fn() } },
                { provide: ErrorHelper, useValue: { handleLoadingError: jest.fn() } }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const effects = TestBed.inject(BulletinBoardEffects);
        const bulletinBoardService = TestBed.inject(BulletinBoardService);
        const errorHelper = TestBed.inject(ErrorHelper);

        return { effects, store, bulletinBoardService, errorHelper };
    }

    let action$: ReplaySubject<Action>;
    beforeEach(() => {
        action$ = new ReplaySubject<Action>();
    });

    it('should create', async () => {
        const { effects } = await setup();
        expect(effects).toBeTruthy();
    });

    describe('Load Bulletin Board', () => {
        it('should load bulletin board successfully', async () => {
            const { effects, bulletinBoardService } = await setup();

            const request = { after: 1, limit: 10 };
            action$.next(BulletinBoardActions.loadBulletinBoard({ request }));
            jest.spyOn(bulletinBoardService, 'getBulletins').mockReturnValueOnce(of(mockResponse) as never);

            const result = await new Promise((resolve) => effects.loadBulletinBoard$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                BulletinBoardActions.loadBulletinBoardSuccess({
                    response: {
                        bulletinBoard: mockResponse.bulletinBoard,
                        loadedTimestamp: mockResponse.bulletinBoard.generated
                    }
                })
            );
        });

        it('should fail to load bulletin board on initial load with hasExistingData=false', async () => {
            const { effects, bulletinBoardService } = await setup();

            const request = { after: 1, limit: 10 };
            action$.next(BulletinBoardActions.loadBulletinBoard({ request }));
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(bulletinBoardService, 'getBulletins').mockImplementationOnce(() => throwError(() => error));

            const result = await new Promise((resolve) => effects.loadBulletinBoard$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                BulletinBoardActions.loadBulletinBoardError({
                    errorResponse: error,
                    loadedTimestamp: initialBulletinBoardState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });

        it('should fail to load bulletin board on refresh with hasExistingData=true', async () => {
            const stateWithData = { ...initialBulletinBoardState, loadedTimestamp: '2023-01-01 11:00:00 EST' };
            const { effects, bulletinBoardService } = await setup({ bulletinBoardState: stateWithData });

            const request = { after: 1, limit: 10 };
            action$.next(BulletinBoardActions.loadBulletinBoard({ request }));
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(bulletinBoardService, 'getBulletins').mockImplementationOnce(() => throwError(() => error));

            const result = await new Promise((resolve) => effects.loadBulletinBoard$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                BulletinBoardActions.loadBulletinBoardError({
                    errorResponse: error,
                    loadedTimestamp: stateWithData.loadedTimestamp,
                    status: 'success'
                })
            );
        });
    });

    describe('Bulletin Board Listing Error', () => {
        it('should handle bulletin board listing error for initial load', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = BulletinBoardActions.loadBulletinBoardSuccess({
                response: {
                    bulletinBoard: mockResponse.bulletinBoard,
                    loadedTimestamp: mockResponse.bulletinBoard.generated
                }
            });
            action$.next(
                BulletinBoardActions.loadBulletinBoardError({
                    errorResponse: error,
                    loadedTimestamp: initialBulletinBoardState.loadedTimestamp,
                    status: 'pending'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.bulletinBoardListingError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(false, error);
            expect(result).toEqual(errorAction);
        });

        it('should handle bulletin board listing error for refresh', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = BulletinBoardActions.loadBulletinBoardSuccess({
                response: {
                    bulletinBoard: mockResponse.bulletinBoard,
                    loadedTimestamp: mockResponse.bulletinBoard.generated
                }
            });
            action$.next(
                BulletinBoardActions.loadBulletinBoardError({
                    errorResponse: error,
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.bulletinBoardListingError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(true, error);
            expect(result).toEqual(errorAction);
        });
    });
});
