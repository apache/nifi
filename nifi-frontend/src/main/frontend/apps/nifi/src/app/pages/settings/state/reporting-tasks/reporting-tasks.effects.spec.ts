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

import * as ReportingTaskActions from './reporting-tasks.actions';
import { ReportingTasksEffects } from './reporting-tasks.effects';
import { ReportingTaskService } from '../../service/reporting-task.service';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { initialState } from './reporting-tasks.reducer';

describe('ReportingTasksEffects', () => {
    interface SetupOptions {
        reportingTasksState?: any;
    }

    const mockResponse = {
        reportingTasks: [],
        currentTime: '2023-01-01 12:00:00 EST'
    };

    async function setup({ reportingTasksState = initialState }: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            providers: [
                ReportingTasksEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        settings: {
                            reportingTasks: reportingTasksState
                        }
                    }
                }),
                { provide: ReportingTaskService, useValue: { getReportingTasks: jest.fn() } },
                { provide: ManagementControllerServiceService, useValue: {} },
                { provide: ErrorHelper, useValue: { handleLoadingError: jest.fn() } },
                { provide: MatDialog, useValue: { open: jest.fn(), closeAll: jest.fn() } },
                { provide: Router, useValue: { navigate: jest.fn() } },
                { provide: PropertyTableHelperService, useValue: {} },
                { provide: ExtensionTypesService, useValue: {} }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const effects = TestBed.inject(ReportingTasksEffects);
        const reportingTaskService = TestBed.inject(ReportingTaskService);
        const errorHelper = TestBed.inject(ErrorHelper);

        return { effects, store, reportingTaskService, errorHelper };
    }

    let action$: ReplaySubject<Action>;
    beforeEach(() => {
        action$ = new ReplaySubject<Action>();
    });

    it('should create', async () => {
        const { effects } = await setup();
        expect(effects).toBeTruthy();
    });

    describe('Load Reporting Tasks', () => {
        it('should load reporting tasks successfully', async () => {
            const { effects, reportingTaskService } = await setup();

            action$.next(ReportingTaskActions.loadReportingTasks());
            jest.spyOn(reportingTaskService, 'getReportingTasks').mockReturnValueOnce(of(mockResponse) as never);

            const result = await new Promise((resolve) => effects.loadReportingTasks$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                ReportingTaskActions.loadReportingTasksSuccess({
                    response: {
                        reportingTasks: mockResponse.reportingTasks,
                        loadedTimestamp: mockResponse.currentTime
                    }
                })
            );
        });

        it('should fail to load reporting tasks on initial load with hasExistingData=false', async () => {
            const { effects, reportingTaskService } = await setup();

            action$.next(ReportingTaskActions.loadReportingTasks());
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(reportingTaskService, 'getReportingTasks').mockImplementationOnce(() => throwError(() => error));

            const result = await new Promise((resolve) => effects.loadReportingTasks$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                ReportingTaskActions.loadReportingTasksError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });

        it('should fail to load reporting tasks on refresh with hasExistingData=true', async () => {
            const stateWithData = {
                ...initialState,
                loadedTimestamp: '2023-01-01 11:00:00 EST'
            };
            const { effects, reportingTaskService } = await setup({
                reportingTasksState: stateWithData
            });

            action$.next(ReportingTaskActions.loadReportingTasks());
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(reportingTaskService, 'getReportingTasks').mockImplementationOnce(() => throwError(() => error));

            const result = await new Promise((resolve) => effects.loadReportingTasks$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                ReportingTaskActions.loadReportingTasksError({
                    errorResponse: error,
                    loadedTimestamp: stateWithData.loadedTimestamp,
                    status: 'success'
                })
            );
        });

        it('should handle loadReportingTasksError with pending status (no existing data)', async () => {
            const { effects, errorHelper } = await setup();

            action$.next(
                ReportingTaskActions.loadReportingTasksError({
                    errorResponse: new HttpErrorResponse({ status: 500 }),
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );

            const errorAction = ReportingTaskActions.reportingTasksBannerApiError({ error: 'Error loading' });
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadReportingTasksError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(false, expect.any(HttpErrorResponse));
            expect(result).toEqual(errorAction);
        });

        it('should handle loadReportingTasksError with success status (existing data)', async () => {
            const { effects, errorHelper } = await setup();

            action$.next(
                ReportingTaskActions.loadReportingTasksError({
                    errorResponse: new HttpErrorResponse({ status: 503 }),
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );

            const errorAction = ReportingTaskActions.reportingTasksBannerApiError({ error: 'Error loading' });
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadReportingTasksError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(true, expect.any(HttpErrorResponse));
            expect(result).toEqual(errorAction);
        });
    });
});
