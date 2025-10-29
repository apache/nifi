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

import * as FlowAnalysisRuleActions from './flow-analysis-rules.actions';
import { FlowAnalysisRulesEffects } from './flow-analysis-rules.effects';
import { FlowAnalysisRuleService } from '../../service/flow-analysis-rule.service';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { initialState } from './flow-analysis-rules.reducer';
import { settingsFeatureKey } from '../index';
import { flowAnalysisRulesFeatureKey } from './index';

describe('FlowAnalysisRulesEffects', () => {
    interface SetupOptions {
        flowAnalysisRulesState?: any;
    }

    const mockResponse = {
        flowAnalysisRules: [],
        currentTime: '2023-01-01 12:00:00 EST'
    };

    async function setup({ flowAnalysisRulesState = initialState }: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            providers: [
                FlowAnalysisRulesEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        [settingsFeatureKey]: {
                            [flowAnalysisRulesFeatureKey]: flowAnalysisRulesState
                        }
                    }
                }),
                { provide: FlowAnalysisRuleService, useValue: { getFlowAnalysisRule: jest.fn() } },
                { provide: ManagementControllerServiceService, useValue: {} },
                { provide: ErrorHelper, useValue: { handleLoadingError: jest.fn() } },
                { provide: MatDialog, useValue: { open: jest.fn(), closeAll: jest.fn() } },
                { provide: Router, useValue: { navigate: jest.fn() } },
                { provide: PropertyTableHelperService, useValue: {} },
                { provide: ExtensionTypesService, useValue: {} }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const effects = TestBed.inject(FlowAnalysisRulesEffects);
        const flowAnalysisRuleService = TestBed.inject(FlowAnalysisRuleService);
        const errorHelper = TestBed.inject(ErrorHelper);

        return { effects, store, flowAnalysisRuleService, errorHelper };
    }

    let action$: ReplaySubject<Action>;
    beforeEach(() => {
        action$ = new ReplaySubject<Action>();
    });

    it('should create', async () => {
        const { effects } = await setup();
        expect(effects).toBeTruthy();
    });

    describe('Load Flow Analysis Rules', () => {
        it('should load flow analysis rules successfully', async () => {
            const { effects, flowAnalysisRuleService } = await setup();

            action$.next(FlowAnalysisRuleActions.loadFlowAnalysisRules());
            jest.spyOn(flowAnalysisRuleService, 'getFlowAnalysisRule').mockReturnValueOnce(of(mockResponse) as never);

            const result = await new Promise((resolve) =>
                effects.loadFlowAnalysisRule$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                FlowAnalysisRuleActions.loadFlowAnalysisRulesSuccess({
                    response: {
                        flowAnalysisRules: mockResponse.flowAnalysisRules,
                        loadedTimestamp: mockResponse.currentTime
                    }
                })
            );
        });

        it('should fail to load flow analysis rules on initial load with hasExistingData=false', async () => {
            const { effects, flowAnalysisRuleService } = await setup();

            action$.next(FlowAnalysisRuleActions.loadFlowAnalysisRules());
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(flowAnalysisRuleService, 'getFlowAnalysisRule').mockImplementationOnce(() =>
                throwError(() => error)
            );

            const result = await new Promise((resolve) =>
                effects.loadFlowAnalysisRule$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                FlowAnalysisRuleActions.loadFlowAnalysisRulesError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });

        it('should fail to load flow analysis rules on refresh with hasExistingData=true', async () => {
            const stateWithData = { ...initialState, loadedTimestamp: '2023-01-01 11:00:00 EST' };
            const { effects, flowAnalysisRuleService } = await setup({
                flowAnalysisRulesState: stateWithData
            });

            action$.next(FlowAnalysisRuleActions.loadFlowAnalysisRules());
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(flowAnalysisRuleService, 'getFlowAnalysisRule').mockImplementationOnce(() =>
                throwError(() => error)
            );

            const result = await new Promise((resolve) =>
                effects.loadFlowAnalysisRule$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                FlowAnalysisRuleActions.loadFlowAnalysisRulesError({
                    errorResponse: error,
                    loadedTimestamp: stateWithData.loadedTimestamp,
                    status: 'success'
                })
            );
        });
    });

    describe('Load Flow Analysis Rules Error', () => {
        it('should handle flow analysis rules error for initial load', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = FlowAnalysisRuleActions.flowAnalysisRuleBannerApiError({ error: 'Error loading' });
            action$.next(
                FlowAnalysisRuleActions.loadFlowAnalysisRulesError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadFlowAnalysisRulesError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(false, error);
            expect(result).toEqual(errorAction);
        });

        it('should handle flow analysis rules error for refresh', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = FlowAnalysisRuleActions.flowAnalysisRuleBannerApiError({ error: 'Error loading' });
            action$.next(
                FlowAnalysisRuleActions.loadFlowAnalysisRulesError({
                    errorResponse: error,
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadFlowAnalysisRulesError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(true, error);
            expect(result).toEqual(errorAction);
        });
    });
});
