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
import { ReplaySubject, of, take, throwError } from 'rxjs';
import { Action } from '@ngrx/store';

import * as GeneralActions from './general.actions';
import { GeneralEffects } from './general.effects';
import { ControllerService } from '../../service/controller.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';

describe('GeneralEffects', () => {
    interface SetupOptions {
        settingsState?: any;
    }

    async function setup({ settingsState = { general: {} } }: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            providers: [
                GeneralEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        settings: settingsState
                    }
                }),
                {
                    provide: ControllerService,
                    useValue: {
                        getControllerConfig: jest.fn(),
                        updateControllerConfig: jest.fn()
                    }
                },
                {
                    provide: ErrorHelper,
                    useValue: {
                        fullScreenError: jest.fn(),
                        getErrorString: jest.fn()
                    }
                }
            ]
        }).compileComponents();

        const effects = TestBed.inject(GeneralEffects);
        const controllerService = TestBed.inject(ControllerService);
        const errorHelper = TestBed.inject(ErrorHelper);

        return { effects, controllerService, errorHelper };
    }

    let action$: ReplaySubject<Action>;
    beforeEach(() => {
        action$ = new ReplaySubject<Action>();
    });

    it('should create', async () => {
        const { effects } = await setup();
        expect(effects).toBeTruthy();
    });

    describe('loadControllerConfig', () => {
        it('loads successfully', async () => {
            const { effects, controllerService } = await setup();

            action$.next(GeneralActions.loadControllerConfig());

            const controllerEntity = {
                revision: { version: 1, clientId: 'c1' },
                component: { maxTimerDrivenThreadCount: 10 }
            } as any;
            jest.spyOn(controllerService, 'getControllerConfig').mockReturnValueOnce(of(controllerEntity) as never);

            const result = await new Promise((resolve) =>
                effects.loadControllerConfig$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                GeneralActions.loadControllerConfigSuccess({
                    response: {
                        controller: controllerEntity
                    }
                })
            );
        });

        it('handles error with fullScreenError', async () => {
            const { effects, controllerService, errorHelper } = await setup();

            action$.next(GeneralActions.loadControllerConfig());

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = GeneralActions.controllerConfigApiError({ error: 'e' });

            jest.spyOn(controllerService, 'getControllerConfig').mockImplementationOnce(() => throwError(() => error));
            jest.spyOn(errorHelper, 'fullScreenError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadControllerConfig$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.fullScreenError).toHaveBeenCalledWith(error);
            expect(result).toEqual(errorAction);
        });
    });
});
