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
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { Action } from '@ngrx/store';
import { HttpErrorResponse } from '@angular/common/http';
import { MatDialog } from '@angular/material/dialog';
import { NavigationStart, Router } from '@angular/router';
import { filter, firstValueFrom, Observable, of, Subject, throwError } from 'rxjs';
import { ProcessGroupDrainEffects } from './process-group-drain.effects';
import * as ProcessGroupDrainActions from './process-group-drain.actions';
import * as FlowActions from '../flow/flow.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import { selectCurrentProcessGroupId } from '../flow/flow.selectors';
import { selectDrainProcessGroupId, selectDrainRequestId } from './process-group-drain.selectors';
import { ProcessGroupDrainService } from '../../../../service/process-group-drain.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import {
    createDrainBlockingComponent,
    createProcessGroupDrainRequestEntity
} from '../../../../testing/process-group-drain-fixture';
import { ComponentType } from '@nifi/shared';

describe('ProcessGroupDrainEffects', () => {
    async function setup(
        options: {
            processGroupId?: string | null;
            drainRequestId?: string | null;
            currentProcessGroupId?: string;
            processGroupDrainService?: Partial<ProcessGroupDrainService>;
        } = {}
    ) {
        let actions$: Observable<Action>;
        const yes$ = new Subject<void>();
        const cancelDrain$ = new Subject<void>();
        const goToComponent$ = new Subject<unknown>();
        const afterClosed$ = new Subject<void>();
        const routerEvents$ = new Subject<unknown>();
        const mockDialog = {
            open: vi.fn().mockReturnValue({
                componentInstance: {
                    yes: yes$,
                    cancelDrain: cancelDrain$,
                    goToComponent: goToComponent$,
                    drainState$: undefined
                },
                close: vi.fn(),
                afterClosed: () => afterClosed$
            })
        };

        await TestBed.configureTestingModule({
            providers: [
                ProcessGroupDrainEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    selectors: [
                        {
                            selector: selectDrainProcessGroupId,
                            value: options.processGroupId !== undefined ? options.processGroupId : 'pg-1'
                        },
                        {
                            selector: selectDrainRequestId,
                            value: options.drainRequestId !== undefined ? options.drainRequestId : 'drain-1'
                        },
                        {
                            selector: selectCurrentProcessGroupId,
                            value: options.currentProcessGroupId ?? 'pg-1'
                        }
                    ]
                }),
                { provide: MatDialog, useValue: mockDialog },
                { provide: Router, useValue: { events: routerEvents$ } },
                {
                    provide: ProcessGroupDrainService,
                    useValue: {
                        beginDrain: vi.fn(),
                        getDrain: vi.fn(),
                        endDrain: vi.fn(),
                        ...options.processGroupDrainService
                    }
                },
                {
                    provide: ErrorHelper,
                    useValue: {
                        getErrorString: (error: HttpErrorResponse) => error.message,
                        showErrorInContext: (status: number) => [400, 403, 404, 409, 413, 503].includes(status),
                        fullScreenError: vi
                            .fn()
                            .mockReturnValue(
                                ErrorActions.fullScreenError({ errorDetail: { title: 'Error', message: 'boom' } })
                            )
                    }
                }
            ]
        }).compileComponents();

        const effects = TestBed.inject(ProcessGroupDrainEffects);
        const store = TestBed.inject(MockStore);
        const processGroupDrainService = TestBed.inject(ProcessGroupDrainService);
        const dispatchSpy = vi.spyOn(store, 'dispatch');

        return {
            effects,
            setActions: (source: Observable<Action>) => {
                actions$ = source;
            },
            mockDialog,
            yes$,
            cancelDrain$,
            goToComponent$,
            afterClosed$,
            routerEvents$,
            processGroupDrainService,
            dispatchSpy
        };
    }

    it('dispatches beginDrain only when the prompt is confirmed', async () => {
        const { effects, setActions, mockDialog, yes$, dispatchSpy } = await setup();
        const actions$ = new Subject<Action>();
        setActions(actions$);

        effects.promptDrain$.subscribe();
        actions$.next(ProcessGroupDrainActions.promptDrain({ request: { processGroupId: 'pg-1' } }));

        expect(mockDialog.open).toHaveBeenCalled();
        expect(dispatchSpy).not.toHaveBeenCalled();

        yes$.next();

        expect(dispatchSpy).toHaveBeenCalledWith(
            ProcessGroupDrainActions.beginDrain({ request: { processGroupId: 'pg-1' } })
        );
    });

    it('opens the dialog, starts polling, and reloads the current group after a successful POST', async () => {
        const entity = createProcessGroupDrainRequestEntity();
        const { effects, setActions, mockDialog } = await setup({
            processGroupDrainService: {
                beginDrain: vi.fn().mockReturnValue(of(entity))
            }
        });
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const beginResult = firstValueFrom(effects.beginDrain$);
        actions$.next(ProcessGroupDrainActions.beginDrain({ request: { processGroupId: 'pg-1' } }));
        expect(await beginResult).toEqual(
            ProcessGroupDrainActions.beginDrainSuccess({ response: { drainRequestEntity: entity } })
        );

        const successResults = firstValueFrom(effects.beginDrainSuccess$.pipe());
        const openPromise = firstValueFrom(effects.openDrainDialog$);
        actions$.next(ProcessGroupDrainActions.beginDrainSuccess({ response: { drainRequestEntity: entity } }));
        await openPromise;
        const success = await successResults;

        expect(mockDialog.open).toHaveBeenCalled();
        expect(success).toEqual(FlowActions.reloadFlow());
    });

    it('reloads a child process group after a successful POST when it is not the current group', async () => {
        const entity = createProcessGroupDrainRequestEntity();
        const { effects, setActions } = await setup({
            currentProcessGroupId: 'current-pg',
            processGroupId: 'pg-1'
        });
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const result = firstValueFrom(effects.beginDrainSuccess$);
        actions$.next(ProcessGroupDrainActions.beginDrainSuccess({ response: { drainRequestEntity: entity } }));

        expect(await result).toEqual(FlowActions.loadChildProcessGroup({ request: { id: 'pg-1' } }));
    });

    it('ends the drain without reloading in beginDrainSuccess$ when the POST returns an ended drain', async () => {
        const ended = createProcessGroupDrainRequestEntity({ state: 'ENDED', currentCount: 0 });
        const { effects, setActions } = await setup({ currentProcessGroupId: 'pg-1', processGroupId: 'pg-1' });
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const emitted: Action[] = [];
        const subscription = effects.beginDrainSuccess$.subscribe((action) => emitted.push(action));
        actions$.next(ProcessGroupDrainActions.beginDrainSuccess({ response: { drainRequestEntity: ended } }));
        subscription.unsubscribe();

        expect(emitted).toEqual([ProcessGroupDrainActions.drainEnded()]);
    });

    it('shows a snackbar and does not open the dialog when the POST fails', async () => {
        const errorResponse = new HttpErrorResponse({ status: 409, statusText: 'Conflict', error: 'already draining' });
        Object.defineProperty(errorResponse, 'message', { value: 'already draining' });
        const { effects, setActions, mockDialog, processGroupDrainService } = await setup({
            processGroupDrainService: {
                beginDrain: vi.fn().mockReturnValue(throwError(() => errorResponse))
            }
        });
        vi.spyOn(processGroupDrainService, 'beginDrain').mockReturnValue(throwError(() => errorResponse));
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const result = firstValueFrom(effects.beginDrain$);
        actions$.next(ProcessGroupDrainActions.beginDrain({ request: { processGroupId: 'pg-1' } }));

        expect(await result).toEqual(ProcessGroupDrainActions.drainApiError({ error: 'already draining' }));
        expect(mockDialog.open).not.toHaveBeenCalled();
    });

    it('stops polling and reloads when a poll returns an ended drain', async () => {
        const { effects, setActions } = await setup();
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const prepared = firstValueFrom(effects.pollDrainSuccess$);
        actions$.next(
            ProcessGroupDrainActions.pollDrainSuccess({
                response: { drainRequestEntity: createProcessGroupDrainRequestEntity({ prepared: true }) }
            })
        );
        await prepared;

        const result = firstValueFrom(effects.pollDrainSuccess$);
        actions$.next(
            ProcessGroupDrainActions.pollDrainSuccess({
                response: {
                    drainRequestEntity: createProcessGroupDrainRequestEntity({ state: 'ENDED', currentCount: 0 })
                }
            })
        );

        expect(await result).toEqual(ProcessGroupDrainActions.drainEnded());
    });

    it('stops polling when the drain entity is empty', async () => {
        const { effects, setActions } = await setup();
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const result = firstValueFrom(effects.pollDrainSuccess$);
        actions$.next(
            ProcessGroupDrainActions.pollDrainSuccess({
                response: { drainRequestEntity: createProcessGroupDrainRequestEntity(null) }
            })
        );

        expect(await result).toEqual(ProcessGroupDrainActions.drainEnded());
    });

    it('reloads the flow once when prepared flips to true', async () => {
        const { effects, setActions } = await setup();
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const first = firstValueFrom(effects.pollDrainSuccess$);
        actions$.next(
            ProcessGroupDrainActions.pollDrainSuccess({
                response: { drainRequestEntity: createProcessGroupDrainRequestEntity({ prepared: true }) }
            })
        );
        expect(await first).toEqual(FlowActions.reloadFlow());

        const secondPromise = new Promise<Action | 'none'>((resolve) => {
            const subscription = effects.pollDrainSuccess$.subscribe({
                next: (action) => {
                    subscription.unsubscribe();
                    resolve(action);
                }
            });
            actions$.next(
                ProcessGroupDrainActions.pollDrainSuccess({
                    response: {
                        drainRequestEntity: createProcessGroupDrainRequestEntity({ prepared: true, currentCount: 3 })
                    }
                })
            );
            setTimeout(() => {
                subscription.unsubscribe();
                resolve('none');
            }, 20);
        });

        expect(await secondPromise).toBe('none');
    });

    it('DELETEs the drain and reloads when the user cancels', async () => {
        const ended = createProcessGroupDrainRequestEntity({ state: 'ENDED', currentCount: 2 });
        const { effects, setActions, processGroupDrainService } = await setup({
            processGroupDrainService: {
                endDrain: vi.fn().mockReturnValue(of(ended))
            }
        });
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const cancelResult = firstValueFrom(effects.cancelDrain$);
        actions$.next(ProcessGroupDrainActions.cancelDrain());
        expect(await cancelResult).toEqual(
            ProcessGroupDrainActions.cancelDrainSuccess({ response: { drainRequestEntity: ended } })
        );
        expect(processGroupDrainService.endDrain).toHaveBeenCalledWith('pg-1', 'drain-1');

        const reloadResult = firstValueFrom(effects.drainEnded$);
        actions$.next(ProcessGroupDrainActions.cancelDrainSuccess({ response: { drainRequestEntity: ended } }));
        expect(await reloadResult).toEqual(FlowActions.reloadFlow());
    });

    it('never DELETEs when a poll errors', async () => {
        const errorResponse = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
        Object.defineProperty(errorResponse, 'message', { value: 'poll failed' });
        const { effects, setActions, processGroupDrainService } = await setup({
            processGroupDrainService: {
                getDrain: vi.fn().mockReturnValue(throwError(() => errorResponse)),
                endDrain: vi.fn()
            }
        });
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const result = firstValueFrom(effects.pollDrain$);
        actions$.next(ProcessGroupDrainActions.pollDrain());

        expect(await result).toEqual(ProcessGroupDrainActions.drainApiError({ error: 'poll failed' }));
        expect(processGroupDrainService.endDrain).not.toHaveBeenCalled();
    });

    it('maps a remote input port to its Remote Process Group on Go To', async () => {
        const { effects, setActions } = await setup();
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const component = createDrainBlockingComponent({
            type: 'REMOTE_INPUT_PORT',
            id: 'port-1',
            remoteProcessGroupId: 'rpg-1',
            groupId: 'pg-2'
        });
        const result = firstValueFrom(effects.goToBlockingComponent$);
        actions$.next(ProcessGroupDrainActions.goToBlockingComponent({ component }));

        expect(await result).toEqual(
            FlowActions.navigateToComponent({
                request: {
                    id: 'rpg-1',
                    type: ComponentType.RemoteProcessGroup,
                    processGroupId: 'pg-2'
                }
            })
        );
    });

    it('GETs immediately when viewing an existing drain', async () => {
        const entity = createProcessGroupDrainRequestEntity();
        const { effects, setActions, processGroupDrainService } = await setup({
            processGroupDrainService: {
                getDrain: vi.fn().mockReturnValue(of(entity))
            }
        });
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const pollResult = firstValueFrom(effects.pollDrain$);
        const startResult = firstValueFrom(effects.viewDrain$);
        actions$.next(
            ProcessGroupDrainActions.viewDrain({ request: { processGroupId: 'pg-1', drainRequestId: 'drain-1' } })
        );

        expect(await startResult).toEqual(ProcessGroupDrainActions.startPollingDrain());
        expect(await pollResult).toEqual(
            ProcessGroupDrainActions.pollDrainSuccess({ response: { drainRequestEntity: entity } })
        );
        expect(processGroupDrainService.getDrain).toHaveBeenCalledWith('pg-1', 'drain-1');
    });

    it('ends the drain without a prepared reload when the same poll also reports it ended', async () => {
        const { effects, setActions } = await setup();
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const emitted: Action[] = [];
        const subscription = effects.pollDrainSuccess$.subscribe((action) => emitted.push(action));
        actions$.next(
            ProcessGroupDrainActions.pollDrainSuccess({
                response: {
                    drainRequestEntity: createProcessGroupDrainRequestEntity({
                        state: 'ENDED',
                        prepared: true,
                        currentCount: 0
                    })
                }
            })
        );
        subscription.unsubscribe();

        expect(emitted).toEqual([ProcessGroupDrainActions.drainEnded()]);
    });

    it('reloads then starts polling when a begin returns a draining drain', async () => {
        const { effects, setActions } = await setup({ currentProcessGroupId: 'pg-1', processGroupId: 'pg-1' });
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const emitted: Action[] = [];
        const subscription = effects.beginDrainSuccess$.subscribe((action) => emitted.push(action));
        actions$.next(
            ProcessGroupDrainActions.beginDrainSuccess({
                response: { drainRequestEntity: createProcessGroupDrainRequestEntity() }
            })
        );
        subscription.unsubscribe();

        expect(emitted).toEqual([FlowActions.reloadFlow(), ProcessGroupDrainActions.startPollingDrain()]);
    });

    it('GETs immediately on View and then every two seconds', async () => {
        const { effects, setActions, processGroupDrainService } = await setup({
            processGroupDrainService: {
                getDrain: vi.fn().mockReturnValue(of(createProcessGroupDrainRequestEntity()))
            }
        });
        const dispatched$ = new Subject<Action>();
        setActions(dispatched$);
        const polls: Action[] = [];
        // Forward every emitted action back into the stream, as the store does
        const subscriptions = [effects.viewDrain$, effects.startPollingDrain$, effects.pollDrain$].map((effect) =>
            effect.subscribe((action) => dispatched$.next(action))
        );
        subscriptions.push(
            dispatched$
                .pipe(filter((action) => action.type === ProcessGroupDrainActions.pollDrain.type))
                .subscribe((action) => polls.push(action))
        );
        vi.useFakeTimers();

        try {
            dispatched$.next(
                ProcessGroupDrainActions.viewDrain({ request: { processGroupId: 'pg-1', drainRequestId: 'drain-1' } })
            );
            vi.advanceTimersByTime(4000);

            // Two interval ticks in four seconds, and one immediate GET from the View itself
            expect(polls.length).toBe(2);
            expect(processGroupDrainService.getDrain).toHaveBeenCalledTimes(3);
        } finally {
            subscriptions.forEach((subscription) => subscription.unsubscribe());
            vi.useRealTimers();
        }
    });

    it('closes the open drain dialog when a drain API error is dispatched', async () => {
        const { effects, setActions, mockDialog } = await setup();
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const openSubscription = effects.openDrainDialog$.subscribe();
        actions$.next(
            ProcessGroupDrainActions.beginDrainSuccess({
                response: { drainRequestEntity: createProcessGroupDrainRequestEntity() }
            })
        );
        const errorSubscription = effects.drainApiError$.subscribe();
        actions$.next(ProcessGroupDrainActions.drainApiError({ error: 'boom' }));
        openSubscription.unsubscribe();
        errorSubscription.unsubscribe();

        const dialogRef = mockDialog.open.mock.results[0].value;
        expect(dialogRef.close).toHaveBeenCalled();
    });

    it('resets the drain state when the dialog is closed', async () => {
        const { effects, setActions, afterClosed$, dispatchSpy } = await setup();
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const subscription = effects.openDrainDialog$.subscribe();
        actions$.next(
            ProcessGroupDrainActions.beginDrainSuccess({
                response: { drainRequestEntity: createProcessGroupDrainRequestEntity() }
            })
        );
        afterClosed$.next();
        subscription.unsubscribe();

        expect(dispatchSpy).toHaveBeenCalledWith(ProcessGroupDrainActions.resetDrainState());
    });

    it('closes the drain dialog when navigation starts', async () => {
        const { effects, setActions, mockDialog, routerEvents$ } = await setup();
        const actions$ = new Subject<Action>();
        setActions(actions$);

        const openSubscription = effects.openDrainDialog$.subscribe();
        const closeSubscription = effects.closeOnNavigation$.subscribe();
        actions$.next(
            ProcessGroupDrainActions.beginDrainSuccess({
                response: { drainRequestEntity: createProcessGroupDrainRequestEntity() }
            })
        );
        routerEvents$.next(new NavigationStart(1, '/'));
        openSubscription.unsubscribe();
        closeSubscription.unsubscribe();

        const dialogRef = mockDialog.open.mock.results[0].value;
        expect(dialogRef.close).toHaveBeenCalled();
    });
});
