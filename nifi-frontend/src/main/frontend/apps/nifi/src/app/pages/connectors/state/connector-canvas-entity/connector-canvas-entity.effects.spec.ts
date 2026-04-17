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
import { Action } from '@ngrx/store';
import { provideMockStore } from '@ngrx/store/testing';
import { firstValueFrom, Observable, of, ReplaySubject, Subject, throwError } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { MatDialog } from '@angular/material/dialog';
import { YesNoDialog } from '@nifi/shared';
import { ConnectorCanvasEntityEffects } from './connector-canvas-entity.effects';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import * as ConnectorCanvasActions from '../connector-canvas/connector-canvas.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorContextKey } from '../../../../state/error';
import { selectConnectorCanvasEntity } from './connector-canvas-entity.selectors';
import { selectConnectorIdFromRoute } from '../connector-canvas/connector-canvas.selectors';
import {
    cancelConnectorDrain,
    cancelConnectorDrainSuccess,
    connectorActionApiError,
    drainConnector,
    drainConnectorSuccess,
    loadConnectorEntity,
    loadConnectorEntityFailure,
    loadConnectorEntitySuccess,
    promptDrainConnector,
    startConnector,
    startConnectorSuccess,
    stopConnector,
    stopConnectorSuccess
} from './connector-canvas-entity.actions';
import { ConnectorEntity } from '@nifi/shared';
import type { Mock } from 'vitest';

describe('ConnectorCanvasEntityEffects', () => {
    function createMockConnectorEntity(overrides: Partial<ConnectorEntity> = {}): ConnectorEntity {
        return {
            id: 'connector-123',
            uri: '/connectors/connector-123',
            permissions: { canRead: true, canWrite: true },
            operatePermissions: { canRead: true, canWrite: true },
            revision: { version: 0 },
            bulletins: [],
            status: {},
            component: {
                id: 'connector-123',
                name: 'Test Connector',
                type: 'org.test.TestConnector',
                state: 'STOPPED',
                bundle: { group: 'org.test', artifact: 'test', version: '1.0' },
                availableActions: [],
                managedProcessGroupId: 'pg-123'
            },
            ...overrides
        } as ConnectorEntity;
    }

    interface SetupOptions {
        existingConnectorEntity?: ConnectorEntity | null;
        connectorIdFromRoute?: string | null;
    }

    function createMockDialogRef(data: Record<string, Observable<unknown> | Subject<unknown>> = {}) {
        return { componentInstance: data, afterClosed: () => new Subject<void>() };
    }

    async function setup(options: SetupOptions = {}) {
        const actions$ = new ReplaySubject<Action>(1);

        const mockConnectorService = {
            getConnector: vi.fn().mockReturnValue(of(createMockConnectorEntity())),
            drainConnector: vi.fn(),
            cancelConnectorDrain: vi.fn(),
            updateConnectorRunStatus: vi.fn()
        };

        const mockErrorHelper = {
            getErrorString: vi.fn().mockReturnValue('Error message')
        };

        const mockDialog = {
            open: vi.fn(),
            closeAll: vi.fn()
        };

        await TestBed.configureTestingModule({
            providers: [
                ConnectorCanvasEntityEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    selectors: [
                        {
                            selector: selectConnectorCanvasEntity,
                            value: options.existingConnectorEntity ?? null
                        },
                        {
                            selector: selectConnectorIdFromRoute,
                            value: options.connectorIdFromRoute ?? null
                        }
                    ]
                }),
                { provide: ConnectorService, useValue: mockConnectorService },
                { provide: ErrorHelper, useValue: mockErrorHelper },
                { provide: MatDialog, useValue: mockDialog }
            ]
        }).compileComponents();

        const effects = TestBed.inject(ConnectorCanvasEntityEffects);

        return {
            effects,
            actions$,
            mockConnectorService,
            mockErrorHelper,
            mockDialog
        };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('triggerEntityLoad$', () => {
        it('should dispatch loadConnectorEntity when loadConnectorFlow is dispatched and no entity in state', async () => {
            const { effects, actions$ } = await setup();

            actions$.next(
                ConnectorCanvasActions.loadConnectorFlow({
                    connectorId: 'connector-456',
                    processGroupId: 'pg-789'
                })
            );

            const result = await firstValueFrom(effects.triggerEntityLoad$);
            expect(result).toEqual(loadConnectorEntity({ connectorId: 'connector-456' }));
        });

        it('should dispatch loadConnectorEntity when entity in state has a different ID', async () => {
            const existingEntity = createMockConnectorEntity({ id: 'different-connector' });
            const { effects, actions$ } = await setup({ existingConnectorEntity: existingEntity });

            actions$.next(
                ConnectorCanvasActions.loadConnectorFlow({
                    connectorId: 'connector-456',
                    processGroupId: 'pg-789'
                })
            );

            const result = await firstValueFrom(effects.triggerEntityLoad$);
            expect(result).toEqual(loadConnectorEntity({ connectorId: 'connector-456' }));
        });

        it('should not dispatch loadConnectorEntity when entity already matches connector id', () =>
            new Promise<void>((resolve) => {
                setup({ existingConnectorEntity: createMockConnectorEntity({ id: 'connector-456' }) }).then(
                    ({ effects, actions$ }) => {
                        let emissionCount = 0;
                        const subscription = effects.triggerEntityLoad$.subscribe(() => {
                            emissionCount++;
                        });

                        actions$.next(
                            ConnectorCanvasActions.loadConnectorFlow({
                                connectorId: 'connector-456',
                                processGroupId: 'pg-789'
                            })
                        );

                        queueMicrotask(() => {
                            expect(emissionCount).toBe(0);
                            subscription.unsubscribe();
                            resolve();
                        });
                    }
                );
            }));
    });

    describe('loadConnectorEntity$', () => {
        it('should dispatch loadConnectorEntitySuccess when getConnector succeeds', async () => {
            const mockEntity = createMockConnectorEntity();
            const { effects, actions$, mockConnectorService } = await setup();
            (mockConnectorService.getConnector as Mock).mockReturnValue(of(mockEntity));

            actions$.next(loadConnectorEntity({ connectorId: 'connector-456' }));

            const result = await firstValueFrom(effects.loadConnectorEntity$);
            expect(result).toEqual(loadConnectorEntitySuccess({ connectorEntity: mockEntity }));
            expect(mockConnectorService.getConnector).toHaveBeenCalledWith('connector-456');
        });

        it('should dispatch loadConnectorEntityFailure when getConnector errors', async () => {
            const errorResponse = new HttpErrorResponse({ error: 'Error', status: 500, statusText: 'ISE' });
            const { effects, actions$, mockConnectorService, mockErrorHelper } = await setup();
            (mockConnectorService.getConnector as Mock).mockReturnValue(throwError(() => errorResponse));

            actions$.next(loadConnectorEntity({ connectorId: 'connector-456' }));

            const result = await firstValueFrom(effects.loadConnectorEntity$);
            expect(result).toEqual(loadConnectorEntityFailure({ error: 'Error message' }));
            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
        });
    });

    describe('promptDrainConnector$', () => {
        it('should open a confirmation dialog and dispatch drainConnector on confirmation', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockDialog }) => {
                    const mockDialogRef = createMockDialogRef({ yes: of(true) });
                    mockDialog.open.mockReturnValue(mockDialogRef);
                    const store = (effects as unknown as { store: { dispatch: (a: Action) => void } }).store;
                    const dispatchSpy = vi.spyOn(store, 'dispatch');

                    const entity = createMockConnectorEntity();
                    actions$.next(promptDrainConnector({ connector: entity }));

                    effects.promptDrainConnector$.subscribe(() => {
                        expect(mockDialog.open).toHaveBeenCalledWith(
                            YesNoDialog,
                            expect.objectContaining({
                                data: expect.objectContaining({
                                    title: 'Drain Connector',
                                    message: expect.stringContaining(
                                        "Are you sure you want to drain connector 'Test Connector'"
                                    )
                                })
                            })
                        );
                        expect(dispatchSpy).toHaveBeenCalledWith(drainConnector({ connector: entity }));
                        resolve();
                    });
                });
            }));
    });

    describe('drainConnector$', () => {
        it('should dispatch drainConnectorSuccess when drain succeeds', async () => {
            const refreshed = createMockConnectorEntity();
            const { effects, actions$, mockConnectorService } = await setup();
            (mockConnectorService.drainConnector as Mock).mockReturnValue(of(refreshed));

            actions$.next(drainConnector({ connector: createMockConnectorEntity() }));

            const result = await firstValueFrom(effects.drainConnector$);
            expect(result).toEqual(drainConnectorSuccess({ connector: refreshed }));
        });

        it('should dispatch connectorActionApiError when drain errors', async () => {
            const errorResponse = new HttpErrorResponse({ error: 'Error', status: 500, statusText: 'ISE' });
            const { effects, actions$, mockConnectorService } = await setup();
            (mockConnectorService.drainConnector as Mock).mockReturnValue(throwError(() => errorResponse));

            actions$.next(drainConnector({ connector: createMockConnectorEntity() }));

            const result = await firstValueFrom(effects.drainConnector$);
            expect(result).toEqual(connectorActionApiError({ error: 'Error message' }));
        });
    });

    describe('cancelConnectorDrain$', () => {
        it('should dispatch cancelConnectorDrainSuccess when cancel succeeds', async () => {
            const refreshed = createMockConnectorEntity();
            const { effects, actions$, mockConnectorService } = await setup();
            (mockConnectorService.cancelConnectorDrain as Mock).mockReturnValue(of(refreshed));

            actions$.next(cancelConnectorDrain({ connector: createMockConnectorEntity() }));

            const result = await firstValueFrom(effects.cancelConnectorDrain$);
            expect(result).toEqual(cancelConnectorDrainSuccess({ connector: refreshed }));
        });

        it('should dispatch connectorActionApiError when cancel errors', async () => {
            const errorResponse = new HttpErrorResponse({ error: 'Error', status: 500, statusText: 'ISE' });
            const { effects, actions$, mockConnectorService } = await setup();
            (mockConnectorService.cancelConnectorDrain as Mock).mockReturnValue(throwError(() => errorResponse));

            actions$.next(cancelConnectorDrain({ connector: createMockConnectorEntity() }));

            const result = await firstValueFrom(effects.cancelConnectorDrain$);
            expect(result).toEqual(connectorActionApiError({ error: 'Error message' }));
        });
    });

    describe('startConnector$', () => {
        it('should dispatch startConnectorSuccess when start succeeds', async () => {
            const refreshed = createMockConnectorEntity();
            const { effects, actions$, mockConnectorService } = await setup();
            (mockConnectorService.updateConnectorRunStatus as Mock).mockReturnValue(of(refreshed));

            const request = createMockConnectorEntity();
            actions$.next(startConnector({ connector: request }));

            const result = await firstValueFrom(effects.startConnector$);
            expect(result).toEqual(startConnectorSuccess({ connector: refreshed }));
            expect(mockConnectorService.updateConnectorRunStatus).toHaveBeenCalledWith(request, 'RUNNING');
        });

        it('should dispatch connectorActionApiError when start errors', async () => {
            const errorResponse = new HttpErrorResponse({ error: 'Error', status: 500, statusText: 'ISE' });
            const { effects, actions$, mockConnectorService } = await setup();
            (mockConnectorService.updateConnectorRunStatus as Mock).mockReturnValue(throwError(() => errorResponse));

            actions$.next(startConnector({ connector: createMockConnectorEntity() }));

            const result = await firstValueFrom(effects.startConnector$);
            expect(result).toEqual(connectorActionApiError({ error: 'Error message' }));
        });
    });

    describe('stopConnector$', () => {
        it('should dispatch stopConnectorSuccess when stop succeeds', async () => {
            const refreshed = createMockConnectorEntity();
            const { effects, actions$, mockConnectorService } = await setup();
            (mockConnectorService.updateConnectorRunStatus as Mock).mockReturnValue(of(refreshed));

            const request = createMockConnectorEntity();
            actions$.next(stopConnector({ connector: request }));

            const result = await firstValueFrom(effects.stopConnector$);
            expect(result).toEqual(stopConnectorSuccess({ connector: refreshed }));
            expect(mockConnectorService.updateConnectorRunStatus).toHaveBeenCalledWith(request, 'STOPPED');
        });

        it('should dispatch connectorActionApiError when stop errors', async () => {
            const errorResponse = new HttpErrorResponse({ error: 'Error', status: 500, statusText: 'ISE' });
            const { effects, actions$, mockConnectorService } = await setup();
            (mockConnectorService.updateConnectorRunStatus as Mock).mockReturnValue(throwError(() => errorResponse));

            actions$.next(stopConnector({ connector: createMockConnectorEntity() }));

            const result = await firstValueFrom(effects.stopConnector$);
            expect(result).toEqual(connectorActionApiError({ error: 'Error message' }));
        });
    });

    describe('refreshAfterAction$', () => {
        it('should dispatch loadConnectorEntity on drainConnectorSuccess when connectorId is in route', async () => {
            const { effects, actions$ } = await setup({ connectorIdFromRoute: 'connector-789' });

            actions$.next(drainConnectorSuccess({ connector: createMockConnectorEntity() }));

            const result = await firstValueFrom(effects.refreshAfterAction$);
            expect(result).toEqual(loadConnectorEntity({ connectorId: 'connector-789' }));
        });

        it('should dispatch loadConnectorEntity on cancelConnectorDrainSuccess when connectorId is in route', async () => {
            const { effects, actions$ } = await setup({ connectorIdFromRoute: 'connector-789' });

            actions$.next(cancelConnectorDrainSuccess({ connector: createMockConnectorEntity() }));

            const result = await firstValueFrom(effects.refreshAfterAction$);
            expect(result).toEqual(loadConnectorEntity({ connectorId: 'connector-789' }));
        });

        it('should dispatch loadConnectorEntity on startConnectorSuccess when connectorId is in route', async () => {
            const { effects, actions$ } = await setup({ connectorIdFromRoute: 'connector-789' });

            actions$.next(startConnectorSuccess({ connector: createMockConnectorEntity() }));

            const result = await firstValueFrom(effects.refreshAfterAction$);
            expect(result).toEqual(loadConnectorEntity({ connectorId: 'connector-789' }));
        });

        it('should dispatch loadConnectorEntity on stopConnectorSuccess when connectorId is in route', async () => {
            const { effects, actions$ } = await setup({ connectorIdFromRoute: 'connector-789' });

            actions$.next(stopConnectorSuccess({ connector: createMockConnectorEntity() }));

            const result = await firstValueFrom(effects.refreshAfterAction$);
            expect(result).toEqual(loadConnectorEntity({ connectorId: 'connector-789' }));
        });

        it('should not dispatch when no connectorId in route', () =>
            new Promise<void>((resolve) => {
                setup({ connectorIdFromRoute: null }).then(({ effects, actions$ }) => {
                    let emissionCount = 0;
                    const subscription = effects.refreshAfterAction$.subscribe(() => {
                        emissionCount++;
                    });

                    actions$.next(drainConnectorSuccess({ connector: createMockConnectorEntity() }));

                    queueMicrotask(() => {
                        expect(emissionCount).toBe(0);
                        subscription.unsubscribe();
                        resolve();
                    });
                });
            }));
    });

    describe('connectorActionApiError$', () => {
        it('should dispatch addBannerError with CONNECTOR_CANVAS context', async () => {
            const { effects, actions$ } = await setup();

            actions$.next(connectorActionApiError({ error: 'boom' }));

            const result = await firstValueFrom(effects.connectorActionApiError$);
            expect(result).toEqual(
                ErrorActions.addBannerError({
                    errorContext: { errors: ['boom'], context: ErrorContextKey.CONNECTOR_CANVAS }
                })
            );
        });
    });

    describe('clearCanvasErrorsOnAction$', () => {
        const actionFactories = [
            { name: 'drainConnector', factory: drainConnector },
            { name: 'cancelConnectorDrain', factory: cancelConnectorDrain },
            { name: 'startConnector', factory: startConnector },
            { name: 'stopConnector', factory: stopConnector }
        ];

        actionFactories.forEach(({ name, factory }) => {
            it(`should dispatch clearBannerErrors on ${name}`, async () => {
                const { effects, actions$ } = await setup();

                actions$.next(factory({ connector: createMockConnectorEntity() }));

                const result = await firstValueFrom(effects.clearCanvasErrorsOnAction$);
                expect(result).toEqual(ErrorActions.clearBannerErrors({ context: ErrorContextKey.CONNECTOR_CANVAS }));
            });
        });
    });
});
