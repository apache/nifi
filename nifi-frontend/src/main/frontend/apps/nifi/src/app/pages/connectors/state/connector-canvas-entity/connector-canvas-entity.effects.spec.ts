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
import { firstValueFrom, of, ReplaySubject, throwError } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { ConnectorCanvasEntityEffects } from './connector-canvas-entity.effects';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import * as ConnectorCanvasActions from '../connector-canvas/connector-canvas.actions';
import { selectConnectorCanvasEntity } from './connector-canvas-entity.selectors';
import {
    loadConnectorEntity,
    loadConnectorEntityFailure,
    loadConnectorEntitySuccess
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
    }

    async function setup(options: SetupOptions = {}) {
        const actions$ = new ReplaySubject<Action>(1);

        const mockConnectorService = {
            getConnector: vi.fn().mockReturnValue(of(createMockConnectorEntity()))
        };

        const mockErrorHelper = {
            getErrorString: vi.fn().mockReturnValue('Error message')
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
                        }
                    ]
                }),
                { provide: ConnectorService, useValue: mockConnectorService },
                { provide: ErrorHelper, useValue: mockErrorHelper }
            ]
        }).compileComponents();

        const effects = TestBed.inject(ConnectorCanvasEntityEffects);

        return {
            effects,
            actions$,
            mockConnectorService,
            mockErrorHelper
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
});
