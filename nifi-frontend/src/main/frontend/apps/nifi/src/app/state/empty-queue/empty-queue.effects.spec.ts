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
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { Action } from '@ngrx/store';
import { MatDialog } from '@angular/material/dialog';
import { firstValueFrom, Observable, of, Subject } from 'rxjs';
import { EmptyQueueEffects } from './empty-queue.effects';
import {
    deleteEmptyQueueRequest,
    emptyQueueApiError,
    pollEmptyQueueRequest,
    queueEmptied,
    showEmptyQueueResults,
    submitEmptyQueueRequestSuccess,
    submitEmptyQueuesRequest,
    submitEmptyQueuesRequestSuccess
} from './empty-queue.actions';
import {
    selectDropConnectionId,
    selectDropProcessGroupId,
    selectDropRequestEntity,
    selectDropSource
} from './empty-queue.selectors';
import { EmptyQueueService } from '../../service/empty-queue.service';
import { ErrorHelper } from '../../service/error-helper.service';
import { CanvasActionSource } from '../shared';
import { DropRequestEntity } from './index';

function createDropEntity(overrides: Partial<DropRequestEntity['dropRequest']> = {}): DropRequestEntity {
    return {
        dropRequest: {
            id: 'drop-1',
            uri: 'http://example/drop-requests/drop-1',
            submissionTime: '2023-01-01T00:00:00Z',
            lastUpdated: '2023-01-01T00:00:01Z',
            percentCompleted: 100,
            finished: true,
            failureReason: '',
            currentCount: 0,
            currentSize: 0,
            current: '0 / 0 bytes',
            originalCount: 5,
            originalSize: 100,
            original: '5 / 100 bytes',
            droppedCount: 5,
            droppedSize: 100,
            dropped: '5 / 100 bytes',
            state: 'COMPLETE',
            ...overrides
        }
    };
}

describe('EmptyQueueEffects', () => {
    async function setup(
        options: {
            connectionId?: string | null;
            processGroupId?: string | null;
            source?: CanvasActionSource | null;
            dropEntity?: DropRequestEntity | null;
            emptyQueueService?: Partial<EmptyQueueService>;
        } = {}
    ) {
        let actions$: Observable<Action>;

        const okDialogClosed$ = new Subject<void>();
        const mockDialog = {
            open: vi.fn().mockReturnValue({
                componentInstance: {
                    yes: of(),
                    exit: of()
                },
                afterClosed: () => okDialogClosed$
            }),
            closeAll: vi.fn()
        };

        await TestBed.configureTestingModule({
            providers: [
                EmptyQueueEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    initialState: {},
                    selectors: [
                        {
                            selector: selectDropConnectionId,
                            value: options.connectionId !== undefined ? options.connectionId : null
                        },
                        {
                            selector: selectDropProcessGroupId,
                            value: options.processGroupId !== undefined ? options.processGroupId : null
                        },
                        {
                            selector: selectDropSource,
                            value: options.source !== undefined ? options.source : null
                        },
                        {
                            selector: selectDropRequestEntity,
                            value: options.dropEntity !== undefined ? options.dropEntity : null
                        }
                    ]
                }),
                { provide: MatDialog, useValue: mockDialog },
                {
                    provide: EmptyQueueService,
                    useValue: { submitEmptyQueueRequest: vi.fn(), ...(options.emptyQueueService ?? {}) }
                },
                { provide: ErrorHelper, useValue: { getErrorString: vi.fn() } }
            ]
        }).compileComponents();

        const effects = TestBed.inject(EmptyQueueEffects);
        const store = TestBed.inject(MockStore);
        const dispatchSpy = vi.spyOn(store, 'dispatch');

        return {
            effects,
            store,
            dispatchSpy,
            mockDialog,
            actions$: (stream: Observable<Action>) => {
                actions$ = stream;
            }
        };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('submitEmptyQueuesRequest$', () => {
        it('should dispatch submitEmptyQueuesRequestSuccess for the bulk path so success handlers can distinguish single vs bulk submits', async () => {
            const dropEntity = createDropEntity({ finished: false, percentCompleted: 25 });
            const { effects, actions$ } = await setup({
                emptyQueueService: {
                    submitEmptyQueuesRequest: vi.fn().mockReturnValue(of(dropEntity))
                }
            });
            actions$(
                of(
                    submitEmptyQueuesRequest({
                        request: { processGroupId: 'pg-1', source: 'flow-designer' }
                    })
                )
            );

            const action = await firstValueFrom(effects.submitEmptyQueuesRequest$);

            expect(action).toEqual(submitEmptyQueuesRequestSuccess({ response: { dropEntity } }));
        });
    });

    describe('submitEmptyQueueRequestSuccess$', () => {
        it('should request deletion when the drop request is finished', async () => {
            const { effects, actions$ } = await setup();
            actions$(
                of(
                    submitEmptyQueueRequestSuccess({
                        response: { dropEntity: createDropEntity({ finished: true }) }
                    })
                )
            );

            const action = await firstValueFrom(effects.submitEmptyQueueRequestSuccess$);

            expect(action.type).toBe('[Empty Queue] Delete Empty Queue Request');
        });

        it('should also react to the bulk submitEmptyQueuesRequestSuccess action', async () => {
            const { effects, actions$ } = await setup();
            actions$(
                of(
                    submitEmptyQueuesRequestSuccess({
                        response: { dropEntity: createDropEntity({ finished: false, percentCompleted: 50 }) }
                    })
                )
            );

            const action = await firstValueFrom(effects.submitEmptyQueueRequestSuccess$);

            expect(action.type).toBe('[Empty Queue] Start Polling Empty Queue Request');
        });

        it('should start polling when the drop request is not finished', async () => {
            const { effects, actions$ } = await setup();
            actions$(
                of(
                    submitEmptyQueueRequestSuccess({
                        response: { dropEntity: createDropEntity({ finished: false, percentCompleted: 50 }) }
                    })
                )
            );

            const action = await firstValueFrom(effects.submitEmptyQueueRequestSuccess$);

            expect(action.type).toBe('[Empty Queue] Start Polling Empty Queue Request');
        });
    });

    describe('showEmptyQueueResults$', () => {
        async function runShowResults(
            options: {
                connectionId?: string | null;
                processGroupId?: string | null;
                source?: CanvasActionSource | null;
            } = {}
        ) {
            const ctx = await setup(options);
            ctx.actions$(
                of(
                    showEmptyQueueResults({
                        request: { dropEntity: createDropEntity() }
                    })
                )
            );
            await firstValueFrom(ctx.effects.showEmptyQueueResults$);
            return ctx;
        }

        it('should dispatch queueEmptied with the captured source for a connector-canvas connection empty', async () => {
            const { dispatchSpy } = await runShowResults({
                connectionId: 'conn-1',
                processGroupId: null,
                source: 'connector-canvas'
            });

            expect(dispatchSpy).toHaveBeenCalledWith(
                queueEmptied({
                    connectionId: 'conn-1',
                    processGroupId: null,
                    source: 'connector-canvas'
                })
            );
        });

        it('should dispatch queueEmptied with the process group id for an empty-all-queues from flow designer', async () => {
            const { dispatchSpy } = await runShowResults({
                connectionId: null,
                processGroupId: 'pg-1',
                source: 'flow-designer'
            });

            expect(dispatchSpy).toHaveBeenCalledWith(
                queueEmptied({
                    connectionId: null,
                    processGroupId: 'pg-1',
                    source: 'flow-designer'
                })
            );
        });

        it('should not dispatch queueEmptied when no source is captured in state', async () => {
            const { dispatchSpy } = await runShowResults({
                connectionId: 'conn-1',
                processGroupId: null,
                source: null
            });

            const queueEmptiedDispatches = dispatchSpy.mock.calls.filter(
                (call) => (call[0] as { type: string }).type === queueEmptied.type
            );
            expect(queueEmptiedDispatches).toHaveLength(0);
        });
    });

    describe('pollEmptyQueueRequest$', () => {
        it('should dispatch emptyQueueApiError when neither connectionId nor processGroupId is set', async () => {
            const pollEmptyQueueRequestSpy = vi.fn();
            const pollEmptyQueuesRequestSpy = vi.fn();
            const { effects, actions$ } = await setup({
                connectionId: null,
                processGroupId: null,
                dropEntity: createDropEntity(),
                emptyQueueService: {
                    pollEmptyQueueRequest: pollEmptyQueueRequestSpy,
                    pollEmptyQueuesRequest: pollEmptyQueuesRequestSpy
                }
            });
            actions$(of(pollEmptyQueueRequest()));

            const action = await firstValueFrom(effects.pollEmptyQueueRequest$);

            expect(action).toEqual(
                emptyQueueApiError({
                    error: 'Unable to poll empty queue request: no connection or process group is associated with the active request.'
                })
            );
            expect(pollEmptyQueueRequestSpy).not.toHaveBeenCalled();
            expect(pollEmptyQueuesRequestSpy).not.toHaveBeenCalled();
        });
    });

    describe('deleteEmptyQueueRequest$', () => {
        it('should dispatch emptyQueueApiError when neither connectionId nor processGroupId is set', async () => {
            const deleteEmptyQueueRequestSpy = vi.fn();
            const deleteEmptyQueuesRequestSpy = vi.fn();
            const { effects, actions$, mockDialog } = await setup({
                connectionId: null,
                processGroupId: null,
                dropEntity: createDropEntity(),
                emptyQueueService: {
                    deleteEmptyQueueRequest: deleteEmptyQueueRequestSpy,
                    deleteEmptyQueuesRequest: deleteEmptyQueuesRequestSpy
                }
            });
            actions$(of(deleteEmptyQueueRequest()));

            const action = await firstValueFrom(effects.deleteEmptyQueueRequest$);

            expect(action).toEqual(
                emptyQueueApiError({
                    error: 'Unable to delete empty queue request: no connection or process group is associated with the active request.'
                })
            );
            expect(deleteEmptyQueueRequestSpy).not.toHaveBeenCalled();
            expect(deleteEmptyQueuesRequestSpy).not.toHaveBeenCalled();
            expect(mockDialog.closeAll).toHaveBeenCalled();
        });
    });
});
