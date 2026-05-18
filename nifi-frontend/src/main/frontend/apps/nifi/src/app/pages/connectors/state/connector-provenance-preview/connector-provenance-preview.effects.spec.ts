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
import { firstValueFrom, of, ReplaySubject, Subject, throwError } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { MatDialog } from '@angular/material/dialog';

import { ConnectorProvenanceEffects } from './connector-provenance-preview.effects';
import * as ConnectorProvenanceActions from './connector-provenance-preview.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ProvenanceService } from '../../../provenance/service/provenance.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { selectAbout } from '../../../../state/about/about.selectors';
import { ProvenanceEvent } from '../../../../state/shared';
import { ErrorContextKey } from '../../../../state/error';

describe('ConnectorProvenanceEffects', () => {
    const SAMPLE_EVENT: ProvenanceEvent = {
        id: 'e1',
        eventId: 42,
        eventType: 'RECEIVE',
        clusterNodeId: 'node-1',
        attributes: [{ name: 'mime.type', value: 'text/plain', previousValue: 'application/json' }]
    } as unknown as ProvenanceEvent;

    interface SetupOptions {
        about?: { uri: string; contentViewerUrl: string | null } | null;
    }

    function createMockDialogRef() {
        return {
            componentInstance: {
                contentViewerAvailable: false,
                downloadContent: new Subject<string>(),
                viewContent: new Subject<string>(),
                replay: new Subject<void>()
            },
            afterClosed: () => new Subject<void>(),
            close: vi.fn()
        };
    }

    async function setup(options: SetupOptions = {}) {
        const actions$ = new ReplaySubject<Action>(1);

        const mockProvenanceService = {
            getLatestEventsForComponent: vi.fn(),
            downloadContent: vi.fn(),
            viewContent: vi.fn(),
            replay: vi.fn().mockReturnValue(of({}))
        };

        const mockErrorHelper = {
            getErrorString: vi.fn().mockReturnValue('Network failure')
        };

        const mockDialog = {
            open: vi.fn().mockReturnValue(createMockDialogRef())
        };

        await TestBed.configureTestingModule({
            providers: [
                ConnectorProvenanceEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    selectors: [{ selector: selectAbout, value: options.about ?? null }]
                }),
                { provide: ProvenanceService, useValue: mockProvenanceService },
                { provide: ErrorHelper, useValue: mockErrorHelper },
                { provide: MatDialog, useValue: mockDialog }
            ]
        }).compileComponents();

        const effects = TestBed.inject(ConnectorProvenanceEffects);

        return {
            effects,
            actions$,
            mockProvenanceService,
            mockErrorHelper,
            mockDialog
        };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('loadLatestEventsForComponent$', () => {
        it('should dispatch loadLatestEventsForComponentSuccess on a successful response', async () => {
            const { effects, actions$, mockProvenanceService } = await setup();
            mockProvenanceService.getLatestEventsForComponent.mockReturnValue(
                of({ latestProvenanceEvents: { provenanceEvents: [SAMPLE_EVENT] } })
            );

            actions$.next(ConnectorProvenanceActions.loadLatestEventsForComponent({ componentId: 'proc-1' }));

            const result = await firstValueFrom(effects.loadLatestEventsForComponent$);
            expect(mockProvenanceService.getLatestEventsForComponent).toHaveBeenCalledWith('proc-1');
            expect(result).toEqual(
                ConnectorProvenanceActions.loadLatestEventsForComponentSuccess({ events: [SAMPLE_EVENT] })
            );
        });

        it('should dispatch loadError when the request fails', async () => {
            const { effects, actions$, mockProvenanceService, mockErrorHelper } = await setup();
            mockProvenanceService.getLatestEventsForComponent.mockReturnValue(
                throwError(() => new HttpErrorResponse({ status: 500 }))
            );

            actions$.next(ConnectorProvenanceActions.loadLatestEventsForComponent({ componentId: 'proc-1' }));

            const result = await firstValueFrom(effects.loadLatestEventsForComponent$);
            expect(mockErrorHelper.getErrorString).toHaveBeenCalled();
            expect(result).toEqual(ConnectorProvenanceActions.loadError({ error: 'Network failure' }));
        });
    });

    describe('downloadContent$', () => {
        it('should call provenanceService.downloadContent with the event details', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockProvenanceService }) => {
                    const subscription = effects.downloadContent$.subscribe(() => {
                        // non-dispatching effect; subscribe to keep it alive
                    });

                    actions$.next(
                        ConnectorProvenanceActions.downloadContent({
                            request: { event: SAMPLE_EVENT, direction: 'input' }
                        })
                    );

                    queueMicrotask(() => {
                        expect(mockProvenanceService.downloadContent).toHaveBeenCalledWith(
                            SAMPLE_EVENT.eventId,
                            'input',
                            SAMPLE_EVENT.clusterNodeId
                        );
                        subscription.unsubscribe();
                        resolve();
                    });
                });
            }));
    });

    describe('viewContent$', () => {
        it('should call provenanceService.viewContent with the resolved mime type when about is set', () =>
            new Promise<void>((resolve) => {
                setup({
                    about: { uri: 'http://nifi/', contentViewerUrl: 'http://viewer/' }
                }).then(({ effects, actions$, mockProvenanceService }) => {
                    const subscription = effects.viewContent$.subscribe(() => {
                        // non-dispatching effect; subscribe to keep it alive
                    });

                    actions$.next(
                        ConnectorProvenanceActions.viewContent({
                            request: { event: SAMPLE_EVENT, direction: 'input' }
                        })
                    );

                    queueMicrotask(() => {
                        expect(mockProvenanceService.viewContent).toHaveBeenCalledWith(
                            'http://nifi/',
                            'http://viewer/',
                            SAMPLE_EVENT.eventId,
                            'input',
                            SAMPLE_EVENT.clusterNodeId,
                            'application/json'
                        );
                        subscription.unsubscribe();
                        resolve();
                    });
                });
            }));

        it('should NOT call provenanceService.viewContent when about is null', () =>
            new Promise<void>((resolve) => {
                setup({ about: null }).then(({ effects, actions$, mockProvenanceService }) => {
                    const subscription = effects.viewContent$.subscribe(() => {
                        // non-dispatching effect; subscribe to keep it alive
                    });

                    actions$.next(
                        ConnectorProvenanceActions.viewContent({
                            request: { event: SAMPLE_EVENT, direction: 'output' }
                        })
                    );

                    queueMicrotask(() => {
                        expect(mockProvenanceService.viewContent).not.toHaveBeenCalled();
                        subscription.unsubscribe();
                        resolve();
                    });
                });
            }));
    });

    describe('replayEvent$', () => {
        it('should dispatch showOkDialog on successful replay', async () => {
            const { effects, actions$, mockProvenanceService } = await setup();

            actions$.next(
                ConnectorProvenanceActions.replayEvent({
                    request: { event: SAMPLE_EVENT }
                })
            );

            const result = await firstValueFrom(effects.replayEvent$);
            expect(mockProvenanceService.replay).toHaveBeenCalledWith(SAMPLE_EVENT.eventId, SAMPLE_EVENT.clusterNodeId);
            expect(result).toEqual(
                ConnectorProvenanceActions.showOkDialog({
                    title: 'Provenance',
                    message: 'Successfully submitted replay request.'
                })
            );
        });

        it('should dispatch addBannerError under CONNECTOR_CANVAS context when replay fails', async () => {
            const { effects, actions$, mockProvenanceService } = await setup();
            mockProvenanceService.replay.mockReturnValue(throwError(() => new HttpErrorResponse({ status: 500 })));

            actions$.next(
                ConnectorProvenanceActions.replayEvent({
                    request: { event: SAMPLE_EVENT }
                })
            );

            const result = await firstValueFrom(effects.replayEvent$);
            expect(result).toEqual(
                ErrorActions.addBannerError({
                    errorContext: {
                        errors: ['Network failure'],
                        context: ErrorContextKey.CONNECTOR_CANVAS
                    }
                })
            );
        });
    });

    describe('openProvenanceEventDialog$', () => {
        it('should open a ProvenanceEventDialog with the event request', () =>
            new Promise<void>((resolve) => {
                setup({ about: { uri: 'http://nifi/', contentViewerUrl: 'http://viewer/' } }).then(
                    ({ effects, actions$, mockDialog }) => {
                        const subscription = effects.openProvenanceEventDialog$.subscribe(() => {
                            // non-dispatching effect; subscribe to keep it alive
                        });

                        actions$.next(
                            ConnectorProvenanceActions.openProvenanceEventDialog({
                                request: { event: SAMPLE_EVENT }
                            })
                        );

                        queueMicrotask(() => {
                            expect(mockDialog.open).toHaveBeenCalled();
                            const args = mockDialog.open.mock.calls[0];
                            expect(args[1].data).toEqual({ event: SAMPLE_EVENT });
                            subscription.unsubscribe();
                            resolve();
                        });
                    }
                );
            }));
    });

    describe('showOkDialog$', () => {
        it('should open an OkDialog with the supplied title and message', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockDialog }) => {
                    const subscription = effects.showOkDialog$.subscribe(() => {
                        // non-dispatching effect; subscribe to keep it alive
                    });

                    actions$.next(
                        ConnectorProvenanceActions.showOkDialog({
                            title: 'Provenance',
                            message: 'Successfully submitted replay request.'
                        })
                    );

                    queueMicrotask(() => {
                        expect(mockDialog.open).toHaveBeenCalled();
                        const args = mockDialog.open.mock.calls[0];
                        expect(args[1].data).toEqual({
                            title: 'Provenance',
                            message: 'Successfully submitted replay request.'
                        });
                        subscription.unsubscribe();
                        resolve();
                    });
                });
            }));
    });
});
