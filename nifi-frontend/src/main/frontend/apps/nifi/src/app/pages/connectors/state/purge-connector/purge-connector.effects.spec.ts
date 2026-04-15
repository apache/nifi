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
import { Observable, of, throwError, Subject } from 'rxjs';
import { Action } from '@ngrx/store';
import { PurgeConnectorEffects } from './purge-connector.effects';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { MatDialog } from '@angular/material/dialog';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { HttpErrorResponse } from '@angular/common/http';
import { ConnectorAction, ConnectorActionName, ConnectorEntity, ConnectorStatus } from '@nifi/shared';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorContextKey } from '../../../../state/error';
import { selectPurgeConnectorId, selectPurgeDropRequestEntity } from './purge-connector.selectors';
import { loadConnectorsListing } from '../connectors-listing/connectors-listing.actions';
import {
    deletePurgeRequest,
    pollPurgeConnector,
    pollPurgeConnectorSuccess,
    promptPurgeConnector,
    purgeConnectorApiError,
    showPurgeConnectorResults,
    startPollingPurgeConnector,
    stopPollingPurgeConnector,
    submitPurgeConnector,
    submitPurgeConnectorSuccess
} from './purge-connector.actions';
import { DropRequestEntity } from '../../../flow-designer/state/queue';
import type { Mock } from 'vitest';

describe('PurgeConnectorEffects', () => {
    function createMockAction(name: ConnectorActionName, allowed = true, reasonNotAllowed?: string): ConnectorAction {
        const action: ConnectorAction = { name, description: `${name} action`, allowed };
        if (reasonNotAllowed !== undefined) {
            action.reasonNotAllowed = reasonNotAllowed;
        }
        return action;
    }

    function createMockConnector(
        options: {
            id?: string;
            name?: string;
            availableActions?: ConnectorAction[];
        } = {}
    ): ConnectorEntity {
        const defaultActions: ConnectorAction[] = options.availableActions ?? [
            createMockAction('START', true),
            createMockAction('STOP', true),
            createMockAction('CONFIGURE', true),
            createMockAction('DELETE', true)
        ];

        return {
            id: options.id || 'connector-123',
            uri: `http://localhost/nifi-api/connectors/${options.id || 'connector-123'}`,
            permissions: { canRead: true, canWrite: true },
            revision: { version: 1, clientId: 'client-1' },
            bulletins: [],
            component: {
                id: options.id || 'connector-123',
                type: 'org.apache.nifi.connector.TestConnector',
                bundle: { group: 'org.apache.nifi', artifact: 'nifi-test-nar', version: '1.0.0' },
                name: options.name || 'Test Connector',
                state: 'STOPPED',
                managedProcessGroupId: 'pg-root-default',
                availableActions: defaultActions
            },
            status: { runStatus: 'STOPPED' } as ConnectorStatus
        };
    }

    function createMockDropRequestEntity(
        options: {
            id?: string;
            finished?: boolean;
            percentCompleted?: number;
            dropped?: string;
            original?: string;
            failureReason?: string;
        } = {}
    ): DropRequestEntity {
        return {
            dropRequest: {
                id: options.id || 'purge-req-1',
                uri: `/connectors/connector-123/purge-requests/${options.id || 'purge-req-1'}`,
                submissionTime: '2025-01-01T00:00:00.000Z',
                lastUpdated: '2025-01-01T00:00:01.000Z',
                percentCompleted: options.percentCompleted ?? 100,
                finished: options.finished ?? true,
                failureReason: options.failureReason || '',
                currentCount: 0,
                currentSize: 0,
                current: '0 / 0 bytes',
                originalCount: 10,
                originalSize: 1024,
                original: options.original || '10 / 1 KB',
                droppedCount: 10,
                droppedSize: 1024,
                dropped: options.dropped || '10 / 1 KB',
                state: 'Completed'
            }
        };
    }

    async function setup(
        options: {
            purgeConnectorId?: string | null;
            purgeDropEntity?: DropRequestEntity | null;
        } = {}
    ) {
        let actions$: Observable<Action>;

        const mockConnectorService = {
            createPurgeRequest: vi.fn(),
            getPurgeRequest: vi.fn(),
            deletePurgeRequest: vi.fn()
        };

        const mockErrorHelper = {
            getErrorString: vi.fn().mockReturnValue('Error message'),
            handleLoadingError: vi.fn()
        };

        const mockDialog = {
            open: vi.fn(),
            closeAll: vi.fn()
        };

        await TestBed.configureTestingModule({
            providers: [
                PurgeConnectorEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    initialState: {},
                    selectors: [
                        { selector: selectPurgeConnectorId, value: options.purgeConnectorId ?? null },
                        { selector: selectPurgeDropRequestEntity, value: options.purgeDropEntity ?? null }
                    ]
                }),
                { provide: ConnectorService, useValue: mockConnectorService },
                { provide: ErrorHelper, useValue: mockErrorHelper },
                { provide: MatDialog, useValue: mockDialog }
            ]
        }).compileComponents();

        const effects = TestBed.inject(PurgeConnectorEffects);
        const store = TestBed.inject(MockStore);

        return {
            effects,
            store,
            actions$: (action: Observable<Action>) => {
                actions$ = action;
            },
            mockConnectorService,
            mockErrorHelper,
            mockDialog
        };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('promptPurgeConnector$', () => {
        it('should show confirmation dialog', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockDialog }) => {
                    const mockConnector = createMockConnector();
                    const yesSubject = new Subject<void>();
                    mockDialog.open.mockReturnValue({
                        componentInstance: { yes: yesSubject.asObservable(), no: new Subject<void>().asObservable() }
                    });

                    actions$(of(promptPurgeConnector({ request: { connector: mockConnector } })));

                    effects.promptPurgeConnector$.subscribe(() => {
                        expect(mockDialog.open).toHaveBeenCalled();
                        const dialogData = mockDialog.open.mock.calls[0][1].data;
                        expect(dialogData.title).toBe('Purge Connector');
                        expect(dialogData.message).toContain(mockConnector.component.name);
                        resolve();
                    });
                });
            }));

        it('should dispatch submitPurgeConnector when user confirms', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockDialog, store }) => {
                    const mockConnector = createMockConnector();
                    const yesSubject = new Subject<void>();
                    mockDialog.open.mockReturnValue({
                        componentInstance: { yes: yesSubject.asObservable(), no: new Subject<void>().asObservable() }
                    });

                    const dispatchSpy = vi.spyOn(store, 'dispatch');
                    actions$(of(promptPurgeConnector({ request: { connector: mockConnector } })));

                    effects.promptPurgeConnector$.subscribe(() => {
                        yesSubject.next();
                        expect(dispatchSpy).toHaveBeenCalledWith(
                            submitPurgeConnector({ request: { connector: mockConnector } })
                        );
                        resolve();
                    });
                });
            }));
    });

    describe('submitPurgeConnector$', () => {
        it('should submit purge request successfully', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService, mockDialog }) => {
                    const mockConnector = createMockConnector();
                    const dropEntity = createMockDropRequestEntity({ finished: false, percentCompleted: 0 });
                    const exitSubject = new Subject<void>();
                    mockDialog.open.mockReturnValue({ componentInstance: { exit: exitSubject.asObservable() } });

                    (mockConnectorService.createPurgeRequest as Mock).mockReturnValue(of(dropEntity));
                    actions$(of(submitPurgeConnector({ request: { connector: mockConnector } })));

                    effects.submitPurgeConnector$.subscribe((action) => {
                        expect(action).toEqual(submitPurgeConnectorSuccess({ response: { dropEntity } }));
                        expect(mockConnectorService.createPurgeRequest).toHaveBeenCalledWith(mockConnector.id);
                        resolve();
                    });
                });
            }));

        it('should handle error when submitting purge request', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService, mockErrorHelper, mockDialog }) => {
                    const mockConnector = createMockConnector();
                    const exitSubject = new Subject<void>();
                    mockDialog.open.mockReturnValue({ componentInstance: { exit: exitSubject.asObservable() } });

                    const errorResponse = new HttpErrorResponse({ error: 'Failed', status: 500, statusText: 'ISE' });
                    (mockConnectorService.createPurgeRequest as Mock).mockReturnValue(throwError(() => errorResponse));
                    actions$(of(submitPurgeConnector({ request: { connector: mockConnector } })));

                    effects.submitPurgeConnector$.subscribe((action) => {
                        expect(action).toEqual(purgeConnectorApiError({ error: 'Error message' }));
                        expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
                        resolve();
                    });
                });
            }));
    });

    describe('submitPurgeConnectorSuccess$', () => {
        it('should start polling when purge is not finished', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$ }) => {
                    const dropEntity = createMockDropRequestEntity({ finished: false, percentCompleted: 50 });
                    actions$(of(submitPurgeConnectorSuccess({ response: { dropEntity } })));

                    effects.submitPurgeConnectorSuccess$.subscribe((action) => {
                        expect(action).toEqual(startPollingPurgeConnector());
                        resolve();
                    });
                });
            }));

        it('should delete purge request immediately when already finished', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$ }) => {
                    const dropEntity = createMockDropRequestEntity({ finished: true, percentCompleted: 100 });
                    actions$(of(submitPurgeConnectorSuccess({ response: { dropEntity } })));

                    effects.submitPurgeConnectorSuccess$.subscribe((action) => {
                        expect(action).toEqual(deletePurgeRequest());
                        resolve();
                    });
                });
            }));
    });

    describe('pollPurgeConnector$', () => {
        it('should poll and dispatch success', () =>
            new Promise<void>((resolve) => {
                const dropEntity = createMockDropRequestEntity({
                    id: 'purge-req-1',
                    finished: false,
                    percentCompleted: 50
                });
                const updatedDropEntity = createMockDropRequestEntity({
                    id: 'purge-req-1',
                    finished: false,
                    percentCompleted: 75
                });

                setup({ purgeConnectorId: 'connector-123', purgeDropEntity: dropEntity }).then(
                    ({ effects, actions$, mockConnectorService }) => {
                        (mockConnectorService.getPurgeRequest as Mock).mockReturnValue(of(updatedDropEntity));
                        actions$(of(pollPurgeConnector()));

                        effects.pollPurgeConnector$.subscribe((action) => {
                            expect(mockConnectorService.getPurgeRequest).toHaveBeenCalledWith(
                                'connector-123',
                                'purge-req-1'
                            );
                            expect(action).toEqual(
                                pollPurgeConnectorSuccess({ response: { dropEntity: updatedDropEntity } })
                            );
                            resolve();
                        });
                    }
                );
            }));

        it('should dispatch error when polling fails', () =>
            new Promise<void>((resolve) => {
                const dropEntity = createMockDropRequestEntity({ id: 'purge-req-1', finished: false });

                setup({ purgeConnectorId: 'connector-123', purgeDropEntity: dropEntity }).then(
                    ({ effects, actions$, mockConnectorService, mockErrorHelper }) => {
                        const errorResponse = new HttpErrorResponse({
                            error: 'Poll failed',
                            status: 500,
                            statusText: 'ISE'
                        });
                        (mockConnectorService.getPurgeRequest as Mock).mockReturnValue(throwError(() => errorResponse));
                        actions$(of(pollPurgeConnector()));

                        effects.pollPurgeConnector$.subscribe((action) => {
                            expect(action).toEqual(purgeConnectorApiError({ error: 'Error message' }));
                            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
                            resolve();
                        });
                    }
                );
            }));
    });

    describe('pollPurgeConnectorSuccess$', () => {
        it('should stop polling when finished', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$ }) => {
                    const dropEntity = createMockDropRequestEntity({ finished: true, percentCompleted: 100 });
                    actions$(of(pollPurgeConnectorSuccess({ response: { dropEntity } })));

                    effects.pollPurgeConnectorSuccess$.subscribe((action) => {
                        expect(action).toEqual(stopPollingPurgeConnector());
                        resolve();
                    });
                });
            }));
    });

    describe('stopPollingPurgeConnector$', () => {
        it('should dispatch deletePurgeRequest', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$ }) => {
                    actions$(of(stopPollingPurgeConnector()));

                    effects.stopPollingPurgeConnector$.subscribe((action) => {
                        expect(action).toEqual(deletePurgeRequest());
                        resolve();
                    });
                });
            }));
    });

    describe('showPurgeConnectorResults$', () => {
        it('should show purge results dialog', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockDialog, store }) => {
                    const dropEntity = createMockDropRequestEntity({ dropped: '10 / 1 KB', percentCompleted: 100 });
                    const dispatchSpy = vi.spyOn(store, 'dispatch');

                    const afterClosedSubject = new Subject<void>();
                    mockDialog.open.mockReturnValue({ afterClosed: () => afterClosedSubject.asObservable() });

                    actions$(of(showPurgeConnectorResults({ request: { dropEntity } })));

                    effects.showPurgeConnectorResults$.subscribe(() => {
                        expect(mockDialog.open).toHaveBeenCalled();
                        const dialogData = mockDialog.open.mock.calls[0][1].data;
                        expect(dialogData.title).toBe('Purge Connector');
                        expect(dialogData.message).toContain('10');
                        expect(dialogData.message).toContain('were removed from the connector');
                        expect(dispatchSpy).toHaveBeenCalledWith(loadConnectorsListing());
                        resolve();
                    });
                });
            }));
    });

    describe('purgeConnectorApiError$', () => {
        it('should close dialogs and dispatch banner error', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockDialog }) => {
                    actions$(of(purgeConnectorApiError({ error: 'Purge failed' })));

                    effects.purgeConnectorApiError$.subscribe((action) => {
                        expect(mockDialog.closeAll).toHaveBeenCalled();
                        expect(action).toEqual(
                            ErrorActions.addBannerError({
                                errorContext: { errors: ['Purge failed'], context: ErrorContextKey.CONNECTORS }
                            })
                        );
                        resolve();
                    });
                });
            }));
    });
});
