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
import { Observable, of, Subject, throwError } from 'rxjs';
import { Action } from '@ngrx/store';
import { ConnectorsListingEffects } from './connectors-listing.effects';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { Client } from '../../../../service/client.service';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { HttpErrorResponse } from '@angular/common/http';
import { CreateConnector } from '../../ui/create-connector/create-connector.component';
import { ConnectorAction, ConnectorActionName, ConnectorEntity, ConnectorStatus, YesNoDialog } from '@nifi/shared';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorContextKey } from '../../../../state/error';
import { selectLoadedTimestamp, selectSaving } from './connectors-listing.selectors';
import {
    cancelConnectorDrain,
    cancelConnectorDrainSuccess,
    connectorsListingBannerApiError,
    createConnector,
    createConnectorSuccess,
    deleteConnector,
    deleteConnectorSuccess,
    discardConnectorConfig,
    discardConnectorConfigSuccess,
    drainConnector,
    drainConnectorSuccess,
    loadConnectorsListing,
    loadConnectorsListingError,
    loadConnectorsListingSuccess,
    navigateToConfigureConnector,
    openNewConnectorDialog,
    promptConnectorDeletion,
    promptDiscardConnectorConfig,
    promptDrainConnector,
    renameConnector,
    renameConnectorApiError,
    renameConnectorSuccess,
    selectConnector,
    startConnector,
    startConnectorSuccess,
    stopConnector,
    stopConnectorSuccess
} from './connectors-listing.actions';
import type { Mock } from 'vitest';

describe('ConnectorsListingEffects', () => {
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
            type?: string;
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
                type: options.type || 'org.apache.nifi.connector.TestConnector',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-test-nar',
                    version: '1.0.0'
                },
                name: options.name || 'Test Connector',
                state: 'STOPPED',
                managedProcessGroupId: 'pg-root-default',
                availableActions: defaultActions
            },
            status: { runStatus: 'STOPPED' } as ConnectorStatus
        };
    }

    function createMockDialogRef(data: Record<string, Observable<unknown> | Subject<unknown>> = {}) {
        return { componentInstance: data, afterClosed: () => new Subject<void>() };
    }

    async function setup(
        options: {
            saving?: boolean;
            loadedTimestamp?: string;
        } = {}
    ) {
        let actions$: Observable<Action>;

        const mockConnectorService = {
            getConnectors: vi.fn(),
            createConnector: vi.fn(),
            deleteConnector: vi.fn(),
            updateConnector: vi.fn(),
            updateConnectorRunStatus: vi.fn(),
            discardConnectorWorkingConfiguration: vi.fn(),
            drainConnector: vi.fn(),
            cancelConnectorDrain: vi.fn()
        };

        const mockErrorHelper = {
            getErrorString: vi.fn().mockReturnValue('Error message'),
            handleLoadingError: vi.fn()
        };

        const mockClient = {
            getClientId: vi.fn().mockReturnValue('client-1')
        };

        const mockDialog = {
            open: vi.fn(),
            closeAll: vi.fn()
        };

        const mockRouter = {
            navigate: vi.fn()
        };

        await TestBed.configureTestingModule({
            providers: [
                ConnectorsListingEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    initialState: {},
                    selectors: [
                        { selector: selectSaving, value: options.saving ?? false },
                        { selector: selectLoadedTimestamp, value: options.loadedTimestamp ?? '' }
                    ]
                }),
                { provide: ConnectorService, useValue: mockConnectorService },
                { provide: ErrorHelper, useValue: mockErrorHelper },
                { provide: Client, useValue: mockClient },
                { provide: MatDialog, useValue: mockDialog },
                { provide: Router, useValue: mockRouter }
            ]
        }).compileComponents();

        const effects = TestBed.inject(ConnectorsListingEffects);
        const store = TestBed.inject(MockStore);

        return {
            effects,
            store,
            actions$: (action: Observable<Action>) => {
                actions$ = action;
            },
            mockConnectorService,
            mockErrorHelper,
            mockClient,
            mockDialog,
            mockRouter
        };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('loadConnectorsListing$', () => {
        it('should load connectors successfully', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    const mockResponse = { connectors: [mockConnector], currentTime: '2025-10-27 12:00:00 UTC' };

                    (mockConnectorService.getConnectors as Mock).mockReturnValue(of(mockResponse));
                    actions$(of(loadConnectorsListing()));

                    effects.loadConnectorsListing$.subscribe((action) => {
                        expect(action).toEqual(
                            loadConnectorsListingSuccess({
                                response: { connectors: [mockConnector], loadedTimestamp: mockResponse.currentTime }
                            })
                        );
                        resolve();
                    });
                });
            }));

        it('should handle empty connectors response', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockResponse = { currentTime: '2025-10-27 12:00:00 UTC' };
                    (mockConnectorService.getConnectors as Mock).mockReturnValue(of(mockResponse));
                    actions$(of(loadConnectorsListing()));

                    effects.loadConnectorsListing$.subscribe((action) => {
                        expect(action).toEqual(
                            loadConnectorsListingSuccess({
                                response: { connectors: [], loadedTimestamp: mockResponse.currentTime }
                            })
                        );
                        resolve();
                    });
                });
            }));

        it('should handle error on initial load', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const errorResponse = new HttpErrorResponse({ error: 'Error', status: 500, statusText: 'ISE' });
                    (mockConnectorService.getConnectors as Mock).mockReturnValue(throwError(() => errorResponse));
                    actions$(of(loadConnectorsListing()));

                    effects.loadConnectorsListing$.subscribe((action) => {
                        expect(action).toEqual(
                            loadConnectorsListingError({
                                errorResponse,
                                loadedTimestamp: '',
                                status: 'pending'
                            })
                        );
                        resolve();
                    });
                });
            }));

        it('should handle error on reload with existing data', () =>
            new Promise<void>((resolve) => {
                setup({ loadedTimestamp: '2024-01-01 12:00:00 EST' }).then(
                    ({ effects, actions$, mockConnectorService }) => {
                        const errorResponse = new HttpErrorResponse({
                            error: 'Error',
                            status: 500,
                            statusText: 'ISE'
                        });
                        (mockConnectorService.getConnectors as Mock).mockReturnValue(throwError(() => errorResponse));
                        actions$(of(loadConnectorsListing()));

                        effects.loadConnectorsListing$.subscribe((action) => {
                            expect(action).toEqual(
                                loadConnectorsListingError({
                                    errorResponse,
                                    loadedTimestamp: '2024-01-01 12:00:00 EST',
                                    status: 'success'
                                })
                            );
                            resolve();
                        });
                    }
                );
            }));
    });

    describe('openNewConnectorDialog$', () => {
        it('should open create connector dialog', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockDialog }) => {
                    const mockDialogRef = createMockDialogRef({
                        createConnector: of({
                            type: 'org.apache.nifi.connector.TestConnector',
                            bundle: { group: 'org.apache.nifi', artifact: 'nifi-test-nar', version: '1.0.0' }
                        })
                    });

                    mockDialog.open.mockReturnValue(mockDialogRef);
                    actions$(of(openNewConnectorDialog()));

                    effects.openNewConnectorDialog$.subscribe(() => {
                        expect(mockDialog.open).toHaveBeenCalledWith(CreateConnector, expect.objectContaining({}));
                        expect(mockDialog.open.mock.calls[0][1]).not.toHaveProperty('data');
                        resolve();
                    });
                });
            }));
    });

    describe('createConnector$', () => {
        it('should create connector successfully', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    const createRequest = {
                        revision: { version: 0, clientId: 'client-1' },
                        connectorType: 'org.apache.nifi.connector.TestConnector',
                        connectorBundle: { group: 'org.apache.nifi', artifact: 'nifi-test-nar', version: '1.0.0' }
                    };

                    (mockConnectorService.createConnector as Mock).mockReturnValue(of(mockConnector));
                    actions$(of(createConnector({ request: createRequest })));

                    effects.createConnector$.subscribe((action) => {
                        expect(action).toEqual(createConnectorSuccess({ response: { connector: mockConnector } }));
                        resolve();
                    });
                });
            }));

        it('should handle error and close dialog', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService, mockDialog }) => {
                    const createRequest = {
                        revision: { version: 0, clientId: 'client-1' },
                        connectorType: 'org.apache.nifi.connector.TestConnector',
                        connectorBundle: { group: 'org.apache.nifi', artifact: 'nifi-test-nar', version: '1.0.0' }
                    };

                    const errorResponse = new HttpErrorResponse({
                        error: 'Failed',
                        status: 400,
                        statusText: 'Bad Request'
                    });
                    (mockConnectorService.createConnector as Mock).mockReturnValue(throwError(() => errorResponse));
                    actions$(of(createConnector({ request: createRequest })));

                    effects.createConnector$.subscribe((action) => {
                        expect(action).toEqual(connectorsListingBannerApiError({ error: 'Error message' }));
                        expect(mockDialog.closeAll).toHaveBeenCalled();
                        resolve();
                    });
                });
            }));
    });

    describe('createConnectorSuccess$', () => {
        it('should close dialog and select newly created connector', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockDialog }) => {
                    const mockConnector = createMockConnector({ id: 'new-connector-123' });
                    actions$(of(createConnectorSuccess({ response: { connector: mockConnector } })));

                    effects.createConnectorSuccess$.subscribe((action) => {
                        expect(action).toEqual(selectConnector({ request: { id: 'new-connector-123' } }));
                        expect(mockDialog.closeAll).toHaveBeenCalled();
                        resolve();
                    });
                });
            }));
    });

    describe('promptConnectorDeletion$', () => {
        it('should open confirmation dialog', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockDialog, store }) => {
                    const mockConnector = createMockConnector();
                    const mockDialogRef = createMockDialogRef({ yes: of(true) });
                    mockDialog.open.mockReturnValue(mockDialogRef);
                    const dispatchSpy = vi.spyOn(store, 'dispatch');

                    actions$(of(promptConnectorDeletion({ request: { connector: mockConnector } })));

                    effects.promptConnectorDeletion$.subscribe(() => {
                        expect(mockDialog.open).toHaveBeenCalledWith(
                            YesNoDialog,
                            expect.objectContaining({
                                data: {
                                    title: 'Delete Connector',
                                    message: `Are you sure you want to delete connector '${mockConnector.component.name}'?`
                                }
                            })
                        );
                        expect(dispatchSpy).toHaveBeenCalledWith(
                            deleteConnector({ request: { connector: mockConnector } })
                        );
                        resolve();
                    });
                });
            }));
    });

    describe('deleteConnector$', () => {
        it('should delete connector successfully', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    (mockConnectorService.deleteConnector as Mock).mockReturnValue(of(mockConnector));
                    actions$(of(deleteConnector({ request: { connector: mockConnector } })));

                    effects.deleteConnector$.subscribe((action) => {
                        expect(action).toEqual(deleteConnectorSuccess({ response: { connector: mockConnector } }));
                        resolve();
                    });
                });
            }));

        it('should handle error when deleting connector', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    const errorResponse = new HttpErrorResponse({ error: 'Error', status: 500, statusText: 'ISE' });
                    (mockConnectorService.deleteConnector as Mock).mockReturnValue(throwError(() => errorResponse));
                    actions$(of(deleteConnector({ request: { connector: mockConnector } })));

                    effects.deleteConnector$.subscribe((action) => {
                        expect(action).toEqual(connectorsListingBannerApiError({ error: 'Error message' }));
                        resolve();
                    });
                });
            }));
    });

    describe('connectorsListingBannerApiError$', () => {
        it('should dispatch addBannerError', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$ }) => {
                    actions$(of(connectorsListingBannerApiError({ error: 'Test error' })));

                    effects.connectorsListingBannerApiError$.subscribe((action) => {
                        expect(action).toEqual(
                            ErrorActions.addBannerError({
                                errorContext: { errors: ['Test error'], context: ErrorContextKey.CONNECTORS }
                            })
                        );
                        resolve();
                    });
                });
            }));
    });

    describe('startConnector$', () => {
        it('should start connector successfully', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    const updatedConnector = {
                        ...mockConnector,
                        component: { ...mockConnector.component, state: 'RUNNING' }
                    };
                    (mockConnectorService.updateConnectorRunStatus as Mock).mockReturnValue(of(updatedConnector));
                    actions$(of(startConnector({ request: { connector: mockConnector } })));

                    effects.startConnector$.subscribe((action) => {
                        expect(action).toEqual(startConnectorSuccess({ response: { connector: updatedConnector } }));
                        expect(mockConnectorService.updateConnectorRunStatus).toHaveBeenCalledWith(
                            mockConnector,
                            'RUNNING'
                        );
                        resolve();
                    });
                });
            }));

        it('should handle error when starting connector', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    const errorResponse = new HttpErrorResponse({ error: 'Error', status: 500, statusText: 'ISE' });
                    (mockConnectorService.updateConnectorRunStatus as Mock).mockReturnValue(
                        throwError(() => errorResponse)
                    );
                    actions$(of(startConnector({ request: { connector: mockConnector } })));

                    effects.startConnector$.subscribe((action) => {
                        expect(action).toEqual(connectorsListingBannerApiError({ error: 'Error message' }));
                        resolve();
                    });
                });
            }));
    });

    describe('stopConnector$', () => {
        it('should stop connector successfully', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    const updatedConnector = { ...mockConnector };
                    (mockConnectorService.updateConnectorRunStatus as Mock).mockReturnValue(of(updatedConnector));
                    actions$(of(stopConnector({ request: { connector: mockConnector } })));

                    effects.stopConnector$.subscribe((action) => {
                        expect(action).toEqual(stopConnectorSuccess({ response: { connector: updatedConnector } }));
                        expect(mockConnectorService.updateConnectorRunStatus).toHaveBeenCalledWith(
                            mockConnector,
                            'STOPPED'
                        );
                        resolve();
                    });
                });
            }));

        it('should handle error when stopping connector', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    const errorResponse = new HttpErrorResponse({ error: 'Error', status: 500, statusText: 'ISE' });
                    (mockConnectorService.updateConnectorRunStatus as Mock).mockReturnValue(
                        throwError(() => errorResponse)
                    );
                    actions$(of(stopConnector({ request: { connector: mockConnector } })));

                    effects.stopConnector$.subscribe((action) => {
                        expect(action).toEqual(connectorsListingBannerApiError({ error: 'Error message' }));
                        resolve();
                    });
                });
            }));
    });

    describe('selectConnector$', () => {
        it('should navigate to connector route', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockRouter }) => {
                    actions$(of(selectConnector({ request: { id: 'connector-123' } })));

                    effects.selectConnector$.subscribe(() => {
                        expect(mockRouter.navigate).toHaveBeenCalledWith(['/connectors', 'connector-123']);
                        resolve();
                    });
                });
            }));
    });

    describe('renameConnector$', () => {
        it('should rename connector successfully', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    const updatedConnector = {
                        ...mockConnector,
                        component: { ...mockConnector.component, name: 'Renamed' }
                    };
                    (mockConnectorService.updateConnector as Mock).mockReturnValue(of(updatedConnector));
                    actions$(of(renameConnector({ request: { connector: mockConnector, newName: 'Renamed' } })));

                    effects.renameConnector$.subscribe((action) => {
                        expect(action).toEqual(renameConnectorSuccess({ response: { connector: updatedConnector } }));
                        resolve();
                    });
                });
            }));

        it('should handle error when renaming connector', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    const errorResponse = new HttpErrorResponse({ error: 'Error', status: 500, statusText: 'ISE' });
                    (mockConnectorService.updateConnector as Mock).mockReturnValue(throwError(() => errorResponse));
                    actions$(of(renameConnector({ request: { connector: mockConnector, newName: 'Renamed' } })));

                    effects.renameConnector$.subscribe((action) => {
                        expect(action).toEqual(renameConnectorApiError({ error: 'Error message' }));
                        resolve();
                    });
                });
            }));
    });

    describe('renameConnectorSuccess$', () => {
        it('should close dialog', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockDialog }) => {
                    const mockConnector = createMockConnector();
                    actions$(of(renameConnectorSuccess({ response: { connector: mockConnector } })));

                    effects.renameConnectorSuccess$.subscribe(() => {
                        expect(mockDialog.closeAll).toHaveBeenCalled();
                        resolve();
                    });
                });
            }));
    });

    describe('renameConnectorApiError$', () => {
        it('should dispatch addBannerError with rename dialog context', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$ }) => {
                    actions$(of(renameConnectorApiError({ error: 'Rename failed' })));

                    effects.renameConnectorApiError$.subscribe((action) => {
                        expect(action).toEqual(
                            ErrorActions.addBannerError({
                                errorContext: {
                                    errors: ['Rename failed'],
                                    context: ErrorContextKey.CONNECTOR_RENAME_DIALOG
                                }
                            })
                        );
                        resolve();
                    });
                });
            }));
    });

    describe('discardConnectorConfig$', () => {
        it('should discard connector config successfully', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    (mockConnectorService.discardConnectorWorkingConfiguration as Mock).mockReturnValue(
                        of(mockConnector)
                    );
                    actions$(of(discardConnectorConfig({ request: { connector: mockConnector } })));

                    effects.discardConnectorConfig$.subscribe((action) => {
                        expect(action).toEqual(
                            discardConnectorConfigSuccess({ response: { connector: mockConnector } })
                        );
                        resolve();
                    });
                });
            }));

        it('should handle error when discarding config', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    const errorResponse = new HttpErrorResponse({ error: 'Error', status: 500, statusText: 'ISE' });
                    (mockConnectorService.discardConnectorWorkingConfiguration as Mock).mockReturnValue(
                        throwError(() => errorResponse)
                    );
                    actions$(of(discardConnectorConfig({ request: { connector: mockConnector } })));

                    effects.discardConnectorConfig$.subscribe((action) => {
                        expect(action).toEqual(connectorsListingBannerApiError({ error: 'Error message' }));
                        resolve();
                    });
                });
            }));
    });

    describe('promptDiscardConnectorConfig$', () => {
        it('should open confirmation dialog', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockDialog, store }) => {
                    const mockConnector = createMockConnector();
                    const mockDialogRef = createMockDialogRef({ yes: of(true) });
                    mockDialog.open.mockReturnValue(mockDialogRef);
                    const dispatchSpy = vi.spyOn(store, 'dispatch');

                    actions$(of(promptDiscardConnectorConfig({ request: { connector: mockConnector } })));

                    effects.promptDiscardConnectorConfig$.subscribe(() => {
                        expect(mockDialog.open).toHaveBeenCalledWith(
                            YesNoDialog,
                            expect.objectContaining({
                                data: expect.objectContaining({ title: 'Discard Configuration Changes' })
                            })
                        );
                        expect(dispatchSpy).toHaveBeenCalledWith(
                            discardConnectorConfig({ request: { connector: mockConnector } })
                        );
                        resolve();
                    });
                });
            }));
    });

    describe('drainConnector$', () => {
        it('should drain connector successfully', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    (mockConnectorService.drainConnector as Mock).mockReturnValue(of(mockConnector));
                    actions$(of(drainConnector({ request: { connector: mockConnector } })));

                    effects.drainConnector$.subscribe((action) => {
                        expect(action).toEqual(drainConnectorSuccess({ response: { connector: mockConnector } }));
                        resolve();
                    });
                });
            }));

        it('should handle error when draining connector', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    const errorResponse = new HttpErrorResponse({ error: 'Error', status: 500, statusText: 'ISE' });
                    (mockConnectorService.drainConnector as Mock).mockReturnValue(throwError(() => errorResponse));
                    actions$(of(drainConnector({ request: { connector: mockConnector } })));

                    effects.drainConnector$.subscribe((action) => {
                        expect(action).toEqual(connectorsListingBannerApiError({ error: 'Error message' }));
                        resolve();
                    });
                });
            }));
    });

    describe('promptDrainConnector$', () => {
        it('should open confirmation dialog', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockDialog, store }) => {
                    const mockConnector = createMockConnector();
                    const mockDialogRef = createMockDialogRef({ yes: of(true) });
                    mockDialog.open.mockReturnValue(mockDialogRef);
                    const dispatchSpy = vi.spyOn(store, 'dispatch');

                    actions$(of(promptDrainConnector({ request: { connector: mockConnector } })));

                    effects.promptDrainConnector$.subscribe(() => {
                        expect(mockDialog.open).toHaveBeenCalledWith(
                            YesNoDialog,
                            expect.objectContaining({
                                data: expect.objectContaining({ title: 'Drain Connector' })
                            })
                        );
                        expect(dispatchSpy).toHaveBeenCalledWith(
                            drainConnector({ request: { connector: mockConnector } })
                        );
                        resolve();
                    });
                });
            }));
    });

    describe('cancelConnectorDrain$', () => {
        it('should cancel drain successfully', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    (mockConnectorService.cancelConnectorDrain as Mock).mockReturnValue(of(mockConnector));
                    actions$(of(cancelConnectorDrain({ request: { connector: mockConnector } })));

                    effects.cancelConnectorDrain$.subscribe((action) => {
                        expect(action).toEqual(cancelConnectorDrainSuccess({ response: { connector: mockConnector } }));
                        resolve();
                    });
                });
            }));

        it('should handle error when cancelling drain', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
                    const mockConnector = createMockConnector();
                    const errorResponse = new HttpErrorResponse({ error: 'Error', status: 500, statusText: 'ISE' });
                    (mockConnectorService.cancelConnectorDrain as Mock).mockReturnValue(
                        throwError(() => errorResponse)
                    );
                    actions$(of(cancelConnectorDrain({ request: { connector: mockConnector } })));

                    effects.cancelConnectorDrain$.subscribe((action) => {
                        expect(action).toEqual(connectorsListingBannerApiError({ error: 'Error message' }));
                        resolve();
                    });
                });
            }));
    });

    describe('navigateToConfigureConnector$', () => {
        it('should navigate to connector configure route', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockRouter }) => {
                    const mockConnector = createMockConnector();
                    actions$(
                        of(
                            navigateToConfigureConnector({
                                request: { id: mockConnector.id, connector: mockConnector }
                            })
                        )
                    );

                    effects.navigateToConfigureConnector$.subscribe(() => {
                        expect(mockRouter.navigate).toHaveBeenCalledWith([
                            '/connectors',
                            mockConnector.id,
                            'configure'
                        ]);
                        resolve();
                    });
                });
            }));
    });
});
