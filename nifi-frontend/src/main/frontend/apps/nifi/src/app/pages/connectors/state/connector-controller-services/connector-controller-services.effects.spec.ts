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
import { Router } from '@angular/router';
import { MatDialog } from '@angular/material/dialog';
import { firstValueFrom, Observable, Subject, of, throwError } from 'rxjs';
import { ConnectorControllerServicesEffects } from './connector-controller-services.effects';
import {
    clearConnectorControllerServiceBulletins,
    clearConnectorControllerServiceBulletinsFailure,
    clearConnectorControllerServiceBulletinsSuccess,
    createConnectorControllerService,
    createConnectorControllerServiceFailure,
    createConnectorControllerServiceSuccess,
    deleteConnectorControllerService,
    deleteConnectorControllerServiceFailure,
    deleteConnectorControllerServiceSuccess,
    loadConnectorControllerServices,
    loadConnectorControllerServicesFailure,
    loadConnectorControllerServicesSuccess,
    navigateToAdvancedConnectorControllerServiceUi,
    navigateToConnectorControllerService,
    openChangeConnectorControllerServiceVersionDialog,
    openConfigureConnectorControllerServiceDialog,
    openDisableConnectorControllerServiceDialog,
    openEnableConnectorControllerServiceDialog,
    openNewConnectorControllerServiceDialog,
    openViewControllerServiceDialog,
    promptConnectorControllerServiceDeletion,
    saveConnectorControllerService,
    saveConnectorControllerServiceFailure,
    saveConnectorControllerServiceSuccess,
    selectConnectorControllerService,
    startConnectorControllerServicesEntityPolling,
    stopConnectorControllerServicesEntityPolling
} from './connector-controller-services.actions';
import { selectConnectorParameterContext } from '../connector-canvas/connector-canvas.selectors';
import { navigateToControllerService } from '../connector-canvas/connector-canvas.actions';
import { selectConnectorIdFromRoute, selectProcessGroupIdFromRoute } from './connector-controller-services.selectors';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { ErrorContextKey } from '../../../../state/error';
import * as ErrorActions from '../../../../state/error/error.actions';
import { BreadcrumbEntity, ParameterContextEntity } from '../../../../state/shared';
import {
    ComponentType,
    ControllerServiceEntity,
    ControllerServiceReferencingComponentDTO,
    YesNoDialog
} from '@nifi/shared';
import { createParameterContextFixture } from '../../testing/parameter-context-fixture';
import { EditControllerService } from '../../../../ui/common/controller-service/edit-controller-service/edit-controller-service.component';
import { CreateControllerService } from '../../../../ui/common/controller-service/create-controller-service/create-controller-service.component';
import { EnableControllerService } from '../../../../ui/common/controller-service/enable-controller-service/enable-controller-service.component';
import { DisableControllerService } from '../../../../ui/common/controller-service/disable-controller-service/disable-controller-service.component';
import { HttpErrorResponse } from '@angular/common/http';
import { MockStore } from '@ngrx/store/testing';
import { ControllerServiceService } from '../../../flow-designer/service/controller-service.service';
import { selectConnectorCanvasEntity } from '../connector-canvas-entity/connector-canvas-entity.selectors';
import {
    loadConnectorEntity,
    loadConnectorEntitySuccess
} from '../connector-canvas-entity/connector-canvas-entity.actions';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { Client } from '../../../../service/client.service';
import {
    resetPropertyVerificationState,
    verifyProperties
} from '../../../../state/property-verification/property-verification.actions';
import { VerifyPropertiesRequestContext } from '../../../../state/property-verification';
import { DocumentVisibility } from '../../../../state/document-visibility';
import { selectDocumentVisibilityState } from '../../../../state/document-visibility/document-visibility.selectors';

function buildService(overrides: Partial<ControllerServiceEntity> = {}): ControllerServiceEntity {
    return {
        id: 'svc-1',
        permissions: { canRead: true, canWrite: true },
        component: { id: 'svc-1', name: 'My Service', state: 'DISABLED' },
        ...overrides
    } as unknown as ControllerServiceEntity;
}

function reference(referenceType: string, groupId: string, id: string): ControllerServiceReferencingComponentDTO {
    return { referenceType, groupId, id } as ControllerServiceReferencingComponentDTO;
}

describe('ConnectorControllerServicesEffects', () => {
    interface SetupOptions {
        parameterContext?: ParameterContextEntity | null;
        connectorId?: string | null;
        connectorState?: string;
        documentVisibility?: DocumentVisibility;
    }

    async function setup(options: SetupOptions = {}) {
        let actions$: Observable<Action>;

        const mockConnectorService = {
            getConnectorControllerServices: vi.fn(),
            getConnectorFlow: vi.fn(),
            getControllerService: vi.fn()
        };
        const mockControllerServiceService = {
            createControllerService: vi.fn(),
            updateControllerService: vi.fn(),
            deleteControllerService: vi.fn(),
            clearBulletins: vi.fn(),
            getControllerService: vi.fn()
        };
        const mockExtensionTypesService = {
            getControllerServiceVersionsForType: vi.fn()
        };

        const mockPropertyTableHelperService = {
            getComponentHistory: vi.fn().mockReturnValue(of({ componentHistory: {} })),
            createNewProperty: vi.fn(),
            createNewService: vi.fn()
        };

        const mockErrorHelper = {
            getErrorString: vi.fn().mockReturnValue('Error message')
        };

        const mockRouter = {
            navigate: vi.fn().mockResolvedValue(true)
        };

        const afterClosed$ = new Subject<unknown>();
        const mockDialogInstance: {
            createControllerService: Subject<{
                type: string;
                bundle: { group: string; artifact: string; version: string };
            }>;
            changeVersion: Subject<{ bundle: { group: string; artifact: string; version: string } }>;
            verify: Subject<VerifyPropertiesRequestContext>;
            editControllerService: Subject<{ payload: unknown }>;
            yes: Subject<void>;
            goToReferencingComponent?: (component: ControllerServiceReferencingComponentDTO) => void;
            createNewService?: unknown;
            goToParameter?: unknown;
            convertToParameter?: unknown;
            goToService?: (serviceId: string) => void;
            parameterContext?: ParameterContextEntity;
            supportsParameters: boolean;
        } = {
            createControllerService: new Subject(),
            changeVersion: new Subject(),
            verify: new Subject(),
            editControllerService: new Subject(),
            yes: new Subject(),
            supportsParameters: true
        };
        const mockDialogRef = {
            componentInstance: mockDialogInstance,
            afterClosed: () => afterClosed$.asObservable(),
            close: vi.fn()
        };
        const mockDialog = {
            open: vi.fn().mockReturnValue(mockDialogRef),
            closeAll: vi.fn()
        };

        await TestBed.configureTestingModule({
            providers: [
                ConnectorControllerServicesEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    selectors: [
                        {
                            selector: selectConnectorParameterContext,
                            value: options.parameterContext ?? null
                        },
                        {
                            selector: selectConnectorIdFromRoute,
                            value: options.connectorId !== undefined ? options.connectorId : 'conn-1'
                        },
                        {
                            selector: selectProcessGroupIdFromRoute,
                            value: 'pg-1'
                        },
                        {
                            selector: selectConnectorCanvasEntity,
                            value: {
                                id: 'conn-1',
                                component: { state: options.connectorState ?? 'TROUBLESHOOTING' }
                            }
                        },
                        {
                            selector: selectDocumentVisibilityState,
                            value: {
                                documentVisibility: options.documentVisibility ?? DocumentVisibility.Visible,
                                changedTimestamp: 0
                            }
                        }
                    ]
                }),
                { provide: ConnectorService, useValue: mockConnectorService },
                { provide: ControllerServiceService, useValue: mockControllerServiceService },
                { provide: ErrorHelper, useValue: mockErrorHelper },
                { provide: Router, useValue: mockRouter },
                { provide: MatDialog, useValue: mockDialog },
                { provide: ExtensionTypesService, useValue: mockExtensionTypesService },
                { provide: PropertyTableHelperService, useValue: mockPropertyTableHelperService },
                { provide: Client, useValue: { getClientId: () => 'client-1' } }
            ]
        }).compileComponents();

        return {
            effects: TestBed.inject(ConnectorControllerServicesEffects),
            store: TestBed.inject(MockStore),
            mockConnectorService,
            mockControllerServiceService,
            mockExtensionTypesService,
            mockPropertyTableHelperService,
            mockErrorHelper,
            mockRouter,
            mockDialog,
            mockDialogInstance,
            mockDialogRef,
            afterClosed$,
            actions$: (stream: Observable<Action>) => {
                actions$ = stream;
            }
        };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('loadConnectorControllerServices$', () => {
        it('should combine controller services and flow responses into a load success', async () => {
            const { effects, actions$, mockConnectorService } = await setup();
            const breadcrumb = { id: 'pg-1' } as unknown as BreadcrumbEntity;
            const service = buildService();
            mockConnectorService.getConnectorControllerServices.mockReturnValue(
                of({ controllerServices: [service], currentTime: '2023-04-01T00:00:00Z' })
            );
            mockConnectorService.getConnectorFlow.mockReturnValue(of({ processGroupFlow: { breadcrumb } }));
            actions$(
                of(
                    loadConnectorControllerServices({
                        request: { connectorId: 'conn-1', processGroupId: 'pg-1' }
                    })
                )
            );

            const result = await firstValueFrom(effects.loadConnectorControllerServices$);

            expect(result).toEqual(
                loadConnectorControllerServicesSuccess({
                    response: {
                        connectorId: 'conn-1',
                        processGroupId: 'pg-1',
                        breadcrumb,
                        controllerServices: [service],
                        loadedTimestamp: '2023-04-01T00:00:00Z'
                    }
                })
            );
        });

        it('should emit a load failure with the controller-services error context when the API call fails', async () => {
            const { effects, actions$, mockConnectorService } = await setup();
            mockConnectorService.getConnectorControllerServices.mockReturnValue(throwError(() => new Error('boom')));
            mockConnectorService.getConnectorFlow.mockReturnValue(of({ processGroupFlow: { breadcrumb: null } }));
            actions$(
                of(
                    loadConnectorControllerServices({
                        request: { connectorId: 'conn-1', processGroupId: 'pg-1' }
                    })
                )
            );

            const result = await firstValueFrom(effects.loadConnectorControllerServices$);

            expect(result).toEqual(
                loadConnectorControllerServicesFailure({
                    errorContext: {
                        errors: ['Error message'],
                        context: ErrorContextKey.CONTROLLER_SERVICES
                    }
                })
            );
        });
    });

    describe('loadConnectorControllerServicesFailure$', () => {
        it('should map a load failure into a banner error', async () => {
            const { effects, actions$ } = await setup();
            const errorContext = { errors: ['boom'], context: ErrorContextKey.CONTROLLER_SERVICES };
            actions$(of(loadConnectorControllerServicesFailure({ errorContext })));

            const result = await firstValueFrom(effects.loadConnectorControllerServicesFailure$);

            expect(result).toEqual(ErrorActions.addBannerError({ errorContext }));
        });
    });

    describe('controller service write operations', () => {
        const service = buildService();
        const reload = loadConnectorControllerServices({ request: { connectorId: 'conn-1', processGroupId: 'pg-1' } });

        it('should create a controller service and report failures', async () => {
            const { effects, actions$, mockControllerServiceService, mockDialog } = await setup();
            const request = {
                processGroupId: 'pg-1',
                controllerServiceType: 'org.apache.nifi.Service',
                controllerServiceBundle: { group: 'g', artifact: 'a', version: '1' },
                revision: { clientId: 'client-1', version: 0 }
            };
            mockControllerServiceService.createControllerService.mockReturnValueOnce(of(service));
            actions$(of(createConnectorControllerService({ request })));

            await expect(firstValueFrom(effects.createConnectorControllerService$)).resolves.toEqual(
                createConnectorControllerServiceSuccess({ response: { controllerService: service } })
            );

            mockControllerServiceService.createControllerService.mockReturnValueOnce(
                throwError(() => new HttpErrorResponse({ status: 500 }))
            );
            actions$(of(createConnectorControllerService({ request })));

            await expect(firstValueFrom(effects.createConnectorControllerService$)).resolves.toEqual(
                createConnectorControllerServiceFailure({ error: 'Error message' })
            );
            expect(mockDialog.closeAll).toHaveBeenCalledTimes(1);
        });

        it('should close dialogs, select the new service, and reload after a create', async () => {
            const { effects, actions$, store, mockDialog } = await setup();
            const dispatch = vi.spyOn(store, 'dispatch');
            actions$(of(createConnectorControllerServiceSuccess({ response: { controllerService: service } })));

            await firstValueFrom(effects.createConnectorControllerServiceSuccess$);

            expect(mockDialog.closeAll).toHaveBeenCalledTimes(1);
            expect(dispatch).toHaveBeenCalledWith(
                selectConnectorControllerService({
                    request: { connectorId: 'conn-1', processGroupId: 'pg-1', serviceId: service.id }
                })
            );
            expect(dispatch).toHaveBeenCalledWith(reload);
        });

        it('should save a controller service and report failures', async () => {
            const { effects, actions$, mockControllerServiceService } = await setup();
            const request = {
                id: 'svc-1',
                uri: '/controller-services/svc-1',
                payload: { revision: { version: 1 }, component: { id: 'svc-1' } }
            };
            mockControllerServiceService.updateControllerService.mockReturnValueOnce(of(service));
            actions$(of(saveConnectorControllerService({ request })));

            await expect(firstValueFrom(effects.saveConnectorControllerService$)).resolves.toEqual(
                saveConnectorControllerServiceSuccess({ response: service })
            );

            mockControllerServiceService.updateControllerService.mockReturnValueOnce(
                throwError(() => new HttpErrorResponse({ status: 409 }))
            );
            actions$(of(saveConnectorControllerService({ request })));

            await expect(firstValueFrom(effects.saveConnectorControllerService$)).resolves.toEqual(
                saveConnectorControllerServiceFailure({ error: 'Error message' })
            );
        });

        it('should close dialogs and reload after a save', async () => {
            const { effects, actions$, store, mockDialog } = await setup();
            const dispatch = vi.spyOn(store, 'dispatch');
            actions$(of(saveConnectorControllerServiceSuccess({ response: service })));

            await firstValueFrom(effects.saveConnectorControllerServiceSuccess$);

            expect(mockDialog.closeAll).toHaveBeenCalledTimes(1);
            expect(dispatch).toHaveBeenCalledWith(reload);
        });

        it('should delete a controller service, reload, and report failures', async () => {
            const { effects, actions$, store, mockControllerServiceService } = await setup();
            const request = { controllerService: service };
            mockControllerServiceService.deleteControllerService.mockReturnValueOnce(of(undefined));
            actions$(of(deleteConnectorControllerService({ request })));

            await expect(firstValueFrom(effects.deleteConnectorControllerService$)).resolves.toEqual(
                deleteConnectorControllerServiceSuccess()
            );

            const dispatch = vi.spyOn(store, 'dispatch');
            actions$(of(deleteConnectorControllerServiceSuccess()));
            await firstValueFrom(effects.deleteConnectorControllerServiceSuccess$);
            expect(dispatch).toHaveBeenCalledWith(reload);

            mockControllerServiceService.deleteControllerService.mockReturnValueOnce(
                throwError(() => new HttpErrorResponse({ status: 409 }))
            );
            actions$(of(deleteConnectorControllerService({ request })));

            await expect(firstValueFrom(effects.deleteConnectorControllerService$)).resolves.toEqual(
                deleteConnectorControllerServiceFailure({ error: 'Error message' })
            );
        });

        it('should clear bulletins and report failures', async () => {
            const { effects, actions$, mockControllerServiceService } = await setup();
            const request = {
                uri: '/controller-services/svc-1',
                componentId: 'svc-1',
                componentType: ComponentType.ControllerService,
                fromTimestamp: '2023-04-01T00:00:00Z'
            };
            mockControllerServiceService.clearBulletins.mockReturnValueOnce(of({}));
            actions$(of(clearConnectorControllerServiceBulletins({ request })));

            await expect(firstValueFrom(effects.clearConnectorControllerServiceBulletins$)).resolves.toEqual(
                clearConnectorControllerServiceBulletinsSuccess()
            );
            expect(mockControllerServiceService.clearBulletins).toHaveBeenCalledWith({
                id: 'svc-1',
                fromTimestamp: '2023-04-01T00:00:00Z'
            });

            mockControllerServiceService.clearBulletins.mockReturnValueOnce(
                throwError(() => new HttpErrorResponse({ status: 500 }))
            );
            actions$(of(clearConnectorControllerServiceBulletins({ request })));

            await expect(firstValueFrom(effects.clearConnectorControllerServiceBulletins$)).resolves.toEqual(
                clearConnectorControllerServiceBulletinsFailure({ error: 'Error message' })
            );
        });

        it.each([
            ['create', 'createConnectorControllerServiceFailure$', createConnectorControllerServiceFailure],
            ['save', 'saveConnectorControllerServiceFailure$', saveConnectorControllerServiceFailure],
            ['delete', 'deleteConnectorControllerServiceFailure$', deleteConnectorControllerServiceFailure],
            [
                'clear bulletins',
                'clearConnectorControllerServiceBulletinsFailure$',
                clearConnectorControllerServiceBulletinsFailure
            ]
        ] as const)('should map a %s failure into a banner error', async (_name, effectName, failure) => {
            const { effects, actions$ } = await setup();
            actions$(of(failure({ error: 'boom' })));

            const result = await firstValueFrom(effects[effectName]);

            expect(result).toEqual(
                ErrorActions.addBannerError({
                    errorContext: { errors: ['boom'], context: ErrorContextKey.CONTROLLER_SERVICES }
                })
            );
        });

        it.each<
            [
                string,
                (request: { id: string; controllerService: ControllerServiceEntity }) => Action,
                'openEnableConnectorControllerServiceDialog$' | 'openDisableConnectorControllerServiceDialog$',
                unknown
            ]
        >([
            [
                'enable',
                (request) => openEnableConnectorControllerServiceDialog({ request }),
                'openEnableConnectorControllerServiceDialog$',
                EnableControllerService
            ],
            [
                'disable',
                (request) => openDisableConnectorControllerServiceDialog({ request }),
                'openDisableConnectorControllerServiceDialog$',
                DisableControllerService
            ]
        ])(
            'should open the %s dialog, route references onto the connector canvas, and reload unless it closed for navigation',
            async (_name, open, effectName, dialogComponent) => {
                const { effects, actions$, store, mockDialog, mockDialogInstance, mockRouter, afterClosed$ } =
                    await setup();
                const dispatch = vi.spyOn(store, 'dispatch');
                const request = { id: service.id, controllerService: service };
                actions$(of(open(request)));

                await firstValueFrom(effects[effectName]);

                const [component, config] = mockDialog.open.mock.calls[0];
                expect(component).toBe(dialogComponent);
                expect(config.data).toBe(request);

                mockDialogInstance.goToReferencingComponent?.(reference('Processor', 'pg-2', 'proc-1'));
                expect(mockRouter.navigate).toHaveBeenCalledWith([
                    '/connectors',
                    'conn-1',
                    'canvas',
                    'pg-2',
                    ComponentType.Processor,
                    'proc-1'
                ]);

                afterClosed$.next('ROUTED');
                expect(dispatch).not.toHaveBeenCalledWith(reload);
                afterClosed$.next(undefined);
                expect(dispatch).toHaveBeenCalledWith(reload);
            }
        );

        it('should close the change version dialog when the save fails', async () => {
            const { effects, actions$, mockExtensionTypesService, mockDialogRef } = await setup();
            const actionStream = new Subject<Action>();
            actions$(actionStream);
            mockExtensionTypesService.getControllerServiceVersionsForType.mockReturnValue(
                of({ controllerServiceTypes: [] })
            );
            effects.openChangeConnectorControllerServiceVersionDialog$.subscribe();

            actionStream.next(
                openChangeConnectorControllerServiceVersionDialog({
                    request: {
                        id: service.id,
                        uri: '/controller-services/svc-1',
                        revision: { version: 1 },
                        type: 'org.apache.nifi.Service',
                        bundle: { group: 'g', artifact: 'a', version: '1' }
                    }
                })
            );
            actionStream.next(saveConnectorControllerServiceFailure({ error: 'boom' }));

            expect(mockDialogRef.close).toHaveBeenCalledTimes(1);
        });

        it('should create the selected type in the current process group from the create dialog', async () => {
            const { effects, actions$, store, mockDialog, mockDialogInstance } = await setup();
            const dispatch = vi.spyOn(store, 'dispatch');
            actions$(of(openNewConnectorControllerServiceDialog()));

            await firstValueFrom(effects.openNewConnectorControllerServiceDialog$);

            expect(mockDialog.open.mock.calls[0][0]).toBe(CreateControllerService);
            const bundle = { group: 'g', artifact: 'a', version: '1' };
            mockDialogInstance.createControllerService.next({ type: 'org.apache.nifi.Service', bundle });
            expect(dispatch).toHaveBeenCalledWith(
                createConnectorControllerService({
                    request: {
                        revision: { clientId: 'client-1', version: 0 },
                        processGroupId: 'pg-1',
                        controllerServiceType: 'org.apache.nifi.Service',
                        controllerServiceBundle: bundle
                    }
                })
            );
        });

        it('should delete only after the deletion prompt is confirmed', async () => {
            const { effects, actions$, store, mockDialog, mockDialogInstance } = await setup();
            const dispatch = vi.spyOn(store, 'dispatch');
            const request = { controllerService: service };
            actions$(of(promptConnectorControllerServiceDeletion({ request })));

            await firstValueFrom(effects.promptConnectorControllerServiceDeletion$);

            const [component, config] = mockDialog.open.mock.calls[0];
            expect(component).toBe(YesNoDialog);
            expect(config.data.message).toBe('Delete controller service My Service?');
            expect(dispatch).not.toHaveBeenCalled();
            mockDialogInstance.yes.next();
            expect(dispatch).toHaveBeenCalledWith(deleteConnectorControllerService({ request }));
        });
    });

    describe('startConnectorControllerServicesEntityPolling$', () => {
        afterEach(() => {
            vi.useRealTimers();
        });

        it('should load the connector entity on each tick until polling stops', async () => {
            vi.useFakeTimers();
            const { effects, actions$ } = await setup();
            const actionStream = new Subject<Action>();
            actions$(actionStream);
            const results: Action[] = [];
            effects.startConnectorControllerServicesEntityPolling$.subscribe((action) => results.push(action));

            actionStream.next(startConnectorControllerServicesEntityPolling({ connectorId: 'conn-1' }));
            vi.advanceTimersByTime(30_000);
            expect(results).toEqual([
                loadConnectorEntity({ connectorId: 'conn-1' }),
                loadConnectorEntity({ connectorId: 'conn-1' })
            ]);

            actionStream.next(stopConnectorControllerServicesEntityPolling());
            vi.advanceTimersByTime(30_000);
            expect(results).toHaveLength(2);
        });

        it('should skip ticks while the document is hidden', async () => {
            vi.useFakeTimers();
            const { effects, actions$ } = await setup({ documentVisibility: DocumentVisibility.Hidden });
            const actionStream = new Subject<Action>();
            actions$(actionStream);
            const results: Action[] = [];
            effects.startConnectorControllerServicesEntityPolling$.subscribe((action) => results.push(action));

            actionStream.next(startConnectorControllerServicesEntityPolling({ connectorId: 'conn-1' }));
            vi.advanceTimersByTime(60_000);

            expect(results).toEqual([]);
        });
    });

    describe('reloadOnTroubleshootingExit$', () => {
        const connector = (state: string, id = 'conn-1') => ({ id, component: { state } }) as never;

        async function setupExitStream() {
            const context = await setup();
            const actionStream = new Subject<Action>();
            context.actions$(actionStream);
            const results: Action[] = [];
            context.effects.reloadOnTroubleshootingExit$.subscribe((action) => results.push(action));
            const startPolling = () =>
                actionStream.next(startConnectorControllerServicesEntityPolling({ connectorId: 'conn-1' }));
            const stopPolling = () => actionStream.next(stopConnectorControllerServicesEntityPolling());

            // The selector holds the entity the store retained, which differs from the
            // response when the reducer rejects a stale revision.
            const emit = (connectorEntity: never, retained: never = connectorEntity) => {
                context.store.overrideSelector(selectConnectorCanvasEntity, retained);
                context.store.refreshState();
                actionStream.next(loadConnectorEntitySuccess({ connectorEntity }));
            };
            return { emit, results, startPolling, stopPolling };
        }

        it.each([
            ['the first load', [connector('STOPPED')]],
            ['entering troubleshooting', [connector('STOPPED'), connector('TROUBLESHOOTING')]],
            ['staying in troubleshooting', [connector('TROUBLESHOOTING'), connector('TROUBLESHOOTING')]],
            ['switching connectors', [connector('TROUBLESHOOTING', 'conn-0'), connector('STOPPED')]]
        ])('should not reload on %s', async (_name, entities) => {
            const { emit, results, startPolling } = await setupExitStream();
            startPolling();

            entities.forEach((entity) => emit(entity));

            expect(results).toEqual([]);
        });

        it('should not reload when the store keeps the current entity over a stale response', async () => {
            const { emit, results, startPolling } = await setupExitStream();
            const current = connector('TROUBLESHOOTING');
            startPolling();

            emit(current);
            emit(connector('STOPPED'), current);

            expect(results).toEqual([]);
        });

        it('should reload the listing only when the connector exits troubleshooting', async () => {
            const { emit, results, startPolling } = await setupExitStream();
            startPolling();

            emit(connector('TROUBLESHOOTING'));
            expect(results).toEqual([]);

            emit(connector('STOPPED'));

            expect(results).toEqual([
                loadConnectorControllerServices({ request: { connectorId: 'conn-1', processGroupId: 'pg-1' } })
            ]);
        });

        it('should not reload before polling starts', async () => {
            const { emit, results } = await setupExitStream();

            emit(connector('TROUBLESHOOTING'));
            emit(connector('STOPPED'));

            expect(results).toEqual([]);
        });

        it('should not reload when the connector exits troubleshooting after polling stops', async () => {
            const { emit, results, startPolling, stopPolling } = await setupExitStream();
            startPolling();

            emit(connector('TROUBLESHOOTING'));
            stopPolling();
            emit(connector('STOPPED'));

            expect(results).toEqual([]);
        });
    });

    describe('selectConnectorControllerService$', () => {
        it('should navigate to the controller service deep link with replaceUrl', async () => {
            const { effects, actions$, mockRouter } = await setup();
            actions$(
                of(
                    selectConnectorControllerService({
                        request: { connectorId: 'conn-1', processGroupId: 'pg-1', serviceId: 'svc-1' }
                    })
                )
            );

            await firstValueFrom(effects.selectConnectorControllerService$);

            expect(mockRouter.navigate).toHaveBeenCalledWith(
                ['/connectors', 'conn-1', 'canvas', 'pg-1', 'controller-services', 'svc-1'],
                { replaceUrl: true }
            );
        });
    });

    describe('openConfigureConnectorControllerServiceDialog$', () => {
        it('does not open the configure dialog when the Controller Service component is not readable', async () => {
            const { effects, actions$, mockDialog } = await setup();
            actions$(
                of(
                    openConfigureConnectorControllerServiceDialog({
                        request: {
                            id: 'svc-1',
                            controllerService: buildService({ component: undefined }),
                            readonly: false
                        }
                    })
                )
            );

            effects.openConfigureConnectorControllerServiceDialog$.subscribe();

            expect(mockDialog.open).not.toHaveBeenCalled();
        });

        const service = buildService({ uri: '/controller-services/svc-1' });
        const open = openConfigureConnectorControllerServiceDialog({
            request: { id: 'svc-1', controllerService: service, readonly: false }
        });

        it('should open the editable dialog and wire verification, save, and the verification reset on close', async () => {
            const { effects, actions$, store, mockDialog, mockDialogInstance, afterClosed$ } = await setup();
            const dispatch = vi.spyOn(store, 'dispatch');
            actions$(of(open));

            await firstValueFrom(effects.openConfigureConnectorControllerServiceDialog$);

            const [component, config] = mockDialog.open.mock.calls[0];
            expect(component).toBe(EditControllerService);
            expect(config.data).toEqual({ ...open.request, history: {} });

            const verification: VerifyPropertiesRequestContext = {
                entity: service,
                properties: { foo: 'bar' },
                componentType: ComponentType.ControllerService
            };
            mockDialogInstance.verify.next(verification);
            expect(dispatch).toHaveBeenCalledWith(verifyProperties({ request: verification }));

            const payload = { revision: { version: 1 }, component: { id: 'svc-1' } };
            mockDialogInstance.editControllerService.next({ payload });
            expect(dispatch).toHaveBeenCalledWith(
                saveConnectorControllerService({ request: { id: 'svc-1', uri: service.uri, payload } })
            );

            afterClosed$.next(undefined);
            expect(dispatch).toHaveBeenCalledWith(resetPropertyVerificationState());
        });

        it('should route references onto the connector canvas and ignore references with no connector route', async () => {
            const { effects, actions$, mockDialogInstance, mockRouter } = await setup();
            actions$(of(open));

            await firstValueFrom(effects.openConfigureConnectorControllerServiceDialog$);

            mockDialogInstance.goToReferencingComponent?.(reference('Processor', 'pg-2', 'proc-1'));
            mockDialogInstance.goToReferencingComponent?.(reference('ControllerService', 'pg-2', 'svc-2'));
            mockDialogInstance.goToReferencingComponent?.(reference('ReportingTask', 'pg-2', 'rt-1'));
            mockDialogInstance.goToReferencingComponent?.(reference('ControllerService', '', 'svc-root'));

            expect(mockRouter.navigate.mock.calls).toEqual([
                [['/connectors', 'conn-1', 'canvas', 'pg-2', ComponentType.Processor, 'proc-1']],
                [['/connectors', 'conn-1', 'canvas', 'pg-2', 'controller-services', 'svc-2']]
            ]);
        });

        it('should navigate goToService to the parent group of the referenced Controller Service entity', async () => {
            const { effects, actions$, mockControllerServiceService, mockDialogInstance, mockRouter } = await setup();
            mockControllerServiceService.getControllerService.mockReturnValue(
                of(buildService({ id: 'svc-2', parentGroupId: 'pg-x' }))
            );
            actions$(of(open));

            await firstValueFrom(effects.openConfigureConnectorControllerServiceDialog$);
            mockDialogInstance.goToService?.('svc-2');

            expect(mockControllerServiceService.getControllerService).toHaveBeenCalledWith('svc-2');
            expect(mockRouter.navigate).toHaveBeenCalledWith([
                '/connectors',
                'conn-1',
                'canvas',
                'pg-x',
                'controller-services',
                'svc-2'
            ]);
        });

        it('should show a banner instead of navigating when goToService cannot determine the parent group', async () => {
            const { effects, actions$, store, mockControllerServiceService, mockDialogInstance, mockRouter } =
                await setup();
            const dispatch = vi.spyOn(store, 'dispatch');
            mockControllerServiceService.getControllerService.mockReturnValue(of(buildService()));
            actions$(of(open));

            await firstValueFrom(effects.openConfigureConnectorControllerServiceDialog$);
            mockDialogInstance.goToService?.('svc-2');

            expect(dispatch).toHaveBeenCalledWith(
                ErrorActions.addBannerError({
                    errorContext: {
                        errors: ['Unable to determine the Controller Service parent group.'],
                        context: ErrorContextKey.CONTROLLER_SERVICES
                    }
                })
            );
            expect(mockRouter.navigate).not.toHaveBeenCalled();
        });

        it('should surface a component history failure and still open later requests', async () => {
            const { effects, actions$, store, mockDialog, mockPropertyTableHelperService } = await setup();
            const dispatch = vi.spyOn(store, 'dispatch');
            const actionStream = new Subject<Action>();
            actions$(actionStream);
            mockPropertyTableHelperService.getComponentHistory.mockReturnValueOnce(
                throwError(() => new HttpErrorResponse({ status: 500 }))
            );
            effects.openConfigureConnectorControllerServiceDialog$.subscribe();

            actionStream.next(open);
            expect(dispatch).toHaveBeenCalledWith(
                ErrorActions.addBannerError({
                    errorContext: { errors: ['Error message'], context: ErrorContextKey.CONTROLLER_SERVICES }
                })
            );
            expect(mockDialog.open).not.toHaveBeenCalled();

            actionStream.next(open);
            expect(mockDialog.open).toHaveBeenCalledTimes(1);
        });
    });

    describe('navigateToConnectorControllerService$', () => {
        it('should navigate to the defining group listing with back navigation to the current listing', async () => {
            const { effects, actions$, mockRouter } = await setup();
            actions$(of(navigateToConnectorControllerService({ processGroupId: 'pg-parent', serviceId: 'svc-1' })));

            await firstValueFrom(effects.navigateToConnectorControllerService$);

            const routeBoundary = ['/connectors', 'conn-1', 'canvas', 'pg-parent', 'controller-services'];
            expect(mockRouter.navigate).toHaveBeenCalledWith([...routeBoundary, 'svc-1'], {
                state: {
                    backNavigation: {
                        route: ['/connectors', 'conn-1', 'canvas', 'pg-1', 'controller-services', 'svc-1'],
                        routeBoundary,
                        context: 'Controller Service'
                    }
                }
            });
        });
    });

    describe('navigateToAdvancedConnectorControllerServiceUi$', () => {
        it('should navigate to the Advanced UI with back navigation to the selected service', async () => {
            const { effects, actions$, mockRouter } = await setup();
            actions$(of(navigateToAdvancedConnectorControllerServiceUi({ id: 'svc-1' })));

            await firstValueFrom(effects.navigateToAdvancedConnectorControllerServiceUi$);

            const componentRoute = ['/connectors', 'conn-1', 'canvas', 'pg-1', 'controller-services', 'svc-1'];
            const routeBoundary = [...componentRoute, 'advanced'];
            expect(mockRouter.navigate).toHaveBeenCalledWith(routeBoundary, {
                state: { backNavigation: { route: componentRoute, routeBoundary, context: 'Controller Service' } }
            });
        });
    });

    describe('openViewControllerServiceDialog$', () => {
        it('should open the EditControllerService dialog with the readonly flag set and the entity passed through unchanged', async () => {
            const { effects, actions$, mockDialog } = await setup();
            const service = buildService();
            actions$(of(openViewControllerServiceDialog({ controllerService: service })));

            await firstValueFrom(effects.openViewControllerServiceDialog$);

            expect(mockDialog.open).toHaveBeenCalledTimes(1);
            const [component, config] = mockDialog.open.mock.calls[0];
            expect(component).toBe(EditControllerService);
            expect(config.id).toBe('svc-1');
            expect(config.data.id).toBe('svc-1');
            expect(config.data.readonly).toBe(true);
            expect(config.data.controllerService).toBe(service);
            expect(config.data.controllerService.permissions.canWrite).toBe(true);
        });

        it('should wire the bound parameter context onto the EditControllerService dialog instance', async () => {
            const parameterContext = createParameterContextFixture({
                component: { id: 'pc-1', name: 'Bound PC', parameters: [] }
            });
            const { effects, actions$, mockDialogInstance } = await setup({ parameterContext });

            actions$(of(openViewControllerServiceDialog({ controllerService: buildService() })));

            await firstValueFrom(effects.openViewControllerServiceDialog$);

            expect(mockDialogInstance.parameterContext).toBe(parameterContext);
            expect(mockDialogInstance.supportsParameters).toBe(true);
            // Read-only mode should not wire a navigation callback. The property table hides
            // "Go to Parameter" when this is undefined, while still rendering parameter values.
            expect(mockDialogInstance.goToParameter).toBeUndefined();
        });

        it('should set supportsParameters to false when no parameter context is bound', async () => {
            const { effects, actions$, mockDialogInstance } = await setup({ parameterContext: null });

            actions$(of(openViewControllerServiceDialog({ controllerService: buildService() })));

            await firstValueFrom(effects.openViewControllerServiceDialog$);

            expect(mockDialogInstance.parameterContext).toBeUndefined();
            expect(mockDialogInstance.supportsParameters).toBe(false);
            expect(mockDialogInstance.goToParameter).toBeUndefined();
        });

        it('should wire goToService to fetch via the connector-scoped API and navigate to the controller service', async () => {
            const { effects, actions$, store, mockConnectorService, mockDialogInstance, mockDialogRef } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');
            mockConnectorService.getControllerService.mockReturnValue(
                of({ id: 'svc-2', parentGroupId: 'pg-cs', component: { parentGroupId: 'pg-cs', id: 'svc-2' } })
            );

            actions$(of(openViewControllerServiceDialog({ controllerService: buildService() })));
            await firstValueFrom(effects.openViewControllerServiceDialog$);

            expect(mockDialogInstance.goToService).toBeInstanceOf(Function);
            mockDialogInstance.goToService?.('svc-2');

            expect(mockConnectorService.getControllerService).toHaveBeenCalledWith('conn-1', 'svc-2');
            expect(mockDialogRef.close).toHaveBeenCalled();
            expect(dispatchSpy).toHaveBeenCalledWith(
                navigateToControllerService({
                    processGroupId: 'pg-cs',
                    serviceId: 'svc-2'
                })
            );
        });

        it('should dispatch a banner error when goToService fails to fetch the controller service', async () => {
            const { effects, actions$, store, mockConnectorService, mockDialogInstance, mockErrorHelper } =
                await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');
            const errorResponse = new HttpErrorResponse({ status: 404, statusText: 'Not Found' });
            mockConnectorService.getControllerService.mockReturnValue(throwError(() => errorResponse));

            actions$(of(openViewControllerServiceDialog({ controllerService: buildService() })));
            await firstValueFrom(effects.openViewControllerServiceDialog$);

            mockDialogInstance.goToService?.('svc-missing');

            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(dispatchSpy).toHaveBeenCalledWith(
                ErrorActions.addBannerError({
                    errorContext: {
                        errors: ['Error message'],
                        context: ErrorContextKey.CONTROLLER_SERVICES
                    }
                })
            );
        });

        it('should dispatch a banner error when goToService cannot resolve the connector id', async () => {
            const { effects, actions$, store, mockConnectorService, mockDialogInstance } = await setup({
                connectorId: null
            });
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            actions$(of(openViewControllerServiceDialog({ controllerService: buildService() })));
            await firstValueFrom(effects.openViewControllerServiceDialog$);

            mockDialogInstance.goToService?.('svc-1');

            expect(mockConnectorService.getControllerService).not.toHaveBeenCalled();
            expect(dispatchSpy).toHaveBeenCalledWith(
                ErrorActions.addBannerError({
                    errorContext: {
                        errors: ['Unable to determine Connector id for navigation.'],
                        context: ErrorContextKey.CONTROLLER_SERVICES
                    }
                })
            );
        });
    });
});
