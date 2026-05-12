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
import { Router } from '@angular/router';
import { firstValueFrom, Observable, of, Subject, throwError } from 'rxjs';
import { toArray } from 'rxjs/operators';
import { HttpErrorResponse } from '@angular/common/http';
import { ComponentType, ComponentTypeNamePipe } from '@nifi/shared';
import { MatDialog } from '@angular/material/dialog';
import { ParameterContextEntity } from '../../../../state/shared';
import { createParameterContextFixture } from '../../testing/parameter-context-fixture';
import { ConnectorCanvasEffects } from './connector-canvas.effects';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { ErrorContextKey } from '../../../../state/error';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import {
    deselectAllComponents,
    enterProcessGroup,
    leaveProcessGroup,
    loadConnectorFlow,
    loadConnectorFlowComplete,
    loadConnectorFlowFailure,
    loadConnectorFlowSuccess,
    loadConnectorParameterContext,
    loadConnectorParameterContextFailure,
    loadConnectorParameterContextSuccess,
    navigateToControllerService,
    navigateToControllerServices,
    navigateToProvenanceForComponent,
    navigateToQueueListing,
    navigateWithoutTransform,
    reloadConnectorFlow,
    selectComponents,
    startConnectorCanvasPolling,
    stopConnectorCanvasPolling,
    viewComponentConfiguration
} from './connector-canvas.actions';
import { loadConnectorControllerServicesSuccess } from '../connector-controller-services/connector-controller-services.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import { queueEmptied } from '../../../../state/empty-queue/empty-queue.actions';
import {
    selectConnectorId,
    selectConnectorIdFromRoute,
    selectConnectorParameterContext,
    selectLoadingStatus,
    selectParentProcessGroupId,
    selectProcessGroupId,
    selectProcessGroupIdFromRoute
} from './connector-canvas.selectors';
import { selectDocumentVisibilityState } from '../../../../state/document-visibility/document-visibility.selectors';
import { DocumentVisibility } from '../../../../state/document-visibility';
import type { Mock } from 'vitest';

describe('ConnectorCanvasEffects', () => {
    // Setup function following SIFERS pattern
    async function setup(
        options: {
            connectorId?: string | null;
            parentProcessGroupId?: string | null;
            processGroupId?: string | null;
            processGroupIdFromRoute?: string | null;
            documentVisibility?: DocumentVisibility;
            loadingStatus?: 'pending' | 'loading' | 'success' | 'error';
            parameterContext?: ParameterContextEntity | null;
            parameterContextResponse?: ParameterContextEntity | null;
            parameterContextError?: unknown;
        } = {}
    ) {
        let actions$: Observable<Action>;

        const connectorId = options.connectorId !== undefined ? options.connectorId : 'connector-1';
        const parentProcessGroupId =
            options.parentProcessGroupId !== undefined ? options.parentProcessGroupId : 'parent-pg';
        const processGroupId = options.processGroupId !== undefined ? options.processGroupId : 'child-pg';
        const processGroupIdFromRoute =
            options.processGroupIdFromRoute !== undefined ? options.processGroupIdFromRoute : processGroupId;
        const documentVisibility = options.documentVisibility ?? DocumentVisibility.Visible;
        const loadingStatus = options.loadingStatus ?? 'success';

        // Mock services
        const mockConnectorService = {
            getConnectorFlow: vi.fn(),
            getConnectorParameterContext: vi.fn()
        };

        if (options.parameterContextError !== undefined) {
            mockConnectorService.getConnectorParameterContext.mockReturnValue(
                throwError(() => options.parameterContextError)
            );
        } else {
            mockConnectorService.getConnectorParameterContext.mockReturnValue(
                of(options.parameterContextResponse ?? null)
            );
        }

        const mockErrorHelper = {
            getErrorString: vi.fn().mockReturnValue('Error message'),
            handleLoadingError: vi.fn()
        };

        const mockRouter = {
            navigate: vi.fn().mockResolvedValue(true)
        };

        const afterClosed$ = new Subject<unknown>();
        const mockDialogRef = {
            componentInstance: {} as {
                parameterContext?: ParameterContextEntity | null;
                supportsParameters?: boolean;
                goToParameter?: (parameter: string) => void;
                [key: string]: any;
            },
            afterClosed: () => afterClosed$.asObservable()
        };
        const mockDialog = {
            open: vi.fn().mockReturnValue(mockDialogRef)
        };

        await TestBed.configureTestingModule({
            providers: [
                ConnectorCanvasEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    initialState: {},
                    selectors: [
                        { selector: selectConnectorIdFromRoute, value: connectorId },
                        { selector: selectConnectorId, value: connectorId },
                        { selector: selectParentProcessGroupId, value: parentProcessGroupId },
                        { selector: selectProcessGroupId, value: processGroupId },
                        { selector: selectProcessGroupIdFromRoute, value: processGroupIdFromRoute },
                        { selector: selectLoadingStatus, value: loadingStatus },
                        {
                            selector: selectDocumentVisibilityState,
                            value: { documentVisibility, changedTimestamp: 0 }
                        },
                        {
                            selector: selectConnectorParameterContext,
                            value: options.parameterContext ?? null
                        }
                    ]
                }),
                { provide: ConnectorService, useValue: mockConnectorService },
                { provide: ErrorHelper, useValue: mockErrorHelper },
                { provide: Router, useValue: mockRouter },
                { provide: MatDialog, useValue: mockDialog },
                ComponentTypeNamePipe
            ]
        }).compileComponents();

        const effects = TestBed.inject(ConnectorCanvasEffects);
        const store = TestBed.inject(MockStore);

        return {
            effects,
            store,
            actions$: (stream: Observable<Action>) => {
                actions$ = stream;
            },
            mockConnectorService,
            mockErrorHelper,
            mockRouter,
            mockDialog,
            mockDialogRef,
            afterClosed$
        };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('loadConnectorFlow$', () => {
        it('should call getConnectorFlow and dispatch loadConnectorFlowSuccess', async () => {
            const { effects, actions$, mockConnectorService } = await setup();
            const flowResponse = {
                processGroupFlow: {
                    id: 'pg-123',
                    parentGroupId: 'parent-456',
                    breadcrumb: null,
                    flow: {
                        labels: [{ id: 'l1' }],
                        funnels: [],
                        inputPorts: [],
                        outputPorts: [],
                        remoteProcessGroups: [],
                        processGroups: [],
                        processors: [],
                        connections: []
                    }
                }
            };
            (mockConnectorService.getConnectorFlow as Mock).mockReturnValue(of(flowResponse));
            actions$(
                of(
                    loadConnectorFlow({
                        connectorId: 'conn-a',
                        processGroupId: 'pg-in'
                    })
                )
            );

            const action = await firstValueFrom(effects.loadConnectorFlow$);
            expect(mockConnectorService.getConnectorFlow).toHaveBeenCalledWith('conn-a', 'pg-in');
            expect(action).toEqual(
                loadConnectorFlowSuccess({
                    connectorId: 'conn-a',
                    processGroupId: 'pg-123',
                    parentProcessGroupId: 'parent-456',
                    breadcrumb: null,
                    labels: [{ id: 'l1' }],
                    funnels: [],
                    inputPorts: [],
                    outputPorts: [],
                    remoteProcessGroups: [],
                    processGroups: [],
                    processors: [],
                    connections: []
                })
            );
        });

        it('should default flow collections when processGroupFlow.flow is missing', async () => {
            const { effects, actions$, mockConnectorService } = await setup();
            const flowResponse = {
                processGroupFlow: {
                    id: 'pg-123',
                    parentGroupId: null,
                    breadcrumb: null
                }
            };
            (mockConnectorService.getConnectorFlow as Mock).mockReturnValue(of(flowResponse));
            actions$(
                of(
                    loadConnectorFlow({
                        connectorId: 'conn-a',
                        processGroupId: 'pg-in'
                    })
                )
            );

            const action = await firstValueFrom(effects.loadConnectorFlow$);
            expect(action).toEqual(
                loadConnectorFlowSuccess({
                    connectorId: 'conn-a',
                    processGroupId: 'pg-123',
                    parentProcessGroupId: null,
                    breadcrumb: null,
                    labels: [],
                    funnels: [],
                    inputPorts: [],
                    outputPorts: [],
                    remoteProcessGroups: [],
                    processGroups: [],
                    processors: [],
                    connections: []
                })
            );
        });

        it('should dispatch loadConnectorFlowFailure when getConnectorFlow errors', async () => {
            const { effects, actions$, mockConnectorService, mockErrorHelper } = await setup();
            const errorResponse = new HttpErrorResponse({ error: 'Failed', status: 500, statusText: 'ISE' });
            (mockConnectorService.getConnectorFlow as Mock).mockReturnValue(throwError(() => errorResponse));
            actions$(
                of(
                    loadConnectorFlow({
                        connectorId: 'conn-a',
                        processGroupId: 'pg-in'
                    })
                )
            );

            const action = await firstValueFrom(effects.loadConnectorFlow$);
            expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(action).toEqual(
                loadConnectorFlowFailure({
                    errorContext: {
                        errors: ['Error message'],
                        context: ErrorContextKey.CONNECTORS
                    }
                })
            );
        });
    });

    describe('loadConnectorFlowComplete$', () => {
        it('should dispatch loadConnectorFlowComplete on loadConnectorFlowSuccess', async () => {
            const { effects, actions$ } = await setup();
            actions$(
                of(
                    loadConnectorFlowSuccess({
                        connectorId: 'c1',
                        processGroupId: 'pg1',
                        parentProcessGroupId: null,
                        breadcrumb: null,
                        labels: [],
                        funnels: [],
                        inputPorts: [],
                        outputPorts: [],
                        remoteProcessGroups: [],
                        processGroups: [],
                        processors: [],
                        connections: []
                    })
                )
            );

            const action = await firstValueFrom(effects.loadConnectorFlowComplete$);
            expect(action).toEqual(loadConnectorFlowComplete());
        });
    });

    describe('enterProcessGroup$', () => {
        it('should navigate to the child process group canvas URL', async () => {
            const { effects, actions$, mockRouter } = await setup({ connectorId: 'my-connector' });
            actions$(
                of(
                    enterProcessGroup({
                        request: { id: 'child-pg-99' }
                    })
                )
            );

            await firstValueFrom(effects.enterProcessGroup$);
            expect(mockRouter.navigate).toHaveBeenCalledWith(['/connectors', 'my-connector', 'canvas', 'child-pg-99']);
        });
    });

    describe('leaveProcessGroup$', () => {
        it('should navigate to parent, then after loadConnectorFlowComplete dispatch navigateWithoutTransform', async () => {
            const { effects, actions$, mockRouter } = await setup({
                connectorId: 'conn-x',
                parentProcessGroupId: 'parent-pg',
                processGroupId: 'child-pg'
            });
            const actionSubject = new Subject<Action>();
            actions$(actionSubject.asObservable());

            const resultPromise = firstValueFrom(effects.leaveProcessGroup$);

            actionSubject.next(leaveProcessGroup());
            actionSubject.next(loadConnectorFlowComplete());
            actionSubject.complete();

            const action = await resultPromise;
            expect(mockRouter.navigate).toHaveBeenCalledWith(['/connectors', 'conn-x', 'canvas', 'parent-pg']);
            expect(action).toEqual(
                navigateWithoutTransform({
                    url: ['/connectors', 'conn-x', 'canvas', 'parent-pg', ComponentType.ProcessGroup, 'child-pg']
                })
            );
        });

        it('should not navigate when parent process group id is null', async () => {
            const { effects, actions$, mockRouter } = await setup({
                connectorId: 'conn-x',
                parentProcessGroupId: null,
                processGroupId: 'child-pg'
            });
            actions$(of(leaveProcessGroup()));

            await expect(firstValueFrom(effects.leaveProcessGroup$)).rejects.toThrow();
            expect(mockRouter.navigate).not.toHaveBeenCalled();
        });

        it('should not navigate when current process group id is null', async () => {
            const { effects, actions$, mockRouter } = await setup({
                connectorId: 'conn-x',
                parentProcessGroupId: 'parent-pg',
                processGroupId: null
            });
            actions$(of(leaveProcessGroup()));

            await expect(firstValueFrom(effects.leaveProcessGroup$)).rejects.toThrow();
            expect(mockRouter.navigate).not.toHaveBeenCalled();
        });
    });

    describe('selectComponents$', () => {
        it('should dispatch deselectAllComponents when components array is empty', async () => {
            const { effects, actions$ } = await setup({
                connectorId: 'conn-1',
                processGroupIdFromRoute: 'pg-root'
            });
            actions$(
                of(
                    selectComponents({
                        request: {
                            components: []
                        }
                    })
                )
            );

            const action = await firstValueFrom(effects.selectComponents$);
            expect(action).toEqual(deselectAllComponents());
        });

        it('should dispatch navigateWithoutTransform with single component URL', async () => {
            const { effects, actions$ } = await setup({
                connectorId: 'conn-1',
                processGroupIdFromRoute: 'pg-root'
            });
            actions$(
                of(
                    selectComponents({
                        request: {
                            components: [{ id: 'proc-1', componentType: ComponentType.Processor }]
                        }
                    })
                )
            );

            const action = await firstValueFrom(effects.selectComponents$);
            expect(action).toEqual(
                navigateWithoutTransform({
                    url: ['/connectors', 'conn-1', 'canvas', 'pg-root', ComponentType.Processor, 'proc-1']
                })
            );
        });

        it('should dispatch navigateWithoutTransform with bulk URL for multiple components', async () => {
            const { effects, actions$ } = await setup({
                connectorId: 'conn-1',
                processGroupIdFromRoute: 'pg-root'
            });
            actions$(
                of(
                    selectComponents({
                        request: {
                            components: [
                                { id: 'proc-1', componentType: ComponentType.Processor },
                                { id: 'conn-2', componentType: ComponentType.Connection }
                            ]
                        }
                    })
                )
            );

            const action = await firstValueFrom(effects.selectComponents$);
            expect(action).toEqual(
                navigateWithoutTransform({
                    url: ['/connectors', 'conn-1', 'canvas', 'pg-root', 'bulk', 'proc-1,conn-2']
                })
            );
        });
    });

    describe('deselectAllComponents$', () => {
        it('should dispatch navigateWithoutTransform with base canvas URL', async () => {
            const { effects, actions$ } = await setup({
                connectorId: 'conn-1',
                processGroupIdFromRoute: 'pg-root'
            });
            actions$(of(deselectAllComponents()));

            const action = await firstValueFrom(effects.deselectAllComponents$);
            expect(action).toEqual(
                navigateWithoutTransform({
                    url: ['/connectors', 'conn-1', 'canvas', 'pg-root']
                })
            );
        });
    });

    describe('navigateWithoutTransform$', () => {
        it('should call router.navigate with replaceUrl', async () => {
            const { effects, actions$, mockRouter } = await setup();
            const url = ['/connectors', 'conn-1', 'canvas', 'pg-root', 'Processor', 'proc-1'];
            actions$(of(navigateWithoutTransform({ url })));

            await firstValueFrom(effects.navigateWithoutTransform$);
            expect(mockRouter.navigate).toHaveBeenCalledWith(url, { replaceUrl: true });
        });
    });

    describe('viewComponentConfiguration$', () => {
        const baseEntity = {
            id: 'comp-1',
            uri: 'https://localhost/nifi-api/processors/comp-1',
            permissions: { canRead: true, canWrite: true },
            operatePermissions: { canRead: true, canWrite: true },
            component: { name: 'My Component' }
        };

        async function dispatchView(componentType: ComponentType, entity: any = baseEntity) {
            const { effects, actions$, mockDialog } = await setup();
            actions$(of(viewComponentConfiguration({ request: { entity, componentType } })));
            await firstValueFrom(effects.viewComponentConfiguration$);
            return { mockDialog };
        }

        it('opens the processor dialog with read-only permissions and the entity uri', async () => {
            const { mockDialog } = await dispatchView(ComponentType.Processor);
            expect(mockDialog.open).toHaveBeenCalledTimes(1);
            const [, config] = (mockDialog.open as Mock).mock.calls[0];
            expect(config.data.type).toBe(ComponentType.Processor);
            expect(config.data.uri).toBe(baseEntity.uri);
            expect(config.data.entity.permissions.canWrite).toBe(false);
            expect(config.data.entity.permissions.canRead).toBe(true);
            expect(config.data.entity.operatePermissions.canWrite).toBe(false);
            expect(config.data.entity.operatePermissions.canRead).toBe(true);
            expect(config.id).toBe(baseEntity.id);
        });

        it('opens the connection dialog with the breadcrumbs observable wired', async () => {
            const { effects, actions$, mockDialog } = await setup();
            const componentInstance: any = {};
            (mockDialog.open as Mock).mockReturnValue({ componentInstance });
            actions$(
                of(
                    viewComponentConfiguration({
                        request: { entity: baseEntity, componentType: ComponentType.Connection }
                    })
                )
            );
            await firstValueFrom(effects.viewComponentConfiguration$);

            const [, config] = (mockDialog.open as Mock).mock.calls[0];
            expect(config.data.type).toBe(ComponentType.Connection);
            expect(componentInstance.breadcrumbs$).toBeDefined();
            expect(componentInstance.availablePrioritizers$).toBeDefined();
            expect(componentInstance.getChildInputPorts).toBeInstanceOf(Function);
            expect(componentInstance.getChildOutputPorts).toBeInstanceOf(Function);
        });

        it('opens the input port dialog with read-only permissions', async () => {
            const { mockDialog } = await dispatchView(ComponentType.InputPort);
            const [, config] = (mockDialog.open as Mock).mock.calls[0];
            expect(config.data.type).toBe(ComponentType.InputPort);
            expect(config.data.entity.permissions.canWrite).toBe(false);
        });

        it('opens the output port dialog with read-only permissions', async () => {
            const { mockDialog } = await dispatchView(ComponentType.OutputPort);
            const [, config] = (mockDialog.open as Mock).mock.calls[0];
            expect(config.data.type).toBe(ComponentType.OutputPort);
            expect(config.data.entity.permissions.canWrite).toBe(false);
        });

        it('opens the label dialog with read-only permissions', async () => {
            const { mockDialog } = await dispatchView(ComponentType.Label);
            const [, config] = (mockDialog.open as Mock).mock.calls[0];
            expect(config.data.type).toBe(ComponentType.Label);
            expect(config.data.entity.permissions.canWrite).toBe(false);
        });

        it('opens the process group dialog with the current user observable wired', async () => {
            const { effects, actions$, mockDialog } = await setup();
            const componentInstance: any = {};
            (mockDialog.open as Mock).mockReturnValue({ componentInstance });
            actions$(
                of(
                    viewComponentConfiguration({
                        request: { entity: baseEntity, componentType: ComponentType.ProcessGroup }
                    })
                )
            );
            await firstValueFrom(effects.viewComponentConfiguration$);

            const [, config] = (mockDialog.open as Mock).mock.calls[0];
            expect(config.data.type).toBe(ComponentType.ProcessGroup);
            expect(componentInstance.currentUser$).toBeDefined();
            expect(componentInstance.parameterContexts).toEqual([]);
        });

        it('opens the remote process group dialog with read-only permissions', async () => {
            const { mockDialog } = await dispatchView(ComponentType.RemoteProcessGroup);
            const [, config] = (mockDialog.open as Mock).mock.calls[0];
            expect(config.data.type).toBe(ComponentType.RemoteProcessGroup);
            expect(config.data.entity.permissions.canWrite).toBe(false);
        });

        it('does not open a dialog for unsupported component types', async () => {
            const { mockDialog } = await dispatchView(ComponentType.Funnel);
            expect(mockDialog.open).not.toHaveBeenCalled();
        });

        it('handles entities without operatePermissions by forcing operatePermissions.canWrite to false', async () => {
            const entityWithoutOperatePermissions = {
                id: 'comp-1',
                uri: 'https://localhost/nifi-api/processors/comp-1',
                permissions: { canRead: true, canWrite: true },
                component: { name: 'My Component' }
            };
            const { mockDialog } = await dispatchView(ComponentType.Processor, entityWithoutOperatePermissions);
            const [, config] = (mockDialog.open as Mock).mock.calls[0];
            expect(config.data.entity.operatePermissions).toBeDefined();
            expect(config.data.entity.operatePermissions.canWrite).toBe(false);
        });

        it('wires parameter context onto the EditProcessor dialog when one is bound', async () => {
            const parameterContext = createParameterContextFixture({
                component: { id: 'pc-1', name: 'Bound PC', parameters: [] }
            });
            const { effects, actions$, mockDialogRef } = await setup({ parameterContext });

            actions$(
                of(
                    viewComponentConfiguration({
                        request: { entity: baseEntity, componentType: ComponentType.Processor }
                    })
                )
            );
            await firstValueFrom(effects.viewComponentConfiguration$);

            expect(mockDialogRef.componentInstance.parameterContext).toBe(parameterContext);
            // Read-only mode in nifi-frontend deliberately does NOT set supportsParameters
            // on EditProcessor (EditProcessor lacks that input). Pin this behavior.
            expect(mockDialogRef.componentInstance.supportsParameters).toBeUndefined();
            // The property table hides "Go to Parameter" when this callback is undefined,
            // while still rendering parameter values in the value tip.
            expect(mockDialogRef.componentInstance.goToParameter).toBeUndefined();
        });

        it('leaves parameterContext undefined on the EditProcessor dialog when no PC is bound', async () => {
            const { effects, actions$, mockDialogRef } = await setup({ parameterContext: null });

            actions$(
                of(
                    viewComponentConfiguration({
                        request: { entity: baseEntity, componentType: ComponentType.Processor }
                    })
                )
            );
            await firstValueFrom(effects.viewComponentConfiguration$);

            expect(mockDialogRef.componentInstance.parameterContext).toBeUndefined();
            expect(mockDialogRef.componentInstance.goToParameter).toBeUndefined();
        });
    });

    describe('navigateToProvenanceForComponent$', () => {
        it('should navigate to /provenance with componentId query param and back navigation state', async () => {
            const { effects, actions$, mockRouter } = await setup({
                connectorId: 'conn-1',
                processGroupIdFromRoute: 'pg-root'
            });
            actions$(
                of(
                    navigateToProvenanceForComponent({
                        id: 'proc-1',
                        componentType: ComponentType.Processor
                    })
                )
            );

            await firstValueFrom(effects.navigateToProvenanceForComponent$);

            expect(mockRouter.navigate).toHaveBeenCalledWith(['/provenance'], {
                queryParams: { componentId: 'proc-1' },
                state: {
                    backNavigation: {
                        route: ['/connectors', 'conn-1', 'canvas', 'pg-root', ComponentType.Processor, 'proc-1'],
                        routeBoundary: ['/provenance'],
                        context: 'Processor'
                    }
                }
            });
        });

        it('should use the supplied component type in the back navigation route for a funnel', async () => {
            const { effects, actions$, mockRouter } = await setup({
                connectorId: 'conn-1',
                processGroupIdFromRoute: 'pg-root'
            });
            actions$(
                of(
                    navigateToProvenanceForComponent({
                        id: 'funnel-1',
                        componentType: ComponentType.Funnel
                    })
                )
            );

            await firstValueFrom(effects.navigateToProvenanceForComponent$);

            expect(mockRouter.navigate).toHaveBeenCalledWith(['/provenance'], {
                queryParams: { componentId: 'funnel-1' },
                state: {
                    backNavigation: {
                        route: ['/connectors', 'conn-1', 'canvas', 'pg-root', ComponentType.Funnel, 'funnel-1'],
                        routeBoundary: ['/provenance'],
                        context: 'Funnel'
                    }
                }
            });
        });
    });

    describe('navigateToQueueListing$', () => {
        it('should navigate to /queue/:connectionId with back navigation to the originating connection', async () => {
            const { effects, actions$, mockRouter } = await setup({
                connectorId: 'conn-1',
                processGroupIdFromRoute: 'pg-root'
            });
            actions$(of(navigateToQueueListing({ request: { connectionId: 'conn-listing-1' } })));

            await firstValueFrom(effects.navigateToQueueListing$);

            expect(mockRouter.navigate).toHaveBeenCalledWith(['/queue', 'conn-listing-1'], {
                state: {
                    backNavigation: {
                        route: [
                            '/connectors',
                            'conn-1',
                            'canvas',
                            'pg-root',
                            ComponentType.Connection,
                            'conn-listing-1'
                        ],
                        routeBoundary: ['/queue', 'conn-listing-1'],
                        context: 'connection'
                    }
                }
            });
        });
    });

    describe('navigateToControllerServices$', () => {
        it('should navigate to controller services using the supplied process group when provided', async () => {
            const { effects, actions$, mockRouter } = await setup({
                connectorId: 'conn-1',
                processGroupIdFromRoute: 'pg-root'
            });
            actions$(of(navigateToControllerServices({ processGroupId: 'pg-target' })));

            await firstValueFrom(effects.navigateToControllerServices$);

            expect(mockRouter.navigate).toHaveBeenCalledWith(
                ['/connectors', 'conn-1', 'canvas', 'pg-target', 'controller-services'],
                {
                    state: {
                        backNavigation: {
                            route: ['/connectors', 'conn-1', 'canvas', 'pg-root'],
                            routeBoundary: ['/connectors', 'conn-1', 'canvas', 'pg-target', 'controller-services'],
                            context: 'process group'
                        }
                    }
                }
            );
        });
    });

    describe('navigateToControllerService$', () => {
        it('should navigate to a deep-linked controller service with back navigation to the canvas', async () => {
            const { effects, actions$, mockRouter } = await setup({
                connectorId: 'conn-1',
                processGroupIdFromRoute: 'pg-root'
            });
            actions$(of(navigateToControllerService({ processGroupId: 'pg-target', serviceId: 'svc-1' })));

            await firstValueFrom(effects.navigateToControllerService$);

            expect(mockRouter.navigate).toHaveBeenCalledWith(
                ['/connectors', 'conn-1', 'canvas', 'pg-target', 'controller-services', 'svc-1'],
                {
                    state: {
                        backNavigation: {
                            route: ['/connectors', 'conn-1', 'canvas', 'pg-root'],
                            routeBoundary: [
                                '/connectors',
                                'conn-1',
                                'canvas',
                                'pg-target',
                                'controller-services',
                                'svc-1'
                            ],
                            context: 'process group'
                        }
                    }
                }
            );
        });
    });

    describe('refreshAfterQueueEmptied$', () => {
        it('should dispatch loadConnectorFlow when the queueEmptied source is connector-canvas', async () => {
            const { effects, actions$ } = await setup({
                connectorId: 'conn-1',
                processGroupIdFromRoute: 'pg-root'
            });
            actions$(
                of(
                    queueEmptied({
                        connectionId: 'conn-listing-1',
                        processGroupId: null,
                        source: 'connector-canvas'
                    })
                )
            );

            const result = await firstValueFrom(effects.refreshAfterQueueEmptied$);

            expect(result).toEqual(
                loadConnectorFlow({
                    connectorId: 'conn-1',
                    processGroupId: 'pg-root'
                })
            );
        });

        it('should dispatch loadConnectorFlow when all queues are emptied for a process group with connector-canvas source', async () => {
            const { effects, actions$ } = await setup({
                connectorId: 'conn-1',
                processGroupIdFromRoute: 'pg-root'
            });
            actions$(
                of(
                    queueEmptied({
                        connectionId: null,
                        processGroupId: 'pg-root',
                        source: 'connector-canvas'
                    })
                )
            );

            const result = await firstValueFrom(effects.refreshAfterQueueEmptied$);

            expect(result).toEqual(
                loadConnectorFlow({
                    connectorId: 'conn-1',
                    processGroupId: 'pg-root'
                })
            );
        });

        it('should ignore queueEmptied events from the flow designer', async () => {
            const { effects, actions$ } = await setup({
                connectorId: 'conn-1',
                processGroupIdFromRoute: 'pg-root'
            });
            actions$(
                of(
                    queueEmptied({
                        connectionId: 'conn-listing-1',
                        processGroupId: null,
                        source: 'flow-designer'
                    })
                )
            );

            let emitted: Action | undefined;
            effects.refreshAfterQueueEmptied$.subscribe((action) => {
                emitted = action;
            });

            await new Promise<void>((resolve) => setTimeout(resolve, 0));

            expect(emitted).toBeUndefined();
        });

        it('should not dispatch when the connectorId route param is missing', async () => {
            const { effects, actions$ } = await setup({
                connectorId: null,
                processGroupIdFromRoute: 'pg-root'
            });
            actions$(
                of(
                    queueEmptied({
                        connectionId: 'conn-listing-1',
                        processGroupId: null,
                        source: 'connector-canvas'
                    })
                )
            );

            let emitted: Action | undefined;
            effects.refreshAfterQueueEmptied$.subscribe((action) => {
                emitted = action;
            });

            await new Promise<void>((resolve) => setTimeout(resolve, 0));

            expect(emitted).toBeUndefined();
        });
    });

    describe('reloadConnectorFlow$', () => {
        it('should dispatch loadConnectorFlow with the connector and process group ids from state', async () => {
            const { effects, actions$ } = await setup({
                connectorId: 'conn-state',
                processGroupId: 'pg-state'
            });
            actions$(of(reloadConnectorFlow()));

            const action = await firstValueFrom(effects.reloadConnectorFlow$);
            expect(action).toEqual(
                loadConnectorFlow({
                    connectorId: 'conn-state',
                    processGroupId: 'pg-state'
                })
            );
        });

        it('should not dispatch loadConnectorFlow when the connector id is empty', async () => {
            const { effects, actions$ } = await setup({
                connectorId: '',
                processGroupId: 'pg-state'
            });
            actions$(of(reloadConnectorFlow()));

            let emitted: Action | undefined;
            effects.reloadConnectorFlow$.subscribe((action) => {
                emitted = action;
            });

            await new Promise<void>((resolve) => setTimeout(resolve, 0));

            expect(emitted).toBeUndefined();
        });

        it('should not dispatch loadConnectorFlow when the process group id is null', async () => {
            const { effects, actions$ } = await setup({
                connectorId: 'conn-state',
                processGroupId: null
            });
            actions$(of(reloadConnectorFlow()));

            let emitted: Action | undefined;
            effects.reloadConnectorFlow$.subscribe((action) => {
                emitted = action;
            });

            await new Promise<void>((resolve) => setTimeout(resolve, 0));

            expect(emitted).toBeUndefined();
        });

        describe('throttleTime', () => {
            beforeEach(() => {
                vi.useFakeTimers();
            });

            afterEach(() => {
                vi.useRealTimers();
            });

            async function flushPromises(): Promise<void> {
                await Promise.resolve();
                await Promise.resolve();
            }

            it('should collapse rapid back-to-back reloadConnectorFlow dispatches into a single loadConnectorFlow within the throttle window', async () => {
                const { effects, actions$ } = await setup({
                    connectorId: 'conn-state',
                    processGroupId: 'pg-state'
                });
                const actionSubject = new Subject<Action>();
                actions$(actionSubject.asObservable());

                const emitted: Action[] = [];
                const subscription = effects.reloadConnectorFlow$.subscribe((action) => {
                    emitted.push(action);
                });

                // Three reloadConnectorFlow dispatches inside the 1s throttle window:
                // throttleTime defaults to leading-only, so only the first should produce
                // a loadConnectorFlow. The remaining two are dropped.
                actionSubject.next(reloadConnectorFlow());
                actionSubject.next(reloadConnectorFlow());
                actionSubject.next(reloadConnectorFlow());

                await flushPromises();

                expect(emitted).toEqual([
                    loadConnectorFlow({
                        connectorId: 'conn-state',
                        processGroupId: 'pg-state'
                    })
                ]);

                // After the throttle window expires the next dispatch is allowed through.
                await vi.advanceTimersByTimeAsync(1500);
                actionSubject.next(reloadConnectorFlow());
                await flushPromises();

                expect(emitted).toEqual([
                    loadConnectorFlow({
                        connectorId: 'conn-state',
                        processGroupId: 'pg-state'
                    }),
                    loadConnectorFlow({
                        connectorId: 'conn-state',
                        processGroupId: 'pg-state'
                    })
                ]);

                subscription.unsubscribe();
            });
        });
    });

    describe('document visibility wake-up', () => {
        it('should dispatch reloadConnectorFlow when the document becomes visible after being hidden longer than the polling interval', async () => {
            const { store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            // Simulate the tab being foregrounded after the polling cadence elapsed.
            // The lastReload field starts at 0 in the freshly constructed effects, so
            // any changedTimestamp greater than the 30 second threshold satisfies the
            // wake-up filter.
            store.overrideSelector(selectDocumentVisibilityState, {
                documentVisibility: DocumentVisibility.Visible,
                changedTimestamp: 31 * 1000
            });
            store.refreshState();

            expect(dispatchSpy).toHaveBeenCalledWith(reloadConnectorFlow());
        });

        it('should not dispatch reloadConnectorFlow when the document becomes visible within the polling interval', async () => {
            const { store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            store.overrideSelector(selectDocumentVisibilityState, {
                documentVisibility: DocumentVisibility.Visible,
                changedTimestamp: 5 * 1000
            });
            store.refreshState();

            expect(dispatchSpy).not.toHaveBeenCalledWith(reloadConnectorFlow());
        });

        it('should not dispatch reloadConnectorFlow when the document transitions to hidden', async () => {
            const { store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            store.overrideSelector(selectDocumentVisibilityState, {
                documentVisibility: DocumentVisibility.Hidden,
                changedTimestamp: 60 * 1000
            });
            store.refreshState();

            expect(dispatchSpy).not.toHaveBeenCalledWith(reloadConnectorFlow());
        });
    });

    describe('startConnectorCanvasPolling$', () => {
        beforeEach(() => {
            vi.useFakeTimers();
        });

        afterEach(() => {
            vi.useRealTimers();
        });

        async function flushPromises(): Promise<void> {
            await Promise.resolve();
            await Promise.resolve();
        }

        it('should dispatch reloadConnectorFlow on each polling tick while the document is visible', async () => {
            const { effects, actions$ } = await setup();
            const actionSubject = new Subject<Action>();
            actions$(actionSubject.asObservable());

            const emitted: Action[] = [];
            const subscription = effects.startConnectorCanvasPolling$.subscribe((action) => {
                emitted.push(action);
            });

            actionSubject.next(startConnectorCanvasPolling());

            await vi.advanceTimersByTimeAsync(30000);
            await flushPromises();
            await vi.advanceTimersByTimeAsync(30000);
            await flushPromises();

            subscription.unsubscribe();

            expect(emitted).toEqual([reloadConnectorFlow(), reloadConnectorFlow()]);
        });

        it('should suppress reloadConnectorFlow while the document is hidden', async () => {
            const { effects, actions$ } = await setup({
                documentVisibility: DocumentVisibility.Hidden
            });
            const actionSubject = new Subject<Action>();
            actions$(actionSubject.asObservable());

            const emitted: Action[] = [];
            const subscription = effects.startConnectorCanvasPolling$.subscribe((action) => {
                emitted.push(action);
            });

            actionSubject.next(startConnectorCanvasPolling());

            await vi.advanceTimersByTimeAsync(60000);
            await flushPromises();

            subscription.unsubscribe();

            expect(emitted).toEqual([]);
        });

        it('should suppress reloadConnectorFlow while a load is already in flight', async () => {
            const { effects, actions$ } = await setup({ loadingStatus: 'loading' });
            const actionSubject = new Subject<Action>();
            actions$(actionSubject.asObservable());

            const emitted: Action[] = [];
            const subscription = effects.startConnectorCanvasPolling$.subscribe((action) => {
                emitted.push(action);
            });

            actionSubject.next(startConnectorCanvasPolling());

            await vi.advanceTimersByTimeAsync(30000);
            await flushPromises();

            subscription.unsubscribe();

            expect(emitted).toEqual([]);
        });

        it('should stop polling once stopConnectorCanvasPolling is dispatched', async () => {
            const { effects, actions$ } = await setup();
            const actionSubject = new Subject<Action>();
            actions$(actionSubject.asObservable());

            const emitted: Action[] = [];
            const subscription = effects.startConnectorCanvasPolling$.subscribe((action) => {
                emitted.push(action);
            });

            actionSubject.next(startConnectorCanvasPolling());

            await vi.advanceTimersByTimeAsync(30000);
            await flushPromises();
            expect(emitted).toEqual([reloadConnectorFlow()]);

            actionSubject.next(stopConnectorCanvasPolling());

            await vi.advanceTimersByTimeAsync(60000);
            await flushPromises();

            subscription.unsubscribe();

            // No additional emissions after stop.
            expect(emitted).toEqual([reloadConnectorFlow()]);
        });
    });

    describe('loadConnectorParameterContextOnLoadSuccess$', () => {
        function flowSuccessAction(connectorId: string, processGroupId: string | null) {
            return loadConnectorFlowSuccess({
                connectorId,
                processGroupId,
                parentProcessGroupId: null,
                breadcrumb: null,
                labels: [],
                funnels: [],
                inputPorts: [],
                outputPorts: [],
                remoteProcessGroups: [],
                processGroups: [],
                processors: [],
                connections: []
            });
        }

        function controllerServicesSuccessAction(connectorId: string, processGroupId: string) {
            return loadConnectorControllerServicesSuccess({
                response: {
                    connectorId,
                    processGroupId,
                    controllerServices: [],
                    breadcrumb: null,
                    loadedTimestamp: '2024-01-01T00:00:00.000Z'
                }
            });
        }

        it('should dispatch loadConnectorParameterContext scoped to CONNECTOR_CANVAS after flow load success', async () => {
            const { effects, actions$ } = await setup();
            actions$(of(flowSuccessAction('connector-123', 'pg-abc')));

            const result = await firstValueFrom(effects.loadConnectorParameterContextOnLoadSuccess$);
            expect(result).toEqual(
                loadConnectorParameterContext({
                    connectorId: 'connector-123',
                    processGroupId: 'pg-abc',
                    errorContext: ErrorContextKey.CONNECTOR_CANVAS
                })
            );
        });

        it('should dispatch loadConnectorParameterContext scoped to CONTROLLER_SERVICES after controller services load success (deep link)', async () => {
            const { effects, actions$ } = await setup();
            actions$(of(controllerServicesSuccessAction('connector-123', 'pg-abc')));

            const result = await firstValueFrom(effects.loadConnectorParameterContextOnLoadSuccess$);
            expect(result).toEqual(
                loadConnectorParameterContext({
                    connectorId: 'connector-123',
                    processGroupId: 'pg-abc',
                    errorContext: ErrorContextKey.CONTROLLER_SERVICES
                })
            );
        });

        it('should not dispatch when processGroupId is null', async () => {
            const { effects, actions$ } = await setup();
            actions$(of(flowSuccessAction('connector-123', null)));

            const emitted = await firstValueFrom(effects.loadConnectorParameterContextOnLoadSuccess$.pipe(toArray()));

            expect(emitted).toEqual([]);
        });

        it('should dispatch only once when polling refires loadConnectorFlowSuccess for the same process group', async () => {
            const { effects, actions$ } = await setup();
            actions$(
                of(
                    flowSuccessAction('connector-123', 'pg-abc'),
                    flowSuccessAction('connector-123', 'pg-abc'),
                    flowSuccessAction('connector-123', 'pg-abc')
                )
            );

            const emitted = await firstValueFrom(effects.loadConnectorParameterContextOnLoadSuccess$.pipe(toArray()));

            expect(emitted).toEqual([
                loadConnectorParameterContext({
                    connectorId: 'connector-123',
                    processGroupId: 'pg-abc',
                    errorContext: ErrorContextKey.CONNECTOR_CANVAS
                })
            ]);
        });

        it('should dedupe canvas → controller-services transition within the same process group', async () => {
            const { effects, actions$ } = await setup();
            actions$(
                of(
                    flowSuccessAction('connector-123', 'pg-abc'),
                    controllerServicesSuccessAction('connector-123', 'pg-abc')
                )
            );

            const emitted = await firstValueFrom(effects.loadConnectorParameterContextOnLoadSuccess$.pipe(toArray()));

            expect(emitted).toEqual([
                loadConnectorParameterContext({
                    connectorId: 'connector-123',
                    processGroupId: 'pg-abc',
                    errorContext: ErrorContextKey.CONNECTOR_CANVAS
                })
            ]);
        });

        it('should dispatch again when the process group changes', async () => {
            const { effects, actions$ } = await setup();
            actions$(
                of(
                    flowSuccessAction('connector-123', 'pg-abc'),
                    flowSuccessAction('connector-123', 'pg-abc'),
                    flowSuccessAction('connector-123', 'pg-xyz')
                )
            );

            const emitted = await firstValueFrom(effects.loadConnectorParameterContextOnLoadSuccess$.pipe(toArray()));

            expect(emitted).toEqual([
                loadConnectorParameterContext({
                    connectorId: 'connector-123',
                    processGroupId: 'pg-abc',
                    errorContext: ErrorContextKey.CONNECTOR_CANVAS
                }),
                loadConnectorParameterContext({
                    connectorId: 'connector-123',
                    processGroupId: 'pg-xyz',
                    errorContext: ErrorContextKey.CONNECTOR_CANVAS
                })
            ]);
        });
    });

    describe('loadConnectorParameterContext$', () => {
        const parameterContext = createParameterContextFixture();

        it('should dispatch success with the parameter context returned by the service', async () => {
            const { effects, actions$, mockConnectorService } = await setup({
                parameterContextResponse: parameterContext
            });
            actions$(
                of(
                    loadConnectorParameterContext({
                        connectorId: 'connector-123',
                        processGroupId: 'pg-abc',
                        errorContext: ErrorContextKey.CONNECTOR_CANVAS
                    })
                )
            );

            const result = await firstValueFrom(effects.loadConnectorParameterContext$);

            expect(mockConnectorService.getConnectorParameterContext).toHaveBeenCalledWith('connector-123', 'pg-abc');
            expect(result).toEqual(loadConnectorParameterContextSuccess({ parameterContext }));
        });

        it('should dispatch success with null when the service returns null (HTTP 204)', async () => {
            const { effects, actions$ } = await setup({ parameterContextResponse: null });
            actions$(
                of(
                    loadConnectorParameterContext({
                        connectorId: 'connector-123',
                        processGroupId: 'pg-abc',
                        errorContext: ErrorContextKey.CONNECTOR_CANVAS
                    })
                )
            );

            const result = await firstValueFrom(effects.loadConnectorParameterContext$);
            expect(result).toEqual(loadConnectorParameterContextSuccess({ parameterContext: null }));
        });

        it('should dispatch failure using the errorContext carried on the action', async () => {
            const errorResponse = new HttpErrorResponse({ error: 'boom', status: 500, statusText: 'ISE' });
            const { effects, actions$ } = await setup({ parameterContextError: errorResponse });
            actions$(
                of(
                    loadConnectorParameterContext({
                        connectorId: 'connector-123',
                        processGroupId: 'pg-abc',
                        errorContext: ErrorContextKey.CONNECTOR_CANVAS
                    })
                )
            );

            const result = await firstValueFrom(effects.loadConnectorParameterContext$);

            expect(result).toEqual(
                loadConnectorParameterContextFailure({
                    errorContext: {
                        errors: ['Error message'],
                        context: ErrorContextKey.CONNECTOR_CANVAS
                    }
                })
            );
        });

        it('should propagate CONTROLLER_SERVICES error context for CS-triggered loads', async () => {
            const errorResponse = new HttpErrorResponse({ error: 'boom', status: 500, statusText: 'ISE' });
            const { effects, actions$ } = await setup({ parameterContextError: errorResponse });
            actions$(
                of(
                    loadConnectorParameterContext({
                        connectorId: 'connector-123',
                        processGroupId: 'pg-abc',
                        errorContext: ErrorContextKey.CONTROLLER_SERVICES
                    })
                )
            );

            const result = await firstValueFrom(effects.loadConnectorParameterContext$);

            expect(result).toEqual(
                loadConnectorParameterContextFailure({
                    errorContext: {
                        errors: ['Error message'],
                        context: ErrorContextKey.CONTROLLER_SERVICES
                    }
                })
            );
        });
    });

    describe('loadConnectorParameterContextFailure$', () => {
        it('should dispatch addBannerError with the failure error context', async () => {
            const { effects, actions$ } = await setup();
            const errorContext = {
                errors: ['Unable to load parameter context'],
                context: ErrorContextKey.CONNECTORS
            };
            actions$(of(loadConnectorParameterContextFailure({ errorContext })));

            const result = await firstValueFrom(effects.loadConnectorParameterContextFailure$);

            expect(result).toEqual(ErrorActions.addBannerError({ errorContext }));
        });
    });
});
