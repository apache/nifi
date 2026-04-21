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
import { HttpErrorResponse } from '@angular/common/http';
import { ComponentType, ComponentTypeNamePipe } from '@nifi/shared';
import { MatDialog } from '@angular/material/dialog';
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
    navigateToProvenanceForComponent,
    navigateWithoutTransform,
    selectComponents,
    viewComponentConfiguration
} from './connector-canvas.actions';
import {
    selectConnectorIdFromRoute,
    selectParentProcessGroupId,
    selectProcessGroupId,
    selectProcessGroupIdFromRoute
} from './connector-canvas.selectors';
import type { Mock } from 'vitest';

describe('ConnectorCanvasEffects', () => {
    // Setup function following SIFERS pattern
    async function setup(
        options: {
            connectorId?: string | null;
            parentProcessGroupId?: string | null;
            processGroupId?: string | null;
            processGroupIdFromRoute?: string | null;
        } = {}
    ) {
        let actions$: Observable<Action>;

        const connectorId = options.connectorId !== undefined ? options.connectorId : 'connector-1';
        const parentProcessGroupId =
            options.parentProcessGroupId !== undefined ? options.parentProcessGroupId : 'parent-pg';
        const processGroupId = options.processGroupId !== undefined ? options.processGroupId : 'child-pg';
        const processGroupIdFromRoute =
            options.processGroupIdFromRoute !== undefined ? options.processGroupIdFromRoute : processGroupId;

        // Mock services
        const mockConnectorService = {
            getConnectorFlow: vi.fn()
        };

        const mockErrorHelper = {
            getErrorString: vi.fn().mockReturnValue('Error message'),
            handleLoadingError: vi.fn()
        };

        const mockRouter = {
            navigate: vi.fn().mockResolvedValue(true)
        };

        const mockDialog = {
            open: vi.fn().mockReturnValue({ componentInstance: {} })
        };

        await TestBed.configureTestingModule({
            providers: [
                ConnectorCanvasEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    initialState: {},
                    selectors: [
                        { selector: selectConnectorIdFromRoute, value: connectorId },
                        { selector: selectParentProcessGroupId, value: parentProcessGroupId },
                        { selector: selectProcessGroupId, value: processGroupId },
                        { selector: selectProcessGroupIdFromRoute, value: processGroupIdFromRoute }
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
            mockDialog
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
    });
});
