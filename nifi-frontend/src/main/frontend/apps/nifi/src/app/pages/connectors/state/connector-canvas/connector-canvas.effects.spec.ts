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
import { Observable, of, Subject, throwError } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { ComponentType } from '@nifi/shared';
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
    navigateWithoutTransform,
    selectComponents
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
                { provide: Router, useValue: mockRouter }
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
            mockRouter
        };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('loadConnectorFlow$', () => {
        it('should call getConnectorFlow and dispatch loadConnectorFlowSuccess', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
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

                    effects.loadConnectorFlow$.subscribe((action) => {
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
                        resolve();
                    });
                });
            }));

        it('should default flow collections when processGroupFlow.flow is missing', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService }) => {
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

                    effects.loadConnectorFlow$.subscribe((action) => {
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
                        resolve();
                    });
                });
            }));

        it('should dispatch loadConnectorFlowFailure when getConnectorFlow errors', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockConnectorService, mockErrorHelper }) => {
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

                    effects.loadConnectorFlow$.subscribe((action) => {
                        expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
                        expect(action).toEqual(
                            loadConnectorFlowFailure({
                                errorContext: {
                                    errors: ['Error message'],
                                    context: ErrorContextKey.CONNECTORS
                                }
                            })
                        );
                        resolve();
                    });
                });
            }));
    });

    describe('loadConnectorFlowComplete$', () => {
        it('should dispatch loadConnectorFlowComplete on loadConnectorFlowSuccess', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$ }) => {
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

                    effects.loadConnectorFlowComplete$.subscribe((action) => {
                        expect(action).toEqual(loadConnectorFlowComplete());
                        resolve();
                    });
                });
            }));
    });

    describe('enterProcessGroup$', () => {
        it('should navigate to the child process group canvas URL', () =>
            new Promise<void>((resolve) => {
                setup({ connectorId: 'my-connector' }).then(({ effects, actions$, mockRouter }) => {
                    actions$(
                        of(
                            enterProcessGroup({
                                request: { id: 'child-pg-99' }
                            })
                        )
                    );

                    effects.enterProcessGroup$.subscribe(() => {
                        expect(mockRouter.navigate).toHaveBeenCalledWith([
                            '/connectors',
                            'my-connector',
                            'canvas',
                            'child-pg-99'
                        ]);
                        resolve();
                    });
                });
            }));
    });

    describe('leaveProcessGroup$', () => {
        it('should navigate to parent, then after loadConnectorFlowComplete dispatch navigateWithoutTransform', () =>
            new Promise<void>((resolve) => {
                setup({
                    connectorId: 'conn-x',
                    parentProcessGroupId: 'parent-pg',
                    processGroupId: 'child-pg'
                }).then(({ effects, actions$, mockRouter }) => {
                    const actionSubject = new Subject<Action>();
                    actions$(actionSubject.asObservable());

                    effects.leaveProcessGroup$.subscribe((action) => {
                        expect(mockRouter.navigate).toHaveBeenCalledWith([
                            '/connectors',
                            'conn-x',
                            'canvas',
                            'parent-pg'
                        ]);
                        expect(action).toEqual(
                            navigateWithoutTransform({
                                url: [
                                    '/connectors',
                                    'conn-x',
                                    'canvas',
                                    'parent-pg',
                                    ComponentType.ProcessGroup,
                                    'child-pg'
                                ]
                            })
                        );
                        resolve();
                    });

                    actionSubject.next(leaveProcessGroup());
                    actionSubject.next(loadConnectorFlowComplete());
                    actionSubject.complete();
                });
            }));

        it('should not navigate when parent process group id is null', () =>
            new Promise<void>((resolve) => {
                setup({
                    connectorId: 'conn-x',
                    parentProcessGroupId: null,
                    processGroupId: 'child-pg'
                }).then(({ effects, actions$, mockRouter }) => {
                    actions$(of(leaveProcessGroup()));

                    effects.leaveProcessGroup$.subscribe({
                        complete: () => {
                            expect(mockRouter.navigate).not.toHaveBeenCalled();
                            resolve();
                        }
                    });
                });
            }));

        it('should not navigate when current process group id is null', () =>
            new Promise<void>((resolve) => {
                setup({
                    connectorId: 'conn-x',
                    parentProcessGroupId: 'parent-pg',
                    processGroupId: null
                }).then(({ effects, actions$, mockRouter }) => {
                    actions$(of(leaveProcessGroup()));

                    effects.leaveProcessGroup$.subscribe({
                        complete: () => {
                            expect(mockRouter.navigate).not.toHaveBeenCalled();
                            resolve();
                        }
                    });
                });
            }));
    });

    describe('selectComponents$', () => {
        it('should dispatch navigateWithoutTransform with single component URL', () =>
            new Promise<void>((resolve) => {
                setup({
                    connectorId: 'conn-1',
                    processGroupIdFromRoute: 'pg-root'
                }).then(({ effects, actions$ }) => {
                    actions$(
                        of(
                            selectComponents({
                                request: {
                                    components: [{ id: 'proc-1', componentType: ComponentType.Processor }]
                                }
                            })
                        )
                    );

                    effects.selectComponents$.subscribe((action) => {
                        expect(action).toEqual(
                            navigateWithoutTransform({
                                url: ['/connectors', 'conn-1', 'canvas', 'pg-root', ComponentType.Processor, 'proc-1']
                            })
                        );
                        resolve();
                    });
                });
            }));

        it('should dispatch navigateWithoutTransform with bulk URL for multiple components', () =>
            new Promise<void>((resolve) => {
                setup({
                    connectorId: 'conn-1',
                    processGroupIdFromRoute: 'pg-root'
                }).then(({ effects, actions$ }) => {
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

                    effects.selectComponents$.subscribe((action) => {
                        expect(action).toEqual(
                            navigateWithoutTransform({
                                url: ['/connectors', 'conn-1', 'canvas', 'pg-root', 'bulk', 'proc-1,conn-2']
                            })
                        );
                        resolve();
                    });
                });
            }));
    });

    describe('deselectAllComponents$', () => {
        it('should dispatch navigateWithoutTransform with base canvas URL', () =>
            new Promise<void>((resolve) => {
                setup({
                    connectorId: 'conn-1',
                    processGroupIdFromRoute: 'pg-root'
                }).then(({ effects, actions$ }) => {
                    actions$(of(deselectAllComponents()));

                    effects.deselectAllComponents$.subscribe((action) => {
                        expect(action).toEqual(
                            navigateWithoutTransform({
                                url: ['/connectors', 'conn-1', 'canvas', 'pg-root']
                            })
                        );
                        resolve();
                    });
                });
            }));
    });

    describe('navigateWithoutTransform$', () => {
        it('should call router.navigate with replaceUrl', () =>
            new Promise<void>((resolve) => {
                setup().then(({ effects, actions$, mockRouter }) => {
                    const url = ['/connectors', 'conn-1', 'canvas', 'pg-root', 'Processor', 'proc-1'];
                    actions$(of(navigateWithoutTransform({ url })));

                    effects.navigateWithoutTransform$.subscribe(() => {
                        expect(mockRouter.navigate).toHaveBeenCalledWith(url, { replaceUrl: true });
                        resolve();
                    });
                });
            }));
    });
});
