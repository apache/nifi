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

import { Injectable } from '@angular/core';
import { FlowService } from '../../service/flow.service';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import * as FlowActions from './flow.actions';
import { catchError, combineLatest, filter, from, map, mergeMap, of, switchMap, tap, withLatestFrom } from 'rxjs';
import {
    DeleteComponentResponse,
    LoadProcessGroupRequest,
    LoadProcessGroupResponse,
    Snippet,
    UpdateComponentFailure,
    UpdateComponentResponse
} from './index';
import { CanvasState } from '../index';
import { Store } from '@ngrx/store';
import { selectCurrentProcessGroupId, selectParentProcessGroupId, selectSelectedComponentIds } from './flow.selectors';
import { ConnectionManager } from '../../service/manager/connection-manager.service';
import { MatDialog } from '@angular/material/dialog';
import { CreatePort } from '../../ui/port/create-port/create-port.component';
import { EditPort } from '../../ui/port/edit-port/edit-port.component';
import { ComponentType } from '../shared';
import { Router } from '@angular/router';
import { selectUrl } from '../../../state/router/router.selectors';
import { Client } from '../../service/client.service';
import { CanvasUtils } from '../../service/canvas-utils.service';

@Injectable()
export class FlowEffects {
    constructor(
        private actions$: Actions,
        private store: Store<CanvasState>,
        private flowService: FlowService,
        private client: Client,
        private canvasUtils: CanvasUtils,
        private connectionManager: ConnectionManager,
        private router: Router,
        private dialog: MatDialog
    ) {}

    reloadFlow$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.reloadFlow),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([action, processGroupId]) => {
                return of(
                    FlowActions.loadProcessGroup({
                        request: {
                            id: processGroupId
                        }
                    })
                );
            })
        )
    );

    loadProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadProcessGroup),
            map((action) => action.request),
            switchMap((request: LoadProcessGroupRequest) =>
                combineLatest([
                    this.flowService.getFlow(request.id),
                    this.flowService.getFlowStatus(),
                    this.flowService.getClusterSummary(),
                    this.flowService.getControllerBulletins()
                ]).pipe(
                    map(([flow, flowStatus, clusterSummary, controllerBulletins]) => {
                        return FlowActions.loadProcessGroupSuccess({
                            response: {
                                id: request.id,
                                flow: flow,
                                flowStatus: flowStatus,
                                clusterSummary: clusterSummary.clusterSummary,
                                controllerBulletins: controllerBulletins
                            }
                        });
                    }),
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                )
            )
        )
    );

    loadProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadProcessGroupSuccess),
            map((action) => action.response),
            switchMap((response: LoadProcessGroupResponse) => {
                return of(
                    FlowActions.loadProcessGroupComplete({
                        response: response
                    })
                );
            })
        )
    );

    loadProcessGroupComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadProcessGroupComplete),
            switchMap(() => {
                return of(FlowActions.setRenderRequired({ renderRequired: false }));
            })
        )
    );

    createComponentRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createComponentRequest),
            map((action) => action.request),
            switchMap((request) => {
                switch (request.type) {
                    case ComponentType.Funnel:
                        return of(FlowActions.createFunnel({ request }));
                    case ComponentType.Label:
                        return of(FlowActions.createLabel({ request }));
                    case ComponentType.InputPort:
                    case ComponentType.OutputPort:
                        return of(FlowActions.openNewPortDialog({ request }));
                    default:
                        return of(FlowActions.flowApiError({ error: 'Unsupported type of Component.' }));
                }
            })
        )
    );

    openNewPortDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openNewPortDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog
                        .open(CreatePort, {
                            data: request,
                            panelClass: 'small-dialog'
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                            this.store.dispatch(FlowActions.clearFlowApiError());
                        });
                })
            ),
        { dispatch: false }
    );

    createPort$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createPort),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createPort(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: request.type,
                                payload: response
                            }
                        })
                    ),
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                )
            )
        )
    );

    createFunnel$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createFunnel),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createFunnel(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: request.type,
                                payload: response
                            }
                        })
                    ),
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                )
            )
        )
    );

    createLabel$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createLabel),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createLabel(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: request.type,
                                payload: response
                            }
                        })
                    ),
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                )
            )
        )
    );

    createComponentSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createComponentSuccess),
            map((action) => action.response),
            switchMap((response) => {
                this.dialog.closeAll();
                return of(FlowActions.createComponentComplete({ response }));
            })
        )
    );

    createComponentComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createComponentComplete),
            map((action) => action.response),
            switchMap((response) =>
                of(
                    FlowActions.selectComponents({
                        request: {
                            components: [
                                {
                                    id: response.payload.id,
                                    componentType: response.type
                                }
                            ]
                        }
                    }),
                    FlowActions.setRenderRequired({ renderRequired: false })
                )
            )
        )
    );

    navigateToEditComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToEditComponent),
                map((action) => action.request),
                withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
                tap(([request, processGroupId]) => {
                    this.router.navigate(['/process-groups', processGroupId, request.type, request.id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    editComponentRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.editComponent),
            map((action) => action.request),
            switchMap((request) => {
                switch (request.type) {
                    case ComponentType.InputPort:
                    case ComponentType.OutputPort:
                        return of(FlowActions.openEditPortDialog({ request }));
                    default:
                        return of(FlowActions.flowApiError({ error: 'Unsupported type of Component.' }));
                }
            })
        )
    );

    openEditPortDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openEditPortDialog),
                map((action) => action.request),
                withLatestFrom(this.store.select(selectUrl)),
                tap(([request, currentUrl]) => {
                    this.dialog
                        .open(EditPort, {
                            data: request,
                            panelClass: 'medium-dialog'
                        })
                        .afterClosed()
                        .subscribe(() => {
                            // determine the parent url (TODO: not sure how best to access
                            // the current activated route for use in navigate below). Could
                            // possible subscribe to router events but router-state does not
                            // seem to surface the activated route
                            const url: string[] = currentUrl.split('/');
                            url.pop();

                            this.store.dispatch(FlowActions.clearFlowApiError());
                            this.router.navigate(url);
                        });
                })
            ),
        { dispatch: false }
    );

    updateComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updateComponent),
            map((action) => action.request),
            mergeMap((request) =>
                from(this.flowService.updateComponent(request)).pipe(
                    map((response) => {
                        const updateComponentResponse: UpdateComponentResponse = {
                            requestId: request.requestId,
                            id: request.id,
                            type: request.type,
                            response: response
                        };
                        return FlowActions.updateComponentSuccess({ response: updateComponentResponse });
                    }),
                    catchError((error) => {
                        const updateComponentFailure: UpdateComponentFailure = {
                            id: request.id,
                            type: request.type,
                            restoreOnFailure: request.restoreOnFailure,
                            error: error.error
                        };
                        return of(FlowActions.updateComponentFailure({ response: updateComponentFailure }));
                    })
                )
            )
        )
    );

    updateComponentSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.updateComponentSuccess),
                tap(() => {
                    this.dialog.closeAll();
                })
            ),
        { dispatch: false }
    );

    updateComponentFailure$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updateComponentFailure),
            map((action) => action.response),
            switchMap((response) => of(FlowActions.flowApiError({ error: response.error })))
        )
    );

    updatePositions$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updatePositions),
            map((action) => action.request),
            mergeMap((request) => [
                ...request.componentUpdates.map((componentUpdate) => {
                    return FlowActions.updateComponent({
                        request: {
                            ...componentUpdate,
                            requestId: request.requestId
                        }
                    });
                }),
                ...request.connectionUpdates.map((connectionUpdate) => {
                    return FlowActions.updateComponent({
                        request: {
                            ...connectionUpdate,
                            requestId: request.requestId
                        }
                    });
                })
            ])
        )
    );

    awaitUpdatePositions$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updatePositions),
            map((action) => action.request),
            mergeMap((request) =>
                this.actions$.pipe(
                    ofType(FlowActions.updateComponentSuccess),
                    filter((updateSuccess) => ComponentType.Connection !== updateSuccess.response.type),
                    filter((updateSuccess) => request.requestId === updateSuccess.response.requestId),
                    map((response) => FlowActions.updatePositionComplete(response))
                )
            )
        )
    );

    updatePositionSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.updatePositionComplete),
                map((action) => action.response),
                tap((response) => {
                    this.connectionManager.renderConnectionForComponent(response.id, {
                        updatePath: true,
                        updateLabel: true
                    });
                })
            ),
        { dispatch: false }
    );

    deleteComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.deleteComponents),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            mergeMap(([requests, processGroupId]) => {
                if (requests.length === 1) {
                    return from(this.flowService.deleteComponent(requests[0])).pipe(
                        map((response) => {
                            const deleteResponses: DeleteComponentResponse[] = [
                                {
                                    id: requests[0].id,
                                    type: requests[0].type
                                }
                            ];

                            if (requests[0].type !== ComponentType.Connection) {
                                const componentConnections: any[] = this.canvasUtils.getComponentConnections(
                                    requests[0].id
                                );
                                componentConnections.forEach((componentConnection) =>
                                    deleteResponses.push({
                                        id: componentConnection.id,
                                        type: ComponentType.Connection
                                    })
                                );
                            }

                            return FlowActions.deleteComponentsSuccess({
                                response: deleteResponses
                            });
                        }),
                        catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                    );
                } else {
                    const snippet: Snippet = requests.reduce(
                        (snippet, request) => {
                            switch (request.type) {
                                case ComponentType.Processor:
                                    snippet.processors[request.id] = this.client.getRevision(request.entity);
                                    break;
                                case ComponentType.InputPort:
                                    snippet.inputPorts[request.id] = this.client.getRevision(request.entity);
                                    break;
                                case ComponentType.OutputPort:
                                    snippet.outputPorts[request.id] = this.client.getRevision(request.entity);
                                    break;
                                case ComponentType.ProcessGroup:
                                    snippet.processGroups[request.id] = this.client.getRevision(request.entity);
                                    break;
                                case ComponentType.RemoteProcessGroup:
                                    snippet.remoteProcessGroups[request.id] = this.client.getRevision(request.entity);
                                    break;
                                case ComponentType.Funnel:
                                    snippet.funnels[request.id] = this.client.getRevision(request.entity);
                                    break;
                                case ComponentType.Label:
                                    snippet.labels[request.id] = this.client.getRevision(request.entity);
                                    break;
                                case ComponentType.Connection:
                                    snippet.connections[request.id] = this.client.getRevision(request.entity);
                                    break;
                            }
                            return snippet;
                        },
                        {
                            parentGroupId: processGroupId,
                            processors: {},
                            funnels: {},
                            inputPorts: {},
                            outputPorts: {},
                            remoteProcessGroups: {},
                            processGroups: {},
                            connections: {},
                            labels: {}
                        } as Snippet
                    );

                    return from(this.flowService.createSnippet(snippet)).pipe(
                        switchMap((response) => this.flowService.deleteSnippet(response.snippet.id)),
                        map((response) => {
                            const deleteResponses: DeleteComponentResponse[] = [];

                            // prepare the delete responses with all requested components that are now deleted
                            requests.forEach((request) => {
                                deleteResponses.push({
                                    id: request.id,
                                    type: request.type
                                });

                                // if the component is not a connection, also include any of it's connections
                                if (request.type !== ComponentType.Connection) {
                                    const componentConnections: any[] = this.canvasUtils.getComponentConnections(
                                        request.id
                                    );
                                    componentConnections.forEach((componentConnection) =>
                                        deleteResponses.push({
                                            id: componentConnection.id,
                                            type: ComponentType.Connection
                                        })
                                    );
                                }
                            });

                            return FlowActions.deleteComponentsSuccess({
                                response: deleteResponses
                            });
                        }),
                        catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                    );
                }
            })
        )
    );

    enterProcessGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.enterProcessGroup),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/process-groups', request.id]);
                })
            ),
        { dispatch: false }
    );

    leaveProcessGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.leaveProcessGroup),
                withLatestFrom(this.store.select(selectParentProcessGroupId)),
                filter(([action, parentProcessGroupId]) => parentProcessGroupId != null),
                tap(([action, parentProcessGroupId]) => {
                    this.router.navigate(['/process-groups', parentProcessGroupId]);
                })
            ),
        { dispatch: false }
    );

    addSelectedComponents$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.addSelectedComponents),
                map((action) => action.request),
                withLatestFrom(
                    this.store.select(selectCurrentProcessGroupId),
                    this.store.select(selectSelectedComponentIds)
                ),
                tap(([request, processGroupId, selected]) => {
                    if (selected.length === 0) {
                        if (request.components.length === 1) {
                            this.router.navigate([
                                '/process-groups',
                                processGroupId,
                                request.components[0].componentType,
                                request.components[0].id
                            ]);
                        } else if (request.components.length > 1) {
                            const ids: string[] = request.components.map((selectedComponent) => selectedComponent.id);
                            this.router.navigate(['/process-groups', processGroupId, 'bulk', ids.join(',')]);
                        }
                    } else {
                        const ids: string[] = request.components.map((selectedComponent) => selectedComponent.id);
                        ids.push(...selected);
                        this.router.navigate(['/process-groups', processGroupId, 'bulk', ids.join(',')]);
                    }
                })
            ),
        { dispatch: false }
    );

    removeSelectedComponents$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.removeSelectedComponents),
                map((action) => action.request),
                withLatestFrom(
                    this.store.select(selectCurrentProcessGroupId),
                    this.store.select(selectSelectedComponentIds)
                ),
                tap(([request, processGroupId, selected]) => {
                    if (selected.length === 0) {
                        this.router.navigate(['/process-groups', processGroupId]);
                    } else {
                        const idsToRemove: string[] = request.components.map(
                            (selectedComponent) => selectedComponent.id
                        );
                        const ids: string[] = selected.filter((id) => !idsToRemove.includes(id));
                        this.router.navigate(['/process-groups', processGroupId, 'bulk', ids.join(',')]);
                    }
                })
            ),
        { dispatch: false }
    );

    selectComponents$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.selectComponents),
                map((action) => action.request),
                withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
                tap(([request, processGroupId]) => {
                    if (request.components.length === 1) {
                        this.router.navigate([
                            '/process-groups',
                            processGroupId,
                            request.components[0].componentType,
                            request.components[0].id
                        ]);
                    } else if (request.components.length > 1) {
                        const ids: string[] = request.components.map((selectedComponent) => selectedComponent.id);
                        this.router.navigate(['/process-groups', processGroupId, 'bulk', ids.join(',')]);
                    }
                })
            ),
        { dispatch: false }
    );

    deselectAllComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.deselectAllComponents),
                withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
                tap(([action, processGroupId]) => {
                    this.router.navigate(['/process-groups', processGroupId]);
                })
            ),
        { dispatch: false }
    );
}
