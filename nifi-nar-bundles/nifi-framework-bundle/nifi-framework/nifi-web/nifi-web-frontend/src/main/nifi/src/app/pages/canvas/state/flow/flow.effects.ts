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
import {
    asyncScheduler,
    catchError,
    combineLatest,
    filter,
    from,
    interval,
    map,
    mergeMap,
    Observable,
    of,
    switchMap,
    take,
    takeUntil,
    tap,
    withLatestFrom
} from 'rxjs';
import {
    DeleteComponentResponse,
    LoadProcessGroupRequest,
    LoadProcessGroupResponse,
    Snippet,
    UpdateComponentFailure,
    UpdateComponentResponse,
    UpdateConnectionSuccess
} from './index';
import { Action, Store } from '@ngrx/store';
import {
    selectAnySelectedComponentIds,
    selectCurrentProcessGroupId,
    selectParentProcessGroupId,
    selectProcessGroup,
    selectProcessor,
    selectRemoteProcessGroup,
    selectSaving
} from './flow.selectors';
import { ConnectionManager } from '../../service/manager/connection-manager.service';
import { MatDialog } from '@angular/material/dialog';
import { CreatePort } from '../../ui/port/create-port/create-port.component';
import { EditPort } from '../../ui/port/edit-port/edit-port.component';
import { ComponentType, NewPropertyDialogRequest, NewPropertyDialogResponse, Property } from '../../../../state/shared';
import { Router } from '@angular/router';
import { Client } from '../../../../service/client.service';
import { CanvasUtils } from '../../service/canvas-utils.service';
import { CanvasView } from '../../service/canvas-view.service';
import { selectProcessorTypes } from '../../../../state/extension-types/extension-types.selectors';
import { NiFiState } from '../../../../state';
import { CreateProcessor } from '../../ui/processor/create-processor/create-processor.component';
import { EditProcessor } from '../../ui/processor/edit-processor/edit-processor.component';
import { NewPropertyDialog } from '../../../../ui/common/new-property-dialog/new-property-dialog.component';
import { BirdseyeView } from '../../service/birdseye-view.service';
import { CreateProcessGroup } from '../../ui/process-group/create-process-group/create-process-group.component';
import { CreateConnection } from '../../ui/connection/create-connection/create-connection.component';
import { EditConnectionComponent } from '../../ui/connection/edit-connection/edit-connection.component';
import { OkDialog } from '../../../../ui/common/ok-dialog/ok-dialog.component';
import { GroupComponents } from '../../ui/process-group/group-components/group-components.component';

@Injectable()
export class FlowEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private flowService: FlowService,
        private client: Client,
        private canvasUtils: CanvasUtils,
        private canvasView: CanvasView,
        private birdseyeView: BirdseyeView,
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
                            id: processGroupId,
                            transitionRequired: true
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
                this.canvasView.updateCanvasVisibility();
                this.birdseyeView.refresh();

                return of(FlowActions.setTransitionRequired({ transitionRequired: false }));
            })
        )
    );

    startProcessGroupPolling$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startProcessGroupPolling),
            switchMap(() =>
                interval(30000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(FlowActions.stopProcessGroupPolling)))
                )
            ),
            switchMap((request) => of(FlowActions.reloadFlow()))
        )
    );

    createComponentRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createComponentRequest),
            map((action) => action.request),
            switchMap((request) => {
                switch (request.type) {
                    case ComponentType.Processor:
                        return of(FlowActions.openNewProcessorDialog({ request }));
                    case ComponentType.ProcessGroup:
                        return from(this.flowService.getParameterContexts()).pipe(
                            map((response) =>
                                FlowActions.openNewProcessGroupDialog({
                                    request: {
                                        request,
                                        parameterContexts: response.parameterContexts
                                    }
                                })
                            ),
                            catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                        );
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

    openNewProcessorDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openNewProcessorDialog),
                map((action) => action.request),
                withLatestFrom(this.store.select(selectProcessorTypes)),
                tap(([request, processorTypes]) => {
                    this.dialog
                        .open(CreateProcessor, {
                            data: {
                                request,
                                processorTypes
                            },
                            panelClass: 'medium-dialog'
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                        });
                })
            ),
        { dispatch: false }
    );

    createProcessor$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createProcessor),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createProcessor(processGroupId, request)).pipe(
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

    openNewProcessGroupDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openNewProcessGroupDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog
                        .open(CreateProcessGroup, {
                            data: request,
                            panelClass: 'medium-dialog'
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                        });
                })
            ),
        { dispatch: false }
    );

    createProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createProcessGroup),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createProcessGroup(processGroupId, request)).pipe(
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

    uploadProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.uploadProcessGroup),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.uploadProcessGroup(processGroupId, request)).pipe(
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

    getParameterContextsAndOpenGroupComponentsDialog$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.getParameterContextsAndOpenGroupComponentsDialog),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, currentProcessGroupId]) =>
                from(this.flowService.getParameterContexts()).pipe(
                    map((response) =>
                        FlowActions.openGroupComponentsDialog({
                            request: {
                                request,
                                parameterContexts: response.parameterContexts
                            }
                        })
                    ),
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                )
            )
        )
    );

    openGroupComponentsDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openGroupComponentsDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog
                        .open(GroupComponents, {
                            data: request,
                            panelClass: 'medium-dialog'
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                        });
                })
            ),
        { dispatch: false }
    );

    groupComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.groupComponents),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createProcessGroup(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.groupComponentsSuccess({
                            response: {
                                type: request.type,
                                payload: response,
                                components: request.components
                            }
                        })
                    ),
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                )
            )
        )
    );

    groupComponentsSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.groupComponentsSuccess),
            map((action) => action.response),
            tap(() => this.dialog.closeAll()),
            switchMap((response) => [
                FlowActions.createComponentComplete({
                    response: {
                        type: response.type,
                        payload: response.payload
                    }
                }),
                FlowActions.moveComponents({
                    request: {
                        groupId: response.payload.id,
                        components: response.components
                    }
                })
            ])
        )
    );

    getDefaultsAndOpenNewConnectionDialog$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.getDefaultsAndOpenNewConnectionDialog),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, currentProcessGroupId]) =>
                from(this.flowService.getProcessGroup(currentProcessGroupId)).pipe(
                    map((response) =>
                        FlowActions.openNewConnectionDialog({
                            request: {
                                request,
                                defaults: {
                                    flowfileExpiration: response.component.defaultFlowFileExpiration,
                                    objectThreshold: response.component.defaultBackPressureObjectThreshold,
                                    dataSizeThreshold: response.component.defaultBackPressureDataSizeThreshold
                                }
                            }
                        })
                    ),
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                )
            )
        )
    );

    openNewConnectionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openNewConnectionDialog),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(CreateConnection, {
                        data: request,
                        panelClass: 'large-dialog'
                    });

                    dialogReference.componentInstance.getChildOutputPorts = (groupId: string): Observable<any> => {
                        return this.flowService.getFlow(groupId).pipe(
                            take(1),
                            map((response) => response.processGroupFlow.flow.outputPorts)
                        );
                    };

                    dialogReference.componentInstance.getChildInputPorts = (groupId: string): Observable<any> => {
                        return this.flowService.getFlow(groupId).pipe(
                            take(1),
                            map((response) => response.processGroupFlow.flow.inputPorts)
                        );
                    };

                    dialogReference.afterClosed().subscribe(() => {
                        this.canvasUtils.removeTempEdge();
                        this.store.dispatch(FlowActions.clearFlowApiError());
                    });
                })
            ),
        { dispatch: false }
    );

    createConnection$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createConnection),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createConnection(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: ComponentType.Connection,
                                payload: response
                            }
                        })
                    ),
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                )
            )
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
            switchMap((response) => {
                this.canvasView.updateCanvasVisibility();
                this.birdseyeView.refresh();

                const actions: any[] = [
                    FlowActions.selectComponents({
                        request: {
                            components: [
                                {
                                    id: response.payload.id,
                                    componentType: response.type
                                }
                            ]
                        }
                    })
                ];

                // if the component that was just created is a connection, reload the source and destination if necessary
                if (response.type == ComponentType.Connection) {
                    actions.push(FlowActions.loadComponentsForConnection({ connection: response.payload }));
                }

                return actions;
            })
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
                    case ComponentType.Processor:
                        return of(FlowActions.openEditProcessorDialog({ request }));
                    case ComponentType.Connection:
                        return of(FlowActions.openEditConnectionDialog({ request }));
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
                tap((request) => {
                    this.dialog
                        .open(EditPort, {
                            data: request,
                            panelClass: 'medium-dialog'
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(FlowActions.clearFlowApiError());
                            this.store.dispatch(
                                FlowActions.selectComponents({
                                    request: {
                                        components: [
                                            {
                                                id: request.entity.id,
                                                componentType: request.type
                                            }
                                        ]
                                    }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    openEditProcessorDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openEditProcessorDialog),
                map((action) => action.request),
                tap((request) => {
                    const editDialogReference = this.dialog.open(EditProcessor, {
                        data: request,
                        panelClass: 'large-dialog'
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.createNewProperty = (
                        existingProperties: string[],
                        allowsSensitive: boolean
                    ): Observable<Property> => {
                        const dialogRequest: NewPropertyDialogRequest = { existingProperties, allowsSensitive };
                        const newPropertyDialogReference = this.dialog.open(NewPropertyDialog, {
                            data: dialogRequest,
                            panelClass: 'small-dialog'
                        });

                        return newPropertyDialogReference.componentInstance.newProperty.pipe(
                            take(1),
                            switchMap((dialogResponse: NewPropertyDialogResponse) => {
                                return this.flowService
                                    .getPropertyDescriptor(
                                        request.entity.id,
                                        dialogResponse.name,
                                        dialogResponse.sensitive
                                    )
                                    .pipe(
                                        take(1),
                                        map((response) => {
                                            newPropertyDialogReference.close();

                                            return {
                                                property: dialogResponse.name,
                                                value: null,
                                                descriptor: response.propertyDescriptor
                                            };
                                        })
                                    );
                            })
                        );
                    };

                    editDialogReference.componentInstance.getServiceLink = (serviceId: string) => {
                        return this.flowService.getControllerService(serviceId).pipe(
                            take(1),
                            map((serviceEntity) => {
                                // TODO - finalize once route is defined
                                return [
                                    '/process-groups',
                                    serviceEntity.component.parentGroupId,
                                    'controller-services',
                                    serviceEntity.id
                                ];
                            })
                        );
                    };

                    // TODO - inline service creation...

                    editDialogReference.componentInstance.editProcessor.pipe(take(1)).subscribe((payload: any) => {
                        this.store.dispatch(
                            FlowActions.updateProcessor({
                                request: {
                                    id: request.entity.id,
                                    uri: request.uri,
                                    type: request.type,
                                    payload
                                }
                            })
                        );
                    });

                    editDialogReference.afterClosed().subscribe(() => {
                        this.store.dispatch(FlowActions.clearFlowApiError());
                        this.store.dispatch(
                            FlowActions.selectComponents({
                                request: {
                                    components: [
                                        {
                                            id: request.entity.id,
                                            componentType: request.type
                                        }
                                    ]
                                }
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    openEditConnectionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openEditConnectionDialog),
                map((action) => action.request),
                tap((request) => {
                    const editDialogReference = this.dialog.open(EditConnectionComponent, {
                        data: request,
                        panelClass: 'large-dialog'
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.getChildOutputPorts = (groupId: string): Observable<any> => {
                        return this.flowService.getFlow(groupId).pipe(
                            take(1),
                            map((response) => response.processGroupFlow.flow.outputPorts)
                        );
                    };
                    editDialogReference.componentInstance.getChildInputPorts = (groupId: string): Observable<any> => {
                        return this.flowService.getFlow(groupId).pipe(
                            take(1),
                            map((response) => response.processGroupFlow.flow.inputPorts)
                        );
                    };

                    editDialogReference.componentInstance.selectProcessor = (id: string) => {
                        return this.store.select(selectProcessor(id));
                    };
                    editDialogReference.componentInstance.selectProcessGroup = (id: string) => {
                        return this.store.select(selectProcessGroup(id));
                    };
                    editDialogReference.componentInstance.selectRemoteProcessGroup = (id: string) => {
                        return this.store.select(selectRemoteProcessGroup(id));
                    };

                    editDialogReference.afterClosed().subscribe((response) => {
                        if (response == 'CANCELLED') {
                            this.connectionManager.renderConnection(request.entity.id, {
                                updatePath: true,
                                updateLabel: false
                            });
                        }

                        this.store.dispatch(FlowActions.clearFlowApiError());
                        this.store.dispatch(
                            FlowActions.selectComponents({
                                request: {
                                    components: [
                                        {
                                            id: request.entity.id,
                                            componentType: request.type
                                        }
                                    ]
                                }
                            })
                        );
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

    updateProcessor$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updateProcessor),
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
                        return FlowActions.updateProcessorSuccess({ response: updateComponentResponse });
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

    updateProcessorSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updateProcessorSuccess),
            tap(() => {
                this.dialog.closeAll();
            }),
            map((action) => action.response),
            switchMap((response) => of(FlowActions.loadConnectionsForComponent({ id: response.id })))
        )
    );

    loadConnectionsForComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadConnectionsForComponent),
            map((action) => action.id),
            switchMap((id: string) => {
                const componentConnections: any[] = this.canvasUtils.getComponentConnections(id);
                return componentConnections.map((componentConnection) =>
                    FlowActions.loadConnection({ id: componentConnection.id })
                );
            })
        )
    );

    loadConnection$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadConnection),
            map((action) => action.id),
            mergeMap((id) =>
                from(this.flowService.getConnection(id)).pipe(
                    map((response) => {
                        return FlowActions.loadConnectionSuccess({
                            response: {
                                id: id,
                                connection: response
                            }
                        });
                    }),
                    catchError((error) => of(FlowActions.flowApiError({ error })))
                )
            )
        )
    );

    loadComponentsForConnection$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadComponentsForConnection),
            map((action) => action.connection),
            switchMap((connection: any) => {
                const actions: any[] = [];

                const processTerminal = function (type: ComponentType | null, terminal: any) {
                    switch (type) {
                        case ComponentType.Processor:
                            actions.push(FlowActions.loadProcessor({ id: terminal.id }));
                            break;
                        case ComponentType.InputPort:
                            actions.push(FlowActions.loadInputPort({ id: terminal.id }));
                            break;
                        case ComponentType.RemoteProcessGroup:
                            actions.push(FlowActions.loadRemoteProcessGroup({ id: terminal.groupId }));
                            break;
                    }
                };

                processTerminal(
                    this.canvasUtils.getComponentTypeForSource(connection.component.source.type),
                    connection.component.source
                );
                processTerminal(
                    this.canvasUtils.getComponentTypeForDestination(connection.component.destination.type),
                    connection.component.destination
                );

                return actions;
            })
        )
    );

    loadProcessor$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadProcessor),
            map((action) => action.id),
            mergeMap((id) =>
                from(this.flowService.getProcessor(id)).pipe(
                    map((response) => {
                        return FlowActions.loadProcessorSuccess({
                            response: {
                                id: id,
                                processor: response
                            }
                        });
                    }),
                    catchError((error) => of(FlowActions.flowApiError({ error })))
                )
            )
        )
    );

    loadInputPort$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadInputPort),
            map((action) => action.id),
            mergeMap((id) =>
                from(this.flowService.getInputPort(id)).pipe(
                    map((response) => {
                        return FlowActions.loadInputPortSuccess({
                            response: {
                                id: id,
                                inputPort: response
                            }
                        });
                    }),
                    catchError((error) => of(FlowActions.flowApiError({ error })))
                )
            )
        )
    );

    loadRemoteProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadRemoteProcessGroup),
            map((action) => action.id),
            mergeMap((id) =>
                from(this.flowService.getRemoteProcessGroup(id)).pipe(
                    map((response) => {
                        return FlowActions.loadRemoteProcessGroupSuccess({
                            response: {
                                id: id,
                                remoteProcessGroup: response
                            }
                        });
                    }),
                    catchError((error) => of(FlowActions.flowApiError({ error })))
                )
            )
        )
    );

    updateConnection$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updateConnection),
            map((action) => action.request),
            mergeMap((request) => {
                return from(this.flowService.updateComponent(request)).pipe(
                    map((response) => {
                        const updateComponentResponse: UpdateConnectionSuccess = {
                            requestId: request.requestId,
                            id: request.id,
                            type: request.type,
                            response: response,
                            previousDestination: request.previousDestination
                        };
                        return FlowActions.updateConnectionSuccess({ response: updateComponentResponse });
                    }),
                    catchError((error) => {
                        this.connectionManager.renderConnection(request.id, {
                            updatePath: true,
                            updateLabel: false
                        });

                        const updateComponentFailure: UpdateComponentFailure = {
                            id: request.id,
                            type: request.type,
                            restoreOnFailure: request.restoreOnFailure,
                            error: error.error
                        };
                        return of(FlowActions.updateComponentFailure({ response: updateComponentFailure }));
                    })
                );
            })
        )
    );

    updateConnectionSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updateConnectionSuccess),
            tap(() => {
                this.dialog.closeAll();
            }),
            map((action) => action.response),
            switchMap((response) => {
                const actions: Action[] = [FlowActions.loadComponentsForConnection({ connection: response.response })];

                if (response.previousDestination) {
                    const type = this.canvasUtils.getComponentTypeForDestination(response.previousDestination.type);
                    switch (type) {
                        case ComponentType.Processor:
                            actions.push(FlowActions.loadProcessor({ id: response.previousDestination.id }));
                            break;
                        case ComponentType.InputPort:
                            actions.push(FlowActions.loadInputPort({ id: response.previousDestination.id }));
                            break;
                        case ComponentType.RemoteProcessGroup:
                            actions.push(
                                FlowActions.loadRemoteProcessGroup({ id: response.previousDestination.groupId })
                            );
                            break;
                    }
                }

                return actions;
            })
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

    updatePositionComplete$ = createEffect(
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

    moveComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.moveComponents),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            mergeMap(([request, processGroupId]) => {
                const components: any[] = request.components;

                const snippet: Snippet = components.reduce(
                    (snippet, component) => {
                        switch (component.type) {
                            case ComponentType.Processor:
                                snippet.processors[component.id] = this.client.getRevision(component.entity);
                                break;
                            case ComponentType.InputPort:
                                snippet.inputPorts[component.id] = this.client.getRevision(component.entity);
                                break;
                            case ComponentType.OutputPort:
                                snippet.outputPorts[component.id] = this.client.getRevision(component.entity);
                                break;
                            case ComponentType.ProcessGroup:
                                snippet.processGroups[component.id] = this.client.getRevision(component.entity);
                                break;
                            case ComponentType.RemoteProcessGroup:
                                snippet.remoteProcessGroups[component.id] = this.client.getRevision(component.entity);
                                break;
                            case ComponentType.Funnel:
                                snippet.funnels[component.id] = this.client.getRevision(component.entity);
                                break;
                            case ComponentType.Label:
                                snippet.labels[component.id] = this.client.getRevision(component.entity);
                                break;
                            case ComponentType.Connection:
                                snippet.connections[component.id] = this.client.getRevision(component.entity);
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
                    switchMap((response) => this.flowService.moveSnippet(response.snippet.id, request.groupId)),
                    map((response) => {
                        const deleteResponses: DeleteComponentResponse[] = [];

                        // prepare the delete responses with all requested components that are now deleted
                        components.forEach((request) => {
                            deleteResponses.push({
                                id: request.id,
                                type: request.type
                            });
                        });

                        return FlowActions.deleteComponentsSuccess({
                            response: deleteResponses
                        });
                    }),
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                );
            })
        )
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

    deleteComponentsSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.deleteComponentsSuccess),
                tap(() => {
                    this.birdseyeView.refresh();
                })
            ),
        { dispatch: false }
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

    addSelectedComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.addSelectedComponents),
            map((action) => action.request),
            withLatestFrom(
                this.store.select(selectCurrentProcessGroupId),
                this.store.select(selectAnySelectedComponentIds)
            ),
            switchMap(([request, processGroupId, selected]) => {
                let commands: string[] = [];
                if (selected.length === 0) {
                    if (request.components.length === 1) {
                        commands = [
                            '/process-groups',
                            processGroupId,
                            request.components[0].componentType,
                            request.components[0].id
                        ];
                    } else if (request.components.length > 1) {
                        const ids: string[] = request.components.map((selectedComponent) => selectedComponent.id);
                        commands = ['/process-groups', processGroupId, 'bulk', ids.join(',')];
                    }
                } else {
                    const ids: string[] = request.components.map((selectedComponent) => selectedComponent.id);
                    ids.push(...selected);
                    commands = ['/process-groups', processGroupId, 'bulk', ids.join(',')];
                }
                return of(FlowActions.navigateWithoutTransform({ url: commands }));
            })
        )
    );

    removeSelectedComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.removeSelectedComponents),
            map((action) => action.request),
            withLatestFrom(
                this.store.select(selectCurrentProcessGroupId),
                this.store.select(selectAnySelectedComponentIds)
            ),
            switchMap(([request, processGroupId, selected]) => {
                let commands: string[];
                if (selected.length === 0) {
                    commands = ['/process-groups', processGroupId];
                } else {
                    const idsToRemove: string[] = request.components.map((selectedComponent) => selectedComponent.id);
                    const ids: string[] = selected.filter((id) => !idsToRemove.includes(id));
                    commands = ['/process-groups', processGroupId, 'bulk', ids.join(',')];
                }
                return of(FlowActions.navigateWithoutTransform({ url: commands }));
            })
        )
    );

    selectComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.selectComponents),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) => {
                let commands: string[] = [];
                if (request.components.length === 1) {
                    commands = [
                        '/process-groups',
                        processGroupId,
                        request.components[0].componentType,
                        request.components[0].id
                    ];
                } else if (request.components.length > 1) {
                    const ids: string[] = request.components.map((selectedComponent) => selectedComponent.id);
                    commands = ['/process-groups', processGroupId, 'bulk', ids.join(',')];
                }
                return of(FlowActions.navigateWithoutTransform({ url: commands }));
            })
        )
    );

    deselectAllComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.deselectAllComponents),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([action, processGroupId]) => {
                return of(FlowActions.navigateWithoutTransform({ url: ['/process-groups', processGroupId] }));
            })
        )
    );

    navigateToComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToComponent),
                map((action) => action.request),
                withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
                tap(([request, processGroupId]) => {
                    this.router.navigate(['/process-groups', processGroupId, request.type, request.id]);
                })
            ),
        { dispatch: false }
    );

    navigateWithoutTransform$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateWithoutTransform),
                map((action) => action.url),
                tap((url) => {
                    this.router.navigate(url);
                })
            ),
        { dispatch: false }
    );

    centerSelectedComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.centerSelectedComponent),
                tap(() => {
                    this.canvasView.centerSelectedComponent();
                })
            ),
        { dispatch: false }
    );

    showOkDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.showOkDialog),
                tap((request) => {
                    this.dialog.open(OkDialog, {
                        data: {
                            title: request.title,
                            message: request.message
                        },
                        panelClass: 'medium-dialog'
                    });
                })
            ),
        { dispatch: false }
    );
}
