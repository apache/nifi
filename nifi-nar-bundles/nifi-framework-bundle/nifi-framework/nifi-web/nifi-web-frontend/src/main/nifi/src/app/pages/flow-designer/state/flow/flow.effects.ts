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
import { Actions, concatLatestFrom, createEffect, ofType } from '@ngrx/effects';
import * as FlowActions from './flow.actions';
import * as StatusHistoryActions from '../../../../state/status-history/status-history.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import {
    asyncScheduler,
    catchError,
    combineLatest,
    filter,
    from,
    interval,
    map,
    mergeMap,
    NEVER,
    Observable,
    of,
    switchMap,
    take,
    takeUntil,
    tap
} from 'rxjs';
import {
    CreateProcessGroupDialogRequest,
    DeleteComponentResponse,
    GroupComponentsDialogRequest,
    ImportFromRegistryDialogRequest,
    LoadProcessGroupRequest,
    LoadProcessGroupResponse,
    SaveVersionDialogRequest,
    SaveVersionRequest,
    Snippet,
    StopVersionControlRequest,
    StopVersionControlResponse,
    UpdateComponentFailure,
    UpdateComponentResponse,
    UpdateConnectionSuccess,
    UpdateProcessorRequest,
    VersionControlInformationEntity
} from './index';
import { Action, Store } from '@ngrx/store';
import {
    selectAnySelectedComponentIds,
    selectChangeVersionRequest,
    selectCurrentParameterContext,
    selectCurrentProcessGroupId,
    selectParentProcessGroupId,
    selectProcessGroup,
    selectProcessor,
    selectRefreshRpgDetails,
    selectRemoteProcessGroup,
    selectSaving,
    selectVersionSaving
} from './flow.selectors';
import { ConnectionManager } from '../../service/manager/connection-manager.service';
import { MatDialog } from '@angular/material/dialog';
import { CreatePort } from '../../ui/canvas/items/port/create-port/create-port.component';
import { EditPort } from '../../ui/canvas/items/port/edit-port/edit-port.component';
import {
    BucketEntity,
    ComponentType,
    isDefinedAndNotNull,
    RegistryClientEntity,
    VersionedFlowEntity,
    VersionedFlowSnapshotMetadataEntity
} from '../../../../state/shared';
import { Router } from '@angular/router';
import { Client } from '../../../../service/client.service';
import { CanvasUtils } from '../../service/canvas-utils.service';
import { CanvasView } from '../../service/canvas-view.service';
import { selectProcessorTypes } from '../../../../state/extension-types/extension-types.selectors';
import { NiFiState } from '../../../../state';
import { CreateProcessor } from '../../ui/canvas/items/processor/create-processor/create-processor.component';
import { EditProcessor } from '../../ui/canvas/items/processor/edit-processor/edit-processor.component';
import { BirdseyeView } from '../../service/birdseye-view.service';
import { CreateRemoteProcessGroup } from '../../ui/canvas/items/remote-process-group/create-remote-process-group/create-remote-process-group.component';
import { CreateProcessGroup } from '../../ui/canvas/items/process-group/create-process-group/create-process-group.component';
import { CreateConnection } from '../../ui/canvas/items/connection/create-connection/create-connection.component';
import { EditConnectionComponent } from '../../ui/canvas/items/connection/edit-connection/edit-connection.component';
import { OkDialog } from '../../../../ui/common/ok-dialog/ok-dialog.component';
import { GroupComponents } from '../../ui/canvas/items/process-group/group-components/group-components.component';
import { EditProcessGroup } from '../../ui/canvas/items/process-group/edit-process-group/edit-process-group.component';
import { ControllerServiceService } from '../../service/controller-service.service';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { ParameterHelperService } from '../../service/parameter-helper.service';
import { RegistryService } from '../../service/registry.service';
import { ImportFromRegistry } from '../../ui/canvas/items/flow/import-from-registry/import-from-registry.component';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { NoRegistryClientsDialog } from '../../ui/common/no-registry-clients-dialog/no-registry-clients-dialog.component';
import { EditRemoteProcessGroup } from '../../ui/canvas/items/remote-process-group/edit-remote-process-group/edit-remote-process-group.component';
import { LARGE_DIALOG, MEDIUM_DIALOG, SMALL_DIALOG, XL_DIALOG } from '../../../../index';
import { HttpErrorResponse } from '@angular/common/http';
import { SaveVersionDialog } from '../../ui/canvas/items/flow/save-version-dialog/save-version-dialog.component';
import { ChangeVersionDialog } from '../../ui/canvas/items/flow/change-version-dialog/change-version-dialog';
import { ChangeVersionProgressDialog } from '../../ui/canvas/items/flow/change-version-progress-dialog/change-version-progress-dialog';
import { LocalChangesDialog } from '../../ui/canvas/items/flow/local-changes-dialog/local-changes-dialog';

@Injectable()
export class FlowEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private flowService: FlowService,
        private controllerServiceService: ControllerServiceService,
        private registryService: RegistryService,
        private client: Client,
        private canvasUtils: CanvasUtils,
        private canvasView: CanvasView,
        private birdseyeView: BirdseyeView,
        private connectionManager: ConnectionManager,
        private router: Router,
        private dialog: MatDialog,
        private propertyTableHelperService: PropertyTableHelperService,
        private parameterHelperService: ParameterHelperService
    ) {}

    reloadFlow$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.reloadFlow),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([, processGroupId]) => {
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
                    this.flowService.getControllerBulletins()
                ]).pipe(
                    map(([flow, flowStatus, controllerBulletins]) => {
                        return FlowActions.loadProcessGroupSuccess({
                            response: {
                                id: request.id,
                                flow: flow,
                                flowStatus: flowStatus,
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
            switchMap(() => of(FlowActions.reloadFlow()))
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
                            concatLatestFrom(() => this.store.select(selectCurrentParameterContext)),
                            map(([response, parameterContext]) => {
                                const dialogRequest: CreateProcessGroupDialogRequest = {
                                    request,
                                    parameterContexts: response.parameterContexts
                                };

                                if (parameterContext) {
                                    dialogRequest.currentParameterContextId = parameterContext.id;
                                }

                                return FlowActions.openNewProcessGroupDialog({ request: dialogRequest });
                            }),
                            catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                        );
                    case ComponentType.RemoteProcessGroup:
                        return of(FlowActions.openNewRemoteProcessGroupDialog({ request }));
                    case ComponentType.Funnel:
                        return of(FlowActions.createFunnel({ request }));
                    case ComponentType.Label:
                        return of(FlowActions.createLabel({ request }));
                    case ComponentType.InputPort:
                    case ComponentType.OutputPort:
                        return of(FlowActions.openNewPortDialog({ request }));
                    case ComponentType.Flow:
                        return from(this.registryService.getRegistryClients()).pipe(
                            map((response) => {
                                const dialogRequest: ImportFromRegistryDialogRequest = {
                                    request,
                                    registryClients: response.registries
                                };

                                return FlowActions.openImportFromRegistryDialog({ request: dialogRequest });
                            }),
                            catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                        );
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
                concatLatestFrom(() => this.store.select(selectProcessorTypes)),
                tap(([request, processorTypes]) => {
                    this.dialog
                        .open(CreateProcessor, {
                            ...LARGE_DIALOG,
                            data: {
                                request,
                                processorTypes
                            }
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
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
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

    openNewRemoteProcessGroupDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openNewRemoteProcessGroupDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog
                        .open(CreateRemoteProcessGroup, {
                            ...LARGE_DIALOG,
                            data: request
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                        });
                })
            ),
        { dispatch: false }
    );

    createRemoteProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createRemoteProcessGroup),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createRemoteProcessGroup(processGroupId, request)).pipe(
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

    goToRemoteProcessGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.goToRemoteProcessGroup),
                map((action) => action.request),
                tap((request) => {
                    if (request.uri) {
                        this.flowService.goToRemoteProcessGroup(request);
                    } else {
                        this.store.dispatch(
                            FlowActions.showOkDialog({
                                title: 'Remote Process Group',
                                message: 'No target URI defined.'
                            })
                        );
                    }
                })
            ),
        { dispatch: false }
    );

    startRemoteProcessGroupPolling$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startRemoteProcessGroupPolling),
            switchMap(() => {
                return interval(3000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(FlowActions.stopRemoteProcessGroupPolling)))
                );
            }),
            switchMap(() => {
                return of(FlowActions.refreshRemoteProcessGroup());
            })
        )
    );

    requestRefreshRemoteProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.requestRefreshRemoteProcessGroup),
            switchMap(() => {
                return of(FlowActions.refreshRemoteProcessGroup());
            })
        )
    );

    refreshRemoteProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.refreshRemoteProcessGroup),
            concatLatestFrom(() => this.store.select(selectRefreshRpgDetails).pipe(isDefinedAndNotNull())),
            switchMap(([, refreshRpgDetails]) =>
                from(
                    this.flowService.getRemoteProcessGroup(refreshRpgDetails.request.id).pipe(
                        map((response: any) => {
                            const entity = response;

                            if (refreshRpgDetails.request.refreshTimestamp !== entity.component.flowRefreshed) {
                                this.store.dispatch(FlowActions.stopRemoteProcessGroupPolling());

                                // reload the group's connections
                                this.store.dispatch(FlowActions.loadConnectionsForComponent({ id: entity.id }));
                            } else {
                                if (!refreshRpgDetails.polling) {
                                    this.store.dispatch(FlowActions.startRemoteProcessGroupPolling());
                                }

                                entity.component.flowRefreshed = 'Refreshing...';
                            }

                            return FlowActions.loadRemoteProcessGroupSuccess({
                                response: {
                                    id: entity.id,
                                    remoteProcessGroup: entity
                                }
                            });
                        }),
                        catchError((error) => {
                            this.store.dispatch(FlowActions.stopRemoteProcessGroupPolling());

                            return of(FlowActions.flowApiError({ error: error.error }));
                        })
                    )
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
                            ...MEDIUM_DIALOG,
                            data: request
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
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
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
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
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
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request]) =>
                from(this.flowService.getParameterContexts()).pipe(
                    concatLatestFrom(() => this.store.select(selectCurrentParameterContext)),
                    map(([response, parameterContext]) => {
                        const dialogRequest: GroupComponentsDialogRequest = {
                            request,
                            parameterContexts: response.parameterContexts
                        };

                        if (parameterContext) {
                            dialogRequest.currentParameterContextId = parameterContext.id;
                        }

                        return FlowActions.openGroupComponentsDialog({
                            request: dialogRequest
                        });
                    }),
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
                            ...MEDIUM_DIALOG,
                            data: request
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
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
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
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
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
                        ...LARGE_DIALOG,
                        data: request
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
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
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
                            ...SMALL_DIALOG,
                            data: request
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
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
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
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
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
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
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

    openImportFromRegistryDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openImportFromRegistryDialog),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentUser)),
                tap(([request, currentUser]) => {
                    const someRegistries = request.registryClients.some(
                        (registryClient: RegistryClientEntity) => registryClient.permissions.canRead
                    );

                    if (someRegistries) {
                        const dialogReference = this.dialog.open(ImportFromRegistry, {
                            ...LARGE_DIALOG,
                            data: request
                        });

                        dialogReference.componentInstance.getBuckets = (
                            registryId: string
                        ): Observable<BucketEntity[]> => {
                            return this.registryService.getBuckets(registryId).pipe(
                                take(1),
                                map((response) => response.buckets)
                            );
                        };

                        dialogReference.componentInstance.getFlows = (
                            registryId: string,
                            bucketId: string
                        ): Observable<VersionedFlowEntity[]> => {
                            return this.registryService.getFlows(registryId, bucketId).pipe(
                                take(1),
                                map((response) => response.versionedFlows)
                            );
                        };

                        dialogReference.componentInstance.getFlowVersions = (
                            registryId: string,
                            bucketId: string,
                            flowId: string
                        ): Observable<VersionedFlowSnapshotMetadataEntity[]> => {
                            return this.registryService.getFlowVersions(registryId, bucketId, flowId).pipe(
                                take(1),
                                map((response) => response.versionedFlowSnapshotMetadataSet)
                            );
                        };

                        dialogReference.afterClosed().subscribe(() => {
                            this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                        });
                    } else {
                        this.dialog
                            .open(NoRegistryClientsDialog, {
                                ...MEDIUM_DIALOG,
                                data: {
                                    controllerPermissions: currentUser.controllerPermissions
                                }
                            })
                            .afterClosed()
                            .subscribe(() => {
                                this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                            });
                    }
                })
            ),
        { dispatch: false }
    );

    importFromRegistry$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.importFromRegistry),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.registryService.importFromRegistry(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: ComponentType.ProcessGroup,
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
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([request, processGroupId]) => {
                    const url = ['/process-groups', processGroupId, request.type, request.id, 'edit'];
                    if (this.canvasView.isSelectedComponentOnScreen()) {
                        this.store.dispatch(FlowActions.navigateWithoutTransform({ url }));
                    } else {
                        this.router.navigate(url);
                    }
                })
            ),
        { dispatch: false }
    );

    navigateToAdvancedProcessorUi$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToAdvancedProcessorUi),
                map((action) => action.id),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([id, processGroupId]) => {
                    this.router.navigate(['/process-groups', processGroupId, ComponentType.Processor, id, 'advanced']);
                })
            ),
        { dispatch: false }
    );

    navigateToEditCurrentProcessGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToEditCurrentProcessGroup),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([, processGroupId]) => {
                    this.router.navigate(['/process-groups', processGroupId, 'edit']);
                })
            ),
        { dispatch: false }
    );

    navigateToManageComponentPolicies$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToManageComponentPolicies),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/access-policies', 'read', 'component', request.resource, request.id]);
                })
            ),
        { dispatch: false }
    );

    navigateToQueueListing$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToQueueListing),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/queue', request.connectionId]);
                })
            ),
        { dispatch: false }
    );

    navigateToViewStatusHistoryForComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToViewStatusHistoryForComponent),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([request, currentProcessGroupId]) => {
                    this.router.navigate([
                        '/process-groups',
                        currentProcessGroupId,
                        request.type,
                        request.id,
                        'history'
                    ]);
                })
            ),
        { dispatch: false }
    );

    completeStatusHistoryForComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(StatusHistoryActions.viewStatusHistoryComplete),
                map((action) => action.request),
                filter((request) => request.source === 'canvas'),
                tap((request) => {
                    this.store.dispatch(
                        FlowActions.selectComponents({
                            request: {
                                components: [
                                    {
                                        id: request.componentId,
                                        componentType: request.componentType
                                    }
                                ]
                            }
                        })
                    );
                })
            ),
        { dispatch: false }
    );

    navigateToControllerServicesForProcessGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToControllerServicesForProcessGroup),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/process-groups', request.id, 'controller-services']);
                })
            ),
        { dispatch: false }
    );

    editComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.editComponent),
            map((action) => action.request),
            switchMap((request) => {
                switch (request.type) {
                    case ComponentType.Processor:
                        return of(FlowActions.openEditProcessorDialog({ request }));
                    case ComponentType.Connection:
                        return of(FlowActions.openEditConnectionDialog({ request }));
                    case ComponentType.ProcessGroup:
                        return of(FlowActions.openEditProcessGroupDialog({ request }));
                    case ComponentType.RemoteProcessGroup:
                        return of(FlowActions.openEditRemoteProcessGroupDialog({ request }));
                    case ComponentType.InputPort:
                    case ComponentType.OutputPort:
                        return of(FlowActions.openEditPortDialog({ request }));
                    default:
                        return of(FlowActions.flowApiError({ error: 'Unsupported type of Component.' }));
                }
            })
        )
    );

    editCurrentProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.editCurrentProcessGroup),
            map((action) => action.request),
            switchMap((request) =>
                from(this.flowService.getProcessGroup(request.id)).pipe(
                    map((response) =>
                        FlowActions.openEditProcessGroupDialog({
                            request: {
                                type: ComponentType.ProcessGroup,
                                uri: response.uri,
                                entity: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            FlowActions.flowApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
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
                            ...MEDIUM_DIALOG,
                            data: request
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
                switchMap((request) =>
                    from(this.flowService.getProcessor(request.entity.id)).pipe(
                        map((entity) => {
                            return {
                                ...request,
                                entity
                            };
                        }),
                        tap({
                            error: (errorResponse: HttpErrorResponse) => {
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
                                this.store.dispatch(ErrorActions.snackBarError({ error: errorResponse.error }));
                            }
                        })
                    )
                ),
                concatLatestFrom(() => [
                    this.store.select(selectCurrentParameterContext),
                    this.store.select(selectCurrentProcessGroupId)
                ]),
                tap(([request, parameterContext, processGroupId]) => {
                    const processorId: string = request.entity.id;

                    const editDialogReference = this.dialog.open(EditProcessor, {
                        ...LARGE_DIALOG,
                        data: request,
                        id: processorId
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.createNewProperty =
                        this.propertyTableHelperService.createNewProperty(processorId, this.flowService);

                    const goTo = (commands: string[], destination: string): void => {
                        if (editDialogReference.componentInstance.editProcessorForm.dirty) {
                            const saveChangesDialogReference = this.dialog.open(YesNoDialog, {
                                ...SMALL_DIALOG,
                                data: {
                                    title: 'Processor Configuration',
                                    message: `Save changes before going to this ${destination}?`
                                }
                            });

                            saveChangesDialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                                editDialogReference.componentInstance.submitForm(commands);
                            });

                            saveChangesDialogReference.componentInstance.no.pipe(take(1)).subscribe(() => {
                                editDialogReference.close('ROUTED');
                                this.router.navigate(commands);
                            });
                        } else {
                            editDialogReference.close('ROUTED');
                            this.router.navigate(commands);
                        }
                    };

                    if (parameterContext != null) {
                        editDialogReference.componentInstance.getParameters = this.parameterHelperService.getParameters(
                            parameterContext.id
                        );

                        editDialogReference.componentInstance.parameterContext = parameterContext;
                        editDialogReference.componentInstance.goToParameter = () => {
                            const commands: string[] = ['/parameter-contexts', parameterContext.id];
                            goTo(commands, 'Parameter');
                        };

                        editDialogReference.componentInstance.convertToParameter =
                            this.parameterHelperService.convertToParameter(parameterContext.id);
                    }

                    editDialogReference.componentInstance.goToService = (serviceId: string) => {
                        this.controllerServiceService.getControllerService(serviceId).subscribe({
                            next: (serviceEntity) => {
                                const commands: string[] = [
                                    '/process-groups',
                                    serviceEntity.component.parentGroupId,
                                    'controller-services',
                                    serviceEntity.id
                                ];
                                goTo(commands, 'Controller Service');
                            },
                            error: () => {
                                // TODO - handle error
                            }
                        });
                    };

                    editDialogReference.componentInstance.createNewService =
                        this.propertyTableHelperService.createNewService(
                            processorId,
                            this.controllerServiceService,
                            this.flowService,
                            processGroupId
                        );

                    editDialogReference.componentInstance.editProcessor
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((updateProcessorRequest: UpdateProcessorRequest) => {
                            this.store.dispatch(
                                FlowActions.updateProcessor({
                                    request: {
                                        id: processorId,
                                        uri: request.uri,
                                        type: request.type,
                                        payload: updateProcessorRequest.payload,
                                        postUpdateNavigation: updateProcessorRequest.postUpdateNavigation
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        this.store.dispatch(FlowActions.clearFlowApiError());

                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                FlowActions.selectComponents({
                                    request: {
                                        components: [
                                            {
                                                id: processorId,
                                                componentType: request.type
                                            }
                                        ]
                                    }
                                })
                            );
                        }
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
                        ...LARGE_DIALOG,
                        data: request
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

    openEditProcessGroupDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openEditProcessGroupDialog),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                switchMap(([request, currentProcessGroupId]) =>
                    this.flowService.getParameterContexts().pipe(
                        take(1),
                        map((response) => [request, response.parameterContexts, currentProcessGroupId])
                    )
                ),
                tap(([request, parameterContexts, currentProcessGroupId]) => {
                    const editDialogReference = this.dialog.open(EditProcessGroup, {
                        ...LARGE_DIALOG,
                        data: request
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);
                    editDialogReference.componentInstance.parameterContexts = parameterContexts;

                    editDialogReference.componentInstance.editProcessGroup
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((payload: any) => {
                            this.store.dispatch(
                                FlowActions.updateComponent({
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
                        if (request.entity.id === currentProcessGroupId) {
                            this.store.dispatch(
                                FlowActions.enterProcessGroup({
                                    request: {
                                        id: currentProcessGroupId
                                    }
                                })
                            );
                        } else {
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
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    navigateToManageRemotePorts$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToManageRemotePorts),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/remote-process-group', request.id, 'manage-remote-ports']);
                })
            ),
        { dispatch: false }
    );

    openEditRemoteProcessGroupDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openEditRemoteProcessGroupDialog),
                map((action) => action.request),
                tap((request) => {
                    const editDialogReference = this.dialog.open(EditRemoteProcessGroup, {
                        ...LARGE_DIALOG,
                        data: request
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.editRemoteProcessGroup
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((payload: any) => {
                            this.store.dispatch(
                                FlowActions.updateComponent({
                                    request: {
                                        id: request.entity.id,
                                        uri: request.uri,
                                        type: request.type,
                                        payload
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        this.store.dispatch(FlowActions.clearFlowApiError());

                        if (response != 'ROUTED') {
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
                        }
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
                            postUpdateNavigation: request.postUpdateNavigation,
                            response
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
                map((action) => action.response),
                tap((response) => {
                    if (response.postUpdateNavigation) {
                        this.router.navigate(response.postUpdateNavigation);
                        this.dialog.getDialogById(response.id)?.close('ROUTED');
                    } else {
                        this.dialog.closeAll();
                    }
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
                            postUpdateNavigation: request.postUpdateNavigation,
                            response
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
            map((action) => action.response),
            tap((response) => {
                if (response.postUpdateNavigation) {
                    this.router.navigate(response.postUpdateNavigation);
                    this.dialog.getDialogById(response.id)?.close('ROUTED');
                } else {
                    this.dialog.closeAll();
                }
            }),
            filter((response) => response.postUpdateNavigation == null),
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

    updatePositionComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updatePositionComplete),
            map((action) => action.response),
            switchMap((response) =>
                of(FlowActions.renderConnectionsForComponent({ id: response.id, updatePath: true, updateLabel: false }))
            )
        )
    );

    renderConnectionsForComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.renderConnectionsForComponent),
                tap((action) => {
                    this.connectionManager.renderConnectionsForComponent(action.id, {
                        updatePath: action.updatePath,
                        updateLabel: action.updateLabel
                    });
                })
            ),
        { dispatch: false }
    );

    moveComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.moveComponents),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
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
                    map(() => {
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
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            mergeMap(([requests, processGroupId]) => {
                if (requests.length === 1) {
                    return from(this.flowService.deleteComponent(requests[0])).pipe(
                        map(() => {
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
                        map(() => {
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
                concatLatestFrom(() => this.store.select(selectParentProcessGroupId)),
                filter(([, parentProcessGroupId]) => parentProcessGroupId != null),
                tap(([, parentProcessGroupId]) => {
                    this.router.navigate(['/process-groups', parentProcessGroupId]);
                })
            ),
        { dispatch: false }
    );

    addSelectedComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.addSelectedComponents),
            map((action) => action.request),
            concatLatestFrom(() => [
                this.store.select(selectCurrentProcessGroupId),
                this.store.select(selectAnySelectedComponentIds)
            ]),
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
            concatLatestFrom(() => [
                this.store.select(selectCurrentProcessGroupId),
                this.store.select(selectAnySelectedComponentIds)
            ]),
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
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
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
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([, processGroupId]) => {
                return of(FlowActions.navigateWithoutTransform({ url: ['/process-groups', processGroupId] }));
            })
        )
    );

    navigateToComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToComponent),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([request, currentProcessGroupId]) => {
                    if (request.processGroupId) {
                        this.router.navigate(['/process-groups', request.processGroupId, request.type, request.id]);
                    } else {
                        this.router.navigate(['/process-groups', currentProcessGroupId, request.type, request.id]);
                    }
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

    centerSelectedComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.centerSelectedComponents),
            map((action) => action.request),
            tap((request) => {
                this.canvasView.centerSelectedComponents(request.allowTransition);
            }),
            switchMap(() => of(FlowActions.setAllowTransition({ allowTransition: false })))
        )
    );

    navigateToProvenanceForComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToProvenanceForComponent),
                map((action) => action.id),
                tap((componentId) => {
                    this.router.navigate(['/provenance'], { queryParams: { componentId } });
                })
            ),
        { dispatch: false }
    );

    replayLastProvenanceEvent = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.replayLastProvenanceEvent),
                map((action) => action.request),
                tap((request) => {
                    this.flowService.replayLastProvenanceEvent(request).subscribe({
                        next: (response) => {
                            if (response.aggregateSnapshot.failureExplanation) {
                                this.store.dispatch(
                                    FlowActions.showOkDialog({
                                        title: 'Replay Event Failure',
                                        message: response.aggregateSnapshot.failureExplanation
                                    })
                                );
                            } else if (response.aggregateSnapshot.eventAvailable !== true) {
                                this.store.dispatch(
                                    FlowActions.showOkDialog({
                                        title: 'No Event Available',
                                        message: 'There was no recent event available to be replayed.'
                                    })
                                );
                            } else if (response.nodeSnapshots) {
                                let replayedCount = 0;
                                let unavailableCount = 0;

                                response.nodeSnapshots.forEach((nodeResponse: any) => {
                                    if (nodeResponse.snapshot.eventAvailable) {
                                        replayedCount++;
                                    } else {
                                        unavailableCount++;
                                    }
                                });

                                let message: string;
                                if (unavailableCount === 0) {
                                    message = 'All nodes successfully replayed the latest event.';
                                } else {
                                    message = `${replayedCount} nodes successfully replayed the latest event but ${unavailableCount} had no recent event available to be replayed.`;
                                }

                                this.store.dispatch(
                                    FlowActions.showOkDialog({
                                        title: 'Events Replayed',
                                        message
                                    })
                                );
                            } else {
                                this.store.dispatch(
                                    FlowActions.showOkDialog({
                                        title: 'Events Replayed',
                                        message: 'Successfully replayed the latest event.'
                                    })
                                );
                            }

                            this.store.dispatch(
                                FlowActions.loadConnectionsForComponent({
                                    id: request.componentId
                                })
                            );
                        },
                        error: (error) => {
                            this.store.dispatch(
                                FlowActions.showOkDialog({
                                    title: 'Failed to Replay Event',
                                    message: error.error
                                })
                            );
                        }
                    });
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
                        ...MEDIUM_DIALOG,
                        data: {
                            title: request.title,
                            message: request.message
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    startCurrentProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startCurrentProcessGroup),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([, pgId]) => {
                return of(
                    FlowActions.startComponent({
                        request: {
                            id: pgId,
                            type: ComponentType.ProcessGroup
                        }
                    })
                );
            }),
            catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
        )
    );

    startComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startComponents),
            map((action) => action.request),
            mergeMap((request) => [
                ...request.components.map((component) => {
                    return FlowActions.startComponent({
                        request: component
                    });
                })
            ])
        )
    );

    startComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startComponent),
            map((action) => action.request),
            mergeMap((request) => {
                switch (request.type) {
                    case ComponentType.InputPort:
                    case ComponentType.OutputPort:
                    case ComponentType.Processor:
                    case ComponentType.RemoteProcessGroup:
                        if ('uri' in request && 'revision' in request) {
                            return from(this.flowService.startComponent(request)).pipe(
                                map((response) => {
                                    return FlowActions.startComponentSuccess({
                                        response: {
                                            type: request.type,
                                            component: response
                                        }
                                    });
                                }),
                                catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                            );
                        }
                        return of(
                            FlowActions.flowApiError({
                                error: `Starting ${request.type} requires both uri and revision properties`
                            })
                        );
                    case ComponentType.ProcessGroup:
                        return combineLatest([
                            this.flowService.startProcessGroup(request),
                            this.flowService.startRemoteProcessGroupsInProcessGroup(request)
                        ]).pipe(
                            map(([startPgResponse]) => {
                                return FlowActions.startProcessGroupSuccess({
                                    response: {
                                        type: request.type,
                                        component: startPgResponse
                                    }
                                });
                            }),
                            catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                        );
                    default:
                        return of(FlowActions.flowApiError({ error: `${request.type} does not support starting` }));
                }
            }),
            catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
        )
    );

    /**
     * If the component started was the current process group, reload the flow
     */
    startCurrentProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startProcessGroupSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            filter(([response, currentPg]) => response.component.id === currentPg),
            switchMap(() => of(FlowActions.reloadFlow()))
        )
    );

    /**
     * If a ProcessGroup was started, it should be reloaded as the response from the start operation doesn't contain all the displayed info
     */
    startProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startProcessGroupSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            filter(([response, currentPg]) => response.component.id !== currentPg),
            switchMap(([response]) =>
                of(
                    FlowActions.loadChildProcessGroup({
                        request: {
                            id: response.component.id
                        }
                    })
                )
            )
        )
    );

    stopCurrentProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.stopCurrentProcessGroup),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([, pgId]) => {
                return of(
                    FlowActions.stopComponent({
                        request: {
                            id: pgId,
                            type: ComponentType.ProcessGroup
                        }
                    })
                );
            })
        )
    );

    stopComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.stopComponents),
            map((action) => action.request),
            mergeMap((request) => [
                ...request.components.map((component) => {
                    return FlowActions.stopComponent({
                        request: component
                    });
                })
            ])
        )
    );

    stopComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.stopComponent),
            map((action) => action.request),
            mergeMap((request) => {
                switch (request.type) {
                    case ComponentType.InputPort:
                    case ComponentType.OutputPort:
                    case ComponentType.Processor:
                    case ComponentType.RemoteProcessGroup:
                        if ('uri' in request && 'revision' in request) {
                            return from(this.flowService.stopComponent(request)).pipe(
                                map((response) => {
                                    return FlowActions.stopComponentSuccess({
                                        response: {
                                            type: request.type,
                                            component: response
                                        }
                                    });
                                }),
                                catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                            );
                        }
                        return of(
                            FlowActions.flowApiError({
                                error: `Stopping ${request.type} requires both uri and revision properties`
                            })
                        );
                    case ComponentType.ProcessGroup:
                        return combineLatest([
                            this.flowService.stopProcessGroup(request),
                            this.flowService.stopRemoteProcessGroupsInProcessGroup(request)
                        ]).pipe(
                            map(([stopPgResponse]) => {
                                return FlowActions.stopProcessGroupSuccess({
                                    response: {
                                        type: request.type,
                                        component: stopPgResponse
                                    }
                                });
                            }),
                            catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                        );
                    default:
                        return of(FlowActions.flowApiError({ error: `${request.type} does not support stopping` }));
                }
            }),
            catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
        )
    );

    /**
     * If the component stopped was the current process group, reload the flow
     */
    stopCurrentProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.stopProcessGroupSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            filter(([response, currentPg]) => response.component.id === currentPg),
            switchMap(() => of(FlowActions.reloadFlow()))
        )
    );

    /**
     * If a ProcessGroup was stopped, it should be reloaded as the response from the stop operation doesn't contain all the displayed info
     */
    stopProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.stopProcessGroupSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            filter(([response, currentPg]) => response.component.id !== currentPg),
            switchMap(([response]) =>
                of(
                    FlowActions.loadChildProcessGroup({
                        request: {
                            id: response.component.id
                        }
                    })
                )
            )
        )
    );

    runOnce$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.runOnce),
            map((action) => action.request),
            switchMap((request) => {
                return from(this.flowService.runOnce(request)).pipe(
                    map((response) =>
                        FlowActions.runOnceSuccess({
                            response: {
                                component: response
                            }
                        })
                    ),
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                );
            }),
            catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
        )
    );

    reloadProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadChildProcessGroup),
            map((action) => action.request),
            mergeMap((request) => {
                return from(this.flowService.getProcessGroup(request.id)).pipe(
                    map((response) =>
                        FlowActions.loadChildProcessGroupSuccess({
                            response
                        })
                    ),
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                );
            })
        )
    );

    //////////////////////////////////
    // Start version control effects
    //////////////////////////////////
    openSaveVersionDialogRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.openSaveVersionDialogRequest),
            map((action) => action.request),
            switchMap((request) => {
                return combineLatest([
                    this.registryService.getRegistryClients(),
                    this.flowService.getVersionInformation(request.processGroupId)
                ]).pipe(
                    map(([registryClients, versionInfo]) => {
                        const dialogRequest: SaveVersionDialogRequest = {
                            processGroupId: request.processGroupId,
                            revision: versionInfo.processGroupRevision,
                            registryClients: registryClients.registries
                        };

                        return FlowActions.openSaveVersionDialog({ request: dialogRequest });
                    }),
                    catchError((error) => of(FlowActions.flowSnackbarError({ error: error.error })))
                );
            })
        )
    );

    openSaveVersionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openSaveVersionDialog),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(SaveVersionDialog, {
                        ...MEDIUM_DIALOG,
                        data: request
                    });

                    dialogReference.componentInstance.getBuckets = (registryId: string): Observable<BucketEntity[]> => {
                        return this.registryService.getBuckets(registryId).pipe(
                            take(1),
                            map((response) => response.buckets)
                        );
                    };

                    dialogReference.componentInstance.saving = this.store.selectSignal(selectVersionSaving);

                    dialogReference.componentInstance.save
                        .pipe(takeUntil(dialogReference.afterClosed()))
                        .subscribe((saveRequest: SaveVersionRequest) => {
                            if (saveRequest.existingFlowId) {
                                this.store.dispatch(
                                    FlowActions.saveToFlowRegistry({
                                        request: {
                                            versionedFlow: {
                                                action: request.forceCommit ? 'FORCE_COMMIT' : 'COMMIT',
                                                flowId: saveRequest.existingFlowId,
                                                bucketId: saveRequest.bucket,
                                                registryId: saveRequest.registry,
                                                comments: saveRequest.comments || ''
                                            },
                                            processGroupId: saveRequest.processGroupId,
                                            processGroupRevision: saveRequest.revision
                                        }
                                    })
                                );
                            } else {
                                this.store.dispatch(
                                    FlowActions.saveToFlowRegistry({
                                        request: {
                                            versionedFlow: {
                                                action: 'COMMIT',
                                                bucketId: saveRequest.bucket,
                                                registryId: saveRequest.registry,
                                                flowName: saveRequest.flowName,
                                                description: saveRequest.flowDescription || '',
                                                comments: saveRequest.comments || ''
                                            },
                                            processGroupId: saveRequest.processGroupId,
                                            processGroupRevision: saveRequest.revision
                                        }
                                    })
                                );
                            }
                        });

                    dialogReference.afterClosed().subscribe(() => {
                        this.store.dispatch(ErrorActions.clearBannerErrors());
                    });
                })
            ),
        { dispatch: false }
    );

    saveToFlowRegistry$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.saveToFlowRegistry),
            map((action) => action.request),
            switchMap((request) => {
                return from(this.flowService.saveToFlowRegistry(request)).pipe(
                    map((response) => {
                        return FlowActions.saveToFlowRegistrySuccess({ response });
                    }),
                    catchError((error) => of(FlowActions.flowVersionBannerError({ error: error.error })))
                );
            })
        )
    );

    saveToFlowRegistrySuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.saveToFlowRegistrySuccess),
            tap(() => {
                this.dialog.closeAll();
            }),
            switchMap(() => of(FlowActions.reloadFlow()))
        )
    );

    flowVersionBannerError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.flowVersionBannerError),
            map((action) => action.error),
            switchMap((error) => of(ErrorActions.addBannerError({ error })))
        )
    );

    /////////////////////////////////
    // Stop version control effects
    /////////////////////////////////
    stopVersionControlRequest$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.stopVersionControlRequest),
                map((action) => action.request),
                switchMap((request) => {
                    return from(this.flowService.getVersionInformation(request.processGroupId)).pipe(
                        map((response) => {
                            const dialogRequest: StopVersionControlRequest = {
                                processGroupId: request.processGroupId,
                                revision: response.processGroupRevision
                            };
                            return dialogRequest;
                        })
                    );
                }),
                tap((request) => {
                    const dialogRef = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Stop Version Control',
                            message: `Are you sure you want to stop version control?`
                        }
                    });

                    dialogRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(FlowActions.stopVersionControl({ request }));
                    });

                    dialogRef.componentInstance.no.pipe(take(1)).subscribe(() => {
                        dialogRef.close();
                    });
                }),
                catchError((error) => of(FlowActions.flowSnackbarError({ error: error.error })))
            ),
        { dispatch: false }
    );

    stopVersionControl$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.stopVersionControl),
            map((action) => action.request),
            switchMap((request) =>
                from(this.flowService.stopVersionControl(request)).pipe(
                    map((response) => {
                        const stopResponse: StopVersionControlResponse = {
                            processGroupRevision: response.processGroupRevision,
                            processGroupId: request.processGroupId
                        };
                        return FlowActions.stopVersionControlSuccess({ response: stopResponse });
                    }),
                    catchError((errorResponse) => of(FlowActions.flowSnackbarError({ error: errorResponse.error })))
                )
            )
        )
    );

    stopVersionControlSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.stopVersionControlSuccess),
            tap(() => {
                this.store.dispatch(
                    FlowActions.showOkDialog({
                        title: 'Disconnect',
                        message: 'This Process Group is no longer under version control.'
                    })
                );
            }),
            switchMap(() => of(FlowActions.reloadFlow()))
        )
    );

    /////////////////////////////////
    // Commit local changes effects
    /////////////////////////////////
    openCommitLocalChangesDialogRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.openCommitLocalChangesDialogRequest),
            map((action) => action.request),
            switchMap((request) => {
                return from(this.flowService.getVersionInformation(request.processGroupId)).pipe(
                    map((response) => {
                        const dialogRequest: SaveVersionDialogRequest = {
                            processGroupId: request.processGroupId,
                            revision: response.processGroupRevision,
                            versionControlInformation: response.versionControlInformation,
                            forceCommit: request.forceCommit
                        };

                        return FlowActions.openSaveVersionDialog({ request: dialogRequest });
                    }),
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                );
            })
        )
    );

    openForceCommitLocalChangesDialogRequest$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openForceCommitLocalChangesDialogRequest),
                map((action) => action.request),
                tap((request) => {
                    const dialogRef = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Commit',
                            message:
                                'Committing will ignore available upgrades and commit local changes as the next version. Are you sure you want to proceed?'
                        }
                    });

                    dialogRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            FlowActions.openCommitLocalChangesDialogRequest({
                                request: {
                                    ...request,
                                    forceCommit: true
                                }
                            })
                        );
                    });

                    dialogRef.componentInstance.no.pipe(take(1)).subscribe(() => {
                        dialogRef.close();
                    });
                })
            ),
        { dispatch: false }
    );

    /////////////////////////////
    // Change version effects
    /////////////////////////////
    openChangeVersionDialogRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.openChangeVersionDialogRequest),
            map((action) => action.request),
            switchMap((request) =>
                from(this.flowService.getVersionInformation(request.processGroupId)).pipe(
                    tap({
                        error: (errorResponse: HttpErrorResponse) =>
                            this.store.dispatch(FlowActions.flowSnackbarError({ error: errorResponse.error }))
                    })
                )
            ),
            switchMap((versionControlInfo: VersionControlInformationEntity) => {
                const vci = versionControlInfo.versionControlInformation;
                if (vci) {
                    return from(this.registryService.getFlowVersions(vci.registryId, vci.bucketId, vci.flowId)).pipe(
                        map((versions) =>
                            FlowActions.openChangeVersionDialog({
                                request: {
                                    processGroupId: vci.groupId,
                                    revision: versionControlInfo.processGroupRevision,
                                    versionControlInformation: vci,
                                    versions:
                                        versions.versionedFlowSnapshotMetadataSet as unknown as VersionedFlowSnapshotMetadataEntity[]
                                }
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(FlowActions.flowSnackbarError({ error: errorResponse.error }))
                        )
                    );
                } else {
                    // should never happen
                    return NEVER;
                }
            })
        )
    );

    openChangeVersionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openChangeVersionDialog),
                map((action) => action.request),
                tap((request) => {
                    const dialogRef = this.dialog.open(ChangeVersionDialog, {
                        ...LARGE_DIALOG,
                        data: request
                    });

                    dialogRef.componentInstance.changeVersion.pipe(take(1)).subscribe((selectedVersion) => {
                        const entity: VersionControlInformationEntity = {
                            versionControlInformation: {
                                ...request.versionControlInformation,
                                version: selectedVersion.version
                            },
                            processGroupRevision: request.revision,
                            disconnectedNodeAcknowledged: false
                        };
                        dialogRef.close();

                        this.store.dispatch(FlowActions.openChangeVersionProgressDialog({ request: entity }));
                    });
                })
            ),
        { dispatch: false }
    );

    openChangeVersionProgressDialog$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.openChangeVersionProgressDialog),
            map((action) => action.request),
            tap(() => {
                const dialogRef = this.dialog.open(ChangeVersionProgressDialog, {
                    ...SMALL_DIALOG,
                    minWidth: 365,
                    disableClose: true
                });
                dialogRef.componentInstance.flowUpdateRequest$ = this.store.select(selectChangeVersionRequest);
                dialogRef.componentInstance.changeVersionComplete.pipe(take(1)).subscribe((entity) => {
                    this.store.dispatch(FlowActions.changeVersionComplete({ response: entity }));
                });
            }),
            switchMap((request) => of(FlowActions.changeVersion({ request })))
        )
    );

    changeVersion$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.changeVersion),
            map((action) => action.request),
            switchMap((request) => {
                return from(this.flowService.initiateChangeVersionUpdate(request)).pipe(
                    map((flowUpdate) => FlowActions.changeVersionSuccess({ response: flowUpdate })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(FlowActions.flowSnackbarError({ error: errorResponse.error }))
                    )
                );
            })
        )
    );

    changeVersionSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.changeVersionSuccess),
            map((action) => action.response),
            filter((response) => !response.request.complete),
            switchMap(() => {
                return of(FlowActions.startPollingChangeVersion());
            })
        )
    );

    startPollingChangeVersion = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startPollingChangeVersion),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(FlowActions.stopPollingChangeVersion)))
                )
            ),
            switchMap(() => of(FlowActions.pollChangeVersion()))
        )
    );

    pollChangeVersion$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.pollChangeVersion),
            concatLatestFrom(() => [this.store.select(selectChangeVersionRequest).pipe(isDefinedAndNotNull())]),
            switchMap(([, changeVersionRequest]) => {
                return from(
                    this.flowService.getChangeVersionUpdateRequest(changeVersionRequest.request.requestId).pipe(
                        map((response) => FlowActions.pollChangeVersionSuccess({ response })),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(FlowActions.flowSnackbarError({ error: errorResponse.error }))
                        )
                    )
                );
            })
        )
    );

    pollChangeVersionSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.pollChangeVersionSuccess),
            map((action) => action.response),
            filter((response) => response.request.complete),
            switchMap(() => of(FlowActions.stopPollingChangeVersion()))
        )
    );

    changeVersionComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.changeVersionComplete),
            map((action) => action.response),
            switchMap((response) =>
                from(this.flowService.deleteChangeVersionUpdateRequest(response.request.requestId)).pipe(
                    map(() => FlowActions.reloadFlow()),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(FlowActions.flowSnackbarError({ error: errorResponse.error }))
                    )
                )
            )
        )
    );

    flowSnackbarError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.flowSnackbarError),
            map((action) => action.error),
            tap(() => {
                this.dialog.closeAll();
            }),
            switchMap((error) => of(ErrorActions.snackBarError({ error })))
        )
    );

    ///////////////////////////////
    // Show local changes effects
    ///////////////////////////////
    openLocalChangesDialogRequest = (mode: 'SHOW' | 'REVERT') =>
        createEffect(() =>
            this.actions$.pipe(
                ofType(
                    mode === 'SHOW'
                        ? FlowActions.openShowLocalChangesDialogRequest
                        : FlowActions.openRevertLocalChangesDialogRequest
                ),
                map((action) => action.request),
                switchMap((request) =>
                    combineLatest([
                        this.flowService.getVersionInformation(request.processGroupId),
                        this.flowService.getLocalModifications(request.processGroupId)
                    ]).pipe(
                        map(([versionControlInfo, localModifications]) =>
                            FlowActions.openLocalChangesDialog({
                                request: {
                                    localModifications,
                                    versionControlInformation: versionControlInfo,
                                    mode
                                }
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(FlowActions.flowSnackbarError({ error: errorResponse.error }))
                        )
                    )
                )
            )
        );

    openShowLocalChangesDialogRequest$ = this.openLocalChangesDialogRequest('SHOW');

    openLocalChangesDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openLocalChangesDialog),
                map((action) => action.request),
                tap((request) => {
                    const dialogRef = this.dialog.open(LocalChangesDialog, {
                        ...XL_DIALOG,
                        data: request
                    });
                    if (request.mode === 'REVERT') {
                        dialogRef.componentInstance.revert.pipe(take(1)).subscribe((request) => {
                            dialogRef.close();
                            this.store.dispatch(
                                FlowActions.openRevertChangesProgressDialog({
                                    request: request.versionControlInformation
                                })
                            );
                        });
                    }
                    dialogRef.componentInstance.goToChange
                        .pipe(take(1))
                        .subscribe((request) => this.store.dispatch(FlowActions.goToChange({ request })));
                })
            ),
        { dispatch: false }
    );

    goToChange$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.goToChange),
            map((action) => action.request),
            tap(() => this.dialog.closeAll()),
            switchMap((request) => of(FlowActions.navigateToComponent({ request })))
        )
    );

    /////////////////////////////////
    // Revert version effects
    /////////////////////////////////
    openRevertLocalChangesDialogRequest$ = this.openLocalChangesDialogRequest('REVERT');

    openRevertChangesProgressDialog$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.openRevertChangesProgressDialog),
            map((action) => action.request),
            tap(() => {
                const dialogRef = this.dialog.open(ChangeVersionProgressDialog, {
                    ...SMALL_DIALOG,
                    minWidth: 365,
                    disableClose: true
                });
                dialogRef.componentInstance.flowUpdateRequest$ = this.store.select(selectChangeVersionRequest);
                dialogRef.componentInstance.changeVersionComplete.pipe(take(1)).subscribe((entity) => {
                    this.store.dispatch(FlowActions.revertChangesComplete({ response: entity }));
                });
            }),
            switchMap((request) => of(FlowActions.revertChanges({ request })))
        )
    );

    revertChanges$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.revertChanges),
            map((action) => action.request),
            switchMap((request) => {
                return from(this.flowService.initiateRevertFlowVersion(request)).pipe(
                    map((flowUpdate) => FlowActions.revertChangesSuccess({ response: flowUpdate })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(FlowActions.flowSnackbarError({ error: errorResponse.error }))
                    )
                );
            })
        )
    );

    revertChangesSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.revertChangesSuccess),
            map((action) => action.response),
            filter((response) => !response.request.complete),
            switchMap(() => {
                return of(FlowActions.startPollingRevertChanges());
            })
        )
    );

    startPollingRevertChanges = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startPollingRevertChanges),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(FlowActions.stopPollingRevertChanges)))
                )
            ),
            switchMap(() => of(FlowActions.pollRevertChanges()))
        )
    );

    pollRevertChanges$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.pollRevertChanges),
            concatLatestFrom(() => [this.store.select(selectChangeVersionRequest).pipe(isDefinedAndNotNull())]),
            switchMap(([, changeVersionRequest]) => {
                return from(
                    this.flowService.getRevertChangesUpdateRequest(changeVersionRequest.request.requestId).pipe(
                        map((response) => FlowActions.pollRevertChangesSuccess({ response })),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(FlowActions.flowSnackbarError({ error: errorResponse.error }))
                        )
                    )
                );
            })
        )
    );

    pollRevertChangesSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.pollRevertChangesSuccess),
            map((action) => action.response),
            filter((response) => response.request.complete),
            switchMap(() => of(FlowActions.stopPollingRevertChanges()))
        )
    );

    revertChangesComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.revertChangesComplete),
            map((action) => action.response),
            switchMap((response) =>
                from(this.flowService.deleteRevertChangesUpdateRequest(response.request.requestId)).pipe(
                    map(() => FlowActions.reloadFlow()),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(FlowActions.flowSnackbarError({ error: errorResponse.error }))
                    )
                )
            )
        )
    );

    downloadFlow$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.downloadFlow),
                map((action) => action.request),
                tap((request) => {
                    this.flowService.downloadFlow(request);
                })
            ),
        { dispatch: false }
    );
}
