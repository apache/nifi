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

import { Injectable, inject } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import { Store } from '@ngrx/store';
import { Router } from '@angular/router';
import { NEVER, Observable, of } from 'rxjs';
import { catchError, filter, map, switchMap, take, tap } from 'rxjs/operators';
import { ComponentType, ComponentTypeNamePipe, LARGE_DIALOG, MEDIUM_DIALOG, XL_DIALOG } from '@nifi/shared';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { selectPrioritizerTypes } from '../../../../state/extension-types/extension-types.selectors';
import {
    selectPropertyVerificationResults,
    selectPropertyVerificationStatus
} from '../../../../state/property-verification/property-verification.selectors';
import { MatDialog } from '@angular/material/dialog';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { ErrorContextKey } from '../../../../state/error';
import { BackNavigation } from '../../../../state/navigation';
import { EditComponentDialogRequest, EditConnectionDialogRequest } from '../../../../state/shared';
import { EditProcessor } from '../../../../ui/common/component-dialogs/edit-processor/edit-processor.component';
import { EditConnectionComponent } from '../../../../ui/common/component-dialogs/edit-connection/edit-connection.component';
import { EditPort } from '../../../../ui/common/component-dialogs/edit-port/edit-port.component';
import { EditLabel } from '../../../../ui/common/component-dialogs/edit-label/edit-label.component';
import { EditProcessGroup } from '../../../../ui/common/component-dialogs/edit-process-group/edit-process-group.component';
import { EditRemoteProcessGroup } from '../../../../ui/common/component-dialogs/edit-remote-process-group/edit-remote-process-group.component';
import * as ConnectorCanvasActions from './connector-canvas.actions';
import { SelectedComponent } from './connector-canvas.actions';
import {
    selectBreadcrumbs,
    selectConnectorIdFromRoute,
    selectInputPort,
    selectOutputPort,
    selectParentProcessGroupId,
    selectProcessGroup,
    selectProcessGroupId,
    selectProcessGroupIdFromRoute,
    selectProcessor,
    selectRemoteProcessGroup
} from './connector-canvas.selectors';

@Injectable()
export class ConnectorCanvasEffects {
    private actions$ = inject(Actions);
    private store = inject(Store);
    private router = inject(Router);
    private connectorService = inject(ConnectorService);
    private errorHelper = inject(ErrorHelper);
    private componentTypeNamePipe = inject(ComponentTypeNamePipe);
    private dialog = inject(MatDialog);

    loadConnectorFlow$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.loadConnectorFlow),
            switchMap((action) =>
                this.connectorService.getConnectorFlow(action.connectorId, action.processGroupId).pipe(
                    map((flowResponse) => {
                        const processGroupFlow = flowResponse.processGroupFlow;
                        const flow = processGroupFlow?.flow;
                        const labels = flow?.labels || [];
                        const funnels = flow?.funnels || [];
                        const inputPorts = flow?.inputPorts || [];
                        const outputPorts = flow?.outputPorts || [];
                        const remoteProcessGroups = flow?.remoteProcessGroups || [];
                        const processGroups = flow?.processGroups || [];
                        const processors = flow?.processors || [];
                        const connections = flow?.connections || [];

                        // Extract process group IDs for navigation
                        const processGroupId = processGroupFlow?.id ?? null;
                        const parentProcessGroupId = processGroupFlow?.parentGroupId ?? null;

                        // Extract breadcrumb for navigation trail
                        const breadcrumb = processGroupFlow?.breadcrumb ?? null;

                        return ConnectorCanvasActions.loadConnectorFlowSuccess({
                            connectorId: action.connectorId,
                            processGroupId,
                            parentProcessGroupId,
                            breadcrumb,
                            labels,
                            funnels,
                            inputPorts,
                            outputPorts,
                            remoteProcessGroups,
                            processGroups,
                            processors,
                            connections
                        });
                    }),
                    catchError((error) => {
                        return of(
                            ConnectorCanvasActions.loadConnectorFlowFailure({
                                errorContext: {
                                    errors: [this.errorHelper.getErrorString(error)],
                                    context: ErrorContextKey.CONNECTORS
                                }
                            })
                        );
                    })
                )
            )
        )
    );

    loadConnectorFlowComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.loadConnectorFlowSuccess),
            map(() => ConnectorCanvasActions.loadConnectorFlowComplete())
        )
    );

    /**
     * Select components - updates route with selection
     * Routes to /connectors/:id/canvas/:processGroupId/:type/:componentId
     */
    selectComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.selectComponents),
            map((action) => action.request),
            concatLatestFrom(() => [
                this.store.select(selectConnectorIdFromRoute),
                this.store.select(selectProcessGroupIdFromRoute)
            ]),
            switchMap(([request, connectorId, processGroupId]) => {
                if (request.components.length === 0) {
                    return of(ConnectorCanvasActions.deselectAllComponents());
                }

                let commands: string[];
                if (request.components.length === 1) {
                    commands = [
                        '/connectors',
                        connectorId,
                        'canvas',
                        processGroupId,
                        request.components[0].componentType,
                        request.components[0].id
                    ];
                } else {
                    const ids: string[] = request.components.map(
                        (selectedComponent: SelectedComponent) => selectedComponent.id
                    );
                    commands = ['/connectors', connectorId, 'canvas', processGroupId, 'bulk', ids.join(',')];
                }
                return of(ConnectorCanvasActions.navigateWithoutTransform({ url: commands }));
            })
        )
    );

    /**
     * Deselect all components - returns to base canvas route
     */
    deselectAllComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.deselectAllComponents),
            concatLatestFrom(() => [
                this.store.select(selectConnectorIdFromRoute),
                this.store.select(selectProcessGroupIdFromRoute)
            ]),
            switchMap(([, connectorId, processGroupId]) => {
                return of(
                    ConnectorCanvasActions.navigateWithoutTransform({
                        url: ['/connectors', connectorId, 'canvas', processGroupId]
                    })
                );
            })
        )
    );

    /**
     * Navigate without transform - updates router
     */
    navigateWithoutTransform$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorCanvasActions.navigateWithoutTransform),
                tap((action) => {
                    this.router.navigate(action.url, {
                        replaceUrl: true
                    });
                })
            ),
        { dispatch: false }
    );

    enterProcessGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorCanvasActions.enterProcessGroup),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectConnectorIdFromRoute)),
                tap(([request, connectorId]) => {
                    this.router.navigate(['/connectors', connectorId, 'canvas', request.id]);
                })
            ),
        { dispatch: false }
    );

    leaveProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.leaveProcessGroup),
            concatLatestFrom(() => [
                this.store.select(selectConnectorIdFromRoute),
                this.store.select(selectParentProcessGroupId),
                this.store.select(selectProcessGroupId)
            ]),
            filter(
                ([, , parentProcessGroupId, currentProcessGroupId]) =>
                    parentProcessGroupId != null && currentProcessGroupId != null
            ),
            switchMap(([, connectorId, parentProcessGroupId, childProcessGroupId]) => {
                this.router.navigate(['/connectors', connectorId, 'canvas', parentProcessGroupId]);

                return this.actions$.pipe(
                    ofType(ConnectorCanvasActions.loadConnectorFlowComplete),
                    take(1),
                    map(() =>
                        ConnectorCanvasActions.navigateWithoutTransform({
                            url: [
                                '/connectors',
                                connectorId,
                                'canvas',
                                parentProcessGroupId!,
                                ComponentType.ProcessGroup,
                                childProcessGroupId!
                            ]
                        })
                    )
                );
            })
        )
    );

    /**
     * Open a read-only configuration dialog for the given canvas component.
     * Forces read-only mode by overriding canWrite regardless of entity permissions
     * since the connector canvas is a view-only interface.
     */
    viewComponentConfiguration$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorCanvasActions.viewComponentConfiguration),
                map((action) => action.request),
                tap(({ entity, componentType }) => {
                    const dialogRequest = this.buildDialogRequest(entity, componentType);

                    switch (componentType) {
                        case ComponentType.Processor:
                            this.openReadOnlyProcessorDialog(dialogRequest);
                            break;
                        case ComponentType.Connection:
                            this.openReadOnlyConnectionDialog(dialogRequest);
                            break;
                        case ComponentType.InputPort:
                        case ComponentType.OutputPort:
                            this.openReadOnlyPortDialog(dialogRequest);
                            break;
                        case ComponentType.Label:
                            this.openReadOnlyLabelDialog(dialogRequest);
                            break;
                        case ComponentType.ProcessGroup:
                            this.openReadOnlyProcessGroupDialog(dialogRequest);
                            break;
                        case ComponentType.RemoteProcessGroup:
                            this.openReadOnlyRemoteProcessGroupDialog(dialogRequest);
                            break;
                        default:
                            break;
                    }
                })
            ),
        { dispatch: false }
    );

    navigateToProvenanceForComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorCanvasActions.navigateToProvenanceForComponent),
                map((action) => ({ id: action.id, componentType: action.componentType })),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectProcessGroupIdFromRoute)
                ]),
                tap(([{ id: componentId, componentType }, connectorId, processGroupId]) => {
                    this.router.navigate(['/provenance'], {
                        queryParams: { componentId },
                        state: {
                            backNavigation: {
                                route: [
                                    '/connectors',
                                    connectorId,
                                    'canvas',
                                    processGroupId,
                                    componentType,
                                    componentId
                                ],
                                routeBoundary: ['/provenance'],
                                context: this.componentTypeNamePipe.transform(componentType)
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    /**
     * Build a dialog request from the supplied entity. The connector canvas surfaces
     * configuration in a strictly read-only mode, so the entity permissions are
     * forced to readable-only-without-write before being passed to the dialog.
     */
    private buildDialogRequest(entity: any, componentType: ComponentType): EditComponentDialogRequest {
        const readOnlyEntity = {
            ...entity,
            permissions: { ...entity?.permissions, canWrite: false },
            operatePermissions: { ...entity?.operatePermissions, canWrite: false }
        };
        return {
            type: componentType,
            uri: readOnlyEntity.uri,
            entity: readOnlyEntity
        };
    }

    private openReadOnlyProcessorDialog(request: EditComponentDialogRequest): void {
        const dialogRef = this.dialog.open(EditProcessor, {
            ...XL_DIALOG,
            data: request,
            id: request.entity.id
        });
        const instance = dialogRef.componentInstance;
        instance.saving$ = of(false);
        instance.propertyVerificationResults$ = this.store.select(selectPropertyVerificationResults);
        instance.propertyVerificationStatus$ = this.store.select(selectPropertyVerificationStatus);

        // provide no-op stubs for callbacks not needed in read-only mode
        instance.createNewProperty = () => NEVER;
        instance.createNewService = () => NEVER;
        instance.convertToParameter = () => NEVER;
        instance.goToParameter = () => undefined;
        instance.goToService = () => undefined;
    }

    private openReadOnlyConnectionDialog(request: EditComponentDialogRequest): void {
        const dialogRef = this.dialog.open(EditConnectionComponent, {
            ...LARGE_DIALOG,
            data: request as EditConnectionDialogRequest
        });
        const instance = dialogRef.componentInstance;
        instance.saving$ = of(false);
        instance.availablePrioritizers$ = this.store.select(selectPrioritizerTypes);
        instance.breadcrumbs$ = this.store.select(selectBreadcrumbs);
        instance.getChildOutputPorts = (groupId: string): Observable<any> =>
            this.store.select(selectConnectorIdFromRoute).pipe(
                take(1),
                switchMap((connectorId) =>
                    this.connectorService
                        .getConnectorFlow(connectorId!, groupId)
                        .pipe(map((response: any) => response.processGroupFlow.flow.outputPorts))
                )
            );
        instance.getChildInputPorts = (groupId: string): Observable<any> =>
            this.store.select(selectConnectorIdFromRoute).pipe(
                take(1),
                switchMap((connectorId) =>
                    this.connectorService
                        .getConnectorFlow(connectorId!, groupId)
                        .pipe(map((response: any) => response.processGroupFlow.flow.inputPorts))
                )
            );
        instance.selectProcessor = (id: string) => this.store.select(selectProcessor(id));
        instance.selectInputPort = (id: string) => this.store.select(selectInputPort(id));
        instance.selectOutputPort = (id: string) => this.store.select(selectOutputPort(id));
        instance.selectProcessGroup = (id: string) => this.store.select(selectProcessGroup(id));
        instance.selectRemoteProcessGroup = (id: string) => this.store.select(selectRemoteProcessGroup(id));
    }

    private openReadOnlyPortDialog(request: EditComponentDialogRequest): void {
        const dialogRef = this.dialog.open(EditPort, {
            ...MEDIUM_DIALOG,
            data: request
        });
        dialogRef.componentInstance.saving$ = of(false);
    }

    private openReadOnlyLabelDialog(request: EditComponentDialogRequest): void {
        const dialogRef = this.dialog.open(EditLabel, {
            ...MEDIUM_DIALOG,
            data: request
        });
        dialogRef.componentInstance.saving$ = of(false);
    }

    private openReadOnlyProcessGroupDialog(request: EditComponentDialogRequest): void {
        const dialogRef = this.dialog.open(EditProcessGroup, {
            ...LARGE_DIALOG,
            data: request
        });
        const instance = dialogRef.componentInstance;
        instance.saving$ = of(false);
        instance.currentUser$ = this.store.select(selectCurrentUser);
        instance.parameterContexts = [];
    }

    private openReadOnlyRemoteProcessGroupDialog(request: EditComponentDialogRequest): void {
        const dialogRef = this.dialog.open(EditRemoteProcessGroup, {
            ...LARGE_DIALOG,
            data: request
        });
        dialogRef.componentInstance.saving$ = of(false);
    }
}
