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

import { DestroyRef, Injectable, inject } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import { Store } from '@ngrx/store';
import { Router } from '@angular/router';
import { asyncScheduler, interval, NEVER, Observable, of } from 'rxjs';
import {
    catchError,
    distinctUntilChanged,
    filter,
    map,
    startWith,
    switchMap,
    take,
    takeUntil,
    tap,
    throttleTime
} from 'rxjs/operators';
import { ComponentType, ComponentTypeNamePipe, LARGE_DIALOG, MEDIUM_DIALOG, NiFiCommon, XL_DIALOG } from '@nifi/shared';
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
import * as ErrorActions from '../../../../state/error/error.actions';
import { BackNavigation } from '../../../../state/navigation';
import { EditComponentDialogRequest, EditConnectionDialogRequest } from '../../../../state/shared';
import { EditProcessor } from '../../../../ui/common/component-dialogs/edit-processor/edit-processor.component';
import { EditConnectionComponent } from '../../../../ui/common/component-dialogs/edit-connection/edit-connection.component';
import { EditPort } from '../../../../ui/common/component-dialogs/edit-port/edit-port.component';
import { EditLabel } from '../../../../ui/common/component-dialogs/edit-label/edit-label.component';
import { EditProcessGroup } from '../../../../ui/common/component-dialogs/edit-process-group/edit-process-group.component';
import { EditRemoteProcessGroup } from '../../../../ui/common/component-dialogs/edit-remote-process-group/edit-remote-process-group.component';
import * as ConnectorCanvasActions from './connector-canvas.actions';
import * as ConnectorControllerServicesActions from '../connector-controller-services/connector-controller-services.actions';
import { bindConnectorParameterContext } from './bind-connector-parameter-context';
import { SelectedComponent } from './connector-canvas.actions';
import * as EmptyQueueActions from '../../../../state/empty-queue/empty-queue.actions';
import {
    selectBreadcrumbs,
    selectConnectorId,
    selectConnectorIdFromRoute,
    selectInputPort,
    selectLoadingStatus,
    selectOutputPort,
    selectParentProcessGroupId,
    selectProcessGroup,
    selectProcessGroupId,
    selectProcessGroupIdFromRoute,
    selectProcessor,
    selectRemoteProcessGroup
} from './connector-canvas.selectors';
import { selectDocumentVisibilityState } from '../../../../state/document-visibility/document-visibility.selectors';
import { DocumentVisibility } from '../../../../state/document-visibility';

@Injectable()
export class ConnectorCanvasEffects {
    private actions$ = inject(Actions);
    private store = inject(Store);
    private router = inject(Router);
    private connectorService = inject(ConnectorService);
    private errorHelper = inject(ErrorHelper);
    private componentTypeNamePipe = inject(ComponentTypeNamePipe);
    private dialog = inject(MatDialog);
    private destroyRef = inject(DestroyRef);

    /**
     * Timestamp of the most recent reload (epoch millis). Used by the document
     * visibility subscription to debounce "tab refocus" reloads so that
     * switching tabs in quick succession does not produce a thrash of loads.
     */
    private lastReload = 0;

    constructor() {
        // When the document becomes visible after having been hidden long enough
        // for the polling interval to be skipped, trigger a reload so the canvas
        // does not display stale bulletins / status while the next interval tick
        // is pending. The 30 second floor matches the polling cadence and keeps
        // rapid tab switches from forcing a fresh load every time.
        this.store
            .select(selectDocumentVisibilityState)
            .pipe(
                takeUntilDestroyed(this.destroyRef),
                filter((documentVisibility) => documentVisibility.documentVisibility === DocumentVisibility.Visible),
                filter(
                    (documentVisibility) =>
                        documentVisibility.changedTimestamp - this.lastReload > 30 * NiFiCommon.MILLIS_PER_SECOND
                )
            )
            .subscribe(() => {
                this.store.dispatch(ConnectorCanvasActions.reloadConnectorFlow());
            });
    }

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
     * Request the parameter context bound to the loaded process group whenever the
     * connector canvas or controller-services page reports a successful load. The
     * backend returns 204 No Content when no bound context exists, which surfaces as
     * a null parameter context on the canvas state.
     *
     * The controller-services route is a sibling of the canvas route, not a child, so
     * deep-linking directly to `/connectors/:id/canvas/:pgId/controller-services`
     * never activates the canvas component and thus never emits
     * {@link loadConnectorFlowSuccess}. Listening to both trigger actions in a single
     * pipeline keeps that deep-link case covered. {@link distinctUntilChanged} keyed
     * on connectorId + processGroupId dedupes the polling refire on the canvas page
     * and the canvas → controller-services transition within the same process group;
     * navigating to a different process group (or to a different connector with the
     * same process group id) changes the key and triggers a fresh fetch.
     *
     * NgRx effects are application-level singletons, so the inner `distinctUntilChanged`
     * would otherwise accumulate the last (connectorId, processGroupId) pair across the
     * entire app lifetime — silently suppressing the re-fetch when a user navigates
     * away from a connector and back into the same one. Wrapping the inner pipeline in
     * `switchMap` over `resetConnectorCanvasState` (which fires on canvas ngOnDestroy)
     * tears down and rebuilds the inner subscription, clearing the stale "previous"
     * reference so the next load-success emission is always treated as a first
     * emission. `startWith(null)` is required so the inner pipeline is subscribed on
     * app startup without waiting for the first reset.
     */
    loadConnectorParameterContextOnLoadSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.resetConnectorCanvasState),
            startWith(null),
            switchMap(() =>
                this.actions$.pipe(
                    ofType(
                        ConnectorCanvasActions.loadConnectorFlowSuccess,
                        ConnectorControllerServicesActions.loadConnectorControllerServicesSuccess
                    ),
                    map((action) => {
                        if (action.type === ConnectorCanvasActions.loadConnectorFlowSuccess.type) {
                            return {
                                connectorId: action.connectorId,
                                processGroupId: action.processGroupId,
                                errorContext: ErrorContextKey.CONNECTOR_CANVAS
                            };
                        }
                        return {
                            connectorId: action.response.connectorId,
                            processGroupId: action.response.processGroupId,
                            errorContext: ErrorContextKey.CONTROLLER_SERVICES
                        };
                    }),
                    filter(
                        (
                            target
                        ): target is { connectorId: string; processGroupId: string; errorContext: ErrorContextKey } =>
                            target.processGroupId !== null && target.processGroupId !== undefined
                    ),
                    distinctUntilChanged(
                        (previous, current) =>
                            previous.connectorId === current.connectorId &&
                            previous.processGroupId === current.processGroupId
                    ),
                    map((target) => ConnectorCanvasActions.loadConnectorParameterContext(target))
                )
            )
        )
    );

    loadConnectorParameterContext$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.loadConnectorParameterContext),
            switchMap((action) =>
                this.connectorService.getConnectorParameterContext(action.connectorId, action.processGroupId).pipe(
                    map((parameterContext) =>
                        ConnectorCanvasActions.loadConnectorParameterContextSuccess({ parameterContext })
                    ),
                    catchError((error) =>
                        of(
                            ConnectorCanvasActions.loadConnectorParameterContextFailure({
                                errorContext: {
                                    errors: [this.errorHelper.getErrorString(error)],
                                    context: action.errorContext
                                }
                            })
                        )
                    )
                )
            )
        )
    );

    /**
     * Surface parameter context load failures via a banner so the user sees that
     * parameter references in the canvas property tables will not resolve.
     */
    loadConnectorParameterContextFailure$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.loadConnectorParameterContextFailure),
            map((action) => ErrorActions.addBannerError({ errorContext: action.errorContext }))
        )
    );

    /**
     * Translate a reload request into a fresh {@link loadConnectorFlow} for the
     * connector and process group currently mounted on the canvas.
     *
     * The connector and process group ids come from the canvas state (set by the
     * most recent successful load) rather than the route so that a reload fired
     * mid-navigation does not race the URL change. A 1 second throttle protects
     * against rapid back-to-back reload requests, and the effect is a no-op when
     * the canvas has not yet completed an initial load.
     */
    reloadConnectorFlow$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.reloadConnectorFlow),
            throttleTime(1000),
            concatLatestFrom(() => [this.store.select(selectConnectorId), this.store.select(selectProcessGroupId)]),
            filter(([, connectorId, processGroupId]) => !!connectorId && processGroupId != null),
            switchMap(([, connectorId, processGroupId]) => {
                this.lastReload = Date.now();

                return of(
                    ConnectorCanvasActions.loadConnectorFlow({
                        connectorId,
                        processGroupId: processGroupId!
                    })
                );
            })
        )
    );

    /**
     * Drive periodic refresh of the connector flow.
     *
     * The interval ticks every 30 seconds and is torn down when
     * {@link stopConnectorCanvasPolling} is dispatched (typically from the
     * canvas component's ngOnDestroy). Polling is skipped when the document is
     * hidden so background tabs do not generate load, and it is also skipped
     * while a load is already in flight to avoid racing user-initiated process
     * group navigations.
     */
    startConnectorCanvasPolling$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.startConnectorCanvasPolling),
            switchMap(() =>
                interval(30000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(ConnectorCanvasActions.stopConnectorCanvasPolling)))
                )
            ),
            concatLatestFrom(() => [
                this.store.select(selectDocumentVisibilityState),
                this.store.select(selectLoadingStatus)
            ]),
            // Skip polling when the document is hidden or while a flow load is
            // already in flight. Without the loading guard the polling-driven
            // reload would race a user-initiated process group navigation: the
            // load effect uses switchMap, so a polling reload would cancel the
            // pending navigation load and replace it with a load of the
            // previously-rendered group, leaving the URL and canvas out of sync.
            filter(([, documentVisibility]) => documentVisibility.documentVisibility === DocumentVisibility.Visible),
            filter(([, , loadingStatus]) => loadingStatus !== 'loading'),
            switchMap(() => of(ConnectorCanvasActions.reloadConnectorFlow()))
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
     * Navigate to the controller services view for the supplied process group.
     * The current route's process group is still consulted to compute the
     * back-navigation target so the user returns to the canvas they came from.
     * The routeBoundary includes the controller-services segment so back
     * navigation clears as soon as the user navigates away from the controller
     * services view.
     */
    navigateToControllerServices$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorCanvasActions.navigateToControllerServices),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectProcessGroupIdFromRoute)
                ]),
                tap(([action, connectorId, currentProcessGroupId]) => {
                    const routeBoundary = [
                        '/connectors',
                        connectorId,
                        'canvas',
                        action.processGroupId,
                        'controller-services'
                    ];
                    this.router.navigate(routeBoundary, {
                        state: {
                            backNavigation: {
                                route: ['/connectors', connectorId, 'canvas', currentProcessGroupId],
                                routeBoundary,
                                context: 'process group'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    /**
     * Navigate to a specific controller service within a process group, appending
     * the serviceId for deep linking.
     */
    navigateToControllerService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorCanvasActions.navigateToControllerService),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectProcessGroupIdFromRoute)
                ]),
                tap(([action, connectorId, currentProcessGroupId]) => {
                    const routeBoundary = [
                        '/connectors',
                        connectorId,
                        'canvas',
                        action.processGroupId,
                        'controller-services',
                        action.serviceId
                    ];
                    this.router.navigate(routeBoundary, {
                        state: {
                            backNavigation: {
                                route: ['/connectors', connectorId, 'canvas', currentProcessGroupId],
                                routeBoundary,
                                context: 'process group'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    /**
     * Navigate to the queue listing page for a connection. Provides back navigation
     * back to the originating connection on the connector canvas.
     */
    navigateToQueueListing$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorCanvasActions.navigateToQueueListing),
                map((action) => action.request),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectProcessGroupIdFromRoute)
                ]),
                tap(([request, connectorId, processGroupId]) => {
                    const routeBoundary: string[] = ['/queue', request.connectionId];
                    this.router.navigate([...routeBoundary], {
                        state: {
                            backNavigation: {
                                route: [
                                    '/connectors',
                                    connectorId,
                                    'canvas',
                                    processGroupId,
                                    ComponentType.Connection,
                                    request.connectionId
                                ],
                                routeBoundary,
                                context: 'connection'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    /**
     * Refreshes the connector canvas after a queue has been emptied from this surface.
     * Listens for the shared queueEmptied event and filters on source so flow-designer
     * empties do not trigger a connector-canvas refresh.
     */
    refreshAfterQueueEmptied$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EmptyQueueActions.queueEmptied),
            filter((action) => action.source === 'connector-canvas'),
            concatLatestFrom(() => [
                this.store.select(selectConnectorIdFromRoute),
                this.store.select(selectProcessGroupIdFromRoute)
            ]),
            filter(([, connectorId, processGroupId]) => connectorId != null && processGroupId != null),
            switchMap(([, connectorId, processGroupId]) =>
                of(
                    ConnectorCanvasActions.loadConnectorFlow({
                        connectorId: connectorId!,
                        processGroupId: processGroupId!
                    })
                )
            )
        )
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

        // Provide no-op stubs for callbacks not needed in read-only mode. `goToParameter` is
        // intentionally left undefined so the property table hides the "Go to Parameter" affordance
        // for the connector canvas, while parameter values still render in the value tip.
        instance.createNewProperty = () => NEVER;
        instance.createNewService = () => NEVER;
        instance.convertToParameter = () => NEVER;
        instance.goToService = () => undefined;

        // EditProcessor only exposes `parameterContext`; the underlying property table
        // disables parameter affordances on its own when parameterContext is undefined.
        bindConnectorParameterContext(this.store, dialogRef.afterClosed(), (parameterContext) => {
            instance.parameterContext = parameterContext ?? undefined;
        });
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
