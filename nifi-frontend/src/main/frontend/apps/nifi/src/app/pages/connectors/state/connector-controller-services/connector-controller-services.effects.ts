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
import { Router } from '@angular/router';
import { MatDialog } from '@angular/material/dialog';
import { HttpErrorResponse } from '@angular/common/http';
import { asyncScheduler, combineLatest, EMPTY, from, interval, NEVER, of } from 'rxjs';
import {
    catchError,
    distinctUntilChanged,
    filter,
    map,
    pairwise,
    startWith,
    switchMap,
    take,
    takeUntil,
    tap
} from 'rxjs/operators';
import { Store } from '@ngrx/store';
import {
    ComponentType,
    ConnectorEntity,
    ControllerServiceReferencingComponentDTO,
    LARGE_DIALOG,
    SMALL_DIALOG,
    XL_DIALOG,
    YesNoDialog
} from '@nifi/shared';
import { ConnectorService } from '../../service/connector.service';
import { ErrorContextKey } from '../../../../state/error';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { EditControllerService } from '../../../../ui/common/controller-service/edit-controller-service/edit-controller-service.component';
import { CreateControllerService } from '../../../../ui/common/controller-service/create-controller-service/create-controller-service.component';
import { EnableControllerService } from '../../../../ui/common/controller-service/enable-controller-service/enable-controller-service.component';
import { DisableControllerService } from '../../../../ui/common/controller-service/disable-controller-service/disable-controller-service.component';
import { ChangeComponentVersionDialog } from '../../../../ui/common/change-component-version-dialog/change-component-version-dialog';
import { EditControllerServiceDialogRequest, OpenChangeComponentVersionDialogRequest } from '../../../../state/shared';
import * as ConnectorControllerServicesActions from './connector-controller-services.actions';
import {
    selectConnectorControllerServicesSaving,
    selectConnectorIdFromRoute,
    selectProcessGroupIdFromRoute
} from './connector-controller-services.selectors';
import { bindConnectorParameterContext } from '../connector-canvas/bind-connector-parameter-context';
import { bindGoToService } from '../connector-canvas/bind-go-to-service';
import { ControllerServiceService } from '../../../flow-designer/service/controller-service.service';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { Client } from '../../../../service/client.service';
import {
    selectControllerServiceTypes,
    selectExtensionTypesLoadingStatus
} from '../../../../state/extension-types/extension-types.selectors';
import {
    resetPropertyVerificationState,
    verifyProperties
} from '../../../../state/property-verification/property-verification.actions';
import {
    selectPropertyVerificationResults,
    selectPropertyVerificationStatus
} from '../../../../state/property-verification/property-verification.selectors';
import { VerifyPropertiesRequestContext } from '../../../../state/property-verification';
import {
    loadConnectorEntity,
    loadConnectorEntitySuccess
} from '../connector-canvas-entity/connector-canvas-entity.actions';
import { selectConnectorCanvasEntity } from '../connector-canvas-entity/connector-canvas-entity.selectors';
import { DocumentVisibility } from '../../../../state/document-visibility';
import { selectDocumentVisibilityState } from '../../../../state/document-visibility/document-visibility.selectors';
import { BackNavigation } from '../../../../state/navigation';

const CONNECTOR_ENTITY_POLL_MS = 15_000;

@Injectable()
export class ConnectorControllerServicesEffects {
    private actions$ = inject(Actions);
    private router = inject(Router);
    private dialog = inject(MatDialog);
    private store = inject(Store);
    private connectorService = inject(ConnectorService);
    private controllerServiceService = inject(ControllerServiceService);
    private extensionTypesService = inject(ExtensionTypesService);
    private propertyTableHelperService = inject(PropertyTableHelperService);
    private client = inject(Client);
    private errorHelper = inject(ErrorHelper);

    /**
     * Load controller services for a connector's process group. Combines the
     * controller services response with the flow response to capture the
     * breadcrumb that drives navigation context.
     */
    loadConnectorControllerServices$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorControllerServicesActions.loadConnectorControllerServices),
            map((action) => action.request),
            switchMap((request) =>
                combineLatest([
                    this.connectorService.getConnectorControllerServices(request.connectorId, request.processGroupId),
                    this.connectorService.getConnectorFlow(request.connectorId, request.processGroupId)
                ]).pipe(
                    map(([controllerServicesResponse, flowResponse]) =>
                        ConnectorControllerServicesActions.loadConnectorControllerServicesSuccess({
                            response: {
                                connectorId: request.connectorId,
                                processGroupId: request.processGroupId,
                                controllerServices: controllerServicesResponse.controllerServices ?? [],
                                breadcrumb: flowResponse.processGroupFlow?.breadcrumb ?? null,
                                loadedTimestamp: controllerServicesResponse.currentTime ?? new Date().toISOString()
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ConnectorControllerServicesActions.loadConnectorControllerServicesFailure({
                                errorContext: {
                                    errors: [this.errorHelper.getErrorString(error)],
                                    context: ErrorContextKey.CONTROLLER_SERVICES
                                }
                            })
                        )
                    )
                )
            )
        )
    );

    /**
     * Surface load failures via a banner above the controller services view.
     */
    loadConnectorControllerServicesFailure$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorControllerServicesActions.loadConnectorControllerServicesFailure),
            map((action) => ErrorActions.addBannerError({ errorContext: action.errorContext }))
        )
    );

    /**
     * Selecting a controller service updates the route so the selection can be
     * deep linked. Uses replaceUrl so back navigation is not polluted with each
     * intermediate selection.
     */
    selectConnectorControllerService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.selectConnectorControllerService),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(
                        [
                            '/connectors',
                            request.connectorId,
                            'canvas',
                            request.processGroupId,
                            'controller-services',
                            request.serviceId
                        ],
                        { replaceUrl: true }
                    );
                })
            ),
        { dispatch: false }
    );

    /**
     * Opens the EditControllerService dialog in read-only mode. The dialog
     * honors the readonly flag on the request and forces a strictly read-only
     * view regardless of the underlying entity's permissions or run status.
     */
    openViewControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.openViewControllerServiceDialog),
                tap((action) => {
                    const dialogRequest: EditControllerServiceDialogRequest = {
                        id: action.controllerService.id,
                        controllerService: action.controllerService,
                        readonly: true
                    };

                    const dialogRef = this.dialog.open(EditControllerService, {
                        ...XL_DIALOG,
                        autoFocus: 'dialog',
                        data: dialogRequest,
                        id: action.controllerService.id
                    });

                    const instance = dialogRef.componentInstance;
                    // Read-only mode: stub callbacks the property table needs but never invokes.
                    // `goToParameter` is intentionally left undefined so the property table hides
                    // the "Go to Parameter" affordance for the connector canvas, while parameter
                    // values still render in the value tip.
                    instance.createNewService = () => NEVER;
                    instance.convertToParameter = () => NEVER;
                    instance.goToService = bindGoToService(
                        this.store,
                        this.connectorService,
                        this.errorHelper,
                        dialogRef,
                        this.store.select(selectConnectorIdFromRoute),
                        ErrorContextKey.CONTROLLER_SERVICES
                    );

                    bindConnectorParameterContext(
                        this.store,
                        dialogRef.afterClosed(),
                        (parameterContext, supportsParameters) => {
                            instance.parameterContext = parameterContext ?? undefined;
                            instance.supportsParameters = supportsParameters;
                        }
                    );
                })
            ),
        { dispatch: false }
    );

    /**
     * Open the configure controller service dialog in editable mode (troubleshooting only).
     * Fetches component history, wires property-table callbacks, and dispatches
     * saveConnectorControllerService on submit.
     */
    openConfigureConnectorControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.openConfigureConnectorControllerServiceDialog),
                map((action) => action.request),
                filter((request) => request.controllerService.component != null),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectProcessGroupIdFromRoute)
                ]),
                switchMap(([request, connectorId, processGroupId]) =>
                    from(this.propertyTableHelperService.getComponentHistory(request.id)).pipe(
                        map((history) => ({
                            enrichedRequest: { ...request, history: history.componentHistory },
                            connectorId,
                            processGroupId
                        })),
                        // Swallow the error so the outer switchMap stays alive and later
                        // configure requests still open the dialog.
                        catchError((err: HttpErrorResponse) => {
                            this.store.dispatch(
                                ErrorActions.addBannerError({
                                    errorContext: {
                                        errors: [this.errorHelper.getErrorString(err)],
                                        context: ErrorContextKey.CONTROLLER_SERVICES
                                    }
                                })
                            );
                            return EMPTY;
                        })
                    )
                ),
                tap(({ enrichedRequest, connectorId, processGroupId }) => {
                    const serviceId = enrichedRequest.id;

                    const dialogRef = this.dialog.open(EditControllerService, {
                        ...XL_DIALOG,
                        autoFocus: 'dialog',
                        data: enrichedRequest,
                        id: serviceId
                    });

                    const instance = dialogRef.componentInstance;

                    instance.saving$ = this.store.select(selectConnectorControllerServicesSaving);
                    instance.convertToParameter = () => NEVER;

                    instance.propertyVerificationResults$ = this.store.select(selectPropertyVerificationResults);
                    instance.propertyVerificationStatus$ = this.store.select(selectPropertyVerificationStatus);
                    instance.verify
                        .pipe(takeUntil(dialogRef.afterClosed()))
                        .subscribe((verificationRequest: VerifyPropertiesRequestContext) => {
                            this.store.dispatch(verifyProperties({ request: verificationRequest }));
                        });

                    instance.createNewProperty = this.propertyTableHelperService.createNewProperty(
                        serviceId,
                        this.controllerServiceService
                    );

                    instance.createNewService = this.propertyTableHelperService.createNewService(
                        serviceId,
                        this.controllerServiceService,
                        this.controllerServiceService,
                        processGroupId!,
                        (createdService) => {
                            this.store.dispatch(
                                ConnectorControllerServicesActions.inlineCreateConnectorControllerServiceSuccess({
                                    response: { controllerService: createdService }
                                })
                            );
                        }
                    );

                    instance.goToService = (targetServiceId: string) => {
                        this.controllerServiceService.getControllerService(targetServiceId).subscribe({
                            next: (serviceEntity) => {
                                const processGroupId = serviceEntity.parentGroupId;
                                if (!processGroupId) {
                                    this.store.dispatch(
                                        ErrorActions.addBannerError({
                                            errorContext: {
                                                errors: ['Unable to determine the Controller Service parent group.'],
                                                context: ErrorContextKey.CONTROLLER_SERVICES
                                            }
                                        })
                                    );
                                    return;
                                }
                                this.router.navigate([
                                    '/connectors',
                                    connectorId,
                                    'canvas',
                                    processGroupId,
                                    'controller-services',
                                    serviceEntity.id
                                ]);
                            },
                            error: (err: HttpErrorResponse) => {
                                this.store.dispatch(
                                    ErrorActions.addBannerError({
                                        errorContext: {
                                            errors: [this.errorHelper.getErrorString(err)],
                                            context: ErrorContextKey.CONTROLLER_SERVICES
                                        }
                                    })
                                );
                            }
                        });
                    };

                    bindConnectorParameterContext(
                        this.store,
                        dialogRef.afterClosed(),
                        (parameterContext, supportsParameters) => {
                            instance.parameterContext = parameterContext ?? undefined;
                            instance.supportsParameters = supportsParameters;
                        }
                    );

                    // Reference types with no connector-canvas equivalent (ReportingTask,
                    // ParameterProvider, etc.) are a no-op. Navigation closes the dialog via
                    // the app-level NavigationStart hook, discarding any unsaved changes.
                    instance.goToReferencingComponent = (component: ControllerServiceReferencingComponentDTO) => {
                        if (!connectorId) {
                            return;
                        }
                        const route = this.getRouteForConnectorReference(connectorId, component);
                        if (route) {
                            this.router.navigate(route);
                        }
                    };

                    // This dialog does not wire a save-before-navigate flow, so submissions
                    // never carry postUpdateNavigation. saveConnectorControllerServiceSuccess$
                    // handles the listing refresh.
                    instance.editControllerService
                        .pipe(takeUntil(dialogRef.afterClosed()))
                        .subscribe((updateRequest) => {
                            this.store.dispatch(
                                ConnectorControllerServicesActions.saveConnectorControllerService({
                                    request: {
                                        id: serviceId,
                                        uri: enrichedRequest.controllerService.uri,
                                        payload: updateRequest.payload
                                    }
                                })
                            );
                        });

                    dialogRef.afterClosed().subscribe(() => {
                        this.store.dispatch(resetPropertyVerificationState());
                    });
                })
            ),
        { dispatch: false }
    );

    /**
     * Save an edited controller service.
     */
    saveConnectorControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorControllerServicesActions.saveConnectorControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.controllerServiceService.updateControllerService(request)).pipe(
                    map((response) =>
                        ConnectorControllerServicesActions.saveConnectorControllerServiceSuccess({
                            response
                        })
                    ),
                    catchError((err: HttpErrorResponse) =>
                        of(
                            ConnectorControllerServicesActions.saveConnectorControllerServiceFailure({
                                error: this.errorHelper.getErrorString(err)
                            })
                        )
                    )
                )
            )
        )
    );

    /**
     * On any successful save (configure or change-version), close the open dialogs
     * and reload the listing so the row reflects the new version or property values.
     */
    saveConnectorControllerServiceSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.saveConnectorControllerServiceSuccess),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectProcessGroupIdFromRoute)
                ]),
                tap(([, connectorId, processGroupId]) => {
                    this.dialog.closeAll();
                    if (connectorId && processGroupId) {
                        this.store.dispatch(
                            ConnectorControllerServicesActions.loadConnectorControllerServices({
                                request: { connectorId, processGroupId }
                            })
                        );
                    }
                })
            ),
        { dispatch: false }
    );

    saveConnectorControllerServiceFailure$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorControllerServicesActions.saveConnectorControllerServiceFailure),
            map((action) =>
                ErrorActions.addBannerError({
                    errorContext: {
                        errors: [action.error],
                        context: ErrorContextKey.CONTROLLER_SERVICES
                    }
                })
            )
        )
    );

    // ─── Create ──────────────────────────────────────────────────────────────────

    openNewConnectorControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.openNewConnectorControllerServiceDialog),
                concatLatestFrom(() => this.store.select(selectProcessGroupIdFromRoute)),
                tap(([, processGroupId]) => {
                    const dialogRef = this.dialog.open(CreateControllerService, { ...XL_DIALOG });

                    dialogRef.componentInstance.saving$ = this.store.select(selectConnectorControllerServicesSaving);
                    dialogRef.componentInstance.controllerServiceTypes$ =
                        this.store.select(selectControllerServiceTypes);
                    dialogRef.componentInstance.controllerServiceTypesLoadingStatus$ = this.store.select(
                        selectExtensionTypesLoadingStatus
                    );

                    dialogRef.componentInstance.createControllerService.pipe(take(1)).subscribe((serviceType) => {
                        this.store.dispatch(
                            ConnectorControllerServicesActions.createConnectorControllerService({
                                request: {
                                    revision: {
                                        clientId: this.client.getClientId(),
                                        version: 0
                                    },
                                    processGroupId: processGroupId!,
                                    controllerServiceType: serviceType.type,
                                    controllerServiceBundle: serviceType.bundle
                                }
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    createConnectorControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorControllerServicesActions.createConnectorControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.controllerServiceService.createControllerService(request)).pipe(
                    map((response) =>
                        ConnectorControllerServicesActions.createConnectorControllerServiceSuccess({
                            response: { controllerService: response }
                        })
                    ),
                    catchError((err: HttpErrorResponse) => {
                        this.dialog.closeAll();
                        return of(
                            ConnectorControllerServicesActions.createConnectorControllerServiceFailure({
                                error: this.errorHelper.getErrorString(err)
                            })
                        );
                    })
                )
            )
        )
    );

    createConnectorControllerServiceSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.createConnectorControllerServiceSuccess),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectProcessGroupIdFromRoute)
                ]),
                tap(([{ response }, connectorId, processGroupId]) => {
                    this.dialog.closeAll();

                    if (connectorId && processGroupId) {
                        this.store.dispatch(
                            ConnectorControllerServicesActions.selectConnectorControllerService({
                                request: {
                                    connectorId,
                                    processGroupId,
                                    serviceId: response.controllerService.id
                                }
                            })
                        );
                        this.store.dispatch(
                            ConnectorControllerServicesActions.loadConnectorControllerServices({
                                request: { connectorId, processGroupId }
                            })
                        );
                    }
                })
            ),
        { dispatch: false }
    );

    createConnectorControllerServiceFailure$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorControllerServicesActions.createConnectorControllerServiceFailure),
            map((action) =>
                ErrorActions.addBannerError({
                    errorContext: {
                        errors: [action.error],
                        context: ErrorContextKey.CONTROLLER_SERVICES
                    }
                })
            )
        )
    );

    // ─── Enable ──────────────────────────────────────────────────────────────────

    openEnableConnectorControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.openEnableConnectorControllerServiceDialog),
                map((action) => action.request),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectProcessGroupIdFromRoute)
                ]),
                tap(([request, connectorId, processGroupId]) => {
                    const dialogRef = this.dialog.open(EnableControllerService, {
                        ...XL_DIALOG,
                        data: request,
                        id: request.id
                    });

                    dialogRef.componentInstance.goToReferencingComponent = (
                        component: ControllerServiceReferencingComponentDTO
                    ) => {
                        if (!connectorId) {
                            return;
                        }
                        const route = this.getRouteForConnectorReference(connectorId, component);
                        if (route) {
                            this.router.navigate(route);
                        }
                    };

                    // A dialog closed by navigation ('ROUTED') skips the reload so it does not
                    // race the page the user is navigating to.
                    dialogRef.afterClosed().subscribe((response) => {
                        if (response === 'ROUTED') {
                            return;
                        }
                        if (connectorId && processGroupId) {
                            this.store.dispatch(
                                ConnectorControllerServicesActions.loadConnectorControllerServices({
                                    request: { connectorId, processGroupId }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    // ─── Disable ─────────────────────────────────────────────────────────────────

    openDisableConnectorControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.openDisableConnectorControllerServiceDialog),
                map((action) => action.request),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectProcessGroupIdFromRoute)
                ]),
                tap(([request, connectorId, processGroupId]) => {
                    const dialogRef = this.dialog.open(DisableControllerService, {
                        ...XL_DIALOG,
                        data: request,
                        id: request.id
                    });

                    dialogRef.componentInstance.goToReferencingComponent = (
                        component: ControllerServiceReferencingComponentDTO
                    ) => {
                        if (!connectorId) {
                            return;
                        }
                        const route = this.getRouteForConnectorReference(connectorId, component);
                        if (route) {
                            this.router.navigate(route);
                        }
                    };

                    // A dialog closed by navigation ('ROUTED') skips the reload so it does not
                    // race the page the user is navigating to.
                    dialogRef.afterClosed().subscribe((response) => {
                        if (response === 'ROUTED') {
                            return;
                        }
                        if (connectorId && processGroupId) {
                            this.store.dispatch(
                                ConnectorControllerServicesActions.loadConnectorControllerServices({
                                    request: { connectorId, processGroupId }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    // ─── Change Version ───────────────────────────────────────────────────────────

    openChangeConnectorControllerServiceVersionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.openChangeConnectorControllerServiceVersionDialog),
                map((action) => action.request),
                switchMap((request) =>
                    from(
                        this.extensionTypesService.getControllerServiceVersionsForType(request.type, request.bundle)
                    ).pipe(
                        map(
                            (response) =>
                                ({
                                    fetchRequest: request,
                                    componentVersions: response.controllerServiceTypes
                                }) as OpenChangeComponentVersionDialogRequest
                        ),
                        tap({
                            error: (err: HttpErrorResponse) => {
                                this.store.dispatch(
                                    ErrorActions.addBannerError({
                                        errorContext: {
                                            errors: [this.errorHelper.getErrorString(err)],
                                            context: ErrorContextKey.CONTROLLER_SERVICES
                                        }
                                    })
                                );
                            }
                        })
                    )
                ),
                tap((request) => {
                    const dialogRef = this.dialog.open(ChangeComponentVersionDialog, {
                        ...LARGE_DIALOG,
                        data: request
                    });

                    dialogRef.componentInstance.changeVersion.pipe(take(1)).subscribe((newVersion) => {
                        this.store.dispatch(
                            ConnectorControllerServicesActions.saveConnectorControllerService({
                                request: {
                                    id: request.fetchRequest.id,
                                    uri: request.fetchRequest.uri,
                                    payload: {
                                        component: {
                                            bundle: newVersion.bundle,
                                            id: request.fetchRequest.id
                                        },
                                        revision: request.fetchRequest.revision
                                    }
                                }
                            })
                        );
                    });

                    // ChangeComponentVersionDialog renders no error banner of its own, so close it
                    // on failure to reveal the page banner from saveConnectorControllerServiceFailure$.
                    this.actions$
                        .pipe(
                            ofType(ConnectorControllerServicesActions.saveConnectorControllerServiceFailure),
                            take(1),
                            takeUntil(dialogRef.afterClosed())
                        )
                        .subscribe(() => dialogRef.close());
                })
            ),
        { dispatch: false }
    );

    // ─── Delete ───────────────────────────────────────────────────────────────────

    promptConnectorControllerServiceDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.promptConnectorControllerServiceDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogRef = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Delete Controller Service',
                            message: `Delete controller service ${request.controllerService.component?.name ?? request.controllerService.id}?`
                        }
                    });

                    dialogRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            ConnectorControllerServicesActions.deleteConnectorControllerService({ request })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    deleteConnectorControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorControllerServicesActions.deleteConnectorControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.controllerServiceService.deleteControllerService(request)).pipe(
                    map(() => ConnectorControllerServicesActions.deleteConnectorControllerServiceSuccess()),
                    catchError((err: HttpErrorResponse) =>
                        of(
                            ConnectorControllerServicesActions.deleteConnectorControllerServiceFailure({
                                error: this.errorHelper.getErrorString(err)
                            })
                        )
                    )
                )
            )
        )
    );

    deleteConnectorControllerServiceSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.deleteConnectorControllerServiceSuccess),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectProcessGroupIdFromRoute)
                ]),
                tap(([, connectorId, processGroupId]) => {
                    if (connectorId && processGroupId) {
                        this.store.dispatch(
                            ConnectorControllerServicesActions.loadConnectorControllerServices({
                                request: { connectorId, processGroupId }
                            })
                        );
                    }
                })
            ),
        { dispatch: false }
    );

    deleteConnectorControllerServiceFailure$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorControllerServicesActions.deleteConnectorControllerServiceFailure),
            map((action) =>
                ErrorActions.addBannerError({
                    errorContext: {
                        errors: [action.error],
                        context: ErrorContextKey.CONTROLLER_SERVICES
                    }
                })
            )
        )
    );

    // ─── Clear Bulletins ──────────────────────────────────────────────────────────

    clearConnectorControllerServiceBulletins$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorControllerServicesActions.clearConnectorControllerServiceBulletins),
            map((action) => action.request),
            switchMap((request) =>
                from(
                    this.controllerServiceService.clearBulletins({
                        id: request.componentId,
                        fromTimestamp: request.fromTimestamp
                    })
                ).pipe(
                    map(() => ConnectorControllerServicesActions.clearConnectorControllerServiceBulletinsSuccess()),
                    catchError((err: HttpErrorResponse) =>
                        of(
                            ConnectorControllerServicesActions.clearConnectorControllerServiceBulletinsFailure({
                                error: this.errorHelper.getErrorString(err)
                            })
                        )
                    )
                )
            )
        )
    );

    clearConnectorControllerServiceBulletinsSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.clearConnectorControllerServiceBulletinsSuccess),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectProcessGroupIdFromRoute)
                ]),
                tap(([, connectorId, processGroupId]) => {
                    if (connectorId && processGroupId) {
                        this.store.dispatch(
                            ConnectorControllerServicesActions.loadConnectorControllerServices({
                                request: { connectorId, processGroupId }
                            })
                        );
                    }
                })
            ),
        { dispatch: false }
    );

    clearConnectorControllerServiceBulletinsFailure$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorControllerServicesActions.clearConnectorControllerServiceBulletinsFailure),
            map((action) =>
                ErrorActions.addBannerError({
                    errorContext: {
                        errors: [action.error],
                        context: ErrorContextKey.CONTROLLER_SERVICES
                    }
                })
            )
        )
    );

    // ─── Entity polling (keeps selectConnectorCanvasEntity fresh on this page) ───

    /**
     * Poll the connector entity every {@link CONNECTOR_ENTITY_POLL_MS} so the
     * troubleshooting state stays current on this route, where the canvas
     * component's own polling is not active. Ticks are skipped while the
     * document is hidden.
     */
    startConnectorControllerServicesEntityPolling$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorControllerServicesActions.startConnectorControllerServicesEntityPolling),
            switchMap(({ connectorId }) =>
                interval(CONNECTOR_ENTITY_POLL_MS, asyncScheduler).pipe(
                    takeUntil(
                        this.actions$.pipe(
                            ofType(ConnectorControllerServicesActions.stopConnectorControllerServicesEntityPolling)
                        )
                    ),
                    concatLatestFrom(() => this.store.select(selectDocumentVisibilityState)),
                    filter(([, visibility]) => visibility.documentVisibility === DocumentVisibility.Visible),
                    map(() => loadConnectorEntity({ connectorId }))
                )
            )
        )
    );

    /**
     * When the connector entity transitions out of TROUBLESHOOTING while the
     * listing page is active, reload the listing so the table returns to
     * read-only with fresh data. The first emission and cross-connector
     * navigation are treated as initial loads rather than transitions.
     *
     * Feature effects outlive the listing page, so the transition is only
     * observed between starting and stopping the listing's entity polling.
     */
    reloadOnTroubleshootingExit$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorControllerServicesActions.startConnectorControllerServicesEntityPolling),
            switchMap(() =>
                this.actions$.pipe(
                    ofType(loadConnectorEntitySuccess),
                    takeUntil(
                        this.actions$.pipe(
                            ofType(ConnectorControllerServicesActions.stopConnectorControllerServicesEntityPolling)
                        )
                    ),
                    concatLatestFrom(() => this.store.select(selectConnectorCanvasEntity)),
                    map(([, connectorEntity]) => connectorEntity),
                    filter((connectorEntity): connectorEntity is ConnectorEntity => connectorEntity !== null),
                    distinctUntilChanged(),
                    startWith(null as ConnectorEntity | null),
                    pairwise()
                )
            ),
            concatLatestFrom(() => [
                this.store.select(selectConnectorIdFromRoute),
                this.store.select(selectProcessGroupIdFromRoute)
            ]),
            switchMap(([[prev, curr], connectorId, processGroupId]) => {
                if (!curr || !connectorId || !processGroupId) {
                    return EMPTY;
                }
                const isInitialOrNewConnector = prev === null || prev.id !== curr.id;
                if (isInitialOrNewConnector) {
                    return EMPTY;
                }
                const exitedTroubleshooting =
                    prev.component.state === 'TROUBLESHOOTING' && curr.component.state !== 'TROUBLESHOOTING';
                if (!exitedTroubleshooting) {
                    return EMPTY;
                }
                return of(
                    ConnectorControllerServicesActions.loadConnectorControllerServices({
                        request: { connectorId, processGroupId }
                    })
                );
            })
        )
    );

    /**
     * Navigate to a controller service in the listing of the process group that defines
     * it. Back returns to the current listing with the service selected.
     */
    navigateToConnectorControllerService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.navigateToConnectorControllerService),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectProcessGroupIdFromRoute)
                ]),
                filter(([, connectorId, currentProcessGroupId]) => !!connectorId && !!currentProcessGroupId),
                tap(([{ processGroupId, serviceId }, connectorId, currentProcessGroupId]) => {
                    const routeBoundary = ['/connectors', connectorId, 'canvas', processGroupId, 'controller-services'];
                    this.router.navigate([...routeBoundary, serviceId], {
                        state: {
                            backNavigation: {
                                route: [
                                    '/connectors',
                                    connectorId,
                                    'canvas',
                                    currentProcessGroupId,
                                    'controller-services',
                                    serviceId
                                ],
                                routeBoundary,
                                context: 'Controller Service'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    /**
     * Navigate to the Advanced UI for a controller service. The component route is
     * the listing with the service selected so Back returns to the highlighted row.
     */
    navigateToAdvancedConnectorControllerServiceUi$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.navigateToAdvancedConnectorControllerServiceUi),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectProcessGroupIdFromRoute)
                ]),
                filter(([, connectorId, processGroupId]) => !!connectorId && !!processGroupId),
                tap(([{ id }, connectorId, processGroupId]) => {
                    const componentRoute = [
                        '/connectors',
                        connectorId,
                        'canvas',
                        processGroupId,
                        'controller-services',
                        id
                    ];
                    const routeBoundary = [...componentRoute, 'advanced'];
                    this.router.navigate(routeBoundary, {
                        state: {
                            backNavigation: {
                                route: componentRoute,
                                routeBoundary,
                                context: 'Controller Service'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    /**
     * Builds the connector-canvas route for a referencing component. Returns null for
     * reference types with no connector-canvas equivalent (ReportingTask,
     * ParameterProvider, RegistryClient, or a root-scoped ControllerService).
     */
    private getRouteForConnectorReference(
        connectorId: string,
        reference: ControllerServiceReferencingComponentDTO
    ): string[] | null {
        if (!reference.groupId) {
            return null;
        }
        if (reference.referenceType === 'Processor') {
            return ['/connectors', connectorId, 'canvas', reference.groupId, ComponentType.Processor, reference.id];
        }
        if (reference.referenceType === 'ControllerService') {
            return ['/connectors', connectorId, 'canvas', reference.groupId, 'controller-services', reference.id];
        }
        return null;
    }
}
