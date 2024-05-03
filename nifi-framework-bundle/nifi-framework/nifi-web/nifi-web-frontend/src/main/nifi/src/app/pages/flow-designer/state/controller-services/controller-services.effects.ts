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
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import * as ControllerServicesActions from './controller-services.actions';
import { catchError, combineLatest, from, map, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { selectControllerServiceTypes } from '../../../../state/extension-types/extension-types.selectors';
import { CreateControllerService } from '../../../../ui/common/controller-service/create-controller-service/create-controller-service.component';
import { Client } from '../../../../service/client.service';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { EditControllerService } from '../../../../ui/common/controller-service/edit-controller-service/edit-controller-service.component';
import {
    ComponentType,
    ControllerServiceReferencingComponent,
    EditControllerServiceDialogRequest,
    OpenChangeComponentVersionDialogRequest,
    UpdateControllerServiceRequest
} from '../../../../state/shared';
import { Router } from '@angular/router';
import {
    selectCurrentProcessGroupId,
    selectParameterContext,
    selectSaving,
    selectStatus
} from './controller-services.selectors';
import { ControllerServiceService } from '../../service/controller-service.service';
import { EnableControllerService } from '../../../../ui/common/controller-service/enable-controller-service/enable-controller-service.component';
import { DisableControllerService } from '../../../../ui/common/controller-service/disable-controller-service/disable-controller-service.component';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { ParameterHelperService } from '../../service/parameter-helper.service';
import { LARGE_DIALOG, SMALL_DIALOG, XL_DIALOG } from '../../../../index';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { ChangeComponentVersionDialog } from '../../../../ui/common/change-component-version-dialog/change-component-version-dialog';
import { FlowService } from '../../service/flow.service';

@Injectable()
export class ControllerServicesEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private client: Client,
        private controllerServiceService: ControllerServiceService,
        private flowService: FlowService,
        private errorHelper: ErrorHelper,
        private dialog: MatDialog,
        private router: Router,
        private propertyTableHelperService: PropertyTableHelperService,
        private parameterHelperService: ParameterHelperService,
        private extensionTypesService: ExtensionTypesService
    ) {}

    loadControllerServices$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServicesActions.loadControllerServices),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectStatus)),
            switchMap(([request, status]) =>
                combineLatest([
                    this.controllerServiceService.getControllerServices(request.processGroupId),
                    this.controllerServiceService.getFlow(request.processGroupId)
                ]).pipe(
                    map(([controllerServicesResponse, flowResponse]) =>
                        ControllerServicesActions.loadControllerServicesSuccess({
                            response: {
                                processGroupId: flowResponse.processGroupFlow.id,
                                controllerServices: controllerServicesResponse.controllerServices,
                                loadedTimestamp: controllerServicesResponse.currentTime,
                                breadcrumb: flowResponse.processGroupFlow.breadcrumb,
                                parameterContext: flowResponse.processGroupFlow.parameterContext ?? null
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.errorHelper.handleLoadingError(status, errorResponse))
                    )
                )
            )
        )
    );

    openNewControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.openNewControllerServiceDialog),
                concatLatestFrom(() => [
                    this.store.select(selectControllerServiceTypes),
                    this.store.select(selectCurrentProcessGroupId)
                ]),
                tap(([, controllerServiceTypes, processGroupId]) => {
                    const dialogReference = this.dialog.open(CreateControllerService, {
                        ...LARGE_DIALOG,
                        data: {
                            controllerServiceTypes
                        }
                    });

                    dialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    dialogReference.componentInstance.createControllerService
                        .pipe(take(1))
                        .subscribe((controllerServiceType) => {
                            this.store.dispatch(
                                ControllerServicesActions.createControllerService({
                                    request: {
                                        revision: {
                                            clientId: this.client.getClientId(),
                                            version: 0
                                        },
                                        processGroupId,
                                        controllerServiceType: controllerServiceType.type,
                                        controllerServiceBundle: controllerServiceType.bundle
                                    }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    createControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServicesActions.createControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.controllerServiceService.createControllerService(request)).pipe(
                    map((response) =>
                        ControllerServicesActions.createControllerServiceSuccess({
                            response: {
                                controllerService: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.dialog.closeAll();
                        return of(
                            ErrorActions.snackBarError({ error: this.errorHelper.getErrorString(errorResponse) })
                        );
                    })
                )
            )
        )
    );

    createControllerServiceSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.createControllerServiceSuccess),
                tap(() => {
                    this.dialog.closeAll();
                })
            ),
        { dispatch: false }
    );

    navigateToEditService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.navigateToEditService),
                map((action) => action.id),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([id, processGroupId]) => {
                    this.router.navigate(['/process-groups', processGroupId, 'controller-services', id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    navigateToAdvancedServiceUi$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.navigateToAdvancedServiceUi),
                map((action) => action.id),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([id, processGroupId]) => {
                    this.router.navigate(['/process-groups', processGroupId, 'controller-services', id, 'advanced']);
                })
            ),
        { dispatch: false }
    );

    openConfigureControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.openConfigureControllerServiceDialog),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                switchMap(([request, processGroupId]) =>
                    from(this.propertyTableHelperService.getComponentHistory(request.id)).pipe(
                        map((history) => {
                            return {
                                ...request,
                                history: history.componentHistory
                            } as EditControllerServiceDialogRequest;
                        }),
                        tap({
                            error: (errorResponse: HttpErrorResponse) => {
                                this.store.dispatch(
                                    ControllerServicesActions.selectControllerService({
                                        request: {
                                            processGroupId,
                                            id: request.id
                                        }
                                    })
                                );
                                this.store.dispatch(
                                    ErrorActions.snackBarError({
                                        error: this.errorHelper.getErrorString(errorResponse)
                                    })
                                );
                            }
                        })
                    )
                ),
                concatLatestFrom(() => [
                    this.store.select(selectParameterContext),
                    this.store.select(selectCurrentProcessGroupId)
                ]),
                switchMap(([request, parameterContextReference, processGroupId]) => {
                    if (parameterContextReference && parameterContextReference.permissions.canRead) {
                        return from(this.flowService.getParameterContext(parameterContextReference.id)).pipe(
                            map((parameterContext) => {
                                return [request, parameterContext, processGroupId];
                            }),
                            tap({
                                error: (errorResponse: HttpErrorResponse) => {
                                    this.store.dispatch(
                                        ControllerServicesActions.selectControllerService({
                                            request: {
                                                processGroupId,
                                                id: request.id
                                            }
                                        })
                                    );
                                    this.store.dispatch(
                                        ErrorActions.snackBarError({
                                            error: this.errorHelper.getErrorString(errorResponse)
                                        })
                                    );
                                }
                            })
                        );
                    }

                    return of([request, null, processGroupId]);
                }),
                tap(([request, parameterContext, processGroupId]) => {
                    const serviceId: string = request.id;

                    const editDialogReference = this.dialog.open(EditControllerService, {
                        ...LARGE_DIALOG,
                        data: request,
                        id: serviceId
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.createNewProperty =
                        this.propertyTableHelperService.createNewProperty(request.id, this.controllerServiceService);

                    const goTo = (commands: string[], destination: string): void => {
                        if (editDialogReference.componentInstance.editControllerServiceForm.dirty) {
                            const saveChangesDialogReference = this.dialog.open(YesNoDialog, {
                                ...SMALL_DIALOG,
                                data: {
                                    title: 'Controller Service Configuration',
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

                    editDialogReference.componentInstance.goToReferencingComponent = (
                        component: ControllerServiceReferencingComponent
                    ) => {
                        const route: string[] = this.getRouteForReference(component);
                        goTo(route, component.referenceType);
                    };

                    if (parameterContext != null) {
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
                            error: (errorResponse: HttpErrorResponse) => {
                                this.store.dispatch(
                                    ErrorActions.snackBarError({
                                        error: this.errorHelper.getErrorString(errorResponse)
                                    })
                                );
                            }
                        });
                    };

                    editDialogReference.componentInstance.createNewService =
                        this.propertyTableHelperService.createNewService(
                            request.id,
                            this.controllerServiceService,
                            this.controllerServiceService,
                            processGroupId,
                            (createResponse) =>
                                this.store.dispatch(
                                    ControllerServicesActions.inlineCreateControllerServiceSuccess({
                                        response: {
                                            controllerService: createResponse
                                        }
                                    })
                                )
                        );

                    editDialogReference.componentInstance.editControllerService
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((updateControllerServiceRequest: UpdateControllerServiceRequest) => {
                            this.store.dispatch(
                                ControllerServicesActions.configureControllerService({
                                    request: {
                                        id: request.controllerService.id,
                                        uri: request.controllerService.uri,
                                        payload: updateControllerServiceRequest.payload,
                                        postUpdateNavigation: updateControllerServiceRequest.postUpdateNavigation
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                ControllerServicesActions.selectControllerService({
                                    request: {
                                        processGroupId,
                                        id: serviceId
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    configureControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServicesActions.configureControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.controllerServiceService.updateControllerService(request)).pipe(
                    map((response) =>
                        ControllerServicesActions.configureControllerServiceSuccess({
                            response: {
                                id: request.id,
                                controllerService: response,
                                postUpdateNavigation: request.postUpdateNavigation
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                            return of(
                                ControllerServicesActions.controllerServicesBannerApiError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            );
                        } else {
                            this.dialog.getDialogById(request.id)?.close('ROUTED');
                            return of(this.errorHelper.fullScreenError(errorResponse));
                        }
                    })
                )
            )
        )
    );

    controllerServicesBannerApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServicesActions.controllerServicesBannerApiError),
            map((action) => action.error),
            switchMap((error) => of(ErrorActions.addBannerError({ error })))
        )
    );

    configureControllerServiceSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.configureControllerServiceSuccess),
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

    openEnableControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.openEnableControllerServiceDialog),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([request, currentProcessGroupId]) => {
                    const serviceId: string = request.id;

                    const enableDialogReference = this.dialog.open(EnableControllerService, {
                        ...XL_DIALOG,
                        data: request,
                        id: serviceId
                    });

                    enableDialogReference.componentInstance.goToReferencingComponent = (
                        component: ControllerServiceReferencingComponent
                    ) => {
                        enableDialogReference.close('ROUTED');

                        const route: string[] = this.getRouteForReference(component);
                        this.router.navigate(route);
                    };

                    enableDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                ControllerServicesActions.loadControllerServices({
                                    request: {
                                        processGroupId: currentProcessGroupId
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    openDisableControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.openDisableControllerServiceDialog),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([request, currentProcessGroupId]) => {
                    const serviceId: string = request.id;

                    const enableDialogReference = this.dialog.open(DisableControllerService, {
                        ...XL_DIALOG,
                        data: request,
                        id: serviceId
                    });

                    enableDialogReference.componentInstance.goToReferencingComponent = (
                        component: ControllerServiceReferencingComponent
                    ) => {
                        enableDialogReference.close('ROUTED');

                        const route: string[] = this.getRouteForReference(component);
                        this.router.navigate(route);
                    };

                    enableDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                ControllerServicesActions.loadControllerServices({
                                    request: {
                                        processGroupId: currentProcessGroupId
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    promptControllerServiceDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.promptControllerServiceDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Delete Controller Service',
                            message: `Delete controller service ${request.controllerService.component.name}?`
                        }
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            ControllerServicesActions.deleteControllerService({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    deleteControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServicesActions.deleteControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.controllerServiceService.deleteControllerService(request)).pipe(
                    map((response) =>
                        ControllerServicesActions.deleteControllerServiceSuccess({
                            response: {
                                controllerService: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(ErrorActions.snackBarError({ error: this.errorHelper.getErrorString(errorResponse) }))
                    )
                )
            )
        )
    );

    selectControllerService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.selectControllerService),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate([
                        '/process-groups',
                        request.processGroupId,
                        'controller-services',
                        request.id
                    ]);
                })
            ),
        { dispatch: false }
    );

    openChangeControllerServiceVersionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.openChangeControllerServiceVersionDialog),
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
                            error: (errorResponse: HttpErrorResponse) => {
                                this.store.dispatch(
                                    ErrorActions.snackBarError({
                                        error: this.errorHelper.getErrorString(errorResponse)
                                    })
                                );
                            }
                        })
                    )
                ),
                tap((request) => {
                    const dialogRequest = this.dialog.open(ChangeComponentVersionDialog, {
                        ...LARGE_DIALOG,
                        data: request,
                        autoFocus: false
                    });

                    dialogRequest.componentInstance.changeVersion.pipe(take(1)).subscribe((newVersion) => {
                        this.store.dispatch(
                            ControllerServicesActions.configureControllerService({
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
                        dialogRequest.close();
                    });
                })
            ),
        { dispatch: false }
    );

    private getRouteForReference(reference: ControllerServiceReferencingComponent): string[] {
        if (reference.referenceType == 'ControllerService') {
            if (reference.groupId == null) {
                return ['/settings', 'management-controller-services', reference.id];
            } else {
                return ['/process-groups', reference.groupId, 'controller-services', reference.id];
            }
        } else if (reference.referenceType == 'ReportingTask') {
            return ['/settings', 'reporting-tasks', reference.id];
        } else if (reference.referenceType == 'Processor') {
            return ['/process-groups', reference.groupId, ComponentType.Processor, reference.id];
        } else if (reference.referenceType == 'FlowAnalysisRule') {
            return ['/settings', 'flow-analysis-rules', reference.id];
        } else if (reference.referenceType == 'ParameterProvider') {
            return ['/settings', 'parameter-providers', reference.id];
        } else {
            return ['/settings', 'registry-clients', reference.id];
        }
    }
}
