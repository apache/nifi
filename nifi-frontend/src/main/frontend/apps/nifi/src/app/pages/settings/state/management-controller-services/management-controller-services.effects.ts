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
import * as ManagementControllerServicesActions from './management-controller-services.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import { catchError, from, map, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { selectControllerServiceTypes } from '../../../../state/extension-types/extension-types.selectors';
import { CreateControllerService } from '../../../../ui/common/controller-service/create-controller-service/create-controller-service.component';
import { Client } from '../../../../service/client.service';
import { EditControllerService } from '../../../../ui/common/controller-service/edit-controller-service/edit-controller-service.component';
import {
    ControllerServiceReferencingComponent,
    EditControllerServiceDialogRequest,
    OpenChangeComponentVersionDialogRequest,
    UpdateControllerServiceRequest
} from '../../../../state/shared';
import { Router } from '@angular/router';
import { selectSaving, selectStatus } from './management-controller-services.selectors';
import { EnableControllerService } from '../../../../ui/common/controller-service/enable-controller-service/enable-controller-service.component';
import { DisableControllerService } from '../../../../ui/common/controller-service/disable-controller-service/disable-controller-service.component';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { ComponentType, LARGE_DIALOG, SMALL_DIALOG, XL_DIALOG, YesNoDialog } from '@nifi/shared';
import { ChangeComponentVersionDialog } from '../../../../ui/common/change-component-version-dialog/change-component-version-dialog';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import {
    resetPropertyVerificationState,
    verifyProperties
} from '../../../../state/property-verification/property-verification.actions';
import {
    selectPropertyVerificationResults,
    selectPropertyVerificationStatus
} from '../../../../state/property-verification/property-verification.selectors';
import { VerifyPropertiesRequestContext } from '../../../../state/property-verification';
import { BackNavigation } from '../../../../state/navigation';
import { ErrorContextKey } from '../../../../state/error';

@Injectable()
export class ManagementControllerServicesEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private client: Client,
        private managementControllerServiceService: ManagementControllerServiceService,
        private errorHelper: ErrorHelper,
        private dialog: MatDialog,
        private router: Router,
        private propertyTableHelperService: PropertyTableHelperService,
        private extensionTypesService: ExtensionTypesService
    ) {}

    loadManagementControllerServices$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.loadManagementControllerServices),
            concatLatestFrom(() => this.store.select(selectStatus)),
            switchMap(([, status]) =>
                from(this.managementControllerServiceService.getControllerServices()).pipe(
                    map((response) =>
                        ManagementControllerServicesActions.loadManagementControllerServicesSuccess({
                            response: {
                                controllerServices: response.controllerServices,
                                loadedTimestamp: response.currentTime
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
                ofType(ManagementControllerServicesActions.openNewControllerServiceDialog),
                concatLatestFrom(() => this.store.select(selectControllerServiceTypes)),
                tap(([, controllerServiceTypes]) => {
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
                                ManagementControllerServicesActions.createControllerService({
                                    request: {
                                        revision: {
                                            clientId: this.client.getClientId(),
                                            version: 0
                                        },
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
            ofType(ManagementControllerServicesActions.createControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.managementControllerServiceService.createControllerService(request)).pipe(
                    map((response) =>
                        ManagementControllerServicesActions.createControllerServiceSuccess({
                            response: {
                                controllerService: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.dialog.closeAll();
                        return of(
                            ManagementControllerServicesActions.managementControllerServicesSnackbarApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        );
                    })
                )
            )
        )
    );

    createControllerServiceSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.createControllerServiceSuccess),
            map((action) => action.response),
            tap((response) => {
                this.dialog.closeAll();

                this.store.dispatch(
                    ManagementControllerServicesActions.selectControllerService({
                        request: {
                            id: response.controllerService.id
                        }
                    })
                );
            }),
            switchMap(() => of(ManagementControllerServicesActions.loadManagementControllerServices()))
        )
    );

    navigateToEditService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.navigateToEditService),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['/settings', 'management-controller-services', id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    navigateToAdvancedServiceUi$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.navigateToAdvancedServiceUi),
                map((action) => action.id),
                tap((id) => {
                    const routeBoundary: string[] = ['/settings', 'management-controller-services', id, 'advanced'];
                    this.router.navigate([...routeBoundary], {
                        state: {
                            backNavigation: {
                                route: ['/settings', 'management-controller-services', id],
                                routeBoundary,
                                context: 'Controller Service'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    navigateToManageComponentPolicies$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.navigateToManageComponentPolicies),
                map((action) => action.id),
                tap((id) => {
                    const routeBoundary: string[] = ['/access-policies'];
                    this.router.navigate([...routeBoundary, 'read', 'component', 'controller-services', id], {
                        state: {
                            backNavigation: {
                                route: ['/settings', 'management-controller-services', id],
                                routeBoundary,
                                context: 'Controller Service'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    openConfigureControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.openConfigureControllerServiceDialog),
                map((action) => action.request),
                switchMap((request) =>
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
                                    ManagementControllerServicesActions.selectControllerService({
                                        request: {
                                            id: request.id
                                        }
                                    })
                                );
                                this.store.dispatch(
                                    ManagementControllerServicesActions.managementControllerServicesSnackbarApiError({
                                        error: this.errorHelper.getErrorString(errorResponse)
                                    })
                                );
                            }
                        })
                    )
                ),
                tap((request) => {
                    const serviceId: string = request.id;

                    const editDialogReference = this.dialog.open(EditControllerService, {
                        ...XL_DIALOG,
                        data: request,
                        id: serviceId
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);
                    editDialogReference.componentInstance.supportsParameters = false;

                    editDialogReference.componentInstance.createNewProperty =
                        this.propertyTableHelperService.createNewProperty(
                            request.id,
                            this.managementControllerServiceService
                        );

                    editDialogReference.componentInstance.verify
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((verificationRequest: VerifyPropertiesRequestContext) => {
                            this.store.dispatch(
                                verifyProperties({
                                    request: verificationRequest
                                })
                            );
                        });

                    editDialogReference.componentInstance.propertyVerificationResults$ = this.store.select(
                        selectPropertyVerificationResults
                    );
                    editDialogReference.componentInstance.propertyVerificationStatus$ = this.store.select(
                        selectPropertyVerificationStatus
                    );

                    const goTo = (commands: string[], destination: string, commandBoundary?: string[]): void => {
                        if (editDialogReference.componentInstance.editControllerServiceForm.dirty) {
                            const saveChangesDialogReference = this.dialog.open(YesNoDialog, {
                                ...SMALL_DIALOG,
                                data: {
                                    title: 'Controller Service Configuration',
                                    message: `Save changes before going to this ${destination}?`
                                }
                            });

                            saveChangesDialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                                editDialogReference.componentInstance.submitForm(commands, commandBoundary);
                            });

                            saveChangesDialogReference.componentInstance.no.pipe(take(1)).subscribe(() => {
                                if (commandBoundary) {
                                    this.router.navigate(commands, {
                                        state: {
                                            backNavigation: {
                                                route: [
                                                    '/settings',
                                                    'management-controller-services',
                                                    serviceId,
                                                    'edit'
                                                ],
                                                routeBoundary: commandBoundary,
                                                context: 'Controller Service'
                                            } as BackNavigation
                                        }
                                    });
                                } else {
                                    this.router.navigate(commands);
                                }
                            });
                        } else {
                            if (commandBoundary) {
                                this.router.navigate(commands, {
                                    state: {
                                        backNavigation: {
                                            route: ['/settings', 'management-controller-services', serviceId, 'edit'],
                                            routeBoundary: commandBoundary,
                                            context: 'Controller Service'
                                        } as BackNavigation
                                    }
                                });
                            } else {
                                this.router.navigate(commands);
                            }
                        }
                    };

                    editDialogReference.componentInstance.goToService = (serviceId: string) => {
                        const commandBoundary: string[] = ['/settings', 'management-controller-services'];
                        const commands: string[] = [...commandBoundary, serviceId];
                        goTo(commands, 'Controller Service', commandBoundary);
                    };

                    editDialogReference.componentInstance.goToReferencingComponent = (
                        component: ControllerServiceReferencingComponent
                    ) => {
                        const route: string[] = this.getRouteForReference(component);
                        goTo(route, component.referenceType);
                    };

                    editDialogReference.componentInstance.createNewService =
                        this.propertyTableHelperService.createNewService(
                            request.id,
                            this.managementControllerServiceService,
                            this.managementControllerServiceService,
                            null,
                            (createResponse) =>
                                this.store.dispatch(
                                    ManagementControllerServicesActions.inlineCreateControllerServiceSuccess({
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
                                ManagementControllerServicesActions.configureControllerService({
                                    request: {
                                        id: request.controllerService.id,
                                        uri: request.controllerService.uri,
                                        payload: updateControllerServiceRequest.payload,
                                        postUpdateNavigation: updateControllerServiceRequest.postUpdateNavigation,
                                        postUpdateNavigationBoundary:
                                            updateControllerServiceRequest.postUpdateNavigationBoundary
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        this.store.dispatch(resetPropertyVerificationState());

                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                ManagementControllerServicesActions.selectControllerService({
                                    request: {
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
            ofType(ManagementControllerServicesActions.configureControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.managementControllerServiceService.updateControllerService(request)).pipe(
                    map((response) =>
                        ManagementControllerServicesActions.configureControllerServiceSuccess({
                            response: {
                                id: request.id,
                                controllerService: response,
                                postUpdateNavigation: request.postUpdateNavigation,
                                postUpdateNavigationBoundary: request.postUpdateNavigationBoundary
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                            return of(
                                ManagementControllerServicesActions.managementControllerServicesBannerApiError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            );
                        } else {
                            return of(this.errorHelper.fullScreenError(errorResponse));
                        }
                    })
                )
            )
        )
    );

    managementControllerServicesBannerApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.managementControllerServicesBannerApiError),
            map((action) => action.error),
            switchMap((error) =>
                of(
                    ErrorActions.addBannerError({
                        errorContext: { errors: [error], context: ErrorContextKey.CONTROLLER_SERVICES }
                    })
                )
            )
        )
    );

    managementControllerServicesSnackbarApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.managementControllerServicesSnackbarApiError),
            map((action) => action.error),
            switchMap((error) => of(ErrorActions.snackBarError({ error })))
        )
    );

    configureControllerServiceSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.configureControllerServiceSuccess),
                map((action) => action.response),
                tap((response) => {
                    if (response.postUpdateNavigation) {
                        if (response.postUpdateNavigationBoundary) {
                            this.router.navigate(response.postUpdateNavigation, {
                                state: {
                                    backNavigation: {
                                        route: ['/settings', 'management-controller-services', response.id, 'edit'],
                                        routeBoundary: response.postUpdateNavigationBoundary,
                                        context: 'Controller Service'
                                    } as BackNavigation
                                }
                            });
                        } else {
                            this.router.navigate(response.postUpdateNavigation);
                        }
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
                ofType(ManagementControllerServicesActions.openEnableControllerServiceDialog),
                map((action) => action.request),
                tap((request) => {
                    const serviceId: string = request.id;

                    const enableDialogReference = this.dialog.open(EnableControllerService, {
                        ...XL_DIALOG,
                        data: request,
                        id: serviceId
                    });

                    enableDialogReference.componentInstance.goToReferencingComponent = (
                        component: ControllerServiceReferencingComponent
                    ) => {
                        const route: string[] = this.getRouteForReference(component);
                        this.router.navigate(route);
                    };

                    enableDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(ManagementControllerServicesActions.loadManagementControllerServices());
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    openDisableControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.openDisableControllerServiceDialog),
                map((action) => action.request),
                tap((request) => {
                    const serviceId: string = request.id;

                    const enableDialogReference = this.dialog.open(DisableControllerService, {
                        ...XL_DIALOG,
                        data: request,
                        id: serviceId
                    });

                    enableDialogReference.componentInstance.goToReferencingComponent = (
                        component: ControllerServiceReferencingComponent
                    ) => {
                        const route: string[] = this.getRouteForReference(component);
                        this.router.navigate(route);
                    };

                    enableDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(ManagementControllerServicesActions.loadManagementControllerServices());
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    promptControllerServiceDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.promptControllerServiceDeletion),
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
                            ManagementControllerServicesActions.deleteControllerService({
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
            ofType(ManagementControllerServicesActions.deleteControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.managementControllerServiceService.deleteControllerService(request)).pipe(
                    map((response) =>
                        ManagementControllerServicesActions.deleteControllerServiceSuccess({
                            response: {
                                controllerService: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ManagementControllerServicesActions.managementControllerServicesSnackbarApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    deleteControllerServiceSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.deleteControllerServiceSuccess),
            switchMap(() => of(ManagementControllerServicesActions.loadManagementControllerServices()))
        )
    );

    selectControllerService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.selectControllerService),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/settings', 'management-controller-services', request.id]);
                })
            ),
        { dispatch: false }
    );

    openChangeMgtControllerServiceVersionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.openChangeMgtControllerServiceVersionDialog),
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
                            ManagementControllerServicesActions.configureControllerService({
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
