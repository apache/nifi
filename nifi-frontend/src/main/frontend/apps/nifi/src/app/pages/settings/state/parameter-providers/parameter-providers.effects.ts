/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { Client } from '../../../../service/client.service';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { ParameterProviderService } from '../../service/parameter-provider.service';
import * as ParameterProviderActions from './parameter-providers.actions';
import { loadParameterProviders } from './parameter-providers.actions';
import {
    asyncScheduler,
    catchError,
    filter,
    from,
    interval,
    map,
    NEVER,
    of,
    switchMap,
    take,
    takeUntil,
    tap
} from 'rxjs';
import {
    selectApplyParameterProviderParametersRequest,
    selectSaving,
    selectStatus
} from './parameter-providers.selectors';
import { selectParameterProviderTypes } from '../../../../state/extension-types/extension-types.selectors';
import { CreateParameterProvider } from '../../ui/parameter-providers/create-parameter-provider/create-parameter-provider.component';
import { EditParameterProvider } from '../../ui/parameter-providers/edit-parameter-provider/edit-parameter-provider.component';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { EditParameterProviderRequest, ParameterProviderEntity, UpdateParameterProviderRequest } from './index';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { FetchParameterProviderParameters } from '../../ui/parameter-providers/fetch-parameter-provider-parameters/fetch-parameter-provider-parameters.component';
import * as ErrorActions from '../../../../state/error/error.actions';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { LARGE_DIALOG, SMALL_DIALOG, XL_DIALOG, YesNoDialog } from '@nifi/shared';
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
export class ParameterProvidersEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private client: Client,
        private dialog: MatDialog,
        private router: Router,
        private parameterProviderService: ParameterProviderService,
        private propertyTableHelperService: PropertyTableHelperService,
        private managementControllerServiceService: ManagementControllerServiceService,
        private errorHelper: ErrorHelper
    ) {}

    loadParameterProviders$ = createEffect(() =>
        this.actions$.pipe(
            ofType(loadParameterProviders),
            concatLatestFrom(() => this.store.select(selectStatus)),
            switchMap(([, status]) =>
                from(this.parameterProviderService.getParameterProviders()).pipe(
                    map((response) =>
                        ParameterProviderActions.loadParameterProvidersSuccess({
                            response: {
                                parameterProviders: response.parameterProviders,
                                loadedTimestamp: response.currentTime
                            }
                        })
                    ),
                    catchError((error: HttpErrorResponse) => of(this.errorHelper.handleLoadingError(status, error)))
                )
            )
        )
    );

    selectParameterProvider$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.selectParameterProvider),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/settings', 'parameter-providers', request.id]);
                })
            ),
        { dispatch: false }
    );

    openNewParameterProviderDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.openNewParameterProviderDialog),
                concatLatestFrom(() => this.store.select(selectParameterProviderTypes)),
                tap(([, parameterProviderTypes]) => {
                    const dialogReference = this.dialog.open(CreateParameterProvider, {
                        ...LARGE_DIALOG,
                        data: {
                            parameterProviderTypes
                        }
                    });

                    dialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    dialogReference.componentInstance.createParameterProvider
                        .pipe(take(1))
                        .subscribe((parameterProviderType) => {
                            this.store.dispatch(
                                ParameterProviderActions.createParameterProvider({
                                    request: {
                                        parameterProviderType: parameterProviderType.type,
                                        parameterProviderBundle: parameterProviderType.bundle,
                                        revision: {
                                            clientId: this.client.getClientId(),
                                            version: 0
                                        }
                                    }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    createParameterProvider$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.createParameterProvider),
            map((action) => action.request),
            switchMap((request) =>
                from(this.parameterProviderService.createParameterProvider(request)).pipe(
                    map((response: any) =>
                        ParameterProviderActions.createParameterProviderSuccess({
                            response: {
                                parameterProvider: response
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

    createParameterProviderSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.createParameterProviderSuccess),
            map((action) => action.response),
            tap(() => {
                this.dialog.closeAll();
            }),
            switchMap((response) =>
                of(
                    ParameterProviderActions.selectParameterProvider({
                        request: {
                            id: response.parameterProvider.id
                        }
                    })
                )
            )
        )
    );

    promptParameterProviderDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.promptParameterProviderDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Delete Parameter Provider',
                            message: `Delete parameter provider ${request.parameterProvider.component.name}?`
                        }
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() =>
                        this.store.dispatch(
                            ParameterProviderActions.deleteParameterProvider({
                                request
                            })
                        )
                    );
                })
            ),
        { dispatch: false }
    );

    deleteParameterProvider = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.deleteParameterProvider),
            map((action) => action.request),
            switchMap((request) =>
                from(this.parameterProviderService.deleteParameterProvider(request)).pipe(
                    map((response: any) =>
                        ParameterProviderActions.deleteParameterProviderSuccess({
                            response: {
                                parameterProvider: response
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

    navigateToEditParameterProvider$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.navigateToEditParameterProvider),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['settings', 'parameter-providers', id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    navigateToAdvancedParameterProviderUi$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.navigateToAdvancedParameterProviderUi),
                map((action) => action.id),
                tap((id) => {
                    const routeBoundary: string[] = ['/settings', 'parameter-providers', id, 'advanced'];
                    this.router.navigate([...routeBoundary], {
                        state: {
                            backNavigation: {
                                route: ['/settings', 'parameter-providers', id],
                                routeBoundary,
                                context: 'Parameter Provider'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    navigateToManageAccessPolicies$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.navigateToManageAccessPolicies),
                map((action) => action.id),
                tap((id) => {
                    const routeBoundary: string[] = ['/access-policies'];
                    this.router.navigate([...routeBoundary, 'read', 'component', 'parameter-providers', id], {
                        state: {
                            backNavigation: {
                                route: ['/settings', 'parameter-providers', id],
                                routeBoundary,
                                context: 'Parameter Provider'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    navigateToFetchParameterProvider$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.navigateToFetchParameterProvider),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['settings', 'parameter-providers', id, 'fetch']);
                })
            ),
        { dispatch: false }
    );

    openConfigureParameterProviderDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.openConfigureParameterProviderDialog),
                map((action) => action.request),
                switchMap((request) =>
                    from(this.propertyTableHelperService.getComponentHistory(request.id)).pipe(
                        map((history) => {
                            return {
                                ...request,
                                history: history.componentHistory
                            } as EditParameterProviderRequest;
                        }),
                        tap({
                            error: (errorResponse: HttpErrorResponse) => {
                                this.store.dispatch(
                                    ParameterProviderActions.selectParameterProvider({
                                        request: {
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
                tap((request) => {
                    const id = request.id;
                    const editDialogReference = this.dialog.open(EditParameterProvider, {
                        ...XL_DIALOG,
                        data: request,
                        id
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

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

                    const goTo = (commands: string[], destination: string, commandBoundary: string[]) => {
                        // confirm navigating away while changes are unsaved
                        if (editDialogReference.componentInstance.editParameterProviderForm.dirty) {
                            const promptSaveDialogRef = this.dialog.open(YesNoDialog, {
                                ...SMALL_DIALOG,
                                data: {
                                    title: 'Parameter Provider Configuration',
                                    message: `Save changes before going to this ${destination}`
                                }
                            });

                            promptSaveDialogRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
                                editDialogReference.componentInstance.submitForm(commands, commandBoundary);
                            });

                            promptSaveDialogRef.componentInstance.no.pipe(take(1)).subscribe(() => {
                                this.router.navigate(commands, {
                                    state: {
                                        backNavigation: {
                                            route: ['/settings', 'parameter-providers', id, 'edit'],
                                            routeBoundary: commandBoundary,
                                            context: 'Parameter Provider'
                                        } as BackNavigation
                                    }
                                });
                            });
                        } else {
                            this.router.navigate(commands, {
                                state: {
                                    backNavigation: {
                                        route: ['/settings', 'parameter-providers', id, 'edit'],
                                        routeBoundary: commandBoundary,
                                        context: 'Parameter Provider'
                                    } as BackNavigation
                                }
                            });
                        }
                    };

                    editDialogReference.componentInstance.goToReferencingParameterContext = (id: string) => {
                        const commandBoundary: string[] = ['/parameter-contexts'];
                        const commands: string[] = [...commandBoundary, id];
                        goTo(commands, 'Parameter Context', commandBoundary);
                    };

                    editDialogReference.componentInstance.goToService = (serviceId: string) => {
                        const commandBoundary: string[] = ['/settings', 'management-controller-services'];
                        const commands: string[] = [...commandBoundary, serviceId];
                        goTo(commands, 'Controller Service', commandBoundary);
                    };

                    editDialogReference.componentInstance.createNewProperty =
                        this.propertyTableHelperService.createNewProperty(request.id, this.parameterProviderService);

                    editDialogReference.componentInstance.createNewService =
                        this.propertyTableHelperService.createNewService(
                            request.id,
                            this.managementControllerServiceService,
                            this.parameterProviderService
                        );

                    editDialogReference.componentInstance.editParameterProvider
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((updateRequest: UpdateParameterProviderRequest) => {
                            this.store.dispatch(
                                ParameterProviderActions.configureParameterProvider({
                                    request: {
                                        id: request.parameterProvider.id,
                                        uri: request.parameterProvider.uri,
                                        payload: updateRequest.payload,
                                        postUpdateNavigation: updateRequest.postUpdateNavigation,
                                        postUpdateNavigationBoundary: updateRequest.postUpdateNavigationBoundary
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        this.store.dispatch(resetPropertyVerificationState());

                        if (response !== 'ROUTED') {
                            this.store.dispatch(
                                ParameterProviderActions.selectParameterProvider({
                                    request: {
                                        id
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    configureParameterProvider$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.configureParameterProvider),
            map((action) => action.request),
            switchMap((request) =>
                from(this.parameterProviderService.updateParameterProvider(request)).pipe(
                    map((response) =>
                        ParameterProviderActions.configureParameterProviderSuccess({
                            response: {
                                id: request.id,
                                parameterProvider: response,
                                postUpdateNavigation: request.postUpdateNavigation,
                                postUpdateNavigationBoundary: request.postUpdateNavigationBoundary
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                            return of(
                                ParameterProviderActions.parameterProvidersBannerApiError({
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

    configureParameterProviderSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.configureParameterProviderSuccess),
                map((action) => action.response),
                tap((response) => {
                    if (response.postUpdateNavigation) {
                        if (response.postUpdateNavigationBoundary) {
                            this.router.navigate(response.postUpdateNavigation, {
                                state: {
                                    backNavigation: {
                                        route: ['/settings', 'parameter-providers', response.id, 'edit'],
                                        routeBoundary: response.postUpdateNavigationBoundary,
                                        context: 'Parameter Provider'
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

    fetchParameterProviderParametersAndOpenDialog$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.fetchParameterProviderParametersAndOpenDialog),
            map((action) => action.request),
            switchMap((request) =>
                from(this.parameterProviderService.fetchParameters(request)).pipe(
                    map((response: ParameterProviderEntity) =>
                        ParameterProviderActions.fetchParameterProviderParametersSuccess({
                            response: { parameterProvider: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                            this.store.dispatch(
                                ParameterProviderActions.selectParameterProvider({
                                    request: {
                                        id: request.id
                                    }
                                })
                            );
                            return of(
                                ErrorActions.snackBarError({ error: this.errorHelper.getErrorString(errorResponse) })
                            );
                        } else {
                            return of(this.errorHelper.fullScreenError(errorResponse));
                        }
                    })
                )
            )
        )
    );

    fetchParameterProviderParametersSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.fetchParameterProviderParametersSuccess),
            map((action) => action.response),
            switchMap((response) =>
                of(ParameterProviderActions.openFetchParameterProviderDialog({ request: response }))
            )
        )
    );

    openFetchParameterProvidersParametersDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.openFetchParameterProviderDialog),
                map((action) => action.request),
                tap((request) => {
                    const dialogRef = this.dialog.open(FetchParameterProviderParameters, {
                        ...XL_DIALOG,
                        data: request,
                        autoFocus: false
                    });

                    const referencingParameterContexts =
                        request.parameterProvider.component.referencingParameterContexts;
                    if (referencingParameterContexts?.length > 0) {
                        // add an error if one of the referenced parameter contexts is not readable/writeable
                        const canReadWriteAll = referencingParameterContexts.every(
                            (paramContextRef) =>
                                paramContextRef.permissions.canRead && paramContextRef.permissions.canWrite
                        );
                        if (!canReadWriteAll) {
                            this.store.dispatch(
                                ParameterProviderActions.parameterProvidersBannerApiError({
                                    error: 'You do not have permissions to modify one or more synced parameter contexts.'
                                })
                            );
                        }
                    }
                    const affectedComponents = request.parameterProvider.component.affectedComponents;
                    if (affectedComponents?.length > 0) {
                        // add an error if one of the affected components is not readable/writeable
                        const canReadWriteAll = affectedComponents.every(
                            (paramContextRef) =>
                                paramContextRef.permissions.canRead && paramContextRef.permissions.canWrite
                        );
                        if (!canReadWriteAll) {
                            this.store.dispatch(
                                ParameterProviderActions.parameterProvidersBannerApiError({
                                    error: 'You do not have permissions to modify one or more affected components.'
                                })
                            );
                        }
                    }

                    dialogRef.componentInstance.updateRequest = this.store.select(
                        selectApplyParameterProviderParametersRequest
                    );
                    dialogRef.componentInstance.saving$ = this.store.select(selectSaving);

                    dialogRef.afterClosed().subscribe((response) => {
                        this.store.dispatch(ParameterProviderActions.resetFetchedParameterProvider());

                        if (response !== 'ROUTED') {
                            this.store.dispatch(
                                ParameterProviderActions.selectParameterProvider({
                                    request: {
                                        id: request.parameterProvider.id
                                    }
                                })
                            );
                            this.store.dispatch(
                                ParameterProviderActions.submitParameterProviderParametersUpdateComplete()
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    parameterProvidersBannerApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.parameterProvidersBannerApiError),
            map((action) => action.error),
            tap(() =>
                this.store.dispatch(ParameterProviderActions.stopPollingParameterProviderParametersUpdateRequest())
            ),
            switchMap((error) =>
                of(
                    ErrorActions.addBannerError({
                        errorContext: { errors: [error], context: ErrorContextKey.PARAMETER_PROVIDERS }
                    })
                )
            )
        )
    );

    submitParameterProviderParametersUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.submitParameterProviderParametersUpdateRequest),
            map((action) => action.request),
            switchMap((request) =>
                from(
                    this.parameterProviderService.applyParameters(request).pipe(
                        map((response: any) =>
                            ParameterProviderActions.submitParameterProviderParametersUpdateRequestSuccess({
                                response: {
                                    request: response.request
                                }
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(
                                ParameterProviderActions.parameterProvidersBannerApiError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            )
                        )
                    )
                )
            )
        )
    );

    submitParameterProviderParametersUpdateRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.submitParameterProviderParametersUpdateRequestSuccess),
            map((action) => action.response),
            switchMap((response) => {
                const updateRequest = response.request;
                if (updateRequest.complete) {
                    return of(ParameterProviderActions.deleteParameterProviderParametersUpdateRequest());
                } else {
                    return of(ParameterProviderActions.startPollingParameterProviderParametersUpdateRequest());
                }
            })
        )
    );

    startPollingParameterProviderParametersUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.startPollingParameterProviderParametersUpdateRequest),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(
                        this.actions$.pipe(
                            ofType(ParameterProviderActions.stopPollingParameterProviderParametersUpdateRequest)
                        )
                    )
                )
            ),
            switchMap(() => of(ParameterProviderActions.pollParameterProviderParametersUpdateRequest()))
        )
    );

    pollParameterProviderParametersUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.pollParameterProviderParametersUpdateRequest),
            concatLatestFrom(() => this.store.select(selectApplyParameterProviderParametersRequest)),
            switchMap(([, updateRequest]) => {
                if (updateRequest) {
                    return from(
                        this.parameterProviderService.pollParameterProviderParametersUpdateRequest(updateRequest)
                    ).pipe(
                        map((response) =>
                            ParameterProviderActions.pollParameterProviderParametersUpdateRequestSuccess({
                                response: {
                                    request: response.request
                                }
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(
                                ParameterProviderActions.parameterProvidersBannerApiError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            )
                        )
                    );
                } else {
                    return NEVER;
                }
            })
        )
    );

    pollParameterProviderParametersUpdateRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.pollParameterProviderParametersUpdateRequestSuccess),
            map((action) => action.response),
            filter((response) => response.request.complete),
            switchMap(() => {
                return of(ParameterProviderActions.stopPollingParameterProviderParametersUpdateRequest());
            })
        )
    );

    stopPollingParameterProviderParametersUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.stopPollingParameterProviderParametersUpdateRequest),
            switchMap(() => of(ParameterProviderActions.deleteParameterProviderParametersUpdateRequest()))
        )
    );

    deleteParameterProviderParametersUpdateRequest$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.deleteParameterProviderParametersUpdateRequest),
                concatLatestFrom(() => this.store.select(selectApplyParameterProviderParametersRequest)),
                tap(([, updateRequest]) => {
                    if (updateRequest) {
                        this.parameterProviderService
                            .deleteParameterProviderParametersUpdateRequest(updateRequest)
                            .subscribe();
                    }
                })
            ),
        { dispatch: false }
    );
}
