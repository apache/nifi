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
import * as ParameterContextListingActions from './parameter-context-listing.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import {
    asyncScheduler,
    catchError,
    filter,
    from,
    interval,
    map,
    Observable,
    of,
    switchMap,
    take,
    takeUntil,
    tap
} from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { Router } from '@angular/router';
import { ParameterContextService } from '../../service/parameter-contexts.service';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { EditParameterContext } from '../../ui/parameter-context-listing/edit-parameter-context/edit-parameter-context.component';
import {
    selectParameterContexts,
    selectParameterContextStatus,
    selectSaving,
    selectUpdateRequest
} from './parameter-context-listing.selectors';
import {
    EditParameterRequest,
    EditParameterResponse,
    isDefinedAndNotNull,
    Parameter,
    ParameterContextUpdateRequest
} from '../../../../state/shared';
import { EditParameterDialog } from '../../../../ui/common/edit-parameter-dialog/edit-parameter-dialog.component';
import { OkDialog } from '../../../../ui/common/ok-dialog/ok-dialog.component';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { MEDIUM_DIALOG, SMALL_DIALOG, XL_DIALOG } from '../../../../index';

@Injectable()
export class ParameterContextListingEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private parameterContextService: ParameterContextService,
        private dialog: MatDialog,
        private router: Router,
        private errorHelper: ErrorHelper
    ) {}

    loadParameterContexts$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.loadParameterContexts),
            concatLatestFrom(() => this.store.select(selectParameterContextStatus)),
            switchMap(([, status]) =>
                from(this.parameterContextService.getParameterContexts()).pipe(
                    map((response) =>
                        ParameterContextListingActions.loadParameterContextsSuccess({
                            response: {
                                parameterContexts: response.parameterContexts,
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

    openNewParameterContextDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterContextListingActions.openNewParameterContextDialog),
                tap(() => {
                    const dialogReference = this.dialog.open(EditParameterContext, {
                        ...XL_DIALOG,
                        data: {}
                    });

                    dialogReference.componentInstance.availableParameterContexts$ =
                        this.store.select(selectParameterContexts);
                    dialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    dialogReference.componentInstance.createNewParameter = (
                        existingParameters: string[]
                    ): Observable<Parameter> => {
                        const dialogRequest: EditParameterRequest = { existingParameters };
                        const newParameterDialogReference = this.dialog.open(EditParameterDialog, {
                            ...MEDIUM_DIALOG,
                            data: dialogRequest
                        });

                        newParameterDialogReference.componentInstance.saving$ = of(false);

                        return newParameterDialogReference.componentInstance.editParameter.pipe(
                            take(1),
                            map((dialogResponse: EditParameterResponse) => {
                                newParameterDialogReference.close();

                                return {
                                    ...dialogResponse.parameter
                                };
                            })
                        );
                    };

                    dialogReference.componentInstance.addParameterContext
                        .pipe(takeUntil(dialogReference.afterClosed()))
                        .subscribe((payload: any) => {
                            this.store.dispatch(
                                ParameterContextListingActions.createParameterContext({
                                    request: { payload }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    createParameterContext$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.createParameterContext),
            map((action) => action.request),
            switchMap((request) =>
                from(this.parameterContextService.createParameterContext(request)).pipe(
                    map((response) =>
                        ParameterContextListingActions.createParameterContextSuccess({
                            response: {
                                parameterContext: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                            return of(
                                ParameterContextListingActions.parameterContextListingBannerApiError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            );
                        } else {
                            this.dialog.closeAll();
                            return of(this.errorHelper.fullScreenError(errorResponse));
                        }
                    })
                )
            )
        )
    );

    createParameterContextSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterContextListingActions.createParameterContextSuccess),
                tap(() => {
                    this.dialog.closeAll();
                })
            ),
        { dispatch: false }
    );

    parameterContextListingBannerApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.parameterContextListingBannerApiError),
            map((action) => action.error),
            switchMap((error) => of(ErrorActions.addBannerError({ error })))
        )
    );

    parameterContextListingSnackbarApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.parameterContextListingSnackbarApiError),
            map((action) => action.error),
            switchMap((error) => of(ErrorActions.snackBarError({ error })))
        )
    );

    navigateToEditService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterContextListingActions.navigateToEditParameterContext),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['/parameter-contexts', id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    getEffectiveParameterContextAndOpenDialog$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.getEffectiveParameterContextAndOpenDialog),
            map((action) => action.request),
            switchMap((request) =>
                from(this.parameterContextService.getParameterContext(request.id, true)).pipe(
                    map((response) =>
                        ParameterContextListingActions.openParameterContextDialog({
                            request: {
                                parameterContext: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.router.navigate(['/parameter-contexts']);
                        return of(
                            ParameterContextListingActions.parameterContextListingSnackbarApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        );
                    })
                )
            )
        )
    );

    openParameterContextDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterContextListingActions.openParameterContextDialog),
                map((action) => action.request),
                tap((request) => {
                    // @ts-ignore
                    const parameterContextId: string = request.parameterContext.id;

                    const editDialogReference = this.dialog.open(EditParameterContext, {
                        ...XL_DIALOG,
                        data: {
                            parameterContext: request.parameterContext
                        }
                    });

                    editDialogReference.componentInstance.updateRequest = this.store.select(selectUpdateRequest);
                    editDialogReference.componentInstance.availableParameterContexts$ = this.store
                        .select(selectParameterContexts)
                        .pipe(
                            map((parameterContexts) => parameterContexts.filter((pc) => pc.id != parameterContextId))
                        );
                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.createNewParameter = (
                        existingParameters: string[]
                    ): Observable<Parameter> => {
                        const dialogRequest: EditParameterRequest = { existingParameters };
                        const newParameterDialogReference = this.dialog.open(EditParameterDialog, {
                            ...MEDIUM_DIALOG,
                            data: dialogRequest
                        });

                        newParameterDialogReference.componentInstance.saving$ = of(false);

                        return newParameterDialogReference.componentInstance.editParameter.pipe(
                            take(1),
                            map((dialogResponse: EditParameterResponse) => {
                                newParameterDialogReference.close();

                                return {
                                    ...dialogResponse.parameter
                                };
                            })
                        );
                    };

                    editDialogReference.componentInstance.editParameter = (
                        parameter: Parameter
                    ): Observable<Parameter> => {
                        const dialogRequest: EditParameterRequest = {
                            parameter: {
                                ...parameter
                            }
                        };
                        const editParameterDialogReference = this.dialog.open(EditParameterDialog, {
                            ...MEDIUM_DIALOG,
                            data: dialogRequest
                        });

                        editParameterDialogReference.componentInstance.saving$ = of(false);

                        return editParameterDialogReference.componentInstance.editParameter.pipe(
                            take(1),
                            map((dialogResponse: EditParameterResponse) => {
                                editParameterDialogReference.close();

                                return {
                                    ...dialogResponse.parameter
                                };
                            })
                        );
                    };

                    editDialogReference.componentInstance.editParameterContext
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((payload: any) => {
                            this.store.dispatch(
                                ParameterContextListingActions.submitParameterContextUpdateRequest({
                                    request: {
                                        id: parameterContextId,
                                        payload
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        this.store.dispatch(ErrorActions.clearBannerErrors());

                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                ParameterContextListingActions.selectParameterContext({
                                    request: {
                                        id: parameterContextId
                                    }
                                })
                            );
                            this.store.dispatch(ParameterContextListingActions.editParameterContextComplete());
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    submitParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.submitParameterContextUpdateRequest),
            map((action) => action.request),
            switchMap((request) =>
                from(this.parameterContextService.submitParameterContextUpdate(request)).pipe(
                    map((response) =>
                        ParameterContextListingActions.submitParameterContextUpdateRequestSuccess({
                            response: {
                                requestEntity: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                            return of(
                                ParameterContextListingActions.parameterContextListingBannerApiError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            );
                        } else {
                            this.dialog.closeAll();
                            return of(this.errorHelper.fullScreenError(errorResponse));
                        }
                    })
                )
            )
        )
    );

    submitParameterContextUpdateRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.submitParameterContextUpdateRequestSuccess),
            map((action) => action.response),
            switchMap((response) => {
                const updateRequest: ParameterContextUpdateRequest = response.requestEntity.request;
                if (updateRequest.complete) {
                    return of(ParameterContextListingActions.deleteParameterContextUpdateRequest());
                } else {
                    return of(ParameterContextListingActions.startPollingParameterContextUpdateRequest());
                }
            })
        )
    );

    startPollingParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.startPollingParameterContextUpdateRequest),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(
                        this.actions$.pipe(
                            ofType(ParameterContextListingActions.stopPollingParameterContextUpdateRequest)
                        )
                    )
                )
            ),
            switchMap(() => of(ParameterContextListingActions.pollParameterContextUpdateRequest()))
        )
    );

    pollParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.pollParameterContextUpdateRequest),
            concatLatestFrom(() => this.store.select(selectUpdateRequest).pipe(isDefinedAndNotNull())),
            switchMap(([, updateRequest]) =>
                from(this.parameterContextService.pollParameterContextUpdate(updateRequest.request)).pipe(
                    map((response) =>
                        ParameterContextListingActions.pollParameterContextUpdateRequestSuccess({
                            response: {
                                requestEntity: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.store.dispatch(ParameterContextListingActions.stopPollingParameterContextUpdateRequest());

                        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                            return of(
                                ParameterContextListingActions.parameterContextListingBannerApiError({
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

    pollParameterContextUpdateRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.pollParameterContextUpdateRequestSuccess),
            map((action) => action.response),
            filter((response) => response.requestEntity.request.complete),
            switchMap(() => of(ParameterContextListingActions.stopPollingParameterContextUpdateRequest()))
        )
    );

    stopPollingParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.stopPollingParameterContextUpdateRequest),
            switchMap(() => of(ParameterContextListingActions.deleteParameterContextUpdateRequest()))
        )
    );

    deleteParameterContextUpdateRequest$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterContextListingActions.deleteParameterContextUpdateRequest),
                concatLatestFrom(() => this.store.select(selectUpdateRequest).pipe(isDefinedAndNotNull())),
                tap(([, updateRequest]) => {
                    this.parameterContextService
                        .deleteParameterContextUpdate(updateRequest.request)
                        .subscribe((response) => {
                            this.store.dispatch(
                                ParameterContextListingActions.deleteParameterContextUpdateRequestSuccess({
                                    response: {
                                        requestEntity: response
                                    }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    promptParameterContextDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterContextListingActions.promptParameterContextDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Delete Parameter Context',
                            message: `Delete parameter context ${request.parameterContext.component.name}?`
                        }
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            ParameterContextListingActions.deleteParameterContext({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    deleteParameterContext$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.deleteParameterContext),
            map((action) => action.request),
            switchMap((request) =>
                from(this.parameterContextService.deleteParameterContext(request)).pipe(
                    map((response) =>
                        ParameterContextListingActions.deleteParameterContextSuccess({
                            response: {
                                parameterContext: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ParameterContextListingActions.parameterContextListingSnackbarApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    selectParameterContext$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterContextListingActions.selectParameterContext),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/parameter-contexts', request.id]);
                })
            ),
        { dispatch: false }
    );

    showOkDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterContextListingActions.showOkDialog),
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
}
