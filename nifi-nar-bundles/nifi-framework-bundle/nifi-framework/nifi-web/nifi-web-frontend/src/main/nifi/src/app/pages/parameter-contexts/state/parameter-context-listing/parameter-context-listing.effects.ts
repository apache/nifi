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
import * as ParameterContextListingActions from './parameter-context-listing.actions';
import {
    asyncScheduler,
    catchError,
    from,
    interval,
    map,
    NEVER,
    Observable,
    of,
    switchMap,
    take,
    takeUntil,
    tap,
    withLatestFrom
} from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { Router } from '@angular/router';
import { ParameterContextService } from '../../service/parameter-contexts.service';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { EditParameterContext } from '../../ui/parameter-context-listing/edit-parameter-context/edit-parameter-context.component';
import { selectSaving, selectUpdateRequest } from './parameter-context-listing.selectors';
import { EditParameterRequest, EditParameterResponse } from '../../../../state/shared';
import { EditParameterDialog } from '../../../../ui/common/edit-parameter-dialog/edit-parameter-dialog.component';
import { Parameter } from './index';

@Injectable()
export class ParameterContextListingEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private parameterContextService: ParameterContextService,
        private dialog: MatDialog,
        private router: Router
    ) {}

    loadParameterContexts$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.loadParameterContexts),
            switchMap(() =>
                from(this.parameterContextService.getParameterContexts()).pipe(
                    map((response) =>
                        ParameterContextListingActions.loadParameterContextsSuccess({
                            response: {
                                parameterContexts: response.parameterContexts,
                                loadedTimestamp: response.currentTime
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ParameterContextListingActions.parameterContextListingApiError({
                                error: error.error
                            })
                        )
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
                        data: {},
                        panelClass: 'large-dialog'
                    });

                    dialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    dialogReference.componentInstance.createNewParameter = (): Observable<Parameter> => {
                        const dialogRequest: EditParameterRequest = {};
                        const newParameterDialogReference = this.dialog.open(EditParameterDialog, {
                            data: dialogRequest,
                            panelClass: 'medium-dialog'
                        });

                        newParameterDialogReference.componentInstance.saving = false;

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

                    dialogReference.componentInstance.addParameterContext.pipe(take(1)).subscribe((payload: any) => {
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
                    catchError((error) =>
                        of(
                            ParameterContextListingActions.parameterContextListingApiError({
                                error: error.error
                            })
                        )
                    )
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

    openConfigureControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterContextListingActions.openParameterContextServiceDialog),
                map((action) => action.request),
                tap((request) => {
                    // @ts-ignore
                    const parameterContextId: string = request.parameterContext.id;

                    const editDialogReference = this.dialog.open(EditParameterContext, {
                        data: {
                            parameterContext: request.parameterContext
                        },
                        panelClass: 'edit-parameter-context-dialog'
                    });

                    editDialogReference.componentInstance.updateRequest = this.store.select(selectUpdateRequest);
                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.createNewParameter = (): Observable<Parameter> => {
                        const dialogRequest: EditParameterRequest = {};
                        const newParameterDialogReference = this.dialog.open(EditParameterDialog, {
                            data: dialogRequest,
                            panelClass: 'medium-dialog'
                        });

                        newParameterDialogReference.componentInstance.saving = false;

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
                            data: dialogRequest,
                            panelClass: 'medium-dialog'
                        });

                        editParameterDialogReference.componentInstance.saving = false;

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
                        .pipe(take(1))
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

                    editDialogReference.afterClosed().subscribe(() => {
                        this.store.dispatch(
                            ParameterContextListingActions.selectParameterContext({
                                request: {
                                    id: parameterContextId
                                }
                            })
                        );
                        this.store.dispatch(ParameterContextListingActions.editParameterContextComplete());
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
                                request: response.request
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ParameterContextListingActions.parameterContextListingApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    submitParameterContextUpdateRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.submitParameterContextUpdateRequestSuccess),
            map((action) => action.response),
            switchMap((response) => of(ParameterContextListingActions.startPollingParameterContextUpdateRequest()))
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
            withLatestFrom(this.store.select(selectUpdateRequest)),
            switchMap(([action, updateRequest]) => {
                if (updateRequest) {
                    return from(this.parameterContextService.pollParameterContextUpdate(updateRequest)).pipe(
                        map((response) =>
                            ParameterContextListingActions.pollParameterContextUpdateRequestSuccess({
                                response: { request: response.request }
                            })
                        ),
                        catchError((error) =>
                            of(
                                ParameterContextListingActions.parameterContextListingApiError({
                                    error: error.error
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

    pollParameterContextUpdateRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.pollParameterContextUpdateRequestSuccess),
            map((action) => action.response),
            switchMap((response) => {
                if (response.request.complete) {
                    return of(ParameterContextListingActions.stopPollingParameterContextUpdateRequest());
                } else {
                    return NEVER;
                }
            })
        )
    );

    stopPollingParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterContextListingActions.stopPollingParameterContextUpdateRequest),
            switchMap((response) => of(ParameterContextListingActions.deleteParameterContextUpdateRequest()))
        )
    );

    deleteParameterContextUpdateRequest$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterContextListingActions.deleteParameterContextUpdateRequest),
                withLatestFrom(this.store.select(selectUpdateRequest)),
                tap(([action, updateRequest]) => {
                    if (updateRequest) {
                        this.parameterContextService.deleteParameterContextUpdate(updateRequest).subscribe();
                    }
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
                        data: {
                            title: 'Delete Parameter Context',
                            message: `Delete parameter context ${request.parameterContext.component.name}?`
                        },
                        panelClass: 'small-dialog'
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
                    catchError((error) =>
                        of(
                            ParameterContextListingActions.parameterContextListingApiError({
                                error: error.error
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
}
