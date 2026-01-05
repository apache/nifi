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

import { Injectable, inject } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import { Store } from '@ngrx/store';
import { NiFiState } from '../index';
import * as ComponentStateActions from './component-state.actions';
import { resetComponentState } from './component-state.actions';
import { catchError, from, map, of, switchMap, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { ComponentStateService } from '../../service/component-state.service';
import { ComponentStateDialog } from '../../ui/common/component-state/component-state.component';
import { selectComponentType, selectComponentId, selectComponentState } from './component-state.selectors';
import { isDefinedAndNotNull, XL_DIALOG } from '@nifi/shared';
import * as ErrorActions from '../error/error.actions';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../service/error-helper.service';
import { ErrorContextKey } from '../error';
import { ComponentState, ComponentStateEntity } from './index';

@Injectable()
export class ComponentStateEffects {
    private actions$ = inject(Actions);
    private store = inject<Store<NiFiState>>(Store);
    private componentStateService = inject(ComponentStateService);
    private dialog = inject(MatDialog);
    private errorHelper = inject(ErrorHelper);

    getComponentStateAndOpenDialog$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ComponentStateActions.getComponentStateAndOpenDialog),
            map((action) => action.request),
            switchMap((request) =>
                from(
                    this.componentStateService
                        .getComponentState({
                            componentType: request.componentType,
                            componentId: request.componentId
                        })
                        .pipe(
                            map((response: ComponentStateEntity) =>
                                ComponentStateActions.loadComponentStateSuccess({
                                    response: {
                                        componentState: response.componentState
                                    }
                                })
                            ),
                            catchError((errorResponse: HttpErrorResponse) =>
                                of(
                                    ErrorActions.snackBarError({
                                        error: this.errorHelper.getErrorString(
                                            errorResponse,
                                            `Failed to get the component state for ${request.componentName}.`
                                        )
                                    })
                                )
                            )
                        )
                )
            )
        )
    );

    loadComponentStateSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ComponentStateActions.loadComponentStateSuccess),
            map((action) => action.response),
            switchMap(() => of(ComponentStateActions.openComponentStateDialog()))
        )
    );

    openComponentStateDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ComponentStateActions.openComponentStateDialog),
                tap(() => {
                    const dialogReference = this.dialog.open(ComponentStateDialog, {
                        ...XL_DIALOG,
                        autoFocus: false
                    });

                    dialogReference.afterClosed().subscribe(() => {
                        this.store.dispatch(resetComponentState());
                    });
                })
            ),
        { dispatch: false }
    );

    clearComponentState$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ComponentStateActions.clearComponentState),
            concatLatestFrom(() => [
                this.store.select(selectComponentType).pipe(isDefinedAndNotNull()),
                this.store.select(selectComponentId).pipe(isDefinedAndNotNull())
            ]),
            switchMap(([, componentType, componentId]) =>
                from(
                    this.componentStateService.clearComponentState({ componentType, componentId }).pipe(
                        map(() => ComponentStateActions.reloadComponentState()),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(
                                ComponentStateActions.clearComponentStateFailure({
                                    errorContext: {
                                        errors: [
                                            this.errorHelper.getErrorString(
                                                errorResponse,
                                                'Failed to clear the component state.'
                                            )
                                        ],
                                        context: ErrorContextKey.COMPONENT_STATE
                                    }
                                })
                            )
                        )
                    )
                )
            )
        )
    );

    reloadComponentState$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ComponentStateActions.reloadComponentState),
            concatLatestFrom(() => [
                this.store.select(selectComponentType).pipe(isDefinedAndNotNull()),
                this.store.select(selectComponentId).pipe(isDefinedAndNotNull())
            ]),
            switchMap(([, componentType, componentId]) =>
                from(
                    this.componentStateService.getComponentState({ componentType, componentId }).pipe(
                        map((response: any) =>
                            ComponentStateActions.reloadComponentStateSuccess({
                                response: {
                                    componentState: response.componentState
                                }
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(
                                ErrorActions.addBannerError({
                                    errorContext: {
                                        errors: [
                                            this.errorHelper.getErrorString(
                                                errorResponse,
                                                'Failed to reload the component state.'
                                            )
                                        ],
                                        context: ErrorContextKey.COMPONENT_STATE
                                    }
                                })
                            )
                        )
                    )
                )
            )
        )
    );

    clearComponentStateEntry$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ComponentStateActions.clearComponentStateEntry),
            concatLatestFrom(() => [
                this.store.select(selectComponentType).pipe(isDefinedAndNotNull()),
                this.store.select(selectComponentId).pipe(isDefinedAndNotNull()),
                this.store.select(selectComponentState).pipe(isDefinedAndNotNull())
            ]),
            switchMap(([action, componentType, componentId, currentState]) => {
                const { keyToDelete, scope } = action.request;

                // Create new state without the deleted key
                const newState: ComponentState = { ...currentState };

                if (scope === 'LOCAL' && newState.localState?.state) {
                    newState.localState = {
                        ...newState.localState,
                        state: newState.localState.state.filter((entry) => entry.key !== keyToDelete)
                    };
                } else if (scope === 'CLUSTER' && newState.clusterState?.state) {
                    newState.clusterState = {
                        ...newState.clusterState,
                        state: newState.clusterState.state.filter((entry) => entry.key !== keyToDelete)
                    };
                }

                const componentStateEntity: ComponentStateEntity = {
                    componentState: newState
                };

                return from(
                    this.componentStateService
                        .clearComponentStateEntry(componentType, componentId, componentStateEntity)
                        .pipe(
                            map(() => ComponentStateActions.reloadComponentState()),
                            catchError((errorResponse: HttpErrorResponse) =>
                                of(
                                    ComponentStateActions.clearComponentStateFailure({
                                        errorContext: {
                                            errors: [
                                                this.errorHelper.getErrorString(
                                                    errorResponse,
                                                    `Failed to clear state entry: ${keyToDelete}.`
                                                )
                                            ],
                                            context: ErrorContextKey.COMPONENT_STATE
                                        }
                                    })
                                )
                            )
                        )
                );
            })
        )
    );

    clearComponentStateFailure$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ComponentStateActions.clearComponentStateFailure),
            map((action) => action.errorContext),
            switchMap((errorContext) => of(ErrorActions.addBannerError({ errorContext })))
        )
    );
}
