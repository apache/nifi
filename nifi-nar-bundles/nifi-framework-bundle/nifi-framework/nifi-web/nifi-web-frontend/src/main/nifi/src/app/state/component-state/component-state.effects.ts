/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { Injectable } from '@angular/core';
import { Actions, concatLatestFrom, createEffect, ofType } from '@ngrx/effects';
import { Store } from '@ngrx/store';
import { NiFiState } from '../index';
import * as ComponentStateActions from './component-state.actions';
import { catchError, from, map, of, switchMap, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { ComponentStateService } from '../../service/component-state.service';
import { ComponentStateDialog } from '../../ui/common/component-state/component-state.component';
import { resetComponentState } from './component-state.actions';
import { selectComponentUri } from './component-state.selectors';
import { isDefinedAndNotNull } from '../shared';

@Injectable()
export class ComponentStateEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private componentStateService: ComponentStateService,
        private dialog: MatDialog
    ) {}

    getComponentStateAndOpenDialog$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ComponentStateActions.getComponentStateAndOpenDialog),
            map((action) => action.request),
            switchMap((request) =>
                from(
                    this.componentStateService.getComponentState({ componentUri: request.componentUri }).pipe(
                        map((response: any) =>
                            ComponentStateActions.loadComponentStateSuccess({
                                response: {
                                    componentState: response.componentState
                                }
                            })
                        ),
                        catchError((error) =>
                            of(
                                ComponentStateActions.componentStateApiError({
                                    error: error.error
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
            switchMap((response) => of(ComponentStateActions.openComponentStateDialog()))
        )
    );

    openComponentStateDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ComponentStateActions.openComponentStateDialog),
                tap(() => {
                    const dialogReference = this.dialog.open(ComponentStateDialog, {
                        panelClass: 'large-dialog'
                    });

                    dialogReference.afterClosed().subscribe((response) => {
                        this.store.dispatch(resetComponentState());
                    });
                })
            ),
        { dispatch: false }
    );

    clearComponentState$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ComponentStateActions.clearComponentState),
            concatLatestFrom(() => this.store.select(selectComponentUri).pipe(isDefinedAndNotNull())),
            switchMap(([action, componentUri]) =>
                from(
                    this.componentStateService.clearComponentState({ componentUri }).pipe(
                        map((response: any) => ComponentStateActions.reloadComponentState()),
                        catchError((error) =>
                            of(
                                ComponentStateActions.componentStateApiError({
                                    error: error.error
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
            concatLatestFrom(() => this.store.select(selectComponentUri).pipe(isDefinedAndNotNull())),
            switchMap(([action, componentUri]) =>
                from(
                    this.componentStateService.getComponentState({ componentUri }).pipe(
                        map((response: any) =>
                            ComponentStateActions.reloadComponentStateSuccess({
                                response: {
                                    componentState: response.componentState
                                }
                            })
                        ),
                        catchError((error) =>
                            of(
                                ComponentStateActions.componentStateApiError({
                                    error: error.error
                                })
                            )
                        )
                    )
                )
            )
        )
    );
}
