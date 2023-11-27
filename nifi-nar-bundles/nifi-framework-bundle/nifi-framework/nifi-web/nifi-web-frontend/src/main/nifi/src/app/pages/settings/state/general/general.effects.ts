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
import * as GeneralActions from './general.actions';
import { catchError, from, map, of, switchMap, tap } from 'rxjs';
import { ControllerService } from '../../service/controller.service';
import { MatDialog } from '@angular/material/dialog';
import { OkDialog } from '../../../../ui/common/ok-dialog/ok-dialog.component';

@Injectable()
export class GeneralEffects {
    constructor(
        private actions$: Actions,
        private controllerService: ControllerService,
        private dialog: MatDialog
    ) {}

    loadControllerConfig$ = createEffect(() =>
        this.actions$.pipe(
            ofType(GeneralActions.loadControllerConfig),
            switchMap(() =>
                from(this.controllerService.getControllerConfig()).pipe(
                    map((response) =>
                        GeneralActions.loadControllerConfigSuccess({
                            response: {
                                controller: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            GeneralActions.controllerConfigApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    updateControllerConfig$ = createEffect(() =>
        this.actions$.pipe(
            ofType(GeneralActions.updateControllerConfig),
            map((action) => action.request),
            switchMap((request) =>
                from(this.controllerService.updateControllerConfig(request.controller)).pipe(
                    map((response) =>
                        GeneralActions.updateControllerConfigSuccess({
                            response: {
                                controller: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            GeneralActions.controllerConfigApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    updateControllerConfigSuccess = createEffect(
        () =>
            this.actions$.pipe(
                ofType(GeneralActions.updateControllerConfigSuccess),
                tap(() => {
                    this.dialog.open(OkDialog, {
                        data: {
                            title: 'Settings',
                            message: 'Settings successfully applied'
                        },
                        panelClass: 'small-dialog'
                    });
                })
            ),
        { dispatch: false }
    );
}
