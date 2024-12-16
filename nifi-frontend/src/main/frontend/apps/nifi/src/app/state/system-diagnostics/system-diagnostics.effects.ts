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
import { Store } from '@ngrx/store';
import { NiFiState } from '../index';
import { MatDialog } from '@angular/material/dialog';
import { SystemDiagnosticsService } from '../../service/system-diagnostics.service';
import * as SystemDiagnosticsActions from './system-diagnostics.actions';
import { catchError, from, map, of, switchMap, tap } from 'rxjs';
import { SystemDiagnosticsRequest } from './index';
import { SystemDiagnosticsDialog } from '../../ui/common/system-diagnostics-dialog/system-diagnostics-dialog.component';
import { LARGE_DIALOG } from '@nifi/shared';
import * as ErrorActions from '../error/error.actions';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../service/error-helper.service';
import { ErrorContextKey } from '../error';

@Injectable()
export class SystemDiagnosticsEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private systemDiagnosticsService: SystemDiagnosticsService,
        private dialog: MatDialog,
        private errorHelper: ErrorHelper
    ) {}

    reloadSystemDiagnostics$ = createEffect(() =>
        this.actions$.pipe(
            ofType(SystemDiagnosticsActions.reloadSystemDiagnostics),
            map((action) => action.request),
            switchMap((request: SystemDiagnosticsRequest) =>
                from(this.systemDiagnosticsService.getSystemDiagnostics(request.nodewise)).pipe(
                    map((response: any) =>
                        SystemDiagnosticsActions.reloadSystemDiagnosticsSuccess({
                            response: {
                                systemDiagnostics: response.systemDiagnostics
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        const error = this.errorHelper.getErrorString(
                            errorResponse,
                            'Failed to reload System Diagnostics.'
                        );
                        if (request.errorStrategy === 'snackbar') {
                            return of(SystemDiagnosticsActions.systemDiagnosticsSnackbarError({ error }));
                        }
                        return of(SystemDiagnosticsActions.systemDiagnosticsBannerError({ error }));
                    })
                )
            )
        )
    );

    getSystemDiagnosticsAndOpenDialog$ = createEffect(() =>
        this.actions$.pipe(
            ofType(SystemDiagnosticsActions.getSystemDiagnosticsAndOpenDialog),
            map((action) => action.request),
            switchMap((request) =>
                from(this.systemDiagnosticsService.getSystemDiagnostics(request.nodewise)).pipe(
                    map((response: any) =>
                        SystemDiagnosticsActions.loadSystemDiagnosticsSuccess({
                            response: {
                                systemDiagnostics: response.systemDiagnostics
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            SystemDiagnosticsActions.systemDiagnosticsSnackbarError({
                                error: this.errorHelper.getErrorString(
                                    errorResponse,
                                    'Failed to load System Diagnostics.'
                                )
                            })
                        )
                    )
                )
            )
        )
    );

    loadSystemDiagnosticsSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(SystemDiagnosticsActions.loadSystemDiagnosticsSuccess),
            switchMap(() => of(SystemDiagnosticsActions.openSystemDiagnosticsDialog()))
        )
    );

    openSystemDiagnosticsDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SystemDiagnosticsActions.openSystemDiagnosticsDialog),
                tap(() => {
                    this.dialog
                        .open(SystemDiagnosticsDialog, {
                            ...LARGE_DIALOG
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(SystemDiagnosticsActions.viewSystemDiagnosticsComplete());
                        });
                })
            ),
        { dispatch: false }
    );

    systemDiagnosticsBannerError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(SystemDiagnosticsActions.systemDiagnosticsBannerError),
            map((action) => action.error),
            switchMap((error) =>
                of(
                    ErrorActions.addBannerError({
                        errorContext: { errors: [error], context: ErrorContextKey.SYSTEM_DIAGNOSTICS }
                    })
                )
            )
        )
    );

    systemDiagnosticsSnackbarError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(SystemDiagnosticsActions.systemDiagnosticsSnackbarError),
            map((action) => action.error),
            switchMap((error) => of(ErrorActions.snackBarError({ error })))
        )
    );
}
