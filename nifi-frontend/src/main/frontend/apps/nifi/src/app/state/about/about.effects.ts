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
import * as AboutActions from './about.actions';
import * as ErrorActions from '../error/error.actions';
import { catchError, from, map, of, switchMap, tap } from 'rxjs';
import { AboutService } from '../../service/about.service';
import { HttpErrorResponse } from '@angular/common/http';
import { MatDialog } from '@angular/material/dialog';
import { AboutDialog } from '../../ui/common/about-dialog/about-dialog.component';
import { MEDIUM_DIALOG } from '@nifi/shared';
import { ErrorHelper } from '../../service/error-helper.service';

@Injectable()
export class AboutEffects {
    constructor(
        private actions$: Actions,
        private aboutService: AboutService,
        private dialog: MatDialog,
        private errorHelper: ErrorHelper
    ) {}

    loadAbout$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AboutActions.loadAbout),
            switchMap(() => {
                return from(
                    this.aboutService.getAbout().pipe(
                        map((response) =>
                            AboutActions.loadAboutSuccess({
                                response
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(ErrorActions.snackBarError({ error: this.errorHelper.getErrorString(errorResponse) }))
                        )
                    )
                );
            })
        )
    );

    openAboutDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(AboutActions.openAboutDialog),
                tap(() => {
                    this.dialog.open(AboutDialog, {
                        ...MEDIUM_DIALOG,
                        autoFocus: false
                    });
                })
            ),
        { dispatch: false }
    );
}
