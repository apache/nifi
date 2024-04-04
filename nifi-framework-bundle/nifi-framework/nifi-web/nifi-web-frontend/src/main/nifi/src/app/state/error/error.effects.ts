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
import * as ErrorActions from './error.actions';
import { map, tap } from 'rxjs';
import { Router } from '@angular/router';
import { MatSnackBar } from '@angular/material/snack-bar';
import { MatDialog } from '@angular/material/dialog';

@Injectable()
export class ErrorEffects {
    constructor(
        private actions$: Actions,
        private router: Router,
        private snackBar: MatSnackBar,
        private dialog: MatDialog
    ) {}

    fullScreenError$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ErrorActions.fullScreenError),
                tap(() => {
                    this.dialog.openDialogs.forEach((dialog) => dialog.close('ROUTED'));
                    this.router.navigate(['/error'], { replaceUrl: true });
                })
            ),
        { dispatch: false }
    );

    snackBarError$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ErrorActions.snackBarError),
                map((action) => action.error),
                tap((error) => {
                    this.snackBar.open(error, 'Dismiss', { duration: 30000 });
                })
            ),
        { dispatch: false }
    );
}
