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
import * as UserActions from './current-user.actions';
import { asyncScheduler, catchError, from, interval, map, of, switchMap, takeUntil } from 'rxjs';
import { CurrentUserService } from '../../service/current-user.service';
import { ErrorHelper } from '../../service/error-helper.service';

@Injectable()
export class CurrentUserEffects {
    constructor(
        private actions$: Actions,
        private userService: CurrentUserService,
        private errorHelper: ErrorHelper
    ) {}

    loadCurrentUser$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserActions.loadCurrentUser),
            switchMap(() => {
                return from(
                    this.userService.getUser().pipe(
                        map((response) =>
                            UserActions.loadCurrentUserSuccess({
                                response: {
                                    user: response
                                }
                            })
                        ),
                        catchError((error) => of(this.errorHelper.fullScreenError(error)))
                    )
                );
            })
        )
    );

    startCurrentUserPolling$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserActions.startCurrentUserPolling),
            switchMap(() =>
                interval(30000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(UserActions.stopCurrentUserPolling)))
                )
            ),
            switchMap(() => of(UserActions.loadCurrentUser()))
        )
    );
}
