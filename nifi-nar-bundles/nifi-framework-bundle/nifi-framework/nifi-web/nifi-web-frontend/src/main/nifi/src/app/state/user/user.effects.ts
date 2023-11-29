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
import * as UserActions from './user.actions';
import { asyncScheduler, catchError, from, interval, map, of, switchMap, takeUntil } from 'rxjs';
import { UserService } from '../../service/user.service';

@Injectable()
export class UserEffects {
    constructor(
        private actions$: Actions,
        private userService: UserService
    ) {}

    loadUser$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserActions.loadUser),
            switchMap(() => {
                return from(
                    this.userService.getUser().pipe(
                        map((response) =>
                            UserActions.loadUserSuccess({
                                response: {
                                    user: response
                                }
                            })
                        ),
                        catchError((error) => of(UserActions.userApiError({ error: error.error })))
                    )
                );
            })
        )
    );

    startUserPolling$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserActions.startUserPolling),
            switchMap(() =>
                interval(30000, asyncScheduler).pipe(takeUntil(this.actions$.pipe(ofType(UserActions.stopUserPolling))))
            ),
            switchMap(() => of(UserActions.loadUser()))
        )
    );
}
