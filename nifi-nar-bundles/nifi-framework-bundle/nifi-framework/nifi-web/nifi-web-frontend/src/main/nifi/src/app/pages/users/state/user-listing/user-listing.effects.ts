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
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { NiFiState } from '../../../../state';
import { Store } from '@ngrx/store';
import { Router } from '@angular/router';
import * as UserListingActions from './user-listing.actions';
import { catchError, combineLatest, map, of, switchMap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { UsersService } from '../../service/users.service';

@Injectable()
export class UserListingEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private router: Router,
        private usersService: UsersService,
        private dialog: MatDialog
    ) {}

    loadTenants$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.loadTenants),
            switchMap(() =>
                combineLatest([this.usersService.getUsers(), this.usersService.getUserGroups()]).pipe(
                    map(([usersResponse, userGroupsResponse]) =>
                        UserListingActions.loadTenantsSuccess({
                            response: {
                                users: usersResponse.users,
                                userGroups: userGroupsResponse.userGroups,
                                loadedTimestamp: usersResponse.generated
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            UserListingActions.usersApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );
}
