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
import * as TenantsActions from './tenants.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import { catchError, combineLatest, map, of, switchMap } from 'rxjs';
import { AccessPolicyService } from '../../service/access-policy.service';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../../../service/error-helper.service';

@Injectable()
export class TenantsEffects {
    constructor(
        private actions$: Actions,
        private accessPoliciesService: AccessPolicyService,
        private errorHelper: ErrorHelper
    ) {}

    loadTenants$ = createEffect(() =>
        this.actions$.pipe(
            ofType(TenantsActions.loadTenants),
            switchMap(() =>
                combineLatest([this.accessPoliciesService.getUsers(), this.accessPoliciesService.getUserGroups()]).pipe(
                    map(([usersResponse, userGroupsResponse]) =>
                        TenantsActions.loadTenantsSuccess({
                            response: {
                                users: usersResponse.users,
                                userGroups: userGroupsResponse.userGroups
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ErrorActions.snackBarError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );
}
