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

import { inject, Injectable } from '@angular/core';
import { HttpErrorResponse } from '@angular/common/http';
import { MatSnackBar } from '@angular/material/snack-bar';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { from, of } from 'rxjs';
import { catchError, map, switchMap, tap, mergeMap } from 'rxjs/operators';
import * as PoliciesActions from './policies.actions';
import { BucketsService } from '../../service/buckets.service';
import { ErrorHelper } from '../../service/error-helper.service';
import * as ErrorActions from '../error/error.actions';

@Injectable()
export class PoliciesEffects {
    private bucketsService = inject(BucketsService);
    private errorHelper = inject(ErrorHelper);
    private actions$ = inject(Actions);
    private snackBar = inject(MatSnackBar);

    loadPolicies$ = createEffect(() =>
        this.actions$.pipe(
            ofType(PoliciesActions.loadPolicies),
            switchMap(({ request }) =>
                from(this.bucketsService.getPolicies()).pipe(
                    map((policies) =>
                        PoliciesActions.loadPoliciesSuccess({
                            response: {
                                bucketId: request.bucketId,
                                policies
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            PoliciesActions.loadPoliciesFailure(),
                            ErrorActions.addBannerError({
                                errorContext: {
                                    errors: [this.errorHelper.getErrorString(errorResponse)],
                                    context: request.context
                                }
                            })
                        )
                    )
                )
            )
        )
    );

    loadPolicyTenants$ = createEffect(() =>
        this.actions$.pipe(
            ofType(PoliciesActions.loadPolicyTenants),
            switchMap(({ request }) =>
                from(this.bucketsService.getBucketPolicyTenants()).pipe(
                    map((response) => PoliciesActions.loadPolicyTenantsSuccess({ response })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            PoliciesActions.loadPolicyTenantsFailure(),
                            ErrorActions.addBannerError({
                                errorContext: {
                                    errors: [this.errorHelper.getErrorString(errorResponse)],
                                    context: request.context
                                }
                            })
                        )
                    )
                )
            )
        )
    );

    saveBucketPolicy$ = createEffect(() =>
        this.actions$.pipe(
            ofType(PoliciesActions.saveBucketPolicy),
            mergeMap(({ request }) =>
                from(
                    this.bucketsService.saveBucketPolicy({
                        bucketId: request.bucketId,
                        action: request.action,
                        policyId: request.policyId,
                        users: request.users,
                        userGroups: request.userGroups,
                        revision: request.revision
                    })
                ).pipe(
                    mergeMap(() => {
                        // Only reload policies and show toast if this is the last in a batch
                        if (request.isLastInBatch !== false) {
                            return from(this.bucketsService.getPolicies()).pipe(
                                mergeMap((policies) =>
                                    of(
                                        PoliciesActions.loadPoliciesSuccess({
                                            response: {
                                                bucketId: request.bucketId,
                                                policies
                                            }
                                        }),
                                        PoliciesActions.policyChangeSuccessToast({
                                            message: 'Bucket policies saved'
                                        })
                                    )
                                )
                            );
                        }
                        // For non-last saves, just complete without dispatching success
                        return of(PoliciesActions.policiesNoOp());
                    }),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            PoliciesActions.saveBucketPolicyFailure(),
                            ErrorActions.snackBarError({ error: this.errorHelper.getErrorString(errorResponse) })
                        )
                    )
                )
            )
        )
    );

    policyChangeSuccessToast$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(PoliciesActions.policyChangeSuccessToast),
                tap(({ message }) => {
                    this.snackBar.open(message || 'Policy updated', 'Dismiss', {
                        duration: 3000
                    });
                })
            ),
        { dispatch: false }
    );
}
