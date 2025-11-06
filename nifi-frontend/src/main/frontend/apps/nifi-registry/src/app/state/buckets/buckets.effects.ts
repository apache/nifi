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
import { MatDialog } from '@angular/material/dialog';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { from, of, take, takeUntil, forkJoin, race } from 'rxjs';
import { catchError, map, switchMap, tap } from 'rxjs/operators';
import * as BucketsActions from './buckets.actions';
import { deleteBucket } from './buckets.actions';
import { BucketsService } from '../../service/buckets.service';
import { ErrorHelper } from '../../service/error-helper.service';
import { ErrorContextKey } from '../error';
import * as ErrorActions from '../error/error.actions';
import { CreateBucketDialogComponent } from '../../pages/buckets/feature/ui/create-bucket-dialog/create-bucket-dialog.component';
import { EditBucketDialogComponent } from '../../pages/buckets/feature/ui/edit-bucket-dialog/edit-bucket-dialog.component';
import { ManageBucketPoliciesDialogComponent } from '../../pages/buckets/feature/ui/manage-bucket-policies-dialog/manage-bucket-policies-dialog.component';
import { MEDIUM_DIALOG, XL_DIALOG, YesNoDialog } from '@nifi/shared';
import { Store } from '@ngrx/store';
import {
    selectPoliciesLoading,
    selectPoliciesSaving,
    selectPolicyOptions,
    selectPolicySelection
} from '../policies/policies.selectors';
import { selectCurrentUser } from '../current-user/current-user.selectors';
import * as PoliciesActions from '../policies/policies.actions';
import { selectBannerErrors } from '../error/error.selectors';

@Injectable()
export class BucketsEffects {
    private bucketsService = inject(BucketsService);
    private errorHelper = inject(ErrorHelper);
    private dialog = inject(MatDialog);
    private actions$ = inject(Actions);
    private store = inject(Store);

    loadBuckets$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BucketsActions.loadBuckets),
            switchMap(() =>
                from(this.bucketsService.getBuckets()).pipe(
                    map((response) =>
                        BucketsActions.loadBucketsSuccess({
                            response: {
                                buckets: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => of(this.bannerError(errorResponse)))
                )
            )
        )
    );

    openCreateBucketDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(BucketsActions.openCreateBucketDialog),
                tap(() => {
                    this.dialog.open(CreateBucketDialogComponent, {
                        ...MEDIUM_DIALOG,
                        autoFocus: false
                    });
                })
            ),
        { dispatch: false }
    );

    createBucket$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BucketsActions.createBucket),
            switchMap(({ request, keepDialogOpen }) =>
                from(this.bucketsService.createBucket(request)).pipe(
                    map((bucket) => BucketsActions.createBucketSuccess({ response: bucket, keepDialogOpen })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            BucketsActions.createBucketFailure(),
                            this.bannerError(errorResponse, ErrorContextKey.CREATE_BUCKET)
                        )
                    )
                )
            )
        )
    );

    createBucketSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(BucketsActions.createBucketSuccess),
                tap(({ keepDialogOpen }) => {
                    if (!keepDialogOpen) {
                        this.dialog.closeAll();
                    }
                })
            ),
        { dispatch: false }
    );

    openEditBucketDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(BucketsActions.openEditBucketDialog),
                tap(({ request }) => {
                    this.dialog.open(EditBucketDialogComponent, {
                        ...MEDIUM_DIALOG,
                        autoFocus: false,
                        data: { bucket: request.bucket }
                    });
                })
            ),
        { dispatch: false }
    );

    updateBucket$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BucketsActions.updateBucket),
            switchMap(({ request }) =>
                from(this.bucketsService.updateBucket(request)).pipe(
                    map((bucket) => BucketsActions.updateBucketSuccess({ response: bucket })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            BucketsActions.updateBucketFailure(),
                            this.bannerError(errorResponse, ErrorContextKey.UPDATE_BUCKET)
                        )
                    )
                )
            )
        )
    );

    updateBucketSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(BucketsActions.updateBucketSuccess),
                tap(() => this.dialog.closeAll())
            ),
        { dispatch: false }
    );

    openDeleteBucketDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(BucketsActions.openDeleteBucketDialog),
                tap(({ request }) => {
                    const dialogRef = this.dialog.open(YesNoDialog, {
                        ...MEDIUM_DIALOG,
                        data: {
                            title: 'Delete Bucket',
                            message: `All items stored in this bucket will be deleted as well.`
                        }
                    });
                    dialogRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            deleteBucket({
                                request: {
                                    bucket: request.bucket,
                                    version: request.bucket.revision.version
                                }
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    deleteBucket$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BucketsActions.deleteBucket),
            switchMap(({ request }) =>
                from(this.bucketsService.deleteBucket(request)).pipe(
                    map((bucket) => BucketsActions.deleteBucketSuccess({ response: bucket })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            BucketsActions.deleteBucketFailure(),
                            ErrorActions.snackBarError({ error: this.errorHelper.getErrorString(errorResponse) })
                        )
                    )
                )
            )
        )
    );

    deleteBucketSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(BucketsActions.deleteBucketSuccess),
                tap(() => this.dialog.closeAll())
            ),
        { dispatch: false }
    );

    openManageBucketPoliciesDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(BucketsActions.openManageBucketPoliciesDialog),
                switchMap(({ request }) => {
                    // Dispatch both load actions
                    this.store.dispatch(
                        PoliciesActions.loadPolicyTenants({ request: { context: ErrorContextKey.GLOBAL } })
                    );
                    this.store.dispatch(
                        PoliciesActions.loadPolicies({
                            request: { bucketId: request.bucket.identifier, context: ErrorContextKey.GLOBAL }
                        })
                    );

                    // Wait for both success actions or either failure action
                    const tenantsSuccess$ = this.actions$.pipe(
                        ofType(PoliciesActions.loadPolicyTenantsSuccess),
                        take(1)
                    );

                    const policiesSuccess$ = this.actions$.pipe(ofType(PoliciesActions.loadPoliciesSuccess), take(1));

                    const anyFailure$ = this.actions$.pipe(
                        ofType(PoliciesActions.loadPolicyTenantsFailure, PoliciesActions.loadPoliciesFailure),
                        take(1),
                        map(() => null) // Return null to indicate failure
                    );

                    // Race between both succeeding or any failing
                    return race(forkJoin([tenantsSuccess$, policiesSuccess$]), anyFailure$).pipe(
                        tap((result) => {
                            // Only open dialog if both succeeded (result is not null)
                            if (result) {
                                const dialogRef = this.dialog.open(ManageBucketPoliciesDialogComponent, {
                                    ...XL_DIALOG,
                                    autoFocus: false,
                                    data: {
                                        bucket: request.bucket,
                                        options$: this.store.select(selectPolicyOptions),
                                        selection$: this.store.select(selectPolicySelection),
                                        loading$: this.store.select(selectPoliciesLoading),
                                        saving$: this.store.select(selectPoliciesSaving),
                                        isPolicyError$: this.store
                                            .select(selectBannerErrors(ErrorContextKey.MANAGE_ACCESS))
                                            .pipe(
                                                map((bannerErrors) => {
                                                    if (bannerErrors.length > 0) {
                                                        return true;
                                                    }
                                                    return false;
                                                })
                                            ),
                                        isAddPolicyDisabled$: this.store.select(selectCurrentUser).pipe(
                                            map((currentUser) => {
                                                // Disable if anonymous
                                                if (currentUser.anonymous) {
                                                    return true;
                                                }
                                                // Disable if user can't write policies
                                                if (!currentUser.resourcePermissions.policies.canWrite) {
                                                    return true;
                                                }
                                                // Disable if user can't read tenants
                                                if (!currentUser.resourcePermissions.tenants.canRead) {
                                                    return true;
                                                }
                                                return false;
                                            })
                                        )
                                    }
                                });

                                // Subscribe to output and handle multiple emissions until dialog closes
                                dialogRef.componentInstance.savePolicies
                                    .pipe(takeUntil(dialogRef.afterClosed()))
                                    .subscribe((saveRequest) => {
                                        this.store.dispatch(
                                            PoliciesActions.saveBucketPolicy({
                                                request: {
                                                    bucketId: saveRequest.bucketId,
                                                    action: saveRequest.action,
                                                    policyId: saveRequest.policyId,
                                                    revision: saveRequest.revision,
                                                    users: saveRequest.users,
                                                    userGroups: saveRequest.userGroups,
                                                    isLastInBatch: saveRequest.isLastInBatch
                                                }
                                            })
                                        );
                                    });
                            }
                            // If result is null, errors are already handled by the individual effects
                        })
                    );
                })
            ),
        { dispatch: false }
    );

    private bannerError(errorResponse: HttpErrorResponse, context: ErrorContextKey = ErrorContextKey.GLOBAL) {
        return ErrorActions.addBannerError({
            errorContext: {
                errors: [this.errorHelper.getErrorString(errorResponse)],
                context
            }
        });
    }
}
