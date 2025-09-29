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
import { from, of, take } from 'rxjs';
import { catchError, map, switchMap, tap } from 'rxjs/operators';
import * as BucketsActions from './buckets.actions';
import { BucketsService } from '../../service/buckets.service';
import { ErrorHelper } from '../../service/error-helper.service';
import { ErrorContextKey } from '../error';
import * as ErrorActions from '../error/error.actions';
import { CreateBucketDialogComponent } from '../../pages/buckets/feature/ui/create-bucket-dialog/create-bucket-dialog.component';
import { EditBucketDialogComponent } from '../../pages/buckets/feature/ui/edit-bucket-dialog/edit-bucket-dialog.component';
import { ManageBucketPoliciesDialogComponent } from '../../pages/buckets/feature/ui/manage-bucket-policies-dialog/manage-bucket-policies-dialog.component';
import { LARGE_DIALOG, MEDIUM_DIALOG, SMALL_DIALOG, YesNoDialog } from '@nifi/shared';
import { deleteBucket } from './buckets.actions';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../../nifi/src/app/state';

@Injectable()
export class BucketsEffects {
    private bucketsService = inject(BucketsService);
    private errorHelper = inject(ErrorHelper);
    private dialog = inject(MatDialog);
    private actions$ = inject(Actions);
    private store = inject<Store<NiFiState>>(Store);

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
                        ...SMALL_DIALOG,
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
                tap(({ request }) => {
                    this.dialog.open(ManageBucketPoliciesDialogComponent, {
                        ...LARGE_DIALOG,
                        autoFocus: false,
                        data: { bucket: request.bucket }
                    });
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
