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
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { catchError, from, map, of, switchMap } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import * as BucketsActions from './buckets.actions';
import { BucketsService } from '../../service/buckets.service';
import { ErrorHelper } from '../../service/error-helper.service';
import { ErrorContextKey } from '../error';
import * as DropletsActions from '../droplets/droplets.actions';

@Injectable()
export class BucketsEffects {
    private bucketsService = inject(BucketsService);
    private errorHelper = inject(ErrorHelper);

    actions$ = inject(Actions);

    loadBuckets$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BucketsActions.loadBuckets),
            switchMap(() => {
                return from(
                    this.bucketsService.getBuckets().pipe(
                        map((response) =>
                            BucketsActions.loadBucketsSuccess({
                                response: {
                                    buckets: response
                                }
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) => of(this.bannerError(errorResponse)))
                    )
                );
            })
        )
    );

    private bannerError(errorResponse: HttpErrorResponse, context: ErrorContextKey = ErrorContextKey.GLOBAL) {
        return DropletsActions.dropletsBannerError({
            errorContext: {
                errors: [this.errorHelper.getErrorString(errorResponse)],
                context
            }
        });
    }
}
