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
import { catchError, from, map, of, switchMap, tap } from 'rxjs';
import { Store } from '@ngrx/store';
import { Router } from '@angular/router';
import { HttpErrorResponse } from '@angular/common/http';
import { NiFiState } from '../index';
import * as BucketsActions from './buckets.actions';
import { BucketsService } from '../../service/buckets.service';
import { ErrorHelper } from '../../service/error-helper.service';
import * as ErrorActions from '../../state/error/error.actions';

@Injectable()
export class BucketsEffects {
    constructor(
        private store: Store<NiFiState>,
        private router: Router,
        private bucketsService: BucketsService,
        private errorHelper: ErrorHelper
    ) {}

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
                        catchError((errorResponse: HttpErrorResponse) => {
                            return of(
                                ErrorActions.snackBarError({ error: this.errorHelper.getErrorString(errorResponse) })
                            );
                        })
                    )
                );
            })
        )
    );
}
