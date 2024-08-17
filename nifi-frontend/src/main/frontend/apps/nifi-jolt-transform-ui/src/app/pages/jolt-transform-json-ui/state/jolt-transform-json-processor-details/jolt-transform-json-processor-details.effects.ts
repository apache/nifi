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
import * as JoltTransformJsonUiActions from './jolt-transform-json-processor-details.actions';
import { JoltTransformJsonUiService } from '../../service/jolt-transform-json-ui.service';
import { HttpErrorResponse } from '@angular/common/http';
import { catchError, from, map, of, switchMap } from 'rxjs';
import { loadProcessorDetailsFailure } from './jolt-transform-json-processor-details.actions';
import { ProcessorDetails } from './index';

@Injectable()
export class JoltTransformJsonProcessorDetailsEffects {
    constructor(
        private actions$: Actions,
        private joltTransformJsonUiService: JoltTransformJsonUiService
    ) {}

    loadProcessorDetails$ = createEffect(() =>
        this.actions$.pipe(
            ofType(JoltTransformJsonUiActions.loadProcessorDetails),
            map((action) => action.processorId),
            switchMap((processorId) =>
                from(this.joltTransformJsonUiService.getProcessorDetails(processorId)).pipe(
                    map((response: ProcessorDetails) =>
                        JoltTransformJsonUiActions.loadProcessorDetailsSuccess({
                            response: response
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        let errorMessage = 'An unspecified error occurred.';

                        if (errorResponse.status !== 0) {
                            if (typeof errorResponse.error === 'string') {
                                errorMessage = errorResponse.error;
                            } else {
                                errorMessage = errorResponse.message || `${errorResponse.status}`;
                            }
                        }

                        return of(
                            loadProcessorDetailsFailure({
                                response: errorMessage
                            })
                        );
                    })
                )
            )
        )
    );
}
