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
import * as JoltTransformJsonUiActions from './jolt-transform-json-property.actions';
import { JoltTransformJsonUiService } from '../../service/jolt-transform-json-ui.service';
import { HttpErrorResponse } from '@angular/common/http';
import { catchError, from, map, of, switchMap, tap } from 'rxjs';
import { savePropertiesFailure } from './jolt-transform-json-property.actions';
import { SavePropertiesSuccess } from './index';
import { Store } from '@ngrx/store';
import { resetJoltTransformJsonValidateState } from '../jolt-transform-json-validate/jolt-transform-json-validate.actions';
import { NiFiJoltTransformJsonUiState } from '../../../../state';

@Injectable()
export class JoltTransformJsonPropertyEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiJoltTransformJsonUiState>,
        private joltTransformJsonUiService: JoltTransformJsonUiService
    ) {}

    saveProperties$ = createEffect(() =>
        this.actions$.pipe(
            ofType(JoltTransformJsonUiActions.saveProperties),
            map((action) => action.request),
            switchMap((request) =>
                from(this.joltTransformJsonUiService.saveProperties(request)).pipe(
                    tap(() => {
                        this.store.dispatch(resetJoltTransformJsonValidateState());
                    }),
                    map((response: SavePropertiesSuccess) =>
                        JoltTransformJsonUiActions.savePropertiesSuccess({
                            response: response
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        return of(
                            savePropertiesFailure({
                                response: errorResponse
                            })
                        );
                    })
                )
            )
        )
    );
}
