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
import * as EvaluationContextActions from './evaluation-context.actions';
import { UpdateAttributeService } from '../../service/update-attribute.service';
import { HttpErrorResponse } from '@angular/common/http';
import { catchError, from, map, of, switchMap, tap } from 'rxjs';
import { Store } from '@ngrx/store';
import { UpdateAttributeApplicationState } from '../../../../state';
import { concatLatestFrom } from '@ngrx/operators';
import { selectAdvancedUiParameters } from '../advanced-ui-parameters/advanced-ui-parameters.selectors';
import { EvaluationContextEntity } from './index';
import { updateRevision } from '../advanced-ui-parameters/advanced-ui-parameters.actions';
import { MatSnackBar } from '@angular/material/snack-bar';
import { resetEvaluationContextFailure } from './evaluation-context.actions';

@Injectable()
export class EvaluationContextEffects {
    constructor(
        private actions$: Actions,
        private snackBar: MatSnackBar,
        private store: Store<UpdateAttributeApplicationState>,
        private updateAttributeService: UpdateAttributeService
    ) {}

    loadEvaluationContext$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EvaluationContextActions.loadEvaluationContext),
            concatLatestFrom(() => this.store.select(selectAdvancedUiParameters)),
            switchMap(([, advancedUiParameters]) =>
                from(this.updateAttributeService.getEvaluationContext(advancedUiParameters.processorId)).pipe(
                    map((evaluationContextEntity: EvaluationContextEntity) =>
                        EvaluationContextActions.loadEvaluationContextSuccess({
                            evaluationContextEntity
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        const error = this.updateAttributeService.getErrorString(errorResponse);
                        return of(
                            EvaluationContextActions.loadEvaluationContextFailure({
                                error
                            })
                        );
                    })
                )
            )
        )
    );

    saveEvaluationContext$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EvaluationContextActions.saveEvaluationContext),
            map((action) => action.evaluationContext),
            concatLatestFrom(() => this.store.select(selectAdvancedUiParameters)),
            switchMap(([evaluationContext, advancedUiParameters]) =>
                from(this.updateAttributeService.saveEvaluationContext(advancedUiParameters, evaluationContext)).pipe(
                    map((evaluationContextEntity: EvaluationContextEntity) =>
                        EvaluationContextActions.saveEvaluationContextSuccess({
                            evaluationContextEntity
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        const error = this.updateAttributeService.getErrorString(errorResponse);
                        return of(
                            EvaluationContextActions.saveEvaluationContextFailure({
                                error
                            })
                        );
                    })
                )
            )
        )
    );

    saveEvaluationContextSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EvaluationContextActions.saveEvaluationContextSuccess),
            map((action) => action.evaluationContextEntity),
            tap(() => {
                this.snackBar.open('Evaluation criteria successfully saved', 'Ok', { duration: 30000 });
            }),
            switchMap((evaluationContextEntity) =>
                of(
                    updateRevision({
                        revision: evaluationContextEntity.revision
                    })
                )
            )
        )
    );

    saveEvaluationContextFailure$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EvaluationContextActions.saveEvaluationContextFailure),
            map((action) => action.error),
            tap((error) => {
                this.snackBar.open(error, 'Dismiss', { duration: 30000 });
            }),
            switchMap(() => of(resetEvaluationContextFailure()))
        )
    );
}
