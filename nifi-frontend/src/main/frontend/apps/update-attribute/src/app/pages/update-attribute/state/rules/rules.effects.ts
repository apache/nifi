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
import * as RulesActions from './rules.actions';
import { UpdateAttributeService } from '../../service/update-attribute.service';
import { HttpErrorResponse } from '@angular/common/http';
import { catchError, from, map, of, switchMap, take, tap } from 'rxjs';
import { concatLatestFrom } from '@ngrx/operators';
import { Store } from '@ngrx/store';
import { UpdateAttributeApplicationState } from '../../../../state';
import { selectAdvancedUiParameters } from '../advanced-ui-parameters/advanced-ui-parameters.selectors';
import { RulesEntity } from './index';
import { MatDialog } from '@angular/material/dialog';
import { SMALL_DIALOG, YesNoDialog } from '@nifi/shared';
import { updateRevision } from '../advanced-ui-parameters/advanced-ui-parameters.actions';
import { MatSnackBar } from '@angular/material/snack-bar';
import { loadEvaluationContext } from '../evaluation-context/evaluation-context.actions';

@Injectable()
export class RulesEffects {
    constructor(
        private actions$: Actions,
        private dialog: MatDialog,
        private snackBar: MatSnackBar,
        private store: Store<UpdateAttributeApplicationState>,
        private updateAttributeService: UpdateAttributeService
    ) {}

    loadRules$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RulesActions.loadRules),
            concatLatestFrom(() => this.store.select(selectAdvancedUiParameters)),
            switchMap(([, advancedUiParameters]) =>
                from(this.updateAttributeService.getRules(advancedUiParameters.processorId)).pipe(
                    map((rulesEntity: RulesEntity) =>
                        RulesActions.loadRulesSuccess({
                            rulesEntity
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        const error = this.updateAttributeService.getErrorString(errorResponse);
                        return of(
                            RulesActions.loadRulesFailure({
                                error
                            })
                        );
                    })
                )
            )
        )
    );

    createRule$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RulesActions.createRule),
            map((action) => action.newRule),
            concatLatestFrom(() => this.store.select(selectAdvancedUiParameters)),
            switchMap(([newRule, advancedUiParameters]) =>
                from(this.updateAttributeService.createRule(advancedUiParameters, newRule)).pipe(
                    map((entity) =>
                        RulesActions.createRuleSuccess({
                            entity
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        const error = this.updateAttributeService.getErrorString(errorResponse);
                        return of(
                            RulesActions.saveRuleFailure({
                                error
                            })
                        );
                    })
                )
            )
        )
    );

    createRuleSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RulesActions.createRuleSuccess),
            map((action) => action.entity),
            tap(() => {
                this.snackBar.open('Rule successfully added', 'Ok', { duration: 30000 });
                this.store.dispatch(loadEvaluationContext());
            }),
            switchMap((entity) =>
                of(
                    updateRevision({
                        revision: entity.revision
                    })
                )
            )
        )
    );

    editRule$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RulesActions.editRule),
            map((action) => action.rule),
            concatLatestFrom(() => this.store.select(selectAdvancedUiParameters)),
            switchMap(([rule, advancedUiParameters]) =>
                from(this.updateAttributeService.saveRule(advancedUiParameters, rule)).pipe(
                    map((entity) =>
                        RulesActions.editRuleSuccess({
                            entity
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        const error = this.updateAttributeService.getErrorString(errorResponse);
                        return of(
                            RulesActions.saveRuleFailure({
                                error
                            })
                        );
                    })
                )
            )
        )
    );

    editRuleSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RulesActions.editRuleSuccess),
            map((action) => action.entity),
            tap(() => {
                this.snackBar.open('Rule successfully saved', 'Ok', { duration: 30000 });
            }),
            switchMap((entity) =>
                of(
                    updateRevision({
                        revision: entity.revision
                    })
                )
            )
        )
    );

    promptRuleDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(RulesActions.promptRuleDeletion),
                map((action) => action.rule),
                tap((rule) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Delete Rule',
                            message: `Delete rule ${rule.name}?`
                        }
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            RulesActions.deleteRule({
                                rule
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    deleteRule$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RulesActions.deleteRule),
            map((action) => action.rule),
            concatLatestFrom(() => this.store.select(selectAdvancedUiParameters)),
            switchMap(([rule, advancedUiParameters]) =>
                from(this.updateAttributeService.deleteRule(advancedUiParameters, rule.id)).pipe(
                    map((entity) =>
                        RulesActions.deleteRuleSuccess({
                            response: {
                                id: rule.id,
                                revision: entity.revision
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        const error = this.updateAttributeService.getErrorString(errorResponse);
                        return of(
                            RulesActions.saveRuleFailure({
                                error
                            })
                        );
                    })
                )
            )
        )
    );

    deleteRuleSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RulesActions.deleteRuleSuccess),
            map((action) => action.response),
            tap(() => {
                this.snackBar.open('Rule successfully deleted', 'Ok', { duration: 30000 });
                this.store.dispatch(loadEvaluationContext());
            }),
            switchMap((response) =>
                of(
                    updateRevision({
                        revision: response.revision
                    })
                )
            )
        )
    );

    saveRuleFailure$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(RulesActions.saveRuleFailure),
                map((action) => action.error),
                tap((error) => {
                    this.snackBar.open(error, 'Dismiss', { duration: 30000 });
                })
            ),
        { dispatch: false }
    );
}
