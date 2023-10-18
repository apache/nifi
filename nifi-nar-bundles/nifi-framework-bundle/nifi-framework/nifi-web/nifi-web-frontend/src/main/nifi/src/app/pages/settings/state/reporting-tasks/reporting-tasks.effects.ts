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

import { Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import * as ReportingTaskActions from './reporting-tasks.actions';
import { catchError, from, map, of, switchMap, take, tap, withLatestFrom } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { selectReportingTaskTypes } from '../../../../state/extension-types/extension-types.selectors';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { ReportingTaskService } from '../../service/reporting-task.service';
import { CreateReportingTask } from '../../ui/reporting-tasks/create-reporting-task/create-reporting-task.component';
import { Router } from '@angular/router';

@Injectable()
export class ReportingTasksEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private reportingTaskService: ReportingTaskService,
        private dialog: MatDialog,
        private router: Router
    ) {}

    loadReportingTasks$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ReportingTaskActions.loadReportingTasks),
            switchMap(() =>
                from(this.reportingTaskService.getReportingTasks()).pipe(
                    map((response) =>
                        ReportingTaskActions.loadReportingTasksSuccess({
                            response: {
                                reportingTasks: response.reportingTasks,
                                loadedTimestamp: response.currentTime
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ReportingTaskActions.reportingTasksApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    openNewReportingTaskDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ReportingTaskActions.openNewReportingTaskDialog),
                withLatestFrom(this.store.select(selectReportingTaskTypes)),
                tap(([action, reportingTaskTypes]) => {
                    this.dialog.open(CreateReportingTask, {
                        data: {
                            reportingTaskTypes
                        },
                        panelClass: 'medium-dialog'
                    });
                })
            ),
        { dispatch: false }
    );

    createReportingTask$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ReportingTaskActions.createReportingTask),
            map((action) => action.request),
            switchMap((request) =>
                from(this.reportingTaskService.createReportingTask(request)).pipe(
                    map((response) =>
                        ReportingTaskActions.createReportingTaskSuccess({
                            response: {
                                reportingTask: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ReportingTaskActions.reportingTasksApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    createReportingTaskSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ReportingTaskActions.createReportingTaskSuccess),
                tap(() => {
                    this.dialog.closeAll();
                })
            ),
        { dispatch: false }
    );

    promptReportingTaskDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ReportingTaskActions.promptReportingTaskDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        data: {
                            title: 'Delete Controller Service',
                            message: `Delete controller service ${request.reportingTask.component.name}?`
                        },
                        panelClass: 'small-dialog'
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            ReportingTaskActions.deleteReportingTask({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    deleteReportingTask$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ReportingTaskActions.deleteReportingTask),
            map((action) => action.request),
            switchMap((request) =>
                from(this.reportingTaskService.deleteReportingTask(request)).pipe(
                    map((response) =>
                        ReportingTaskActions.deleteReportingTaskSuccess({
                            response: {
                                reportingTask: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ReportingTaskActions.reportingTasksApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    startReportingTask$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ReportingTaskActions.startReportingTask),
            map((action) => action.request),
            switchMap((request) =>
                from(this.reportingTaskService.startReportingTask(request)).pipe(
                    map((response) =>
                        ReportingTaskActions.startReportingTaskSuccess({
                            response: {
                                reportingTask: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ReportingTaskActions.reportingTasksApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    stopReportingTask$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ReportingTaskActions.stopReportingTask),
            map((action) => action.request),
            switchMap((request) =>
                from(this.reportingTaskService.stopReportingTask(request)).pipe(
                    map((response) =>
                        ReportingTaskActions.stopReportingTaskSuccess({
                            response: {
                                reportingTask: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ReportingTaskActions.reportingTasksApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    selectReportingTask$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ReportingTaskActions.selectReportingTask),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/settings', 'reporting-tasks', request.reportingTask.id]);
                })
            ),
        { dispatch: false }
    );
}
