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
import { Actions, concatLatestFrom, createEffect, ofType } from '@ngrx/effects';
import * as ReportingTaskActions from './reporting-tasks.actions';
import { catchError, from, map, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { selectReportingTaskTypes } from '../../../../state/extension-types/extension-types.selectors';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { ReportingTaskService } from '../../service/reporting-task.service';
import { CreateReportingTask } from '../../ui/reporting-tasks/create-reporting-task/create-reporting-task.component';
import { Router } from '@angular/router';
import { selectSaving } from '../management-controller-services/management-controller-services.selectors';
import { UpdateControllerServiceRequest } from '../../../../state/shared';
import { EditReportingTask } from '../../ui/reporting-tasks/edit-reporting-task/edit-reporting-task.component';
import { CreateReportingTaskSuccess } from './index';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { Client } from '../../../../service/client.service';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';

@Injectable()
export class ReportingTasksEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private client: Client,
        private reportingTaskService: ReportingTaskService,
        private managementControllerServiceService: ManagementControllerServiceService,
        private extensionTypesService: ExtensionTypesService,
        private dialog: MatDialog,
        private router: Router,
        private propertyTableHelperService: PropertyTableHelperService
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
                concatLatestFrom(() => this.store.select(selectReportingTaskTypes)),
                tap(([, reportingTaskTypes]) => {
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

    createReportingTaskSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ReportingTaskActions.createReportingTaskSuccess),
            map((action) => action.response),
            tap(() => {
                this.dialog.closeAll();
            }),
            switchMap((response: CreateReportingTaskSuccess) =>
                of(
                    ReportingTaskActions.selectReportingTask({
                        request: {
                            id: response.reportingTask.id
                        }
                    })
                )
            )
        )
    );

    promptReportingTaskDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ReportingTaskActions.promptReportingTaskDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        data: {
                            title: 'Delete Reporting Task',
                            message: `Delete reporting task ${request.reportingTask.component.name}?`
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

    navigateToEditReportingTask$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ReportingTaskActions.navigateToEditReportingTask),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['/settings', 'reporting-tasks', id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    openConfigureReportingTaskDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ReportingTaskActions.openConfigureReportingTaskDialog),
                map((action) => action.request),
                tap((request) => {
                    const taskId: string = request.id;

                    const editDialogReference = this.dialog.open(EditReportingTask, {
                        data: {
                            reportingTask: request.reportingTask
                        },
                        id: taskId,
                        panelClass: 'large-dialog'
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.createNewProperty =
                        this.propertyTableHelperService.createNewProperty(request.id, this.reportingTaskService);

                    const goTo = (commands: string[], destination: string): void => {
                        if (editDialogReference.componentInstance.editReportingTaskForm.dirty) {
                            const saveChangesDialogReference = this.dialog.open(YesNoDialog, {
                                data: {
                                    title: 'Reporting Task Configuration',
                                    message: `Save changes before going to this ${destination}?`
                                },
                                panelClass: 'small-dialog'
                            });

                            saveChangesDialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                                editDialogReference.componentInstance.submitForm(commands);
                            });

                            saveChangesDialogReference.componentInstance.no.pipe(take(1)).subscribe(() => {
                                editDialogReference.close('ROUTED');
                                this.router.navigate(commands);
                            });
                        } else {
                            editDialogReference.close('ROUTED');
                            this.router.navigate(commands);
                        }
                    };

                    editDialogReference.componentInstance.goToService = (serviceId: string) => {
                        const commands: string[] = ['/settings', 'management-controller-services', serviceId];
                        goTo(commands, 'Controller Service');
                    };

                    editDialogReference.componentInstance.createNewService =
                        this.propertyTableHelperService.createNewService(
                            request.id,
                            this.managementControllerServiceService,
                            this.reportingTaskService
                        );

                    editDialogReference.componentInstance.editReportingTask
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((updateControllerServiceRequest: UpdateControllerServiceRequest) => {
                            this.store.dispatch(
                                ReportingTaskActions.configureReportingTask({
                                    request: {
                                        id: request.reportingTask.id,
                                        uri: request.reportingTask.uri,
                                        payload: updateControllerServiceRequest.payload,
                                        postUpdateNavigation: updateControllerServiceRequest.postUpdateNavigation
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                ReportingTaskActions.selectReportingTask({
                                    request: {
                                        id: taskId
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    configureReportingTask$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ReportingTaskActions.configureReportingTask),
            map((action) => action.request),
            switchMap((request) =>
                from(this.reportingTaskService.updateReportingTask(request)).pipe(
                    map((response) =>
                        ReportingTaskActions.configureReportingTaskSuccess({
                            response: {
                                id: request.id,
                                reportingTask: response,
                                postUpdateNavigation: request.postUpdateNavigation
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

    configureReportingTaskSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ReportingTaskActions.configureReportingTaskSuccess),
                map((action) => action.response),
                tap((response) => {
                    if (response.postUpdateNavigation) {
                        this.router.navigate(response.postUpdateNavigation);
                        this.dialog.getDialogById(response.id)?.close('ROUTED');
                    } else {
                        this.dialog.closeAll();
                    }
                })
            ),
        { dispatch: false }
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
                    this.router.navigate(['/settings', 'reporting-tasks', request.id]);
                })
            ),
        { dispatch: false }
    );
}
