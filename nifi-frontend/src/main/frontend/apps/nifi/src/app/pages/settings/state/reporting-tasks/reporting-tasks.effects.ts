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
import { concatLatestFrom } from '@ngrx/operators';
import * as ReportingTaskActions from './reporting-tasks.actions';
import { catchError, from, map, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { selectReportingTaskTypes } from '../../../../state/extension-types/extension-types.selectors';
import { LARGE_DIALOG, SMALL_DIALOG, XL_DIALOG, YesNoDialog } from '@nifi/shared';
import { ReportingTaskService } from '../../service/reporting-task.service';
import { CreateReportingTask } from '../../ui/reporting-tasks/create-reporting-task/create-reporting-task.component';
import { Router } from '@angular/router';
import { selectSaving } from '../management-controller-services/management-controller-services.selectors';
import { OpenChangeComponentVersionDialogRequest } from '../../../../state/shared';
import { EditReportingTask } from '../../ui/reporting-tasks/edit-reporting-task/edit-reporting-task.component';
import { CreateReportingTaskSuccess, EditReportingTaskDialogRequest, UpdateReportingTaskRequest } from './index';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { selectStatus } from './reporting-tasks.selectors';
import { HttpErrorResponse } from '@angular/common/http';
import { ChangeComponentVersionDialog } from '../../../../ui/common/change-component-version-dialog/change-component-version-dialog';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import {
    resetPropertyVerificationState,
    verifyProperties
} from '../../../../state/property-verification/property-verification.actions';
import {
    selectPropertyVerificationResults,
    selectPropertyVerificationStatus
} from '../../../../state/property-verification/property-verification.selectors';
import { VerifyPropertiesRequestContext } from '../../../../state/property-verification';
import { BackNavigation } from '../../../../state/navigation';
import { ErrorContextKey } from '../../../../state/error';

@Injectable()
export class ReportingTasksEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private reportingTaskService: ReportingTaskService,
        private managementControllerServiceService: ManagementControllerServiceService,
        private errorHelper: ErrorHelper,
        private dialog: MatDialog,
        private router: Router,
        private propertyTableHelperService: PropertyTableHelperService,
        private extensionTypesService: ExtensionTypesService
    ) {}

    loadReportingTasks$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ReportingTaskActions.loadReportingTasks),
            concatLatestFrom(() => this.store.select(selectStatus)),
            switchMap(([, status]) =>
                from(this.reportingTaskService.getReportingTasks()).pipe(
                    map((response) =>
                        ReportingTaskActions.loadReportingTasksSuccess({
                            response: {
                                reportingTasks: response.reportingTasks,
                                loadedTimestamp: response.currentTime
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.errorHelper.handleLoadingError(status, errorResponse))
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
                        ...LARGE_DIALOG,
                        data: {
                            reportingTaskTypes
                        }
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
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.dialog.closeAll();
                        return of(
                            ReportingTaskActions.reportingTasksSnackbarApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        );
                    })
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

    reportingTasksBannerApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ReportingTaskActions.reportingTasksBannerApiError),
            map((action) => action.error),
            switchMap((error) =>
                of(
                    ErrorActions.addBannerError({
                        errorContext: { errors: [error], context: ErrorContextKey.REPORTING_TASKS }
                    })
                )
            )
        )
    );

    reportingTasksSnackbarApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ReportingTaskActions.reportingTasksSnackbarApiError),
            map((action) => action.error),
            switchMap((error) => of(ErrorActions.snackBarError({ error })))
        )
    );

    promptReportingTaskDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ReportingTaskActions.promptReportingTaskDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Delete Reporting Task',
                            message: `Delete reporting task ${request.reportingTask.component.name}?`
                        }
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
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ReportingTaskActions.reportingTasksSnackbarApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
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

    navigateToAdvancedReportingTaskUi$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ReportingTaskActions.navigateToAdvancedReportingTaskUi),
                map((action) => action.id),
                tap((id) => {
                    const routeBoundary: string[] = ['/settings', 'reporting-tasks', id, 'advanced'];
                    this.router.navigate([...routeBoundary], {
                        state: {
                            backNavigation: {
                                route: ['/settings', 'reporting-tasks', id],
                                routeBoundary,
                                context: 'Reporting Task'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    navigateToManageAccessPolicies$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ReportingTaskActions.navigateToManageAccessPolicies),
                map((action) => action.id),
                tap((id) => {
                    const routeBoundary = ['/access-policies'];
                    this.router.navigate([...routeBoundary, 'read', 'component', 'reporting-tasks', id], {
                        state: {
                            backNavigation: {
                                route: ['/settings', 'reporting-tasks', id],
                                routeBoundary,
                                context: 'Reporting Task'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    openConfigureReportingTaskDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ReportingTaskActions.openConfigureReportingTaskDialog),
                map((action) => action.request),
                switchMap((request) =>
                    from(this.propertyTableHelperService.getComponentHistory(request.id)).pipe(
                        map((history) => {
                            return {
                                ...request,
                                history: history.componentHistory
                            } as EditReportingTaskDialogRequest;
                        }),
                        tap({
                            error: (errorResponse: HttpErrorResponse) => {
                                this.store.dispatch(
                                    ReportingTaskActions.selectReportingTask({
                                        request: {
                                            id: request.id
                                        }
                                    })
                                );
                                this.store.dispatch(
                                    ReportingTaskActions.reportingTasksSnackbarApiError({
                                        error: this.errorHelper.getErrorString(errorResponse)
                                    })
                                );
                            }
                        })
                    )
                ),
                tap((request) => {
                    const taskId: string = request.id;

                    const editDialogReference = this.dialog.open(EditReportingTask, {
                        ...XL_DIALOG,
                        data: request,
                        id: taskId
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.createNewProperty =
                        this.propertyTableHelperService.createNewProperty(request.id, this.reportingTaskService);

                    editDialogReference.componentInstance.verify
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((verificationRequest: VerifyPropertiesRequestContext) => {
                            this.store.dispatch(
                                verifyProperties({
                                    request: verificationRequest
                                })
                            );
                        });

                    editDialogReference.componentInstance.propertyVerificationResults$ = this.store.select(
                        selectPropertyVerificationResults
                    );
                    editDialogReference.componentInstance.propertyVerificationStatus$ = this.store.select(
                        selectPropertyVerificationStatus
                    );

                    const goTo = (commands: string[], destination: string, commandBoundary: string[]): void => {
                        if (editDialogReference.componentInstance.editReportingTaskForm.dirty) {
                            const saveChangesDialogReference = this.dialog.open(YesNoDialog, {
                                ...SMALL_DIALOG,
                                data: {
                                    title: 'Reporting Task Configuration',
                                    message: `Save changes before going to this ${destination}?`
                                }
                            });

                            saveChangesDialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                                editDialogReference.componentInstance.submitForm(commands, commandBoundary);
                            });

                            saveChangesDialogReference.componentInstance.no.pipe(take(1)).subscribe(() => {
                                this.router.navigate(commands, {
                                    state: {
                                        backNavigation: {
                                            route: ['/settings', 'reporting-tasks', taskId, 'edit'],
                                            routeBoundary: commandBoundary,
                                            context: 'Reporting Task'
                                        } as BackNavigation
                                    }
                                });
                            });
                        } else {
                            this.router.navigate(commands, {
                                state: {
                                    backNavigation: {
                                        route: ['/settings', 'reporting-tasks', taskId, 'edit'],
                                        routeBoundary: commandBoundary,
                                        context: 'Reporting Task'
                                    } as BackNavigation
                                }
                            });
                        }
                    };

                    editDialogReference.componentInstance.goToService = (serviceId: string) => {
                        const commandBoundary: string[] = ['/settings', 'management-controller-services'];
                        const commands: string[] = [...commandBoundary, serviceId];
                        goTo(commands, 'Controller Service', commandBoundary);
                    };

                    editDialogReference.componentInstance.createNewService =
                        this.propertyTableHelperService.createNewService(
                            request.id,
                            this.managementControllerServiceService,
                            this.reportingTaskService
                        );

                    editDialogReference.componentInstance.editReportingTask
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((updateReportingTaskRequest: UpdateReportingTaskRequest) => {
                            this.store.dispatch(
                                ReportingTaskActions.configureReportingTask({
                                    request: {
                                        id: request.reportingTask.id,
                                        uri: request.reportingTask.uri,
                                        payload: updateReportingTaskRequest.payload,
                                        postUpdateNavigation: updateReportingTaskRequest.postUpdateNavigation,
                                        postUpdateNavigationBoundary:
                                            updateReportingTaskRequest.postUpdateNavigationBoundary
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        this.store.dispatch(resetPropertyVerificationState());

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
                                postUpdateNavigation: request.postUpdateNavigation,
                                postUpdateNavigationBoundary: request.postUpdateNavigationBoundary
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ReportingTaskActions.reportingTasksBannerApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
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
                        if (response.postUpdateNavigationBoundary) {
                            this.router.navigate(response.postUpdateNavigation, {
                                state: {
                                    backNavigation: {
                                        route: ['/settings', 'reporting-tasks', response.id, 'edit'],
                                        routeBoundary: response.postUpdateNavigationBoundary,
                                        context: 'Reporting Task'
                                    } as BackNavigation
                                }
                            });
                        } else {
                            this.router.navigate(response.postUpdateNavigation);
                        }
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
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ReportingTaskActions.reportingTasksSnackbarApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
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
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ReportingTaskActions.reportingTasksSnackbarApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
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

    openChangeReportingTaskVersionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ReportingTaskActions.openChangeReportingTaskVersionDialog),
                map((action) => action.request),
                switchMap((request) =>
                    from(this.extensionTypesService.getReportingTaskVersionsForType(request.type, request.bundle)).pipe(
                        map(
                            (response) =>
                                ({
                                    fetchRequest: request,
                                    componentVersions: response.reportingTaskTypes
                                }) as OpenChangeComponentVersionDialogRequest
                        ),
                        tap({
                            error: (errorResponse: HttpErrorResponse) => {
                                this.store.dispatch(
                                    ErrorActions.snackBarError({
                                        error: this.errorHelper.getErrorString(errorResponse)
                                    })
                                );
                            }
                        })
                    )
                ),
                tap((request) => {
                    const dialogRequest = this.dialog.open(ChangeComponentVersionDialog, {
                        ...LARGE_DIALOG,
                        data: request,
                        autoFocus: false
                    });

                    dialogRequest.componentInstance.changeVersion.pipe(take(1)).subscribe((newVersion) => {
                        this.store.dispatch(
                            ReportingTaskActions.configureReportingTask({
                                request: {
                                    id: request.fetchRequest.id,
                                    uri: request.fetchRequest.uri,
                                    payload: {
                                        component: {
                                            bundle: newVersion.bundle,
                                            id: request.fetchRequest.id
                                        },
                                        revision: request.fetchRequest.revision
                                    }
                                }
                            })
                        );
                        dialogRequest.close();
                    });
                })
            ),
        { dispatch: false }
    );
}
