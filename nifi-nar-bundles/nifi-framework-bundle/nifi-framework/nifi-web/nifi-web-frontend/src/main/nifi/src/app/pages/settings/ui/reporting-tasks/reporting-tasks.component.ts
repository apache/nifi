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

import { Component, OnDestroy, OnInit } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Store } from '@ngrx/store';
import { filter, switchMap, take } from 'rxjs';
import { ReportingTaskEntity, ReportingTasksState } from '../../state/reporting-tasks';
import {
    selectReportingTaskIdFromRoute,
    selectReportingTasksState,
    selectSingleEditedReportingTask,
    selectTask
} from '../../state/reporting-tasks/reporting-tasks.selectors';
import {
    loadReportingTasks,
    navigateToEditReportingTask,
    openConfigureReportingTaskDialog,
    openNewReportingTaskDialog,
    promptReportingTaskDeletion,
    resetReportingTasksState,
    startReportingTask,
    stopReportingTask,
    selectReportingTask
} from '../../state/reporting-tasks/reporting-tasks.actions';
import { initialState } from '../../state/reporting-tasks/reporting-tasks.reducer';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { NiFiState } from '../../../../state';

@Component({
    selector: 'reporting-tasks',
    templateUrl: './reporting-tasks.component.html',
    styleUrls: ['./reporting-tasks.component.scss']
})
export class ReportingTasks implements OnInit, OnDestroy {
    reportingTaskState$ = this.store.select(selectReportingTasksState);
    selectedReportingTaskId$ = this.store.select(selectReportingTaskIdFromRoute);
    currentUser$ = this.store.select(selectCurrentUser);

    constructor(private store: Store<NiFiState>) {
        this.store
            .select(selectSingleEditedReportingTask)
            .pipe(
                filter((id: string) => id != null),
                switchMap((id: string) =>
                    this.store.select(selectTask(id)).pipe(
                        filter((entity) => entity != null),
                        take(1)
                    )
                ),
                takeUntilDestroyed()
            )
            .subscribe((entity) => {
                if (entity) {
                    this.store.dispatch(
                        openConfigureReportingTaskDialog({
                            request: {
                                id: entity.id,
                                reportingTask: entity
                            }
                        })
                    );
                }
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadReportingTasks());
    }

    isInitialLoading(state: ReportingTasksState): boolean {
        // using the current timestamp to detect the initial load event
        return state.loadedTimestamp == initialState.loadedTimestamp;
    }

    openNewReportingTaskDialog(): void {
        this.store.dispatch(openNewReportingTaskDialog());
    }

    refreshReportingTaskListing(): void {
        this.store.dispatch(loadReportingTasks());
    }

    selectReportingTask(entity: ReportingTaskEntity): void {
        this.store.dispatch(
            selectReportingTask({
                request: {
                    id: entity.id
                }
            })
        );
    }

    deleteReportingTask(entity: ReportingTaskEntity): void {
        this.store.dispatch(
            promptReportingTaskDeletion({
                request: {
                    reportingTask: entity
                }
            })
        );
    }

    configureReportingTask(entity: ReportingTaskEntity): void {
        this.store.dispatch(
            navigateToEditReportingTask({
                id: entity.id
            })
        );
    }

    startReportingTask(entity: ReportingTaskEntity): void {
        this.store.dispatch(
            startReportingTask({
                request: {
                    reportingTask: entity
                }
            })
        );
    }

    stopReportingTask(entity: ReportingTaskEntity): void {
        this.store.dispatch(
            stopReportingTask({
                request: {
                    reportingTask: entity
                }
            })
        );
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetReportingTasksState());
    }
}
