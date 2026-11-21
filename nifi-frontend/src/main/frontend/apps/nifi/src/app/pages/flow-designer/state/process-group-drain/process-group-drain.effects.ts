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

import { Injectable, inject } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import { Action, Store } from '@ngrx/store';
import { HttpErrorResponse } from '@angular/common/http';
import { MatDialog, MatDialogRef } from '@angular/material/dialog';
import { NavigationStart, Router } from '@angular/router';
import {
    asyncScheduler,
    catchError,
    exhaustMap,
    filter,
    from,
    interval,
    map,
    of,
    switchMap,
    take,
    takeUntil,
    tap
} from 'rxjs';
import {
    ComponentType,
    DrainBlockingComponentType,
    isDefinedAndNotNull,
    MEDIUM_DIALOG,
    SMALL_DIALOG,
    YesNoDialog
} from '@nifi/shared';
import * as ProcessGroupDrainActions from './process-group-drain.actions';
import * as FlowActions from '../flow/flow.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import {
    selectDrainDialogState,
    selectDrainProcessGroupId,
    selectDrainRequestId
} from './process-group-drain.selectors';
import { selectCurrentProcessGroupId } from '../flow/flow.selectors';
import { ProcessGroupDrainService } from '../../../../service/process-group-drain.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { DrainProcessGroupDialog } from '../../../../ui/common/drain-process-group-dialog/drain-process-group-dialog';
import { NiFiState } from '../../../../state';

const COMPONENT_TYPES: Record<DrainBlockingComponentType, ComponentType> = {
    PROCESSOR: ComponentType.Processor,
    INPUT_PORT: ComponentType.InputPort,
    OUTPUT_PORT: ComponentType.OutputPort,
    REMOTE_INPUT_PORT: ComponentType.RemoteProcessGroup
};

const POLLING_STOP_ACTIONS = [
    ProcessGroupDrainActions.drainEnded,
    ProcessGroupDrainActions.cancelDrain,
    ProcessGroupDrainActions.resetDrainState,
    ProcessGroupDrainActions.drainApiError
] as const;

@Injectable()
export class ProcessGroupDrainEffects {
    private actions$ = inject(Actions);
    private store = inject<Store<NiFiState>>(Store);
    private processGroupDrainService = inject(ProcessGroupDrainService);
    private errorHelper = inject(ErrorHelper);
    private dialog = inject(MatDialog);
    private router = inject(Router);

    private drainDialogRef: MatDialogRef<DrainProcessGroupDialog> | undefined;
    private lastPrepared = false;

    promptDrain$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ProcessGroupDrainActions.promptDrain),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Drain Process Group',
                            message:
                                'Drain stops the components that bring data into this Process Group, enables the Controller Services its components reference, and starts the other components so the queued data can flow out. The drain ends when the group is empty or when you cancel it. Components keep their run state afterwards, so sources remain stopped. Are you sure you want to drain this Process Group?'
                        }
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(ProcessGroupDrainActions.beginDrain({ request }));
                    });
                })
            ),
        { dispatch: false }
    );

    beginDrain$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.beginDrain),
            map((action) => action.request),
            switchMap((request) =>
                this.processGroupDrainService.beginDrain(request.processGroupId).pipe(
                    map((response) =>
                        ProcessGroupDrainActions.beginDrainSuccess({
                            response: { drainRequestEntity: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => this.handleDrainApiError(errorResponse))
                )
            )
        )
    );

    beginDrainSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.beginDrainSuccess),
            concatLatestFrom(() => [
                this.store.select(selectDrainProcessGroupId),
                this.store.select(selectCurrentProcessGroupId)
            ]),
            switchMap(([action, processGroupId, currentProcessGroupId]) => {
                this.lastPrepared = action.response.drainRequestEntity.drainRequest?.prepared === true;
                const drainRequest = action.response.drainRequestEntity.drainRequest;
                // drainEnded$ performs the reload when the drain has already ended, so the group is reloaded once
                if (!drainRequest || drainRequest.state !== 'DRAINING') {
                    return of(ProcessGroupDrainActions.drainEnded());
                }
                return from([
                    this.reloadProcessGroupAction(processGroupId, currentProcessGroupId),
                    ProcessGroupDrainActions.startPollingDrain()
                ]);
            })
        )
    );

    openDrainDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ProcessGroupDrainActions.beginDrainSuccess, ProcessGroupDrainActions.viewDrain),
                tap(() => {
                    if (this.drainDialogRef) {
                        return;
                    }

                    const dialogReference = this.dialog.open(DrainProcessGroupDialog, {
                        ...MEDIUM_DIALOG,
                        disableClose: true,
                        autoFocus: false
                    });
                    this.drainDialogRef = dialogReference;

                    dialogReference.componentInstance.drainState$ = this.store.select(selectDrainDialogState);

                    dialogReference.componentInstance.cancelDrain
                        .pipe(takeUntil(dialogReference.afterClosed()))
                        .subscribe(() => {
                            this.store.dispatch(ProcessGroupDrainActions.cancelDrain());
                        });

                    dialogReference.componentInstance.goToComponent
                        .pipe(takeUntil(dialogReference.afterClosed()))
                        .subscribe((component) => {
                            this.store.dispatch(ProcessGroupDrainActions.goToBlockingComponent({ component }));
                        });

                    dialogReference.afterClosed().subscribe(() => {
                        this.drainDialogRef = undefined;
                        this.lastPrepared = false;
                        this.store.dispatch(ProcessGroupDrainActions.resetDrainState());
                    });
                })
            ),
        { dispatch: false }
    );

    viewDrain$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.viewDrain),
            map(() => ProcessGroupDrainActions.startPollingDrain())
        )
    );

    // viewDrain reaches this effect through viewDrain$, which dispatches startPollingDrain. Listening
    // for both would still be a single interval because switchMap replaces the previous one, but it
    // would restart the timer on every View.
    startPollingDrain$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.startPollingDrain),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(takeUntil(this.actions$.pipe(ofType(...POLLING_STOP_ACTIONS))))
            ),
            map(() => ProcessGroupDrainActions.pollDrain())
        )
    );

    pollDrain$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.pollDrain, ProcessGroupDrainActions.viewDrain),
            concatLatestFrom(() => [
                this.store.select(selectDrainProcessGroupId).pipe(isDefinedAndNotNull()),
                this.store.select(selectDrainRequestId).pipe(isDefinedAndNotNull())
            ]),
            exhaustMap(([, processGroupId, drainRequestId]) =>
                this.processGroupDrainService.getDrain(processGroupId, drainRequestId).pipe(
                    map((response) =>
                        ProcessGroupDrainActions.pollDrainSuccess({
                            response: { drainRequestEntity: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => this.handleDrainApiError(errorResponse)),
                    takeUntil(this.actions$.pipe(ofType(...POLLING_STOP_ACTIONS)))
                )
            )
        )
    );

    pollDrainSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.pollDrainSuccess),
            concatLatestFrom(() => [
                this.store.select(selectDrainProcessGroupId),
                this.store.select(selectCurrentProcessGroupId)
            ]),
            switchMap(([action, processGroupId, currentProcessGroupId]) => {
                const drainRequest = action.response.drainRequestEntity.drainRequest;
                const actions: Action[] = [];
                const isDraining = drainRequest?.state === 'DRAINING';
                const prepared = drainRequest?.prepared === true;
                // An ended drain is reloaded by drainEnded$, so the prepared flip only reloads while draining
                if (isDraining && prepared && !this.lastPrepared) {
                    actions.push(this.reloadProcessGroupAction(processGroupId, currentProcessGroupId));
                }
                this.lastPrepared = prepared;
                if (!isDraining) {
                    actions.push(ProcessGroupDrainActions.drainEnded());
                }
                return from(actions);
            })
        )
    );

    drainEnded$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.drainEnded, ProcessGroupDrainActions.cancelDrainSuccess),
            concatLatestFrom(() => [
                this.store.select(selectDrainProcessGroupId),
                this.store.select(selectCurrentProcessGroupId)
            ]),
            map(([, processGroupId, currentProcessGroupId]) =>
                this.reloadProcessGroupAction(processGroupId, currentProcessGroupId)
            )
        )
    );

    cancelDrain$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.cancelDrain),
            concatLatestFrom(() => [
                this.store.select(selectDrainProcessGroupId).pipe(isDefinedAndNotNull()),
                this.store.select(selectDrainRequestId).pipe(isDefinedAndNotNull())
            ]),
            switchMap(([, processGroupId, drainRequestId]) =>
                this.processGroupDrainService.endDrain(processGroupId, drainRequestId).pipe(
                    map((response) =>
                        ProcessGroupDrainActions.cancelDrainSuccess({
                            response: { drainRequestEntity: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => this.handleDrainApiError(errorResponse)),
                    takeUntil(this.actions$.pipe(ofType(ProcessGroupDrainActions.resetDrainState)))
                )
            )
        )
    );

    goToBlockingComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.goToBlockingComponent),
            map((action) => action.component),
            tap(() => {
                this.drainDialogRef?.close();
            }),
            map((component) =>
                FlowActions.navigateToComponent({
                    request: {
                        id: component.remoteProcessGroupId ?? component.id,
                        type: COMPONENT_TYPES[component.type],
                        processGroupId: component.groupId
                    }
                })
            )
        )
    );

    drainApiError$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ProcessGroupDrainActions.drainApiError),
                tap(() => {
                    this.lastPrepared = false;
                    this.drainDialogRef?.close();
                })
            ),
        { dispatch: false }
    );

    closeOnNavigation$ = createEffect(
        () =>
            this.router.events.pipe(
                filter((event): event is NavigationStart => event instanceof NavigationStart),
                tap(() => {
                    this.drainDialogRef?.close();
                })
            ),
        { dispatch: false }
    );

    private handleDrainApiError(errorResponse: HttpErrorResponse) {
        return from([
            ProcessGroupDrainActions.drainApiError({ error: this.errorHelper.getErrorString(errorResponse) }),
            this.snackBarOrFullScreenError(errorResponse)
        ]);
    }

    private snackBarOrFullScreenError(errorResponse: HttpErrorResponse) {
        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
            return ErrorActions.snackBarError({ error: this.errorHelper.getErrorString(errorResponse) });
        }
        return this.errorHelper.fullScreenError(errorResponse);
    }

    private reloadProcessGroupAction(processGroupId: string | null, currentProcessGroupId: string | null): Action {
        if (processGroupId && processGroupId !== currentProcessGroupId) {
            return FlowActions.loadChildProcessGroup({
                request: {
                    id: processGroupId
                }
            });
        }

        return FlowActions.reloadFlow();
    }
}
