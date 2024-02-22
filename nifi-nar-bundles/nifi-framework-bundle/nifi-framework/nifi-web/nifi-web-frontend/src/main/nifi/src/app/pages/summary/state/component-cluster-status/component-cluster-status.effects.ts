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
import { Actions, concatLatestFrom, createEffect, ofType } from '@ngrx/effects';
import { NiFiState } from '../../../../state';
import { Store } from '@ngrx/store';
import { ErrorHelper } from '../../../../service/error-helper.service';
import * as ClusterStatusActions from './component-cluster-status.actions';
import { catchError, from, map, of, switchMap, tap } from 'rxjs';
import { ComponentClusterStatusService } from '../../service/component-cluster-status.service';
import { MatDialog } from '@angular/material/dialog';
import { ClusterSummaryDialog } from '../../ui/common/cluster-summary-dialog/cluster-summary-dialog.component';
import { selectComponentClusterStatusLatestRequest } from './component-cluster-status.selectors';
import { isDefinedAndNotNull } from '../../../../state/shared';

@Injectable()
export class ComponentClusterStatusEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private errorHelper: ErrorHelper,
        private clusterStatusService: ComponentClusterStatusService,
        private dialog: MatDialog
    ) {}

    loadComponentClusterStatusAndOpenDialog$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ClusterStatusActions.loadComponentClusterStatusAndOpenDialog),
            map((action) => action.request),
            switchMap((request) =>
                from(this.clusterStatusService.getClusterStatus(request.id, request.componentType)).pipe(
                    map((response) => {
                        return ClusterStatusActions.openComponentClusterStatusDialog({
                            response: {
                                clusterStatusEntity: response,
                                componentType: request.componentType
                            }
                        });
                    }),
                    catchError((error) => of(this.errorHelper.handleLoadingError(error.error, error)))
                )
            )
        )
    );

    loadComponentClusterStatus$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ClusterStatusActions.loadComponentClusterStatus),
            map((action) => action.request),
            switchMap((request) =>
                from(this.clusterStatusService.getClusterStatus(request.id, request.componentType)).pipe(
                    map((response) => {
                        return ClusterStatusActions.loadComponentClusterStatusSuccess({
                            response: {
                                clusterStatusEntity: response,
                                componentType: request.componentType
                            }
                        });
                    }),
                    catchError((error) => of(this.errorHelper.handleLoadingError(error.error, error)))
                )
            )
        )
    );

    openComponentClusterStatusDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ClusterStatusActions.openComponentClusterStatusDialog),
                map((action) => action.response),
                concatLatestFrom(() =>
                    this.store.select(selectComponentClusterStatusLatestRequest).pipe(isDefinedAndNotNull())
                ),
                tap(([response, request]) => {
                    const dialogRef = this.dialog.open(ClusterSummaryDialog, {
                        panelClass: 'xl-dialog',
                        data: request
                    });
                    dialogRef.componentInstance.componentType = response.componentType;
                    dialogRef.afterClosed().subscribe(() => {
                        this.store.dispatch(ClusterStatusActions.resetComponentClusterStatusState());
                    });
                })
            ),
        { dispatch: false }
    );
}
