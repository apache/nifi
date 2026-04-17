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
import { Store } from '@ngrx/store';
import { MatDialog } from '@angular/material/dialog';
import { catchError, filter, map, of, switchMap, take, tap } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { SMALL_DIALOG, YesNoDialog } from '@nifi/shared';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorContextKey } from '../../../../state/error';
import * as ConnectorCanvasActions from '../connector-canvas/connector-canvas.actions';
import { selectConnectorIdFromRoute } from '../connector-canvas/connector-canvas.selectors';
import { selectConnectorCanvasEntity } from './connector-canvas-entity.selectors';
import {
    loadConnectorEntity,
    loadConnectorEntitySuccess,
    loadConnectorEntityFailure,
    promptDrainConnector,
    drainConnector,
    drainConnectorSuccess,
    cancelConnectorDrain,
    cancelConnectorDrainSuccess,
    startConnector,
    startConnectorSuccess,
    stopConnector,
    stopConnectorSuccess,
    connectorActionApiError
} from './connector-canvas-entity.actions';

@Injectable()
export class ConnectorCanvasEntityEffects {
    private actions$ = inject(Actions);
    private store = inject(Store);
    private connectorService = inject(ConnectorService);
    private errorHelper = inject(ErrorHelper);
    private dialog = inject(MatDialog);

    triggerEntityLoad$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.loadConnectorFlow),
            concatLatestFrom(() => this.store.select(selectConnectorCanvasEntity)),
            filter(([action, existingEntity]) => existingEntity == null || existingEntity.id !== action.connectorId),
            map(([action]) => loadConnectorEntity({ connectorId: action.connectorId }))
        )
    );

    loadConnectorEntity$ = createEffect(() =>
        this.actions$.pipe(
            ofType(loadConnectorEntity),
            switchMap((action) =>
                this.connectorService.getConnector(action.connectorId).pipe(
                    map((connectorEntity) => loadConnectorEntitySuccess({ connectorEntity })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(loadConnectorEntityFailure({ error: this.errorHelper.getErrorString(errorResponse) }))
                    )
                )
            )
        )
    );

    /**
     * Prompt user to confirm draining FlowFiles.
     */
    promptDrainConnector$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(promptDrainConnector),
                tap((action) => {
                    const dialogRef = this.dialog.open<YesNoDialog>(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Drain Connector',
                            message: `Are you sure you want to drain connector '${action.connector.component.name}'? This will process existing FlowFiles but prevent new data from being ingested.`
                        }
                    });

                    dialogRef.componentInstance!.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(drainConnector({ connector: action.connector }));
                    });
                })
            ),
        { dispatch: false }
    );

    /**
     * Drain connector API call.
     */
    drainConnector$ = createEffect(() =>
        this.actions$.pipe(
            ofType(drainConnector),
            switchMap((action) =>
                this.connectorService.drainConnector(action.connector).pipe(
                    map((response) => drainConnectorSuccess({ connector: response })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(connectorActionApiError({ error: this.errorHelper.getErrorString(errorResponse) }))
                    )
                )
            )
        )
    );

    /**
     * Cancel drain API call (no confirmation needed).
     */
    cancelConnectorDrain$ = createEffect(() =>
        this.actions$.pipe(
            ofType(cancelConnectorDrain),
            switchMap((action) =>
                this.connectorService.cancelConnectorDrain(action.connector).pipe(
                    map((response) => cancelConnectorDrainSuccess({ connector: response })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(connectorActionApiError({ error: this.errorHelper.getErrorString(errorResponse) }))
                    )
                )
            )
        )
    );

    /**
     * Start connector API call (transition to RUNNING).
     */
    startConnector$ = createEffect(() =>
        this.actions$.pipe(
            ofType(startConnector),
            switchMap((action) =>
                this.connectorService.updateConnectorRunStatus(action.connector, 'RUNNING').pipe(
                    map((response) => startConnectorSuccess({ connector: response })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(connectorActionApiError({ error: this.errorHelper.getErrorString(errorResponse) }))
                    )
                )
            )
        )
    );

    /**
     * Stop connector API call (transition to STOPPED).
     */
    stopConnector$ = createEffect(() =>
        this.actions$.pipe(
            ofType(stopConnector),
            switchMap((action) =>
                this.connectorService.updateConnectorRunStatus(action.connector, 'STOPPED').pipe(
                    map((response) => stopConnectorSuccess({ connector: response })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(connectorActionApiError({ error: this.errorHelper.getErrorString(errorResponse) }))
                    )
                )
            )
        )
    );

    /**
     * On any successful connector action, reload the connector entity to ensure
     * availableActions and state are fully up to date.
     */
    refreshAfterAction$ = createEffect(() =>
        this.actions$.pipe(
            ofType(drainConnectorSuccess, cancelConnectorDrainSuccess, startConnectorSuccess, stopConnectorSuccess),
            concatLatestFrom(() => this.store.select(selectConnectorIdFromRoute)),
            filter(([, connectorId]) => connectorId != null),
            map(([, connectorId]) => loadConnectorEntity({ connectorId: connectorId! }))
        )
    );

    /**
     * Handle connector action API errors by dispatching to the banner error system.
     */
    connectorActionApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(connectorActionApiError),
            map((action) => action.error),
            switchMap((error) =>
                of(
                    ErrorActions.addBannerError({
                        errorContext: { errors: [error], context: ErrorContextKey.CONNECTOR_CANVAS }
                    })
                )
            )
        )
    );

    /**
     * When a connector action is initiated, clear any lingering canvas banner errors so
     * a retry does not stack on top of a previous failure message.
     */
    clearCanvasErrorsOnAction$ = createEffect(() =>
        this.actions$.pipe(
            ofType(drainConnector, cancelConnectorDrain, startConnector, stopConnector),
            map(() => ErrorActions.clearBannerErrors({ context: ErrorContextKey.CONNECTOR_CANVAS }))
        )
    );
}
