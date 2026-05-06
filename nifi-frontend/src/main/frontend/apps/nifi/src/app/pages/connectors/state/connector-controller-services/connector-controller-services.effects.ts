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
import { Router } from '@angular/router';
import { MatDialog } from '@angular/material/dialog';
import { combineLatest, of } from 'rxjs';
import { catchError, map, switchMap, tap } from 'rxjs/operators';
import { XL_DIALOG } from '@nifi/shared';
import { ConnectorService } from '../../service/connector.service';
import { ErrorContextKey } from '../../../../state/error';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { EditControllerService } from '../../../../ui/common/controller-service/edit-controller-service/edit-controller-service.component';
import { EditControllerServiceDialogRequest } from '../../../../state/shared';
import * as ConnectorControllerServicesActions from './connector-controller-services.actions';

@Injectable()
export class ConnectorControllerServicesEffects {
    private actions$ = inject(Actions);
    private router = inject(Router);
    private dialog = inject(MatDialog);
    private connectorService = inject(ConnectorService);
    private errorHelper = inject(ErrorHelper);

    /**
     * Load controller services for a connector's process group. Combines the
     * controller services response with the flow response to capture the
     * breadcrumb that drives navigation context.
     */
    loadConnectorControllerServices$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorControllerServicesActions.loadConnectorControllerServices),
            map((action) => action.request),
            switchMap((request) =>
                combineLatest([
                    this.connectorService.getConnectorControllerServices(request.connectorId, request.processGroupId),
                    this.connectorService.getConnectorFlow(request.connectorId, request.processGroupId)
                ]).pipe(
                    map(([controllerServicesResponse, flowResponse]) =>
                        ConnectorControllerServicesActions.loadConnectorControllerServicesSuccess({
                            response: {
                                connectorId: request.connectorId,
                                processGroupId: request.processGroupId,
                                controllerServices: controllerServicesResponse.controllerServices ?? [],
                                breadcrumb: flowResponse.processGroupFlow?.breadcrumb ?? null,
                                loadedTimestamp: controllerServicesResponse.currentTime ?? new Date().toISOString()
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ConnectorControllerServicesActions.loadConnectorControllerServicesFailure({
                                errorContext: {
                                    errors: [this.errorHelper.getErrorString(error)],
                                    context: ErrorContextKey.CONTROLLER_SERVICES
                                }
                            })
                        )
                    )
                )
            )
        )
    );

    /**
     * Surface load failures via a banner above the controller services view.
     */
    loadConnectorControllerServicesFailure$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorControllerServicesActions.loadConnectorControllerServicesFailure),
            map((action) => ErrorActions.addBannerError({ errorContext: action.errorContext }))
        )
    );

    /**
     * Selecting a controller service updates the route so the selection can be
     * deep linked. Uses replaceUrl so back navigation is not polluted with each
     * intermediate selection.
     */
    selectConnectorControllerService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.selectConnectorControllerService),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(
                        [
                            '/connectors',
                            request.connectorId,
                            'canvas',
                            request.processGroupId,
                            'controller-services',
                            request.serviceId
                        ],
                        { replaceUrl: true }
                    );
                })
            ),
        { dispatch: false }
    );

    /**
     * Opens the EditControllerService dialog in read-only mode. The dialog
     * honors the readonly flag on the request and forces a strictly read-only
     * view regardless of the underlying entity's permissions or run status.
     */
    openViewControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorControllerServicesActions.openViewControllerServiceDialog),
                tap((action) => {
                    const dialogRequest: EditControllerServiceDialogRequest = {
                        id: action.controllerService.id,
                        controllerService: action.controllerService,
                        readonly: true
                    };

                    this.dialog.open(EditControllerService, {
                        ...XL_DIALOG,
                        autoFocus: 'dialog',
                        data: dialogRequest,
                        id: action.controllerService.id
                    });
                })
            ),
        { dispatch: false }
    );
}
