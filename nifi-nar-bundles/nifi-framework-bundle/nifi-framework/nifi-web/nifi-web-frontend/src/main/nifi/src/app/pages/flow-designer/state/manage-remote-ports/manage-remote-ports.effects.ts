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
import * as ManageRemotePortsActions from './manage-remote-ports.actions';
import { catchError, combineLatest, from, map, of, switchMap, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { Router } from '@angular/router';
import { selectRpg, selectRpgIdFromRoute, selectSaving, selectStatus } from './manage-remote-ports.selectors';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { ManageRemotePortService } from '../../service/manage-remote-port.service';
import { PortSummary } from './index';
import { EditRemotePortComponent } from '../../ui/manage-remote-ports/edit-remote-port/edit-remote-port.component';
import { EditComponentDialogRequest } from '../flow';
import { ComponentType, isDefinedAndNotNull } from '../../../../state/shared';

@Injectable()
export class ManageRemotePortsEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private manageRemotePortService: ManageRemotePortService,
        private errorHelper: ErrorHelper,
        private dialog: MatDialog,
        private router: Router
    ) {}

    loadRemotePorts$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManageRemotePortsActions.loadRemotePorts),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectStatus)),
            switchMap(([request, status]) => {
                return combineLatest([this.manageRemotePortService.getRemotePorts(request.rpgId)]).pipe(
                    map(([response]) => {
                        const ports: PortSummary[] = [];

                        response.component.contents.inputPorts.forEach((inputPort: PortSummary) => {
                            const port = {
                                ...inputPort,
                                type: ComponentType.InputPort
                            } as PortSummary;

                            ports.push(port);
                        });

                        response.component.contents.outputPorts.forEach((outputPort: PortSummary) => {
                            const port = {
                                ...outputPort,
                                type: ComponentType.OutputPort
                            } as PortSummary;

                            ports.push(port);
                        });

                        return ManageRemotePortsActions.loadRemotePortsSuccess({
                            response: {
                                ports,
                                loadedTimestamp: response.loadedTimestamp,
                                rpg: response
                            }
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.errorHelper.handleLoadingError(status, errorResponse))
                    )
                );
            })
        )
    );

    navigateToEditPort$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManageRemotePortsActions.navigateToEditPort),
                map((action) => action.id),
                concatLatestFrom(() => this.store.select(selectRpgIdFromRoute)),
                tap(([id, rpgId]) => {
                    this.router.navigate(['/remote-process-group', rpgId, 'manage-remote-ports', id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    remotePortsBannerApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManageRemotePortsActions.remotePortsBannerApiError),
            map((action) => action.error),
            switchMap((error) => of(ErrorActions.addBannerError({ error })))
        )
    );

    toggleRemotePortTransmission$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManageRemotePortsActions.toggleRemotePortTransmission),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectStatus)),
            switchMap(([request, status]) => {
                return combineLatest([
                    this.manageRemotePortService.togglePortTransmission({
                        rpg: request.rpg,
                        port: request.port
                    })
                ]).pipe(
                    map(([response]) => {
                        return ManageRemotePortsActions.loadRemotePorts({
                            request: {
                                rpgId: response.remoteProcessGroupPort.groupId
                            }
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.errorHelper.handleLoadingError(status, errorResponse))
                    )
                );
            })
        )
    );

    selectRemotePort$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManageRemotePortsActions.selectRemotePort),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/remote-process-group', request.rpgId, 'manage-remote-ports', request.id]);
                })
            ),
        { dispatch: false }
    );

    openConfigureRemotePortDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManageRemotePortsActions.openConfigureRemotePortDialog),
                map((action) => action.request),
                concatLatestFrom(() => [this.store.select(selectRpg).pipe(isDefinedAndNotNull())]),
                tap(([request, rpg]) => {
                    const portId: string = request.id;

                    const editDialogReference = this.dialog.open(EditRemotePortComponent, {
                        data: {
                            type: request.port.type,
                            entity: request.port,
                            rpg
                        } as EditComponentDialogRequest,
                        id: portId
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                ManageRemotePortsActions.selectRemotePort({
                                    request: {
                                        rpgId: rpg.id,
                                        id: portId
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    configureRemotePort$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManageRemotePortsActions.configureRemotePort),
            map((action) => action.request),
            switchMap((request) =>
                from(this.manageRemotePortService.updateRemotePort(request)).pipe(
                    map((response) =>
                        ManageRemotePortsActions.configureRemotePortSuccess({
                            response: {
                                id: request.id,
                                port: response.remoteProcessGroupPort
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                            return of(
                                ManageRemotePortsActions.remotePortsBannerApiError({
                                    error: errorResponse.error
                                })
                            );
                        } else {
                            this.dialog.getDialogById(request.id)?.close('ROUTED');
                            return of(this.errorHelper.fullScreenError(errorResponse));
                        }
                    })
                )
            )
        )
    );

    configureRemotePortSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManageRemotePortsActions.configureRemotePortSuccess),
                map((action) => action.response),
                tap(() => {
                    this.dialog.closeAll();
                })
            ),
        { dispatch: false }
    );
}
