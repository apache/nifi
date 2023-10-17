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
import * as ManagementControllerServicesActions from './management-controller-services.actions';
import { catchError, from, map, NEVER, Observable, of, switchMap, take, tap, withLatestFrom } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../state';
import { selectControllerServiceTypes } from '../../../state/extension-types/extension-types.selectors';
import { CreateControllerService } from '../../../ui/common/controller-service/create-controller-service/create-controller-service.component';
import { Client } from '../../../service/client.service';
import { YesNoDialog } from '../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { EditControllerService } from '../../../ui/common/controller-service/edit-controller-service/edit-controller-service.component';
import { NewPropertyDialogRequest, NewPropertyDialogResponse, Property } from '../../../state/shared';
import { NewPropertyDialog } from '../../../ui/common/new-property-dialog/new-property-dialog.component';

@Injectable()
export class ManagementControllerServicesEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private client: Client,
        private managementControllerServiceService: ManagementControllerServiceService,
        private dialog: MatDialog
    ) {}

    loadControllerConfig$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.loadManagementControllerServices),
            switchMap(() =>
                from(this.managementControllerServiceService.getControllerServices()).pipe(
                    map((response) =>
                        ManagementControllerServicesActions.loadManagementControllerServicesSuccess({
                            response: {
                                controllerServices: response.controllerServices,
                                loadedTimestamp: response.currentTime
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ManagementControllerServicesActions.managementControllerServicesApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    openNewControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.openNewControllerServiceDialog),
                withLatestFrom(this.store.select(selectControllerServiceTypes)),
                tap(([action, controllerServiceTypes]) => {
                    const dialogReference = this.dialog.open(CreateControllerService, {
                        data: {
                            controllerServiceTypes
                        },
                        panelClass: 'medium-dialog'
                    });

                    dialogReference.componentInstance.createControllerService
                        .pipe(take(1))
                        .subscribe((controllerServiceType) => {
                            this.store.dispatch(
                                ManagementControllerServicesActions.createControllerService({
                                    request: {
                                        revision: {
                                            clientId: this.client.getClientId(),
                                            version: 0
                                        },
                                        controllerServiceType: controllerServiceType.type,
                                        controllerServiceBundle: controllerServiceType.bundle
                                    }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    createControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.createControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.managementControllerServiceService.createControllerService(request)).pipe(
                    map((response) =>
                        ManagementControllerServicesActions.createControllerServiceSuccess({
                            response: {
                                controllerService: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ManagementControllerServicesActions.managementControllerServicesApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    createControllerServiceSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.createControllerServiceSuccess),
                tap(() => {
                    this.dialog.closeAll();
                })
            ),
        { dispatch: false }
    );

    openConfigureControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.openConfigureControllerServiceDialog),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(EditControllerService, {
                        data: {
                            controllerService: request.controllerService
                        },
                        panelClass: 'large-dialog'
                    });

                    dialogReference.componentInstance.createNewProperty = (
                        allowsSensitive: boolean
                    ): Observable<Property> => {
                        const dialogRequest: NewPropertyDialogRequest = { allowsSensitive };
                        return this.dialog
                            .open(NewPropertyDialog, {
                                data: { dialogRequest },
                                panelClass: 'small-dialog'
                            })
                            .afterClosed()
                            .pipe(
                                take(1),
                                switchMap((dialogResponse: NewPropertyDialogResponse) => {
                                    if (dialogResponse) {
                                        return this.managementControllerServiceService
                                            .getPropertyDescriptor(
                                                request.id,
                                                dialogResponse.name,
                                                dialogResponse.senstive
                                            )
                                            .pipe(
                                                take(1),
                                                map((response) => {
                                                    return {
                                                        property: dialogResponse.name,
                                                        value: null,
                                                        descriptor: response.propertyDescriptor
                                                    };
                                                })
                                            );
                                    } else {
                                        return NEVER;
                                    }
                                })
                            );
                    };

                    dialogReference.componentInstance.editControllerService.pipe(take(1)).subscribe((payload: any) => {
                        this.store.dispatch(
                            ManagementControllerServicesActions.configureControllerService({
                                request: {
                                    id: request.controllerService.id,
                                    uri: request.controllerService.uri,
                                    payload
                                }
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    configureControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.configureControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.managementControllerServiceService.updateControllerService(request)).pipe(
                    map((response) =>
                        ManagementControllerServicesActions.configureControllerServiceSuccess({
                            response: {
                                id: request.id,
                                controllerService: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ManagementControllerServicesActions.managementControllerServicesApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    configureControllerServiceSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.configureControllerServiceSuccess),
                tap(() => {
                    this.dialog.closeAll();
                })
            ),
        { dispatch: false }
    );

    promptControllerServiceDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.promptControllerServiceDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        data: {
                            title: 'Delete Controller Service',
                            message: `Delete controller service ${request.controllerService.component.name}?`
                        },
                        panelClass: 'small-dialog'
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            ManagementControllerServicesActions.deleteControllerService({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    deleteControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.deleteControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.managementControllerServiceService.deleteControllerService(request)).pipe(
                    map((response) =>
                        ManagementControllerServicesActions.deleteControllerServiceSuccess({
                            response: {
                                controllerService: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ManagementControllerServicesActions.managementControllerServicesApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );
}
