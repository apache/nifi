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
import * as RegistryClientsActions from './registry-clients.actions';
import { catchError, from, map, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { selectRegistryClientTypes } from '../../../../state/extension-types/extension-types.selectors';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { Router } from '@angular/router';
import { RegistryClientService } from '../../service/registry-client.service';
import { CreateRegistryClient } from '../../ui/registry-clients/create-registry-client/create-registry-client.component';
import { selectSaving } from './registry-clients.selectors';
import { EditRegistryClient } from '../../ui/registry-clients/edit-registry-client/edit-registry-client.component';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { Client } from '../../../../service/client.service';
import { EditRegistryClientRequest } from './index';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';

@Injectable()
export class RegistryClientsEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private client: Client,
        private registryClientService: RegistryClientService,
        private extensionTypesService: ExtensionTypesService,
        private managementControllerServiceService: ManagementControllerServiceService,
        private dialog: MatDialog,
        private router: Router,
        private propertyTableHelperService: PropertyTableHelperService
    ) {}

    loadRegistryClients$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RegistryClientsActions.loadRegistryClients),
            switchMap(() =>
                from(this.registryClientService.getRegistryClients()).pipe(
                    map((response) =>
                        RegistryClientsActions.loadRegistryClientsSuccess({
                            response: {
                                registryClients: response.registries,
                                loadedTimestamp: response.currentTime
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            RegistryClientsActions.registryClientsApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    openNewRegistryClientDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(RegistryClientsActions.openNewRegistryClientDialog),
                concatLatestFrom(() => this.store.select(selectRegistryClientTypes)),
                tap(([, registryClientTypes]) => {
                    const dialogReference = this.dialog.open(CreateRegistryClient, {
                        data: {
                            registryClientTypes
                        },
                        panelClass: 'medium-dialog'
                    });

                    dialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    dialogReference.componentInstance.createRegistryClient.pipe(take(1)).subscribe((request) => {
                        this.store.dispatch(
                            RegistryClientsActions.createRegistryClient({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    createRegistryClient$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RegistryClientsActions.createRegistryClient),
            map((action) => action.request),
            switchMap((request) =>
                from(this.registryClientService.createRegistryClient(request)).pipe(
                    map((response) =>
                        RegistryClientsActions.createRegistryClientSuccess({
                            response: {
                                registryClient: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            RegistryClientsActions.registryClientsApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    createRegistryClientSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RegistryClientsActions.createRegistryClientSuccess),
            map((action) => action.response),
            tap(() => {
                this.dialog.closeAll();
            }),
            switchMap((response) =>
                of(
                    RegistryClientsActions.selectClient({
                        request: {
                            id: response.registryClient.id
                        }
                    })
                )
            )
        )
    );

    navigateToEditRegistryClient$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(RegistryClientsActions.navigateToEditRegistryClient),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['/settings', 'registry-clients', id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    openConfigureControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(RegistryClientsActions.openConfigureRegistryClientDialog),
                map((action) => action.request),
                tap((request) => {
                    const registryClientId: string = request.registryClient.id;

                    const editDialogReference = this.dialog.open(EditRegistryClient, {
                        data: request,
                        id: registryClientId,
                        panelClass: 'large-dialog'
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.createNewProperty =
                        this.propertyTableHelperService.createNewProperty(registryClientId, this.registryClientService);

                    editDialogReference.componentInstance.goToService = (serviceId: string) => {
                        const commands: string[] = ['/settings', 'management-controller-services', serviceId];

                        if (editDialogReference.componentInstance.editRegistryClientForm.dirty) {
                            const saveChangesDialogReference = this.dialog.open(YesNoDialog, {
                                data: {
                                    title: 'Registry Client Configuration',
                                    message: `Save changes before going to this Controller Service?`
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

                    editDialogReference.componentInstance.createNewService =
                        this.propertyTableHelperService.createNewService(
                            registryClientId,
                            this.managementControllerServiceService,
                            this.registryClientService
                        );

                    editDialogReference.componentInstance.editRegistryClient
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((editRegistryClientRequest: EditRegistryClientRequest) => {
                            this.store.dispatch(
                                RegistryClientsActions.configureRegistryClient({
                                    request: editRegistryClientRequest
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                RegistryClientsActions.selectClient({
                                    request: {
                                        id: registryClientId
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    configureRegistryClient$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RegistryClientsActions.configureRegistryClient),
            map((action) => action.request),
            switchMap((request) =>
                from(this.registryClientService.updateRegistryClient(request)).pipe(
                    map((response) =>
                        RegistryClientsActions.configureRegistryClientSuccess({
                            response: {
                                id: request.id,
                                registryClient: response,
                                postUpdateNavigation: request.postUpdateNavigation
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            RegistryClientsActions.registryClientsApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    configureRegistryClientSuccess = createEffect(
        () =>
            this.actions$.pipe(
                ofType(RegistryClientsActions.configureRegistryClientSuccess),
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

    promptRegistryClientDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(RegistryClientsActions.promptRegistryClientDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        data: {
                            title: 'Delete Registry Client',
                            message: `Delete registry client ${request.registryClient.component.name}?`
                        },
                        panelClass: 'small-dialog'
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            RegistryClientsActions.deleteRegistryClient({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    deleteRegistryClient$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RegistryClientsActions.deleteRegistryClient),
            map((action) => action.request),
            switchMap((request) =>
                from(this.registryClientService.deleteRegistryClient(request)).pipe(
                    map((response) =>
                        RegistryClientsActions.deleteRegistryClientSuccess({
                            response: {
                                registryClient: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            RegistryClientsActions.registryClientsApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    selectRegistryClient$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(RegistryClientsActions.selectClient),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/settings', 'registry-clients', request.id]);
                })
            ),
        { dispatch: false }
    );
}
