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
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { Client } from '../../../../service/client.service';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { ParameterProviderService } from '../../service/parameter-provider.service';
import * as ParameterProviderActions from './parameter-providers.actions';
import { loadParameterProviders, selectParameterProvider } from './parameter-providers.actions';
import { catchError, from, map, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { selectSaving } from './parameter-providers.selectors';
import { selectParameterProviderTypes } from '../../../../state/extension-types/extension-types.selectors';
import { CreateParameterProvider } from '../../ui/parameter-providers/create-parameter-provider/create-parameter-provider.component';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { EditParameterProvider } from '../../ui/parameter-providers/edit-parameter-provider/edit-parameter-provider.component';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { UpdateParameterProviderRequest } from './index';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';

@Injectable()
export class ParameterProvidersEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private client: Client,
        private dialog: MatDialog,
        private router: Router,
        private parameterProviderService: ParameterProviderService,
        private propertyTableHelperService: PropertyTableHelperService,
        private managementControllerServiceService: ManagementControllerServiceService
    ) {}

    loadParameterProviders$ = createEffect(() =>
        this.actions$.pipe(
            ofType(loadParameterProviders),
            switchMap(() =>
                from(this.parameterProviderService.getParameterProviders()).pipe(
                    map((response) =>
                        ParameterProviderActions.loadParameterProvidersSuccess({
                            response: {
                                parameterProviders: response.parameterProviders,
                                loadedTimestamp: response.currentTime
                            }
                        })
                    ),
                    catchError((error) =>
                        of(ParameterProviderActions.parameterProvidersApiError({ error: error.error }))
                    )
                )
            )
        )
    );

    selectParameterProvider$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(selectParameterProvider),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/settings', 'parameter-providers', request.id]);
                })
            ),
        { dispatch: false }
    );

    openNewParameterProviderDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.openNewParameterProviderDialog),
                concatLatestFrom(() => this.store.select(selectParameterProviderTypes)),
                tap(([action, parameterProviderTypes]) => {
                    const dialogReference = this.dialog.open(CreateParameterProvider, {
                        data: {
                            parameterProviderTypes
                        },
                        panelClass: 'medium-dialog'
                    });

                    dialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    dialogReference.componentInstance.createParameterProvider
                        .pipe(take(1))
                        .subscribe((parameterProviderType) => {
                            this.store.dispatch(
                                ParameterProviderActions.createParameterProvider({
                                    request: {
                                        parameterProviderType: parameterProviderType.type,
                                        parameterProviderBundle: parameterProviderType.bundle,
                                        revision: {
                                            clientId: this.client.getClientId(),
                                            version: 0
                                        }
                                    }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    createParameterProvider$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.createParameterProvider),
            map((action) => action.request),
            switchMap((request) =>
                from(this.parameterProviderService.createParameterProvider(request)).pipe(
                    map((response: any) =>
                        ParameterProviderActions.createParameterProviderSuccess({
                            response: {
                                parameterProvider: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(ParameterProviderActions.parameterProvidersApiError({ error: error.error }))
                    )
                )
            )
        )
    );

    createParameterProviderSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.createParameterProviderSuccess),
            map((action) => action.response),
            tap(() => {
                this.dialog.closeAll();
            }),
            switchMap((response) =>
                of(
                    ParameterProviderActions.selectParameterProvider({
                        request: {
                            id: response.parameterProvider.id
                        }
                    })
                )
            )
        )
    );

    promptParameterProviderDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.promptParameterProviderDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        data: {
                            title: 'Delete Parameter Provider',
                            message: `Delete parameter provider ${request.parameterProvider.component.name}?`
                        },
                        panelClass: 'small-dialog'
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() =>
                        this.store.dispatch(
                            ParameterProviderActions.deleteParameterProvider({
                                request
                            })
                        )
                    );
                })
            ),
        { dispatch: false }
    );

    deleteParameterProvider = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.deleteParameterProvider),
            map((action) => action.request),
            switchMap((request) =>
                from(this.parameterProviderService.deleteParameterProvider(request)).pipe(
                    map((response: any) =>
                        ParameterProviderActions.deleteParameterProviderSuccess({
                            response: {
                                parameterProvider: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ParameterProviderActions.parameterProvidersApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    navigateToEditParameterProvider$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.navigateToEditParameterProvider),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['settings', 'parameter-providers', id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    openConfigureParameterProviderDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.openConfigureParameterProviderDialog),
                map((action) => action.request),
                tap((request) => {
                    const id = request.id;
                    const editDialogReference = this.dialog.open(EditParameterProvider, {
                        data: {
                            parameterProvider: request.parameterProvider
                        },
                        id,
                        panelClass: 'large-dialog'
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    const goTo = (commands: string[], destination: string) => {
                        // confirm navigating away while changes are unsaved
                        if (editDialogReference.componentInstance.editParameterProviderForm.dirty) {
                            const promptSaveDialogRef = this.dialog.open(YesNoDialog, {
                                data: {
                                    title: 'Parameter Provider Configuration',
                                    message: `Save changes before going to this ${destination}`
                                },
                                panelClass: 'small-dialog'
                            });

                            promptSaveDialogRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
                                editDialogReference.componentInstance.submitForm(commands);
                            });

                            promptSaveDialogRef.componentInstance.no.pipe(take(1)).subscribe(() => {
                                editDialogReference.close('ROUTED');
                                this.router.navigate(commands);
                            });
                        } else {
                            editDialogReference.close('ROUTED');
                            this.router.navigate(commands);
                        }
                    };

                    editDialogReference.componentInstance.goToReferencingParameterContext = (id: string) => {
                        const commands: string[] = ['parameter-contexts', id];
                        goTo(commands, 'Parameter Context');
                    };

                    editDialogReference.componentInstance.goToService = (serviceId: string) => {
                        const commands: string[] = ['/settings', 'management-controller-services', serviceId];
                        goTo(commands, 'Controller Service');
                    };

                    editDialogReference.componentInstance.createNewProperty =
                        this.propertyTableHelperService.createNewProperty(request.id, this.parameterProviderService);

                    editDialogReference.componentInstance.createNewService =
                        this.propertyTableHelperService.createNewService(
                            request.id,
                            this.managementControllerServiceService,
                            this.parameterProviderService
                        );

                    editDialogReference.componentInstance.editParameterProvider
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((updateRequest: UpdateParameterProviderRequest) => {
                            this.store.dispatch(
                                ParameterProviderActions.configureParameterProvider({
                                    request: {
                                        id: request.parameterProvider.id,
                                        uri: request.parameterProvider.uri,
                                        payload: updateRequest.payload,
                                        postUpdateNavigation: updateRequest.postUpdateNavigation
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        if (response !== 'ROUTED') {
                            this.store.dispatch(
                                ParameterProviderActions.selectParameterProvider({
                                    request: {
                                        id
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    configureParameterProvider$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterProviderActions.configureParameterProvider),
            map((action) => action.request),
            switchMap((request) =>
                from(this.parameterProviderService.updateParameterProvider(request)).pipe(
                    map((response) =>
                        ParameterProviderActions.configureParameterProviderSuccess({
                            response: {
                                id: request.id,
                                parameterProvider: response,
                                postUpdateNavigation: request.postUpdateNavigation
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ParameterProviderActions.parameterProvidersApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    configureParameterProviderSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterProviderActions.configureParameterProviderSuccess),
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
}
