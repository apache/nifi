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
import { Store } from '@ngrx/store';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { concatLatestFrom } from '@ngrx/operators';
import { catchError, concatMap, EMPTY, from, map, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { LARGE_DIALOG, MEDIUM_DIALOG, SMALL_DIALOG, YesNoDialog } from '@nifi/shared';
import { NiFiState } from '../../../../state';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { Client } from '../../../../service/client.service';
import { CreateConnector } from '../../ui/create-connector/create-connector.component';
import { RenameConnectorDialog } from '../../ui/rename-connector-dialog/rename-connector-dialog.component';
import { selectLoadedTimestamp, selectSaving } from './connectors-listing.selectors';
import { initialState } from './connectors-listing.reducer';
import { DocumentedType, OpenChangeComponentVersionDialogRequest } from '../../../../state/shared';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorContextKey } from '../../../../state/error';
import {
    cancelConnectorDrain,
    cancelConnectorDrainSuccess,
    changeConnectorVersion,
    changeConnectorVersionApiError,
    changeConnectorVersionSuccess,
    connectorsListingBannerApiError,
    createConnector,
    createConnectorSuccess,
    deleteConnector,
    deleteConnectorSuccess,
    discardConnectorConfig,
    discardConnectorConfigSuccess,
    drainConnector,
    drainConnectorSuccess,
    loadConnectorsListing,
    loadConnectorsListingError,
    loadConnectorsListingSuccess,
    navigateToConfigureConnector,
    navigateToManageAccessPolicies,
    navigateToViewConnector,
    navigateToViewConnectorDetails,
    openChangeConnectorVersionDialog,
    openNewConnectorDialog,
    openRenameConnectorDialog,
    promptConnectorDeletion,
    promptDiscardConnectorConfig,
    promptDrainConnector,
    renameConnector,
    renameConnectorApiError,
    renameConnectorSuccess,
    selectConnector,
    startConnector,
    startConnectorSuccess,
    stopConnector,
    stopConnectorSuccess
} from './connectors-listing.actions';
import { RenameConnectorRequest } from '../index';
import { BackNavigation } from '../../../../state/navigation';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { ChangeComponentVersionDialog } from '../../../../ui/common/change-component-version-dialog/change-component-version-dialog';

@Injectable()
export class ConnectorsListingEffects {
    private actions$ = inject(Actions);
    private store = inject<Store<NiFiState>>(Store);
    private connectorService = inject(ConnectorService);
    private errorHelper = inject(ErrorHelper);
    private client = inject(Client);
    private dialog = inject(MatDialog);
    private router = inject(Router);
    private extensionTypesService = inject(ExtensionTypesService);

    loadConnectorsListing$ = createEffect(() =>
        this.actions$.pipe(
            ofType(loadConnectorsListing),
            concatLatestFrom(() => this.store.select(selectLoadedTimestamp)),
            switchMap(([, loadedTimestamp]) =>
                from(this.connectorService.getConnectors()).pipe(
                    map((response) =>
                        loadConnectorsListingSuccess({
                            response: {
                                connectors: response.connectors || [],
                                loadedTimestamp: response.currentTime
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            loadConnectorsListingError({
                                errorResponse,
                                loadedTimestamp,
                                status: loadedTimestamp !== initialState.loadedTimestamp ? 'success' : 'pending'
                            })
                        )
                    )
                )
            )
        )
    );

    openNewConnectorDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(openNewConnectorDialog),
                tap(() => {
                    const dialogRef = this.dialog.open(CreateConnector, {
                        ...LARGE_DIALOG
                    });

                    dialogRef
                        .componentInstance!.createConnector.pipe(take(1))
                        .subscribe((connectorType: DocumentedType) => {
                            this.store.dispatch(
                                createConnector({
                                    request: {
                                        revision: {
                                            clientId: this.client.getClientId(),
                                            version: 0
                                        },
                                        connectorType: connectorType.type,
                                        connectorBundle: connectorType.bundle
                                    }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    createConnector$ = createEffect(() =>
        this.actions$.pipe(
            ofType(createConnector),
            map((action) => action.request),
            switchMap((request) =>
                from(this.connectorService.createConnector(request)).pipe(
                    map((response) =>
                        createConnectorSuccess({
                            response: {
                                connector: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.dialog.closeAll();
                        return of(
                            connectorsListingBannerApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        );
                    })
                )
            )
        )
    );

    createConnectorSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(createConnectorSuccess),
            map((action) => action.response),
            tap(() => {
                this.dialog.closeAll();
            }),
            switchMap((response) =>
                of(
                    selectConnector({
                        request: {
                            id: response.connector.id
                        }
                    })
                )
            )
        )
    );

    promptConnectorDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(promptConnectorDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogRef = this.dialog.open<YesNoDialog>(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Delete Connector',
                            message: `Are you sure you want to delete connector '${request.connector.component.name}'?`
                        }
                    });

                    dialogRef.componentInstance!.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(deleteConnector({ request }));
                    });
                })
            ),
        { dispatch: false }
    );

    deleteConnector$ = createEffect(() =>
        this.actions$.pipe(
            ofType(deleteConnector),
            map((action) => action.request),
            switchMap((request) =>
                from(this.connectorService.deleteConnector(request.connector)).pipe(
                    map((response) =>
                        deleteConnectorSuccess({
                            response: {
                                connector: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            connectorsListingBannerApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    connectorsListingError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(loadConnectorsListingError),
            map((action) =>
                this.errorHelper.handleLoadingError(
                    action.loadedTimestamp !== initialState.loadedTimestamp,
                    action.errorResponse
                )
            )
        )
    );

    connectorsListingBannerApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(connectorsListingBannerApiError),
            map((action) => action.error),
            switchMap((error) =>
                of(
                    ErrorActions.addBannerError({
                        errorContext: { errors: [error], context: ErrorContextKey.CONNECTORS }
                    })
                )
            )
        )
    );

    startConnector$ = createEffect(() =>
        this.actions$.pipe(
            ofType(startConnector),
            map((action) => action.request),
            switchMap((request) =>
                from(this.connectorService.updateConnectorRunStatus(request.connector, 'RUNNING')).pipe(
                    map((response) =>
                        startConnectorSuccess({
                            response: { connector: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            connectorsListingBannerApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    stopConnector$ = createEffect(() =>
        this.actions$.pipe(
            ofType(stopConnector),
            map((action) => action.request),
            switchMap((request) =>
                from(this.connectorService.updateConnectorRunStatus(request.connector, 'STOPPED')).pipe(
                    map((response) =>
                        stopConnectorSuccess({
                            response: { connector: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            connectorsListingBannerApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    selectConnector$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(selectConnector),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/connectors', request.id]);
                })
            ),
        { dispatch: false }
    );

    navigateToConfigureConnector$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(navigateToConfigureConnector),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/connectors', request.id, 'configure']);
                })
            ),
        { dispatch: false }
    );

    navigateToManageAccessPolicies$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(navigateToManageAccessPolicies),
                map((action) => action.id),
                tap((id) => {
                    const routeBoundary: string[] = ['/access-policies'];
                    this.router.navigate([...routeBoundary, 'read', 'component', 'connectors', id], {
                        state: {
                            backNavigation: {
                                route: ['/connectors', id],
                                routeBoundary,
                                context: 'connector'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    openRenameConnectorDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(openRenameConnectorDialog),
                map((action) => action.connector),
                tap((connector) => {
                    const dialogRef = this.dialog.open(RenameConnectorDialog, {
                        ...MEDIUM_DIALOG,
                        data: { connector }
                    });

                    // Pass saving observable to dialog
                    dialogRef.componentInstance.saving$ = this.store.select(selectSaving);

                    dialogRef.componentInstance.rename
                        .pipe(takeUntil(dialogRef.afterClosed()))
                        .subscribe((request: RenameConnectorRequest) => {
                            this.store.dispatch(renameConnector({ request }));
                        });

                    dialogRef.componentInstance.exit.pipe(takeUntil(dialogRef.afterClosed())).subscribe(() => {
                        dialogRef.close();
                    });
                })
            ),
        { dispatch: false }
    );

    renameConnector$ = createEffect(() =>
        this.actions$.pipe(
            ofType(renameConnector),
            map((action) => action.request),
            switchMap((request) => {
                // Create updated connector with new name
                const updatedConnector = {
                    ...request.connector,
                    component: {
                        ...request.connector.component,
                        name: request.newName
                    }
                };

                return from(this.connectorService.updateConnector(updatedConnector)).pipe(
                    map((response) =>
                        renameConnectorSuccess({
                            response: { connector: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            renameConnectorApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                );
            })
        )
    );

    renameConnectorSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(renameConnectorSuccess),
                tap(() => {
                    this.dialog.closeAll();
                })
            ),
        { dispatch: false }
    );

    renameConnectorApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(renameConnectorApiError),
            map((action) => action.error),
            switchMap((error) =>
                of(
                    ErrorActions.addBannerError({
                        errorContext: { errors: [error], context: ErrorContextKey.CONNECTOR_RENAME_DIALOG }
                    })
                )
            )
        )
    );

    openChangeConnectorVersionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(openChangeConnectorVersionDialog),
                map((action) => action.request),
                switchMap((request) =>
                    from(this.extensionTypesService.getConnectorVersionsForType(request.type, request.bundle)).pipe(
                        map(
                            (response) =>
                                ({
                                    fetchRequest: request,
                                    componentVersions: response.connectorTypes
                                }) as OpenChangeComponentVersionDialogRequest
                        ),
                        catchError((errorResponse: HttpErrorResponse) => {
                            this.store.dispatch(
                                ErrorActions.snackBarError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            );
                            return EMPTY;
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
                            changeConnectorVersion({
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

    changeConnectorVersion$ = createEffect(() =>
        this.actions$.pipe(
            ofType(changeConnectorVersion),
            map((action) => action.request),
            concatMap((request) =>
                from(this.connectorService.changeConnectorVersion(request)).pipe(
                    map((response) => changeConnectorVersionSuccess({ response: { connector: response } })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(changeConnectorVersionApiError({ error: this.errorHelper.getErrorString(errorResponse) }))
                    )
                )
            )
        )
    );

    changeConnectorVersionApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(changeConnectorVersionApiError),
            map((action) => ErrorActions.snackBarError({ error: action.error }))
        )
    );

    navigateToViewConnector$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(navigateToViewConnector),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/connectors', request.connectorId, 'canvas', request.processGroupId]);
                })
            ),
        { dispatch: false }
    );

    navigateToViewConnectorDetails$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(navigateToViewConnectorDetails),
                tap((action) => {
                    this.router.navigate(['/connectors', action.id, 'detail']);
                })
            ),
        { dispatch: false }
    );

    promptDiscardConnectorConfig$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(promptDiscardConnectorConfig),
                map((action) => action.request),
                tap((request) => {
                    const dialogRef = this.dialog.open<YesNoDialog>(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Discard Configuration Changes',
                            message: `Are you sure you want to discard all configuration changes for connector '${request.connector.component.name}'? This will revert to the last applied configuration and cannot be undone.`
                        }
                    });

                    dialogRef.componentInstance!.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(discardConnectorConfig({ request }));
                    });
                })
            ),
        { dispatch: false }
    );

    discardConnectorConfig$ = createEffect(() =>
        this.actions$.pipe(
            ofType(discardConnectorConfig),
            map((action) => action.request),
            switchMap((request) =>
                from(this.connectorService.discardConnectorWorkingConfiguration(request.connector)).pipe(
                    map((response) =>
                        discardConnectorConfigSuccess({
                            response: { connector: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            connectorsListingBannerApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    promptDrainConnector$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(promptDrainConnector),
                map((action) => action.request),
                tap((request) => {
                    const dialogRef = this.dialog.open<YesNoDialog>(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Drain Connector',
                            message: `Are you sure you want to drain connector '${request.connector.component.name}'? This will process existing FlowFiles but prevent new data from being ingested.`
                        }
                    });

                    dialogRef.componentInstance!.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(drainConnector({ request }));
                    });
                })
            ),
        { dispatch: false }
    );

    drainConnector$ = createEffect(() =>
        this.actions$.pipe(
            ofType(drainConnector),
            map((action) => action.request),
            switchMap((request) =>
                from(this.connectorService.drainConnector(request.connector)).pipe(
                    map((response) =>
                        drainConnectorSuccess({
                            response: { connector: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            connectorsListingBannerApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    cancelConnectorDrain$ = createEffect(() =>
        this.actions$.pipe(
            ofType(cancelConnectorDrain),
            map((action) => action.request),
            switchMap((request) =>
                from(this.connectorService.cancelConnectorDrain(request.connector)).pipe(
                    map((response) =>
                        cancelConnectorDrainSuccess({
                            response: { connector: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            connectorsListingBannerApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );
}
