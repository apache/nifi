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

import { inject, Injectable } from '@angular/core';
import { HttpErrorResponse } from '@angular/common/http';
import { MatDialog } from '@angular/material/dialog';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { Store } from '@ngrx/store';
import { concatLatestFrom } from '@ngrx/operators';
import { catchError, from, map, of, switchMap, tap } from 'rxjs';
import { MEDIUM_DIALOG, SMALL_DIALOG, XL_DIALOG } from '@nifi/shared';
import { NiFiState } from '../index';
import { DropletsService } from '../../service/droplets.service';
import * as DropletsActions from './droplets.actions';
import { DeleteDropletDialogComponent } from '../../pages/resources/feature/ui/delete-droplet-dialog/delete-droplet-dialog.component';
import { ImportNewFlowDialogComponent } from '../../pages/resources/feature/ui/import-new-flow-dialog/import-new-flow-dialog.component';
import { ImportNewFlowDialogData } from '../../pages/resources/feature/ui/import-new-flow-dialog/import-new-flow-dialog.component';
import {
    ImportNewFlowVersionDialogComponent,
    ImportNewFlowVersionDialogData
} from '../../pages/resources/feature/ui/import-new-flow-version-dialog/import-new-flow-version-dialog.component';
import {
    ExportFlowVersionDialogComponent,
    ExportFlowVersionDialogData
} from '../../pages/resources/feature/ui/export-flow-version-dialog/export-flow-version-dialog.component';
import { FlowVersionsDialogComponent } from '../../pages/resources/feature/ui/flow-versions-dialog/flow-versions-dialog.component';
import { ErrorHelper } from '../../service/error-helper.service';
import * as ErrorActions from '../../state/error/error.actions';
import { selectStatus } from './droplets.selectors';
import { Router } from '@angular/router';
import { ErrorContextKey } from '../error';

@Injectable()
export class DropletsEffects {
    constructor(
        private store: Store<NiFiState>,
        private dropletsService: DropletsService,
        private dialog: MatDialog,
        private errorHelper: ErrorHelper,
        private router: Router
    ) {}

    actions$ = inject(Actions);

    loadDroplets$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.loadDroplets),
            concatLatestFrom(() => this.store.select(selectStatus)),
            switchMap(([, status]) => {
                return from(
                    this.dropletsService.getDroplets().pipe(
                        map((response) =>
                            DropletsActions.loadDropletsSuccess({
                                response: {
                                    droplets: response
                                }
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(this.errorHelper.handleLoadingError(status, errorResponse))
                        )
                    )
                );
            })
        )
    );

    openDeleteDropletDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(DropletsActions.openDeleteDropletDialog),
                tap(({ request }) => {
                    const dialogRef = this.dialog.open<DeleteDropletDialogComponent, ExportFlowVersionDialogData>(
                        DeleteDropletDialogComponent,
                        {
                            ...SMALL_DIALOG,
                            autoFocus: false,
                            data: request
                        }
                    );

                    dialogRef.afterClosed().subscribe(() => this.store.dispatch(DropletsActions.loadDroplets()));
                })
            ),
        { dispatch: false }
    );

    deleteDroplet$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.deleteDroplet),
            map((action) => action.request),
            switchMap((request) =>
                from(this.dropletsService.deleteDroplet(request.droplet.link.href)).pipe(
                    map((res) => DropletsActions.deleteDropletSuccess({ response: res })),
                    tap({
                        error: (errorResponse: HttpErrorResponse) => {
                            this.store.dispatch(
                                DropletsActions.dropletsBannerError({
                                    errorContext: {
                                        context: ErrorContextKey.DELETE_DROPLET,
                                        errors: [this.errorHelper.getErrorString(errorResponse)]
                                    }
                                })
                            );
                        }
                    })
                )
            )
        )
    );

    deleteDropletSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.deleteDropletSuccess),
            tap(() => this.dialog.closeAll()),
            switchMap(() => of(DropletsActions.loadDroplets()))
        )
    );

    openImportNewFlowDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(DropletsActions.openImportNewFlowDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog.open<ImportNewFlowDialogComponent, ImportNewFlowDialogData>(
                        ImportNewFlowDialogComponent,
                        {
                            ...MEDIUM_DIALOG,
                            autoFocus: false,
                            data: {
                                buckets: request.buckets
                            }
                        }
                    );
                })
            ),
        { dispatch: false }
    );

    createNewFlow$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.createNewFlow),
            map((action) => action.request),
            switchMap((request) =>
                from(
                    this.dropletsService.createNewFlow(request.bucket.link.href, request.name, request.description)
                ).pipe(
                    map((res) =>
                        DropletsActions.createNewFlowSuccess({
                            request: {
                                href: res.link.href,
                                file: request.file,
                                description: request.description
                            }
                        })
                    ),
                    tap({
                        error: (errorResponse: HttpErrorResponse) => {
                            this.store.dispatch(
                                DropletsActions.dropletsBannerError({
                                    errorContext: {
                                        context: ErrorContextKey.CREATE_DROPLET,
                                        errors: [this.errorHelper.getErrorString(errorResponse)]
                                    }
                                })
                            );
                        }
                    })
                )
            )
        )
    );

    createNewFlowSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.createNewFlowSuccess),
            tap(() => this.store.dispatch(DropletsActions.loadDroplets())),
            map((action) => action.request),
            map(({ href, file, description }) =>
                DropletsActions.importNewFlow({ request: { href, file, description } })
            )
        )
    );

    importNewFlow$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.importNewFlow),
            map((action) => action.request),
            switchMap(({ href, file, description }) =>
                from(this.dropletsService.uploadFlow(href, file, description)).pipe(
                    map((res) => DropletsActions.importNewFlowSuccess({ response: res })),
                    tap({
                        error: (errorResponse: HttpErrorResponse) => {
                            this.store.dispatch(
                                DropletsActions.dropletsBannerError({
                                    errorContext: {
                                        context: ErrorContextKey.IMPORT_DROPLET_VERSION,
                                        errors: [this.errorHelper.getErrorString(errorResponse)]
                                    }
                                })
                            );
                        }
                    })
                )
            )
        )
    );

    importNewFlowSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.importNewFlowSuccess),
            tap(() => this.dialog.closeAll()),
            switchMap(() => of(DropletsActions.loadDroplets()))
        )
    );

    openImportNewFlowVersionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(DropletsActions.openImportNewFlowVersionDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog.open<ImportNewFlowVersionDialogComponent, ImportNewFlowVersionDialogData>(
                        ImportNewFlowVersionDialogComponent,
                        {
                            ...MEDIUM_DIALOG,
                            autoFocus: false,
                            data: {
                                droplet: request.droplet
                            }
                        }
                    );
                })
            ),
        { dispatch: false }
    );

    openExportFlowVersionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(DropletsActions.openExportFlowVersionDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog.open<ExportFlowVersionDialogComponent, ExportFlowVersionDialogData>(
                        ExportFlowVersionDialogComponent,
                        {
                            ...MEDIUM_DIALOG,
                            autoFocus: false,
                            data: {
                                droplet: request.droplet
                            }
                        }
                    );
                })
            ),
        { dispatch: false }
    );

    exportFlowVersion$ = createEffect(() => {
        return this.actions$.pipe(
            ofType(DropletsActions.exportFlowVersion),
            map((action) => action.request),
            switchMap((request) =>
                from(
                    this.dropletsService.exportDropletVersionedSnapshot(request.droplet.link.href, request.version)
                ).pipe(
                    tap((res) => {
                        const stringSnapshot = encodeURIComponent(res.body);
                        const filename = res.headers.get('Filename');

                        const anchorElement = document.createElement('a');
                        anchorElement.href = 'data:application/json;charset=utf-8,' + stringSnapshot;
                        anchorElement.download = filename;
                        anchorElement.setAttribute('style', 'display: none;');

                        document.body.appendChild(anchorElement);
                        anchorElement.click();
                        document.body.removeChild(anchorElement);

                        return res;
                    }),
                    map((res) => DropletsActions.exportFlowVersionSuccess({ response: res })),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.dialog.closeAll();
                        return of(
                            DropletsActions.dropletsSnackbarError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        );
                    })
                )
            )
        );
    });

    exportFlowVersionSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.exportFlowVersionSuccess),
            switchMap(() => of(DropletsActions.loadDroplets()))
        )
    );

    openFlowVersionsDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(DropletsActions.openFlowVersionsDialog),
                map((action) => action.request),
                switchMap((request) =>
                    from(this.dropletsService.getDropletSnapshotMetadata(request.droplet.link.href)).pipe(
                        map((res) => {
                            this.dialog.open(FlowVersionsDialogComponent, {
                                ...XL_DIALOG,
                                autoFocus: false,
                                data: {
                                    droplet: request.droplet,
                                    versions: res
                                }
                            });
                        }),
                        catchError((errorResponse: HttpErrorResponse) => {
                            return of(
                                DropletsActions.dropletsSnackbarError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            );
                        })
                    )
                )
            ),
        { dispatch: false }
    );

    selectDroplet$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(DropletsActions.selectDroplet),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/resources', request.id]);
                })
            ),
        { dispatch: false }
    );

    dropletsBannerError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.dropletsBannerError),
            map((action) => action.errorContext),
            switchMap((errorContext) => of(ErrorActions.addBannerError({ errorContext })))
        )
    );

    dropletsSnackbarError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.dropletsSnackbarError),
            map((action) => action.error),
            tap(() => {
                this.dialog.closeAll();
            }),
            switchMap((error) => of(ErrorActions.snackBarError({ error })))
        )
    );
}
