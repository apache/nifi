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
import { catchError, from, map, of, switchMap, take, tap } from 'rxjs';
import { MEDIUM_DIALOG, SMALL_DIALOG, XL_DIALOG, YesNoDialog } from '@nifi/shared';
import { DropletsService } from '../../service/droplets.service';
import * as DropletsActions from './droplets.actions';
import {
    ImportNewDropletDialogComponent,
    ImportNewFlowDialogData
} from '../../pages/resources/feature/ui/import-new-droplet-dialog/import-new-droplet-dialog.component';
import {
    ImportNewDropletVersionDialogComponent,
    ImportNewFlowVersionDialogData
} from '../../pages/resources/feature/ui/import-new-droplet-version-dialog/import-new-droplet-version-dialog.component';
import {
    ExportDropletVersionDialogComponent,
    ExportFlowVersionDialogData
} from '../../pages/resources/feature/ui/export-droplet-version-dialog/export-droplet-version-dialog.component';
import { DropletVersionsDialogComponent } from '../../pages/resources/feature/ui/droplet-versions-dialog/droplet-versions-dialog.component';
import { ErrorHelper } from '../../service/error-helper.service';
import { ErrorContextKey } from '../error';
import * as ErrorActions from '../../state/error/error.actions';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../../nifi/src/app/state';
import { deleteDroplet } from './droplets.actions';

@Injectable()
export class DropletsEffects {
    private dropletsService = inject(DropletsService);
    private dialog = inject(MatDialog);
    private errorHelper = inject(ErrorHelper);
    private store = inject<Store<NiFiState>>(Store);

    actions$ = inject(Actions);

    loadDroplets$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.loadDroplets),
            switchMap(() => {
                return from(
                    this.dropletsService.getDroplets().pipe(
                        map((response) =>
                            DropletsActions.loadDropletsSuccess({
                                response: {
                                    droplets: response
                                }
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) => of(this.bannerError(errorResponse)))
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
                    const dialogRef = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Delete resource',
                            message: `This action will delete all versions of ${request.droplet.name}`
                        }
                    });
                    dialogRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            deleteDroplet({
                                request: {
                                    droplet: request.droplet
                                }
                            })
                        );
                    });
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
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            DropletsActions.deleteDropletFailure(),
                            ErrorActions.snackBarError({ error: this.errorHelper.getErrorString(errorResponse) })
                        )
                    )
                )
            )
        )
    );

    deleteDropletSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(DropletsActions.deleteDropletSuccess),
                tap(() => this.dialog.closeAll())
            ),
        { dispatch: false }
    );

    openImportNewDropletDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(DropletsActions.openImportNewDropletDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog.open<ImportNewDropletDialogComponent, ImportNewFlowDialogData>(
                        ImportNewDropletDialogComponent,
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

    createNewDroplet$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.createNewDroplet),
            map((action) => action.request),
            switchMap((request) =>
                from(
                    this.dropletsService.createNewDroplet(request.bucket.link.href, request.name, request.description)
                ).pipe(
                    map((res) =>
                        DropletsActions.createNewDropletSuccess({
                            response: res,
                            request: {
                                href: res.link.href,
                                file: request.file,
                                description: request.description
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.bannerError(errorResponse, ErrorContextKey.CREATE_DROPLET))
                    )
                )
            )
        )
    );

    createNewDropletSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.createNewDropletSuccess),
            tap(() => this.dialog.closeAll()),
            map((action) =>
                DropletsActions.importVersionForNewDroplet({
                    request: action.request,
                    createdDroplet: action.response
                })
            )
        )
    );

    importNewDroplet$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.importNewDropletVersion),
            map((action) => action.request),
            switchMap(({ href, file, description }) =>
                from(this.dropletsService.uploadDroplet(href, file, description)).pipe(
                    map((res) => DropletsActions.importNewDropletVersionSuccess({ response: res })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.bannerError(errorResponse, ErrorContextKey.IMPORT_DROPLET_VERSION))
                    )
                )
            )
        )
    );

    importVersionForNewDroplet$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.importVersionForNewDroplet),
            switchMap((action) =>
                from(
                    this.dropletsService.uploadDroplet(
                        action.request.href,
                        action.request.file,
                        action.request.description
                    )
                ).pipe(
                    map((res) => DropletsActions.importNewDropletVersionSuccess({ response: res })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            DropletsActions.importNewDropletVersionError({
                                errorContext: {
                                    errors: [this.errorHelper.getErrorString(errorResponse)],
                                    context: ErrorContextKey.IMPORT_DROPLET_VERSION
                                },
                                createdDroplet: action.createdDroplet
                            })
                        )
                    )
                )
            )
        )
    );

    importNewDropletSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(DropletsActions.importNewDropletVersionSuccess),
                tap(() => this.dialog.closeAll())
            ),
        { dispatch: false }
    );

    importNewDropletVersionError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.importNewDropletVersionError),
            switchMap((action) =>
                from(this.dropletsService.deleteDroplet(action.createdDroplet.link.href + '?version=0')).pipe(
                    switchMap(() => [DropletsActions.dropletsBannerError({ errorContext: action.errorContext })]),
                    catchError(() => [
                        // If deletion also fails, still show the original import error
                        DropletsActions.dropletsBannerError({ errorContext: action.errorContext })
                    ])
                )
            )
        )
    );

    openImportNewDropletVersionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(DropletsActions.openImportNewDropletVersionDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog.open<ImportNewDropletVersionDialogComponent, ImportNewFlowVersionDialogData>(
                        ImportNewDropletVersionDialogComponent,
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

    openExportDropletVersionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(DropletsActions.openExportDropletVersionDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog.open<ExportDropletVersionDialogComponent, ExportFlowVersionDialogData>(
                        ExportDropletVersionDialogComponent,
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

    exportDropletVersion$ = createEffect(() => {
        return this.actions$.pipe(
            ofType(DropletsActions.exportDropletVersion),
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
                    map((res) => DropletsActions.exportDropletVersionSuccess({ response: res })),
                    catchError((errorResponse: HttpErrorResponse) => {
                        return of(this.bannerError(errorResponse, ErrorContextKey.EXPORT_DROPLET_VERSION));
                    })
                )
            )
        );
    });

    exportDropletVersionSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(DropletsActions.exportDropletVersionSuccess),
                tap(() => this.dialog.closeAll())
            ),
        { dispatch: false }
    );

    openDropletVersionsDialog$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.openDropletVersionsDialog),
            map((action) => action.request),
            switchMap((request) =>
                from(this.dropletsService.getDropletSnapshotMetadata(request.droplet.link.href)).pipe(
                    map((res) => {
                        this.dialog.open(DropletVersionsDialogComponent, {
                            ...XL_DIALOG,
                            autoFocus: false,
                            data: {
                                droplet: request.droplet,
                                versions: res
                            }
                        });
                        return DropletsActions.noOp();
                    }),
                    catchError((errorResponse: HttpErrorResponse) => of(this.bannerError(errorResponse)))
                )
            )
        )
    );

    dropletsBannerError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(DropletsActions.dropletsBannerError),
            map((action) => action.errorContext),
            switchMap((errorContext) => of(ErrorActions.addBannerError({ errorContext })))
        )
    );

    private bannerError(errorResponse: HttpErrorResponse, context: ErrorContextKey = ErrorContextKey.GLOBAL) {
        return DropletsActions.dropletsBannerError({
            errorContext: {
                errors: [this.errorHelper.getErrorString(errorResponse)],
                context
            }
        });
    }
}
