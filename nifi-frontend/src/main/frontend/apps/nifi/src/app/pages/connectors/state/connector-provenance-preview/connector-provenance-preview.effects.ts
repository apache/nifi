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
import { HttpErrorResponse } from '@angular/common/http';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import { Store } from '@ngrx/store';
import { MatDialog } from '@angular/material/dialog';
import { catchError, from, map, of, switchMap, takeUntil, tap } from 'rxjs';

import { MEDIUM_DIALOG, XL_DIALOG } from '@nifi/shared';
import * as ConnectorProvenanceActions from './connector-provenance-preview.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { ProvenanceService } from '../../../provenance/service/provenance.service';
import { ProvenanceEventDialog } from '../../../../ui/common/provenance-event-dialog/provenance-event-dialog.component';
import { OkDialog } from '../../../../ui/common/ok-dialog/ok-dialog.component';
import { selectAbout } from '../../../../state/about/about.selectors';
import { Attribute } from '../../../../state/shared';
import { NiFiState } from '../../../../state';
import { ErrorContextKey } from '../../../../state/error';

@Injectable()
export class ConnectorProvenanceEffects {
    private actions$ = inject(Actions);
    private store = inject<Store<NiFiState>>(Store);
    private dialog = inject(MatDialog);
    private provenanceService = inject(ProvenanceService);
    private errorHelper = inject(ErrorHelper);

    loadLatestEventsForComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorProvenanceActions.loadLatestEventsForComponent),
            map((action) => action.componentId),
            switchMap((componentId) =>
                from(
                    this.provenanceService.getLatestEventsForComponent(componentId).pipe(
                        map((response) =>
                            ConnectorProvenanceActions.loadLatestEventsForComponentSuccess({
                                events: response.latestProvenanceEvents.provenanceEvents
                            })
                        ),
                        catchError((errorResponse) =>
                            of(
                                ConnectorProvenanceActions.loadError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            )
                        )
                    )
                )
            )
        )
    );

    openProvenanceEventDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorProvenanceActions.openProvenanceEventDialog),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectAbout)),
                tap(([request, about]) => {
                    const dialogReference = this.dialog.open(ProvenanceEventDialog, {
                        ...XL_DIALOG,
                        autoFocus: 'dialog',
                        data: request
                    });

                    dialogReference.componentInstance.contentViewerAvailable = about?.contentViewerUrl != null;

                    dialogReference.componentInstance.downloadContent
                        .pipe(takeUntil(dialogReference.afterClosed()))
                        .subscribe((direction: string) => {
                            this.store.dispatch(
                                ConnectorProvenanceActions.downloadContent({
                                    request: {
                                        event: request.event,
                                        direction: direction as 'input' | 'output'
                                    }
                                })
                            );
                        });

                    if (about) {
                        dialogReference.componentInstance.viewContent
                            .pipe(takeUntil(dialogReference.afterClosed()))
                            .subscribe((direction: string) => {
                                this.store.dispatch(
                                    ConnectorProvenanceActions.viewContent({
                                        request: {
                                            event: request.event,
                                            direction: direction as 'input' | 'output'
                                        }
                                    })
                                );
                            });
                    }

                    dialogReference.componentInstance.replay
                        .pipe(takeUntil(dialogReference.afterClosed()))
                        .subscribe(() => {
                            dialogReference.close();

                            this.store.dispatch(
                                ConnectorProvenanceActions.replayEvent({
                                    request: {
                                        event: request.event
                                    }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    downloadContent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorProvenanceActions.downloadContent),
                map((action) => action.request),
                tap((request) => {
                    this.provenanceService.downloadContent(
                        request.event.eventId,
                        request.direction,
                        request.event.clusterNodeId
                    );
                })
            ),
        { dispatch: false }
    );

    viewContent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorProvenanceActions.viewContent),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectAbout)),
                tap(([request, about]) => {
                    if (about) {
                        let mimeType: string | undefined;

                        if (request.event.attributes) {
                            const mimeTypeAttribute: Attribute | undefined = request.event.attributes.find(
                                (attribute: Attribute) => attribute.name === 'mime.type'
                            );

                            if (mimeTypeAttribute) {
                                if (request.direction === 'input') {
                                    mimeType = mimeTypeAttribute.previousValue;
                                } else if (request.direction === 'output') {
                                    mimeType = mimeTypeAttribute.value;
                                }
                            }
                        }

                        this.provenanceService.viewContent(
                            about.uri,
                            about.contentViewerUrl,
                            request.event.eventId,
                            request.direction,
                            request.event.clusterNodeId,
                            mimeType
                        );
                    }
                })
            ),
        { dispatch: false }
    );

    replayEvent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorProvenanceActions.replayEvent),
            map((action) => action.request),
            switchMap((request) =>
                this.provenanceService.replay(request.event.eventId, request.event.clusterNodeId).pipe(
                    map(() =>
                        ConnectorProvenanceActions.showOkDialog({
                            title: 'Provenance',
                            message: 'Successfully submitted replay request.'
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ErrorActions.addBannerError({
                                errorContext: {
                                    errors: [this.errorHelper.getErrorString(errorResponse)],
                                    context: ErrorContextKey.CONNECTOR_CANVAS
                                }
                            })
                        )
                    )
                )
            )
        )
    );

    showOkDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorProvenanceActions.showOkDialog),
                tap((request) => {
                    this.dialog.open(OkDialog, {
                        ...MEDIUM_DIALOG,
                        data: {
                            title: request.title,
                            message: request.message
                        }
                    });
                })
            ),
        { dispatch: false }
    );
}
