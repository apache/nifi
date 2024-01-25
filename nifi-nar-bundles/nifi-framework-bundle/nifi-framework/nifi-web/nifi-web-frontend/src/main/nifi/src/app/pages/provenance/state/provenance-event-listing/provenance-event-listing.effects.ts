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
import * as ProvenanceEventListingActions from './provenance-event-listing.actions';
import { asyncScheduler, catchError, from, interval, map, NEVER, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { Router } from '@angular/router';
import { OkDialog } from '../../../../ui/common/ok-dialog/ok-dialog.component';
import { ProvenanceService } from '../../service/provenance.service';
import {
    selectClusterNodeId,
    selectProvenanceId,
    selectProvenanceOptions,
    selectProvenanceRequest,
    selectTimeOffset
} from './provenance-event-listing.selectors';
import { Provenance, ProvenanceRequest } from './index';
import { ProvenanceSearchDialog } from '../../ui/provenance-event-listing/provenance-search-dialog/provenance-search-dialog.component';
import { selectAbout } from '../../../../state/about/about.selectors';
import { ProvenanceEventDialog } from '../../../../ui/common/provenance-event-dialog/provenance-event-dialog.component';
import { CancelDialog } from '../../../../ui/common/cancel-dialog/cancel-dialog.component';

@Injectable()
export class ProvenanceEventListingEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private provenanceService: ProvenanceService,
        private dialog: MatDialog,
        private router: Router
    ) {}

    loadProvenanceOptions$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProvenanceEventListingActions.loadProvenanceOptions),
            switchMap((request) =>
                from(this.provenanceService.getSearchOptions()).pipe(
                    map((response) =>
                        ProvenanceEventListingActions.loadProvenanceOptionsSuccess({
                            response
                        })
                    ),
                    catchError((error) =>
                        of(
                            ProvenanceEventListingActions.provenanceApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    submitProvenanceQuery$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProvenanceEventListingActions.submitProvenanceQuery),
            map((action) => action.request),
            switchMap((request) =>
                from(this.provenanceService.submitProvenanceQuery(request)).pipe(
                    map((response) =>
                        ProvenanceEventListingActions.submitProvenanceQuerySuccess({
                            response: {
                                provenance: response.provenance
                            }
                        })
                    ),
                    catchError((error) => {
                        this.store.dispatch(
                            ProvenanceEventListingActions.showOkDialog({
                                title: 'Error',
                                message: error.error
                            })
                        );

                        return of(
                            ProvenanceEventListingActions.provenanceApiError({
                                error: error.error
                            })
                        );
                    })
                )
            )
        )
    );

    resubmitProvenanceQuery = createEffect(() =>
        this.actions$.pipe(
            ofType(ProvenanceEventListingActions.resubmitProvenanceQuery),
            map((action) => action.request),
            switchMap((request) => {
                const dialogReference = this.dialog.open(CancelDialog, {
                    data: {
                        title: 'Provenance',
                        message: 'Searching provenance events...'
                    },
                    disableClose: true,
                    panelClass: 'small-dialog'
                });

                dialogReference.componentInstance.cancel.pipe(take(1)).subscribe(() => {
                    this.store.dispatch(ProvenanceEventListingActions.stopPollingProvenanceQuery());
                });

                return of(ProvenanceEventListingActions.submitProvenanceQuery({ request }));
            })
        )
    );

    submitProvenanceQuerySuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProvenanceEventListingActions.submitProvenanceQuerySuccess),
            map((action) => action.response),
            switchMap((response) => {
                const query: Provenance = response.provenance;
                if (query.finished) {
                    this.dialog.closeAll();
                    return of(ProvenanceEventListingActions.deleteProvenanceQuery());
                } else {
                    return of(ProvenanceEventListingActions.startPollingProvenanceQuery());
                }
            })
        )
    );

    startPollingProvenanceQuery$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProvenanceEventListingActions.startPollingProvenanceQuery),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(ProvenanceEventListingActions.stopPollingProvenanceQuery)))
                )
            ),
            switchMap(() => of(ProvenanceEventListingActions.pollProvenanceQuery()))
        )
    );

    pollProvenanceQuery$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProvenanceEventListingActions.pollProvenanceQuery),
            concatLatestFrom(() => [this.store.select(selectProvenanceId), this.store.select(selectClusterNodeId)]),
            switchMap(([action, id, clusterNodeId]) => {
                if (id) {
                    return from(this.provenanceService.getProvenanceQuery(id, clusterNodeId)).pipe(
                        map((response) =>
                            ProvenanceEventListingActions.pollProvenanceQuerySuccess({
                                response: {
                                    provenance: response.provenance
                                }
                            })
                        ),
                        catchError((error) =>
                            of(
                                ProvenanceEventListingActions.provenanceApiError({
                                    error: error.error
                                })
                            )
                        )
                    );
                } else {
                    return NEVER;
                }
            })
        )
    );

    pollProvenanceQuerySuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProvenanceEventListingActions.pollProvenanceQuerySuccess),
            map((action) => action.response),
            switchMap((response) => {
                const query: Provenance = response.provenance;
                if (query.finished) {
                    this.dialog.closeAll();
                    return of(ProvenanceEventListingActions.stopPollingProvenanceQuery());
                } else {
                    return NEVER;
                }
            })
        )
    );

    stopPollingProvenanceQuery$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProvenanceEventListingActions.stopPollingProvenanceQuery),
            switchMap((response) => of(ProvenanceEventListingActions.deleteProvenanceQuery()))
        )
    );

    deleteProvenanceQuery$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ProvenanceEventListingActions.deleteProvenanceQuery),
                concatLatestFrom(() => [this.store.select(selectProvenanceId), this.store.select(selectClusterNodeId)]),
                tap(([action, id, clusterNodeId]) => {
                    if (id) {
                        this.provenanceService.deleteProvenanceQuery(id, clusterNodeId).subscribe();
                    }
                })
            ),
        { dispatch: false }
    );

    openSearchDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ProvenanceEventListingActions.openSearchDialog),
                concatLatestFrom(() => [
                    this.store.select(selectTimeOffset),
                    this.store.select(selectProvenanceOptions),
                    this.store.select(selectProvenanceRequest),
                    this.store.select(selectAbout)
                ]),
                tap(([request, timeOffset, options, currentRequest, about]) => {
                    if (about) {
                        const dialogReference = this.dialog.open(ProvenanceSearchDialog, {
                            data: {
                                timeOffset,
                                options,
                                currentRequest
                            },
                            panelClass: 'large-dialog'
                        });

                        dialogReference.componentInstance.timezone = about.timezone;

                        dialogReference.componentInstance.submitSearchCriteria
                            .pipe(take(1))
                            .subscribe((request: ProvenanceRequest) => {
                                if (request.searchTerms) {
                                    const queryParams: any = {};
                                    if (request.searchTerms['ProcessorID']) {
                                        queryParams['componentId'] = request.searchTerms['ProcessorID'].value;
                                    }
                                    if (request.searchTerms['FlowFileUUID']) {
                                        queryParams['flowFileUuid'] = request.searchTerms['FlowFileUUID'].value;
                                    }

                                    // if either of the supported query params are present in the query, update the url
                                    if (Object.keys(queryParams).length > 0) {
                                        this.router.navigate(['/provenance'], { queryParams });
                                    }
                                }

                                this.store.dispatch(ProvenanceEventListingActions.saveProvenanceRequest({ request }));
                            });
                    }

                    // TODO - if about hasn't loaded we should show an error
                })
            ),
        { dispatch: false }
    );

    openProvenanceEventDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ProvenanceEventListingActions.openProvenanceEventDialog),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectAbout)),
                tap(([request, about]) => {
                    this.provenanceService.getProvenanceEvent(request.id).subscribe({
                        next: (response) => {
                            const dialogReference = this.dialog.open(ProvenanceEventDialog, {
                                data: {
                                    event: response.provenanceEvent
                                },
                                panelClass: 'large-dialog'
                            });

                            dialogReference.componentInstance.contentViewerAvailable =
                                about?.contentViewerUrl != null ?? false;

                            dialogReference.componentInstance.downloadContent
                                .pipe(takeUntil(dialogReference.afterClosed()))
                                .subscribe((direction: string) => {
                                    this.provenanceService.downloadContent(request.id, direction);
                                });

                            if (about) {
                                dialogReference.componentInstance.viewContent
                                    .pipe(takeUntil(dialogReference.afterClosed()))
                                    .subscribe((direction: string) => {
                                        this.provenanceService.viewContent(
                                            about.uri,
                                            about.contentViewerUrl,
                                            request.id,
                                            direction
                                        );
                                    });
                            }

                            dialogReference.componentInstance.replay
                                .pipe(takeUntil(dialogReference.afterClosed()))
                                .subscribe(() => {
                                    this.provenanceService.replay(request.id).subscribe(() => {
                                        this.store.dispatch(
                                            ProvenanceEventListingActions.showOkDialog({
                                                title: 'Provenance',
                                                message: 'Successfully submitted replay request.'
                                            })
                                        );
                                    });
                                });
                        },
                        error: (error) => {
                            // TODO - handle error
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    goToProvenanceEventSource$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ProvenanceEventListingActions.goToProvenanceEventSource),
                map((action) => action.request),
                tap((request) => {
                    if (request.eventId) {
                        this.provenanceService.getProvenanceEvent(request.eventId).subscribe((response) => {
                            const event: any = response.provenanceEvent;
                            this.router.navigate(this.getEventComponentLink(event.groupId, event.componentId));
                        });
                    } else if (request.groupId && request.componentId) {
                        this.router.navigate(this.getEventComponentLink(request.groupId, request.componentId));
                    }
                })
            ),
        { dispatch: false }
    );

    showOkDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ProvenanceEventListingActions.showOkDialog),
                tap((request) => {
                    this.dialog.open(OkDialog, {
                        data: {
                            title: request.title,
                            message: request.message
                        },
                        panelClass: 'medium-dialog'
                    });
                })
            ),
        { dispatch: false }
    );

    private getEventComponentLink(groupId: string, componentId: string): string[] {
        let link: string[];

        if (groupId == componentId) {
            link = ['/process-groups', componentId];
        } else if (componentId === 'Connection' || componentId === 'Load Balanced Connection') {
            link = ['/process-groups', groupId, 'Connection', componentId];
        } else if (componentId === 'Output Port') {
            link = ['/process-groups', groupId, 'OutputPort', componentId];
        } else {
            link = ['/process-groups', groupId, 'Processor', componentId];
        }

        return link;
    }
}
