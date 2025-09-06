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
import * as ContentActions from './content.actions';
import { catchError, from, map, of, switchMap, tap, withLatestFrom } from 'rxjs';
import { NavigationExtras, Router } from '@angular/router';
import { NiFiCommon, selectQueryParams } from '@nifi/shared';
import { select, Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { QueueService } from '../../../queue/service/queue.service';
import { ProvenanceService } from '../../../provenance/service/provenance.service';

@Injectable()
export class ContentEffects {
    constructor(
        private actions$: Actions,
        private router: Router,
        private nifiCommon: NiFiCommon,
        private queueService: QueueService,
        private provenanceService: ProvenanceService,
        private store: Store<NiFiState>
    ) {}

    navigateToBundledContentViewer$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ContentActions.navigateToBundledContentViewer),
                map((action) => action.route),
                tap((route) => {
                    const extras: NavigationExtras = {
                        queryParamsHandling: 'preserve',
                        replaceUrl: true
                    };
                    const commands = route.split('/').filter((segment) => !this.nifiCommon.isBlank(segment));
                    this.router.navigate(commands, extras);
                })
            ),
        { dispatch: false }
    );

    loadSideBarData$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ContentActions.loadSideBarData),
            withLatestFrom(this.store.pipe(select(selectQueryParams))),
            switchMap(([, params]) => {
                if (params['eventId']) {
                    return from(
                        this.provenanceService.getProvenanceEvent(params['eventId'], params['clusterNodeId'])
                    ).pipe(
                        map((response) => {
                            return ContentActions.loadSideBarDataSuccess({
                                response: {
                                    data: {
                                        flowFile: undefined,
                                        provEvent: response.provenanceEvent
                                    }
                                }
                            });
                        }),
                        catchError(() =>
                            of(
                                ContentActions.loadSideBarDataSuccess({
                                    response: {
                                        data: {
                                            flowFile: undefined,
                                            provEvent: undefined
                                        }
                                    }
                                })
                            )
                        )
                    );
                } else {
                    return from(
                        this.queueService.getFlowFile({
                            filename: '',
                            lineageDuration: 0,
                            penalized: false,
                            penaltyExpiresIn: 0,
                            queuedDuration: 0,
                            size: 0,
                            uuid: '',
                            uri: params['uri'],
                            clusterNodeId: params['clusterNodeId']
                        })
                    ).pipe(
                        map((response) => {
                            return ContentActions.loadSideBarDataSuccess({
                                response: {
                                    data: {
                                        flowFile: response.flowFile,
                                        provEvent: undefined
                                    }
                                }
                            });
                        }),
                        catchError(() =>
                            of(
                                ContentActions.loadSideBarDataSuccess({
                                    response: {
                                        data: {
                                            flowFile: undefined,
                                            provEvent: undefined
                                        }
                                    }
                                })
                            )
                        )
                    );
                }
            })
        )
    );

    downloadContentWithFlowFile$ = createEffect(
        () => () =>
            this.actions$.pipe(
                ofType(ContentActions.downloadContentWithFlowFile),
                map((action) => action.request),
                tap((request) => this.queueService.downloadContent(request))
            ),
        { dispatch: false }
    );

    downloadContentWithEvent$ = createEffect(
        () => () =>
            this.actions$.pipe(
                ofType(ContentActions.downloadContentWithEvent),
                map((request) => {
                    this.provenanceService.downloadContent(request.eventId, request.direction, request.clusterNodeId);
                })
            ),
        { dispatch: false }
    );
}
