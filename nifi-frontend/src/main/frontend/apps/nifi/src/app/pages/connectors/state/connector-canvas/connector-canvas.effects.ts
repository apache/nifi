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
import { concatLatestFrom } from '@ngrx/operators';
import { Store } from '@ngrx/store';
import { Router } from '@angular/router';
import { of } from 'rxjs';
import { catchError, filter, map, switchMap, take, tap } from 'rxjs/operators';
import { ComponentType } from '@nifi/shared';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { ErrorContextKey } from '../../../../state/error';
import * as ConnectorCanvasActions from './connector-canvas.actions';
import {
    selectConnectorIdFromRoute,
    selectParentProcessGroupId,
    selectProcessGroupId
} from './connector-canvas.selectors';

@Injectable()
export class ConnectorCanvasEffects {
    private actions$ = inject(Actions);
    private store = inject(Store);
    private router = inject(Router);
    private connectorService = inject(ConnectorService);
    private errorHelper = inject(ErrorHelper);

    loadConnectorFlow$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.loadConnectorFlow),
            switchMap((action) =>
                this.connectorService.getConnectorFlow(action.connectorId, action.processGroupId).pipe(
                    map((flowResponse) => {
                        const processGroupFlow = flowResponse.processGroupFlow;
                        const flow = processGroupFlow?.flow;
                        const labels = flow?.labels || [];
                        const funnels = flow?.funnels || [];
                        const inputPorts = flow?.inputPorts || [];
                        const outputPorts = flow?.outputPorts || [];
                        const remoteProcessGroups = flow?.remoteProcessGroups || [];
                        const processGroups = flow?.processGroups || [];
                        const processors = flow?.processors || [];
                        const connections = flow?.connections || [];

                        // Extract process group IDs for navigation
                        const processGroupId = processGroupFlow?.id ?? null;
                        const parentProcessGroupId = processGroupFlow?.parentGroupId ?? null;

                        // Extract breadcrumb for navigation trail
                        const breadcrumb = processGroupFlow?.breadcrumb ?? null;

                        return ConnectorCanvasActions.loadConnectorFlowSuccess({
                            connectorId: action.connectorId,
                            processGroupId,
                            parentProcessGroupId,
                            breadcrumb,
                            labels,
                            funnels,
                            inputPorts,
                            outputPorts,
                            remoteProcessGroups,
                            processGroups,
                            processors,
                            connections
                        });
                    }),
                    catchError((error) => {
                        return of(
                            ConnectorCanvasActions.loadConnectorFlowFailure({
                                errorContext: {
                                    errors: [this.errorHelper.getErrorString(error)],
                                    context: ErrorContextKey.CONNECTORS
                                }
                            })
                        );
                    })
                )
            )
        )
    );

    loadConnectorFlowComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.loadConnectorFlowSuccess),
            map(() => ConnectorCanvasActions.loadConnectorFlowComplete())
        )
    );

    enterProcessGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorCanvasActions.enterProcessGroup),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectConnectorIdFromRoute)),
                tap(([request, connectorId]) => {
                    this.router.navigate(['/connectors', connectorId, 'canvas', request.id]);
                })
            ),
        { dispatch: false }
    );

    leaveProcessGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorCanvasActions.leaveProcessGroup),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectParentProcessGroupId),
                    this.store.select(selectProcessGroupId)
                ]),
                filter(
                    ([, , parentProcessGroupId, currentProcessGroupId]) =>
                        parentProcessGroupId != null && currentProcessGroupId != null
                ),
                switchMap(([, connectorId, parentProcessGroupId, childProcessGroupId]) => {
                    this.router.navigate(['/connectors', connectorId, 'canvas', parentProcessGroupId]);
                    return this.actions$.pipe(
                        ofType(ConnectorCanvasActions.loadConnectorFlowComplete),
                        take(1),
                        tap(() => {
                            this.store.dispatch(ConnectorCanvasActions.setSkipTransform({ skipTransform: true }));
                            this.router.navigate(
                                [
                                    '/connectors',
                                    connectorId!,
                                    'canvas',
                                    parentProcessGroupId!,
                                    ComponentType.ProcessGroup,
                                    childProcessGroupId!
                                ],
                                { replaceUrl: true }
                            );
                        })
                    );
                })
            ),
        { dispatch: false }
    );
}
