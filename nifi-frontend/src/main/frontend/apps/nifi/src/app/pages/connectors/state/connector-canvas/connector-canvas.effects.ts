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
import { ComponentType, ComponentTypeNamePipe } from '@nifi/shared';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { ErrorContextKey } from '../../../../state/error';
import { BackNavigation } from '../../../../state/navigation';
import * as ConnectorCanvasActions from './connector-canvas.actions';
import { SelectedComponent } from './connector-canvas.actions';
import {
    selectConnectorIdFromRoute,
    selectParentProcessGroupId,
    selectProcessGroupId,
    selectProcessGroupIdFromRoute
} from './connector-canvas.selectors';

@Injectable()
export class ConnectorCanvasEffects {
    private actions$ = inject(Actions);
    private store = inject(Store);
    private router = inject(Router);
    private connectorService = inject(ConnectorService);
    private errorHelper = inject(ErrorHelper);
    private componentTypeNamePipe = inject(ComponentTypeNamePipe);

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

    /**
     * Select components - updates route with selection
     * Routes to /connectors/:id/canvas/:processGroupId/:type/:componentId
     */
    selectComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.selectComponents),
            map((action) => action.request),
            concatLatestFrom(() => [
                this.store.select(selectConnectorIdFromRoute),
                this.store.select(selectProcessGroupIdFromRoute)
            ]),
            switchMap(([request, connectorId, processGroupId]) => {
                if (request.components.length === 0) {
                    return of(ConnectorCanvasActions.deselectAllComponents());
                }

                let commands: string[];
                if (request.components.length === 1) {
                    commands = [
                        '/connectors',
                        connectorId,
                        'canvas',
                        processGroupId,
                        request.components[0].componentType,
                        request.components[0].id
                    ];
                } else {
                    const ids: string[] = request.components.map(
                        (selectedComponent: SelectedComponent) => selectedComponent.id
                    );
                    commands = ['/connectors', connectorId, 'canvas', processGroupId, 'bulk', ids.join(',')];
                }
                return of(ConnectorCanvasActions.navigateWithoutTransform({ url: commands }));
            })
        )
    );

    /**
     * Deselect all components - returns to base canvas route
     */
    deselectAllComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.deselectAllComponents),
            concatLatestFrom(() => [
                this.store.select(selectConnectorIdFromRoute),
                this.store.select(selectProcessGroupIdFromRoute)
            ]),
            switchMap(([, connectorId, processGroupId]) => {
                return of(
                    ConnectorCanvasActions.navigateWithoutTransform({
                        url: ['/connectors', connectorId, 'canvas', processGroupId]
                    })
                );
            })
        )
    );

    /**
     * Navigate without transform - updates router
     */
    navigateWithoutTransform$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorCanvasActions.navigateWithoutTransform),
                tap((action) => {
                    this.router.navigate(action.url, {
                        replaceUrl: true
                    });
                })
            ),
        { dispatch: false }
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

    leaveProcessGroup$ = createEffect(() =>
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
                    map(() =>
                        ConnectorCanvasActions.navigateWithoutTransform({
                            url: [
                                '/connectors',
                                connectorId,
                                'canvas',
                                parentProcessGroupId!,
                                ComponentType.ProcessGroup,
                                childProcessGroupId!
                            ]
                        })
                    )
                );
            })
        )
    );

    navigateToProvenanceForComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ConnectorCanvasActions.navigateToProvenanceForComponent),
                map((action) => ({ id: action.id, componentType: action.componentType })),
                concatLatestFrom(() => [
                    this.store.select(selectConnectorIdFromRoute),
                    this.store.select(selectProcessGroupIdFromRoute)
                ]),
                tap(([{ id: componentId, componentType }, connectorId, processGroupId]) => {
                    this.router.navigate(['/provenance'], {
                        queryParams: { componentId },
                        state: {
                            backNavigation: {
                                route: [
                                    '/connectors',
                                    connectorId,
                                    'canvas',
                                    processGroupId,
                                    componentType,
                                    componentId
                                ],
                                routeBoundary: ['/provenance'],
                                context: this.componentTypeNamePipe.transform(componentType)
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );
}
