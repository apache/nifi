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

import { Component, OnDestroy, OnInit, ViewContainerRef } from '@angular/core';
import { CanvasState } from '../../state';
import { Position } from '../../state/shared';
import { Store } from '@ngrx/store';
import {
    centerSelectedComponents,
    deselectAllComponents,
    editComponent,
    editCurrentProcessGroup,
    loadProcessGroup,
    resetFlowState,
    selectComponents,
    setSkipTransform,
    startProcessGroupPolling,
    stopProcessGroupPolling
} from '../../state/flow/flow.actions';
import * as d3 from 'd3';
import { CanvasView } from '../../service/canvas-view.service';
import { INITIAL_SCALE, INITIAL_TRANSLATE } from '../../state/transform/transform.reducer';
import { selectTransform } from '../../state/transform/transform.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { SelectedComponent } from '../../state/flow';
import {
    selectAllowTransition,
    selectBulkSelectedComponentIds,
    selectConnection,
    selectCurrentProcessGroupId,
    selectEditedCurrentProcessGroup,
    selectFunnel,
    selectInputPort,
    selectLabel,
    selectOutputPort,
    selectProcessGroup,
    selectProcessGroupIdFromRoute,
    selectProcessGroupRoute,
    selectProcessor,
    selectRemoteProcessGroup,
    selectSingleEditedComponent,
    selectSingleSelectedComponent,
    selectSkipTransform,
    selectViewStatusHistoryComponent
} from '../../state/flow/flow.selectors';
import { filter, map, switchMap, take } from 'rxjs';
import { restoreViewport } from '../../state/transform/transform.actions';
import { ComponentType, isDefinedAndNotNull } from '../../../../state/shared';
import { initialState } from '../../state/flow/flow.reducer';
import { CanvasContextMenu } from '../../service/canvas-context-menu.service';
import { getStatusHistoryAndOpenDialog } from '../../../../state/status-history/status-history.actions';
import { loadFlowConfiguration } from '../../../../state/flow-configuration/flow-configuration.actions';
import { concatLatestFrom } from '@ngrx/effects';
import { selectUrl } from '../../../../state/router/router.selectors';
import { Storage } from '../../../../service/storage.service';
import {
    loadClusterSummary,
    startClusterSummaryPolling,
    stopClusterSummaryPolling
} from '../../../../state/cluster-summary/cluster-summary.actions';

@Component({
    selector: 'fd-canvas',
    templateUrl: './canvas.component.html',
    styleUrls: ['./canvas.component.scss']
})
export class Canvas implements OnInit, OnDestroy {
    private svg: any;
    private canvas: any;

    private scale: number = INITIAL_SCALE;
    private canvasClicked = false;

    constructor(
        private viewContainerRef: ViewContainerRef,
        private store: Store<CanvasState>,
        private canvasView: CanvasView,
        private storage: Storage,
        public canvasContextMenu: CanvasContextMenu
    ) {
        this.store
            .select(selectTransform)
            .pipe(takeUntilDestroyed())
            .subscribe((transform) => {
                this.scale = transform.scale;
            });

        this.store
            .select(selectUrl)
            .pipe(takeUntilDestroyed())
            .subscribe((route) => {
                this.storage.setItem('current-canvas-route', route);
            });

        // load the process group from the route
        this.store
            .select(selectProcessGroupIdFromRoute)
            .pipe(
                filter((processGroupId) => processGroupId != null),
                takeUntilDestroyed()
            )
            .subscribe((processGroupId) => {
                this.store.dispatch(
                    loadProcessGroup({
                        request: {
                            id: processGroupId,
                            transitionRequired: false
                        }
                    })
                );
            });

        // handle process group loading and viewport restoration
        this.store
            .select(selectCurrentProcessGroupId)
            .pipe(
                filter((processGroupId) => processGroupId != initialState.id),
                switchMap(() => this.store.select(selectProcessGroupRoute)),
                filter((processGroupRoute) => processGroupRoute != null),
                concatLatestFrom(() => this.store.select(selectSkipTransform)),
                takeUntilDestroyed()
            )
            .subscribe(([, skipTransform]) => {
                if (skipTransform) {
                    this.store.dispatch(setSkipTransform({ skipTransform: false }));
                } else {
                    this.store.dispatch(restoreViewport());
                }
            });

        // handle single component selection
        this.store
            .select(selectCurrentProcessGroupId)
            .pipe(
                filter((processGroupId) => processGroupId != initialState.id),
                switchMap(() => this.store.select(selectSingleSelectedComponent)),
                filter((selectedComponent) => selectedComponent != null),
                concatLatestFrom(() => [
                    this.store.select(selectSkipTransform),
                    this.store.select(selectAllowTransition)
                ]),
                takeUntilDestroyed()
            )
            .subscribe(([, skipTransform, allowTransition]) => {
                if (skipTransform) {
                    this.store.dispatch(setSkipTransform({ skipTransform: false }));
                } else {
                    this.store.dispatch(centerSelectedComponents({ request: { allowTransition } }));
                }
            });

        // handle bulk component selection
        this.store
            .select(selectCurrentProcessGroupId)
            .pipe(
                filter((processGroupId) => processGroupId != initialState.id),
                switchMap(() => this.store.select(selectBulkSelectedComponentIds)),
                filter((ids) => ids.length > 0),
                concatLatestFrom(() => [
                    this.store.select(selectSkipTransform),
                    this.store.select(selectAllowTransition)
                ]),
                takeUntilDestroyed()
            )
            .subscribe(([, skipTransform, allowTransition]) => {
                if (skipTransform) {
                    this.store.dispatch(setSkipTransform({ skipTransform: false }));
                } else {
                    this.store.dispatch(centerSelectedComponents({ request: { allowTransition } }));
                }
            });

        // handle component edit
        this.store
            .select(selectCurrentProcessGroupId)
            .pipe(
                filter((processGroupId) => processGroupId != initialState.id),
                switchMap(() => this.store.select(selectSingleEditedComponent)),
                isDefinedAndNotNull(),
                switchMap((selectedComponent) => {
                    let component$;
                    switch (selectedComponent.componentType) {
                        case ComponentType.Processor:
                            component$ = this.store.select(selectProcessor(selectedComponent.id));
                            break;
                        case ComponentType.InputPort:
                            component$ = this.store.select(selectInputPort(selectedComponent.id));
                            break;
                        case ComponentType.OutputPort:
                            component$ = this.store.select(selectOutputPort(selectedComponent.id));
                            break;
                        case ComponentType.ProcessGroup:
                            component$ = this.store.select(selectProcessGroup(selectedComponent.id));
                            break;
                        case ComponentType.RemoteProcessGroup:
                            component$ = this.store.select(selectRemoteProcessGroup(selectedComponent.id));
                            break;
                        case ComponentType.Connection:
                            component$ = this.store.select(selectConnection(selectedComponent.id));
                            break;
                        case ComponentType.Funnel:
                            component$ = this.store.select(selectFunnel(selectedComponent.id));
                            break;
                        case ComponentType.Label:
                            component$ = this.store.select(selectLabel(selectedComponent.id));
                            break;
                        default:
                            throw 'Unrecognized Component Type';
                    }

                    // combine the original selection with the component
                    return component$.pipe(
                        filter((component) => component != null),
                        take(1),
                        map((component) => [selectedComponent, component])
                    );
                }),
                takeUntilDestroyed()
            )
            .subscribe(([selectedComponent, component]) => {
                // initiate the edit request
                this.store.dispatch(
                    editComponent({
                        request: {
                            type: selectedComponent.componentType,
                            uri: component.uri,
                            entity: component
                        }
                    })
                );
            });

        // edit the current process group from the route
        this.store
            .select(selectEditedCurrentProcessGroup)
            .pipe(
                filter((processGroupId) => processGroupId != null),
                takeUntilDestroyed()
            )
            .subscribe((processGroupId) => {
                this.store.dispatch(
                    editCurrentProcessGroup({
                        request: {
                            id: processGroupId
                        }
                    })
                );
            });

        this.store
            .select(selectCurrentProcessGroupId)
            .pipe(
                filter((processGroupId) => processGroupId != initialState.id),
                switchMap(() => this.store.select(selectViewStatusHistoryComponent)),
                filter((selectedComponent) => selectedComponent != null),
                takeUntilDestroyed()
            )
            .subscribe((component) => {
                if (component) {
                    this.store.dispatch(
                        getStatusHistoryAndOpenDialog({
                            request: {
                                source: 'canvas',
                                componentType: component.componentType,
                                componentId: component.id
                            }
                        })
                    );
                }
            });
    }

    ngOnInit(): void {
        // initialize the canvas svg
        this.createSvg();
        this.canvasView.init(this.viewContainerRef, this.svg, this.canvas);

        this.store.dispatch(loadFlowConfiguration());
        this.store.dispatch(loadClusterSummary());
        this.store.dispatch(startProcessGroupPolling());
        this.store.dispatch(startClusterSummaryPolling());
    }

    private createSvg(): void {
        const self: Canvas = this;

        this.svg = d3
            .select('#canvas-container')
            .append('svg')
            .attr('class', 'canvas-svg')
            .on('contextmenu', function (event) {
                // reset the canvas click flag
                self.canvasClicked = false;

                // if this context menu click was on the canvas (and not a nested
                // element) we need to clear the selection
                if (event.target === self.svg.node()) {
                    self.store.dispatch(deselectAllComponents());
                }
            });

        this.createDefs();

        // initialize the canvas element
        this.initCanvas();
    }

    private createDefs(): void {
        // create the definitions element
        const defs = this.svg.append('defs');

        // create arrow definitions for the various line types
        defs.selectAll('marker')
            .data(['normal', 'ghost', 'unauthorized', 'full'])
            .enter()
            .append('marker')
            .attr('id', function (d: string) {
                return d;
            })
            .attr('viewBox', '0 0 6 6')
            .attr('refX', 5)
            .attr('refY', 3)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .attr('class', function (d: string) {
                if (d === 'ghost') {
                    return 'ghost nifi-surface-default';
                } else if (d === 'unauthorized') {
                    return 'unauthorized nifi-warn-darker';
                } else if (d === 'full') {
                    return 'full nifi-warn-darker';
                } else {
                    return 'on-surface-default';
                }
            })
            .append('path')
            .attr('d', 'M2,3 L0,6 L6,3 L0,0 z');

        // filter for drop shadow
        const componentDropShadowFilter = defs
            .append('filter')
            .attr('id', 'component-drop-shadow')
            .attr('height', '140%')
            .attr('y', '-20%');

        // blur
        componentDropShadowFilter
            .append('feGaussianBlur')
            .attr('in', 'SourceAlpha')
            .attr('stdDeviation', 3)
            .attr('result', 'blur');

        // offset
        componentDropShadowFilter
            .append('feOffset')
            .attr('in', 'blur')
            .attr('dx', 0)
            .attr('dy', 1)
            .attr('result', 'offsetBlur');

        // color/opacity
        componentDropShadowFilter.append('feFlood').attr('flood-opacity', 0.4).attr('result', 'offsetColor');

        // combine
        componentDropShadowFilter
            .append('feComposite')
            .attr('in', 'offsetColor')
            .attr('in2', 'offsetBlur')
            .attr('operator', 'in')
            .attr('result', 'offsetColorBlur');

        // stack the effect under the source graph
        const componentDropShadowFeMerge = componentDropShadowFilter.append('feMerge');
        componentDropShadowFeMerge.append('feMergeNode').attr('in', 'offsetColorBlur');
        componentDropShadowFeMerge.append('feMergeNode').attr('in', 'SourceGraphic');

        // filter for drop shadow
        const connectionFullDropShadowFilter = defs
            .append('filter')
            .attr('id', 'connection-full-drop-shadow')
            .attr('height', '140%')
            .attr('y', '-20%');

        // blur
        connectionFullDropShadowFilter
            .append('feGaussianBlur')
            .attr('in', 'SourceAlpha')
            .attr('stdDeviation', 3)
            .attr('result', 'blur');

        // offset
        connectionFullDropShadowFilter
            .append('feOffset')
            .attr('in', 'blur')
            .attr('dx', 0)
            .attr('dy', 1)
            .attr('result', 'offsetBlur');

        // color/opacity
        connectionFullDropShadowFilter.append('feFlood').attr('flood-opacity', 1).attr('result', 'offsetColor');

        // combine
        connectionFullDropShadowFilter
            .append('feComposite')
            .attr('in', 'offsetColor')
            .attr('in2', 'offsetBlur')
            .attr('operator', 'in')
            .attr('result', 'offsetColorBlur');

        // stack the effect under the source graph
        const connectionFullFeMerge = connectionFullDropShadowFilter.append('feMerge');
        connectionFullFeMerge.append('feMergeNode').attr('in', 'offsetColorBlur');
        connectionFullFeMerge.append('feMergeNode').attr('in', 'SourceGraphic');
    }

    private initCanvas(): void {
        const self: Canvas = this;

        const t = [INITIAL_TRANSLATE.x, INITIAL_TRANSLATE.y];
        this.canvas = this.svg
            .append('g')
            .attr('transform', 'translate(' + t + ') scale(' + INITIAL_SCALE + ')')
            .attr('pointer-events', 'all')
            .attr('id', 'canvas');

        // handle canvas events
        this.svg
            .on('mousedown.selection', function (event: MouseEvent) {
                self.canvasClicked = true;

                if (event.button !== 0) {
                    // prevent further propagation (to parents and others handlers
                    // on the same element to prevent zoom behavior)
                    event.stopImmediatePropagation();
                    return;
                }

                // show selection box if shift is held down
                if (event.shiftKey) {
                    const position: any = d3.pointer(event, self.canvas.node());
                    self.canvas
                        .append('rect')
                        .attr('rx', 6)
                        .attr('ry', 6)
                        .attr('x', position[0])
                        .attr('y', position[1])
                        .attr('class', 'component-selection')
                        .attr('width', 0)
                        .attr('height', 0)
                        .attr('stroke-width', function () {
                            return 1 / self.scale;
                        })
                        .attr('stroke-dasharray', function () {
                            return 4 / self.scale;
                        })
                        .datum(position);

                    // prevent further propagation (to parents and others handlers
                    // on the same element to prevent zoom behavior)
                    event.stopImmediatePropagation();

                    // prevents the browser from changing to a text selection cursor
                    event.preventDefault();
                }
            })
            .on('mousemove.selection', function (event: MouseEvent) {
                // update selection box if shift is held down
                if (event.shiftKey) {
                    // get the selection box
                    const selectionBox: any = d3.select('rect.component-selection');
                    if (!selectionBox.empty()) {
                        // get the original position
                        const originalPosition: any = selectionBox.datum();
                        const position: any = d3.pointer(event, self.canvas.node());

                        const d: any = {};
                        if (originalPosition[0] < position[0]) {
                            d.x = originalPosition[0];
                            d.width = position[0] - originalPosition[0];
                        } else {
                            d.x = position[0];
                            d.width = originalPosition[0] - position[0];
                        }

                        if (originalPosition[1] < position[1]) {
                            d.y = originalPosition[1];
                            d.height = position[1] - originalPosition[1];
                        } else {
                            d.y = position[1];
                            d.height = originalPosition[1] - position[1];
                        }

                        // update the selection box
                        selectionBox.attr('width', d.width).attr('height', d.height).attr('x', d.x).attr('y', d.y);

                        // prevent further propagation (to parents)
                        event.stopPropagation();
                    }
                }
            })
            .on('mouseup.selection', function (this: any) {
                // ensure this originated from clicking the canvas, not a component.
                // when clicking on a component, the event propagation is stopped so
                // it never reaches the canvas. we cannot do this however on up events
                // since the drag events break down
                if (!self.canvasClicked) {
                    return;
                }

                // reset the canvas click flag
                self.canvasClicked = false;

                // get the selection box
                const selectionBox: any = d3.select('rect.component-selection');
                if (!selectionBox.empty()) {
                    const selection: SelectedComponent[] = [];

                    const selectionBoundingBox: any = {
                        x: parseInt(selectionBox.attr('x'), 10),
                        y: parseInt(selectionBox.attr('y'), 10),
                        width: parseInt(selectionBox.attr('width'), 10),
                        height: parseInt(selectionBox.attr('height'), 10)
                    };

                    // see if a component should be selected or not
                    d3.selectAll('g.component').each(function (d: any) {
                        // consider it selected if its already selected or enclosed in the bounding box
                        if (
                            d3.select(this).classed('selected') ||
                            (d.position.x >= selectionBoundingBox.x &&
                                d.position.x + d.dimensions.width <=
                                    selectionBoundingBox.x + selectionBoundingBox.width &&
                                d.position.y >= selectionBoundingBox.y &&
                                d.position.y + d.dimensions.height <=
                                    selectionBoundingBox.y + selectionBoundingBox.height)
                        ) {
                            selection.push({
                                id: d.id,
                                componentType: d.type
                            });
                        }
                    });

                    // see if a connection should be selected or not
                    d3.selectAll('g.connection').each(function (d: any) {
                        // consider all points
                        const points: Position[] = [d.start].concat(d.bends, [d.end]);

                        // determine the bounding box
                        const x: any = d3.extent(points, function (pt: Position) {
                            return pt.x;
                        });
                        const y: any = d3.extent(points, function (pt: Position) {
                            return pt.y;
                        });

                        // consider it selected if its already selected or enclosed in the bounding box
                        if (
                            d3.select(this).classed('selected') ||
                            (x[0] >= selectionBoundingBox.x &&
                                x[1] <= selectionBoundingBox.x + selectionBoundingBox.width &&
                                y[0] >= selectionBoundingBox.y &&
                                y[1] <= selectionBoundingBox.y + selectionBoundingBox.height)
                        ) {
                            selection.push({
                                id: d.id,
                                componentType: d.type
                            });
                        }
                    });

                    // dispatch the selected components
                    self.store.dispatch(
                        selectComponents({
                            request: {
                                components: selection
                            }
                        })
                    );

                    // remove the selection box
                    selectionBox.remove();
                }
            });
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetFlowState());
        this.store.dispatch(stopProcessGroupPolling());
        this.store.dispatch(stopClusterSummaryPolling());
    }
}
