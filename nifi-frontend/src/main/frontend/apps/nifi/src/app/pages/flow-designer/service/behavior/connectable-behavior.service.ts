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
import * as d3 from 'd3';
import { CanvasUtils } from '../canvas-utils.service';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../state';
import { openNewConnectionDialog, selectComponents } from '../../state/flow/flow.actions';
import { ConnectionManager } from '../manager/connection-manager.service';
import { Position } from '../../state/shared';
import { CreateConnectionRequest } from '../../state/flow';

@Injectable({
    providedIn: 'root'
})
export class ConnectableBehavior {
    private store = inject<Store<CanvasState>>(Store);
    private canvasUtils = inject(CanvasUtils);

    private readonly connect: any;
    private origin: any;

    constructor() {
        // dragging behavior for the connector
        this.connect = d3
            .drag()
            .subject(function (this: Element, event: any) {
                const origin = d3.pointer(event, d3.select('#canvas'));
                return {
                    x: origin[0],
                    y: origin[1],
                    element: this,
                    origin
                };
            })
            .on('start', (event) => {
                // stop further propagation
                event.sourceEvent.stopPropagation();

                const source: any = d3.select((event.subject.element as Element).parentNode as Element);
                const sourceData: any = source.datum();

                // mark the source component has selected and unselect the previous components
                this.store.dispatch(
                    selectComponents({
                        request: {
                            components: [
                                {
                                    id: sourceData.id,
                                    componentType: sourceData.type
                                }
                            ]
                        }
                    })
                );

                // mark this component as dragging and selected
                d3.select(event.subject.element as Element).classed('dragging', true);

                const canvas: any = d3.select('#canvas');
                const position = d3.pointer(event, canvas.node());

                // start the drag line and insert it first to keep it on the bottom
                canvas
                    .insert('path', ':first-child')
                    .datum({
                        sourceId: sourceData.id,
                        sourceWidth: sourceData.dimensions.width,
                        x: sourceData.position.x + sourceData.dimensions.width / 2,
                        y: sourceData.position.y + sourceData.dimensions.height / 2
                    })
                    .attr('class', 'connector')
                    .attr(
                        'd',
                        (pathDatum: any) =>
                            'M' + pathDatum.x + ' ' + pathDatum.y + 'L' + pathDatum.x + ' ' + pathDatum.y
                    );

                // updates the location of the connection img
                d3.select(event.subject.element as Element).attr(
                    'transform',
                    () => 'translate(' + position[0] + ', ' + (position[1] + 20) + ')'
                );

                // re-append the image to keep it on top
                canvas.node().appendChild(event.subject.element);
            })
            .on('drag', (event) => {
                const position = d3.pointer(event, d3.select('#canvas').node());
                const origin = event.subject.origin;
                const canvasUtils = this.canvasUtils;

                // updates the location of the connection img
                d3.select(event.subject.element as Element).attr(
                    'transform',
                    () => 'translate(' + position[0] + ', ' + (position[1] + 50) + ')'
                );

                // mark node's connectable if supported
                // Uses function() so D3 binds 'this' to the DOM element for isValidConnectionDestination check
                const destination: any = d3.select('g.hover').classed('connectable-destination', function () {
                    // ensure the mouse has moved at least 10px in any direction, it seems that
                    // when the drag event is trigger is not consistent between browsers. as a result
                    // some browser would trigger when the mouse hadn't moved yet which caused
                    // click and contextmenu events to appear like an attempt to connection the
                    // component to itself. requiring the mouse to have actually moved before
                    // checking the eligibility of the destination addresses the issue
                    return (
                        (Math.abs(origin[0] - position[0]) > 10 || Math.abs(origin[1] - position[1]) > 10) &&
                        canvasUtils.isValidConnectionDestination(d3.select(this))
                    );
                });

                // update the drag line
                d3.select('path.connector')
                    .classed('connectable', () => {
                        if (destination.empty()) {
                            return false;
                        }

                        // if there is a potential destination, see if its connectable
                        return destination.classed('connectable-destination');
                    })
                    .attr('d', (pathDatum: any) => {
                        if (!destination.empty() && destination.classed('connectable-destination')) {
                            const destinationData: any = destination.datum();

                            // show the line preview as appropriate
                            if (pathDatum.sourceId === destinationData.id) {
                                const x: number = pathDatum.x;
                                const y: number = pathDatum.y;
                                const componentOffset: number = pathDatum.sourceWidth / 2 - 50;
                                const xOffset: number = ConnectionManager.SELF_LOOP_X_OFFSET;
                                const yOffset: number = ConnectionManager.SELF_LOOP_Y_OFFSET;

                                return (
                                    'M' +
                                    (x + componentOffset) +
                                    ' ' +
                                    y +
                                    'L' +
                                    (x + componentOffset + xOffset) +
                                    ' ' +
                                    (y - yOffset) +
                                    'L' +
                                    (x + componentOffset + xOffset) +
                                    ' ' +
                                    (y + yOffset) +
                                    'Z'
                                );
                            } else {
                                // get the position on the destination perimeter
                                const end: Position = this.canvasUtils.getPerimeterPoint(pathDatum, {
                                    x: destinationData.position.x,
                                    y: destinationData.position.y,
                                    width: destinationData.dimensions.width,
                                    height: destinationData.dimensions.height
                                });

                                // direct line between components to provide a 'snap feel'
                                return 'M' + pathDatum.x + ' ' + pathDatum.y + 'L' + end.x + ' ' + end.y;
                            }
                        } else {
                            return 'M' + pathDatum.x + ' ' + pathDatum.y + 'L' + position[0] + ' ' + position[1];
                        }
                    });
            })
            .on('end', (event) => {
                // stop further propagation
                event.sourceEvent.stopPropagation();

                const subject = event.subject;
                const addConnectElement = subject.element as Element;

                // get the add connect img
                const addConnect: any = d3.select(addConnectElement);

                // get the connector, if the current point is not over a new destination
                // the connector will be removed. otherwise it will be removed after the
                // connection has been configured/cancelled
                const connector: any = d3.select('path.connector');
                const connectorData: any = connector.datum();

                // get the source
                const source: any = d3.select('#id-' + connectorData.sourceId);
                const sourceData: any = source.datum();

                // get the destination
                const destination: any = d3.select('g.connectable-destination');

                // we are not over a new destination
                if (destination.empty()) {
                    // get the mouse position relative to the source
                    const position: any = d3.pointer(event, source.node());

                    // if the position is outside the component, remove the add connect img
                    if (
                        position[0] < 0 ||
                        position[0] > sourceData.dimensions.width ||
                        position[1] < 0 ||
                        position[1] > sourceData.dimensions.height
                    ) {
                        addConnect.remove();
                    } else {
                        // reset the add connect img by restoring the position and place in the DOM
                        const addConnectDatum = addConnect.datum() as { origX: number; origY: number };
                        addConnect
                            .classed('dragging', false)
                            .attr(
                                'transform',
                                () => 'translate(' + addConnectDatum.origX + ', ' + addConnectDatum.origY + ')'
                            );
                        source.node().appendChild(addConnectElement);
                    }

                    // remove the connector
                    connector.remove();
                } else {
                    // remove the add connect img
                    addConnect.remove();

                    // create the connection
                    const destinationData = destination.datum();

                    // prepare the connection request
                    const request: CreateConnectionRequest = {
                        source: {
                            id: sourceData.id,
                            componentType: sourceData.type,
                            entity: sourceData
                        },
                        destination: {
                            id: destinationData.id,
                            componentType: destinationData.type,
                            entity: destinationData
                        }
                    };

                    // add initial bend points if necessary
                    const bends: Position[] = this.calculateInitialBendPoints(sourceData, destinationData);
                    if (bends) {
                        request.bends = bends;
                    }

                    this.store.dispatch(
                        openNewConnectionDialog({
                            request
                        })
                    );
                }
            });
    }

    private calculateInitialBendPoints(sourceData: any, destinationData: any): Position[] {
        return this.canvasUtils.calculateBendPointsForCollisionAvoidance(sourceData, destinationData);
    }

    /**
     * Determines if we want to allow adding connections in the current state:
     *
     * 1) When shift is down, we could be adding components to the current selection.
     * 2) When the selection box is visible, we are in the process of moving all the
     * components currently selected.
     * 3) When the drag selection box is visible, we are in the process or selecting components
     * using the selection box.
     *
     * @returns {boolean}
     */
    private allowConnection(event: MouseEvent): boolean {
        return (
            !event.shiftKey && d3.select('rect.drag-selection').empty() && d3.select('rect.component-selection').empty()
        );
    }

    public activate(components: any): void {
        components
            .classed('connectable', true)
            .on('mouseenter.connectable', (event: MouseEvent, d: any) => {
                if (this.allowConnection(event)) {
                    const selection: any = d3.select(event.currentTarget as Element);

                    // ensure the current component supports connection source
                    if (this.canvasUtils.isValidConnectionSource(selection)) {
                        // see if there's already a connector rendered
                        const addConnect: any = d3.select('text.add-connect');
                        if (addConnect.empty()) {
                            const x: number = d.dimensions.width / 2 - 14;
                            const y: number = d.dimensions.height / 2 + 14;

                            selection
                                .append('text')
                                .attr('class', 'add-connect')
                                .attr('transform', 'translate(' + x + ', ' + y + ')')
                                .text('\ue834')
                                .datum({
                                    origX: x,
                                    origY: y
                                })
                                .call(this.connect);
                        }
                    }
                }
            })
            .on('mouseleave.connectable', (event: MouseEvent) => {
                // conditionally remove the connector
                const addConnect = d3.select(event.currentTarget as Element).select('text.add-connect');
                if (!addConnect.empty() && !addConnect.classed('dragging')) {
                    addConnect.remove();
                }
            })
            // Using mouseover/out to workaround chrome issue #122746
            .on('mouseover.connectable', (event: MouseEvent) => {
                // mark that we are hovering when appropriate
                d3.select(event.currentTarget as Element).classed('hover', () => this.allowConnection(event));
            })
            .on('mouseout.connection', (event: MouseEvent) => {
                // remove all hover related classes
                d3.select(event.currentTarget as Element).classed('hover connectable-destination', false);
            });
    }

    public deactivate(components: any): void {
        components
            .classed('connectable', false)
            .on('mouseenter.connectable', null)
            .on('mouseleave.connectable', null)
            .on('mouseover.connectable', null)
            .on('mouseout.connectable', null);
    }
}
