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
import * as d3 from 'd3';
import { CanvasUtils } from '../canvas-utils.service';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../state';
import { getDefaultsAndOpenNewConnectionDialog, selectComponents } from '../../state/flow/flow.actions';
import { ConnectionManager } from '../manager/connection-manager.service';
import { Position } from '../../state/shared';

@Injectable({
    providedIn: 'root'
})
export class ConnectableBehavior {
    private readonly connect: any;
    private origin: any;

    constructor(
        private store: Store<CanvasState>,
        private canvasUtils: CanvasUtils
    ) {
        const self: ConnectableBehavior = this;

        // dragging behavior for the connector
        this.connect = d3
            .drag()
            .subject(function (event) {
                self.origin = d3.pointer(event, d3.select('#canvas'));
                return {
                    x: self.origin[0],
                    y: self.origin[1]
                };
            })
            .on('start', function (this: any, event) {
                // stop further propagation
                event.sourceEvent.stopPropagation();

                const source: any = d3.select(this.parentNode);
                const sourceData: any = source.datum();

                // mark the source component has selected and unselect the previous components
                self.store.dispatch(
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
                d3.select(this).classed('dragging', true);

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
                    .attr('d', function (pathDatum: any) {
                        return 'M' + pathDatum.x + ' ' + pathDatum.y + 'L' + pathDatum.x + ' ' + pathDatum.y;
                    });

                // updates the location of the connection img
                d3.select(this).attr('transform', function () {
                    return 'translate(' + position[0] + ', ' + (position[1] + 20) + ')';
                });

                // re-append the image to keep it on top
                canvas.node().appendChild(this);
            })
            .on('drag', function (event) {
                const position = d3.pointer(event, d3.select('#canvas').node());

                // updates the location of the connection img
                d3.select(this).attr('transform', function () {
                    return 'translate(' + position[0] + ', ' + (position[1] + 50) + ')';
                });

                // mark node's connectable if supported
                const destination: any = d3.select('g.hover').classed('connectable-destination', function () {
                    // ensure the mouse has moved at least 10px in any direction, it seems that
                    // when the drag event is trigger is not consistent between browsers. as a result
                    // some browser would trigger when the mouse hadn't moved yet which caused
                    // click and contextmenu events to appear like an attempt to connection the
                    // component to itself. requiring the mouse to have actually moved before
                    // checking the eligibility of the destination addresses the issue
                    return (
                        (Math.abs(self.origin[0] - position[0]) > 10 || Math.abs(self.origin[1] - position[1]) > 10) &&
                        self.canvasUtils.isValidConnectionDestination(d3.select(this))
                    );
                });

                // update the drag line
                d3.select('path.connector')
                    .classed('connectable', function () {
                        if (destination.empty()) {
                            return false;
                        }

                        // if there is a potential destination, see if its connectable
                        return destination.classed('connectable-destination');
                    })
                    .attr('d', function (pathDatum: any) {
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
                                const end: Position = self.canvasUtils.getPerimeterPoint(pathDatum, {
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
            .on('end', function (this: any, event, d: any) {
                // stop further propagation
                event.sourceEvent.stopPropagation();

                // get the add connect img
                const addConnect: any = d3.select(this);

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
                        addConnect.classed('dragging', false).attr('transform', function () {
                            return 'translate(' + d.origX + ', ' + d.origY + ')';
                        });
                        source.node().appendChild(this);
                    }

                    // remove the connector
                    connector.remove();
                } else {
                    // remove the add connect img
                    addConnect.remove();

                    // create the connection
                    const destinationData = destination.datum();

                    self.store.dispatch(
                        getDefaultsAndOpenNewConnectionDialog({
                            request: {
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
                            }
                        })
                    );
                }
            });
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
        const self: ConnectableBehavior = this;

        components
            .classed('connectable', true)
            .on('mouseenter.connectable', function (this: any, event: MouseEvent, d: any) {
                if (self.allowConnection(event)) {
                    const selection: any = d3.select(this);

                    // ensure the current component supports connection source
                    if (self.canvasUtils.isValidConnectionSource(selection)) {
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
                                .call(self.connect);
                        }
                    }
                }
            })
            .on('mouseleave.connectable', function (this: any) {
                // conditionally remove the connector
                const addConnect = d3.select(this).select('text.add-connect');
                if (!addConnect.empty() && !addConnect.classed('dragging')) {
                    addConnect.remove();
                }
            })
            // Using mouseover/out to workaround chrome issue #122746
            .on('mouseover.connectable', function (this: any, event: MouseEvent) {
                // mark that we are hovering when appropriate
                d3.select(this).classed('hover', function () {
                    return self.allowConnection(event);
                });
            })
            .on('mouseout.connection', function (this: any) {
                // remove all hover related classes
                d3.select(this).classed('hover connectable-destination', false);
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
