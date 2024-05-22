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
import { Store } from '@ngrx/store';
import { CanvasState } from '../../state';
import { INITIAL_SCALE } from '../../state/transform/transform.reducer';
import { selectTransform } from '../../state/transform/transform.selectors';
import { CanvasUtils } from '../canvas-utils.service';
import { moveComponents, showOkDialog, updatePositions } from '../../state/flow/flow.actions';
import { Client } from '../../../../service/client.service';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { MoveComponentRequest, UpdateComponentRequest } from '../../state/flow';
import { Position } from '../../state/shared';
import { ComponentType } from '../../../../state/shared';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';

@Injectable({
    providedIn: 'root'
})
export class DraggableBehavior {
    private readonly drag: any;
    private snapEnabled = false;
    private snapAlignmentPixels = 8;

    private scale: number = INITIAL_SCALE;

    private updatePositionRequestId = 0;

    constructor(
        private store: Store<CanvasState>,
        private client: Client,
        private canvasUtils: CanvasUtils,
        private clusterConnectionService: ClusterConnectionService
    ) {
        const self: DraggableBehavior = this;

        // subscribe to scale updates
        this.store
            .select(selectTransform)
            .pipe(takeUntilDestroyed())
            .subscribe((transform) => {
                this.scale = transform.scale;
            });

        // handle component drag events
        this.drag = d3
            .drag()
            .on('start', function (event) {
                // stop further propagation
                event.sourceEvent.stopPropagation();
            })
            .on('drag', function (event) {
                const dragSelection = d3.select('rect.drag-selection');

                // lazily create the drag selection box
                if (dragSelection.empty()) {
                    // get the current selection
                    const selection = d3.selectAll('g.component.selected');

                    // determine the appropriate bounding box
                    let minX: number | null = null;
                    let maxX: number | null = null;
                    let minY: number | null = null;
                    let maxY: number | null = null;

                    selection.each(function (d: any) {
                        if (minX === null || d.position.x < minX) {
                            minX = d.position.x;
                        }
                        if (minY === null || d.position.y < minY) {
                            minY = d.position.y;
                        }
                        const componentMaxX = d.position.x + d.dimensions.width;
                        const componentMaxY = d.position.y + d.dimensions.height;
                        if (maxX === null || componentMaxX > maxX) {
                            maxX = componentMaxX;
                        }
                        if (maxY === null || componentMaxY > maxY) {
                            maxY = componentMaxY;
                        }
                    });

                    // create a selection box for the move
                    d3.select('#canvas')
                        .append('rect')
                        .attr('rx', 6)
                        .attr('ry', 6)
                        .attr('x', minX)
                        .attr('y', minY)
                        .attr('class', 'drag-selection')
                        .attr('pointer-events', 'none')
                        // @ts-ignore
                        .attr('width', maxX - minX)
                        // @ts-ignore
                        .attr('height', maxY - minY)
                        .attr('stroke-width', function () {
                            return 1 / self.scale;
                        })
                        .attr('stroke-dasharray', function () {
                            return 4 / self.scale;
                        })
                        .datum({
                            original: {
                                x: minX,
                                y: minY
                            },
                            x: minX,
                            y: minY
                        });
                } else {
                    // update the position of the drag selection
                    // snap align the position unless the user is holding shift
                    self.snapEnabled = !event.sourceEvent.shiftKey;
                    dragSelection
                        .attr('x', function (d: any) {
                            d.x += event.dx;
                            return self.snapEnabled
                                ? Math.round(d.x / self.snapAlignmentPixels) * self.snapAlignmentPixels
                                : d.x;
                        })
                        .attr('y', function (d: any) {
                            d.y += event.dy;
                            return self.snapEnabled
                                ? Math.round(d.y / self.snapAlignmentPixels) * self.snapAlignmentPixels
                                : d.y;
                        });
                }
            })
            .on('end', function (event) {
                // stop further propagation
                event.sourceEvent.stopPropagation();

                // get the drag selection
                const dragSelection: any = d3.select('rect.drag-selection');

                // ensure we found a drag selection
                if (dragSelection.empty()) {
                    return;
                }

                // get the destination group if applicable... remove the drop flag if necessary to prevent
                // subsequent drop events from triggering prior to this move's completion
                const group: any = d3.select('g.drop').classed('drop', false);

                // either move or update the selections group as appropriate
                if (group.empty()) {
                    self.updateComponentsPosition(dragSelection);
                } else {
                    self.updateComponentsGroup(group);
                }

                // remove the drag selection
                dragSelection.remove();
            });
    }

    /**
     * Updates the positioning of all selected components.
     *
     * @param {selection} dragSelection The current drag selection
     */
    private updateComponentsPosition(dragSelection: any): void {
        const componentUpdates: Map<string, UpdateComponentRequest> = new Map();
        const connectionUpdates: Map<string, UpdateComponentRequest> = new Map();

        // determine the drag delta
        const dragData = dragSelection.datum();
        const delta = {
            x: dragData.x - dragData.original.x,
            y: dragData.y - dragData.original.y
        };

        // if the component didn't move, return
        if (delta.x === 0 && delta.y === 0) {
            return;
        }

        const selectedConnections: d3.Selection<any, any, any, any> = d3.selectAll('g.connection.selected');
        const selectedComponents: d3.Selection<any, any, any, any> = d3.selectAll('g.component.selected');

        // ensure every component is writable
        if (!this.canvasUtils.canModify(selectedConnections) || !this.canvasUtils.canModify(selectedComponents)) {
            this.store.dispatch(
                showOkDialog({
                    title: 'Component Position',
                    message: 'Must be authorized to modify every component selected.'
                })
            );
            return;
        }

        // go through each selected connection
        selectedConnections.each((d) => {
            const connectionUpdate = this.updateConnectionPosition(d, delta);
            if (connectionUpdate) {
                connectionUpdates.set(d.id, connectionUpdate);
            }
        });

        // go through each selected component
        selectedComponents.each((d) => {
            // consider any self looping connections
            const componentConnections = this.canvasUtils.getComponentConnections(d.id);

            componentConnections.forEach((connection) => {
                if (!connectionUpdates.has(connection.id)) {
                    const sourceId = this.canvasUtils.getConnectionSourceComponentId(connection);
                    const destinationId = this.canvasUtils.getConnectionDestinationComponentId(connection);

                    if (sourceId === destinationId) {
                        const connectionUpdate = this.updateConnectionPosition(connection, delta);
                        if (connectionUpdate) {
                            connectionUpdates.set(connection.id, connectionUpdate);
                        }
                    }
                }
            });

            // consider the component itself
            componentUpdates.set(d.id, this.updateComponentPosition(d, delta));
        });

        // dispatch the position updates
        this.store.dispatch(
            updatePositions({
                request: {
                    requestId: this.updatePositionRequestId++,
                    componentUpdates: Array.from(componentUpdates.values()),
                    connectionUpdates: Array.from(connectionUpdates.values())
                }
            })
        );
    }

    /**
     * Updates the parent group of all selected components.
     *
     * @param group
     */
    private updateComponentsGroup(group: any): void {
        // get the selection and deselect the components being moved
        const selection: any = d3.selectAll('g.component.selected, g.connection.selected').classed('selected', false);

        if (!this.canvasUtils.canModify(selection)) {
            this.store.dispatch(
                showOkDialog({
                    title: 'Component Position',
                    message: 'Must be authorized to modify every component selected.'
                })
            );
            return;
        }
        if (!this.canvasUtils.canModify(group)) {
            this.store.dispatch(
                showOkDialog({
                    title: 'Component Position',
                    message: 'Not authorized to modify the destination group.'
                })
            );
            return;
        }

        const groupData: any = group.datum();
        const components: MoveComponentRequest[] = [];
        selection.each(function (d: any) {
            components.push({
                id: d.id,
                type: d.type,
                uri: d.uri,
                entity: d
            });
        });

        // move the selection into the group
        this.store.dispatch(
            moveComponents({
                request: {
                    components,
                    groupId: groupData.id
                }
            })
        );
    }

    updateComponentPosition(d: any, delta: Position): UpdateComponentRequest {
        const newPosition = {
            x: this.snapEnabled
                ? Math.round((d.position.x + delta.x) / this.snapAlignmentPixels) * this.snapAlignmentPixels
                : d.position.x + delta.x,
            y: this.snapEnabled
                ? Math.round((d.position.y + delta.y) / this.snapAlignmentPixels) * this.snapAlignmentPixels
                : d.position.y + delta.y
        };

        return {
            id: d.id,
            type: d.type,
            uri: d.uri,
            payload: {
                revision: this.client.getRevision(d),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                component: {
                    id: d.id,
                    position: newPosition
                }
            },
            restoreOnFailure: {
                position: d.position
            },
            errorStrategy: 'snackbar'
        };
    }

    /**
     * Update the connection's position
     *
     * @param connection     The connection
     * @param delta The change in position
     * @returns {*}
     */
    updateConnectionPosition(connection: any, delta: any): UpdateComponentRequest | null {
        // only update if necessary
        if (connection.bends.length === 0) {
            return null;
        }

        // calculate the new bend points
        const newBends: Position[] = connection.bends.map((bend: any) => {
            return {
                x: bend.x + delta.x,
                y: bend.y + delta.y
            };
        });

        return {
            id: connection.id,
            type: ComponentType.Connection,
            uri: connection.uri,
            payload: {
                revision: this.client.getRevision(connection),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                component: {
                    id: connection.id,
                    bends: newBends
                }
            },
            restoreOnFailure: {
                bends: connection.bends
            },
            errorStrategy: 'snackbar'
        };
    }

    public activate(components: any): void {
        components.classed('moveable', true).call(this.drag);
    }

    public deactivate(components: any): void {
        components.classed('moveable', false).on('.drag', null);
    }
}
