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

import { DestroyRef, inject, Injectable, ViewContainerRef } from '@angular/core';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../state';
import { CanvasUtils } from '../canvas-utils.service';
import { Client } from '../../../../service/client.service';
import { SelectableBehavior } from '../behavior/selectable-behavior.service';
import * as d3 from 'd3';
import {
    selectAnySelectedComponentIds,
    selectConnections,
    selectCurrentProcessGroupId,
    selectFlowLoadingStatus,
    selectTransitionRequired
} from '../../state/flow/flow.selectors';
import { initialState } from '../../state/flow/flow.reducer';
import { TransitionBehavior } from '../behavior/transition-behavior.service';
import { INITIAL_SCALE } from '../../state/transform/transform.reducer';
import { selectTransform } from '../../state/transform/transform.selectors';
import {
    openEditConnectionDialog,
    showOkDialog,
    updateComponent,
    updateConnection
} from '../../state/flow/flow.actions';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { UnorderedListTip } from '../../../../ui/common/tooltips/unordered-list-tip/unordered-list-tip.component';
import { Dimension, Position } from '../../state/shared';
import { ComponentType, SelectOption } from '../../../../state/shared';
import { loadBalanceStrategies, UpdateComponentRequest } from '../../state/flow';
import { filter, switchMap } from 'rxjs';
import { NiFiCommon } from '../../../../service/nifi-common.service';
import { QuickSelectBehavior } from '../behavior/quick-select-behavior.service';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';

export class ConnectionRenderOptions {
    updatePath?: boolean;
    updateLabel?: boolean;
}

@Injectable({
    providedIn: 'root'
})
export class ConnectionManager {
    private destroyRef = inject(DestroyRef);

    private static readonly DIMENSIONS: Dimension = {
        width: 224,
        height: 0
    };

    private static readonly BACKPRESSURE_BAR_WIDTH: number = ConnectionManager.DIMENSIONS.width / 2 - 15 - 2;
    private static readonly BACKPRESSURE_COUNT_OFFSET: number = 6;
    private static readonly BACKPRESSURE_DATASIZE_OFFSET: number = ConnectionManager.DIMENSIONS.width / 2 + 10 + 1;

    public static readonly SELF_LOOP_X_OFFSET: number = ConnectionManager.DIMENSIONS.width / 2 + 5;
    public static readonly SELF_LOOP_Y_OFFSET: number = 25;

    private static readonly HEIGHT_FOR_BACKPRESSURE: number = 3;

    private static readonly SNAP_ALIGNMENT_PIXELS: number = 8;

    private connections: [] = [];
    private connectionContainer: any;
    private transitionRequired = false;
    private currentProcessGroupId: string = initialState.id;
    private scale: number = INITIAL_SCALE;

    private lineGenerator: any;

    private bendPointDrag: any;
    private endpointDrag: any;
    private labelDrag: any;

    private snapEnabled = true;

    private viewContainerRef: ViewContainerRef | undefined;

    constructor(
        private store: Store<CanvasState>,
        private canvasUtils: CanvasUtils,
        private nifiCommon: NiFiCommon,
        private client: Client,
        private selectableBehavior: SelectableBehavior,
        private transitionBehavior: TransitionBehavior,
        private quickSelectBehavior: QuickSelectBehavior,
        private clusterConnectionService: ClusterConnectionService
    ) {}

    /**
     * Gets the position of the label for the specified connection.
     *
     * @param {type} connectionLabel      The connection label
     */
    private getLabelPosition(connectionLabel: any): Position {
        const d = connectionLabel.datum();

        let { x, y } = this.canvasUtils.getPositionForCenteringConnection(d);

        // offset to account for the label dimensions
        x -= ConnectionManager.DIMENSIONS.width / 2;
        y -= connectionLabel.attr('height') / 2;

        return {
            x: x,
            y: y
        };
    }

    /**
     * Calculates the distance between the two points specified squared.
     *
     * @param {Position} v        First point
     * @param {Position} w        Second point
     */
    private distanceSquared(v: Position, w: Position): number {
        return Math.pow(v.x - w.x, 2) + Math.pow(v.y - w.y, 2);
    }

    /**
     * Calculates the distance between the two points specified.
     *
     * @param {Position} v        First point
     * @param {Position} w        Second point
     */
    private distanceBetweenPoints(v: Position, w: Position): number {
        return Math.sqrt(this.distanceSquared(v, w));
    }

    /**
     * Calculates the distance between the point and the line created by s1 and s2.
     *
     * @param {Position} p            The point
     * @param {Position} s1           Segment start
     * @param {Position} s2           Segment end
     */
    private distanceToSegment(p: Position, s1: Position, s2: Position): number {
        const l2: number = this.distanceSquared(s1, s2);
        if (l2 === 0) {
            return Math.sqrt(this.distanceSquared(p, s1));
        }

        const t: number = ((p.x - s1.x) * (s2.x - s1.x) + (p.y - s1.y) * (s2.y - s1.y)) / l2;
        if (t < 0) {
            return Math.sqrt(this.distanceSquared(p, s1));
        }
        if (t > 1) {
            return Math.sqrt(this.distanceSquared(p, s2));
        }

        return Math.sqrt(
            this.distanceSquared(p, {
                x: s1.x + t * (s2.x - s1.x),
                y: s1.y + t * (s2.y - s1.y)
            })
        );
    }

    /**
     * Calculates the index of the bend point that is nearest to the specified point.
     *
     * @param {Position} p
     * @param {object} connectionData
     */
    private getNearestSegment(p: Position, connectionData: any): number {
        if (connectionData.bends.length === 0) {
            return 0;
        }

        let minimumDistance: number;
        let index = 0;

        // line is comprised of start -> [bends] -> end
        const line = [connectionData.start].concat(connectionData.bends, [connectionData.end]);

        // consider each segment
        for (let i = 0; i < line.length; i++) {
            if (i + 1 < line.length) {
                const distance: number = this.distanceToSegment(p, line[i], line[i + 1]);

                // @ts-ignore
                if (minimumDistance === undefined || distance < minimumDistance) {
                    minimumDistance = distance;
                    index = i;
                }
            }
        }

        return index;
    }

    /**
     * Determines if the specified type is a type of input port.
     *
     * @argument {string} type      The port type
     */
    private isInputPortType(type: string): boolean {
        return type.indexOf('INPUT_PORT') >= 0;
    }

    /**
     * Determines if the specified type is a type of output port.
     *
     * @argument {string} type      The port type
     */
    private isOutputPortType(type: string): boolean {
        return type.indexOf('OUTPUT_PORT') >= 0;
    }

    /**
     * Determines whether the terminal of the connection (source|destination) is
     * a group.
     *
     * @param {object} terminal
     */
    private isGroup(terminal: any) {
        return (
            terminal.groupId !== this.currentProcessGroupId &&
            (this.isInputPortType(terminal.type) || this.isOutputPortType(terminal.type))
        );
    }

    /**
     * Determines whether expiration is configured for the specified connection.
     *
     * @param {object} connection
     * @return {boolean} Whether expiration is configured
     */
    private isExpirationConfigured(connection: any): boolean {
        if (connection.flowFileExpiration != null) {
            const match: string[] = connection.flowFileExpiration.match(/^(\d+).*/);
            if (match !== null && match.length > 0) {
                if (parseInt(match[0], 10) > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Determines whether load-balance is configured for the specified connection.
     *
     * @param {object} connection
     * @return {boolean} Whether load-balance is configured
     */
    private isLoadBalanceConfigured(connection: any): boolean {
        return connection.loadBalanceStrategy != null && 'DO_NOT_LOAD_BALANCE' !== connection.loadBalanceStrategy;
    }

    /**
     * Sorts the specified connections according to the z index.
     *
     * @param {type} connections
     */
    private sort(connections: any[]): void {
        connections.sort(function (a, b) {
            return a.zIndex === b.zIndex ? 0 : a.zIndex > b.zIndex ? 1 : -1;
        });
    }

    // determines whether the specified connection contains an unsupported relationship
    private hasUnavailableRelationship(d: any): boolean {
        // verify each selected relationship is still available
        if (d.component.selectedRelationships != null && d.component.availableRelationships != null) {
            const unavailableRelationships: string[] = d.component.selectedRelationships.filter(
                (selectedRelationship: string) => !d.component.availableRelationships.includes(selectedRelationship)
            );
            return unavailableRelationships.length > 0;
        }

        return false;
    }

    // determines whether the connection is full based on the object count threshold
    private isFullCount(d: any): boolean {
        return d.status.aggregateSnapshot.percentUseCount === 100;
    }

    // determines whether the connection is in warning based on the object count threshold
    private isWarningCount(d: any): boolean {
        const percentUseCount: number = d.status.aggregateSnapshot.percentUseCount;
        if (percentUseCount != null) {
            return percentUseCount >= 61 && percentUseCount <= 85;
        }

        return false;
    }

    // determines whether the connection is in error based on the object count threshold
    private isErrorCount(d: any): boolean {
        const percentUseCount = d.status.aggregateSnapshot.percentUseCount;
        if (percentUseCount != null) {
            return percentUseCount > 85;
        }

        return false;
    }

    // determines whether the connection is full based on the data size threshold
    private isFullBytes(d: any): boolean {
        return d.status.aggregateSnapshot.percentUseBytes === 100;
    }

    // determines whether the connection is in warning based on the data size threshold
    private isWarningBytes(d: any): boolean {
        const percentUseBytes = d.status.aggregateSnapshot.percentUseBytes;
        if (percentUseBytes != null) {
            return percentUseBytes >= 61 && percentUseBytes <= 85;
        }

        return false;
    }

    // determines whether the connection is in error based on the data size threshold
    private isErrorBytes(d: any): boolean {
        const percentUseBytes = d.status.aggregateSnapshot.percentUseBytes;
        if (percentUseBytes != null) {
            return percentUseBytes > 85;
        }

        return false;
    }

    /**
     * Saves the connection entry specified by d with the new configuration specified
     * in connection.
     *
     * @param {type} d
     * @param {type} connection
     */
    private save(d: any, connection: any, restoreOnFailure?: any): void {
        const updateConnection: UpdateComponentRequest = {
            id: d.id,
            type: ComponentType.Connection,
            uri: d.uri,
            payload: {
                revision: this.client.getRevision(d),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                component: connection
            },
            restoreOnFailure: restoreOnFailure
        };

        // updateConnection is not needed here because we don't need any
        // of the side effects that reload source and destination components
        this.store.dispatch(
            updateComponent({
                request: updateConnection
            })
        );
    }

    private isAtBackPressure(d: any): boolean {
        // TODO - optional chaining default ?? 0
        const percentUseCount: number = d.status.aggregateSnapshot.percentUseCount;
        const percentUseBytes: number = d.status.aggregateSnapshot.percentUseBytes;
        return Math.max(percentUseCount, percentUseBytes) >= 100;
    }

    /**
     * Gets the tooltip content for the back pressure count metric
     * @param d
     */
    private getBackPressureCountTip(d: any) {
        const tooltipLines: string[] = [];
        const percentUseCount: number = d.status.aggregateSnapshot.percentUseCount;

        if (percentUseCount != null) {
            const predictions: any = d.status.aggregateSnapshot.predictions;
            const percentUseCountClamped: number = Math.min(Math.max(percentUseCount, 0), 100);

            if (d.permissions.canRead) {
                const objectThreshold: number = d.component.backPressureObjectThreshold;
                tooltipLines.push(
                    `Queue: ${percentUseCountClamped}% full (based on ${objectThreshold} object threshold)`
                );
            } else {
                tooltipLines.push(`Queue: ${percentUseCountClamped}% full`);
            }

            if (predictions != null) {
                const predictedPercentCount: number = predictions.predictedPercentCount;
                const timeToBackPressure: number = predictions.predictedMillisUntilCountBackpressure;

                // only show predicted percent if it is non-negative
                const predictionIntervalSeconds: number = predictions.predictionIntervalSeconds;
                const predictedPercentCountClamped: number = Math.min(Math.max(predictedPercentCount, 0), 100);
                tooltipLines.push(
                    `Predicted queue (next ${predictionIntervalSeconds / 60} mins): ${predictedPercentCountClamped}%`
                );

                // only show an estimate if it is valid (non-negative but less than the max number supported)
                const isAtBackPressure: boolean = this.isAtBackPressure(d);
                if (timeToBackPressure >= 0 && timeToBackPressure < Number.MAX_SAFE_INTEGER && !isAtBackPressure) {
                    const duration: string = this.canvasUtils.formatPredictedDuration(timeToBackPressure);
                    tooltipLines.push(`Estimated time to back pressure: ${duration}`);
                } else {
                    tooltipLines.push(`Estimated time to back pressure: ${isAtBackPressure ? 'now' : 'NA'}`);
                }
            } else {
                tooltipLines.push('Queue Prediction is not configured');
            }
        } else {
            tooltipLines.push('Back Pressure Object Threshold is not configured');
        }

        return tooltipLines;
    }

    /**
     * Gets the tooltip content for the back pressure size metric
     * @param d
     */
    private getBackPressureSizeTip(d: any): string[] {
        const tooltipLines: string[] = [];
        const percentUseBytes: number = d.status.aggregateSnapshot.percentUseBytes;

        if (percentUseBytes != null) {
            const predictions: any = d.status.aggregateSnapshot.predictions;
            const percentUseBytesClamped: number = Math.min(Math.max(percentUseBytes, 0), 100);

            if (d.permissions.canRead) {
                const dataSizeThreshold: string = d.component.backPressureDataSizeThreshold;
                tooltipLines.push(
                    `Queue: ${percentUseBytesClamped}% full (based on ${dataSizeThreshold} data size threshold)`
                );
            } else {
                tooltipLines.push(`Queue: ${percentUseBytesClamped}% full`);
            }

            if (predictions != null) {
                const predictedPercentBytes: number = predictions.predictedPercentBytes;
                const timeToBackPressure: number = predictions.predictedMillisUntilBytesBackpressure;

                // only show predicted percent if it is non-negative
                const predictionIntervalSeconds: number = predictions.predictionIntervalSeconds;
                const predictedPercentBytesClamped: number = Math.min(Math.max(predictedPercentBytes, 0), 100);
                tooltipLines.push(
                    `Predicted queue (next ${predictionIntervalSeconds / 60} mins): ${predictedPercentBytesClamped}%`
                );

                // only show an estimate if it is valid (non-negative but less than the max number supported)
                const isAtBackPressure: boolean = this.isAtBackPressure(d);
                if (timeToBackPressure >= 0 && timeToBackPressure < Number.MAX_SAFE_INTEGER && !isAtBackPressure) {
                    const duration = this.canvasUtils.formatPredictedDuration(timeToBackPressure);
                    tooltipLines.push(`Estimated time to back pressure: ${duration}`);
                } else {
                    tooltipLines.push(`Estimated time to back pressure: ${isAtBackPressure ? 'now' : 'NA'}`);
                }
            } else {
                tooltipLines.push('Queue Prediction is not configured');
            }
        } else {
            tooltipLines.push('Back Pressure Data Size Threshold is not configured');
        }

        return tooltipLines;
    }

    private select() {
        return this.connectionContainer.selectAll('g.connection').data(this.connections, function (d: any) {
            return d.id;
        });
    }

    private renderConnections(entered: any) {
        if (entered.empty()) {
            return entered;
        }
        const self: ConnectionManager = this;

        const connection = entered
            .append('g')
            .attr('id', function (d: any) {
                return 'id-' + d.id;
            })
            .attr('class', 'connection');

        // create a connection between the two components
        connection.append('path').attr('class', 'connection-path').attr('pointer-events', 'none');

        // path to show when selection
        connection.append('path').attr('class', 'connection-selection-path').attr('pointer-events', 'none');

        // path to make selection easier
        connection
            .append('path')
            .attr('class', 'connection-path-selectable')
            .attr('pointer-events', 'stroke')
            .on('mousedown.selection', function (this: any, event: MouseEvent) {
                // select the connection when clicking the selectable path
                self.selectableBehavior.select(event, d3.select(this.parentNode));
            })
            .on('dblclick', function (this: any, event: MouseEvent, d: any) {
                if (d.permissions.canWrite && d.permissions.canRead) {
                    const position = d3.pointer(event, this.parentNode);

                    // find where to put this bend point
                    const bendIndex = self.getNearestSegment(
                        {
                            x: position[0],
                            y: position[1]
                        },
                        d
                    );

                    // copy the original to restore if necessary
                    const bends = d.component.bends.slice();

                    // add it to the collection of points
                    bends.splice(bendIndex, 0, {
                        x: position[0],
                        y: position[1]
                    });

                    const connectionAddedBend: any = {
                        id: d.id,
                        bends: bends
                    };

                    // update the label index if necessary
                    const labelIndex = d.component.labelIndex;
                    if (bends.length === 1) {
                        connectionAddedBend.labelIndex = 0;
                    } else if (bendIndex <= labelIndex) {
                        connectionAddedBend.labelIndex = labelIndex + 1;
                    }

                    // save the new state
                    self.save(d, connectionAddedBend);

                    event.stopPropagation();
                }
            });

        return connection;
    }

    private updateConnections(updated: any, { updatePath = true, updateLabel = true }: ConnectionRenderOptions = {}) {
        if (updated.empty()) {
            return;
        }

        const self: ConnectionManager = this;
        if (updatePath) {
            updated
                .classed('grouped', function (d: any) {
                    let grouped = false;

                    if (d.permissions.canRead) {
                        // if there are more than one selected relationship, mark this as grouped
                        if (d.component.selectedRelationships?.length > 1) {
                            grouped = true;
                        }
                    }

                    return grouped;
                })
                .classed('ghost', function (d: any) {
                    let ghost = false;

                    if (d.permissions.canRead) {
                        // if the connection has a relationship that is unavailable, mark it a ghost relationship
                        if (self.hasUnavailableRelationship(d)) {
                            ghost = true;
                        }
                    }

                    return ghost;
                });

            // update connection path
            updated.select('path.connection-path').classed('unauthorized', function (d: any) {
                return d.permissions.canRead === false;
            });
        }

        updated.each(function (this: any, d: any) {
            const connection: any = d3.select(this);

            if (updatePath) {
                // calculate the start and end points
                const sourceComponentId: string = self.canvasUtils.getConnectionSourceComponentId(d);
                const sourceData: any = d3.select('#id-' + sourceComponentId).datum();
                let end: Position;

                // get the appropriate end anchor point
                let endAnchor: any;
                if (d.bends.length > 0) {
                    endAnchor = d.bends[d.bends.length - 1];
                } else {
                    endAnchor = {
                        x: sourceData.position.x + sourceData.dimensions.width / 2,
                        y: sourceData.position.y + sourceData.dimensions.height / 2
                    };
                }

                // if we are currently dragging the endpoint to a new target, use that
                // position, otherwise we need to calculate it for the current target
                if (d.end?.dragging) {
                    // since we're dragging, use the same object thats bound to the endpoint drag event
                    end = d.end;

                    // if we're not over a connectable destination use the current point
                    const newDestination: any = d3.select('g.hover.connectable-destination');
                    if (!newDestination.empty()) {
                        const newDestinationData: any = newDestination.datum();

                        // get the position on the new destination perimeter
                        const newEnd: Position = self.canvasUtils.getPerimeterPoint(endAnchor, {
                            x: newDestinationData.position.x,
                            y: newDestinationData.position.y,
                            width: newDestinationData.dimensions.width,
                            height: newDestinationData.dimensions.height
                        });

                        // update the coordinates with the new point
                        end.x = newEnd.x;
                        end.y = newEnd.y;
                    }
                } else {
                    const destinationComponentId: string = self.canvasUtils.getConnectionDestinationComponentId(d);
                    const destinationData: any = d3.select('#id-' + destinationComponentId).datum();

                    // get the position on the destination perimeter
                    end = self.canvasUtils.getPerimeterPoint(endAnchor, {
                        x: destinationData.position.x,
                        y: destinationData.position.y,
                        width: destinationData.dimensions.width,
                        height: destinationData.dimensions.height
                    });
                }

                // get the appropriate start anchor point
                let startAnchor: Position;
                if (d.bends.length > 0) {
                    startAnchor = d.bends[0];
                } else {
                    startAnchor = end;
                }

                // get the position on the source perimeter
                const start: Position = self.canvasUtils.getPerimeterPoint(startAnchor, {
                    x: sourceData.position.x,
                    y: sourceData.position.y,
                    width: sourceData.dimensions.width,
                    height: sourceData.dimensions.height
                });

                // store the updated endpoints
                d.start = start;
                d.end = end;

                // update the connection paths
                self.transitionBehavior
                    .transition(connection.select('path.connection-path'), self.transitionRequired)
                    .attr('d', function () {
                        const datum: Position[] = [d.start].concat(d.bends, [d.end]);
                        return self.lineGenerator(datum);
                    });
                self.transitionBehavior
                    .transition(connection.select('path.connection-selection-path'), self.transitionRequired)
                    .attr('d', function () {
                        const datum: Position[] = [d.start].concat(d.bends, [d.end]);
                        return self.lineGenerator(datum);
                    });
                self.transitionBehavior
                    .transition(connection.select('path.connection-path-selectable'), self.transitionRequired)
                    .attr('d', function () {
                        const datum: Position[] = [d.start].concat(d.bends, [d.end]);
                        return self.lineGenerator(datum);
                    });

                // -----
                // bends
                // -----

                let startpoints: any = connection.selectAll('rect.startpoint');
                let endpoints: any = connection.selectAll('rect.endpoint');
                let midpoints: any = connection.selectAll('rect.midpoint');

                // require read and write permissions as it's required to read the connections available relationships
                // when connecting to a group or remote group
                if (d.permissions.canWrite && d.permissions.canRead) {
                    // ------------------
                    // bends - startpoint
                    // ------------------

                    startpoints = startpoints.data([d.start]);

                    // create a point for the start
                    const startpointsEntered: any = startpoints
                        .enter()
                        .append('rect')
                        .attr('class', 'startpoint linepoint')
                        .attr('pointer-events', 'all')
                        .attr('width', 8)
                        .attr('height', 8)
                        .on('mousedown.selection', function (this: any, event: MouseEvent) {
                            // select the connection when clicking the label
                            self.selectableBehavior.select(event, d3.select(this.parentNode));
                        });

                    // update the start point
                    self.transitionBehavior
                        .transition(startpoints.merge(startpointsEntered), self.transitionRequired)
                        .attr('transform', function (p: any) {
                            return 'translate(' + (p.x - 4) + ', ' + (p.y - 4) + ')';
                        });

                    // remove old items
                    startpoints.exit().remove();

                    // ----------------
                    // bends - endpoint
                    // ----------------

                    endpoints = endpoints.data([d.end]);

                    // create a point for the end
                    const endpointsEntered = endpoints
                        .enter()
                        .append('rect')
                        .attr('class', 'endpoint linepoint')
                        .attr('pointer-events', 'all')
                        .attr('width', 8)
                        .attr('height', 8)
                        .on('mousedown.selection', function (this: any, event: MouseEvent) {
                            // select the connection when clicking the label
                            self.selectableBehavior.select(event, d3.select(this.parentNode));
                        })
                        .call(self.endpointDrag);

                    // update the end point
                    self.transitionBehavior
                        .transition(endpoints.merge(endpointsEntered), self.transitionRequired)
                        .attr('transform', function (p: any) {
                            return 'translate(' + (p.x - 4) + ', ' + (p.y - 4) + ')';
                        });

                    // remove old items
                    endpoints.exit().remove();

                    // -----------------
                    // bends - midpoints
                    // -----------------

                    midpoints = midpoints.data(d.bends);

                    // create a point for the end
                    const midpointsEntered = midpoints
                        .enter()
                        .append('rect')
                        .attr('class', 'midpoint linepoint')
                        .attr('pointer-events', 'all')
                        .attr('width', 8)
                        .attr('height', 8)
                        .on('dblclick', function (this: any, event: MouseEvent, p: any) {
                            // stop even propagation
                            event.stopPropagation();

                            const connection: any = d3.select(this.parentNode);
                            const connectionData: any = connection.datum();

                            // if this is a self loop prevent removing the last two bends
                            const sourceComponentId = self.canvasUtils.getConnectionSourceComponentId(connectionData);
                            const destinationComponentId =
                                self.canvasUtils.getConnectionDestinationComponentId(connectionData);
                            if (sourceComponentId === destinationComponentId && d.component.bends.length <= 2) {
                                this.store.dispatch(
                                    showOkDialog({
                                        title: 'Connection',
                                        message: 'Looping connections must have at least two bend points.'
                                    })
                                );
                                return;
                            }

                            const newBends: any[] = [];
                            let bendIndex = -1;

                            // create a new array of bends without the selected one
                            connectionData.component.bends.forEach((bend: any, i: number) => {
                                if (p.x !== bend.x && p.y !== bend.y) {
                                    newBends.push(bend);
                                } else {
                                    bendIndex = i;
                                }
                            });

                            if (bendIndex < 0) {
                                return;
                            }

                            const connectionRemovedBend: any = {
                                id: connectionData.id,
                                bends: newBends
                            };

                            // update the label index if necessary
                            const labelIndex: number = connectionData.component.labelIndex;
                            if (newBends.length <= 1) {
                                connectionRemovedBend.labelIndex = 0;
                            } else if (bendIndex <= labelIndex) {
                                connectionRemovedBend.labelIndex = Math.max(0, labelIndex - 1);
                            }

                            // save the updated connection
                            self.save(connectionData, connectionRemovedBend);
                        })
                        .on('mousedown.selection', function (this: any, event: MouseEvent) {
                            // select the connection when clicking the label
                            self.selectableBehavior.select(event, d3.select(this.parentNode));
                        })
                        .call(self.bendPointDrag);

                    // update the midpoints
                    self.transitionBehavior
                        .transition(midpoints.merge(midpointsEntered), self.transitionRequired)
                        .attr('transform', function (p: any) {
                            return 'translate(' + (p.x - 4) + ', ' + (p.y - 4) + ')';
                        });

                    // remove old items
                    midpoints.exit().remove();
                } else {
                    // remove the start, mid, and end points
                    startpoints.remove();
                    endpoints.remove();
                    midpoints.remove();
                }
            }

            if (updateLabel) {
                let connectionLabelContainer: any = connection.select('g.connection-label-container');

                // update visible connections
                if (connection.classed('visible')) {
                    // if there is no connection label this connection is becoming
                    // visible so we need to render it
                    if (connectionLabelContainer.empty()) {
                        // connection label container
                        connectionLabelContainer = connection
                            .insert('g', 'rect.startpoint')
                            .attr('class', 'connection-label-container')
                            .attr('pointer-events', 'all')
                            .on('mousedown.selection', function (this: any, event: MouseEvent) {
                                // select the connection when clicking the label
                                self.selectableBehavior.select(event, d3.select(this.parentNode));
                            });

                        self.quickSelectBehavior.activate(connectionLabelContainer);

                        // connection label
                        connectionLabelContainer
                            .append('rect')
                            .attr('class', 'body')
                            .attr('width', ConnectionManager.DIMENSIONS.width)
                            .attr('x', 0)
                            .attr('y', 0);

                        // processor border
                        connectionLabelContainer
                            .append('rect')
                            .attr('class', 'border')
                            .attr('width', ConnectionManager.DIMENSIONS.width)
                            .attr('fill', 'transparent')
                            .attr('stroke', 'transparent');
                    }

                    let labelCount = 0;
                    const rowHeight = 19;
                    const backgrounds: any[] = [];
                    const borders: any[] = [];

                    let connectionFrom = connectionLabelContainer.select('g.connection-from-container');
                    let connectionTo = connectionLabelContainer.select('g.connection-to-container');
                    let connectionName = connectionLabelContainer.select('g.connection-name-container');

                    if (d.permissions.canRead) {
                        // -----------------------
                        // connection label - from
                        // -----------------------

                        // determine if the connection require a from label
                        if (self.isGroup(d.component.source)) {
                            // see if the connection from label is already rendered
                            if (connectionFrom.empty()) {
                                connectionFrom = connectionLabelContainer
                                    .append('g')
                                    .attr('class', 'connection-from-container');

                                // background
                                backgrounds.push(
                                    connectionFrom
                                        .append('rect')
                                        .attr('class', 'connection-label-background')
                                        .attr('width', ConnectionManager.DIMENSIONS.width)
                                        .attr('height', rowHeight)
                                );

                                // border
                                borders.push(
                                    connectionFrom
                                        .append('rect')
                                        .attr('class', 'connection-label-border')
                                        .attr('width', ConnectionManager.DIMENSIONS.width)
                                        .attr('height', 1)
                                );

                                connectionFrom
                                    .append('text')
                                    .attr('class', 'stats-label')
                                    .attr('x', 5)
                                    .attr('y', 14)
                                    .text('From');

                                connectionFrom
                                    .append('text')
                                    .attr('class', 'stats-value connection-from')
                                    .attr('x', 43)
                                    .attr('y', 14)
                                    .attr('width', 130);

                                connectionFrom
                                    .append('text')
                                    .attr('class', 'connection-from-run-status')
                                    .attr('x', 208)
                                    .attr('y', 14);
                            } else {
                                backgrounds.push(connectionFrom.select('rect.connection-label-background'));
                                borders.push(connectionFrom.select('rect.connection-label-border'));
                            }

                            // update the connection from positioning
                            connectionFrom.attr('transform', function () {
                                const y: number = rowHeight * labelCount++;
                                return 'translate(0, ' + y + ')';
                            });

                            // update the label text
                            connectionFrom
                                .select('text.connection-from')
                                .each(function (this: any) {
                                    const connectionFromLabel = d3.select(this);

                                    // reset the label name to handle any previous state
                                    connectionFromLabel.text(null).selectAll('title').remove();

                                    // apply ellipsis to the label as necessary
                                    self.canvasUtils.ellipsis(
                                        connectionFromLabel,
                                        d.component.source.name,
                                        'connection-from'
                                    );
                                })
                                .append('title')
                                .text(function () {
                                    return d.component.source.name;
                                });

                            // update the label run status
                            connectionFrom
                                .select('text.connection-from-run-status')
                                .text(function () {
                                    if (d.component.source.exists === false) {
                                        return '\uf071';
                                    } else if (d.component.source.running === true) {
                                        return '\uf04b';
                                    } else {
                                        return '\uf04d';
                                    }
                                })
                                .classed('running nifi-success-lighter', function () {
                                    if (d.component.source.exists === false) {
                                        return false;
                                    } else {
                                        return d.component.source.running;
                                    }
                                })
                                .classed('stopped nifi-warn-lighter', function () {
                                    if (d.component.source.exists === false) {
                                        return false;
                                    } else {
                                        return !d.component.source.running;
                                    }
                                })
                                .classed('is-missing-port', function () {
                                    return d.component.source.exists === false;
                                });
                        } else {
                            // there is no connection from, remove the previous if necessary
                            connectionFrom.remove();
                        }

                        // ---------------------
                        // connection label - to
                        // ---------------------

                        // determine if the connection require a to label
                        if (self.isGroup(d.component.destination)) {
                            // see if the connection to label is already rendered
                            if (connectionTo.empty()) {
                                connectionTo = connectionLabelContainer
                                    .append('g')
                                    .attr('class', 'connection-to-container');

                                // background
                                backgrounds.push(
                                    connectionTo
                                        .append('rect')
                                        .attr('class', 'connection-label-background')
                                        .attr('width', ConnectionManager.DIMENSIONS.width)
                                        .attr('height', rowHeight)
                                );

                                // border
                                borders.push(
                                    connectionTo
                                        .append('rect')
                                        .attr('class', 'connection-label-border')
                                        .attr('width', ConnectionManager.DIMENSIONS.width)
                                        .attr('height', 1)
                                );

                                connectionTo
                                    .append('text')
                                    .attr('class', 'stats-label')
                                    .attr('x', 5)
                                    .attr('y', 14)
                                    .text('To');

                                connectionTo
                                    .append('text')
                                    .attr('class', 'stats-value connection-to')
                                    .attr('x', 25)
                                    .attr('y', 14)
                                    .attr('width', 145);

                                connectionTo
                                    .append('text')
                                    .attr('class', 'connection-to-run-status')
                                    .attr('x', 208)
                                    .attr('y', 14);
                            } else {
                                backgrounds.push(connectionTo.select('rect.connection-label-background'));
                                borders.push(connectionTo.select('rect.connection-label-border'));
                            }

                            // update the connection to positioning
                            connectionTo.attr('transform', function () {
                                const y: number = rowHeight * labelCount++;
                                return 'translate(0, ' + y + ')';
                            });

                            // update the label text
                            connectionTo
                                .select('text.connection-to')
                                .each(function (this: any, d: any) {
                                    const connectionToLabel = d3.select(this);

                                    // reset the label name to handle any previous state
                                    connectionToLabel.text(null).selectAll('title').remove();

                                    // apply ellipsis to the label as necessary
                                    self.canvasUtils.ellipsis(
                                        connectionToLabel,
                                        d.component.destination.name,
                                        'connection-to'
                                    );
                                })
                                .append('title')
                                .text(function (d: any) {
                                    return d.component.destination.name;
                                });

                            // update the label run status
                            connectionTo
                                .select('text.connection-to-run-status')
                                .text(function () {
                                    if (d.component.destination.exists === false) {
                                        return '\uf071';
                                    } else if (d.component.destination.running === true) {
                                        return '\uf04b';
                                    } else {
                                        return '\uf04d';
                                    }
                                })
                                .classed('running nifi-success-lighter', function () {
                                    if (d.component.destination.exists === false) {
                                        return false;
                                    } else {
                                        return d.component.destination.running;
                                    }
                                })
                                .classed('stopped nifi-warn-lighter', function () {
                                    if (d.component.destination.exists === false) {
                                        return false;
                                    } else {
                                        return !d.component.destination.running;
                                    }
                                })
                                .classed('is-missing-port', function () {
                                    return d.component.destination.exists === false;
                                });
                        } else {
                            // there is no connection to, remove the previous if necessary
                            connectionTo.remove();
                        }

                        // -----------------------
                        // connection label - name
                        // -----------------------

                        // get the connection name
                        const connectionNameValue: string = self.canvasUtils.formatConnectionName(d.component);

                        // is there a name to render
                        if (!self.nifiCommon.isBlank(connectionNameValue)) {
                            // see if the connection name label is already rendered
                            if (connectionName.empty()) {
                                connectionName = connectionLabelContainer
                                    .append('g')
                                    .attr('class', 'connection-name-container');

                                // background
                                backgrounds.push(
                                    connectionName
                                        .append('rect')
                                        .attr('class', 'connection-label-background')
                                        .attr('width', ConnectionManager.DIMENSIONS.width)
                                        .attr('height', rowHeight)
                                );

                                // border
                                borders.push(
                                    connectionName
                                        .append('rect')
                                        .attr('class', 'connection-label-border')
                                        .attr('width', ConnectionManager.DIMENSIONS.width)
                                        .attr('height', 1)
                                );

                                connectionName
                                    .append('text')
                                    .attr('class', 'stats-label')
                                    .attr('x', 5)
                                    .attr('y', 14)
                                    .text('Name');

                                connectionName
                                    .append('text')
                                    .attr('class', 'stats-value connection-name')
                                    .attr('x', 45)
                                    .attr('y', 14)
                                    .attr('width', 142);
                            } else {
                                backgrounds.push(connectionName.select('rect.connection-label-background'));
                                borders.push(connectionName.select('rect.connection-label-border'));
                            }

                            // update the connection name positioning
                            connectionName.attr('transform', function () {
                                const y: number = rowHeight * labelCount++;
                                return 'translate(0, ' + y + ')';
                            });

                            // update the connection name
                            connectionName
                                .select('text.connection-name')
                                .each(function (this: any) {
                                    const connectionToLabel = d3.select(this);

                                    // reset the label name to handle any previous state
                                    connectionToLabel.text(null).selectAll('title').remove();

                                    // apply ellipsis to the label as necessary
                                    self.canvasUtils.ellipsis(
                                        connectionToLabel,
                                        connectionNameValue,
                                        'connection-name'
                                    );
                                })
                                .append('title')
                                .text(function () {
                                    return connectionNameValue;
                                });
                        } else {
                            // there is no connection name, remove the previous if necessary
                            connectionName.remove();
                        }
                    } else {
                        // no permissions to read to remove previous if necessary
                        connectionFrom.remove();
                        connectionTo.remove();
                        connectionName.remove();
                    }

                    // -------------------------
                    // connection label - queued
                    // -------------------------

                    // see if the queue label is already rendered
                    let queued: any = connectionLabelContainer.select('g.queued-container');
                    if (queued.empty()) {
                        queued = connectionLabelContainer.append('g').attr('class', 'queued-container');

                        // background
                        backgrounds.push(
                            queued
                                .append('rect')
                                .attr('class', 'connection-label-background')
                                .attr('width', ConnectionManager.DIMENSIONS.width)
                                .attr('height', rowHeight + ConnectionManager.HEIGHT_FOR_BACKPRESSURE)
                        );

                        // border
                        borders.push(
                            queued
                                .append('rect')
                                .attr('class', 'connection-label-border')
                                .attr('width', ConnectionManager.DIMENSIONS.width)
                                .attr('height', 1)
                        );

                        queued.append('text').attr('class', 'stats-label').attr('x', 5).attr('y', 14).text('Queued');

                        const queuedText = queued
                            .append('text')
                            .attr('class', 'stats-value queued')
                            .attr('x', 55)
                            .attr('y', 14);

                        // queued count
                        queuedText.append('tspan').attr('class', 'count');

                        // queued size
                        queuedText.append('tspan').attr('class', 'size');

                        // load balance icon
                        // x is set dynamically to slide to right, depending on whether expiration icon is shown.
                        queued
                            .append('text')
                            .attr('class', 'load-balance-icon')
                            .attr('y', 14)
                            .text(function () {
                                return '\uf042';
                            })
                            .append('title');

                        // expiration icon
                        queued
                            .append('text')
                            .attr('class', 'expiration-icon')
                            .attr('x', 208)
                            .attr('y', 14)
                            .text(function () {
                                return '\uf017';
                            })
                            .append('title');

                        const yBackpressureOffset: number = rowHeight + ConnectionManager.HEIGHT_FOR_BACKPRESSURE - 4;

                        // backpressure object threshold
                        const backpressureObjectContainer = queued
                            .append('g')
                            .attr(
                                'transform',
                                'translate(' +
                                    ConnectionManager.BACKPRESSURE_COUNT_OFFSET +
                                    ', ' +
                                    yBackpressureOffset +
                                    ')'
                            )
                            .attr('class', 'backpressure-object-container');

                        // start
                        backpressureObjectContainer
                            .append('rect')
                            .attr('class', 'backpressure-tick object')
                            .attr('width', 1)
                            .attr('height', 3)
                            .attr('x', 0)
                            .attr('y', 0);

                        // bar
                        backpressureObjectContainer
                            .append('rect')
                            .attr('class', 'backpressure-object')
                            .attr('width', ConnectionManager.BACKPRESSURE_BAR_WIDTH)
                            .attr('height', 3)
                            .attr('x', 0)
                            .attr('y', 0);

                        // end
                        backpressureObjectContainer
                            .append('rect')
                            .attr('class', 'backpressure-tick object')
                            .attr('width', 1)
                            .attr('height', 3)
                            .attr('x', ConnectionManager.BACKPRESSURE_BAR_WIDTH)
                            .attr('y', 0);

                        // percent full
                        backpressureObjectContainer
                            .append('rect')
                            .attr('class', 'backpressure-percent object')
                            .attr('width', 0)
                            .attr('height', 3)
                            .attr('x', 0)
                            .attr('y', 0);

                        // prediction indicator
                        backpressureObjectContainer
                            .append('rect')
                            .attr('class', 'backpressure-tick object-prediction')
                            .attr('width', 1)
                            .attr('height', 3)
                            .attr('x', ConnectionManager.BACKPRESSURE_BAR_WIDTH)
                            .attr('y', 0);

                        // backpressure data size threshold

                        const backpressureDataSizeContainer = queued
                            .append('g')
                            .attr(
                                'transform',
                                'translate(' +
                                    ConnectionManager.BACKPRESSURE_DATASIZE_OFFSET +
                                    ', ' +
                                    yBackpressureOffset +
                                    ')'
                            )
                            .attr('class', 'backpressure-data-size-container');

                        // start
                        backpressureDataSizeContainer
                            .append('rect')
                            .attr('class', 'backpressure-tick data-size')
                            .attr('width', 1)
                            .attr('height', 3)
                            .attr('x', 0)
                            .attr('y', 0);

                        // bar
                        backpressureDataSizeContainer
                            .append('rect')
                            .attr('class', 'backpressure-data-size')
                            .attr('width', ConnectionManager.BACKPRESSURE_BAR_WIDTH)
                            .attr('height', 3)
                            .attr('x', 0)
                            .attr('y', 0)
                            .append('title');

                        // end
                        backpressureDataSizeContainer
                            .append('rect')
                            .attr('class', 'backpressure-tick data-size')
                            .attr('width', 1)
                            .attr('height', 3)
                            .attr('x', ConnectionManager.BACKPRESSURE_BAR_WIDTH)
                            .attr('y', 0);

                        // percent full
                        backpressureDataSizeContainer
                            .append('rect')
                            .attr('class', 'backpressure-percent data-size')
                            .attr('width', 0)
                            .attr('height', 3)
                            .attr('x', 0)
                            .attr('y', 0);

                        // prediction indicator
                        backpressureDataSizeContainer
                            .append('rect')
                            .attr('class', 'backpressure-tick data-size-prediction')
                            .attr('width', 1)
                            .attr('height', 3)
                            .attr('x', ConnectionManager.BACKPRESSURE_BAR_WIDTH)
                            .attr('y', 0);
                    } else {
                        backgrounds.push(queued.select('rect.connection-label-background'));
                        borders.push(queued.select('rect.connection-label-border'));
                    }

                    // update the queued vertical positioning as necessary
                    queued.attr('transform', function () {
                        const y: number = rowHeight * labelCount++;
                        return 'translate(0, ' + y + ')';
                    });

                    // update the height based on the labels being rendered
                    connectionLabelContainer
                        .select('rect.body')
                        .attr('height', function () {
                            return rowHeight * labelCount + ConnectionManager.HEIGHT_FOR_BACKPRESSURE;
                        })
                        .classed('unauthorized', function () {
                            return d.permissions.canRead === false;
                        });
                    connectionLabelContainer
                        .select('rect.border')
                        .attr('height', function () {
                            return rowHeight * labelCount + ConnectionManager.HEIGHT_FOR_BACKPRESSURE;
                        })
                        .classed('unauthorized', function () {
                            return d.permissions.canRead === false;
                        });

                    // update the coloring of the backgrounds
                    backgrounds.forEach((background, i) => {
                        if (i % 2 === 0) {
                            background.attr('class', 'surface-darker');
                        } else {
                            background.attr('class', 'surface-default');
                        }
                    });

                    // update the coloring of the label borders
                    borders.forEach((border, i) => {
                        if (i > 0) {
                            border.attr('class', 'nifi-surface-default');
                        } else {
                            border.attr('class', 'transparent');
                        }
                    });

                    // determine whether or not to show the load-balance icon
                    connectionLabelContainer
                        .select('text.load-balance-icon')
                        .classed('hidden', function () {
                            if (d.permissions.canRead) {
                                return !self.isLoadBalanceConfigured(d.component);
                            } else {
                                return true;
                            }
                        })
                        .classed('load-balance-icon-active fa-rotate-90', function (d: any) {
                            return d.permissions.canRead && d.component.loadBalanceStatus === 'LOAD_BALANCE_ACTIVE';
                        })
                        .classed('load-balance-icon-184', function () {
                            return d.permissions.canRead && self.isExpirationConfigured(d.component);
                        })
                        .classed('load-balance-icon-200', function () {
                            return d.permissions.canRead && !self.isExpirationConfigured(d.component);
                        })
                        .attr('x', function () {
                            return d.permissions.canRead && self.isExpirationConfigured(d.component) ? 192 : 208;
                        })
                        .select('title')
                        .text(function () {
                            if (d.permissions.canRead) {
                                let loadBalanceStrategyText = '';

                                const loadBalanceStrategyOption: SelectOption | undefined = loadBalanceStrategies.find(
                                    (option) => option.value == d.component.loadBalanceStrategy
                                );
                                if (loadBalanceStrategyOption) {
                                    loadBalanceStrategyText = loadBalanceStrategyOption.text;
                                }

                                if ('PARTITION_BY_ATTRIBUTE' === d.component.loadBalanceStrategy) {
                                    loadBalanceStrategyText += ' (' + d.component.loadBalancePartitionAttribute + ')';
                                }

                                let loadBalanceCompression = 'no compression';
                                switch (d.component.loadBalanceCompression) {
                                    case 'COMPRESS_ATTRIBUTES_ONLY':
                                        loadBalanceCompression = "'Attribute' compression";
                                        break;
                                    case 'COMPRESS_ATTRIBUTES_AND_CONTENT':
                                        loadBalanceCompression = "'Attribute and content' compression";
                                        break;
                                }

                                const loadBalanceStatus: string =
                                    'LOAD_BALANCE_ACTIVE' === d.component.loadBalanceStatus
                                        ? ' Actively balancing...'
                                        : '';
                                return (
                                    'Load Balance is configured' +
                                    " with '" +
                                    loadBalanceStrategyText +
                                    "' strategy" +
                                    ' and ' +
                                    loadBalanceCompression +
                                    '.' +
                                    loadBalanceStatus
                                );
                            } else {
                                return '';
                            }
                        });

                    // determine whether or not to show the expiration icon
                    connectionLabelContainer
                        .select('text.expiration-icon')
                        .classed('hidden', function () {
                            if (d.permissions.canRead) {
                                return !self.isExpirationConfigured(d.component);
                            } else {
                                return true;
                            }
                        })
                        .select('title')
                        .text(function () {
                            if (d.permissions.canRead) {
                                return 'Expires FlowFiles older than ' + d.component.flowFileExpiration;
                            } else {
                                return '';
                            }
                        });

                    // update backpressure object fill
                    connectionLabelContainer.select('rect.backpressure-object').classed('not-configured', function () {
                        return d.status.aggregateSnapshot.percentUseCount == null;
                    });
                    connectionLabelContainer
                        .selectAll('rect.backpressure-tick.object')
                        .classed('not-configured', function () {
                            return d.status.aggregateSnapshot.percentUseCount == null;
                        });
                    connectionLabelContainer
                        .selectAll('rect.backpressure-tick.object-prediction')
                        .classed('not-configured', function () {
                            return d.status.aggregateSnapshot.percentUseCount == null;
                        });

                    // update backpressure data size fill
                    connectionLabelContainer
                        .select('rect.backpressure-data-size')
                        .classed('not-configured', function () {
                            return d.status.aggregateSnapshot.percentUseBytes == null;
                        });
                    connectionLabelContainer
                        .selectAll('rect.backpressure-tick.data-size')
                        .classed('not-configured', function () {
                            return d.status.aggregateSnapshot.percentUseBytes == null;
                        });
                    connectionLabelContainer
                        .selectAll('rect.backpressure-tick.data-size-prediction')
                        .classed('not-configured', function () {
                            return d.status.aggregateSnapshot.percentUseBytes == null;
                        });

                    if (d.permissions.canWrite) {
                        // only support dragging the label when appropriate
                        connectionLabelContainer.call(self.labelDrag);
                    }

                    // update the connection status
                    self.updateConnectionStatus(connection);
                } else {
                    if (!connectionLabelContainer.empty()) {
                        connectionLabelContainer.remove();
                    }
                }
            }

            // update the position of the label if possible
            self.transitionBehavior
                .transition(connection.select('g.connection-label-container'), self.transitionRequired)
                .attr('transform', function (this: any) {
                    const label: any = d3.select(this).select('rect.body');
                    const position: Position = self.getLabelPosition(label);
                    return 'translate(' + position.x + ', ' + position.y + ')';
                });
        });
    }

    private updateConnectionStatus(updated: any): void {
        if (updated.empty()) {
            return;
        }
        const self: ConnectionManager = this;

        // penalized icon
        const connectionLabelContainer: any = updated.select('g.connection-label-container');
        if (connectionLabelContainer.select('text.penalized-icon').empty()) {
            connectionLabelContainer
                .select('g.queued-container')
                .append('text')
                .attr('class', 'penalized-icon')
                .attr('y', 14)
                .text(function () {
                    return '\uf252';
                })
                .append('title');
        }

        // determine whether or not to show the penalized icon
        connectionLabelContainer
            .select('text.penalized-icon')
            .classed('hidden', function (d: any) {
                // TODO - optional chaining
                const flowFileAvailability: string = d.status.aggregateSnapshot.flowFileAvailability;
                return flowFileAvailability !== 'HEAD_OF_QUEUE_PENALIZED';
            })
            .attr('x', function () {
                let offset = 0;
                if (!connectionLabelContainer.select('text.expiration-icon').classed('hidden')) {
                    offset += 16;
                }
                if (!connectionLabelContainer.select('text.load-balance-icon').classed('hidden')) {
                    offset += 16;
                }
                return 208 - offset;
            })
            .select('title')
            .text(function () {
                return 'A FlowFile is currently penalized and data cannot be processed at this time.';
            });

        // update connection once progress bars have transitioned
        Promise.all([this.updateDataSize(updated), this.updateObjectCount(updated)]).then(() => {
            // connection stroke
            updated
                .select('path.connection-path')
                .classed('full', function (d: any) {
                    return self.isFullCount(d) || self.isFullBytes(d);
                })
                .attr('marker-end', function (d: any) {
                    let marker = 'normal';

                    if (d.permissions.canRead) {
                        // if the connection has a relationship that is unavailable, mark it a ghost relationship
                        if (self.isFullBytes(d) || self.isFullCount(d)) {
                            marker = 'full';
                        } else if (self.hasUnavailableRelationship(d)) {
                            marker = 'ghost';
                        }
                    } else {
                        marker = 'unauthorized';
                    }

                    return 'url(#' + marker + ')';
                });

            // border
            updated.select('rect.border').classed('full', function (d: any) {
                return self.isFullCount(d) || self.isFullBytes(d);
            });

            // drop shadow
            updated.select('rect.body').attr('filter', function (d: any) {
                if (self.isFullCount(d) || self.isFullBytes(d)) {
                    return 'url(#connection-full-drop-shadow)';
                } else {
                    return 'url(#component-drop-shadow)';
                }
            });
        });
    }

    private async updateDataSize(updated: any) {
        const self: ConnectionManager = this;
        await new Promise<void>(function (resolve) {
            // queued count value
            updated.select('text.queued tspan.count').text(function (d: any) {
                return self.nifiCommon.substringBeforeFirst(d.status.aggregateSnapshot.queued, ' ');
            });

            const backpressurePercentDataSize: any = updated.select('rect.backpressure-percent.data-size');
            backpressurePercentDataSize
                .transition()
                .duration(400)
                .attr('width', function (d: any) {
                    if (d.status.aggregateSnapshot.percentUseBytes != null) {
                        return (
                            (ConnectionManager.BACKPRESSURE_BAR_WIDTH * d.status.aggregateSnapshot.percentUseBytes) /
                            100
                        );
                    } else {
                        return 0;
                    }
                })
                .on('end', function () {
                    backpressurePercentDataSize
                        .classed('warning', function (d: any) {
                            return self.isWarningBytes(d);
                        })
                        .classed('error', function (d: any) {
                            return self.isErrorBytes(d);
                        });

                    resolve();
                });

            const backpressurePercentDataSizePrediction: any = updated.select(
                'rect.backpressure-tick.data-size-prediction'
            );
            backpressurePercentDataSizePrediction
                .transition()
                .duration(400)
                .attr('x', function (d: any) {
                    // clamp the prediction between 0 and 100 percent
                    const predicted = d.status.aggregateSnapshot.predictions?.predictedPercentBytes ?? 0;
                    return (ConnectionManager.BACKPRESSURE_BAR_WIDTH * Math.min(Math.max(predicted, 0), 100)) / 100;
                })
                .attr('display', function (d: any) {
                    const predicted: number = d.status.aggregateSnapshot.predictions?.predictedPercentBytes ?? -1;
                    if (predicted >= 0) {
                        return 'unset nifi-surface-default';
                    } else {
                        // don't show it if there is not a valid prediction
                        return 'none';
                    }
                })
                .on('end', function () {
                    backpressurePercentDataSizePrediction.classed('prediction-down', function (d: any) {
                        const actual: number = d.status.aggregateSnapshot.predictions?.percentUseBytes ?? 0;
                        const predicted: number = d.status.aggregateSnapshot.predictions?.predictedPercentBytes ?? 0;
                        return predicted < actual;
                    });
                });

            updated.select('g.backpressure-data-size-container').each(function (this: any, d: any) {
                if (self.viewContainerRef) {
                    self.canvasUtils.canvasTooltip(self.viewContainerRef, UnorderedListTip, d3.select(this), {
                        items: self.getBackPressureSizeTip(d)
                    });
                }
            });
        });
    }

    private async updateObjectCount(updated: any) {
        const self: ConnectionManager = this;
        await new Promise<void>(function (resolve) {
            // queued size value
            updated.select('text.queued tspan.size').text(function (d: any) {
                return ' ' + self.nifiCommon.substringAfterFirst(d.status.aggregateSnapshot.queued, ' ');
            });

            const backpressurePercentObject: any = updated.select('rect.backpressure-percent.object');
            backpressurePercentObject
                .transition()
                .duration(400)
                .attr('width', function (d: any) {
                    if (d.status.aggregateSnapshot.percentUseCount != null) {
                        return (
                            (ConnectionManager.BACKPRESSURE_BAR_WIDTH * d.status.aggregateSnapshot.percentUseCount) /
                            100
                        );
                    } else {
                        return 0;
                    }
                })
                .on('end', function () {
                    backpressurePercentObject
                        .classed('warning', function (d: any) {
                            return self.isWarningCount(d);
                        })
                        .classed('error', function (d: any) {
                            return self.isErrorCount(d);
                        });

                    resolve();
                });

            const backpressurePercentObjectPrediction: any = updated.select('rect.backpressure-tick.object-prediction');
            backpressurePercentObjectPrediction
                .transition()
                .duration(400)
                .attr('x', function (d: any) {
                    // clamp the prediction between 0 and 100 percent
                    const predicted: number = d.status.aggregateSnapshot.predictions?.predictedPercentCount ?? 0;
                    return (ConnectionManager.BACKPRESSURE_BAR_WIDTH * Math.min(Math.max(predicted, 0), 100)) / 100;
                })
                .attr('display', function (d: any) {
                    const predicted = d.status.aggregateSnapshot.predictions?.predictedPercentCount ?? -1;
                    if (predicted >= 0) {
                        return 'unset nifi-surface-default';
                    } else {
                        // don't show it if there not a valid prediction
                        return 'none';
                    }
                })
                .on('end', function () {
                    backpressurePercentObjectPrediction.classed('prediction-down', function (d: any) {
                        // TODO - optional chaining with default ?? 0
                        const actual = d.status.aggregateSnapshot.percentUseCount;
                        const predicted = d.status.aggregateSnapshot.predictions?.predictedPercentCount ?? 0;
                        return predicted < actual;
                    });
                });

            updated.select('g.backpressure-object-container').each(function (this: any, d: any) {
                if (self.viewContainerRef) {
                    self.canvasUtils.canvasTooltip(self.viewContainerRef, UnorderedListTip, d3.select(this), {
                        items: self.getBackPressureCountTip(d)
                    });
                }
            });
        });
    }

    private removeProcessGroups(removed: any): void {
        if (removed.empty()) {
            return;
        }

        removed.remove();
    }

    public init(viewContainerRef: ViewContainerRef): void {
        const self: ConnectionManager = this;
        this.viewContainerRef = viewContainerRef;

        this.connectionContainer = d3
            .select('#canvas')
            .append('g')
            .attr('pointer-events', 'stroke')
            .attr('class', 'connections');

        // define the line generator
        this.lineGenerator = d3
            .line()
            .x(function (d: any) {
                return d.x;
            })
            .y(function (d: any) {
                return d.y;
            })
            .curve(d3.curveLinear);

        // handle bend point drag events
        this.bendPointDrag = d3
            .drag()
            .on('start', function (event) {
                // stop further propagation
                event.sourceEvent.stopPropagation();
            })
            .on('drag', function (this: any, event, d: any) {
                self.snapEnabled = !event.sourceEvent.shiftKey;
                d.x = self.snapEnabled
                    ? Math.round(event.x / ConnectionManager.SNAP_ALIGNMENT_PIXELS) *
                      ConnectionManager.SNAP_ALIGNMENT_PIXELS
                    : event.x;
                d.y = self.snapEnabled
                    ? Math.round(event.y / ConnectionManager.SNAP_ALIGNMENT_PIXELS) *
                      ConnectionManager.SNAP_ALIGNMENT_PIXELS
                    : event.y;

                // redraw this connection
                self.updateConnections(d3.select(this.parentNode), {
                    updatePath: true,
                    updateLabel: false
                });
            })
            .on('end', function (this: any, event) {
                const connection: any = d3.select(this.parentNode);
                const connectionData: any = connection.datum();
                const bends: Position[] = connection.selectAll('rect.midpoint').data();

                // ensure the bend lengths are the same
                if (bends.length === connectionData.component.bends.length) {
                    // determine if the bend points have moved
                    let different = false;
                    for (let i = 0; i < bends.length && !different; i++) {
                        if (
                            bends[i].x !== connectionData.component.bends[i].x ||
                            bends[i].y !== connectionData.component.bends[i].y
                        ) {
                            different = true;
                        }
                    }

                    // only save the updated bends if necessary
                    if (different) {
                        self.save(
                            connectionData,
                            {
                                id: connectionData.id,
                                bends: bends
                            },
                            {
                                bends: [...connectionData.component.bends]
                            }
                        );
                    }
                }

                // stop further propagation
                event.sourceEvent.stopPropagation();
            });

        // handle endpoint drag events
        this.endpointDrag = d3
            .drag()
            .on('start', function (event, d: any) {
                // indicate that dragging has begun
                d.dragging = true;

                // stop further propagation
                event.sourceEvent.stopPropagation();
            })
            .on('drag', function (this: any, event, d: any) {
                d.x = event.x - 8;
                d.y = event.y - 8;

                // ensure the new destination is valid
                d3.select('g.hover').classed('connectable-destination', function () {
                    return self.canvasUtils.isValidConnectionDestination(d3.select(this));
                });

                // redraw this connection
                self.updateConnections(d3.select(this.parentNode), {
                    updatePath: true,
                    updateLabel: false
                });
            })
            .on('end', function (this: any, event, d: any) {
                // indicate that dragging as stopped
                d.dragging = false;

                // get the corresponding connection
                const connection: any = d3.select(this.parentNode);
                const connectionData: any = connection.datum();

                // attempt to select a new destination
                const destination: any = d3.select('g.connectable-destination');

                // resets the connection if we're not over a new destination
                if (destination.empty()) {
                    self.updateConnections(connection, {
                        updatePath: true,
                        updateLabel: false
                    });
                } else {
                    const destinationData: any = destination.datum();

                    // prompt for the new port if appropriate
                    if (
                        self.canvasUtils.isProcessGroup(destination) ||
                        self.canvasUtils.isRemoteProcessGroup(destination)
                    ) {
                        // when the new destination is a group, show the edit connection dialog
                        // to allow the user to select the desired port
                        self.store.dispatch(
                            openEditConnectionDialog({
                                request: {
                                    type: ComponentType.Connection,
                                    uri: connectionData.uri,
                                    entity: { ...connectionData },
                                    newDestination: {
                                        type: destinationData.type,
                                        groupId: destinationData.id,
                                        name: destinationData.permissions.canRead
                                            ? destinationData.component.name
                                            : destinationData.id
                                    }
                                }
                            })
                        );
                    } else {
                        const destinationType: string = self.canvasUtils.getConnectableTypeForDestination(
                            destinationData.type
                        );

                        const payload: any = {
                            revision: self.client.getRevision(connectionData),
                            disconnectedNodeAcknowledged: self.clusterConnectionService.isDisconnectionAcknowledged(),
                            component: {
                                id: connectionData.id,
                                destination: {
                                    id: destinationData.id,
                                    groupId: self.currentProcessGroupId,
                                    type: destinationType
                                }
                            }
                        };

                        // if this is a self loop and there are less than 2 bends, add them
                        if (connectionData.bends.length < 2 && connectionData.sourceId === destinationData.id) {
                            const rightCenter: any = {
                                x: destinationData.position.x + destinationData.dimensions.width,
                                y: destinationData.position.y + destinationData.dimensions.height / 2
                            };

                            payload.component.bends = [];
                            payload.component.bends.push({
                                x: rightCenter.x + ConnectionManager.SELF_LOOP_X_OFFSET,
                                y: rightCenter.y - ConnectionManager.SELF_LOOP_Y_OFFSET
                            });
                            payload.component.bends.push({
                                x: rightCenter.x + ConnectionManager.SELF_LOOP_X_OFFSET,
                                y: rightCenter.y + ConnectionManager.SELF_LOOP_Y_OFFSET
                            });
                        }

                        self.store.dispatch(
                            updateConnection({
                                request: {
                                    id: connectionData.id,
                                    type: ComponentType.Connection,
                                    uri: connectionData.uri,
                                    previousDestination: connectionData.component.destination,
                                    payload
                                }
                            })
                        );
                    }
                }

                // stop further propagation
                event.sourceEvent.stopPropagation();
            });

        // label drag behavior
        this.labelDrag = d3
            .drag()
            .on('start', function (event) {
                // stop further propagation
                event.sourceEvent.stopPropagation();
            })
            .on('drag', function (this: any, event, d: any) {
                if (d && d.bends.length > 1) {
                    // get the dragged component
                    let drag: any = d3.select('rect.label-drag');

                    // lazily create the drag selection box
                    if (drag.empty()) {
                        const connectionLabel: any = d3.select(this).select('rect.body');

                        const position: Position = self.getLabelPosition(connectionLabel);
                        const width: number = ConnectionManager.DIMENSIONS.width;
                        const height: number = connectionLabel.attr('height');

                        // create a selection box for the move
                        drag = d3
                            .select('#canvas')
                            .append('rect')
                            .attr('x', position.x)
                            .attr('y', position.y)
                            .attr('class', 'label-drag')
                            .attr('width', width)
                            .attr('height', height)
                            .attr('stroke-width', function () {
                                return 1 / self.scale;
                            })
                            .attr('stroke-dasharray', function () {
                                return 4 / self.scale;
                            })
                            .datum({
                                x: position.x,
                                y: position.y,
                                width: width,
                                height: height
                            });
                    } else {
                        // update the position of the drag selection
                        drag.attr('x', function (d: any) {
                            d.x += event.dx;
                            return d.x;
                        }).attr('y', function (d: any) {
                            d.y += event.dy;
                            return d.y;
                        });
                    }

                    // calculate the current point
                    const datum: any = drag.datum();
                    const currentPoint: Position = {
                        x: datum.x + datum.width / 2,
                        y: datum.y + datum.height / 2
                    };

                    let closestBendIndex = -1;
                    let minDistance: number;
                    d.bends.forEach((bend: Position, i: number) => {
                        const bendPoint: Position = {
                            x: bend.x,
                            y: bend.y
                        };

                        // get the distance
                        const distance: number = self.distanceBetweenPoints(currentPoint, bendPoint);

                        // see if its the minimum
                        if (closestBendIndex === -1 || distance < minDistance) {
                            closestBendIndex = i;
                            minDistance = distance;
                        }
                    });

                    // record the closest bend
                    d.labelIndex = closestBendIndex;

                    // refresh the connection
                    self.updateConnections(d3.select(this.parentNode), {
                        updatePath: true,
                        updateLabel: false
                    });
                }
            })
            .on('end', function (this: any, event, d: any) {
                if (d.bends.length > 1) {
                    // get the drag selection
                    const drag: any = d3.select('rect.label-drag');

                    // ensure we found a drag selection
                    if (!drag.empty()) {
                        // remove the drag selection
                        drag.remove();
                    }

                    // only save if necessary
                    if (d.labelIndex !== d.component.labelIndex) {
                        self.save(
                            d,
                            {
                                id: d.id,
                                labelIndex: d.labelIndex
                            },
                            {
                                labelIndex: d.component.labelIndex
                            }
                        );
                    }
                }

                // stop further propagation
                event.sourceEvent.stopPropagation();
            });

        this.store
            .select(selectConnections)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((connections) => {
                this.set(connections);
            });

        this.store
            .select(selectFlowLoadingStatus)
            .pipe(
                filter((status) => status === 'success'),
                switchMap(() => this.store.select(selectAnySelectedComponentIds)),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe((selected) => {
                this.connectionContainer.selectAll('g.connection').classed('selected', function (d: any) {
                    return selected.includes(d.id);
                });
            });

        this.store
            .select(selectTransitionRequired)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((transitionRequired) => {
                this.transitionRequired = transitionRequired;
            });

        this.store
            .select(selectCurrentProcessGroupId)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((currentProcessGroupId) => {
                this.currentProcessGroupId = currentProcessGroupId;
            });

        this.store
            .select(selectTransform)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((transform) => {
                this.scale = transform.scale;
            });
    }

    private set(connections: any): void {
        // update the connections
        this.connections = connections.map((connection: any) => {
            return {
                ...connection,
                type: ComponentType.Connection,
                bends: connection.bends.map((bend: Position) => {
                    return { ...bend };
                })
            };
        });

        // select
        const selection = this.select();

        // enter
        const entered = this.renderConnections(selection.enter());

        // update
        const updated = selection.merge(entered);
        this.updateConnections(updated);
        this.sort(updated);

        // exit
        this.removeProcessGroups(selection.exit());
    }

    public selectAll(): any {
        return this.connectionContainer.selectAll('g.connection');
    }

    public render({ updatePath = false, updateLabel = true }: ConnectionRenderOptions = {}): void {
        this.updateConnections(this.selectAll(), { updatePath, updateLabel });
    }

    public renderConnection(
        id: string,
        { updatePath = false, updateLabel = true }: ConnectionRenderOptions = {}
    ): void {
        this.updateConnections(this.connectionContainer.select('#id-' + id), { updatePath, updateLabel });
    }

    public renderConnectionsForComponent(
        componentId: string,
        { updatePath = false, updateLabel = true }: ConnectionRenderOptions = {}
    ): void {
        const componentConnections: any[] = this.connections.filter((connection) => {
            return (
                this.canvasUtils.getConnectionSourceComponentId(connection) === componentId ||
                this.canvasUtils.getConnectionDestinationComponentId(connection) === componentId
            );
        });

        componentConnections.forEach((componentConnection) => {
            this.renderConnection(componentConnection.id, { updatePath, updateLabel });
        });
    }

    public pan(): void {
        this.updateConnections(this.connectionContainer.selectAll('g.connection.entering, g.connection.leaving'), {
            updatePath: false,
            updateLabel: true
        });
    }
}
