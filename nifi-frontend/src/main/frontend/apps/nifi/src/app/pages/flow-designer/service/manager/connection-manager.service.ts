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

import { Injectable, OnDestroy, inject } from '@angular/core';
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
import { UnorderedListTip } from '../../../../ui/common/tooltips/unordered-list-tip/unordered-list-tip.component';
import { Dimension, Position } from '../../state/shared';
import { loadBalanceStrategies, UpdateComponentRequest } from '../../state/flow';
import { filter, Subject, switchMap, takeUntil } from 'rxjs';
import { ComponentType, NiFiCommon, SelectOption } from '@nifi/shared';
import { QuickSelectBehavior } from '../behavior/quick-select-behavior.service';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { wouldRemovalCauseOverlap } from '../../../../ui/common/overlap-detection.utils';

export class ConnectionRenderOptions {
    updatePath?: boolean;
    updateLabel?: boolean;
}

@Injectable({
    providedIn: 'root'
})
export class ConnectionManager implements OnDestroy {
    private store = inject<Store<CanvasState>>(Store);
    private canvasUtils = inject(CanvasUtils);
    private nifiCommon = inject(NiFiCommon);
    private client = inject(Client);
    private selectableBehavior = inject(SelectableBehavior);
    private transitionBehavior = inject(TransitionBehavior);
    private quickSelectBehavior = inject(QuickSelectBehavior);
    private clusterConnectionService = inject(ClusterConnectionService);

    private destroyed$: Subject<boolean> = new Subject();

    private static readonly DIMENSIONS: Dimension = {
        width: 240,
        height: 0
    };

    private static readonly BACKPRESSURE_BAR_WIDTH: number = ConnectionManager.DIMENSIONS.width / 2 - 15 - 2;
    private static readonly BACKPRESSURE_COUNT_OFFSET: number = 6;
    private static readonly BACKPRESSURE_DATASIZE_OFFSET: number = ConnectionManager.DIMENSIONS.width / 2 + 10 + 1;

    public static readonly SELF_LOOP_X_OFFSET: number = ConnectionManager.DIMENSIONS.width / 2 + 5;
    public static readonly SELF_LOOP_Y_OFFSET: number = 25;

    public static readonly CONNECTION_OFFSET_Y_INCREMENT: number = 75;
    public static readonly CONNECTION_OFFSET_X_INCREMENT: number = 250;

    private static readonly HEIGHT_FOR_BACKPRESSURE: number = 3;

    private static readonly SNAP_ALIGNMENT_PIXELS: number = 8;

    private connections: [] = [];
    private connectionContainer: any = null;
    private transitionRequired = false;
    private currentProcessGroupId: string = initialState.id;
    private scale: number = INITIAL_SCALE;

    private lineGenerator: any;

    private bendPointDrag: any;
    private endpointDrag: any;
    private labelDrag: any;

    private snapEnabled = true;

    constructor() {
        // define the line generator
        this.lineGenerator = d3
            .line()
            .x((d: any) => d.x)
            .y((d: any) => d.y)
            .curve(d3.curveLinear);

        // handle bend point drag events
        this.bendPointDrag = d3
            .drag()
            .subject(function (this: Element) {
                // Capture DOM element via subject; currentTarget is valid during the initiating mousedown.
                return { element: this };
            })
            .on('start', (event: any) => {
                // stop further propagation
                event.sourceEvent.stopPropagation();

                // indicate dragging start
                const connection: any = d3.select(event.subject.element.parentNode as Element);
                const connectionData: any = connection.datum();
                connectionData.dragging = true;
            })
            .on('drag', (event: any, d: any) => {
                const connection: any = d3.select(event.subject.element.parentNode as Element);
                const connectionData: any = connection.datum();

                if (connectionData.dragging) {
                    this.snapEnabled = !event.sourceEvent.shiftKey;
                    d.x = this.snapEnabled
                        ? Math.round(event.x / ConnectionManager.SNAP_ALIGNMENT_PIXELS) *
                          ConnectionManager.SNAP_ALIGNMENT_PIXELS
                        : event.x;
                    d.y = this.snapEnabled
                        ? Math.round(event.y / ConnectionManager.SNAP_ALIGNMENT_PIXELS) *
                          ConnectionManager.SNAP_ALIGNMENT_PIXELS
                        : event.y;

                    // redraw this connection
                    this.updateConnections(d3.select(event.subject.element.parentNode as Element), {
                        updatePath: true,
                        updateLabel: false
                    });
                }
            })
            .on('end', (event: any) => {
                const connection: any = d3.select(event.subject.element.parentNode as Element);
                const connectionData: any = connection.datum();

                if (connectionData.dragging) {
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
                            this.save(
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
                }

                // stop further propagation
                event.sourceEvent.stopPropagation();

                // indicate dragging complete
                connectionData.dragging = false;
            });

        // handle endpoint drag events
        this.endpointDrag = d3
            .drag()
            .subject(function (this: Element) {
                // Capture DOM element via subject; currentTarget is valid during the initiating mousedown.
                return { element: this };
            })
            .on('start', (event: any, d: any) => {
                // indicate that end point dragging has begun
                d.endPointDragging = true;

                // stop further propagation
                event.sourceEvent.stopPropagation();

                // indicate dragging start
                const connection: any = d3.select(event.subject.element.parentNode as Element);
                const connectionData: any = connection.datum();
                connectionData.dragging = true;
            })
            .on('drag', (event: any, d: any) => {
                const connection: any = d3.select(event.subject.element.parentNode as Element);
                const connectionData: any = connection.datum();

                if (connectionData.dragging) {
                    d.x = event.x - 8;
                    d.y = event.y - 8;

                    // ensure the new destination is valid
                    d3.select('g.hover').classed('connectable-destination', (d: any, i: number, nodes: any) =>
                        this.canvasUtils.isValidConnectionDestination(d3.select(nodes[i]))
                    );

                    // redraw this connection
                    this.updateConnections(d3.select(event.subject.element.parentNode as Element), {
                        updatePath: true,
                        updateLabel: false
                    });
                }
            })
            .on('end', (event: any, d: any) => {
                // indicate that end point dragging as stopped
                d.endPointDragging = false;

                // get the corresponding connection
                const connection: any = d3.select(event.subject.element.parentNode as Element);
                const connectionData: any = connection.datum();

                if (connectionData.dragging) {
                    // attempt to select a new destination
                    const destination: any = d3.select('g.connectable-destination');

                    // resets the connection if we're not over a new destination
                    if (destination.empty()) {
                        this.updateConnections(connection, {
                            updatePath: true,
                            updateLabel: false
                        });
                    } else {
                        const destinationData: any = destination.datum();

                        // prompt for the new port if appropriate
                        if (
                            this.canvasUtils.isProcessGroup(destination) ||
                            this.canvasUtils.isRemoteProcessGroup(destination)
                        ) {
                            // when the new destination is a group, show the edit connection dialog
                            // to allow the user to select the desired port
                            this.store.dispatch(
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
                            const destinationType: string = this.canvasUtils.getConnectableTypeForDestination(
                                destinationData.type
                            );

                            const payload: any = {
                                revision: this.client.getRevision(connectionData),
                                disconnectedNodeAcknowledged:
                                    this.clusterConnectionService.isDisconnectionAcknowledged(),
                                component: {
                                    id: connectionData.id,
                                    destination: {
                                        id: destinationData.id,
                                        groupId: this.currentProcessGroupId,
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
                            } else if (this.nifiCommon.isEmpty(connectionData.bends)) {
                                const sourceComponentId =
                                    this.canvasUtils.getConnectionSourceComponentId(connectionData);
                                const collisionBends = this.canvasUtils.calculateBendPointsForCollisionAvoidanceByIds(
                                    sourceComponentId,
                                    destinationData.id,
                                    connectionData.id
                                );
                                if (collisionBends.length > 0) {
                                    payload.component.bends = collisionBends;
                                }
                            }

                            this.store.dispatch(
                                updateConnection({
                                    request: {
                                        id: connectionData.id,
                                        type: ComponentType.Connection,
                                        uri: connectionData.uri,
                                        previousDestination: connectionData.component.destination,
                                        payload,
                                        errorStrategy: 'snackbar'
                                    }
                                })
                            );
                        }
                    }
                }

                // stop further propagation
                event.sourceEvent.stopPropagation();

                // indicate dragging complete
                connectionData.dragging = false;
            });

        // label drag behavior
        this.labelDrag = d3
            .drag()
            .subject(function (this: Element) {
                // Capture DOM element via subject; currentTarget is valid during the initiating mousedown.
                return { element: this };
            })
            .on('start', (event: any, d: any) => {
                // stop further propagation
                event.sourceEvent.stopPropagation();

                // indicate dragging start
                d.dragging = true;
            })
            .on('drag', (event: any, d: any) => {
                if (d.dragging && d.bends.length > 1) {
                    // get the dragged component
                    let drag: any = d3.select('rect.label-drag');

                    // lazily create the drag selection box
                    if (drag.empty()) {
                        const connectionLabel: any = d3.select(event.subject.element as Element).select('rect.body');

                        const position: Position = this.getLabelPosition(connectionLabel);
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
                            .attr('stroke-width', () => 1 / this.scale)
                            .attr('stroke-dasharray', () => 4 / this.scale)
                            .datum({
                                x: position.x,
                                y: position.y,
                                width: width,
                                height: height
                            });
                    } else {
                        // update the position of the drag selection
                        drag.attr('x', (d: any) => {
                            d.x += event.dx;
                            return d.x;
                        }).attr('y', (d: any) => {
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
                        const distance: number = this.distanceBetweenPoints(currentPoint, bendPoint);

                        // see if its the minimum
                        if (closestBendIndex === -1 || distance < minDistance) {
                            closestBendIndex = i;
                            minDistance = distance;
                        }
                    });

                    // record the closest bend
                    d.labelIndex = closestBendIndex;

                    // refresh the connection
                    this.updateConnections(d3.select(event.subject.element.parentNode as Element), {
                        updatePath: true,
                        updateLabel: false
                    });
                }
            })
            .on('end', (event: any, d: any) => {
                if (d.dragging && d.bends.length > 1) {
                    // get the drag selection
                    const drag: any = d3.select('rect.label-drag');

                    // ensure we found a drag selection
                    if (!drag.empty()) {
                        // remove the drag selection
                        drag.remove();
                    }

                    // only save if necessary
                    if (d.labelIndex !== d.component.labelIndex) {
                        this.save(
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

                // indicate dragging complete
                d.dragging = false;
            });
    }

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
            const match: string[] = connection.flowFileExpiration.match(/^(\d*\.?\d+).*/);
            if (match !== null && match.length > 0) {
                if (parseFloat(match[1]) > 0) {
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
     * Determines whether retried relationships are configured for the specified connection.
     *
     * @param {object} connection
     * @return {boolean} Whether retried relationships are configured
     */
    private isRetryConfigured(connection: any): boolean {
        return connection.retriedRelationships != null && connection.retriedRelationships.length > 0;
    }

    /**
     * Sorts the specified connections according to the z index.
     *
     * @param {type} connections
     */
    private sort(connections: any[]): void {
        connections.sort((a, b) => {
            return this.nifiCommon.compareNumber(a.zIndex, b.zIndex);
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
     * @param d
     * @param connection
     * @param restoreOnFailure
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
            restoreOnFailure: restoreOnFailure,
            errorStrategy: 'snackbar'
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
        return this.connectionContainer.selectAll('g.connection').data(this.connections, (d: any) => d.id);
    }

    private renderConnections(entered: any) {
        if (entered.empty()) {
            return entered;
        }

        const connection = entered
            .append('g')
            .attr('id', (d: any) => 'id-' + d.id)
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
            .on('mousedown.selection', (event: MouseEvent) => {
                // select the connection when clicking the selectable path
                this.selectableBehavior.select(
                    event,
                    d3.select((event.currentTarget as Element).parentNode as Element)
                );
            })
            .on('dblclick', (event: MouseEvent, d: any) => {
                if (d.permissions.canWrite && d.permissions.canRead) {
                    const position = d3.pointer(event, (event.currentTarget as Element).parentNode);

                    // find where to put this bend point
                    const bendIndex = this.getNearestSegment(
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
                    this.save(d, connectionAddedBend);

                    event.stopPropagation();
                }
            });

        return connection;
    }

    private updateConnections(updated: any, { updatePath = true, updateLabel = true }: ConnectionRenderOptions = {}) {
        if (updated.empty()) {
            return;
        }

        if (updatePath) {
            updated
                .classed('grouped', (d: any) => {
                    let grouped = false;

                    if (d.permissions.canRead) {
                        // if there are more than one selected relationship, mark this as grouped
                        if (d.component.selectedRelationships?.length > 1) {
                            grouped = true;
                        }
                    }

                    return grouped;
                })
                .classed('ghost', (d: any) => {
                    let ghost = false;

                    if (d.permissions.canRead) {
                        // if the connection has a relationship that is unavailable, mark it a ghost relationship
                        if (this.hasUnavailableRelationship(d)) {
                            ghost = true;
                        }
                    }

                    return ghost;
                });

            // update connection path
            updated.select('path.connection-path').classed('unauthorized', (d: any) => d.permissions.canRead === false);
        }

        updated.each((d: any, i: number, nodes: Element[]) => {
            const connection: any = d3.select(nodes[i]);

            if (updatePath) {
                // calculate the start and end points
                const sourceComponentId: string = this.canvasUtils.getConnectionSourceComponentId(d);
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
                if (d.end?.endPointDragging) {
                    // since we're dragging, use the same object thats bound to the endpoint drag event
                    end = d.end;

                    // if we're not over a connectable destination use the current point
                    const newDestination: any = d3.select('g.hover.connectable-destination');
                    if (!newDestination.empty()) {
                        const newDestinationData: any = newDestination.datum();

                        // get the position on the new destination perimeter
                        const newEnd: Position = this.canvasUtils.getPerimeterPoint(endAnchor, {
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
                    const destinationComponentId: string = this.canvasUtils.getConnectionDestinationComponentId(d);
                    const destinationData: any = d3.select('#id-' + destinationComponentId).datum();

                    // get the position on the destination perimeter
                    end = this.canvasUtils.getPerimeterPoint(endAnchor, {
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
                const start: Position = this.canvasUtils.getPerimeterPoint(startAnchor, {
                    x: sourceData.position.x,
                    y: sourceData.position.y,
                    width: sourceData.dimensions.width,
                    height: sourceData.dimensions.height
                });

                // store the updated endpoints
                d.start = start;
                d.end = end;

                // update the connection paths
                this.transitionBehavior
                    .transition(connection.select('path.connection-path'), this.transitionRequired)
                    .attr('d', () => {
                        const datum: Position[] = [d.start].concat(d.bends, [d.end]);
                        return this.lineGenerator(datum);
                    });
                this.transitionBehavior
                    .transition(connection.select('path.connection-selection-path'), this.transitionRequired)
                    .attr('d', () => {
                        const datum: Position[] = [d.start].concat(d.bends, [d.end]);
                        return this.lineGenerator(datum);
                    });
                this.transitionBehavior
                    .transition(connection.select('path.connection-path-selectable'), this.transitionRequired)
                    .attr('d', () => {
                        const datum: Position[] = [d.start].concat(d.bends, [d.end]);
                        return this.lineGenerator(datum);
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
                        .on('mousedown.selection', (event: MouseEvent) => {
                            // select the connection when clicking the label
                            this.selectableBehavior.select(
                                event,
                                d3.select((event.currentTarget as Element).parentNode as Element)
                            );
                        });

                    // update the start point
                    this.transitionBehavior
                        .transition(startpoints.merge(startpointsEntered), this.transitionRequired)
                        .attr('transform', (p: any) => 'translate(' + (p.x - 4) + ', ' + (p.y - 4) + ')');

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
                        .on('mousedown.selection', (event: MouseEvent) => {
                            // select the connection when clicking the label
                            this.selectableBehavior.select(
                                event,
                                d3.select((event.currentTarget as Element).parentNode as Element)
                            );
                        })
                        .call(this.endpointDrag);

                    // update the end point
                    this.transitionBehavior
                        .transition(endpoints.merge(endpointsEntered), this.transitionRequired)
                        .attr('transform', (p: any) => 'translate(' + (p.x - 4) + ', ' + (p.y - 4) + ')');

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
                        .on('dblclick', (event: MouseEvent, p: any) => {
                            // stop even propagation
                            event.stopPropagation();

                            const connection: any = d3.select((event.currentTarget as Element).parentNode as Element);
                            const connectionData: any = connection.datum();

                            // if this is a self loop prevent removing the last two bends
                            const sourceComponentId = this.canvasUtils.getConnectionSourceComponentId(connectionData);
                            const destinationComponentId =
                                this.canvasUtils.getConnectionDestinationComponentId(connectionData);
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

                            if (newBends.length === 0 && sourceComponentId !== destinationComponentId) {
                                const wouldOverlap = wouldRemovalCauseOverlap(
                                    connectionData.id,
                                    this.connections,
                                    this.currentProcessGroupId
                                );
                                if (wouldOverlap) {
                                    this.store.dispatch(
                                        showOkDialog({
                                            title: 'Connection',
                                            message:
                                                'This bend point cannot be removed because it would cause this connection to overlap with another connection between the same components.'
                                        })
                                    );
                                    return;
                                }
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
                            this.save(connectionData, connectionRemovedBend);
                        })
                        .on('mousedown.selection', (event: MouseEvent) => {
                            // select the connection when clicking the label
                            this.selectableBehavior.select(
                                event,
                                d3.select((event.currentTarget as Element).parentNode as Element)
                            );
                        })
                        .call(this.bendPointDrag);

                    // update the midpoints
                    this.transitionBehavior
                        .transition(midpoints.merge(midpointsEntered), this.transitionRequired)
                        .attr('transform', (p: any) => 'translate(' + (p.x - 4) + ', ' + (p.y - 4) + ')');

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
                            .on('mousedown.selection', (event: MouseEvent) => {
                                // select the connection when clicking the label
                                this.selectableBehavior.select(
                                    event,
                                    d3.select((event.currentTarget as Element).parentNode as Element)
                                );
                            });

                        this.quickSelectBehavior.activate(connectionLabelContainer);

                        // processor border
                        connectionLabelContainer
                            .append('rect')
                            .attr('class', 'border')
                            .attr('width', ConnectionManager.DIMENSIONS.width)
                            .attr('fill', 'transparent')
                            .attr('stroke', 'transparent');

                        // connection label
                        connectionLabelContainer
                            .append('rect')
                            .attr('class', 'body')
                            .attr('width', ConnectionManager.DIMENSIONS.width)
                            .attr('x', 0)
                            .attr('y', 0);
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
                        if (this.isGroup(d.component.source)) {
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
                                    .attr('width', 146);

                                connectionFrom
                                    .append('text')
                                    .attr('class', 'connection-from-run-status')
                                    .attr('x', 224)
                                    .attr('y', 14);
                            } else {
                                backgrounds.push(connectionFrom.select('rect.connection-label-background'));
                                borders.push(connectionFrom.select('rect.connection-label-border'));
                            }

                            // update the connection from positioning
                            connectionFrom.attr('transform', () => {
                                const y: number = rowHeight * labelCount++;
                                return 'translate(0, ' + y + ')';
                            });

                            // update the label text
                            connectionFrom
                                .select('text.connection-from')
                                .each((_d: any, i: number, nodes: Element[]) => {
                                    const connectionFromLabel = d3.select(nodes[i]);

                                    // reset the label name to handle any previous state
                                    connectionFromLabel.text(null).selectAll('title').remove();

                                    // apply ellipsis to the label as necessary
                                    this.canvasUtils.ellipsis(
                                        connectionFromLabel,
                                        d.component.source.name,
                                        'connection-from'
                                    );
                                })
                                .append('title')
                                .text(() => d.component.source.name);

                            // update the label run status
                            connectionFrom
                                .select('text.connection-from-run-status')
                                .text(() => {
                                    if (d.component.source.exists === false) {
                                        return '\uf071';
                                    } else if (d.component.source.running === true) {
                                        return '\uf04b';
                                    } else {
                                        return '\uf04d';
                                    }
                                })
                                .classed('running success-color-default', () => {
                                    if (d.component.source.exists === false) {
                                        return false;
                                    } else {
                                        return d.component.source.running;
                                    }
                                })
                                .classed('stopped error-color-variant', () => {
                                    if (d.component.source.exists === false) {
                                        return false;
                                    } else {
                                        return !d.component.source.running;
                                    }
                                })
                                .classed(
                                    'is-missing-port invalid caution-color',
                                    () => d.component.source.exists === false
                                );
                        } else {
                            // there is no connection from, remove the previous if necessary
                            connectionFrom.remove();
                        }

                        // ---------------------
                        // connection label - to
                        // ---------------------

                        // determine if the connection require a to label
                        if (this.isGroup(d.component.destination)) {
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
                                    .attr('width', 161);

                                connectionTo
                                    .append('text')
                                    .attr('class', 'connection-to-run-status')
                                    .attr('x', 224)
                                    .attr('y', 14);
                            } else {
                                backgrounds.push(connectionTo.select('rect.connection-label-background'));
                                borders.push(connectionTo.select('rect.connection-label-border'));
                            }

                            // update the connection to positioning
                            connectionTo.attr('transform', () => {
                                const y: number = rowHeight * labelCount++;
                                return 'translate(0, ' + y + ')';
                            });

                            // update the label text
                            connectionTo
                                .select('text.connection-to')
                                .each((_d: any, i: number, nodes: Element[]) => {
                                    const connectionToLabel = d3.select(nodes[i]);

                                    // reset the label name to handle any previous state
                                    connectionToLabel.text(null).selectAll('title').remove();

                                    // apply ellipsis to the label as necessary
                                    this.canvasUtils.ellipsis(
                                        connectionToLabel,
                                        d.component.destination.name,
                                        'connection-to'
                                    );
                                })
                                .append('title')
                                .text(() => d.component.destination.name);

                            // update the label run status
                            connectionTo
                                .select('text.connection-to-run-status')
                                .text(() => {
                                    if (d.component.destination.exists === false) {
                                        return '\uf071';
                                    } else if (d.component.destination.running === true) {
                                        return '\uf04b';
                                    } else {
                                        return '\uf04d';
                                    }
                                })
                                .classed('running success-color-default', () => {
                                    if (d.component.destination.exists === false) {
                                        return false;
                                    } else {
                                        return d.component.destination.running;
                                    }
                                })
                                .classed('stopped error-color-variant', () => {
                                    if (d.component.destination.exists === false) {
                                        return false;
                                    } else {
                                        return !d.component.destination.running;
                                    }
                                })
                                .classed(
                                    'is-missing-port invalid caution-color',
                                    () => d.component.destination.exists === false
                                );
                        } else {
                            // there is no connection to, remove the previous if necessary
                            connectionTo.remove();
                        }

                        // -----------------------
                        // connection label - name
                        // -----------------------

                        // get the connection name
                        const connectionNameValue: string = this.canvasUtils.formatConnectionName(d.component);

                        // is there a name to render
                        if (!this.nifiCommon.isBlank(connectionNameValue)) {
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
                                    .attr('width', 158);
                            } else {
                                backgrounds.push(connectionName.select('rect.connection-label-background'));
                                borders.push(connectionName.select('rect.connection-label-border'));
                            }

                            // update the connection name positioning
                            connectionName.attr('transform', () => {
                                const y: number = rowHeight * labelCount++;
                                return 'translate(0, ' + y + ')';
                            });

                            // update the connection name
                            connectionName
                                .select('text.connection-name')
                                .each((_d: any, i: number, nodes: Element[]) => {
                                    const connectionNameLabel = d3.select(nodes[i]);

                                    // reset the label name to handle any previous state
                                    connectionNameLabel.text(null).selectAll('title').remove();

                                    // apply ellipsis to the label as necessary
                                    this.canvasUtils.ellipsis(
                                        connectionNameLabel,
                                        connectionNameValue,
                                        'connection-name'
                                    );
                                })
                                .append('title')
                                .text(() => connectionNameValue);
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
                            .text(() => '\uf042')
                            .append('title');

                        // retry icon
                        queued
                            .append('text')
                            .attr('class', 'retry-icon')
                            .attr('y', 14)
                            .text(() => '\uf021')
                            .append('title');

                        // expiration icon
                        queued
                            .append('text')
                            .attr('class', 'expiration-icon primary-color')
                            .attr('x', 224)
                            .attr('y', 14)
                            .text(() => '\uf017')
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
                    queued.attr('transform', () => {
                        const y: number = rowHeight * labelCount++;
                        return 'translate(0, ' + y + ')';
                    });

                    // update the height based on the labels being rendered
                    connectionLabelContainer
                        .select('rect.body')
                        .attr('height', () => rowHeight * labelCount + ConnectionManager.HEIGHT_FOR_BACKPRESSURE)
                        .classed('unauthorized', () => d.permissions.canRead === false);
                    connectionLabelContainer
                        .select('rect.border')
                        .attr('height', () => rowHeight * labelCount + ConnectionManager.HEIGHT_FOR_BACKPRESSURE)
                        .classed('unauthorized', () => d.permissions.canRead === false);

                    // update the coloring of the backgrounds
                    backgrounds.forEach((background, i) => {
                        if (i % 2 === 0) {
                            background.attr('class', 'odd');
                        } else {
                            background.attr('class', 'even');
                        }
                    });

                    // update the coloring of the label borders
                    borders.forEach((border, i) => {
                        if (i > 0) {
                            border.attr('class', 'row-border');
                        } else {
                            border.attr('class', 'transparent');
                        }
                    });

                    // determine whether or not to show the retry icon
                    connectionLabelContainer
                        .select('text.retry-icon')
                        .classed('hidden', () => {
                            if (d.permissions.canRead) {
                                return !this.isRetryConfigured(d.component);
                            } else {
                                return true;
                            }
                        })
                        .classed('primary-color', () => d.permissions.canRead)
                        .attr('x', () => {
                            let offset = 224;
                            if (d.permissions.canRead && this.isExpirationConfigured(d.component)) {
                                offset -= 16;
                            }
                            // retry icon comes before load-balance icon, so no additional offset needed here
                            return offset;
                        })
                        .select('title')
                        .text(() => {
                            if (d.permissions.canRead && this.isRetryConfigured(d.component)) {
                                return `Relationships configured to be retried: ${d.component.retriedRelationships.join(', ')}`;
                            } else {
                                return '';
                            }
                        });

                    // determine whether or not to show the load-balance icon
                    connectionLabelContainer
                        .select('text.load-balance-icon')
                        .classed('hidden', () => {
                            if (d.permissions.canRead) {
                                return !this.isLoadBalanceConfigured(d.component);
                            } else {
                                return true;
                            }
                        })
                        .classed('load-balance-icon-active fa-rotate-90 success-color-variant', (d: any) => {
                            return d.permissions.canRead && d.component.loadBalanceStatus === 'LOAD_BALANCE_ACTIVE';
                        })
                        .classed('primary-color', (d: any) => {
                            return d.permissions.canRead && d.component.loadBalanceStatus !== 'LOAD_BALANCE_ACTIVE';
                        })
                        .attr('x', () => {
                            let offset = 224;
                            if (d.permissions.canRead && this.isExpirationConfigured(d.component)) {
                                offset -= 16;
                            }
                            if (d.permissions.canRead && this.isRetryConfigured(d.component)) {
                                offset -= 16;
                            }
                            // load-balance icon is leftmost, so it gets additional offset
                            return offset;
                        })
                        .select('title')
                        .text(() => {
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
                        .classed('hidden', () => {
                            if (d.permissions.canRead) {
                                return !this.isExpirationConfigured(d.component);
                            } else {
                                return true;
                            }
                        })
                        .select('title')
                        .text(() => {
                            if (d.permissions.canRead) {
                                return 'Expires FlowFiles older than ' + d.component.flowFileExpiration;
                            } else {
                                return '';
                            }
                        });

                    // update backpressure object fill
                    connectionLabelContainer
                        .select('rect.backpressure-object')
                        .classed('not-configured', () => d.status.aggregateSnapshot.percentUseCount == null);
                    connectionLabelContainer
                        .selectAll('rect.backpressure-tick.object')
                        .classed('not-configured', () => d.status.aggregateSnapshot.percentUseCount == null);
                    connectionLabelContainer
                        .selectAll('rect.backpressure-tick.object-prediction')
                        .classed('not-configured', () => d.status.aggregateSnapshot.percentUseCount == null);

                    // update backpressure data size fill
                    connectionLabelContainer
                        .select('rect.backpressure-data-size')
                        .classed('not-configured', () => d.status.aggregateSnapshot.percentUseBytes == null);
                    connectionLabelContainer
                        .selectAll('rect.backpressure-tick.data-size')
                        .classed('not-configured', () => d.status.aggregateSnapshot.percentUseBytes == null);
                    connectionLabelContainer
                        .selectAll('rect.backpressure-tick.data-size-prediction')
                        .classed('not-configured', () => d.status.aggregateSnapshot.percentUseBytes == null);

                    if (d.permissions.canWrite) {
                        // only support dragging the label when appropriate
                        connectionLabelContainer.call(this.labelDrag);
                    }

                    // update the connection status
                    this.updateConnectionStatus(connection);
                } else {
                    if (!connectionLabelContainer.empty()) {
                        connectionLabelContainer.remove();
                    }
                }
            }

            // update the position of the label if possible
            this.transitionBehavior
                .transition(connection.select('g.connection-label-container'), this.transitionRequired)
                .attr('transform', (d: any, i: number, nodes: Element[]) => {
                    const label: any = d3.select(nodes[i]).select('rect.body');
                    const position: Position = this.getLabelPosition(label);
                    return 'translate(' + position.x + ', ' + position.y + ')';
                });
        });
    }

    private updateConnectionStatus(updated: any): void {
        if (updated.empty()) {
            return;
        }

        // penalized icon
        const connectionLabelContainer: any = updated.select('g.connection-label-container');
        if (connectionLabelContainer.select('text.penalized-icon').empty()) {
            connectionLabelContainer
                .select('g.queued-container')
                .append('text')
                .attr('class', 'penalized-icon primary-color')
                .attr('y', 14)
                .text(() => '\uf252')
                .append('title');
        }

        // determine whether or not to show the penalized icon
        connectionLabelContainer
            .select('text.penalized-icon')
            .classed('hidden', (d: any) => {
                // TODO - optional chaining
                const flowFileAvailability: string = d.status.aggregateSnapshot.flowFileAvailability;
                return flowFileAvailability !== 'HEAD_OF_QUEUE_PENALIZED';
            })
            .attr('x', () => {
                let offset = 0;
                if (!connectionLabelContainer.select('text.expiration-icon').classed('hidden')) {
                    offset += 16;
                }
                if (!connectionLabelContainer.select('text.load-balance-icon').classed('hidden')) {
                    offset += 16;
                }
                if (!connectionLabelContainer.select('text.retry-icon').classed('hidden')) {
                    offset += 16;
                }
                return 224 - offset;
            })
            .select('title')
            .text(() => 'A FlowFile is currently penalized and data cannot be processed at this time.');

        // update connection once progress bars have transitioned
        Promise.all([this.updateDataSize(updated), this.updateObjectCount(updated)]).then(() => {
            // connection stroke
            updated
                .select('path.connection-path')
                .classed('full', (d: any) => this.isFullCount(d) || this.isFullBytes(d))
                .attr('marker-end', (d: any) => {
                    let marker = 'normal';

                    if (d.permissions.canRead) {
                        // if the connection has a relationship that is unavailable, mark it a ghost relationship
                        if (this.isFullBytes(d) || this.isFullCount(d)) {
                            marker = 'full';
                        } else if (this.hasUnavailableRelationship(d)) {
                            marker = 'ghost';
                        }
                    } else {
                        marker = 'unauthorized';
                    }

                    return 'url(#' + marker + ')';
                });

            // border
            updated.select('rect.border').classed('full', (d: any) => {
                return this.isFullCount(d) || this.isFullBytes(d);
            });

            // drop shadow
            updated.select('rect.body').attr('filter', (d: any) => {
                if (this.isFullCount(d) || this.isFullBytes(d)) {
                    return 'url(#connection-full-drop-shadow)';
                } else {
                    return 'url(#component-drop-shadow)';
                }
            });
        });
    }

    private async updateDataSize(updated: any) {
        await new Promise<void>((resolve) => {
            // queued count value
            updated.select('text.queued tspan.count').text((d: any) => {
                return this.nifiCommon.substringBeforeFirst(d.status.aggregateSnapshot.queued, ' ');
            });

            const backpressurePercentDataSize: any = updated.select('rect.backpressure-percent.data-size');
            backpressurePercentDataSize
                .transition()
                .duration(400)
                .attr('width', (d: any) => {
                    if (d.status.aggregateSnapshot.percentUseBytes != null) {
                        return (
                            (ConnectionManager.BACKPRESSURE_BAR_WIDTH * d.status.aggregateSnapshot.percentUseBytes) /
                            100
                        );
                    } else {
                        return 0;
                    }
                })
                .on('end', () => {
                    backpressurePercentDataSize
                        .classed('warning', (d: any) => this.isWarningBytes(d))
                        .classed('error', (d: any) => this.isErrorBytes(d));

                    resolve();
                });

            const backpressurePercentDataSizePrediction: any = updated.select(
                'rect.backpressure-tick.data-size-prediction'
            );
            backpressurePercentDataSizePrediction
                .transition()
                .duration(400)
                .attr('x', (d: any) => {
                    // clamp the prediction between 0 and 100 percent
                    const predicted = d.status.aggregateSnapshot.predictions?.predictedPercentBytes ?? 0;
                    return (ConnectionManager.BACKPRESSURE_BAR_WIDTH * Math.min(Math.max(predicted, 0), 100)) / 100;
                })
                .attr('display', (d: any) => {
                    const predicted: number = d.status.aggregateSnapshot.predictions?.predictedPercentBytes ?? -1;
                    if (predicted >= 0) {
                        return 'unset neutral-color';
                    } else {
                        // don't show it if there is not a valid prediction
                        return 'none';
                    }
                })
                .on('end', () => {
                    backpressurePercentDataSizePrediction.classed('prediction-down', (d: any) => {
                        const actual: number = d.status.aggregateSnapshot.predictions?.percentUseBytes ?? 0;
                        const predicted: number = d.status.aggregateSnapshot.predictions?.predictedPercentBytes ?? 0;
                        return predicted < actual;
                    });
                });

            updated.select('g.backpressure-data-size-container').each((d: any, i: number, nodes: Element[]) => {
                this.canvasUtils.canvasTooltip(UnorderedListTip, d3.select(nodes[i]), {
                    items: this.getBackPressureSizeTip(d)
                });
            });
        });
    }

    private async updateObjectCount(updated: any) {
        await new Promise<void>((resolve) => {
            // queued size value
            updated.select('text.queued tspan.size').text((d: any) => {
                return ' ' + this.nifiCommon.substringAfterFirst(d.status.aggregateSnapshot.queued, ' ');
            });

            const backpressurePercentObject: any = updated.select('rect.backpressure-percent.object');
            backpressurePercentObject
                .transition()
                .duration(400)
                .attr('width', (d: any) => {
                    if (d.status.aggregateSnapshot.percentUseCount != null) {
                        return (
                            (ConnectionManager.BACKPRESSURE_BAR_WIDTH * d.status.aggregateSnapshot.percentUseCount) /
                            100
                        );
                    } else {
                        return 0;
                    }
                })
                .on('end', () => {
                    backpressurePercentObject
                        .classed('warning', (d: any) => this.isWarningCount(d))
                        .classed('error', (d: any) => this.isErrorCount(d));

                    resolve();
                });

            const backpressurePercentObjectPrediction: any = updated.select('rect.backpressure-tick.object-prediction');
            backpressurePercentObjectPrediction
                .transition()
                .duration(400)
                .attr('x', (d: any) => {
                    // clamp the prediction between 0 and 100 percent
                    const predicted: number = d.status.aggregateSnapshot.predictions?.predictedPercentCount ?? 0;
                    return (ConnectionManager.BACKPRESSURE_BAR_WIDTH * Math.min(Math.max(predicted, 0), 100)) / 100;
                })
                .attr('display', (d: any) => {
                    const predicted = d.status.aggregateSnapshot.predictions?.predictedPercentCount ?? -1;
                    if (predicted >= 0) {
                        return 'unset neutral-color';
                    } else {
                        // don't show it if there not a valid prediction
                        return 'none';
                    }
                })
                .on('end', () => {
                    backpressurePercentObjectPrediction.classed('prediction-down', (d: any) => {
                        // TODO - optional chaining with default ?? 0
                        const actual = d.status.aggregateSnapshot.percentUseCount;
                        const predicted = d.status.aggregateSnapshot.predictions?.predictedPercentCount ?? 0;
                        return predicted < actual;
                    });
                });

            updated.select('g.backpressure-object-container').each((d: any, i: number, nodes: Element[]) => {
                this.canvasUtils.canvasTooltip(UnorderedListTip, d3.select(nodes[i]), {
                    items: this.getBackPressureCountTip(d)
                });
            });
        });
    }

    private removeProcessGroups(removed: any): void {
        if (removed.empty()) {
            return;
        }

        removed.remove();
    }

    public init(): void {
        this.connectionContainer = d3
            .select('#canvas')
            .append('g')
            .attr('pointer-events', 'stroke')
            .attr('class', 'connections');

        this.store
            .select(selectConnections)
            .pipe(
                filter(() => this.connectionContainer !== null),
                takeUntil(this.destroyed$)
            )
            .subscribe((connections) => {
                this.set(connections);
            });

        this.store
            .select(selectFlowLoadingStatus)
            .pipe(
                filter((status) => status === 'success'),
                filter(() => this.connectionContainer !== null),
                switchMap(() => this.store.select(selectAnySelectedComponentIds)),
                takeUntil(this.destroyed$)
            )
            .subscribe((selected) => {
                this.connectionContainer
                    .selectAll('g.connection')
                    .classed('selected', (d: any) => selected.includes(d.id));
            });

        this.store
            .select(selectTransitionRequired)
            .pipe(takeUntil(this.destroyed$))
            .subscribe((transitionRequired) => {
                this.transitionRequired = transitionRequired;
            });

        this.store
            .select(selectCurrentProcessGroupId)
            .pipe(takeUntil(this.destroyed$))
            .subscribe((currentProcessGroupId) => {
                this.currentProcessGroupId = currentProcessGroupId;
            });

        this.store
            .select(selectTransform)
            .pipe(takeUntil(this.destroyed$))
            .subscribe((transform) => {
                this.scale = transform.scale;
            });
    }

    public destroy(): void {
        this.connectionContainer = null;
        this.destroyed$.next(true);
    }

    ngOnDestroy(): void {
        this.destroyed$.complete();
    }

    private set(connections: any): void {
        // update the connections
        this.connections = connections.map((connection: any) => {
            const currentConnection: any = this.connections.find((c: any) => c.id === connection.id);

            // only consider newer when the version is greater which indicates the component configuration has changed.
            // when this happens we should override the current dragging action so that the new changes can be realized.
            const isNewerRevision = connection.revision.version > currentConnection?.revision.version;

            let dragging = false;
            if (currentConnection?.dragging && !isNewerRevision) {
                dragging = true;
            }

            let bends: Position[];
            if (dragging) {
                bends = currentConnection.bends;
            } else {
                bends = connection.bends.map((bend: Position) => {
                    return { ...bend };
                });
            }

            let end: Position | undefined;
            if (dragging) {
                end = currentConnection.end;
            }

            return {
                ...connection,
                type: ComponentType.Connection,
                dragging,
                labelIndex: dragging ? currentConnection.labelIndex : connection.labelIndex,
                bends,
                end
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
