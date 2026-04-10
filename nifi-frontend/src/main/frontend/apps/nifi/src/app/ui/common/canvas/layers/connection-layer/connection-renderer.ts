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

import * as d3 from 'd3';
import { CanvasConnection, Position } from '../../canvas.types';
import { ConnectionRenderContext } from '../render-context.types';
import { CanvasConstants } from '../../canvas.constants';
import { UnorderedListTip } from '../../../tooltips/unordered-list-tip/unordered-list-tip.component';

/**
 * ConnectionRenderer
 *
 * Pure D3 rendering logic for connection paths and labels.
 *
 * Key insight: Connections reference components by ID, not coordinates.
 * We look up source/destination components on the canvas to get their positions.
 *
 * Pattern:
 * - Static utility class with pure rendering functions
 * - render(): Main entry point accepting ConnectionRenderContext
 * - appendConnectionElements(): Create initial SVG structure
 * - updateConnectionElements(): Update existing elements
 * - Uses D3's enter/update/exit pattern for efficiency
 */
export class ConnectionRenderer {
    /**
     * Main render method - orchestrates the D3 data join pattern
     */
    public static render(context: ConnectionRenderContext): void {
        const { containerSelection, connections, canSelect, callbacks, disabledConnectionIds } = context;

        // D3 data join
        const selection = containerSelection
            .selectAll<SVGGElement, CanvasConnection>('g.connection')
            .data(connections, (d: CanvasConnection) => d.entity.id);

        // Enter: create new connection elements
        const entered: any = selection.enter();
        const appendedGroups: any = ConnectionRenderer.appendConnectionElements(entered);

        // Update existing and newly entered connections
        const merged: any = selection.merge(appendedGroups);
        ConnectionRenderer.updateConnectionElements(merged, context, true);

        // Sort connections by zIndex to ensure correct rendering order
        // Connections with higher z-index appear on top of connections with lower z-index
        // D3's sort() method sorts the selection AND reorders DOM elements in one operation
        //
        // Note: zIndex is stored at entity level (entity.zIndex), not in component
        merged.sort((a: CanvasConnection, b: CanvasConnection) => {
            return context.nifiCommon.compareNumber(a.entity.zIndex, b.entity.zIndex);
        });

        // Attach event listeners if selection is enabled
        // Use mousedown for selection to ensure single, deterministic event firing
        if (canSelect) {
            if (callbacks.onClick) {
                merged.on('mousedown.selection', function (event: MouseEvent, d: CanvasConnection) {
                    // Only handle left mouse button
                    if (event.button !== 0) {
                        return;
                    }

                    event.stopPropagation();
                    callbacks.onClick!(d, event);
                });
            }

            // Double-click to add bend point
            if (callbacks.onBendPointAdd && disabledConnectionIds && context.getCanEdit()) {
                // Capture disabledConnectionIds in closure
                const disabled = disabledConnectionIds;

                // Track last double-click time to prevent duplicate events
                let lastDoubleClickTime = 0;
                const DOUBLE_CLICK_DEBOUNCE_MS = 300;

                merged.on('dblclick', function (this: Element, event: MouseEvent, d: CanvasConnection) {
                    // Debounce rapid double-clicks (prevent duplicate events)
                    const now = Date.now();
                    if (now - lastDoubleClickTime < DOUBLE_CLICK_DEBOUNCE_MS) {
                        return;
                    }
                    lastDoubleClickTime = now;

                    // Check if connection is disabled (saving)
                    const isDisabled = disabled?.has(d.entity.id) || false;
                    if (isDisabled) {
                        return;
                    }

                    // Check permissions
                    if (!d.entity.permissions?.canWrite || !d.entity.permissions?.canRead) {
                        return;
                    }

                    event.preventDefault();
                    event.stopPropagation();

                    // Get mouse position relative to the parent (canvas group)
                    const position = d3.pointer(event, this.parentNode);

                    // Find the best position to insert the new bend point
                    const insertIndex = ConnectionRenderer.findBendPointInsertIndex(d, position[0], position[1]);

                    // Add bend point to ui.bends array
                    if (!d.ui.bends) {
                        d.ui.bends = [];
                    }
                    d.ui.bends.splice(insertIndex, 0, { x: position[0], y: position[1] });

                    // Trigger callback to save changes
                    callbacks.onBendPointAdd!(d, {
                        x: position[0],
                        y: position[1],
                        index: insertIndex
                    });

                    // Update visual immediately
                    const connection = d3.select<SVGGElement, CanvasConnection>(`#id-${d.entity.id}`);
                    ConnectionRenderer.updateConnectionPoints(connection, context);
                });
            }
        }

        // Exit: remove connections that are no longer in data
        const exited: any = selection.exit();
        ConnectionRenderer.removeConnectionElements(exited);
    }

    /**
     * Append connection SVG elements (enter selection)
     * Note: Event handlers are attached separately in render() method
     */
    private static appendConnectionElements(
        entered: d3.Selection<any, CanvasConnection, any, any>
    ): d3.Selection<any, CanvasConnection, any, any> {
        // Create group for each connection
        const connGroups = entered
            .append('g')
            .attr('id', (d) => `id-${d.entity.id}`)
            .attr('class', 'connection');

        // Main connection path (visible line)
        // Note: Do NOT set inline stroke/stroke-width - let CSS handle styling
        connGroups
            .append('path')
            .attr('class', 'connection-path')
            .attr('pointer-events', 'none')
            .attr('d', (d) => ConnectionRenderer.calculatePath(d));

        // Selection overlay (shown when connection is selected)
        connGroups
            .append('path')
            .attr('class', 'connection-selection-path')
            .attr('pointer-events', 'none')
            .attr('d', (d) => ConnectionRenderer.calculatePath(d));

        // Selectable path (wide invisible path for easier clicking)
        // Event handlers will be attached in render() method
        connGroups
            .append('path')
            .attr('class', 'connection-path-selectable')
            .attr('pointer-events', 'stroke')
            .attr('d', (d) => ConnectionRenderer.calculatePath(d));

        return connGroups;
    }

    /**
     * Update connection SVG elements (update + enter selection)
     */
    private static updateConnectionElements(
        selection: d3.Selection<any, CanvasConnection, any, any>,
        context: ConnectionRenderContext,
        updateLabel = true
    ): void {
        const { disabledConnectionIds } = context;

        // Apply disabled state to connection (while saving)
        selection
            .classed('disabled', (d) => disabledConnectionIds?.has(d.entity.id) || false)
            .style('opacity', (d) => (disabledConnectionIds?.has(d.entity.id) ? 0.6 : null))
            .style('cursor', (d) => {
                if (disabledConnectionIds?.has(d.entity.id)) return 'not-allowed';
                return context.canSelect ? 'pointer' : 'default';
            });

        // Update main connection path
        selection
            .select('path.connection-path')
            .attr('d', (d) => ConnectionRenderer.calculatePath(d))
            .classed('unauthorized', (d) => d.entity.permissions?.canRead === false);

        // Update selection overlay path (shown when connection has 'selected' class)
        selection.select('path.connection-selection-path').attr('d', (d) => ConnectionRenderer.calculatePath(d));

        // Update selectable path (wide invisible path for easier clicking)
        selection.select('path.connection-path-selectable').attr('d', (d) => ConnectionRenderer.calculatePath(d));

        // Update connection labels if requested
        if (updateLabel) {
            selection.each(function (d) {
                const connection = d3.select(this) as d3.Selection<any, CanvasConnection, any, any>;
                const connectionLabelContainer = connection.select('g.connection-label-container');

                // update visible connections
                if (connection.classed('visible')) {
                    // Connection is visible - render/update label
                    ConnectionRenderer.updateConnectionLabel(connection, d, context);
                } else if (connectionLabelContainer.empty()) {
                    // Connection doesn't have visible class yet (initial render before visibility is set)
                    // AND label doesn't exist - render it so it's ready when visibility is checked
                    ConnectionRenderer.updateConnectionLabel(connection, d, context);
                } else {
                    // Connection is not visible and label exists - remove it
                    connectionLabelContainer.remove();
                }
            });

            // After all labels are rendered/updated, animate backpressure
            // Only animate for visible connections
            const visibleConnections = selection.filter(function () {
                return d3.select(this).classed('visible');
            });
            ConnectionRenderer.updateConnectionStatus(visibleConnections, context);
        }

        // Update connection points (startpoint, endpoint, midpoints)
        ConnectionRenderer.updateConnectionPoints(selection, context);
    }

    /**
     * Update connection points (startpoint, endpoint, midpoints)
     * These are only visible when the connection is selected
     */
    private static updateConnectionPoints(
        selection: d3.Selection<any, CanvasConnection, any, any>,
        context: ConnectionRenderContext
    ): void {
        const { callbacks, canSelect, disabledConnectionIds } = context;

        selection.each(function (d: CanvasConnection) {
            const connection = d3.select<SVGGElement, CanvasConnection>(this);
            const canWrite = d.entity.permissions.canWrite;
            const canRead = d.entity.permissions.canRead;
            const isDisabled = disabledConnectionIds?.has(d.entity.id);

            // Only show connection points if user has read and write permissions
            if (canWrite && canRead) {
                // Use start/end points stored on connection.ui by calculatePath
                const start = d.ui.start;
                const end = d.ui.end;
                const bends = d.ui.bends || [];

                // Update startpoint
                const startpoints = connection.selectAll('rect.startpoint').data([start]);
                startpoints
                    .enter()
                    .append('rect')
                    .attr('class', 'startpoint linepoint')
                    .attr('pointer-events', 'all')
                    .attr('width', 8)
                    .attr('height', 8)
                    .attr('transform', (p: any) => `translate(${p.x - 4}, ${p.y - 4})`);

                startpoints.attr('transform', (p: any) => `translate(${p.x - 4}, ${p.y - 4})`);

                startpoints.exit().remove();

                // Update endpoint
                const endpoints = connection.selectAll('rect.endpoint').data([end]);
                endpoints
                    .enter()
                    .append('rect')
                    .attr('class', 'endpoint linepoint')
                    .attr('pointer-events', 'all')
                    .attr('width', 8)
                    .attr('height', 8)
                    .attr('transform', (p: any) => `translate(${p.x - 4}, ${p.y - 4})`);

                endpoints.attr('transform', (p: any) => `translate(${p.x - 4}, ${p.y - 4})`);

                endpoints.exit().remove();

                // Update midpoints (bend points) - these are draggable
                const midpoints = connection.selectAll('rect.midpoint').data(bends);
                midpoints
                    .enter()
                    .append('rect')
                    .attr('class', 'midpoint linepoint')
                    .attr('pointer-events', 'all')
                    .attr('width', 8)
                    .attr('height', 8)
                    .attr('transform', (p: any) => `translate(${p.x - 4}, ${p.y - 4})`);

                midpoints
                    .attr('transform', (p: any) => `translate(${p.x - 4}, ${p.y - 4})`)
                    .style('cursor', () => {
                        // Only show move cursor if editing is enabled
                        return context.getCanEdit() ? 'move' : 'default';
                    });

                midpoints.exit().remove();

                // Add double-click handler to remove bend points
                if (canSelect && callbacks.onBendPointRemove && !isDisabled) {
                    connection.selectAll('rect.midpoint').on('dblclick', function (event, bendPoint) {
                        event.preventDefault();
                        event.stopPropagation();

                        // Find the index of this bend point
                        const bendIndex = (d.ui.bends || []).indexOf(bendPoint as Position);
                        if (bendIndex !== -1) {
                            // Remove bend point from ui.bends array
                            d.ui.bends!.splice(bendIndex, 1);

                            // Trigger callback to save changes
                            callbacks.onBendPointRemove!(d, bendIndex);

                            // Update visual immediately
                            ConnectionRenderer.updateConnectionPoints(connection, context);
                        }
                    });
                }

                // Add drag behavior to midpoints if selection is enabled and callback is provided (filter will check canEdit)
                if (canSelect && callbacks.onBendPointDragEnd && !isDisabled) {
                    const bendDrag = d3
                        .drag<SVGRectElement, { x: number; y: number }>()
                        .filter(function (_event) {
                            // Block drag if editing is disabled
                            if (!context.getCanEdit()) {
                                return false;
                            }
                            // Block drag if connection is disabled (saving)
                            if (disabledConnectionIds?.has(d.entity.id)) {
                                return false;
                            }
                            return true;
                        })
                        .on('start', function (event) {
                            // Stop propagation to prevent canvas pan
                            event.sourceEvent.stopPropagation();
                            // Mark connection as dragging
                            d.ui.dragging = true;
                        })
                        .on('drag', function (event, bendPoint) {
                            // Stop propagation to prevent canvas pan
                            event.sourceEvent.stopPropagation();

                            // Update bend point position in ui.bends array
                            bendPoint.x += event.dx;
                            bendPoint.y += event.dy;

                            // Update visual position immediately
                            d3.select(this).attr('transform', `translate(${bendPoint.x - 4}, ${bendPoint.y - 4})`);

                            // Recalculate and update connection path
                            connection.select('path.connection-path').attr('d', ConnectionRenderer.calculatePath(d));
                            connection
                                .select('path.connection-selection-path')
                                .attr('d', ConnectionRenderer.calculatePath(d));

                            // Update other connection points positions (start/end updated by calculatePath)
                            connection
                                .selectAll('rect.startpoint')
                                .attr('transform', `translate(${d.ui.start.x - 4}, ${d.ui.start.y - 4})`);
                            connection
                                .selectAll('rect.endpoint')
                                .attr('transform', `translate(${d.ui.end.x - 4}, ${d.ui.end.y - 4})`);

                            // Update connection label position if it exists
                            const labelPosition = ConnectionRenderer.getLabelPosition(d);
                            connection
                                .select('g.connection-label-container')
                                .attr('transform', `translate(${labelPosition.x}, ${labelPosition.y})`);
                        })
                        .on('end', function (event) {
                            // Stop propagation to prevent canvas pan
                            event.sourceEvent.stopPropagation();

                            // Get all current bend points from ui.bends
                            const updatedBends = d.ui.bends || [];

                            // Check if bends have actually changed from entity.bends
                            const entityBends = d.entity.bends || [];
                            let different = updatedBends.length !== entityBends.length;

                            if (!different) {
                                for (let i = 0; i < updatedBends.length && !different; i++) {
                                    if (
                                        updatedBends[i].x !== entityBends[i].x ||
                                        updatedBends[i].y !== entityBends[i].y
                                    ) {
                                        different = true;
                                    }
                                }
                            }

                            // Only trigger save if bends actually changed
                            if (different) {
                                callbacks.onBendPointDragEnd!(d, updatedBends);
                            }

                            // Clear dragging flag
                            d.ui.dragging = false;
                        });

                    // Remove drag behavior to prevent stale closures
                    connection.selectAll('rect.midpoint').on('.drag', null);
                    // Apply drag behavior to all midpoints
                    connection.selectAll<SVGRectElement, { x: number; y: number }>('rect.midpoint').call(bendDrag);
                }
            } else {
                // Remove all connection points if no permissions
                connection.selectAll('rect.startpoint').remove();
                connection.selectAll('rect.endpoint').remove();
                connection.selectAll('rect.midpoint').remove();
            }
        });
    }

    /**
     * Remove connection SVG elements (exit selection)
     */
    private static removeConnectionElements(exited: d3.Selection<any, any, any, any>): void {
        exited.remove();
    }

    /**
     * Pan method for efficient rendering during zoom/pan operations.
     * Called by canvas.component.ts for entering/leaving connections only.
     * Simply delegates to updateConnectionElements which handles all updates
     * including creating/removing labels based on visibility.
     * Matches D3 lifecycle pattern used by other renderers.
     *
     * @param selection - D3 selection of connections to update (typically entering/leaving)
     * @param context - Complete render context with all necessary data
     */
    public static pan(
        selection: d3.Selection<any, CanvasConnection, any, any>,
        context: ConnectionRenderContext
    ): void {
        // Simply delegate to updateConnectionElements which handles all updates
        // including creating/removing labels based on the 'visible' class
        ConnectionRenderer.updateConnectionElements(selection, context);
    }

    /**
     * Helper method to check if a terminal is a group
     *
     * A terminal is considered a "group" if it's a port in a different process group
     */
    private static isGroup(terminal: any, currentProcessGroupId: string | null): boolean {
        if (!terminal) {
            return false;
        }

        // Check if terminal has a groupId that differs from current process group
        // AND if it's an input or output port type
        const isPortType = terminal.type?.indexOf('INPUT_PORT') >= 0 || terminal.type?.indexOf('OUTPUT_PORT') >= 0;

        return terminal.groupId !== currentProcessGroupId && isPortType;
    }

    /**
     * Update connection label with stats and backpressure indicators
     *
     */
    private static updateConnectionLabel(
        connection: d3.Selection<any, CanvasConnection, any, any>,
        d: CanvasConnection,
        context: ConnectionRenderContext
    ): void {
        const textEllipsis = context.textEllipsis;
        const processGroupId = context.processGroupId;
        let connectionLabelContainer: any = connection.select('g.connection-label-container');

        // If label container doesn't exist, create it
        if (connectionLabelContainer.empty()) {
            connectionLabelContainer = connection
                .insert('g', 'rect.startpoint')
                .attr('class', 'connection-label-container')
                .attr('pointer-events', 'all');

            // Add double-click handler to label container to prevent bend point addition
            // and emit connection double-click event instead
            if (context.canSelect && context.callbacks.onDoubleClick) {
                connectionLabelContainer.on('dblclick', function (event: MouseEvent, d: CanvasConnection) {
                    event.preventDefault();
                    event.stopPropagation();

                    // Emit connection double-click event
                    context.callbacks.onDoubleClick!(d, event);
                });
            }

            // Connection label border
            connectionLabelContainer
                .append('rect')
                .attr('class', 'border')
                .attr('width', CanvasConstants.CONNECTION_LABEL.width);

            // Connection label body (background with drop shadow)
            connectionLabelContainer
                .append('rect')
                .attr('class', 'body')
                .attr('width', CanvasConstants.CONNECTION_LABEL.width)
                .attr('fill', '#ffffff')
                .attr('filter', 'url(#component-drop-shadow)');
        }

        // Add click handler to label for connection selection (matches existing ConnectionManager)
        if (context.canSelect && context.callbacks.onClick) {
            connectionLabelContainer.on('mousedown.selection', function (event: MouseEvent, datum: CanvasConnection) {
                // Only handle left mouse button
                if (event.button !== 0) {
                    return;
                }

                event.stopPropagation();
                context.callbacks.onClick!(datum, event);
            });
        } else {
            connectionLabelContainer.on('mousedown.selection', null);
        }

        // Add label drag behavior if selection is enabled and callback exists (filter will check canEdit)
        // Re-apply on every render to ensure fresh closures
        if (context.canSelect && context.callbacks.onLabelDragEnd) {
            ConnectionRenderer.addLabelDragBehavior(connectionLabelContainer, d, connection, context);
        }

        // Calculate label height based on content
        let labelCount = 0; // Track which row we're on
        const connectionData = d.entity.component;
        const connectionStatus = d.entity.status;

        // Skip label rendering if user doesn't have read permissions
        // (component data is only populated when permissions.canRead is true)
        if (!connectionData || !d.entity.permissions.canRead) {
            return;
        }

        // Arrays to track backgrounds and borders for styling
        const backgrounds: any[] = [];
        const borders: any[] = [];

        // Select containers
        let connectionFrom = connectionLabelContainer.select('g.connection-from-container');
        let connectionTo = connectionLabelContainer.select('g.connection-to-container');
        let connectionName = connectionLabelContainer.select('g.connection-name-container');

        // -------------------------
        // connection label - from
        // -------------------------
        if (ConnectionRenderer.isGroup(connectionData.source, processGroupId)) {
            // Check if from container already exists
            if (connectionFrom.empty()) {
                connectionFrom = connectionLabelContainer.append('g').attr('class', 'connection-from-container');

                // Background
                backgrounds.push(
                    connectionFrom
                        .append('rect')
                        .attr('class', 'connection-label-background')
                        .attr('width', CanvasConstants.CONNECTION_LABEL.width)
                        .attr('height', CanvasConstants.CONNECTION_ROW_HEIGHT)
                );

                // Border
                borders.push(
                    connectionFrom
                        .append('rect')
                        .attr('class', 'connection-label-border')
                        .attr('width', CanvasConstants.CONNECTION_LABEL.width)
                        .attr('height', 1)
                );

                // "From" label
                connectionFrom.append('text').attr('class', 'stats-label').attr('x', 5).attr('y', 14).text('From');

                // Source component name
                connectionFrom
                    .append('text')
                    .attr('class', 'stats-value connection-from')
                    .attr('x', 43)
                    .attr('y', 14)
                    .attr('width', 146);

                // Run status icon
                connectionFrom.append('text').attr('class', 'connection-from-run-status').attr('x', 224).attr('y', 14);
            } else {
                backgrounds.push(connectionFrom.select('rect.connection-label-background'));
                borders.push(connectionFrom.select('rect.connection-label-border'));
            }

            // Update the text content (use connection data directly)
            const fromText = connectionFrom.select('text.connection-from');
            fromText.text(null).selectAll('title').remove();
            textEllipsis.applyEllipsis(fromText, connectionData.source.name, 'connection-from');
            fromText.append('title').text(connectionData.source.name);

            // Update the run status icon
            const sourceExists = connectionData.source.exists !== false;
            const sourceRunning = connectionData.source.running === true;

            connectionFrom
                .select('text.connection-from-run-status')
                .text(() => {
                    if (!sourceExists) {
                        return '\uf071'; // warning icon for missing
                    } else if (sourceRunning) {
                        return '\uf04b'; // play icon for running
                    } else {
                        return '\uf04d'; // stop icon for stopped
                    }
                })
                .classed('running success-color-default', sourceExists && sourceRunning)
                .classed('stopped error-color-variant', sourceExists && !sourceRunning)
                .classed('is-missing-port invalid caution-color', !sourceExists);

            labelCount++;
        } else {
            // Remove from container if it exists but shouldn't
            connectionFrom.remove();
        }

        // -------------------------
        // connection label - to
        // -------------------------
        if (ConnectionRenderer.isGroup(connectionData.destination, processGroupId)) {
            // Check if to container already exists
            if (connectionTo.empty()) {
                connectionTo = connectionLabelContainer.append('g').attr('class', 'connection-to-container');

                // Background
                backgrounds.push(
                    connectionTo
                        .append('rect')
                        .attr('class', 'connection-label-background')
                        .attr('width', CanvasConstants.CONNECTION_LABEL.width)
                        .attr('height', CanvasConstants.CONNECTION_ROW_HEIGHT)
                );

                // Border
                borders.push(
                    connectionTo
                        .append('rect')
                        .attr('class', 'connection-label-border')
                        .attr('width', CanvasConstants.CONNECTION_LABEL.width)
                        .attr('height', 1)
                );

                // "To" label
                connectionTo.append('text').attr('class', 'stats-label').attr('x', 5).attr('y', 14).text('To');

                // Destination component name
                connectionTo
                    .append('text')
                    .attr('class', 'stats-value connection-to')
                    .attr('x', 25)
                    .attr('y', 14)
                    .attr('width', 161);

                // Run status icon
                connectionTo.append('text').attr('class', 'connection-to-run-status').attr('x', 224).attr('y', 14);
            } else {
                backgrounds.push(connectionTo.select('rect.connection-label-background'));
                borders.push(connectionTo.select('rect.connection-label-border'));
            }

            // Update the text content (use connection data directly)
            const toText = connectionTo.select('text.connection-to');
            toText.text(null).selectAll('title').remove();
            textEllipsis.applyEllipsis(toText, connectionData.destination.name, 'connection-to');
            toText.append('title').text(connectionData.destination.name);

            // Update the run status icon
            const destExists = connectionData.destination.exists !== false;
            const destRunning = connectionData.destination.running === true;

            connectionTo
                .select('text.connection-to-run-status')
                .text(() => {
                    if (!destExists) {
                        return '\uf071'; // warning icon for missing
                    } else if (destRunning) {
                        return '\uf04b'; // play icon for running
                    } else {
                        return '\uf04d'; // stop icon for stopped
                    }
                })
                .classed('running success-color-default', destExists && destRunning)
                .classed('stopped error-color-variant', destExists && !destRunning)
                .classed('is-missing-port invalid caution-color', !destExists);

            labelCount++;
        } else {
            // Remove to container if it exists but shouldn't
            connectionTo.remove();
        }

        // -------------------------
        // connection label - name
        // -------------------------
        const relationshipsText = connectionData.selectedRelationships?.join(', ') || '';
        if (relationshipsText) {
            // Check if name container already exists
            if (connectionName.empty()) {
                connectionName = connectionLabelContainer.append('g').attr('class', 'connection-name-container');

                // Background
                backgrounds.push(
                    connectionName
                        .append('rect')
                        .attr('class', 'connection-label-background')
                        .attr('width', CanvasConstants.CONNECTION_LABEL.width)
                        .attr('height', CanvasConstants.CONNECTION_ROW_HEIGHT)
                );

                // Border
                borders.push(
                    connectionName
                        .append('rect')
                        .attr('class', 'connection-label-border')
                        .attr('width', CanvasConstants.CONNECTION_LABEL.width)
                        .attr('height', 1)
                );

                // "Name" label
                connectionName.append('text').attr('class', 'stats-label').attr('x', 5).attr('y', 14).text('Name');

                // Relationship names
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

            // Update the text content
            const nameText = connectionName.select('text.connection-name');
            nameText.text(null).selectAll('title').remove();
            textEllipsis.applyEllipsis(nameText, relationshipsText, 'connection-name');
            nameText.append('title').text(relationshipsText);

            labelCount++;
        } else {
            // Remove name container if it exists but shouldn't
            connectionName.remove();
        }

        // -------------------------
        // connection label - queued
        // -------------------------
        let queued = connectionLabelContainer.select('g.queued-container');
        if (queued.empty()) {
            queued = connectionLabelContainer.append('g').attr('class', 'queued-container');

            // Background
            backgrounds.push(
                queued
                    .append('rect')
                    .attr('class', 'connection-label-background')
                    .attr('width', CanvasConstants.CONNECTION_LABEL.width)
                    .attr(
                        'height',
                        CanvasConstants.CONNECTION_ROW_HEIGHT + CanvasConstants.CONNECTION_HEIGHT_FOR_BACKPRESSURE
                    )
            );

            // Border
            borders.push(
                queued
                    .append('rect')
                    .attr('class', 'connection-label-border')
                    .attr('width', CanvasConstants.CONNECTION_LABEL.width)
                    .attr('height', 1)
            );

            queued.append('text').attr('class', 'stats-label').attr('x', 5).attr('y', 14).text('Queued');

            const queuedText = queued.append('text').attr('class', 'stats-value queued').attr('x', 55).attr('y', 14);

            // queued count
            queuedText.append('tspan').attr('class', 'count');

            // queued size
            queuedText.append('tspan').attr('class', 'size');

            // Check if we need backpressure indicators
            const backpressureObjectThreshold = connectionData.backPressureObjectThreshold || 0;
            const backpressureDataSizeThreshold = connectionData.backPressureDataSizeThreshold || '';
            const hasBackpressure = backpressureObjectThreshold > 0 || backpressureDataSizeThreshold;

            if (hasBackpressure) {
                const yBackpressureOffset =
                    CanvasConstants.CONNECTION_ROW_HEIGHT + CanvasConstants.CONNECTION_HEIGHT_FOR_BACKPRESSURE - 4;

                // Object count backpressure bar container
                const backpressureObjectContainer = queued
                    .append('g')
                    .attr(
                        'transform',
                        `translate(${CanvasConstants.CONNECTION_BACKPRESSURE_COUNT_OFFSET}, ${yBackpressureOffset})`
                    )
                    .attr('class', 'backpressure-object-container');

                // start tick
                backpressureObjectContainer
                    .append('rect')
                    .attr('class', 'backpressure-tick object')
                    .attr('width', 1)
                    .attr('height', CanvasConstants.CONNECTION_HEIGHT_FOR_BACKPRESSURE)
                    .attr('x', 0)
                    .attr('y', 0);

                // bar
                backpressureObjectContainer
                    .append('rect')
                    .attr('class', 'backpressure-object')
                    .attr('width', CanvasConstants.CONNECTION_BACKPRESSURE_BAR_WIDTH)
                    .attr('height', CanvasConstants.CONNECTION_HEIGHT_FOR_BACKPRESSURE)
                    .attr('x', 0)
                    .attr('y', 0);

                // end tick
                backpressureObjectContainer
                    .append('rect')
                    .attr('class', 'backpressure-tick object')
                    .attr('width', 1)
                    .attr('height', CanvasConstants.CONNECTION_HEIGHT_FOR_BACKPRESSURE)
                    .attr('x', CanvasConstants.CONNECTION_BACKPRESSURE_BAR_WIDTH)
                    .attr('y', 0);

                // percent full (starts at 0 width)
                backpressureObjectContainer
                    .append('rect')
                    .attr('class', 'backpressure-percent object')
                    .attr('width', 0)
                    .attr('height', CanvasConstants.CONNECTION_HEIGHT_FOR_BACKPRESSURE)
                    .attr('x', 0)
                    .attr('y', 0);

                // prediction indicator
                backpressureObjectContainer
                    .append('rect')
                    .attr('class', 'backpressure-tick object-prediction')
                    .attr('width', 1)
                    .attr('height', CanvasConstants.CONNECTION_HEIGHT_FOR_BACKPRESSURE)
                    .attr('x', CanvasConstants.CONNECTION_BACKPRESSURE_BAR_WIDTH)
                    .attr('y', 0);

                // Data size backpressure bar container
                const backpressureDataSizeContainer = queued
                    .append('g')
                    .attr(
                        'transform',
                        `translate(${CanvasConstants.CONNECTION_BACKPRESSURE_DATASIZE_OFFSET}, ${yBackpressureOffset})`
                    )
                    .attr('class', 'backpressure-data-size-container');

                // start tick
                backpressureDataSizeContainer
                    .append('rect')
                    .attr('class', 'backpressure-tick data-size')
                    .attr('width', 1)
                    .attr('height', CanvasConstants.CONNECTION_HEIGHT_FOR_BACKPRESSURE)
                    .attr('x', 0)
                    .attr('y', 0);

                // bar
                backpressureDataSizeContainer
                    .append('rect')
                    .attr('class', 'backpressure-data-size')
                    .attr('width', CanvasConstants.CONNECTION_BACKPRESSURE_BAR_WIDTH)
                    .attr('height', CanvasConstants.CONNECTION_HEIGHT_FOR_BACKPRESSURE)
                    .attr('x', 0)
                    .attr('y', 0);

                // end tick
                backpressureDataSizeContainer
                    .append('rect')
                    .attr('class', 'backpressure-tick data-size')
                    .attr('width', 1)
                    .attr('height', CanvasConstants.CONNECTION_HEIGHT_FOR_BACKPRESSURE)
                    .attr('x', CanvasConstants.CONNECTION_BACKPRESSURE_BAR_WIDTH)
                    .attr('y', 0);

                // percent full (starts at 0 width)
                backpressureDataSizeContainer
                    .append('rect')
                    .attr('class', 'backpressure-percent data-size')
                    .attr('width', 0)
                    .attr('height', CanvasConstants.CONNECTION_HEIGHT_FOR_BACKPRESSURE)
                    .attr('x', 0)
                    .attr('y', 0);

                // prediction indicator
                backpressureDataSizeContainer
                    .append('rect')
                    .attr('class', 'backpressure-tick data-size-prediction')
                    .attr('width', 1)
                    .attr('height', CanvasConstants.CONNECTION_HEIGHT_FOR_BACKPRESSURE)
                    .attr('x', CanvasConstants.CONNECTION_BACKPRESSURE_BAR_WIDTH)
                    .attr('y', 0);
            }

            // Add connection icons (expiration, load balance, etc.) to the queued container
            ConnectionRenderer.addConnectionIcons(queued, connectionData);
        } else {
            backgrounds.push(queued.select('rect.connection-label-background'));
            borders.push(queued.select('rect.connection-label-border'));
        }

        // Update queued text values with proper formatting
        const queuedValue = connectionStatus?.aggregateSnapshot?.queued || '0 (0 bytes)';
        const formattedQueued = context.formatUtils.formatQueuedStats(queuedValue);

        queued.select('text.queued tspan.count').text(formattedQueued.count);
        queued.select('text.queued tspan.size').text(formattedQueued.size);

        labelCount++; // Queued section is complete

        // Update the positioning of each container
        connectionFrom.attr('transform', function () {
            const index = 0;
            if (!connectionFrom.empty() && ConnectionRenderer.isGroup(connectionData.source, processGroupId)) {
                return `translate(0, ${CanvasConstants.CONNECTION_ROW_HEIGHT * index})`;
            }
            return null;
        });

        connectionTo.attr('transform', function () {
            let index = 0;
            if (!connectionFrom.empty() && ConnectionRenderer.isGroup(connectionData.source, processGroupId)) {
                index++;
            }
            if (!connectionTo.empty() && ConnectionRenderer.isGroup(connectionData.destination, processGroupId)) {
                return `translate(0, ${CanvasConstants.CONNECTION_ROW_HEIGHT * index})`;
            }
            return null;
        });

        connectionName.attr('transform', function () {
            let index = 0;
            if (!connectionFrom.empty() && ConnectionRenderer.isGroup(connectionData.source, processGroupId)) {
                index++;
            }
            if (!connectionTo.empty() && ConnectionRenderer.isGroup(connectionData.destination, processGroupId)) {
                index++;
            }
            if (!connectionName.empty() && relationshipsText) {
                return `translate(0, ${CanvasConstants.CONNECTION_ROW_HEIGHT * index})`;
            }
            return null;
        });

        queued.attr('transform', function () {
            let index = 0;
            if (!connectionFrom.empty() && ConnectionRenderer.isGroup(connectionData.source, processGroupId)) {
                index++;
            }
            if (!connectionTo.empty() && ConnectionRenderer.isGroup(connectionData.destination, processGroupId)) {
                index++;
            }
            if (!connectionName.empty() && relationshipsText) {
                index++;
            }
            return `translate(0, ${CanvasConstants.CONNECTION_ROW_HEIGHT * index})`;
        });

        // Update label dimensions
        // Calculate total height based on all sections (including backpressure area)
        const labelHeight =
            labelCount * CanvasConstants.CONNECTION_ROW_HEIGHT + CanvasConstants.CONNECTION_HEIGHT_FOR_BACKPRESSURE;
        connectionLabelContainer.select('rect.border').attr('height', labelHeight);
        connectionLabelContainer.select('rect.body').attr('height', labelHeight);

        // Update the coloring of the backgrounds (alternating odd/even)
        backgrounds.forEach((background, i) => {
            if (i % 2 === 0) {
                background.attr('class', 'connection-label-background odd');
            } else {
                background.attr('class', 'connection-label-background even');
            }
        });

        // Update the coloring of the label borders
        borders.forEach((border, i) => {
            if (i > 0) {
                border.attr('class', 'connection-label-border row-border');
            } else {
                border.attr('class', 'connection-label-border transparent');
            }
        });

        // Position label at center of connection path
        const labelPosition = ConnectionRenderer.getLabelPosition(d);
        connectionLabelContainer.attr('transform', `translate(${labelPosition.x}, ${labelPosition.y})`);
    }

    private static updateConnectionStatus(
        selection: d3.Selection<any, CanvasConnection, any, any>,
        context: ConnectionRenderContext
    ): void {
        selection.each(function (d) {
            const connection = d3.select(this) as d3.Selection<any, CanvasConnection, any, any>;
            const connectionStatus = d.entity.status;

            if (!connectionStatus) {
                return;
            }

            const percentUseCount = connectionStatus.aggregateSnapshot?.percentUseCount ?? 0;
            const percentUseBytes = connectionStatus.aggregateSnapshot?.percentUseBytes ?? 0;

            // Animate object count bar
            const backpressurePercentObject = connection.select('rect.backpressure-percent.object');
            if (!backpressurePercentObject.empty()) {
                const targetWidth =
                    percentUseCount != null
                        ? (CanvasConstants.CONNECTION_BACKPRESSURE_BAR_WIDTH * percentUseCount) / 100
                        : 0;

                backpressurePercentObject
                    .transition()
                    .duration(400)
                    .attr('width', targetWidth)
                    .on('end', function () {
                        const bar = d3.select(this);
                        bar.classed('warning', percentUseCount >= 61 && percentUseCount <= 85).classed(
                            'error',
                            percentUseCount > 85
                        );
                    });
            }

            // Animate object count prediction indicator
            const backpressurePercentObjectPrediction = connection.select('rect.backpressure-tick.object-prediction');
            if (!backpressurePercentObjectPrediction.empty()) {
                const predictedPercentCount =
                    connectionStatus.aggregateSnapshot?.predictions?.predictedPercentCount ?? -1;
                const clampedPrediction = Math.min(Math.max(predictedPercentCount, 0), 100);

                backpressurePercentObjectPrediction
                    .transition()
                    .duration(400)
                    .attr(
                        'x',
                        predictedPercentCount >= 0
                            ? (CanvasConstants.CONNECTION_BACKPRESSURE_BAR_WIDTH * clampedPrediction) / 100
                            : CanvasConstants.CONNECTION_BACKPRESSURE_BAR_WIDTH
                    )
                    .attr('display', predictedPercentCount >= 0 ? 'unset' : 'none')
                    .on('end', function () {
                        const tick = d3.select(this);
                        tick.classed('prediction-down', predictedPercentCount < percentUseCount);
                    });
            }

            // Animate data size bar
            const backpressurePercentDataSize = connection.select('rect.backpressure-percent.data-size');
            if (!backpressurePercentDataSize.empty()) {
                const targetWidth =
                    percentUseBytes != null
                        ? (CanvasConstants.CONNECTION_BACKPRESSURE_BAR_WIDTH * percentUseBytes) / 100
                        : 0;

                backpressurePercentDataSize
                    .transition()
                    .duration(400)
                    .attr('width', targetWidth)
                    .on('end', function () {
                        const bar = d3.select(this);
                        bar.classed('warning', percentUseBytes >= 61 && percentUseBytes <= 85).classed(
                            'error',
                            percentUseBytes > 85
                        );
                    });
            }

            // Animate data size prediction indicator
            const backpressurePercentDataSizePrediction = connection.select(
                'rect.backpressure-tick.data-size-prediction'
            );
            if (!backpressurePercentDataSizePrediction.empty()) {
                const predictedPercentBytes =
                    connectionStatus.aggregateSnapshot?.predictions?.predictedPercentBytes ?? -1;
                const clampedPrediction = Math.min(Math.max(predictedPercentBytes, 0), 100);

                backpressurePercentDataSizePrediction
                    .transition()
                    .duration(400)
                    .attr(
                        'x',
                        predictedPercentBytes >= 0
                            ? (CanvasConstants.CONNECTION_BACKPRESSURE_BAR_WIDTH * clampedPrediction) / 100
                            : CanvasConstants.CONNECTION_BACKPRESSURE_BAR_WIDTH
                    )
                    .attr('display', predictedPercentBytes >= 0 ? 'unset' : 'none')
                    .on('end', function () {
                        const tick = d3.select(this);
                        tick.classed('prediction-down', predictedPercentBytes < percentUseBytes);
                    });
            }

            // Update penalized icon visibility
            ConnectionRenderer.updatePenalizedIcon(connection, d);

            // Update run status icons
            ConnectionRenderer.updateRunStatusIcons(connection, d);

            // Update ghost relationship and full backpressure styling
            ConnectionRenderer.updateConnectionPathStyling(connection, d);
        });

        // Add tooltips to backpressure containers
        // Use .each() to access the data bound to each element
        selection.select('g.backpressure-object-container').each(function (d: CanvasConnection) {
            context.componentUtils.canvasTooltip(UnorderedListTip, d3.select(this), {
                items: ConnectionRenderer.getBackPressureCountTip(d, context.componentUtils)
            });
        });

        selection.select('g.backpressure-data-size-container').each(function (d: CanvasConnection) {
            context.componentUtils.canvasTooltip(UnorderedListTip, d3.select(this), {
                items: ConnectionRenderer.getBackPressureSizeTip(d, context.componentUtils)
            });
        });
    }

    /**
     * Update connection path styling for grouped, ghost relationships, and full backpressure
     *
     */
    private static updateConnectionPathStyling(
        connection: d3.Selection<any, CanvasConnection, any, any>,
        d: CanvasConnection
    ): void {
        const hasGhostRelationship = ConnectionRenderer.hasUnavailableRelationship(d);
        const isFull = ConnectionRenderer.isFullCount(d) || ConnectionRenderer.isFullBytes(d);
        const isGrouped = ConnectionRenderer.isGrouped(d);

        // Apply grouped class to connection group (multiple relationships)
        connection.classed('grouped', isGrouped);

        // Apply ghost class to connection group
        connection.classed('ghost', hasGhostRelationship);

        // Apply full class to connection group
        connection.classed('full', isFull);

        // Update connection path classes and marker
        const connectionPath = connection.select('path.connection-path');
        if (!connectionPath.empty()) {
            // Apply full class to connection path
            connectionPath.classed('full', isFull);

            // Determine the appropriate marker based on permissions, backpressure, and relationship status
            let marker = 'normal';

            if (d.entity.permissions?.canRead) {
                // Priority: full > ghost > normal
                if (isFull) {
                    marker = 'full';
                } else if (hasGhostRelationship) {
                    marker = 'ghost';
                }
            } else {
                marker = 'unauthorized';
            }

            connectionPath.attr('marker-end', `url(#${marker})`);
        }

        // Note: Label border does NOT get full class - only the connection path changes color
        // Apply full class to rect.border

        // Apply SVG filter to label body for drop shadow
        // Connection path uses CSS drop-shadow, but label uses SVG filter
        const labelBody = connection.select('rect.body');
        if (!labelBody.empty()) {
            if (isFull) {
                labelBody.attr('filter', 'url(#connection-full-drop-shadow)');
            } else {
                labelBody.attr('filter', 'url(#component-drop-shadow)');
            }
        }
    }

    /**
     * Check if connection has unavailable relationships
     *
     */
    private static hasUnavailableRelationship(d: CanvasConnection): boolean {
        const component = d.entity.component;

        // Verify each selected relationship is still available
        if (component?.selectedRelationships && component?.availableRelationships) {
            const unavailableRelationships = component.selectedRelationships.filter(
                (selectedRelationship: string) => !component.availableRelationships!.includes(selectedRelationship)
            );
            return unavailableRelationships.length > 0;
        }

        return false;
    }

    /**
     * Check if connection is full based on object count threshold
     *
     */
    private static isFullCount(d: CanvasConnection): boolean {
        return d.entity.status?.aggregateSnapshot?.percentUseCount === 100;
    }

    /**
     * Check if connection is full based on data size threshold
     *
     */
    private static isFullBytes(d: CanvasConnection): boolean {
        return d.entity.status?.aggregateSnapshot?.percentUseBytes === 100;
    }

    /**
     * Check if connection represents multiple relationships (grouped)
     */
    private static isGrouped(d: CanvasConnection): boolean {
        if (!d.entity.permissions?.canRead) {
            return false;
        }

        // If there are more than one selected relationship, mark this as grouped
        return (d.entity.component?.selectedRelationships?.length || 0) > 1;
    }

    /**
     * Add drag behavior to connection label for moving between bend points
     */
    private static addLabelDragBehavior(
        labelContainer: any,
        connectionData: CanvasConnection,
        connection: d3.Selection<any, CanvasConnection, any, any>,
        context: ConnectionRenderContext
    ): void {
        const labelDrag = d3
            .drag()
            .filter(function () {
                // Block drag if editing is disabled
                if (!context.getCanEdit()) {
                    return false;
                }
                // Block drag if connection is disabled (saving)
                if (context.disabledConnectionIds?.has(connectionData.entity.id)) {
                    return false;
                }
                // Block drag if no write permissions
                if (!connectionData.entity.permissions?.canWrite || !connectionData.entity.permissions?.canRead) {
                    return false;
                }
                return true;
            })
            .on('start', function (event) {
                // Stop propagation to prevent canvas pan
                event.sourceEvent.stopPropagation();

                // Mark as dragging
                connectionData.ui.dragging = true;
            })
            .on('drag', function (event) {
                // Stop propagation to prevent canvas pan
                event.sourceEvent.stopPropagation();

                const bends = connectionData.ui.bends || [];

                // Only allow dragging if there are multiple bend points and not disabled
                if (!connectionData.ui.dragging || bends.length <= 1) {
                    return;
                }

                // Get or create the drag indicator rectangle
                let dragRect: any = d3.select('rect.label-drag');

                if (dragRect.empty()) {
                    // Get label dimensions
                    const body = labelContainer.select('rect.body');
                    const labelWidth = CanvasConstants.CONNECTION_LABEL.width;
                    const labelHeight = parseFloat(body.attr('height')) || 0;

                    // Get current label position
                    const transform = labelContainer.attr('transform');
                    const match = transform.match(/translate\(([^,]+),\s*([^)]+)\)/);
                    const currentX = match ? parseFloat(match[1]) : 0;
                    const currentY = match ? parseFloat(match[2]) : 0;

                    // Create drag indicator
                    dragRect = connection
                        .append('rect')
                        .attr('class', 'label-drag')
                        .attr('x', currentX)
                        .attr('y', currentY)
                        .attr('width', labelWidth)
                        .attr('height', labelHeight)
                        .attr('fill', 'none')
                        .attr('stroke-width', 1)
                        .attr('stroke-dasharray', 4)
                        .attr('pointer-events', 'none')
                        .datum({
                            x: currentX,
                            y: currentY,
                            width: labelWidth,
                            height: labelHeight
                        });
                } else {
                    // Update drag indicator position
                    dragRect
                        .attr('x', function (d: any) {
                            d.x += event.dx;
                            return d.x;
                        })
                        .attr('y', function (d: any) {
                            d.y += event.dy;
                            return d.y;
                        });
                }

                // Calculate current center point of drag indicator
                const datum: any = dragRect.datum();
                const currentPoint: Position = {
                    x: datum.x + datum.width / 2,
                    y: datum.y + datum.height / 2
                };

                // Find closest bend point
                let closestBendIndex = -1;
                let minDistance = Infinity;

                bends.forEach((bend: Position, i: number) => {
                    const distance = ConnectionRenderer.distanceBetweenPoints(currentPoint, bend);

                    if (closestBendIndex === -1 || distance < minDistance) {
                        closestBendIndex = i;
                        minDistance = distance;
                    }
                });

                // Update the temporary label index (not saved yet)
                connectionData.ui.tempLabelIndex = closestBendIndex;

                // Update label position to snap to closest bend
                const snapPosition = ConnectionRenderer.getLabelPosition(connectionData);
                labelContainer.attr('transform', `translate(${snapPosition.x}, ${snapPosition.y})`);
            })
            .on('end', function (event) {
                // Stop propagation
                event.sourceEvent.stopPropagation();

                const bends = connectionData.ui.bends || [];

                if (connectionData.ui.dragging && bends.length > 1) {
                    // Remove drag indicator
                    connection.select('rect.label-drag').remove();

                    // Get the new label index
                    const newLabelIndex = connectionData.ui.tempLabelIndex ?? 0;
                    const currentLabelIndex = connectionData.entity.component?.labelIndex ?? 0;

                    // Only save if the label index actually changed
                    if (newLabelIndex !== currentLabelIndex) {
                        context.callbacks.onLabelDragEnd?.(connectionData, newLabelIndex);
                        // Keep tempLabelIndex until save succeeds/fails
                        // It will be cleared by confirmConnectionLabelIndex() or revertConnectionLabelIndex()
                    } else {
                        // No change, clean up immediately
                        delete connectionData.ui.tempLabelIndex;
                    }
                }

                // Mark dragging complete
                connectionData.ui.dragging = false;
            });

        // Remove drag behavior to prevent stale closures
        labelContainer.on('.drag', null);
        // Apply drag behavior to label container
        labelContainer.call(labelDrag);
    }

    /**
     * Calculate the position for the connection label (centered on path)
     */
    public static getLabelPosition(d: CanvasConnection): { x: number; y: number } {
        const bends = d.ui.bends || [];
        // Use tempLabelIndex during drag, otherwise use saved labelIndex
        const labelIndex = d.ui.tempLabelIndex ?? d.entity.component?.labelIndex ?? 0;

        let x: number;
        let y: number;

        // If there are bend points, use the labelIndex to position on a bend
        if (bends.length > 0) {
            const i = Math.min(Math.max(0, labelIndex), bends.length - 1);
            x = bends[i].x;
            y = bends[i].y;
        } else {
            // Otherwise use the midpoint between start and end
            const start = d.ui.start;
            const end = d.ui.end;

            if (!start || !end) {
                return { x: 0, y: 0 };
            }

            x = (start.x + end.x) / 2;
            y = (start.y + end.y) / 2;
        }

        // Get the actual label height from the DOM
        // The label container is a sibling of the connection path
        const connectionId = d.entity.id;
        const labelBody = d3.select(`#id-${connectionId} g.connection-label-container rect.body`);
        const labelHeight = labelBody.empty()
            ? CanvasConstants.CONNECTION_ROW_HEIGHT
            : parseFloat(labelBody.attr('height'));

        // Offset to account for label dimensions (center the label on the point)
        return {
            x: x - CanvasConstants.CONNECTION_LABEL.width / 2,
            y: y - labelHeight / 2
        };
    }

    /**
     * Check if connection is at backpressure
     */
    private static isAtBackPressure(d: CanvasConnection): boolean {
        return (
            ConnectionRenderer.isFullCount(d) ||
            ConnectionRenderer.isFullBytes(d) ||
            d.entity.status?.aggregateSnapshot?.flowFileAvailability === 'ALL_PENALIZED'
        );
    }

    /**
     * Generate tooltip content for object count backpressure bar
     */
    private static getBackPressureCountTip(d: CanvasConnection, componentUtils: any): string[] {
        const tooltipLines: string[] = [];
        const percentUseCount = d.entity.status?.aggregateSnapshot?.percentUseCount;

        if (percentUseCount != null) {
            const predictions = d.entity.status?.aggregateSnapshot?.predictions;
            const percentUseCountClamped = Math.min(Math.max(percentUseCount, 0), 100);

            if (d.entity.permissions?.canRead) {
                const objectThreshold = d.entity.component?.backPressureObjectThreshold;
                tooltipLines.push(
                    `Queue: ${percentUseCountClamped}% full (based on ${objectThreshold} object threshold)`
                );
            } else {
                tooltipLines.push(`Queue: ${percentUseCountClamped}% full`);
            }

            if (predictions != null) {
                const predictedPercentCount = predictions.predictedPercentCount;
                const timeToBackPressure = predictions.predictedMillisUntilCountBackpressure;

                // only show predicted percent if it is non-negative
                const predictionIntervalSeconds = predictions.predictionIntervalSeconds;
                const predictedPercentCountClamped = Math.min(Math.max(predictedPercentCount, 0), 100);
                tooltipLines.push(
                    `Predicted queue (next ${predictionIntervalSeconds / 60} mins): ${predictedPercentCountClamped}%`
                );

                // only show an estimate if it is valid (non-negative but less than the max number supported)
                const isAtBackPressure = ConnectionRenderer.isAtBackPressure(d);
                if (timeToBackPressure >= 0 && timeToBackPressure < Number.MAX_SAFE_INTEGER && !isAtBackPressure) {
                    const duration = componentUtils.formatPredictedDuration(timeToBackPressure);
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
     * Generate tooltip content for data size backpressure bar
     */
    private static getBackPressureSizeTip(d: CanvasConnection, componentUtils: any): string[] {
        const tooltipLines: string[] = [];
        const percentUseBytes = d.entity.status?.aggregateSnapshot?.percentUseBytes;

        if (percentUseBytes != null) {
            const predictions = d.entity.status?.aggregateSnapshot?.predictions;
            const percentUseBytesClamped = Math.min(Math.max(percentUseBytes, 0), 100);

            if (d.entity.permissions?.canRead) {
                const dataSizeThreshold = d.entity.component?.backPressureDataSizeThreshold;
                tooltipLines.push(
                    `Queue: ${percentUseBytesClamped}% full (based on ${dataSizeThreshold} data size threshold)`
                );
            } else {
                tooltipLines.push(`Queue: ${percentUseBytesClamped}% full`);
            }

            if (predictions != null) {
                const predictedPercentBytes = predictions.predictedPercentBytes;
                const timeToBackPressure = predictions.predictedMillisUntilBytesBackpressure;

                // only show predicted percent if it is non-negative
                const predictionIntervalSeconds = predictions.predictionIntervalSeconds;
                const predictedPercentBytesClamped = Math.min(Math.max(predictedPercentBytes, 0), 100);
                tooltipLines.push(
                    `Predicted queue (next ${predictionIntervalSeconds / 60} mins): ${predictedPercentBytesClamped}%`
                );

                // only show an estimate if it is valid (non-negative but less than the max number supported)
                const isAtBackPressure = ConnectionRenderer.isAtBackPressure(d);
                if (timeToBackPressure >= 0 && timeToBackPressure < Number.MAX_SAFE_INTEGER && !isAtBackPressure) {
                    const duration = componentUtils.formatPredictedDuration(timeToBackPressure);
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

    /**
     * Add tick marks to backpressure bar
     */
    private static addBackpressureTicks(container: any, xOffset: number, yOffset: number, barWidth: number): void {
        const tickPositions = [0, 0.25, 0.5, 0.75, 1.0];

        tickPositions.forEach((position) => {
            const x = xOffset + barWidth * position;
            container
                .append('rect')
                .attr('class', 'backpressure-tick')
                .attr('x', x)
                .attr('y', yOffset)
                .attr('width', 1)
                .attr('height', CanvasConstants.CONNECTION_HEIGHT_FOR_BACKPRESSURE)
                .attr('fill', '#ffffff');
        });
    }

    /**
     * Add connection icons (expiration, load balance, retry)
     *
     * Icons are created but hidden by default - visibility is controlled by CSS
     */
    private static addConnectionIcons(container: any, connectionData: any): void {
        // Check which icons should be visible

        // Expiration: check if flowFileExpiration is set and the numeric value is > 0
        let hasExpiration = false;
        if (connectionData.flowFileExpiration) {
            const match = connectionData.flowFileExpiration.match(/^(\d*\.?\d+).*/);
            if (match !== null && match.length > 0) {
                if (parseFloat(match[1]) > 0) {
                    hasExpiration = true;
                }
            }
        }

        const hasLoadBalance =
            connectionData.loadBalanceStrategy && connectionData.loadBalanceStrategy !== 'DO_NOT_LOAD_BALANCE';
        const hasRetry = connectionData.retriedRelationships && connectionData.retriedRelationships.length > 0;

        // Calculate x positions for icons (stack from right to left)
        // Start at x=224 (width - 16) and move left by 16px for each visible icon
        const baseX = CanvasConstants.CONNECTION_LABEL.width - 16; // 224 for 240px width
        let retryX = baseX;
        let loadBalanceX = baseX;

        // If expiration is visible, shift retry and load-balance left
        if (hasExpiration) {
            retryX -= 16;
            loadBalanceX -= 16;
        }
        // If retry is visible, shift load-balance left
        if (hasRetry) {
            loadBalanceX -= 16;
        }

        // Load balance icon - leftmost position
        const loadBalanceIcon = container
            .append('text')
            .attr('class', hasLoadBalance ? 'load-balance-icon primary-color' : 'load-balance-icon hidden')
            .attr('x', loadBalanceX)
            .attr('y', 14)
            .text('\uf042');

        loadBalanceIcon
            .append('title')
            .text(
                hasLoadBalance ? `Load Balance is configured with '${connectionData.loadBalanceStrategy}' strategy` : ''
            );

        // Retry icon - middle position
        const retryIcon = container
            .append('text')
            .attr('class', hasRetry ? 'retry-icon primary-color' : 'retry-icon hidden')
            .attr('x', retryX)
            .attr('y', 14)
            .text('\uf021');

        retryIcon
            .append('title')
            .text(
                hasRetry
                    ? `Relationships configured to be retried: ${connectionData.retriedRelationships.join(', ')}`
                    : ''
            );

        // Expiration icon - rightmost position at x=224
        const expirationIcon = container
            .append('text')
            .attr('class', hasExpiration ? 'expiration-icon primary-color' : 'expiration-icon primary-color hidden')
            .attr('x', baseX)
            .attr('y', 14)
            .text('\uf017');

        expirationIcon
            .append('title')
            .text(hasExpiration ? `Expires FlowFiles older than ${connectionData.flowFileExpiration}` : '');
    }

    /**
     * Update penalized icon visibility based on connection status
     */
    private static updatePenalizedIcon(
        connection: d3.Selection<any, CanvasConnection, any, any>,
        d: CanvasConnection
    ): void {
        const connectionLabelContainer = connection.select('g.connection-label-container');
        const queuedContainer = connectionLabelContainer.select('g.queued-container');

        if (queuedContainer.empty()) {
            return;
        }

        // Create penalized icon if it doesn't exist
        let penalizedIcon: any = queuedContainer.select('text.penalized-icon');
        if (penalizedIcon.empty()) {
            penalizedIcon = queuedContainer
                .append('text')
                .attr('class', 'penalized-icon primary-color')
                .attr('y', 14)
                .text('\uf252'); // FontAwesome clock-o icon

            penalizedIcon.append('title');
        }

        // Determine visibility based on flowFileAvailability
        const flowFileAvailability = d.entity.status?.aggregateSnapshot?.flowFileAvailability;
        const isPenalized = flowFileAvailability === 'HEAD_OF_QUEUE_PENALIZED';

        penalizedIcon.classed('hidden', !isPenalized);

        // Calculate x position (stacks with other icons from right to left)
        if (!isPenalized) {
            return;
        }

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

        penalizedIcon.attr('x', 224 - offset);

        penalizedIcon
            .select('title')
            .text('A FlowFile is currently penalized and data cannot be processed at this time.');
    }

    /**
     * Update run status icons for source and destination components
     */
    private static updateRunStatusIcons(
        connection: d3.Selection<any, CanvasConnection, any, any>,
        d: CanvasConnection
    ): void {
        const connectionData = d.entity.component;
        if (!connectionData) {
            return;
        }

        const connectionLabelContainer = connection.select('g.connection-label-container');

        // Update source run status icon
        const sourceContainer = connectionLabelContainer.select('g.connection-from-container');
        if (!sourceContainer.empty() && connectionData.source) {
            const sourceRunStatus = ConnectionRenderer.determineRunStatus(
                connectionData.source.exists,
                connectionData.source.running
            );
            ConnectionRenderer.updateRunStatusIcon(sourceContainer, sourceRunStatus, 'connection-from-run-status');
        }

        // Update destination run status icon
        const destinationContainer = connectionLabelContainer.select('g.connection-to-container');
        if (!destinationContainer.empty() && connectionData.destination) {
            const destRunStatus = ConnectionRenderer.determineRunStatus(
                connectionData.destination.exists,
                connectionData.destination.running
            );
            ConnectionRenderer.updateRunStatusIcon(destinationContainer, destRunStatus, 'connection-to-run-status');
        }
    }

    /**
     * Determine run status from exists and running flags
     */
    private static determineRunStatus(exists: boolean | undefined, running: boolean | undefined): string | undefined {
        if (exists === false) {
            return 'invalid';
        } else if (running === true) {
            return 'running';
        } else if (running === false) {
            return 'stopped';
        }
        return undefined;
    }

    /**
     * Update a single run status icon
     */
    private static updateRunStatusIcon(container: any, runStatus: string | undefined, className: string): void {
        let icon: any = container.select(`text.${className}`);

        // Create icon if it doesn't exist
        if (icon.empty()) {
            icon = container.append('text').attr('class', className).attr('x', 5).attr('y', 14);

            // Add title element
            icon.append('title');
        }

        // Update icon based on run status
        if (!runStatus) {
            icon.classed('hidden', true);
            return;
        }

        // Clear previous state classes
        icon.classed('hidden', false)
            .classed('running', false)
            .classed('stopped', false)
            .classed('invalid', false)
            .classed('disabled', false)
            .classed('is-missing-port', false)
            .classed('success-color-default', false)
            .classed('error-color-variant', false)
            .classed('caution-color', false);

        // Set icon glyph and classes based on status
        switch (runStatus.toLowerCase()) {
            case 'running':
                icon.text('\uf04b') // play icon
                    .classed('running', true)
                    .classed('success-color-default', true);
                icon.select('title').text('Running');
                break;
            case 'stopped':
                icon.text('\uf04d') // stop icon
                    .classed('stopped', true)
                    .classed('error-color-variant', true);
                icon.select('title').text('Stopped');
                break;
            case 'invalid':
                icon.text('\uf071') // warning icon
                    .classed('is-missing-port', true)
                    .classed('invalid', true)
                    .classed('caution-color', true);
                icon.select('title').text('Invalid');
                break;
            case 'disabled':
                icon.text('\uf05e') // ban icon
                    .classed('disabled', true);
                icon.select('title').text('Disabled');
                break;
            default:
                icon.classed('hidden', true);
        }
    }

    /**
     * Calculate connection path with proper perimeter intersection
     *
     * Steps:
     * 1. Resolve source/destination component IDs (handle ports in groups)
     * 2. Look up source/destination components on canvas to get positions
     * 3. Initialize ui.bends from entity.bends (source of truth for bend points)
     * 4. Calculate perimeter intersection points (not centers)
     * 5. Build path: source edge → bend points → destination edge
     * 6. Store start/end/bends on connection.ui for box selection
     */
    public static calculatePath(connection: CanvasConnection): string {
        const conn = connection.entity;

        // Get the current process group ID from the first component we can find
        // This is needed to determine if ports are in sub-groups
        const allComponents = d3.selectAll(
            'g.label, g.processor, g.funnel, g.input-port, g.output-port, g.remote-process-group, g.process-group'
        );
        if (allComponents.empty()) {
            return '';
        }
        const firstComponent: any = allComponents.datum();
        const currentProcessGroupId = firstComponent?.entity?.component?.parentGroupId || 'root';

        // Resolve source component ID (handle ports in groups)
        // If the connection's sourceGroupId is different from current group, use the group ID
        let sourceId = conn.sourceId;
        if (conn.sourceGroupId && conn.sourceGroupId !== currentProcessGroupId) {
            sourceId = conn.sourceGroupId;
        }

        // Resolve destination component ID (handle ports in groups)
        let destId = conn.destinationId;
        if (conn.destinationGroupId && conn.destinationGroupId !== currentProcessGroupId) {
            destId = conn.destinationGroupId;
        }

        if (!sourceId || !destId) {
            return '';
        }

        // Look up source and destination components on canvas
        const sourceElement = d3.select(`#id-${sourceId}`);
        const destElement = d3.select(`#id-${destId}`);

        if (sourceElement.empty() || destElement.empty()) {
            return '';
        }

        const sourceData: any = sourceElement.datum();
        const destData: any = destElement.datum();

        if (!sourceData || !destData) {
            return '';
        }

        // Get bounding boxes
        // Use ui.currentPosition if available (during drag), otherwise use entity.position
        const sourcePosition = sourceData.ui?.currentPosition || sourceData.entity?.position || { x: 0, y: 0 };
        const destPosition = destData.ui?.currentPosition || destData.entity?.position || { x: 0, y: 0 };

        const sourceBBox = {
            x: sourcePosition.x,
            y: sourcePosition.y,
            width: sourceData.ui?.dimensions?.width || 0,
            height: sourceData.ui?.dimensions?.height || 0
        };

        const destBBox = {
            x: destPosition.x,
            y: destPosition.y,
            width: destData.ui?.dimensions?.width || 0,
            height: destData.ui?.dimensions?.height || 0
        };

        // Calculate centers
        const sourceCenter = {
            x: sourceBBox.x + sourceBBox.width / 2,
            y: sourceBBox.y + sourceBBox.height / 2
        };

        const destCenter = {
            x: destBBox.x + destBBox.width / 2,
            y: destBBox.y + destBBox.height / 2
        };

        // Initialize ui.bends from entity.bends if not already set (source of truth)
        // ui.bends is used for rendering and allows optimistic updates.
        // Must create a NEW array and NEW objects to prevent state mutation
        // Even though the selector creates a deep copy, we need to ensure ui.bends
        // is a completely separate array that can be safely mutated
        if (!connection.ui.bends) {
            // Create a new array with new objects (not just spreading the existing array)
            connection.ui.bends = conn.bends ? conn.bends.map((b: Position) => ({ x: b.x, y: b.y })) : [];
        }

        // Get bend points from ui (used for rendering, allows optimistic updates)
        const bends: Array<{ x: number; y: number }> = connection.ui.bends || [];

        // Calculate appropriate start anchor (first bend or destination)
        const startAnchor = bends.length > 0 ? bends[0] : destCenter;

        // Calculate appropriate end anchor (last bend or source)
        const endAnchor = bends.length > 0 ? bends[bends.length - 1] : sourceCenter;

        // Calculate perimeter intersection points
        // Source: Find point on source perimeter closest to start anchor
        const sourcePoint = ConnectionRenderer.getPerimeterPoint(startAnchor, sourceBBox);

        // Destination: Find point on destination perimeter closest to end anchor
        const destPoint = ConnectionRenderer.getPerimeterPoint(endAnchor, destBBox);

        // Store calculated positions on connection.ui for box selection logic
        connection.ui.start = sourcePoint;
        connection.ui.end = destPoint;
        connection.ui.bends = bends;

        // Build path: start → bends → end
        if (bends.length === 0) {
            // Simple straight line
            return `M ${sourcePoint.x},${sourcePoint.y} L ${destPoint.x},${destPoint.y}`;
        } else {
            // Path with bend points
            const pathParts: string[] = [`M ${sourcePoint.x},${sourcePoint.y}`];
            bends.forEach((bend) => {
                pathParts.push(`L ${bend.x},${bend.y}`);
            });
            pathParts.push(`L ${destPoint.x},${destPoint.y}`);
            return pathParts.join(' ');
        }
    }

    /**
     * Find the best index to insert a new bend point based on click position
     * Determines which path segment was clicked and returns the index to insert after
     */
    private static findBendPointInsertIndex(connection: CanvasConnection, clickX: number, clickY: number): number {
        const start = connection.ui.start;
        const end = connection.ui.end;
        const bends = connection.ui.bends || [];

        // Build array of all points in the path: [start, ...bends, end]
        const pathPoints: Array<{ x: number; y: number }> = [start, ...bends, end];

        // Find the segment closest to the click point
        let minDistance = Infinity;
        let closestSegmentIndex = 0;

        for (let i = 0; i < pathPoints.length - 1; i++) {
            const p1 = pathPoints[i];
            const p2 = pathPoints[i + 1];

            // Calculate distance from click point to this line segment
            const distance = ConnectionRenderer.pointToSegmentDistance(clickX, clickY, p1.x, p1.y, p2.x, p2.y);

            if (distance < minDistance) {
                minDistance = distance;
                closestSegmentIndex = i;
            }
        }

        // Return the index to insert after (accounting for start point not being in bends array)
        return closestSegmentIndex;
    }

    /**
     * Calculate the distance from a point to a line segment
     */
    private static pointToSegmentDistance(
        px: number,
        py: number,
        x1: number,
        y1: number,
        x2: number,
        y2: number
    ): number {
        const dx = x2 - x1;
        const dy = y2 - y1;
        const lengthSquared = dx * dx + dy * dy;

        if (lengthSquared === 0) {
            // Segment is a point
            return Math.sqrt((px - x1) * (px - x1) + (py - y1) * (py - y1));
        }

        // Calculate projection of point onto line (clamped to segment)
        let t = ((px - x1) * dx + (py - y1) * dy) / lengthSquared;
        t = Math.max(0, Math.min(1, t));

        // Find closest point on segment
        const closestX = x1 + t * dx;
        const closestY = y1 + t * dy;

        // Return distance
        return Math.sqrt((px - closestX) * (px - closestX) + (py - closestY) * (py - closestY));
    }

    /**
     * Calculate the point on the perimeter of a bounding box closest to the specified point
     *
     * This uses trigonometry to find which edge of the rectangle the line intersects
     * and calculates the exact intersection point.
     */
    private static getPerimeterPoint(p: { x: number; y: number }, bBox: any): { x: number; y: number } {
        const TWO_PI = 2 * Math.PI;

        // Calculate theta (angle of rectangle diagonal)
        const theta = Math.atan2(bBox.height, bBox.width);

        // Get rectangle radius
        const xRadius = bBox.width / 2;
        const yRadius = bBox.height / 2;

        // Get center point
        const cx = bBox.x + xRadius;
        const cy = bBox.y + yRadius;

        // Calculate alpha (angle from center to point p)
        const dx = p.x - cx;
        const dy = p.y - cy;
        let alpha = Math.atan2(dy, dx);

        // Normalize alpha into 0 <= alpha < 2*PI
        alpha = alpha % TWO_PI;
        if (alpha < 0) {
            alpha += TWO_PI;
        }

        // Calculate beta (complementary angle)
        const beta = Math.PI / 2 - alpha;

        // Detect quadrant and return point on perimeter
        if ((alpha >= 0 && alpha < theta) || (alpha >= TWO_PI - theta && alpha < TWO_PI)) {
            // Right edge
            return {
                x: bBox.x + bBox.width,
                y: cy + Math.tan(alpha) * xRadius
            };
        } else if (alpha >= theta && alpha < Math.PI - theta) {
            // Bottom edge
            return {
                x: cx + Math.tan(beta) * yRadius,
                y: bBox.y + bBox.height
            };
        } else if (alpha >= Math.PI - theta && alpha < Math.PI + theta) {
            // Left edge
            return {
                x: bBox.x,
                y: cy - Math.tan(alpha) * xRadius
            };
        } else {
            // Top edge
            return {
                x: cx - Math.tan(beta) * yRadius,
                y: bBox.y
            };
        }
    }

    /**
     * Calculate the squared distance between two points
     * Used by distance calculations to avoid unnecessary sqrt operations
     */
    private static distanceSquared(v: Position, w: Position): number {
        return Math.pow(v.x - w.x, 2) + Math.pow(v.y - w.y, 2);
    }

    /**
     * Calculate the distance between two points
     */
    private static distanceBetweenPoints(v: Position, w: Position): number {
        return Math.sqrt(ConnectionRenderer.distanceSquared(v, w));
    }
}
