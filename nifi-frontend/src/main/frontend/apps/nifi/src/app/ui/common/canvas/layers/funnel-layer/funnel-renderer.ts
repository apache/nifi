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
import { CanvasFunnel } from '../../canvas.types';
import { FunnelRenderContext } from '../render-context.types';
import { ConnectionRenderer } from '../connection-layer/connection-renderer';
import { CanvasConstants } from '../../canvas.constants';

export class FunnelRenderer {
    public static render(context: FunnelRenderContext): void {
        const { containerSelection, funnels, canSelect, callbacks } = context;

        // D3 data join
        const selection = containerSelection
            .selectAll<SVGGElement, CanvasFunnel>('g.funnel')
            .data(funnels, (d: CanvasFunnel) => d.entity.id);

        // Enter: create new funnel elements
        const entered: any = selection.enter();
        const appendedGroups: any = FunnelRenderer.appendFunnelElements(entered);

        // Update existing and newly entered funnels
        const merged: any = selection.merge(appendedGroups);
        FunnelRenderer.updateFunnelElements(merged, context);

        // Attach event listeners if interactive
        // Use mousedown for selection to ensure single, deterministic event firing
        if (canSelect && callbacks) {
            if (callbacks.onClick) {
                merged.on('mousedown.selection', function (event: MouseEvent, d: CanvasFunnel) {
                    // Only handle left mouse button
                    if (event.button !== 0) {
                        return;
                    }

                    event.stopPropagation();
                    callbacks.onClick!(d, event);
                });
            }
            if (callbacks.onDoubleClick) {
                merged.on('dblclick', function (event: MouseEvent, d: CanvasFunnel) {
                    event.stopPropagation();
                    callbacks.onDoubleClick!(d, event);
                });
            }

            // Attach drag behavior if callback is provided (filter will check canEdit dynamically)
            if (callbacks.onDragEnd) {
                FunnelRenderer.attachDragBehavior(merged, context);
            }
        }

        // Exit: remove funnels that are no longer in data
        const exited: any = selection.exit();
        FunnelRenderer.removeFunnelElements(exited);
    }

    private static appendFunnelElements(
        entered: d3.Selection<any, CanvasFunnel, any, any>
    ): d3.Selection<any, CanvasFunnel, any, any> {
        // Create group for each funnel
        const funnelGroups = entered
            .append('g')
            .attr('id', (d) => `id-${d.entity.id}`)
            .attr('class', 'funnel component')
            .attr('transform', (d) => `translate(${d.entity.position.x}, ${d.entity.position.y})`);

        // Funnel border (selection/authorization indicator)
        funnelGroups
            .append('rect')
            .attr('rx', 2)
            .attr('ry', 2)
            .attr('class', 'border')
            .attr('width', (d) => d.ui.dimensions.width)
            .attr('height', (d) => d.ui.dimensions.height)
            .attr('fill', 'transparent')
            .attr('stroke', 'transparent');

        // Funnel body
        // Note: Let CSS handle fill, stroke, and styling via global theme
        funnelGroups
            .append('rect')
            .attr('rx', 2)
            .attr('ry', 2)
            .attr('class', 'body')
            .attr('width', (d) => d.ui.dimensions.width)
            .attr('height', (d) => d.ui.dimensions.height)
            .attr('filter', 'url(#component-drop-shadow)')
            .attr('stroke-width', 0);

        // Funnel icon (flowfont character)
        // Note: Let CSS handle font-family, font-size, and fill via global theme
        funnelGroups.append('text').attr('class', 'funnel-icon').attr('x', 9).attr('y', 34).text('\ue803'); // Funnel icon from flowfont

        return funnelGroups;
    }

    private static updateFunnelElements(
        selection: d3.Selection<any, CanvasFunnel, any, any>,
        context: FunnelRenderContext
    ): void {
        // Update transform for position (use currentPosition if dragging, otherwise entity position)
        selection.attr('transform', (d) => {
            const pos = d.ui.currentPosition || d.entity.position;
            return `translate(${pos.x}, ${pos.y})`;
        });

        // Apply disabled class and styling when funnel is being saved
        selection.each(function (d) {
            const isDisabled = context.disabledFunnelIds?.has(d.entity.id) || false;
            const element = d3.select(this);
            element.classed('disabled', isDisabled);

            if (isDisabled) {
                element.style('opacity', 0.6).style('cursor', 'not-allowed');
            } else {
                element.style('opacity', null).style('cursor', context.canSelect ? 'pointer' : 'default');
            }
        });

        // Update border
        selection.select('rect.border').classed('unauthorized', (d) => d.entity.permissions.canRead === false);

        // Update body for authorization
        selection.select('rect.body').classed('unauthorized', (d) => d.entity.permissions.canRead === false);
    }

    private static removeFunnelElements(exited: d3.Selection<any, any, any, any>): void {
        exited.remove();
    }

    private static attachDragBehavior(
        selection: d3.Selection<SVGGElement, CanvasFunnel, any, any>,
        context: FunnelRenderContext
    ): void {
        const drag = d3
            .drag<SVGGElement, CanvasFunnel>()
            .filter(function (event, d) {
                // Match D3's default filter: block right-click and Ctrl+click (Mac right-click)
                if (event.ctrlKey || event.button !== 0) {
                    return false;
                }
                // Block drag if editing is disabled
                if (!context.getCanEdit()) {
                    return false;
                }
                // Block drag if funnel is disabled (saving)
                if (context.disabledFunnelIds?.has(d.entity.id)) {
                    return false;
                }
                return true;
            })
            .clickDistance(4) // Minimum distance in pixels before drag starts (prevents accidental drags during clicks)
            .on('start', function (event: d3.D3DragEvent<SVGGElement, CanvasFunnel, CanvasFunnel>, d: CanvasFunnel) {
                const funnelGroup = d3.select(this as SVGGElement);

                if (!funnelGroup.classed('selected') && context.callbacks.onClick) {
                    context.callbacks.onClick(d, event.sourceEvent as MouseEvent);
                }

                // Stop propagation to prevent canvas pan
                event.sourceEvent.stopPropagation();

                // Store original position for potential revert
                d.ui.dragStartPosition = { ...d.entity.position };
                // Initialize current position in UI state
                d.ui.currentPosition = { ...d.entity.position };
            })
            .on('drag', function (event: d3.D3DragEvent<SVGGElement, CanvasFunnel, CanvasFunnel>, d: CanvasFunnel) {
                // Update current position in UI state (entity is read-only from store)
                if (d.ui.currentPosition) {
                    // Apply snap-to-grid unless shift key is held
                    const snapEnabled = !event.sourceEvent.shiftKey;

                    d.ui.currentPosition.x += event.dx;
                    d.ui.currentPosition.y += event.dy;

                    // Apply snap alignment if enabled
                    const displayX = snapEnabled
                        ? Math.round(d.ui.currentPosition.x / CanvasConstants.SNAP_ALIGNMENT_PIXELS) *
                          CanvasConstants.SNAP_ALIGNMENT_PIXELS
                        : d.ui.currentPosition.x;
                    const displayY = snapEnabled
                        ? Math.round(d.ui.currentPosition.y / CanvasConstants.SNAP_ALIGNMENT_PIXELS) *
                          CanvasConstants.SNAP_ALIGNMENT_PIXELS
                        : d.ui.currentPosition.y;

                    // Update visual position immediately with snapped coordinates
                    d3.select(this).attr('transform', `translate(${displayX}, ${displayY})`);

                    // Update attached connections by recalculating their paths
                    d3.selectAll('g.connection').each(function () {
                        const connectionData: any = d3.select(this).datum();

                        // Check if this connection is attached to the dragged funnel
                        if (
                            connectionData?.entity?.sourceId === d.entity.id ||
                            connectionData?.entity?.destinationId === d.entity.id
                        ) {
                            const connectionGroup = d3.select(this);

                            // Recalculate path using ConnectionRenderer
                            const newPath = ConnectionRenderer.calculatePath(connectionData);

                            // Update all path elements with the new path
                            connectionGroup.selectAll('path').attr('d', newPath);

                            // Update connection label position
                            const labelPosition = ConnectionRenderer.getLabelPosition(connectionData);
                            connectionGroup
                                .select('g.connection-label-container')
                                .attr('transform', `translate(${labelPosition.x}, ${labelPosition.y})`);
                        }
                    });
                }
            })
            .on('end', function (event: d3.D3DragEvent<SVGGElement, CanvasFunnel, CanvasFunnel>, d: CanvasFunnel) {
                if (!d.ui.dragStartPosition || !d.ui.currentPosition) {
                    return;
                }

                // Apply final snap alignment (respecting shift key)
                const snapEnabled = !event.sourceEvent.shiftKey;
                const finalX = snapEnabled
                    ? Math.round(d.ui.currentPosition.x / CanvasConstants.SNAP_ALIGNMENT_PIXELS) *
                      CanvasConstants.SNAP_ALIGNMENT_PIXELS
                    : d.ui.currentPosition.x;
                const finalY = snapEnabled
                    ? Math.round(d.ui.currentPosition.y / CanvasConstants.SNAP_ALIGNMENT_PIXELS) *
                      CanvasConstants.SNAP_ALIGNMENT_PIXELS
                    : d.ui.currentPosition.y;

                // Update currentPosition to final snapped position
                d.ui.currentPosition.x = finalX;
                d.ui.currentPosition.y = finalY;

                const newPosition = { ...d.ui.currentPosition };
                const previousPosition = { ...d.ui.dragStartPosition };

                // Check if position actually changed (prevent API calls on clicks)
                const moved = newPosition.x !== previousPosition.x || newPosition.y !== previousPosition.y;

                // Clean up drag start position only
                // Keep currentPosition until API call completes (for disabled treatment)
                delete d.ui.dragStartPosition;

                // Only emit drag end if position actually changed
                if (moved && context.callbacks.onDragEnd) {
                    context.callbacks.onDragEnd(d, newPosition, previousPosition);
                }
            });

        // Remove drag behavior to prevent stale closures
        selection.on('.drag', null);

        // Apply drag behavior to funnels with write permissions
        selection.filter((d: CanvasFunnel) => d.entity.permissions.canWrite && d.entity.permissions.canRead).call(drag);
    }

    public static pan(selection: d3.Selection<any, CanvasFunnel, any, any>, context: FunnelRenderContext): void {
        // Simply delegate to updateFunnelElements which handles all updates
        // Funnels are simple and don't have details to create/remove like ports or processors
        FunnelRenderer.updateFunnelElements(selection, context);
    }
}
