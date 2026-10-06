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
import { CanvasFunnel, CanvasSelection } from '../../canvas.types';
import { FunnelRenderContext } from '../render-context.types';
import { DragUtils } from '../../utils/drag.utils';

export class FunnelRenderer {
    public static render(context: FunnelRenderContext): void {
        const { containerSelection, funnels, canSelect, callbacks } = context;

        // D3 data join
        const selection = containerSelection
            .selectAll<SVGGElement, CanvasFunnel>('g.funnel')
            .data(funnels, (d: CanvasFunnel) => d.entity.id);

        // Enter: create new funnel elements
        const entered = selection.enter();
        const appendedGroups = FunnelRenderer.appendFunnelElements(entered);

        // Update existing and newly entered funnels
        const merged = selection.merge(appendedGroups);
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
        selection.exit().remove();
    }

    private static appendFunnelElements(entered: d3.Selection<d3.EnterElement, CanvasFunnel, SVGGElement, unknown>) {
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

    private static updateFunnelElements(selection: CanvasSelection<CanvasFunnel>, context: FunnelRenderContext): void {
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

    private static removeFunnelElements(exited: CanvasSelection<CanvasFunnel>): void {
        exited.remove();
    }

    private static attachDragBehavior(selection: CanvasSelection<CanvasFunnel>, context: FunnelRenderContext): void {
        DragUtils.attachComponentDrag(selection, {
            resolveCanvasRoot: context.canvasRootResolver,
            getCanEdit: context.getCanEdit,
            getCanSelect: context.getCanSelect,
            getDisabledIds: context.getDisabledFunnelIds,
            getSelectedIds: context.getSelectedIds,
            onDragEnd: context.callbacks.onDragEnd!
        });
    }

    public static pan(selection: CanvasSelection<CanvasFunnel>, context: FunnelRenderContext): void {
        // Simply delegate to updateFunnelElements which handles all updates
        // Funnels are simple and don't have details to create/remove like ports or processors
        FunnelRenderer.updateFunnelElements(selection, context);
    }
}
