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
import { ComponentType } from '@nifi/shared';
import { CanvasPort, CanvasSelection } from '../../canvas.types';
import { PortRenderContext } from '../render-context.types';
import { DragUtils } from '../../utils/drag.utils';
import { ValidationErrorsTip } from '../../../tooltips/validation-errors-tip/validation-errors-tip.component';
import { CanvasConstants } from '../../canvas.constants';

export class PortRenderer {
    private static offsetY(d: CanvasPort, baseY: number): number {
        return d.entity.allowRemoteAccess ? baseY + CanvasConstants.PORT_REMOTE_BANNER_HEIGHT : baseY;
    }

    private static isLocalPort(d: CanvasPort): boolean {
        return !d.entity.allowRemoteAccess;
    }

    public static render(context: PortRenderContext): void {
        const { containerSelection, ports, canSelect, callbacks } = context;

        // D3 data join
        const selection = containerSelection
            .selectAll<SVGGElement, CanvasPort>('g.input-port, g.output-port')
            .data(ports, (d: CanvasPort) => d.entity.id);

        // Enter: create new port elements
        const entered = selection.enter();
        const appendedGroups = PortRenderer.appendPortElements(entered);

        // Update existing and newly entered ports
        const merged = selection.merge(appendedGroups);
        PortRenderer.updatePortElements(merged, context);

        // Attach event listeners if interactive
        // Use mousedown for selection to ensure single, deterministic event firing
        if (canSelect && callbacks) {
            if (callbacks.onClick) {
                merged.on('mousedown.selection', function (event: MouseEvent, d: CanvasPort) {
                    // Only handle left mouse button
                    if (event.button !== 0) {
                        return;
                    }

                    event.stopPropagation();
                    callbacks.onClick!(d, event);
                });
            }
            if (callbacks.onDoubleClick) {
                merged.on('dblclick', function (event: MouseEvent, d: CanvasPort) {
                    event.stopPropagation();
                    callbacks.onDoubleClick!(d, event);
                });
            }
            // Attach drag behavior for position updates if editing is allowed
            if (callbacks.onDragEnd && context.getCanEdit()) {
                PortRenderer.attachDragBehavior(merged, context);
            }
        }

        // Exit: remove ports that are no longer in data
        selection.exit().remove();
    }

    private static appendPortElements(entered: d3.Selection<d3.EnterElement, CanvasPort, SVGGElement, unknown>) {
        // Create group for each port
        const portGroups = entered
            .append('g')
            .attr('id', (d) => `id-${d.entity.id}`)
            .attr('class', (d) => {
                const type = d.ui.componentType === ComponentType.InputPort ? 'input-port' : 'output-port';
                return `${type} component`;
            })
            .attr('transform', (d) => `translate(${d.entity.position.x}, ${d.entity.position.y})`);

        // Port border (selection/authorization indicator)
        portGroups
            .append('rect')
            .attr('class', 'border')
            .attr('width', (d) => d.ui.dimensions.width)
            .attr('height', (d) => d.ui.dimensions.height)
            .attr('fill', 'transparent')
            .attr('stroke', 'transparent');

        // Port body
        // Note: Let CSS handle fill, stroke, and styling
        portGroups
            .append('rect')
            .attr('class', 'body')
            .attr('width', (d) => d.ui.dimensions.width)
            .attr('height', (d) => d.ui.dimensions.height)
            .attr('filter', 'url(#component-drop-shadow)')
            .attr('stroke-width', 0);

        // Remote banner (only for remote-accessible ports)
        portGroups
            .append('rect')
            .attr('class', 'remote-banner banner')
            .attr('width', 240)
            .attr('height', CanvasConstants.PORT_REMOTE_BANNER_HEIGHT)
            .classed('hidden', (d) => !d.entity.allowRemoteAccess);

        // Port icon (flowfont character)
        // Note: Let CSS handle font-family, font-size, and fill
        portGroups
            .append('text')
            .attr('class', 'port-icon')
            .attr('x', 10)
            .attr('y', (d) => (d.entity.allowRemoteAccess ? 38 + CanvasConstants.PORT_REMOTE_BANNER_HEIGHT : 38))
            .text((d) => {
                // Input port icon: \ue832, Output port icon: \ue833
                return d.ui.componentType === ComponentType.InputPort ? '\ue832' : '\ue833';
            });

        // Port name
        // Note: Let CSS handle font styling
        portGroups
            .append('text')
            .attr('class', 'port-name')
            .attr('x', 70)
            .attr('y', (d) => (d.entity.allowRemoteAccess ? 29 + CanvasConstants.PORT_REMOTE_BANNER_HEIGHT : 29))
            .attr('width', 95)
            .text((d) => d.entity.component?.name || d.entity.id);

        return portGroups;
    }

    private static updatePortElements(selection: CanvasSelection<CanvasPort>, context: PortRenderContext): void {
        const { textEllipsis } = context;

        // Update transform for position (use currentPosition during drag, otherwise entity position)
        selection.attr('transform', (d) => {
            const position = d.ui.currentPosition || d.entity.position;
            return `translate(${position.x}, ${position.y})`;
        });

        // Apply selected class to parent <g> for CSS selector
        // Apply disabled state (during save operation)
        selection
            .classed('disabled', (d) => context.disabledPortIds?.has(d.entity.id) || false)
            .style('opacity', (d) => (context.disabledPortIds?.has(d.entity.id) ? 0.6 : null))
            .style('cursor', (d) => {
                if (context.disabledPortIds?.has(d.entity.id)) return 'not-allowed';
                return context.canSelect ? 'pointer' : 'default';
            });

        // Update border height and authorization
        selection
            .select('rect.border')
            .attr('height', (d) => d.ui.dimensions.height)
            .classed('unauthorized', (d) => d.entity.permissions.canRead === false);

        // Update body height and authorization
        selection
            .select('rect.body')
            .attr('height', (d) => d.ui.dimensions.height)
            .classed('unauthorized', (d) => d.entity.permissions.canRead === false);

        // Update each port individually
        selection.each(function (this: SVGGElement, portData: CanvasPort) {
            const port = d3.select<SVGGElement, CanvasPort>(this);
            let details: CanvasSelection<CanvasPort> = port.select<SVGGElement>('g.port-details');

            // if this port is visible, render everything
            if (port.classed('visible')) {
                if (details.empty()) {
                    // Create details container on first render
                    details = port.append('g').attr('class', 'port-details');

                    // Port transmission icon (for remote ports)
                    details.append('text').attr('class', 'port-transmission-icon').attr('x', 10).attr('y', 18);

                    // Bulletin background
                    details
                        .append('rect')
                        .attr('class', 'bulletin-background')
                        .attr('x', 240 - CanvasConstants.PORT_OFFSET_VALUE)
                        .attr('width', CanvasConstants.PORT_OFFSET_VALUE)
                        .attr('height', CanvasConstants.PORT_OFFSET_VALUE)
                        .classed('hidden', PortRenderer.isLocalPort(portData));

                    // Bulletin icon
                    details
                        .append('text')
                        .attr('class', 'bulletin-icon')
                        .attr('x', 240 - 18)
                        .attr('y', 18)
                        .text('\uf24a')
                        .classed('hidden', PortRenderer.isLocalPort(portData));

                    // Run status icon
                    details
                        .append('text')
                        .attr('class', 'run-status-icon')
                        .attr('x', 50)
                        .attr('y', PortRenderer.offsetY(portData, 25));

                    // Comments icon
                    details
                        .append('text')
                        .attr('class', 'component-comments')
                        .attr(
                            'transform',
                            `translate(${portData.ui.dimensions.width - 11}, ${portData.ui.dimensions.height - 3})`
                        )
                        .text('\uf075');

                    // Active thread count icon
                    details
                        .append('text')
                        .attr('class', 'active-thread-count-icon')
                        .attr('y', PortRenderer.offsetY(portData, 43))
                        .text('\ue83f');

                    // Active thread count text
                    details
                        .append('text')
                        .attr('class', 'active-thread-count')
                        .attr('y', PortRenderer.offsetY(portData, 43));
                }

                // Update remote banner visibility (always update, regardless of permissions)
                port.select('rect.remote-banner').classed('hidden', PortRenderer.isLocalPort(portData));

                // Update port icon position (always update, regardless of permissions)
                port.select('text.port-icon').attr('y', PortRenderer.offsetY(portData, 38));

                // Update transmission icon for remote ports
                PortRenderer.updateTransmissionIcon(details, portData);

                // Update bulletin hidden class based on port type
                details.select('rect.bulletin-background').classed('hidden', PortRenderer.isLocalPort(portData));
                details.select('text.bulletin-icon').classed('hidden', PortRenderer.isLocalPort(portData));

                // Update bulletin visibility and styling based on actual bulletin data
                // Note: Call on port selection (not details)
                PortRenderer.updateBulletins(port, portData, context);

                // Update run status (always update, regardless of permissions)
                PortRenderer.updatePortStatus(port, portData, context);

                // Update active thread count (always update, regardless of permissions)
                PortRenderer.updateActiveThreadCount(port, portData, context);

                // Update port name and comments based on permissions
                //
                if (portData.entity.permissions.canRead) {
                    // update the port name
                    const portName = port.select('text.port-name');

                    // Reset the port name to handle any previous state
                    portName.text(null).selectAll('tspan, title').remove();

                    // Update position based on remote access
                    portName.attr('y', PortRenderer.offsetY(portData, 25));

                    // Update the port name with text wrapping
                    const name = portData.entity.component?.name || portData.entity.id;

                    // Set width for ellipsis calculation
                    portName.attr('width', 95);

                    // Apply text wrapping using TextEllipsisUtils
                    textEllipsis.applyEllipsis(portName, name, 'port-name');

                    // update the port comments
                    context.componentUtils.comments(port, portData.entity.component?.comments);
                } else {
                    // clear the port name
                    port.select('text.port-name').text(null);

                    // clear the port comments
                    context.componentUtils.comments(port, null);
                }
            } else {
                // Port is not visible (outside viewport) - show abbreviated name and remove details
                //
                if (portData.entity.permissions.canRead) {
                    // Show abbreviated port name for non-visible ports
                    const portName = port.select('text.port-name');
                    const name = portData.entity.component?.name || portData.entity.id;
                    if (name.length > 15) {
                        portName.text(name.substring(0, 15) + String.fromCharCode(8230)); // ellipsis character
                    } else {
                        portName.text(name);
                    }
                } else {
                    // Clear the port name for unauthorized ports
                    port.select('text.port-name').text(null);
                }

                // Remove the details if necessary
                if (!details.empty()) {
                    details.remove();
                }
            }
        });
    }

    private static updatePortStatus(
        port: CanvasSelection<CanvasPort>,
        d: CanvasPort,
        context: PortRenderContext
    ): void {
        const runStatusIcon = port.select('text.run-status-icon');

        if (!runStatusIcon.empty()) {
            // Update position based on remote access
            runStatusIcon.attr('y', PortRenderer.offsetY(d, 25));

            // Get run status from aggregateSnapshot
            const runStatus = d.entity.status?.aggregateSnapshot?.runStatus || 'Stopped';

            // Determine class based on status
            let clazz = 'primary-color';
            if (runStatus === 'Invalid') {
                clazz = 'invalid caution-color';
            } else if (runStatus === 'Running') {
                clazz = 'running success-color-default';
            } else if (runStatus === 'Stopped') {
                clazz = 'stopped error-color-variant';
            }

            // Set class
            runStatusIcon.attr('class', `run-status-icon ${clazz}`);

            // Determine font family based on status
            let fontFamily = 'FontAwesome';
            if (runStatus === 'Disabled') {
                fontFamily = 'flowfont';
            }
            runStatusIcon.attr('font-family', fontFamily);

            // Determine icon based on status
            let icon = '';
            if (runStatus === 'Disabled') {
                icon = '\ue802';
            } else if (runStatus === 'Invalid') {
                icon = '\uf071';
            } else if (runStatus === 'Running') {
                icon = '\uf04b';
            } else if (runStatus === 'Stopped') {
                icon = '\uf04d';
            }

            runStatusIcon.text(icon);

            // Add validation error tooltip if there are validation errors
            const needsValidationTip =
                d.entity.permissions.canRead &&
                d.entity.component?.validationErrors &&
                d.entity.component.validationErrors.length > 0;

            if (needsValidationTip) {
                context.componentUtils.canvasTooltip(ValidationErrorsTip, runStatusIcon, {
                    isValidating: false,
                    validationErrors: d.entity.component?.validationErrors || []
                });
            } else {
                context.componentUtils.resetCanvasTooltip(runStatusIcon);
            }
        }
    }

    private static updateTransmissionIcon(details: CanvasSelection<CanvasPort>, d: CanvasPort): void {
        const transmissionIcon = details.select('text.port-transmission-icon');

        if (!transmissionIcon.empty()) {
            // Hide for local ports, show for remote ports
            const isLocal = PortRenderer.isLocalPort(d);
            transmissionIcon.classed('hidden', isLocal);

            if (!isLocal) {
                // Update transmission icon based on status
                const isTransmitting = d.entity.status?.transmitting === true;

                // Set icon character
                const icon = isTransmitting ? '\uf140' : '\ue80a'; // Transmitting: satellite, Not transmitting: flowfont icon
                transmissionIcon.text(icon);

                // Set font family
                const fontFamily = isTransmitting ? 'FontAwesome' : 'flowfont';
                transmissionIcon.attr('font-family', fontFamily);

                // Set classes for styling
                // When transmitting: 'transmitting success-color-variant'
                // When not transmitting: 'not-transmitting neutral-color'
                transmissionIcon
                    .classed('transmitting success-color-variant', isTransmitting)
                    .classed('not-transmitting neutral-color', !isTransmitting);
            }
        }
    }

    private static updateBulletins(port: CanvasSelection<CanvasPort>, d: CanvasPort, context: PortRenderContext): void {
        // Use shared bulletin utility for consistent styling and tooltip behavior
        // Pass port selection (not details)
        context.componentUtils.bulletins(port, d.entity.bulletins ?? []);
    }

    private static updateActiveThreadCount(
        port: CanvasSelection<CanvasPort>,
        d: CanvasPort,
        context: PortRenderContext
    ): void {
        // Delegate to shared component utility for consistent active thread count rendering
        // This handles show/hide, X positioning, text content, classes, and tooltips
        context.componentUtils.activeThreadCount(port, d);

        // Apply port-specific Y positioning adjustment for remote banner offset
        // The shared utility handles X positioning, but Y position is component-specific
        port.select('text.active-thread-count-icon').attr('y', PortRenderer.offsetY(d, 43));
        port.select('text.active-thread-count').attr('y', PortRenderer.offsetY(d, 43));
    }

    private static attachDragBehavior(selection: CanvasSelection<CanvasPort>, context: PortRenderContext): void {
        DragUtils.attachComponentDrag(selection, {
            resolveCanvasRoot: context.canvasRootResolver,
            getCanEdit: context.getCanEdit,
            getCanSelect: context.getCanSelect,
            getDisabledIds: context.getDisabledPortIds,
            getSelectedIds: context.getSelectedIds,
            onDragEnd: context.callbacks.onDragEnd!
        });
    }

    private static removePortElements(exited: CanvasSelection<CanvasPort>): void {
        exited.remove();
    }

    public static pan(selection: CanvasSelection<CanvasPort>, context: PortRenderContext): void {
        // Simply delegate to updatePortElements which already handles
        // creating/removing details based on visibility
        PortRenderer.updatePortElements(selection, context);
    }
}
