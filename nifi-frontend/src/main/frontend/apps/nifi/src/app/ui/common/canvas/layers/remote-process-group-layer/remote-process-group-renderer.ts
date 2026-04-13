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
import { CanvasRemoteProcessGroup } from '../../canvas.types';
import { RemoteProcessGroupRenderContext } from '../render-context.types';
import { ValidationErrorsTip } from '../../../tooltips/validation-errors-tip/validation-errors-tip.component';
import { ConnectionRenderer } from '../connection-layer/connection-renderer';
import { CanvasConstants } from '../../canvas.constants';

export class RemoteProcessGroupRenderer {
    public static render(context: RemoteProcessGroupRenderContext): void {
        const { containerSelection, remoteProcessGroups, canSelect, callbacks } = context;

        // D3 data join
        const selection = containerSelection
            .selectAll<SVGGElement, CanvasRemoteProcessGroup>('g.remote-process-group')
            .data(remoteProcessGroups, (d: CanvasRemoteProcessGroup) => d.entity.id);

        // Enter: create new RPG elements
        const entered: any = selection.enter();
        const appendedGroups: any = RemoteProcessGroupRenderer.appendRemoteProcessGroupElements(entered);

        // Update existing and newly entered remote process groups
        const merged: any = selection.merge(appendedGroups);
        RemoteProcessGroupRenderer.updateRemoteProcessGroupElements(merged, context);

        // Attach event listeners if selection is enabled
        // Use mousedown for selection to ensure single, deterministic event firing
        if (canSelect && callbacks) {
            if (callbacks.onClick) {
                merged.on('mousedown.selection', function (event: MouseEvent, d: CanvasRemoteProcessGroup) {
                    // Only handle left mouse button
                    if (event.button !== 0) {
                        return;
                    }

                    event.stopPropagation();
                    callbacks.onClick!(d, event);
                });
            }
            if (callbacks.onDoubleClick) {
                merged.on('dblclick', function (event: MouseEvent, d: CanvasRemoteProcessGroup) {
                    event.stopPropagation();
                    callbacks.onDoubleClick!(d, event);
                });
            }
            // Attach drag behavior for position updates (filter will check canEdit)
            if (callbacks.onDragEnd) {
                RemoteProcessGroupRenderer.attachDragBehavior(merged, context);
            }
        }

        // Exit: remove RPGs that are no longer in data
        const exited: any = selection.exit();
        RemoteProcessGroupRenderer.removeRemoteProcessGroupElements(exited);
    }

    private static appendRemoteProcessGroupElements(
        entered: d3.Selection<any, CanvasRemoteProcessGroup, any, any>
    ): d3.Selection<any, CanvasRemoteProcessGroup, any, any> {
        // Create group for each remote process group
        const rpgGroups = entered
            .append('g')
            .attr('id', (d) => `id-${d.entity.id}`)
            .attr('class', 'remote-process-group component')
            .attr('transform', (d) => `translate(${d.entity.position.x}, ${d.entity.position.y})`);

        // Remote process group border (selection/authorization indicator)
        rpgGroups
            .append('rect')
            .attr('class', 'border')
            .attr('width', (d) => d.ui.dimensions.width)
            .attr('height', (d) => d.ui.dimensions.height)
            .attr('fill', 'transparent')
            .attr('stroke', 'transparent');

        // Remote process group body
        rpgGroups
            .append('rect')
            .attr('class', 'body')
            .attr('width', (d) => d.ui.dimensions.width)
            .attr('height', (d) => d.ui.dimensions.height)
            .attr('filter', 'url(#component-drop-shadow)')
            .attr('stroke-width', 0);

        // Remote process group banner (name background)
        rpgGroups
            .append('rect')
            .attr('class', 'remote-process-group-banner')
            .attr('width', (d) => d.ui.dimensions.width)
            .attr('height', CanvasConstants.REMOTE_PROCESS_GROUP_BANNER_HEIGHT);

        // Remote process group name
        rpgGroups
            .append('text')
            .attr('class', 'remote-process-group-name secondary-contrast')
            .attr('x', 40)
            .attr('y', 22)
            .attr('width', 300)
            .attr('height', 16)
            .attr('fill', '#ffffff')
            .attr('font-family', 'Arial, sans-serif')
            .attr('font-size', '14px')
            .attr('font-weight', '600')
            .text((d) => d.entity.component?.name || d.entity.id);

        return rpgGroups;
    }

    private static appendRemoteProcessGroupDetails(
        remoteProcessGroup: d3.Selection<any, CanvasRemoteProcessGroup, any, any>,
        rpgData: CanvasRemoteProcessGroup
    ): d3.Selection<any, CanvasRemoteProcessGroup, any, any> {
        const details = remoteProcessGroup.append('g').attr('class', 'remote-process-group-details');
        const width = rpgData.ui.dimensions.width;

        // Transmission status container (top-left corner)
        const transmissionStatusContainer = details
            .append('g')
            .attr('class', 'remote-process-group-transmission-status-container');

        transmissionStatusContainer
            .append('rect')
            .attr('class', 'remote-process-group-transmission-status-background-fill')
            .attr('x', 0)
            .attr('y', 0)
            .attr('width', 32)
            .attr('height', 32)
            .style('visibility', 'visible');

        transmissionStatusContainer
            .append('rect')
            .attr('class', 'remote-process-group-transmission-status-background')
            .attr('x', 0)
            .attr('y', 0)
            .attr('width', 32)
            .attr('height', 32)
            .style('visibility', 'visible');

        transmissionStatusContainer
            .append('text')
            .attr('class', 'remote-process-group-transmission-status')
            .attr('x', 10)
            .attr('y', 22);

        // Details banner (URI section)
        details
            .append('rect')
            .attr('class', 'remote-process-group-details-banner banner')
            .attr('x', 0)
            .attr('y', CanvasConstants.REMOTE_PROCESS_GROUP_BANNER_HEIGHT)
            .attr('width', width)
            .attr('height', CanvasConstants.REMOTE_PROCESS_GROUP_DETAILS_BANNER_HEIGHT);

        // Secure transfer icon
        details
            .append('text')
            .attr('class', 'remote-process-group-transmission-secure')
            .attr('x', 10)
            .attr('y', CanvasConstants.REMOTE_PROCESS_GROUP_BANNER_HEIGHT + 16); // 32 + 16 = 48

        // URI text
        details
            .append('text')
            .attr('class', 'remote-process-group-uri')
            .attr('x', 30)
            .attr('y', CanvasConstants.REMOTE_PROCESS_GROUP_BANNER_HEIGHT + 16)
            .attr('width', 305)
            .attr('height', 12);

        // Statistics table background
        const statsStartY =
            CanvasConstants.REMOTE_PROCESS_GROUP_BANNER_HEIGHT +
            CanvasConstants.REMOTE_PROCESS_GROUP_DETAILS_BANNER_HEIGHT +
            10; // 32 + 24 + 10 = 66

        // Sent row
        details
            .append('rect')
            .attr('class', 'remote-process-group-sent-stats odd')
            .attr('x', 0)
            .attr('y', statsStartY)
            .attr('width', width)
            .attr('height', CanvasConstants.REMOTE_PROCESS_GROUP_STATS_ROW_HEIGHT);

        details
            .append('rect')
            .attr('class', 'remote-process-group-stats-border')
            .attr('x', 0)
            .attr('y', statsStartY + CanvasConstants.REMOTE_PROCESS_GROUP_STATS_ROW_HEIGHT - 1)
            .attr('width', width)
            .attr('height', 1);

        // Received row
        details
            .append('rect')
            .attr('class', 'remote-process-group-received-stats even')
            .attr('x', 0)
            .attr('y', statsStartY + CanvasConstants.REMOTE_PROCESS_GROUP_STATS_ROW_HEIGHT)
            .attr('width', width)
            .attr('height', CanvasConstants.REMOTE_PROCESS_GROUP_STATS_ROW_HEIGHT);

        // Statistics labels (transform to position)
        const statsLabelContainer = details.append('g').attr('transform', 'translate(6, 75)');

        // Sent label
        statsLabelContainer
            .append('text')
            .attr('class', 'stats-label')
            .attr('x', 4)
            .attr('y', 5)
            .attr('width', 73)
            .attr('height', 10)
            .text('Sent');

        // Received label
        statsLabelContainer
            .append('text')
            .attr('class', 'stats-label')
            .attr('x', 4)
            .attr('y', 23)
            .attr('width', 73)
            .attr('height', 10)
            .text('Received');

        // Statistics values (transform to position)
        const statsValueContainer = details.append('g').attr('transform', 'translate(95, 75)');

        // Sent value with tspans
        const sentText = statsValueContainer
            .append('text')
            .attr('class', 'remote-process-group-sent stats-value')
            .attr('x', 4)
            .attr('y', 5)
            .attr('width', 180)
            .attr('height', 10);

        sentText.append('tspan').attr('class', 'count');
        sentText.append('tspan').attr('class', 'size');
        sentText.append('tspan').attr('class', 'ports');

        // Received value with tspans
        const receivedText = statsValueContainer
            .append('text')
            .attr('class', 'remote-process-group-received stats-value')
            .attr('x', 4)
            .attr('y', 23)
            .attr('width', 180)
            .attr('height', 10);

        receivedText.append('tspan').attr('class', 'ports');
        receivedText.append('tspan').attr('class', 'count');
        receivedText.append('tspan').attr('class', 'size');

        // Stats info container (5 min timestamps)
        const statsInfoContainer = details.append('g').attr('transform', 'translate(335, 75)');

        // Sent info
        statsInfoContainer
            .append('text')
            .attr('class', 'stats-info')
            .attr('x', 4)
            .attr('y', 5)
            .attr('width', 25)
            .attr('height', 10)
            .text('5 min');

        // Received info
        statsInfoContainer
            .append('text')
            .attr('class', 'stats-info')
            .attr('x', 4)
            .attr('y', 23)
            .attr('width', 25)
            .attr('height', 10)
            .text('5 min');

        // Last refresh banner (bottom)
        const lastRefreshY = rpgData.ui.dimensions.height - CanvasConstants.REMOTE_PROCESS_GROUP_DETAILS_BANNER_HEIGHT;

        details
            .append('rect')
            .attr('class', 'remote-process-group-last-refresh-rect banner')
            .attr('x', 0)
            .attr('y', lastRefreshY)
            .attr('width', width)
            .attr('height', CanvasConstants.REMOTE_PROCESS_GROUP_DETAILS_BANNER_HEIGHT);

        details
            .append('text')
            .attr('class', 'remote-process-group-last-refresh')
            .attr('x', 10)
            .attr('y', lastRefreshY + 16); // Center text vertically in 24px banner

        // Bulletins (top-right corner)
        details
            .append('rect')
            .attr('class', 'bulletin-background')
            .attr('x', width - 24)
            .attr('y', 32)
            .attr('width', 24)
            .attr('height', 24)
            .style('visibility', 'hidden'); // Initially hidden

        details
            .append('text')
            .attr('class', 'bulletin-icon')
            .attr('x', width - 17)
            .attr('y', 49)
            .style('visibility', 'hidden') // Initially hidden
            .text('\uf24a');

        // -------------------
        // active thread count
        // -------------------

        // active thread count icon
        details.append('text').attr('class', 'active-thread-count-icon').attr('y', 21).text('\ue83f');

        // active thread count text
        details.append('text').attr('class', 'active-thread-count').attr('y', 20);

        // Comments icon (bottom-right corner)
        details
            .append('text')
            .attr('class', 'component-comments')
            .attr('transform', `translate(${width - 11}, ${rpgData.ui.dimensions.height - 3})`)
            .attr('font-family', 'FontAwesome')
            .text('\uf075');

        return details;
    }

    private static updateRemoteProcessGroupElements(
        selection: d3.Selection<any, CanvasRemoteProcessGroup, any, any>,
        context: RemoteProcessGroupRenderContext
    ): void {
        // Update transform for position (use currentPosition during drag, otherwise entity position)
        selection.attr('transform', (d) => {
            const position = d.ui.currentPosition || d.entity.position;
            return `translate(${position.x}, ${position.y})`;
        });

        // Apply disabled state (during save operation)
        selection
            .classed('disabled', (d) => context.disabledRemoteProcessGroupIds?.has(d.entity.id) || false)
            .style('opacity', (d) => (context.disabledRemoteProcessGroupIds?.has(d.entity.id) ? 0.6 : null))
            .style('cursor', (d) => {
                if (context.disabledRemoteProcessGroupIds?.has(d.entity.id)) return 'not-allowed';
                return context.canSelect ? 'pointer' : 'default';
            });

        // Update border
        selection.select('rect.border').classed('unauthorized', (d) => d.entity.permissions.canRead === false);

        // Update body for authorization
        selection.select('rect.body').classed('unauthorized', (d) => d.entity.permissions.canRead === false);

        // Update each remote process group's details
        selection.each(function (d: CanvasRemoteProcessGroup) {
            const rpg = d3.select(this) as d3.Selection<any, CanvasRemoteProcessGroup, any, any>;
            let details = rpg.select('g.remote-process-group-details') as d3.Selection<
                any,
                CanvasRemoteProcessGroup,
                any,
                any
            >;

            // if this remote process group is visible, render everything
            if (rpg.classed('visible')) {
                if (details.empty()) {
                    details = RemoteProcessGroupRenderer.appendRemoteProcessGroupDetails(rpg, d);
                }

                RemoteProcessGroupRenderer.updateRemoteProcessGroupDetails(details, d, context);
            } else {
                // RPG is off-screen - remove details to save memory
                if (!details.empty()) {
                    details.remove();
                }
            }
        });
    }

    private static updateRemoteProcessGroupDetails(
        details: d3.Selection<any, CanvasRemoteProcessGroup, any, any>,
        d: CanvasRemoteProcessGroup,
        context: RemoteProcessGroupRenderContext
    ): void {
        const textEllipsis = context.textEllipsis;
        const component = d.entity.component;
        const status = d.entity.status;

        // Update transmission status icon and background
        const hasIssues = component?.validationErrors && component.validationErrors.length > 0;
        const isTransmitting = status?.transmissionStatus === 'Transmitting';

        const transmissionStatus = details
            .select('text.remote-process-group-transmission-status')
            .text(() => {
                if (hasIssues) {
                    return '\uf071'; // Warning icon
                } else if (isTransmitting) {
                    return '\uf140'; // Transmitting icon
                } else {
                    return '\ue80a'; // Not transmitting icon
                }
            })
            .attr('font-family', () => {
                if (hasIssues || isTransmitting) {
                    return 'FontAwesome';
                } else {
                    return 'flowfont';
                }
            })
            .classed('invalid caution', hasIssues)
            .classed('neutral', !hasIssues)
            .classed('transmitting', isTransmitting)
            .classed('not-transmitting', !isTransmitting);

        // Add validation error tooltip if there are validation errors or authorization issues.
        // Remote process groups combine validation errors and authorization issues into a single listing
        const needsValidationTip = d.entity.permissions.canRead && hasIssues;
        if (needsValidationTip) {
            // Combine validation errors and authorization issues
            let issues: string[] = [];
            if (component?.authorizationIssues && component.authorizationIssues.length > 0) {
                issues = issues.concat(component.authorizationIssues);
            }
            if (component?.validationErrors && component.validationErrors.length > 0) {
                issues = issues.concat(component.validationErrors);
            }

            context.componentUtils.canvasTooltip(ValidationErrorsTip, transmissionStatus, {
                isValidating: false,
                validationErrors: issues
            });
        } else {
            context.componentUtils.resetCanvasTooltip(transmissionStatus);
        }

        // Update transmission status background
        const transmissionStatusBackground = details
            .select('rect.remote-process-group-transmission-status-background')
            .classed('caution', hasIssues)
            .classed('neutral', !hasIssues);

        // Center the transmission status icon
        transmissionStatus.attr('x', (d, i, nodes) => {
            const textLength = (nodes[i] as SVGTextElement).getComputedTextLength();
            const backgroundWidth = parseInt(transmissionStatusBackground.attr('width'), 10);
            return Math.round(backgroundWidth / 2 - textLength / 2);
        });

        // Get the parent RPG selection from details (needed for name, active threads, bulletins, comments)
        const rpg = d3.select(details.node()?.parentNode as Element);

        // Update elements based on permissions
        //
        if (d.entity.permissions.canRead && component) {
            // update the remote process group uri
            const uri = component.targetUri || component.targetUris || '';
            details
                .select('text.remote-process-group-uri')
                .each(function () {
                    const rpgUri = d3.select(this);

                    // reset the remote process group URI to handle any previous state
                    rpgUri.text(null).selectAll('title').remove();

                    // apply ellipsis to the remote process group URI as necessary
                    textEllipsis.applyEllipsis(rpgUri, uri, 'rpg-uri');
                })
                .append('title')
                .text(uri);

            // update the remote process group transmission secure
            const isSecure = component.targetSecure === true;
            details
                .select('text.remote-process-group-transmission-secure')
                .text(() => {
                    if (isSecure) {
                        return '\uf023'; // Lock icon
                    } else {
                        return '\uf09c'; // Globe icon
                    }
                })
                .attr('font-family', 'FontAwesome')
                .classed('success-color', isSecure);

            // update the remote process group comments
            context.componentUtils.comments(rpg, component.comments);

            // update the last refresh time
            if (component.flowRefreshed) {
                details.select('text.remote-process-group-last-refresh').text(component.flowRefreshed);
            } else {
                details.select('text.remote-process-group-last-refresh').text('Remote flow not current');
            }

            // update the remote process group name
            rpg.select('text.remote-process-group-name')
                .each(function () {
                    const rpgName = d3.select(this);

                    // reset the remote process group name to handle any previous state
                    rpgName.text(null).selectAll('title').remove();

                    // apply ellipsis to the remote process group name as necessary
                    const name = component.name || d.entity.id;
                    textEllipsis.applyEllipsis(rpgName, name, 'rpg-name');
                })
                .append('title')
                .text(component.name || d.entity.id);
        } else {
            // clear the target uri
            details.select('text.remote-process-group-uri').text(null);

            // clear the transmission secure icon
            details.select('text.remote-process-group-transmission-secure').text(null);

            // clear the comments
            context.componentUtils.comments(rpg, null);

            // clear the last refresh
            details.select('text.remote-process-group-last-refresh').text(null);

            // clear the name
            rpg.select('text.remote-process-group-name').text(null);
        }

        // Update statistics with tspans using shared formatUtils
        // Note: Stats are shown even for unauthorized RPGs
        const aggregateSnapshot = status?.aggregateSnapshot;
        const sent = aggregateSnapshot?.sent || '0 (0 bytes)';
        const received = aggregateSnapshot?.received || '0 (0 bytes)';

        // Format sent statistics
        const formattedSent = context.formatUtils.formatQueuedStats(sent);
        details.select('text.remote-process-group-sent tspan.count').text(formattedSent.count);
        details.select('text.remote-process-group-sent tspan.size').text(formattedSent.size);

        // Sent ports (arrow and input port count)
        details.select('text.remote-process-group-sent tspan.ports').text(() => {
            const inputPortCount = d.entity.inputPortCount || 0;
            return ' ' + String.fromCharCode(8594) + ' ' + inputPortCount;
        });

        // Received ports (output port count and arrow)
        details.select('text.remote-process-group-received tspan.ports').text(() => {
            const outputPortCount = d.entity.outputPortCount || 0;
            return outputPortCount + ' ' + String.fromCharCode(8594) + ' ';
        });

        // Format received statistics
        const formattedReceived = context.formatUtils.formatQueuedStats(received);
        details.select('text.remote-process-group-received tspan.count').text(formattedReceived.count);
        details.select('text.remote-process-group-received tspan.size').text(formattedReceived.size);

        // -------------------
        // active thread count
        // -------------------

        // Delegate to shared component utility for consistent active thread count rendering
        // This is called outside permissions check to ensure it's always rendered (or hidden) regardless of read permissions
        context.componentUtils.activeThreadCount(rpg, d);

        // ---------
        // bulletins
        // ---------

        // Update bulletins
        // This is called outside permissions check to ensure bulletins are shown regardless of component read permissions
        // (only bulletin.canRead matters, not component.permissions.canRead)
        context.componentUtils.bulletins(rpg, d.entity.bulletins);
    }

    public static pan(
        selection: d3.Selection<any, CanvasRemoteProcessGroup, any, any>,
        context: RemoteProcessGroupRenderContext
    ): void {
        // Update only the entering/leaving RPGs
        RemoteProcessGroupRenderer.updateRemoteProcessGroupElements(selection, context);
    }

    private static attachDragBehavior(
        selection: d3.Selection<SVGGElement, CanvasRemoteProcessGroup, any, any>,
        context: RemoteProcessGroupRenderContext
    ): void {
        const drag = d3
            .drag<SVGGElement, CanvasRemoteProcessGroup>()
            .filter(function (event, d) {
                // Match D3's default filter: block right-click and Ctrl+click (Mac right-click)
                if (event.ctrlKey || event.button !== 0) {
                    return false;
                }
                // Block drag if editing is disabled
                if (!context.getCanEdit()) {
                    return false;
                }
                // Block drag if RPG is disabled (saving)
                if (context.disabledRemoteProcessGroupIds?.has(d.entity.id)) {
                    return false;
                }
                return true;
            })
            .clickDistance(4) // Minimum distance in pixels before drag starts (prevents accidental drags during clicks)
            .on(
                'start',
                function (
                    event: d3.D3DragEvent<SVGGElement, CanvasRemoteProcessGroup, CanvasRemoteProcessGroup>,
                    d: CanvasRemoteProcessGroup
                ) {
                    const rpgGroup = d3.select(this as SVGGElement);

                    if (!rpgGroup.classed('selected') && context.callbacks.onClick) {
                        context.callbacks.onClick(d, event.sourceEvent as MouseEvent);
                    }

                    // Stop propagation to prevent canvas pan
                    event.sourceEvent.stopPropagation();

                    // Store original position for potential revert
                    d.ui.dragStartPosition = { ...d.entity.position };
                    // Initialize current position in UI state
                    d.ui.currentPosition = { ...d.entity.position };
                }
            )
            .on(
                'drag',
                function (
                    event: d3.D3DragEvent<SVGGElement, CanvasRemoteProcessGroup, CanvasRemoteProcessGroup>,
                    d: CanvasRemoteProcessGroup
                ) {
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
                        // For remote process groups, connections connect to ports within the group,
                        // so we check sourceGroupId/destinationGroupId instead of sourceId/destinationId
                        d3.selectAll('g.connection').each(function () {
                            const connectionData: any = d3.select(this).datum();

                            // Check if this connection is attached to the dragged RPG
                            // Connections to/from remote process groups use the group ID as the source/destination group
                            if (
                                connectionData?.entity?.sourceGroupId === d.entity.id ||
                                connectionData?.entity?.destinationGroupId === d.entity.id
                            ) {
                                const connectionGroup = d3.select(this);

                                // Recalculate path using ConnectionRenderer
                                const newPath = ConnectionRenderer.calculatePath(connectionData);

                                // Update all path elements with the new path
                                connectionGroup.selectAll('path').attr('d', newPath);

                                // Update connection label position
                                // getLabelPosition uses ui.start and ui.end which were updated by calculatePath
                                const labelPosition = ConnectionRenderer.getLabelPosition(connectionData);
                                connectionGroup
                                    .select('g.connection-label-container')
                                    .attr('transform', `translate(${labelPosition.x}, ${labelPosition.y})`);
                            }
                        });
                    }
                }
            )
            .on(
                'end',
                function (
                    event: d3.D3DragEvent<SVGGElement, CanvasRemoteProcessGroup, CanvasRemoteProcessGroup>,
                    d: CanvasRemoteProcessGroup
                ) {
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
                }
            );

        // Remove drag behavior to prevent stale closures
        selection.on('.drag', null);

        // Apply drag behavior to RPGs with write permissions
        // Disabled state is checked dynamically in the 'start' handler
        selection
            .filter((d: CanvasRemoteProcessGroup) => d.entity.permissions.canWrite && d.entity.permissions.canRead)
            .call(drag);
    }

    private static removeRemoteProcessGroupElements(exited: d3.Selection<any, any, any, any>): void {
        exited.remove();
    }
}
