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
import { CanvasProcessGroup } from '../../canvas.types';
import { ProcessGroupRenderContext } from '../render-context.types';
import { ConnectionRenderer } from '../connection-layer/connection-renderer';
import { VersionControlTip } from '../../../tooltips/version-control-tip/version-control-tip.component';
import { CanvasConstants } from '../../canvas.constants';

/**
 * ProcessGroupRenderer
 *
 * Responsibilities:
 * - Render process group SVG elements using D3's data join pattern
 * - Render banner with name
 * - Render statistics table (queued, in, read/write, out, tasks/time)
 * - Render component status counts (transmitting, running, stopped, invalid, disabled)
 * - Render version control status indicators
 * - Handle process group visual updates (selection, authorization)
 * - NO business logic or state management
 *
 * Pattern:
 * - Static utility class with pure rendering functions
 * - render(): Main entry point accepting ProcessGroupRenderContext
 * - appendProcessGroupElements(): Create initial SVG structure
 * - updateProcessGroupElements(): Update existing elements
 * - Uses D3's enter/update/exit pattern for efficiency
 */
export class ProcessGroupRenderer {
    /**
     * Main render method - orchestrates the D3 data join pattern
     */
    public static render(context: ProcessGroupRenderContext): void {
        const { containerSelection, processGroups, canSelect, callbacks } = context;

        // D3 data join
        const selection = containerSelection
            .selectAll<SVGGElement, CanvasProcessGroup>('g.process-group')
            .data(processGroups, (d: CanvasProcessGroup) => d.entity.id);

        // Enter: create new PG elements
        const entered: any = selection.enter();
        const appendedGroups: any = ProcessGroupRenderer.appendProcessGroupElements(entered);

        // Update existing and newly entered process groups
        const merged: any = selection.merge(appendedGroups);
        ProcessGroupRenderer.updateProcessGroupElements(merged, context);

        // Attach event listeners if selection is enabled
        // Use mousedown for selection to ensure single, deterministic event firing
        if (canSelect && callbacks) {
            if (callbacks.onClick) {
                merged.on('mousedown.selection', function (event: MouseEvent, d: CanvasProcessGroup) {
                    // Only handle left mouse button
                    if (event.button !== 0) {
                        return;
                    }

                    event.stopPropagation();
                    callbacks.onClick!(d, event);
                });
            }
            if (callbacks.onDoubleClick) {
                merged.on('dblclick', function (event: MouseEvent, d: CanvasProcessGroup) {
                    event.stopPropagation();
                    callbacks.onDoubleClick!(d, event);
                });
            }
            // Attach drag behavior for position updates if editing is allowed
            if (callbacks.onDragEnd && context.getCanEdit()) {
                ProcessGroupRenderer.attachDragBehavior(merged, context);
            }
        }

        // Exit: remove PGs that are no longer in data
        const exited: any = selection.exit();
        ProcessGroupRenderer.removeProcessGroupElements(exited);
    }

    /**
     * Append process group SVG elements (enter selection)
     */
    private static appendProcessGroupElements(
        entered: d3.Selection<any, CanvasProcessGroup, any, any>
    ): d3.Selection<any, CanvasProcessGroup, any, any> {
        // Create group for each process group
        const pgGroups = entered
            .append('g')
            .attr('id', (d) => `id-${d.entity.id}`)
            .attr('class', 'process-group component')
            .attr('transform', (d) => `translate(${d.entity.position.x}, ${d.entity.position.y})`);

        // Process group border (selection/authorization indicator)
        pgGroups
            .append('rect')
            .attr('class', 'border')
            .attr('width', (d) => d.ui.dimensions.width)
            .attr('height', (d) => d.ui.dimensions.height)
            .attr('fill', 'transparent')
            .attr('stroke', 'transparent');

        // Process group body
        pgGroups
            .append('rect')
            .attr('class', 'body')
            .attr('width', (d) => d.ui.dimensions.width)
            .attr('height', (d) => d.ui.dimensions.height)
            .attr('filter', 'url(#component-drop-shadow)')
            .attr('stroke-width', 0);

        // Process group banner (name background)
        pgGroups
            .append('rect')
            .attr('class', 'process-group-banner')
            .attr('width', (d) => d.ui.dimensions.width)
            .attr('height', CanvasConstants.PROCESS_GROUP_BANNER_HEIGHT);

        // Process group name
        pgGroups
            .append('text')
            .attr('class', 'process-group-name secondary-contrast')
            .attr('x', 10)
            .attr('y', 21)
            .attr('width', 300)
            .attr('height', 16)
            .text((d) => d.entity.component?.name || d.entity.id);

        // Version control container (top-right corner)
        const versionControlContainer = pgGroups.append('g').attr('class', 'version-control-container');

        versionControlContainer
            .append('rect')
            .attr('class', 'version-control-background-fill')
            .attr('x', 0)
            .attr('y', 0)
            .attr('width', 32)
            .attr('height', 32);

        versionControlContainer
            .append('rect')
            .attr('class', 'version-control-background')
            .attr('x', 0)
            .attr('y', 0)
            .attr('width', 32)
            .attr('height', 32);

        versionControlContainer.append('text').attr('class', 'version-control').attr('x', 10).attr('y', 22);

        return pgGroups;
    }

    /**
     * Append process group details (statistics and component counts)
     * This is called conditionally when the process group is visible
     */
    private static appendProcessGroupDetails(
        processGroup: d3.Selection<any, CanvasProcessGroup, any, any>,
        processGroupData: CanvasProcessGroup
    ): d3.Selection<any, CanvasProcessGroup, any, any> {
        const details = processGroup.append('g').attr('class', 'process-group-details');
        const width = processGroupData.ui.dimensions.width;
        const height = processGroupData.ui.dimensions.height;

        // Top contents banner (component status counts)
        details
            .append('rect')
            .attr('class', 'process-group-details-banner banner')
            .attr('x', 0)
            .attr('y', CanvasConstants.PROCESS_GROUP_BANNER_HEIGHT)
            .attr('width', width)
            .attr('height', CanvasConstants.PROCESS_GROUP_CONTENTS_BANNER_HEIGHT);

        // Component status icons and counts (top banner)
        const topBannerY = CanvasConstants.PROCESS_GROUP_BANNER_HEIGHT + 17; // 32 + 17 = 49

        // Transmitting RPG icon
        details
            .append('text')
            .attr('class', 'process-group-transmitting process-group-contents-icon secondary-color')
            .attr('x', 10)
            .attr('y', topBannerY)
            .attr('font-family', 'FontAwesome')
            .text('\uf140');

        // Transmitting count
        details
            .append('text')
            .attr('class', 'process-group-transmitting-count process-group-contents-count')
            .attr('y', topBannerY);

        // Not transmitting RPG icon
        details
            .append('text')
            .attr('class', 'process-group-not-transmitting process-group-contents-icon secondary-color')
            .attr('y', topBannerY)
            .attr('font-family', 'flowfont')
            .text('\ue80a');

        // Not transmitting count
        details
            .append('text')
            .attr('class', 'process-group-not-transmitting-count process-group-contents-count')
            .attr('y', topBannerY);

        // Running icon
        details
            .append('text')
            .attr('class', 'process-group-running process-group-contents-icon secondary-color')
            .attr('y', topBannerY)
            .attr('font-family', 'FontAwesome')
            .text('\uf04b');

        // Running count
        details
            .append('text')
            .attr('class', 'process-group-running-count process-group-contents-count')
            .attr('y', topBannerY);

        // Stopped icon
        details
            .append('text')
            .attr('class', 'process-group-stopped process-group-contents-icon secondary-color')
            .attr('y', topBannerY)
            .attr('font-family', 'FontAwesome')
            .text('\uf04d');

        // Stopped count
        details
            .append('text')
            .attr('class', 'process-group-stopped-count process-group-contents-count')
            .attr('y', topBannerY);

        // Invalid icon
        details
            .append('text')
            .attr('class', 'process-group-invalid process-group-contents-icon secondary-color')
            .attr('y', topBannerY)
            .attr('font-family', 'FontAwesome')
            .text('\uf071');

        // Invalid count
        details
            .append('text')
            .attr('class', 'process-group-invalid-count process-group-contents-count')
            .attr('y', topBannerY);

        // Disabled icon
        details
            .append('text')
            .attr('class', 'process-group-disabled process-group-contents-icon secondary-color')
            .attr('y', topBannerY)
            .attr('font-family', 'flowfont')
            .text('\ue802');

        // Disabled count
        details
            .append('text')
            .attr('class', 'process-group-disabled-count process-group-contents-count')
            .attr('y', topBannerY);

        // Bottom contents banner (version control status)
        const bottomBannerY = height - CanvasConstants.PROCESS_GROUP_CONTENTS_BANNER_HEIGHT;
        details
            .append('rect')
            .attr('class', 'process-group-details-banner banner')
            .attr('x', 0)
            .attr('y', bottomBannerY)
            .attr('width', width)
            .attr('height', CanvasConstants.PROCESS_GROUP_CONTENTS_BANNER_HEIGHT);

        // Version control status icons and counts (bottom banner)
        const bottomBannerTextY = height - 7;

        // Up to date icon
        details
            .append('text')
            .attr('class', 'process-group-up-to-date process-group-contents-icon secondary-color')
            .attr('x', 10)
            .attr('y', bottomBannerTextY)
            .attr('font-family', 'FontAwesome')
            .text('\uf00c');

        // Up to date count
        details
            .append('text')
            .attr('class', 'process-group-up-to-date-count process-group-contents-count')
            .attr('y', bottomBannerTextY);

        // Locally modified icon
        details
            .append('text')
            .attr('class', 'process-group-locally-modified process-group-contents-icon secondary-color')
            .attr('y', bottomBannerTextY)
            .attr('font-family', 'FontAwesome')
            .text('\uf069');

        // Locally modified count
        details
            .append('text')
            .attr('class', 'process-group-locally-modified-count process-group-contents-count')
            .attr('y', bottomBannerTextY);

        // Stale icon
        details
            .append('text')
            .attr('class', 'process-group-stale process-group-contents-icon secondary-color')
            .attr('y', bottomBannerTextY)
            .attr('font-family', 'FontAwesome')
            .text('\uf0aa');

        // Stale count
        details
            .append('text')
            .attr('class', 'process-group-stale-count process-group-contents-count')
            .attr('y', bottomBannerTextY);

        // Locally modified and stale icon
        details
            .append('text')
            .attr('class', 'process-group-locally-modified-and-stale process-group-contents-icon secondary-color')
            .attr('y', bottomBannerTextY)
            .attr('font-family', 'FontAwesome')
            .text('\uf06a');

        // Locally modified and stale count
        details
            .append('text')
            .attr('class', 'process-group-locally-modified-and-stale-count process-group-contents-count')
            .attr('y', bottomBannerTextY);

        // Sync failure icon
        details
            .append('text')
            .attr('class', 'process-group-sync-failure process-group-contents-icon secondary-color')
            .attr('y', bottomBannerTextY)
            .attr('font-family', 'FontAwesome')
            .text('\uf128');

        // Sync failure count
        details
            .append('text')
            .attr('class', 'process-group-sync-failure-count process-group-contents-count')
            .attr('y', bottomBannerTextY);

        // Statistics table background
        const statsStartY =
            CanvasConstants.PROCESS_GROUP_BANNER_HEIGHT + CanvasConstants.PROCESS_GROUP_CONTENTS_BANNER_HEIGHT + 10; // 32 + 24 + 10 = 66

        // Queued row
        details
            .append('rect')
            .attr('class', 'process-group-queued-stats odd')
            .attr('x', 0)
            .attr('y', statsStartY)
            .attr('width', width)
            .attr('height', CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT);

        details
            .append('rect')
            .attr('class', 'process-group-stats-border')
            .attr('x', 0)
            .attr('y', statsStartY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT - 1)
            .attr('width', width)
            .attr('height', 1);

        // In row
        details
            .append('rect')
            .attr('class', 'process-group-stats-in-out even')
            .attr('x', 0)
            .attr('y', statsStartY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT)
            .attr('width', width)
            .attr('height', CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT);

        details
            .append('rect')
            .attr('class', 'process-group-stats-border')
            .attr('x', 0)
            .attr('y', statsStartY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT * 2 - 1)
            .attr('width', width)
            .attr('height', 1);

        // Read/Write row
        details
            .append('rect')
            .attr('class', 'process-group-read-write-stats odd')
            .attr('x', 0)
            .attr('y', statsStartY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT * 2)
            .attr('width', width)
            .attr('height', CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT);

        details
            .append('rect')
            .attr('class', 'process-group-stats-border')
            .attr('x', 0)
            .attr('y', statsStartY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT * 3 - 1)
            .attr('width', width)
            .attr('height', 1);

        // Out row
        details
            .append('rect')
            .attr('class', 'process-group-stats-in-out even')
            .attr('x', 0)
            .attr('y', statsStartY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT * 3)
            .attr('width', width)
            .attr('height', CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT);

        details
            .append('rect')
            .attr('class', 'process-group-stats-border')
            .attr('x', 0)
            .attr('y', statsStartY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT * 4 - 1)
            .attr('width', width)
            .attr('height', 1);

        // Statistics text elements
        const statsTextY = statsStartY + 14;
        const statsLabelX = 10;
        const statsValueX = 95;

        // Queued label
        details.append('text').attr('class', 'stats-label').attr('x', statsLabelX).attr('y', statsTextY).text('Queued');

        // Queued value
        const queuedText = details
            .append('text')
            .attr('class', 'process-group-queued stats-value')
            .attr('x', statsValueX)
            .attr('y', statsTextY);

        // queued count
        queuedText.append('tspan').attr('class', 'count');

        // queued size
        queuedText.append('tspan').attr('class', 'size');

        // In label
        details
            .append('text')
            .attr('class', 'stats-label')
            .attr('x', statsLabelX)
            .attr('y', statsTextY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT)
            .text('In');

        // In value (includes port count)
        const inText = details
            .append('text')
            .attr('class', 'process-group-in stats-value')
            .attr('x', statsValueX)
            .attr('y', statsTextY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT);

        // in count
        inText.append('tspan').attr('class', 'count');

        // in size
        inText.append('tspan').attr('class', 'size');

        // in ports
        inText.append('tspan').attr('class', 'ports');

        // In info (5 min)
        details
            .append('text')
            .attr('class', 'stats-info')
            .attr('x', width - 45)
            .attr('y', statsTextY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT)
            .text('5 min');

        // Read/write label (combined)
        details
            .append('text')
            .attr('class', 'stats-label')
            .attr('x', statsLabelX)
            .attr('y', statsTextY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT * 2)
            .text('Read/write');

        // Read/write value (combined) - no tspans needed, just plain text
        details
            .append('text')
            .attr('class', 'process-group-read-write stats-value')
            .attr('x', statsValueX)
            .attr('y', statsTextY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT * 2);

        // Read/write info (5 min)
        details
            .append('text')
            .attr('class', 'stats-info')
            .attr('x', width - 45)
            .attr('y', statsTextY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT * 2)
            .text('5 min');

        // Out label
        details
            .append('text')
            .attr('class', 'stats-label')
            .attr('x', statsLabelX)
            .attr('y', statsTextY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT * 3)
            .text('Out');

        // Out value (includes port count)
        const outText = details
            .append('text')
            .attr('class', 'process-group-out stats-value')
            .attr('x', statsValueX)
            .attr('y', statsTextY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT * 3);

        // out ports
        outText.append('tspan').attr('class', 'ports');

        // out count
        outText.append('tspan').attr('class', 'count');

        // out size
        outText.append('tspan').attr('class', 'size');

        // Out info (5 min)
        details
            .append('text')
            .attr('class', 'stats-info')
            .attr('x', width - 45)
            .attr('y', statsTextY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT * 3)
            .text('5 min');

        // -------------------
        // active thread count
        // -------------------

        // active thread count icon
        details.append('text').attr('class', 'active-thread-count-icon').attr('y', 21).text('\ue83f');

        // active thread count text
        details.append('text').attr('class', 'active-thread-count').attr('y', 21);

        // ---------
        // bulletins
        // ---------

        // bulletin background
        details
            .append('rect')
            .attr('class', 'bulletin-background')
            .attr('x', width - 24)
            .attr('y', 32)
            .attr('width', 24)
            .attr('height', 24)
            .style('visibility', 'hidden'); // Initially hidden

        // bulletin icon
        details
            .append('text')
            .attr('class', 'bulletin-icon')
            .attr('x', width - 17)
            .attr('y', 49)
            .style('visibility', 'hidden') // Initially hidden
            .text('\uf24a'); // fa-bell

        // Comments icon (bottom-right corner)
        details
            .append('text')
            .attr('class', 'component-comments')
            .attr('transform', `translate(${width - 11}, ${height - 3})`)
            .text('\uf075');

        return details;
    }

    /**
     * Update process group SVG elements (update + enter selection)
     */
    private static updateProcessGroupElements(
        selection: d3.Selection<any, CanvasProcessGroup, any, any>,
        context: ProcessGroupRenderContext
    ): void {
        // Update transform for position (use currentPosition during drag, otherwise entity position)
        selection.attr('transform', (d) => {
            const position = d.ui.currentPosition || d.entity.position;
            return `translate(${position.x}, ${position.y})`;
        });

        // Apply disabled state (during save operation)
        selection
            .classed('disabled', (d) => context.disabledProcessGroupIds?.has(d.entity.id) || false)
            .style('opacity', (d) => (context.disabledProcessGroupIds?.has(d.entity.id) ? 0.6 : null))
            .style('cursor', (d) => {
                if (context.disabledProcessGroupIds?.has(d.entity.id)) return 'not-allowed';
                return context.canSelect ? 'pointer' : 'default';
            });

        // Update border
        selection.select('rect.border').classed('unauthorized', (d) => d.entity.permissions.canRead === false);

        // Update body for authorization
        selection.select('rect.body').classed('unauthorized', (d) => d.entity.permissions.canRead === false);

        // Version control container stays at (0, 0) - top-left corner
        // No transform needed as it's already positioned correctly

        // Helper function to check if process group is under version control
        const isUnderVersionControl = (d: CanvasProcessGroup) => {
            return d.entity.versionedFlowState != null && d.entity.versionedFlowState !== '';
        };

        // Update version control status icon and background

        const versionControl = selection
            .select('text.version-control')
            .style('visibility', (d) => (isUnderVersionControl(d) ? 'visible' : 'hidden'))
            .text((d) => {
                const state = d.entity.versionedFlowState;
                if (!state) {
                    return '';
                }
                // Map version control state to icon
                if (state === 'UP_TO_DATE') return '\uf00c';
                if (state === 'LOCALLY_MODIFIED') return '\uf069';
                if (state === 'STALE') return '\uf0aa';
                if (state === 'LOCALLY_MODIFIED_AND_STALE') return '\uf06a';
                if (state === 'SYNC_FAILURE') return '\uf128';
                return '';
            });

        // Update version control background with status classes and visibility
        const versionControlBackground = selection
            .select('rect.version-control-background')
            .style('visibility', (d) => (isUnderVersionControl(d) ? 'visible' : 'hidden'))
            .attr('class', function (d) {
                const classList = ['version-control-background'];
                if (isUnderVersionControl(d)) {
                    const vciState = d.entity.versionedFlowState;
                    if (vciState === 'SYNC_FAILURE') {
                        classList.push('info');
                    } else if (vciState === 'LOCALLY_MODIFIED_AND_STALE') {
                        classList.push('critical');
                    } else if (vciState === 'STALE') {
                        classList.push('critical');
                    } else if (vciState === 'LOCALLY_MODIFIED') {
                        classList.push('info');
                    } else if (vciState === 'UP_TO_DATE') {
                        classList.push('success');
                    }
                }
                return classList.join(' ');
            });

        // Center the version control icon horizontally within the background
        versionControl.attr('x', (d, i, nodes) => {
            const textLength = (nodes[i] as SVGTextElement).getComputedTextLength();
            const versionControlBackgroundWidth = parseInt(versionControlBackground.attr('width'), 10);
            return Math.round(versionControlBackgroundWidth / 2 - textLength / 2);
        });

        // Update version control icon text with status classes
        selection.select('text.version-control').attr('class', function (d) {
            const classList = ['version-control'];
            if (isUnderVersionControl(d)) {
                const vciState = d.entity.versionedFlowState;
                if (vciState === 'SYNC_FAILURE') {
                    classList.push('info');
                } else if (vciState === 'LOCALLY_MODIFIED_AND_STALE') {
                    classList.push('critical');
                } else if (vciState === 'STALE') {
                    classList.push('critical');
                } else if (vciState === 'LOCALLY_MODIFIED') {
                    classList.push('info');
                } else if (vciState === 'UP_TO_DATE') {
                    classList.push('success');
                }
            }
            return classList.join(' ');
        });

        // Update version control background fill visibility (inline style to override CSS)
        selection
            .select('rect.version-control-background-fill')
            .style('visibility', (d) => (isUnderVersionControl(d) ? 'visible' : 'hidden'));

        // Update version control tooltip
        selection.select('g.version-control-container').each(function (d: CanvasProcessGroup) {
            const versionControlContainer = d3.select(this);
            if (d.entity.permissions.canRead) {
                if (isUnderVersionControl(d)) {
                    context.componentUtils.canvasTooltip(VersionControlTip, versionControlContainer, {
                        versionControlInformation: d.entity.component?.versionControlInformation,
                        registryClients: context.registryClients
                    });
                } else {
                    context.componentUtils.resetCanvasTooltip(versionControlContainer);
                }
            }
        });

        // Handle details group (conditionally render based on visibility)
        selection.each(function (d: CanvasProcessGroup) {
            const processGroup = d3.select(this) as d3.Selection<any, CanvasProcessGroup, any, any>;
            let details = processGroup.select('g.process-group-details');

            // if this process group is visible, render everything
            if (processGroup.classed('visible')) {
                if (details.empty()) {
                    // Append details if they don't exist
                    details = ProcessGroupRenderer.appendProcessGroupDetails(processGroup, d);
                }

                // Update details
                if (!details.empty()) {
                    // Update statistics - data is in entity.status.aggregateSnapshot
                    // Note: Statistics are shown even for unauthorized process groups
                    const stats = d.entity.status?.aggregateSnapshot;
                    if (stats) {
                        // Queued count (number before first space)
                        details.select('text.process-group-queued tspan.count').text(() => {
                            const queued = stats.queued || '0 (0 bytes)';
                            return queued.substring(0, queued.indexOf(' '));
                        });

                        // Queued size (everything after first space)
                        details.select('text.process-group-queued tspan.size').text(() => {
                            const queued = stats.queued || '0 (0 bytes)';
                            const spaceIndex = queued.indexOf(' ');
                            return spaceIndex >= 0 ? ' ' + queued.substring(spaceIndex + 1) : '';
                        });

                        // In count (number before first space)
                        details.select('text.process-group-in tspan.count').text(() => {
                            const input = stats.input || '0 (0 bytes)';
                            return input.substring(0, input.indexOf(' '));
                        });

                        // In size (everything after first space)
                        details.select('text.process-group-in tspan.size').text(() => {
                            const input = stats.input || '0 (0 bytes)';
                            const spaceIndex = input.indexOf(' ');
                            return spaceIndex >= 0 ? ' ' + input.substring(spaceIndex + 1) : '';
                        });

                        // In ports (arrow and port count)
                        details.select('text.process-group-in tspan.ports').text(() => {
                            const inputPorts = d.entity.inputPortCount || 0;
                            return ' ' + String.fromCharCode(8594) + ' ' + inputPorts;
                        });

                        // Read/write - combined format: "read / written"
                        details.select('text.process-group-read-write').text(() => {
                            const read = stats.read || '0 bytes';
                            const written = stats.written || '0 bytes';
                            return `${read} / ${written}`;
                        });

                        // Out ports (port count first)
                        details.select('text.process-group-out tspan.ports').text(() => {
                            const outputPorts = d.entity.outputPortCount || 0;
                            return outputPorts.toString();
                        });

                        // Out count (arrow, then number before first space)
                        details.select('text.process-group-out tspan.count').text(() => {
                            const output = stats.output || '0 (0 bytes)';
                            const count = output.substring(0, output.indexOf(' '));
                            return ' ' + String.fromCharCode(8594) + ' ' + count;
                        });

                        // Out size (everything after first space)
                        details.select('text.process-group-out tspan.size').text(() => {
                            const output = stats.output || '0 (0 bytes)';
                            const spaceIndex = output.indexOf(' ');
                            return spaceIndex >= 0 ? ' ' + output.substring(spaceIndex + 1) : '';
                        });

                        // Update component status counts with dynamic positioning and colors
                        const activeRemotePortCount = d.entity.activeRemotePortCount || 0;
                        const inactiveRemotePortCount = d.entity.inactiveRemotePortCount || 0;
                        const runningCount = d.entity.runningCount || 0;
                        const stoppedCount = d.entity.stoppedCount || 0;
                        const invalidCount = d.entity.invalidCount || 0;
                        const disabledCount = d.entity.disabledCount || 0;

                        // Transmitting icon
                        const transmitting = details
                            .select('text.process-group-transmitting')
                            .classed('success-color-variant', activeRemotePortCount > 0)
                            .classed('zero', activeRemotePortCount === 0)
                            .classed('hidden', activeRemotePortCount === 0 && inactiveRemotePortCount === 0);

                        // Transmitting count
                        const transmittingCount = details
                            .select('text.process-group-transmitting-count')
                            .text(activeRemotePortCount)
                            .classed('hidden', activeRemotePortCount === 0 && inactiveRemotePortCount === 0)
                            .attr('x', function () {
                                if (activeRemotePortCount === 0 && inactiveRemotePortCount === 0) {
                                    return 0;
                                }
                                const transmittingX = parseInt(transmitting.attr('x'), 10);
                                const transmittingNode = transmitting.node() as SVGTextElement;
                                const textLength = transmittingNode.getComputedTextLength();
                                return (
                                    transmittingX +
                                    Math.round(textLength) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_VALUE_SPACER
                                );
                            });

                        // Not transmitting icon
                        const notTransmitting = details
                            .select('text.process-group-not-transmitting')
                            .classed('neutral-color', inactiveRemotePortCount > 0)
                            .classed('zero', inactiveRemotePortCount === 0)
                            .attr('x', function () {
                                if (activeRemotePortCount === 0 && inactiveRemotePortCount === 0) {
                                    return 0;
                                }
                                const transmittingCountX = parseInt(transmittingCount.attr('x'), 10);
                                const transmittingCountNode = transmittingCount.node() as SVGTextElement;
                                return (
                                    transmittingCountX +
                                    Math.round(transmittingCountNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_SPACER
                                );
                            })
                            .classed('hidden', activeRemotePortCount === 0 && inactiveRemotePortCount === 0);

                        // Not transmitting count
                        const notTransmittingCount = details
                            .select('text.process-group-not-transmitting-count')
                            .attr('x', function () {
                                if (activeRemotePortCount === 0 && inactiveRemotePortCount === 0) {
                                    return 0;
                                }
                                const notTransmittingX = parseInt(notTransmitting.attr('x'), 10);
                                const notTransmittingNode = notTransmitting.node() as SVGTextElement;
                                return (
                                    notTransmittingX +
                                    Math.round(notTransmittingNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_VALUE_SPACER
                                );
                            })
                            .text(inactiveRemotePortCount)
                            .classed('hidden', activeRemotePortCount === 0 && inactiveRemotePortCount === 0);

                        // Running icon
                        const running = details
                            .select('text.process-group-running')
                            .classed('success-color-default', runningCount > 0)
                            .classed('zero', runningCount === 0)
                            .attr('x', function () {
                                const notTransmittingCountX = parseInt(notTransmittingCount.attr('x'), 10);
                                const notTransmittingCountNode = notTransmittingCount.node() as SVGTextElement;
                                return (
                                    notTransmittingCountX +
                                    Math.round(notTransmittingCountNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_SPACER
                                );
                            });

                        // Running count
                        const runningCountEl = details
                            .select('text.process-group-running-count')
                            .attr('x', function () {
                                const runningX = parseInt(running.attr('x'), 10);
                                const runningNode = running.node() as SVGTextElement;
                                return (
                                    runningX +
                                    Math.round(runningNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_VALUE_SPACER
                                );
                            })
                            .text(runningCount);

                        // Stopped icon
                        const stopped = details
                            .select('text.process-group-stopped')
                            .classed('error-color-variant', stoppedCount > 0)
                            .classed('zero', stoppedCount === 0)
                            .attr('x', function () {
                                const runningCountX = parseInt(runningCountEl.attr('x'), 10);
                                const runningCountNode = runningCountEl.node() as SVGTextElement;
                                return (
                                    runningCountX +
                                    Math.round(runningCountNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_SPACER
                                );
                            });

                        // Stopped count
                        const stoppedCountEl = details
                            .select('text.process-group-stopped-count')
                            .attr('x', function () {
                                const stoppedX = parseInt(stopped.attr('x'), 10);
                                const stoppedNode = stopped.node() as SVGTextElement;
                                return (
                                    stoppedX +
                                    Math.round(stoppedNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_VALUE_SPACER
                                );
                            })
                            .text(stoppedCount);

                        // Invalid icon
                        const invalid = details
                            .select('text.process-group-invalid')
                            .classed('caution-color', invalidCount > 0)
                            .classed('zero', invalidCount === 0)
                            .attr('x', function () {
                                const stoppedCountX = parseInt(stoppedCountEl.attr('x'), 10);
                                const stoppedCountNode = stoppedCountEl.node() as SVGTextElement;
                                return (
                                    stoppedCountX +
                                    Math.round(stoppedCountNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_SPACER
                                );
                            });

                        // Invalid count
                        const invalidCountEl = details
                            .select('text.process-group-invalid-count')
                            .attr('x', function () {
                                const invalidX = parseInt(invalid.attr('x'), 10);
                                const invalidNode = invalid.node() as SVGTextElement;
                                return (
                                    invalidX +
                                    Math.round(invalidNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_VALUE_SPACER
                                );
                            })
                            .text(invalidCount);

                        // Disabled icon
                        details
                            .select('text.process-group-disabled')
                            .classed('neutral-color', disabledCount > 0)
                            .classed('zero', disabledCount === 0)
                            .attr('x', function () {
                                const invalidCountX = parseInt(invalidCountEl.attr('x'), 10);
                                const invalidCountNode = invalidCountEl.node() as SVGTextElement;
                                return (
                                    invalidCountX +
                                    Math.round(invalidCountNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_SPACER
                                );
                            });

                        // Disabled count
                        details
                            .select('text.process-group-disabled-count')
                            .attr('x', function () {
                                const disabled = details.select('text.process-group-disabled');
                                const disabledX = parseInt(disabled.attr('x'), 10);
                                const disabledNode = disabled.node() as SVGTextElement;
                                return (
                                    disabledX +
                                    Math.round(disabledNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_VALUE_SPACER
                                );
                            })
                            .text(disabledCount);

                        // Version control status counts (bottom banner) with dynamic positioning
                        const upToDateCount = d.entity.upToDateCount || 0;
                        const locallyModifiedCount = d.entity.locallyModifiedCount || 0;
                        const staleCount = d.entity.staleCount || 0;
                        const locallyModifiedAndStaleCount = d.entity.locallyModifiedAndStaleCount || 0;
                        const syncFailureCount = d.entity.syncFailureCount || 0;

                        // Up to date icon
                        const upToDate = details
                            .select('text.process-group-up-to-date')
                            .classed('success-color-default', upToDateCount > 0)
                            .classed('zero', upToDateCount === 0);

                        // Up to date count
                        const upToDateCountEl = details
                            .select('text.process-group-up-to-date-count')
                            .attr('x', function () {
                                const upToDateX = parseInt(upToDate.attr('x'), 10);
                                const upToDateNode = upToDate.node() as SVGTextElement;
                                return (
                                    upToDateX +
                                    Math.round(upToDateNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_VALUE_SPACER
                                );
                            })
                            .text(upToDateCount);

                        // Locally modified icon
                        const locallyModified = details
                            .select('text.process-group-locally-modified')
                            .classed('neutral-color', locallyModifiedCount > 0)
                            .classed('zero', locallyModifiedCount === 0)
                            .attr('x', function () {
                                const upToDateCountX = parseInt(upToDateCountEl.attr('x'), 10);
                                const upToDateCountNode = upToDateCountEl.node() as SVGTextElement;
                                return (
                                    upToDateCountX +
                                    Math.round(upToDateCountNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_SPACER
                                );
                            });

                        // Locally modified count
                        const locallyModifiedCountEl = details
                            .select('text.process-group-locally-modified-count')
                            .attr('x', function () {
                                const locallyModifiedX = parseInt(locallyModified.attr('x'), 10);
                                const locallyModifiedNode = locallyModified.node() as SVGTextElement;
                                return (
                                    locallyModifiedX +
                                    Math.round(locallyModifiedNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_VALUE_SPACER
                                );
                            })
                            .text(locallyModifiedCount);

                        // Stale icon
                        const staleIcon = details
                            .select('text.process-group-stale')
                            .classed('critical-color', staleCount > 0)
                            .classed('zero', staleCount === 0)
                            .attr('x', function () {
                                const locallyModifiedCountX = parseInt(locallyModifiedCountEl.attr('x'), 10);
                                const locallyModifiedCountNode = locallyModifiedCountEl.node() as SVGTextElement;
                                return (
                                    locallyModifiedCountX +
                                    Math.round(locallyModifiedCountNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_SPACER
                                );
                            });

                        // Stale count
                        const staleCountEl = details
                            .select('text.process-group-stale-count')
                            .attr('x', function () {
                                const staleX = parseInt(staleIcon.attr('x'), 10);
                                const staleNode = staleIcon.node() as SVGTextElement;
                                return (
                                    staleX +
                                    Math.round(staleNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_VALUE_SPACER
                                );
                            })
                            .text(staleCount);

                        // Locally modified and stale icon
                        const locallyModifiedAndStale = details
                            .select('text.process-group-locally-modified-and-stale')
                            .classed('critical-color', locallyModifiedAndStaleCount > 0)
                            .classed('zero', locallyModifiedAndStaleCount === 0)
                            .attr('x', function () {
                                const staleCountX = parseInt(staleCountEl.attr('x'), 10);
                                const staleCountNode = staleCountEl.node() as SVGTextElement;
                                return (
                                    staleCountX +
                                    Math.round(staleCountNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_SPACER
                                );
                            });

                        // Locally modified and stale count
                        const locallyModifiedAndStaleCountEl = details
                            .select('text.process-group-locally-modified-and-stale-count')
                            .attr('x', function () {
                                const locallyModifiedAndStaleX = parseInt(locallyModifiedAndStale.attr('x'), 10);
                                const locallyModifiedAndStaleNode = locallyModifiedAndStale.node() as SVGTextElement;
                                return (
                                    locallyModifiedAndStaleX +
                                    Math.round(locallyModifiedAndStaleNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_VALUE_SPACER
                                );
                            })
                            .text(locallyModifiedAndStaleCount);

                        // Sync failure icon
                        const syncFailure = details
                            .select('text.process-group-sync-failure')
                            .classed('info-color', syncFailureCount > 0)
                            .classed('zero', syncFailureCount === 0)
                            .attr('x', function () {
                                const locallyModifiedAndStaleCountX = parseInt(
                                    locallyModifiedAndStaleCountEl.attr('x'),
                                    10
                                );
                                const locallyModifiedAndStaleCountNode =
                                    locallyModifiedAndStaleCountEl.node() as SVGTextElement;
                                return (
                                    locallyModifiedAndStaleCountX +
                                    Math.round(locallyModifiedAndStaleCountNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_SPACER -
                                    2
                                );
                            });

                        // Sync failure count
                        details
                            .select('text.process-group-sync-failure-count')
                            .attr('x', function () {
                                const syncFailureX = parseInt(syncFailure.attr('x'), 10);
                                const syncFailureNode = syncFailure.node() as SVGTextElement;
                                return (
                                    syncFailureX +
                                    Math.round(syncFailureNode.getComputedTextLength()) +
                                    CanvasConstants.PROCESS_GROUP_CONTENTS_VALUE_SPACER
                                );
                            })
                            .text(syncFailureCount);
                    }

                    // -------------------
                    // active thread count
                    // -------------------

                    // Delegate to shared component utility for consistent active thread count rendering
                    context.componentUtils.activeThreadCount(processGroup, d);

                    // ---------
                    // bulletins
                    // ---------

                    // Delegate to shared component utility for consistent bulletin rendering
                    context.componentUtils.bulletins(processGroup, d.entity.bulletins);

                    // --------
                    // comments and name
                    // --------

                    //
                    if (d.entity.permissions.canRead) {
                        // update the process group comments
                        context.componentUtils.comments(processGroup, d.entity.component?.comments);

                        // update the process group name
                        processGroup
                            .select('text.process-group-name')
                            .attr('x', function () {
                                if (isUnderVersionControl(d)) {
                                    return 40; // Offset for version control icon
                                } else {
                                    return 10;
                                }
                            })
                            .attr('width', function (this: any) {
                                const processGroupNameX = parseInt(d3.select(this).attr('x'), 10);
                                if (isUnderVersionControl(d)) {
                                    return 300 - (processGroupNameX - 0);
                                } else {
                                    return 300 - processGroupNameX;
                                }
                            })
                            .each(function () {
                                const processGroupName = d3.select(this);

                                // reset the process group name to handle any previous state
                                processGroupName.text(null).selectAll('title').remove();

                                // apply ellipsis to the process group name as necessary
                                const name = d.entity.component?.name || d.entity.id;
                                context.textEllipsis.applyEllipsis(processGroupName, name, 'group-name');
                            })
                            .append('title')
                            .text(d.entity.component?.name || d.entity.id);
                    } else {
                        // clear the process group comments
                        context.componentUtils.comments(processGroup, null);

                        // clear the process group name
                        processGroup.select('text.process-group-name').attr('x', 10).attr('width', 316).text(null);
                    }
                }
            } else {
                // Process group is off-screen - remove details to save memory
                if (!details.empty()) {
                    details.remove();
                }
            }
        });
    }

    /**
     * Pan process groups - update entering/leaving components during zoom/pan
     * This is more efficient than full re-render - only updates components transitioning visibility
     */
    public static pan(
        selection: d3.Selection<any, CanvasProcessGroup, any, any>,
        context: ProcessGroupRenderContext
    ): void {
        // The update method will check the 'visible' class and render/remove details accordingly
        ProcessGroupRenderer.updateProcessGroupElements(selection, context);
    }

    /**
     * Attach D3 drag behavior to process groups for position updates
     * Follows the same pattern as other draggable components
     */
    private static attachDragBehavior(
        selection: d3.Selection<SVGGElement, CanvasProcessGroup, any, any>,
        context: ProcessGroupRenderContext
    ): void {
        const drag = d3
            .drag<SVGGElement, CanvasProcessGroup>()
            .filter(function (event, d) {
                // Match D3's default filter: block right-click and Ctrl+click (Mac right-click)
                if (event.ctrlKey || event.button !== 0) {
                    return false;
                }
                // Block drag if editing is disabled
                if (!context.getCanEdit()) {
                    return false;
                }
                // Block drag if PG is disabled (saving)
                if (context.disabledProcessGroupIds?.has(d.entity.id)) {
                    return false;
                }
                return true;
            })
            .clickDistance(4) // Minimum distance in pixels before drag starts (prevents accidental drags during clicks)
            .on(
                'start',
                function (
                    event: d3.D3DragEvent<SVGGElement, CanvasProcessGroup, CanvasProcessGroup>,
                    d: CanvasProcessGroup
                ) {
                    const processGroup = d3.select(this as SVGGElement);

                    if (!processGroup.classed('selected') && context.callbacks.onClick) {
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
                    event: d3.D3DragEvent<SVGGElement, CanvasProcessGroup, CanvasProcessGroup>,
                    d: CanvasProcessGroup
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
                        // For process groups, connections connect to ports within the group,
                        // so we check sourceGroupId/destinationGroupId instead of sourceId/destinationId
                        d3.selectAll('g.connection').each(function () {
                            const connectionData: any = d3.select(this).datum();

                            // Check if this connection is attached to the dragged PG
                            // Connections to/from process groups use the group ID as the source/destination group
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
                    event: d3.D3DragEvent<SVGGElement, CanvasProcessGroup, CanvasProcessGroup>,
                    d: CanvasProcessGroup
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

        // Apply drag behavior to PGs with write permissions
        // Disabled state is checked dynamically in the 'start' handler
        selection
            .filter((d: CanvasProcessGroup) => d.entity.permissions.canWrite && d.entity.permissions.canRead)
            .call(drag);
    }

    /**
     * Remove process group SVG elements (exit selection)
     */
    private static removeProcessGroupElements(exited: d3.Selection<any, any, any, any>): void {
        exited.remove();
    }
}
