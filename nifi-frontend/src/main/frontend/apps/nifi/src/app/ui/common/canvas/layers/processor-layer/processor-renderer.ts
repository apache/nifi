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
import { CanvasProcessor } from '../../canvas.types';
import { ProcessorRenderContext } from '../render-context.types';
import { ConnectionRenderer } from '../connection-layer/connection-renderer';
import { ValidationErrorsTip } from '../../../tooltips/validation-errors-tip/validation-errors-tip.component';
import { CanvasConstants } from '../../canvas.constants';

/**
 * ProcessorRenderer
 *
 * Pure D3 rendering logic for processors.
 * All methods are static and pure - they accept a RenderContext and perform rendering.
 *
 * Features:
 * - Border and body rectangles
 * - Icon container and icon
 * - Processor name, type, and bundle
 * - Run status icon (running/stopped/invalid/disabled)
 * - Statistics display (In/Read-Write/Out/Tasks-Time rows)
 * - Bulletin/alert badges (error/warning indicators)
 * - Active thread count
 * - Comments indicator
 * - Restricted/unauthorized styling
 */
export class ProcessorRenderer {
    /**
     * Get run status icon character based on processor runStatus
     */
    private static getRunStatusIcon(runStatus: string): string {
        if (runStatus === 'Disabled') {
            return '\ue802'; // flowfont disabled icon
        } else if (runStatus === 'Validating') {
            return '\uf1ce'; // fa-spinner (with fa-spin)
        } else if (runStatus === 'Invalid') {
            return '\uf071'; // fa-warning
        } else if (runStatus === 'Running') {
            return '\uf04b'; // fa-play
        } else if (runStatus === 'Stopped') {
            return '\uf04d'; // fa-stop
        }
        return '';
    }

    /**
     * Get run status CSS class for styling
     */
    private static getRunStatusClass(runStatus: string): string {
        if (runStatus === 'Validating') {
            return 'validating neutral-color';
        } else if (runStatus === 'Invalid') {
            return 'invalid caution-color';
        } else if (runStatus === 'Running') {
            return 'running success-color-default';
        } else if (runStatus === 'Stopped') {
            return 'stopped error-color-variant';
        }
        return 'primary-color';
    }

    /**
     * Get font family for run status icon
     */
    private static getRunStatusFontFamily(runStatus: string): string {
        return runStatus === 'Disabled' ? 'flowfont' : 'FontAwesome';
    }

    /**
     * Check if run status icon should spin
     */
    private static shouldSpinRunStatus(runStatus: string): boolean {
        return runStatus === 'Validating';
    }

    /**
     * Check if processor is a preview extension
     */
    private static isPreviewProcessor(processor: CanvasProcessor, previewExtensions: any[]): boolean {
        if (!processor.entity.permissions.canRead) {
            return false;
        }

        const component = processor.entity.component;
        if (!component || !component.type || !component.bundle) {
            return false;
        }

        return previewExtensions.some(
            (type: any) =>
                type.type === component.type &&
                type.bundle.group === component.bundle.group &&
                type.bundle.artifact === component.bundle.artifact &&
                type.bundle.version === component.bundle.version
        );
    }

    /**
     * Render processors using D3 data join pattern
     *
     * @param context - Complete render context with all necessary data
     */
    public static render(context: ProcessorRenderContext): void {
        // D3 Data Join: Bind data to elements
        const selection = context.containerSelection
            .selectAll<SVGGElement, CanvasProcessor>('g.processor')
            .data(context.processors, (d: CanvasProcessor) => d.entity.id);

        // ENTER: Create new processor elements
        const entered = selection
            .enter()
            .append('g')
            .attr('id', (d) => 'id-' + d.entity.id)
            .attr('class', 'processor component');

        ProcessorRenderer.appendProcessorElements(entered);

        // Attach event handlers only to newly entered processors (not on every render)
        ProcessorRenderer.attachEventHandlers(entered, context);

        // UPDATE: Merge enter and update selections
        const updated = entered.merge(selection);

        ProcessorRenderer.updateProcessorElements(updated, context);

        // EXIT: Remove processors that are no longer in data
        selection.exit().remove();
    }

    /**
     * ENTER phase: Append SVG elements for new processors
     */
    private static appendProcessorElements(entered: d3.Selection<SVGGElement, CanvasProcessor, any, any>): void {
        // Border (for selection highlight)
        entered.append('rect').attr('class', 'border').attr('fill', 'transparent').attr('stroke', 'transparent');

        // Body (main processor rectangle)
        entered
            .append('rect')
            .attr('class', 'body')
            .attr('filter', 'url(#component-drop-shadow)')
            .attr('stroke-width', 0);

        // Icon container
        entered
            .append('rect')
            .attr('x', 0)
            .attr('y', 0)
            .attr('width', 50)
            .attr('height', 50)
            .attr('class', 'processor-icon-container');

        // Icon (processor icon font character)
        entered.append('text').attr('x', 9).attr('y', 35).attr('class', 'processor-icon tertiary-color').text('\ue807');

        // Restricted icon background
        entered.append('circle').attr('r', 9).attr('cx', 12).attr('cy', 12).attr('class', 'restricted-background');

        // Restricted icon
        entered.append('text').attr('x', 7.75).attr('y', 17).attr('class', 'restricted').text('\uf132');

        // Is primary icon background
        entered.append('circle').attr('r', 9).attr('cx', 38).attr('cy', 36).attr('class', 'is-primary-background');

        // Is primary icon
        entered
            .append('text')
            .attr('x', 34.75)
            .attr('y', 40)
            .attr('class', 'is-primary')
            .text('P')
            .append('title')
            .text('This component is only scheduled to execute on the Primary Node');

        // Preview badge (background rectangle)
        entered
            .append('rect')
            .attr('x', 75)
            .attr('y', 5)
            .attr('rx', 2)
            .attr('ry', 2)
            .attr('width', 47)
            .attr('height', 16)
            .attr('class', 'processor-preview-badge')
            .style('visibility', 'hidden');

        // Preview badge text
        entered
            .append('text')
            .attr('x', 79)
            .attr('y', 16.5)
            .attr('width', 55)
            .attr('height', 14)
            .attr('class', 'processor-preview-text')
            .text('Preview')
            .style('visibility', 'hidden');

        // Processor name (appended directly to processor group, NOT in details)
        entered
            .append('text')
            .attr('x', 75)
            .attr('y', 18)
            .attr('width', 230)
            .attr('height', 14)
            .attr('class', 'processor-name');

        // Note: Details are NOT created during ENTER phase
        // They are created dynamically during UPDATE phase based on visibility
    }

    /**
     * Append bulletin/alert elements
     */
    private static appendBulletinElements(details: d3.Selection<SVGGElement, CanvasProcessor, any, any>): void {
        // Bulletin background
        details
            .append('rect')
            .attr('class', 'bulletin-background')
            .attr('x', (d) => d.ui.dimensions.width - 24)
            .attr('y', 0)
            .attr('width', 24)
            .attr('height', 24)
            .style('visibility', 'hidden'); // Initially hidden

        // Bulletin icon
        details
            .append('text')
            .attr('class', 'bulletin-icon')
            .attr('x', (d) => d.ui.dimensions.width - 17)
            .attr('y', 17)
            .style('visibility', 'hidden') // Initially hidden
            .text('\uf24a'); // fa-bell
    }

    /**
     * Append active thread count elements
     */
    private static appendActiveThreadElements(details: d3.Selection<SVGGElement, CanvasProcessor, any, any>): void {
        // Active thread count icon
        details.append('text').attr('class', 'active-thread-count-icon').attr('y', 46).text('\ue83f');

        // Active thread count value
        details.append('text').attr('class', 'active-thread-count').attr('y', 46);
    }

    /**
     * Append comment icon element
     */
    private static appendCommentElements(details: d3.Selection<SVGGElement, CanvasProcessor, any, any>): void {
        // Comment icon (positioned at bottom-right corner)
        details
            .append('text')
            .attr('class', 'component-comments')
            .attr('transform', (d) => `translate(${d.ui.dimensions.width - 11}, ${d.ui.dimensions.height - 3})`)
            .text('\uf075'); // fa-comment
    }

    /**
     * UPDATE phase: Update processor elements
     */
    private static updateProcessorElements(
        updated: d3.Selection<SVGGElement, CanvasProcessor, any, any>,
        context: ProcessorRenderContext
    ): void {
        // Position processors (always update position, even for off-screen components)
        // Use ui.currentPosition if set (during drag/save), otherwise use entity.position
        updated.attr('transform', (d) => {
            const position = d.ui.currentPosition || d.entity.position;
            return `translate(${position.x}, ${position.y})`;
        });

        // Apply disabled state (during save operation)
        updated
            .classed('disabled', (d) => context.disabledProcessorIds?.has(d.entity.id) || false)
            .style('opacity', (d) => (context.disabledProcessorIds?.has(d.entity.id) ? 0.6 : null))
            .style('cursor', (d) => {
                if (context.disabledProcessorIds?.has(d.entity.id)) return 'not-allowed';
                return context.canSelect ? 'pointer' : 'default';
            });

        // Update border (always visible)
        updated
            .select('rect.border')
            .attr('width', (d) => d.ui.dimensions.width)
            .attr('height', (d) => d.ui.dimensions.height)
            .classed('unauthorized', (d) => !d.entity.permissions.canRead);

        // Update body (always visible)
        updated
            .select('rect.body')
            .attr('width', (d) => d.ui.dimensions.width)
            .attr('height', (d) => d.ui.dimensions.height)
            .attr('opacity', 1)
            .classed('unauthorized', (d) => !d.entity.permissions.canRead);

        // Update processor icon container and icon color based on custom style (always visible)
        updated.each(function (d: CanvasProcessor) {
            const processor = d3.select(this as SVGGElement);
            const iconContainer = processor.select('rect.processor-icon-container');
            const icon = processor.select('text.processor-icon');
            const border = processor.select('rect.border');

            // Update icon container unauthorized state
            iconContainer.classed('unauthorized', !d.entity.permissions.canRead);

            // Update processor icon unauthorized state (adds both 'unauthorized' and 'tertiary-color' classes)
            icon.classed('unauthorized tertiary-color', !d.entity.permissions.canRead);

            // Apply custom color if specified and user has read permissions
            if (d.entity.permissions.canRead) {
                const bgColor = d.entity.component?.style?.['background-color'];

                if (bgColor) {
                    // Set icon container fill to custom color
                    iconContainer.style('fill', bgColor);

                    // Set border stroke to custom color
                    border.style('stroke', bgColor);

                    // Remove tertiary-color class and apply contrast color to icon
                    icon.attr('class', 'processor-icon');

                    // Calculate contrast color (black or white) based on background
                    // Uses shared utility method from TextEllipsisUtils
                    const contrastColor = context.textEllipsis.determineContrastColor(bgColor);
                    processor.style('fill', contrastColor);
                } else {
                    // Reset to default styling
                    icon.attr('class', 'processor-icon tertiary-color');
                    iconContainer.style('fill', null);
                    border.style('stroke', null);
                    processor.style('fill', null);
                }
            }
        });

        // Update processors - only render details for visible components (viewport culling)
        updated.each(function (d: CanvasProcessor) {
            const processor = d3.select<SVGGElement, CanvasProcessor>(this as SVGGElement);
            const details = processor.select('g.processor-canvas-details');

            // if this processor is visible, render everything
            if (processor.classed('visible')) {
                if (details.empty()) {
                    ProcessorRenderer.appendProcessorDetails(processor, d);
                }

                // Update all detail elements
                ProcessorRenderer.updateProcessorDetails(processor, d, context);
            } else {
                // Processor is off-screen - remove details to save memory
                details.remove();

                // When not visible, reset processor name position and hide preview badge
                if (d.entity.permissions.canRead) {
                    // Reset processor name to default position (no preview badge offset)
                    processor
                        .select('text.processor-name')
                        .attr('x', 75)
                        .attr('width', 230)
                        .text((d2: CanvasProcessor) => {
                            const name = d2.entity.component?.name || '';
                            const PREVIEW_NAME_LENGTH = 25;
                            if (name.length > PREVIEW_NAME_LENGTH) {
                                return name.substring(0, PREVIEW_NAME_LENGTH) + String.fromCharCode(8230);
                            } else {
                                return name;
                            }
                        });
                } else {
                    // Clear processor name if not readable
                    processor.select('text.processor-name').text(null);
                }

                // Hide preview badge when not visible
                processor.select('rect.processor-preview-badge').style('visibility', 'hidden');
                processor.select('text.processor-preview-text').style('visibility', 'hidden');
            }
        });
    }

    /**
     * Pan method for efficient rendering during zoom/pan operations.
     * Called by canvas.component.ts for entering/leaving processors only.
     * Simply delegates to updateProcessorElements which handles all updates
     * including creating/removing details based on visibility.
     * Matches D3 lifecycle pattern used by other renderers.
     *
     * @param selection - D3 selection of processors to update (typically entering/leaving)
     * @param context - Complete render context with all necessary data
     */
    public static pan(selection: d3.Selection<any, any, any, any>, context: ProcessorRenderContext): void {
        // Simply delegate to updateProcessorElements which handles all updates
        // including creating/removing details based on the 'visible' class
        ProcessorRenderer.updateProcessorElements(selection, context);
    }

    /**
     * Helper to append processor details (called when processor becomes visible)
     */
    private static appendProcessorDetails(
        processor: d3.Selection<SVGGElement, CanvasProcessor, any, any>,
        d: CanvasProcessor
    ): void {
        const details = processor.append('g').attr('class', 'processor-canvas-details');

        // Run status icon
        details
            .append('text')
            .attr('class', 'run-status-icon')
            .attr('x', 55)
            .attr('y', 23)
            .attr('width', 14)
            .attr('height', 14);

        // Processor type
        details
            .append('text')
            .attr('class', 'processor-type')
            .attr('x', 75)
            .attr('y', 32)
            .attr('width', 230)
            .attr('height', 12);

        // Processor bundle
        details
            .append('text')
            .attr('class', 'processor-bundle')
            .attr('x', 75)
            .attr('y', 45)
            .attr('width', 200)
            .attr('height', 12);

        // -----
        // stats
        // -----

        // draw the processor statistics table

        // in
        details
            .append('rect')
            .attr('class', 'processor-stats-in-out odd')
            .attr('width', d.ui.dimensions.width)
            .attr('height', 19)
            .attr('x', 0)
            .attr('y', 50);

        // border
        details
            .append('rect')
            .attr('class', 'processor-stats-border')
            .attr('width', d.ui.dimensions.width)
            .attr('height', 1)
            .attr('x', 0)
            .attr('y', 68);

        // read/write
        details
            .append('rect')
            .attr('class', 'processor-read-write-stats even')
            .attr('width', d.ui.dimensions.width)
            .attr('height', 19)
            .attr('x', 0)
            .attr('y', 69);

        // border
        details
            .append('rect')
            .attr('class', 'processor-stats-border')
            .attr('width', d.ui.dimensions.width)
            .attr('height', 1)
            .attr('x', 0)
            .attr('y', 87);

        // out
        details
            .append('rect')
            .attr('class', 'processor-stats-in-out odd')
            .attr('width', d.ui.dimensions.width)
            .attr('height', 20)
            .attr('x', 0)
            .attr('y', 88);

        // border
        details
            .append('rect')
            .attr('class', 'processor-stats-border')
            .attr('width', d.ui.dimensions.width)
            .attr('height', 1)
            .attr('x', 0)
            .attr('y', 106);

        // tasks/time
        details
            .append('rect')
            .attr('class', 'processor-read-write-stats even')
            .attr('width', d.ui.dimensions.width)
            .attr('height', 19)
            .attr('x', 0)
            .attr('y', 107);

        // stats label container
        const processorStatsLabel = details.append('g').attr('transform', 'translate(10, 55)');

        // in label
        processorStatsLabel
            .append('text')
            .attr('width', 73)
            .attr('height', 10)
            .attr('y', 9)
            .attr('class', 'stats-label')
            .text('In');

        // read/write label
        processorStatsLabel
            .append('text')
            .attr('width', 73)
            .attr('height', 10)
            .attr('y', 27)
            .attr('class', 'stats-label')
            .text('Read/write');

        // out label
        processorStatsLabel
            .append('text')
            .attr('width', 73)
            .attr('height', 10)
            .attr('y', 46)
            .attr('class', 'stats-label')
            .text('Out');

        // tasks/time label
        processorStatsLabel
            .append('text')
            .attr('width', 73)
            .attr('height', 10)
            .attr('y', 65)
            .attr('class', 'stats-label')
            .text('Tasks/time');

        // stats value container
        const processorStatsValue = details.append('g').attr('transform', 'translate(85, 55)');

        // in value
        const inText = processorStatsValue
            .append('text')
            .attr('width', 180)
            .attr('height', 9)
            .attr('y', 9)
            .attr('class', 'processor-in stats-value');

        // in count
        inText.append('tspan').attr('class', 'count');

        // in size
        inText.append('tspan').attr('class', 'size');

        // read/write value
        processorStatsValue
            .append('text')
            .attr('width', 180)
            .attr('height', 10)
            .attr('y', 27)
            .attr('class', 'processor-read-write stats-value');

        // out value
        const outText = processorStatsValue
            .append('text')
            .attr('width', 180)
            .attr('height', 9)
            .attr('y', 46)
            .attr('class', 'processor-out stats-value');

        // out count
        outText.append('tspan').attr('class', 'count');

        // out size
        outText.append('tspan').attr('class', 'size');

        // tasks/time value
        processorStatsValue
            .append('text')
            .attr('width', 180)
            .attr('height', 10)
            .attr('y', 65)
            .attr('class', 'processor-tasks-time stats-value');

        // stats value container
        const processorStatsInfo = details.append('g').attr('transform', 'translate(305, 55)');

        // in info
        processorStatsInfo
            .append('text')
            .attr('width', 25)
            .attr('height', 10)
            .attr('y', 9)
            .attr('class', 'stats-info')
            .text('5 min');

        // read/write info
        processorStatsInfo
            .append('text')
            .attr('width', 25)
            .attr('height', 10)
            .attr('y', 27)
            .attr('class', 'stats-info')
            .text('5 min');

        // out info
        processorStatsInfo
            .append('text')
            .attr('width', 25)
            .attr('height', 10)
            .attr('y', 46)
            .attr('class', 'stats-info')
            .text('5 min');

        // tasks/time info
        processorStatsInfo
            .append('text')
            .attr('width', 25)
            .attr('height', 10)
            .attr('y', 65)
            .attr('class', 'stats-info')
            .text('5 min');

        // Append bulletin elements
        ProcessorRenderer.appendBulletinElements(details);

        // Append active thread count elements
        ProcessorRenderer.appendActiveThreadElements(details);

        // Append comment elements
        ProcessorRenderer.appendCommentElements(details);
    }

    /**
     * Helper to update processor details (called for visible processors)
     */
    private static updateProcessorDetails(
        processor: d3.Selection<SVGGElement, CanvasProcessor, any, any>,
        d: CanvasProcessor,
        context: ProcessorRenderContext
    ): void {
        // Update run status icon
        // Note: Run status is shown even for unauthorized processors
        processor.select('text.run-status-icon').each(function () {
            const icon = d3.select(this as SVGTextElement);

            if (!d.entity.status?.aggregateSnapshot) {
                icon.style('visibility', 'hidden');
                // Reset tooltip if no status
                context.componentUtils.resetCanvasTooltip(icon);
                return;
            }

            const runStatus = d.entity.status.aggregateSnapshot.runStatus || 'Stopped';

            icon.style('visibility', 'visible')
                .attr('class', `run-status-icon ${ProcessorRenderer.getRunStatusClass(runStatus)}`)
                .attr('font-family', ProcessorRenderer.getRunStatusFontFamily(runStatus))
                .classed('fa-spin', ProcessorRenderer.shouldSpinRunStatus(runStatus))
                .text(ProcessorRenderer.getRunStatusIcon(runStatus));

            // Add validation error tooltip if there are validation errors or processor is validating
            const needsValidationTip =
                (d.entity.permissions.canRead &&
                    d.entity.component?.validationErrors &&
                    d.entity.component.validationErrors.length > 0) ||
                runStatus === 'Validating';

            if (needsValidationTip) {
                context.componentUtils.canvasTooltip(ValidationErrorsTip, icon, {
                    isValidating: runStatus === 'Validating',
                    validationErrors: d.entity.component?.validationErrors || []
                });
            } else {
                context.componentUtils.resetCanvasTooltip(icon);
            }
        });

        // Update processor fields based on permissions
        if (d.entity.permissions.canRead) {
            // Check if processor is a preview extension
            const isPreview = ProcessorRenderer.isPreviewProcessor(d, context.previewExtensions);

            // update the processor preview badge visibility
            processor.select('rect.processor-preview-badge').style('visibility', isPreview ? 'visible' : 'hidden');

            // update the processor preview text visibility
            processor.select('text.processor-preview-text').style('visibility', isPreview ? 'visible' : 'hidden');

            // update the processor name (adjust position and width based on preview badge)
            processor
                .select('text.processor-name')
                .attr('x', isPreview ? 125 : 75)
                .attr('width', isPreview ? 180 : 230)
                .each((d2, i, nodes) => {
                    const processorName = d3.select(nodes[i]);

                    // reset the processor name to handle any previous state
                    processorName.text(null).selectAll('title').remove();

                    // apply ellipsis to the processor name as necessary
                    context.textEllipsis.applyEllipsis(processorName, d.entity.component?.name || '', 'processor-name');
                })
                .append('title')
                .text(() => d.entity.component?.name || '');

            // update the processor type
            processor
                .select('text.processor-type')
                .each((d2, i, nodes) => {
                    const processorType = d3.select(nodes[i]);

                    // reset the processor type to handle any previous state
                    processorType.text(null).selectAll('title').remove();

                    if (d.entity.component) {
                        // Format type: extract simple class name from full path
                        const type = d.entity.component.type || '';
                        const typeParts = type.split('.');
                        const simpleType = typeParts[typeParts.length - 1];

                        // Add version if not 'unversioned'
                        const bundle = d.entity.component.bundle;
                        let typeString = simpleType;
                        if (bundle && bundle.version && bundle.version !== 'unversioned') {
                            typeString += ' ' + bundle.version;
                        }

                        // apply ellipsis to the processor type as necessary
                        context.textEllipsis.applyEllipsis(processorType, typeString, 'processor-type');
                    }
                })
                .append('title')
                .text(() => {
                    if (!d.entity.component) {
                        return '';
                    }
                    const type = d.entity.component.type || '';
                    const typeParts = type.split('.');
                    const simpleType = typeParts[typeParts.length - 1];
                    const bundle = d.entity.component.bundle;
                    let typeString = simpleType;
                    if (bundle && bundle.version && bundle.version !== 'unversioned') {
                        typeString += ' ' + bundle.version;
                    }
                    return typeString;
                });

            // update the processor bundle
            processor
                .select('text.processor-bundle')
                .each((d2, i, nodes) => {
                    const processorBundle = d3.select(nodes[i]);

                    // reset the processor bundle to handle any previous state
                    processorBundle.text(null).selectAll('title').remove();

                    if (d.entity.component?.bundle) {
                        const bundle = d.entity.component.bundle;
                        // Format bundle: "group - artifact" (omit group if 'default')
                        let bundleString = '';
                        if (bundle.group && bundle.group !== 'default') {
                            bundleString = bundle.group + ' - ';
                        }
                        bundleString += bundle.artifact;

                        // apply ellipsis to the processor bundle as necessary
                        context.textEllipsis.applyEllipsis(processorBundle, bundleString, 'processor-bundle');
                    }
                })
                .append('title')
                .text(() => {
                    if (!d.entity.component?.bundle) {
                        return '';
                    }
                    const bundle = d.entity.component.bundle;
                    let bundleString = '';
                    if (bundle.group && bundle.group !== 'default') {
                        bundleString = bundle.group + ' - ';
                    }
                    bundleString += bundle.artifact;
                    return bundleString;
                });

            // update the processor comments
            context.componentUtils.comments(processor, d.entity.component?.config?.comments);
        } else {
            // clear the processor name
            processor.select('text.processor-name').text(null);

            // clear the processor type
            processor.select('text.processor-type').text(null);

            // clear the processor bundle
            processor.select('text.processor-bundle').text(null);

            // clear the processor comments
            context.componentUtils.comments(processor, null);

            // clear the processor preview badge
            processor.select('rect.processor-preview-badge').style('visibility', 'hidden');

            // clear the processor preview text
            processor.select('text.processor-preview-text').style('visibility', 'hidden');
        }

        // Restricted component indicator (shown for both authorized and unauthorized)
        const showRestricted = d.entity.permissions.canRead && d.entity.component?.restricted === true;
        processor.select('circle.restricted-background').style('visibility', showRestricted ? 'visible' : 'hidden');
        processor.select('text.restricted').style('visibility', showRestricted ? 'visible' : 'hidden');

        // Is primary component indicator (shown for both authorized and unauthorized)
        const showIsPrimary = d.entity.status?.aggregateSnapshot?.executionNode === 'PRIMARY';
        processor.select('circle.is-primary-background').style('visibility', showIsPrimary ? 'visible' : 'hidden');
        processor.select('text.is-primary').style('visibility', showIsPrimary ? 'visible' : 'hidden');

        // Update statistics (shown for both authorized and unauthorized)
        ProcessorRenderer.updateStatistics(processor, d, context);

        // Update bulletins (shown for both authorized and unauthorized)
        ProcessorRenderer.updateBulletins(processor, d, context);

        // Update active thread count (shown for both authorized and unauthorized)
        ProcessorRenderer.updateActiveThreadCount(processor, d, context);
    }

    /**
     * Update statistics for a processor
     */
    private static updateStatistics(
        processor: d3.Selection<SVGGElement, CanvasProcessor, any, any>,
        d: CanvasProcessor,
        context: ProcessorRenderContext
    ): void {
        if (!d.entity.permissions.canRead || !d.entity.component) {
            // Hide stats if no read permission
            processor.select('.processor-in .count').text('');
            processor.select('.processor-in .size').text('');
            processor.select('.processor-read-write').text('');
            processor.select('.processor-out .count').text('');
            processor.select('.processor-out .size').text('');
            processor.select('.processor-tasks-time').text('');
            return;
        }

        const stats = d.entity.status?.aggregateSnapshot;
        if (!stats) {
            return;
        }

        // In stats - split "0 (0 bytes)" into count and size using shared formatUtils
        const inputStr = stats.input || '0 (0 bytes)';
        const formattedInput = context.formatUtils.formatQueuedStats(inputStr);
        processor.select('.processor-in .count').text(formattedInput.count);
        processor.select('.processor-in .size').text(formattedInput.size);

        // Read/Write stats - direct concatenation
        processor.select('.processor-read-write').text(`${stats.read || '0'} / ${stats.written || '0'}`);

        // Out stats - split "0 (0 bytes)" into count and size using shared formatUtils
        const outputStr = stats.output || '0 (0 bytes)';
        const formattedOutput = context.formatUtils.formatQueuedStats(outputStr);
        processor.select('.processor-out .count').text(formattedOutput.count);
        processor.select('.processor-out .size').text(formattedOutput.size);

        // Tasks/Time stats - direct concatenation
        processor
            .select('.processor-tasks-time')
            .text(`${stats.tasks || '0'} / ${stats.tasksDuration || '00:00:00.000'}`);
    }

    /**
     * Update bulletins for a processor
     */
    private static updateBulletins(
        processor: d3.Selection<SVGGElement, CanvasProcessor, any, any>,
        d: CanvasProcessor,
        context: ProcessorRenderContext
    ): void {
        // Delegate to shared component utility for consistent bulletin rendering
        context.componentUtils.bulletins(processor, d.entity.bulletins);
    }

    /**
     * Update active thread count for a processor
     */
    private static updateActiveThreadCount(
        processor: d3.Selection<SVGGElement, CanvasProcessor, any, any>,
        d: CanvasProcessor,
        context: ProcessorRenderContext
    ): void {
        // Delegate to shared component utility for consistent active thread count rendering
        context.componentUtils.activeThreadCount(processor, d);
    }

    /**
     * Attach event handlers to processors
     */
    private static attachEventHandlers(
        selection: d3.Selection<SVGGElement, CanvasProcessor, any, any>,
        context: ProcessorRenderContext
    ): void {
        // Early exit if selection is empty (no new processors to attach handlers to)
        if (selection.empty()) {
            return;
        }

        if (!context.canSelect) {
            return; // No interactions when selection is disabled
        }

        const callbacks = context.callbacks;

        // Use mousedown for selection to ensure single, deterministic event firing
        if (callbacks.onClick) {
            selection.on('mousedown.selection', function (event: MouseEvent, d: CanvasProcessor) {
                // Only handle left mouse button
                if (event.button !== 0) {
                    return;
                }

                event.stopPropagation();
                callbacks.onClick!(d, event);
            });
        }

        if (callbacks.onDoubleClick) {
            selection.on('dblclick', function (event: MouseEvent, d: CanvasProcessor) {
                event.stopPropagation();
                callbacks.onDoubleClick!(d, event);
            });
        }

        // Attach drag behavior if callback is provided (filter will check canEdit dynamically)
        if (callbacks.onDragEnd) {
            ProcessorRenderer.attachDragBehavior(selection, context);
        }
    }

    /**
     * Attach drag behavior to processors
     */
    private static attachDragBehavior(
        selection: d3.Selection<SVGGElement, CanvasProcessor, any, any>,
        context: ProcessorRenderContext
    ): void {
        const drag = d3
            .drag<SVGGElement, CanvasProcessor>()
            .filter(function (event, d) {
                // Match D3's default filter: block right-click and Ctrl+click (Mac right-click)
                if (event.ctrlKey || event.button !== 0) {
                    return false;
                }
                // Block drag if editing is disabled
                if (!context.getCanEdit()) {
                    return false;
                }
                // Block drag if processor is disabled (saving)
                if (context.disabledProcessorIds?.has(d.entity.id)) {
                    return false;
                }
                return true;
            })
            .clickDistance(4) // Minimum distance in pixels before drag starts (prevents accidental drags during clicks)
            .on(
                'start',
                function (event: d3.D3DragEvent<SVGGElement, CanvasProcessor, CanvasProcessor>, d: CanvasProcessor) {
                    const selectionGroup = d3.select(this as SVGGElement);

                    // If the processor isn't currently selected, select it before starting the drag
                    if (!selectionGroup.classed('selected') && context.callbacks.onClick) {
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
                function (event: d3.D3DragEvent<SVGGElement, CanvasProcessor, CanvasProcessor>, d: CanvasProcessor) {
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
                        // The ConnectionRenderer.calculatePath will use ui.currentPosition
                        d3.selectAll('g.connection').each(function () {
                            const connectionData: any = d3.select(this).datum();

                            // Check if this connection is attached to the dragged processor
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
                function (event: d3.D3DragEvent<SVGGElement, CanvasProcessor, CanvasProcessor>, d: CanvasProcessor) {
                    // Only emit if position actually changed
                    if (d.ui.dragStartPosition && d.ui.currentPosition) {
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

                        const moved =
                            d.ui.currentPosition.x !== d.ui.dragStartPosition.x ||
                            d.ui.currentPosition.y !== d.ui.dragStartPosition.y;

                        if (moved && context.callbacks.onDragEnd) {
                            context.callbacks.onDragEnd(d, d.ui.currentPosition, d.ui.dragStartPosition);
                        }

                        // Clean up drag start position only
                        // Keep currentPosition until API call completes (for disabled treatment)
                        delete d.ui.dragStartPosition;
                    }
                }
            );

        // Remove all drag-related event handlers to prevent stale closures
        // D3 drag uses mousedown/touchstart, so we need to remove those too
        selection.on('.drag', null).on('mousedown.drag', null).on('touchstart.drag', null);

        // Apply drag behavior to processors with write permissions
        // Disabled state is checked dynamically in the 'start' handler
        selection
            .filter((d: CanvasProcessor) => d.entity.permissions.canWrite && d.entity.permissions.canRead)
            .call(drag);
    }
}
