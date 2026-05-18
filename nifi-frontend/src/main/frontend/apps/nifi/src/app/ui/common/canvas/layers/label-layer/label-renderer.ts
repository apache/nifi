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
import { CanvasLabel } from '../../canvas.types';
import { LabelRenderContext } from '../render-context.types';
import { TextEllipsisUtils } from '../../utils/text-ellipsis.utils';
import { CanvasConstants } from '../../canvas.constants';

export class LabelRenderer {
    public static render(context: LabelRenderContext): void {
        // D3 Data Join: Bind data to elements
        const selection = context.containerSelection
            .selectAll<SVGGElement, CanvasLabel>('g.label')
            .data(context.labels, (d: CanvasLabel) => d.entity.id);

        // ENTER: Create new label elements
        const entered = selection
            .enter()
            .append('g')
            .attr('id', (d) => 'id-' + d.entity.id)
            .attr('class', 'label component');

        LabelRenderer.appendLabelElements(entered);
        LabelRenderer.attachEventHandlers(entered, context);

        // UPDATE: Merge enter and update selections
        const updated = entered.merge(selection);

        LabelRenderer.updateLabelElements(updated, context);

        // Sort labels by zIndex to ensure correct rendering order
        // Labels with higher z-index appear on top of labels with lower z-index
        // D3's sort() method sorts the selection AND reorders DOM elements in one operation
        //
        // Note: zIndex is stored at entity level (entity.zIndex), not in component
        updated.sort((a, b) => {
            return context.nifiCommon.compareNumber(a.entity.zIndex, b.entity.zIndex);
        });

        // EXIT: Remove labels that are no longer in data
        selection.exit().remove();
    }

    public static pan(selection: d3.Selection<any, any, any, any>, context: LabelRenderContext): void {
        if (selection.empty()) {
            return;
        }

        // Simply delegate to updateLabelElements which handles all updates
        LabelRenderer.updateLabelElements(selection, context);
    }

    private static appendLabelElements(entered: d3.Selection<SVGGElement, CanvasLabel, any, any>): void {
        // Border (for selection highlight)
        entered
            .append('rect')
            .attr('class', 'border')
            .attr('fill', 'transparent')
            .attr('stroke', 'transparent')
            .attr('stroke-width', 3);

        // Body (main label rectangle)
        entered
            .append('rect')
            .attr('class', 'body')
            .attr('filter', 'url(#component-drop-shadow)')
            .attr('stroke-width', 0);

        // Text (label content)
        entered
            .append('text')
            .attr('x', 10)
            .attr('xml:space', 'preserve')
            .attr('font-weight', 'bold')
            .attr('fill', 'black')
            .attr('class', 'label-value');

        // Resize handle (bottom-right corner)
        entered
            .append('path')
            .attr('class', 'labelpoint resizable-triangle')
            .attr('d', 'm0,0 l0,8 l-8,0 z')
            .attr('pointer-events', 'all')
            .style('cursor', 'nwse-resize');
    }

    private static updateLabelElements(
        updated: d3.Selection<SVGGElement, CanvasLabel, any, any>,
        context: LabelRenderContext
    ): void {
        // Position labels (use currentPosition during drag, otherwise entity position)
        updated.attr('transform', (d) => {
            const position = d.ui.currentPosition || d.entity.position;
            return `translate(${position.x}, ${position.y})`;
        });

        // Apply disabled state (during save operation)
        updated
            .classed('disabled', (d) => context.disabledLabelIds?.has(d.entity.id) || false)
            .style('opacity', (d) => (context.disabledLabelIds?.has(d.entity.id) ? 0.6 : null))
            .style('cursor', (d) => {
                if (context.disabledLabelIds?.has(d.entity.id)) return 'not-allowed';
                return context.canSelect ? 'pointer' : 'default';
            });

        // Update border
        updated
            .select('rect.border')
            .attr('width', (d) => d.ui.dimensions.width)
            .attr('height', (d) => d.ui.dimensions.height)
            .classed('unauthorized', (d) => !d.entity.permissions.canRead);

        // Update body (background) using the configured color
        updated
            .select('rect.body')
            .attr('width', (d) => d.ui.dimensions.width)
            .attr('height', (d) => d.ui.dimensions.height)
            .style('fill', (d) => {
                if (!d.entity.permissions.canRead) {
                    return null;
                }

                let color = CanvasConstants.LABEL_DEFAULT_COLOR;

                // use the specified color if appropriate
                if (d.entity.component?.style?.['background-color']) {
                    color = d.entity.component.style['background-color'];
                }

                return color;
            })
            .classed('unauthorized', (d) => !d.entity.permissions.canRead);

        // Update resize handle visibility, cursor, and position
        updated
            .select('path.resizable-triangle')
            .style('display', (d) => {
                // Hide if not selectable, not editable, or if disabled (saving)
                if (!context.canSelect) return 'none';
                if (!context.getCanEdit()) return 'none';
                if (context.disabledLabelIds?.has(d.entity.id)) return 'none';
                return null; // CSS will control visibility (hidden by default, shown on hover/selected)
            })
            .style('cursor', () => {
                // Only show resize cursor if editing is enabled
                return context.getCanEdit() ? 'nwse-resize' : 'default';
            })
            .attr('transform', (d) => {
                // Position in bottom-right corner
                return `translate(${d.ui.dimensions.width - 2}, ${d.ui.dimensions.height - 10})`;
            });

        // Apply disabled state to label
        updated
            .classed('disabled', (d) => context.disabledLabelIds?.has(d.entity.id) || false)
            .style('opacity', (d) => (context.disabledLabelIds?.has(d.entity.id) ? 0.6 : null));

        // Update text content with multi-line wrapping and ellipsis
        updated.each(function (d: CanvasLabel) {
            const label = d3.select(this);
            const labelText = label.select('text.label-value');

            if (d.entity.permissions.canRead) {
                // update the font size
                labelText.attr('font-size', () => {
                    let fontSize = '12px';

                    // use the specified color if appropriate
                    if (d.entity.component?.style?.['font-size']) {
                        fontSize = d.entity.component.style['font-size'];
                    }

                    return fontSize;
                });

                // remove the previous label value
                labelText.selectAll('tspan').remove();

                // parse the lines in this label
                let lines: string[] = [];
                if (d.entity.component?.label) {
                    lines = d.entity.component.label.split('\n');
                } else {
                    lines.push('');
                }

                let color = CanvasConstants.LABEL_DEFAULT_COLOR;

                // use the specified color if appropriate
                if (d.entity.component?.style?.['background-color']) {
                    color = d.entity.component.style['background-color'];
                }

                // add label value with bounded multi-line ellipsis
                const textWidth = d.ui.dimensions.width - 15;
                const textHeight = d.ui.dimensions.height;
                LabelRenderer.boundedMultilineEllipsis(
                    labelText,
                    textWidth,
                    textHeight,
                    lines,
                    `label-text.${d.entity.id}.width.${textWidth}`,
                    context.scale,
                    context.textEllipsis
                );

                // Apply contrast color to all tspans
                // Uses shared utility method from TextEllipsisUtils
                const contrastColor = context.textEllipsis.determineContrastColor(color);
                labelText.selectAll('tspan').style('fill', contrastColor);
            } else {
                // Clear text for unauthorized labels
                labelText.selectAll('tspan').remove();
            }
        });
    }

    private static attachEventHandlers(
        selection: d3.Selection<SVGGElement, CanvasLabel, any, any>,
        context: LabelRenderContext
    ): void {
        if (!context.canSelect) {
            return; // No interactions when selection is disabled
        }

        const callbacks = context.callbacks;

        // Use mousedown for selection to ensure single, deterministic event firing
        if (callbacks.onClick) {
            selection.on('mousedown.selection', function (event: MouseEvent, d: CanvasLabel) {
                // Only handle left mouse button
                if (event.button !== 0) {
                    return;
                }

                event.stopPropagation();
                callbacks.onClick!(d, event);
            });
        }

        if (callbacks.onDoubleClick) {
            selection.on('dblclick', function (event: MouseEvent, d: CanvasLabel) {
                event.stopPropagation();
                callbacks.onDoubleClick!(d, event);
            });
        }

        // Attach resize drag behavior (filter will check canEdit dynamically)
        if (callbacks.onResizeEnd) {
            const resizeDrag = d3
                .drag<SVGPathElement, CanvasLabel>()
                .filter(function (event, d) {
                    // Match D3's default filter: block right-click and Ctrl+click (Mac right-click)
                    if (event.ctrlKey || event.button !== 0) {
                        return false;
                    }
                    // Block drag if editing is disabled
                    if (!context.getCanEdit()) {
                        return false;
                    }
                    // Block drag if label is disabled (saving)
                    if (context.disabledLabelIds?.has(d.entity.id)) {
                        return false;
                    }
                    return true;
                })
                .on('start', function (event) {
                    event.sourceEvent.stopPropagation();

                    // Add visual feedback that resize is active
                    if (this.parentNode) {
                        d3.select(this.parentNode as Element).classed('resizing', true);
                    }
                })
                .on('drag', function (event, d) {
                    // Apply snap-to-grid unless shift key is held
                    const snapEnabled = !event.sourceEvent.shiftKey;

                    // Calculate new dimensions
                    let newWidth = Math.max(CanvasConstants.LABEL_MIN.width, d.ui.dimensions.width + event.dx);
                    let newHeight = Math.max(CanvasConstants.LABEL_MIN.height, d.ui.dimensions.height + event.dy);

                    // Apply snap alignment if enabled
                    if (snapEnabled) {
                        newWidth =
                            Math.round(newWidth / CanvasConstants.SNAP_ALIGNMENT_PIXELS) *
                            CanvasConstants.SNAP_ALIGNMENT_PIXELS;
                        newHeight =
                            Math.round(newHeight / CanvasConstants.SNAP_ALIGNMENT_PIXELS) *
                            CanvasConstants.SNAP_ALIGNMENT_PIXELS;
                    }

                    // Update local dimensions for immediate visual feedback
                    d.ui.dimensions.width = newWidth;
                    d.ui.dimensions.height = newHeight;

                    // Update visuals
                    if (this.parentNode) {
                        const label = d3.select(this.parentNode as Element);
                        label.select('rect.body').attr('width', newWidth).attr('height', newHeight);

                        label.select('rect.border').attr('width', newWidth).attr('height', newHeight);

                        label
                            .select('path.resizable-triangle')
                            .attr('transform', `translate(${newWidth - 2}, ${newHeight - 10})`);
                    }

                    // Note: Text will be re-wrapped on next full render
                })
                .on('end', function (event, d) {
                    // Remove resizing visual feedback
                    if (this.parentNode) {
                        d3.select(this.parentNode as Element).classed('resizing', false);
                    }

                    if (callbacks.onResizeEnd) {
                        callbacks.onResizeEnd(d, {
                            width: d.ui.dimensions.width,
                            height: d.ui.dimensions.height
                        });
                    }
                });

            // Remove drag behavior to prevent stale closures
            selection.select('path.resizable-triangle').on('.drag', null);
            selection.select<SVGPathElement>('path.resizable-triangle').call(resizeDrag);
        }

        // Attach position drag behavior (filter will check canEdit dynamically)
        if (callbacks.onDragEnd) {
            const positionDrag = d3
                .drag<SVGGElement, CanvasLabel>()
                .filter(function (event, d) {
                    // Match D3's default filter: block right-click and Ctrl+click (Mac right-click)
                    if (event.ctrlKey || event.button !== 0) {
                        return false;
                    }
                    // Block drag if editing is disabled
                    if (!context.getCanEdit()) {
                        return false;
                    }
                    // Block drag if label is disabled (saving)
                    if (context.disabledLabelIds?.has(d.entity.id)) {
                        return false;
                    }
                    // Only allow drag on the body, not the resize handle
                    return event.target.classList.contains('body');
                })
                .clickDistance(4) // Minimum distance in pixels before drag starts (prevents accidental drags during clicks)
                .on('start', function (event, d) {
                    const labelGroup = d3.select(this as SVGGElement);

                    if (!labelGroup.classed('selected') && callbacks.onClick) {
                        callbacks.onClick(d, event.sourceEvent as MouseEvent);
                    }

                    event.sourceEvent.stopPropagation();

                    // Store original position for potential revert
                    d.ui.dragStartPosition = { ...d.entity.position };
                    // Initialize current position in UI state
                    d.ui.currentPosition = { ...d.entity.position };
                })
                .on('drag', function (event, d) {
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
                    }
                })
                .on('end', function (event, d) {
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
                    if (moved && callbacks.onDragEnd) {
                        callbacks.onDragEnd(d, newPosition, previousPosition);
                    }
                });

            // Remove drag behavior to prevent stale closures
            selection.on('.drag', null);
            selection.call(positionDrag);
        }
    }

    private static boundedMultilineEllipsis(
        selection: d3.Selection<any, any, any, any>,
        width: number,
        height: number,
        lines: string[],
        cacheName: string,
        scale: number,
        textEllipsis: TextEllipsisUtils
    ): void {
        let i = 1;

        // get the appropriate position
        const x = parseInt(selection.attr('x'), 10);

        let lineCountCalculated = false;
        let lineCount = 1;
        let lineHeight = height;

        for (const fullLine of lines) {
            // Extract and preserve only the leading whitespace at the start of the line
            const trimmedLine = fullLine.trimStart();
            const leadingWhitespace = fullLine.slice(0, fullLine.length - trimmedLine.length);

            // Split words normally, letting internal whitespace collapse
            const words = trimmedLine.split(/\s+/).reverse();
            if (leadingWhitespace.length > 0 && words.length > 0) {
                words[words.length - 1] = leadingWhitespace + words[words.length - 1]; // Prepend leading space to the first word
            }

            let newLine = true;
            let line: string[] = [];

            let tspan = selection.append('tspan').attr('x', x).attr('width', width).attr('xml:space', 'preserve');

            // go through each word
            let word = words.pop();

            while (word) {
                // add the current word
                line.push(word);

                // update the label text
                tspan.text(line.join(' '));

                if (!lineCountCalculated) {
                    const bbox = (tspan.node() as SVGTextElement).getBoundingClientRect();
                    lineHeight = bbox.height / scale;

                    lineCount = Math.floor(height / lineHeight);
                    lineCountCalculated = true;
                }

                if (newLine) {
                    // set the label height
                    tspan.attr('y', lineHeight * i++);
                    newLine = false;
                }

                // if this word caused us to go too far
                if ((tspan.node() as SVGTextElement).getComputedTextLength() > width) {
                    // remove the current word
                    line.pop();

                    // update the label text
                    tspan.text(line.join(' '));

                    // create the tspan for the next line
                    tspan = selection
                        .append('tspan')
                        .attr('x', x)
                        .attr('dy', '1.2em')
                        .attr('width', width)
                        .attr('xml:space', 'preserve');

                    // if we've reached the last line, use single line ellipsis
                    if (i++ >= lineCount) {
                        // get the remainder using the current word and reversing whats left
                        const remainder = [word].concat(words.reverse());

                        // apply ellipsis to the last line
                        textEllipsis.applyEllipsis(tspan, remainder.join(' '), cacheName);

                        // we've reached the line count
                        return;
                    } else {
                        tspan.text(word);

                        // prep the line for the next iteration
                        line = [word];
                    }
                }

                // get the next word
                word = words.pop();
            }

            if (newLine) {
                // set the label height
                tspan.attr('y', lineHeight * i++);
            }

            if (i >= lineCount) {
                return;
            }
        }
    }
}
