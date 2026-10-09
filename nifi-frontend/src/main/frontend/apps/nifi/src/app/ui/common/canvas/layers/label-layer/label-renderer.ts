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
import { CanvasLabel, CanvasSelection } from '../../canvas.types';
import { LabelRenderContext } from '../render-context.types';
import { DragUtils } from '../../utils/drag.utils';
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

        // Reconcile drag attachment on the merged selection so existing labels
        // respond to permission changes between renders.
        const callbacks = context.callbacks;
        if (callbacks.onResizeEnd) {
            LabelRenderer.applyResizeDragBehavior(updated, context);
        }
        if (callbacks.onDragEnd) {
            LabelRenderer.applyPositionDragBehavior(updated, context);
        }

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

    public static pan(selection: CanvasSelection<CanvasLabel>, context: LabelRenderContext): void {
        if (selection.empty()) {
            return;
        }

        // Simply delegate to updateLabelElements which handles all updates
        LabelRenderer.updateLabelElements(selection, context);
    }

    private static appendLabelElements(entered: CanvasSelection<CanvasLabel>): void {
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

    private static updateLabelElements(updated: CanvasSelection<CanvasLabel>, context: LabelRenderContext): void {
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
        updated.each(function (this: SVGGElement, d: CanvasLabel) {
            const label = d3.select<SVGGElement, CanvasLabel>(this);
            const labelText = label.select<SVGTextElement>('text.label-value');

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

    private static attachEventHandlers(selection: CanvasSelection<CanvasLabel>, context: LabelRenderContext): void {
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
    }

    private static applyPositionDragBehavior(
        selection: CanvasSelection<CanvasLabel>,
        context: LabelRenderContext
    ): void {
        DragUtils.attachComponentDrag(selection, {
            resolveCanvasRoot: context.canvasRootResolver,
            getCanEdit: context.getCanEdit,
            getCanSelect: context.getCanSelect,
            getDisabledIds: context.getDisabledLabelIds,
            getSelectedIds: context.getSelectedIds,
            onDragEnd: context.callbacks.onDragEnd!,
            extraFilter: (event) => (event.target as Element).classList.contains('body')
        });
    }

    private static applyResizeDragBehavior(selection: CanvasSelection<CanvasLabel>, context: LabelRenderContext): void {
        const callbacks = context.callbacks;
        const eligible = selection.filter(
            (d: CanvasLabel) => d.entity.permissions.canWrite && d.entity.permissions.canRead
        );
        const newlyResizeable = eligible.filter(function (this: SVGGElement) {
            return !d3.select(this as Element).classed('resizeable');
        });
        const noLongerResizeable = selection.filter(function (this: SVGGElement, d: CanvasLabel) {
            const isResizeable = d3.select(this as Element).classed('resizeable');
            const stillEligible = d.entity.permissions.canWrite && d.entity.permissions.canRead;
            return isResizeable && !stillEligible;
        });

        noLongerResizeable.classed('resizeable', false).select('path.resizable-triangle').on('.drag', null);

        if (newlyResizeable.empty()) {
            return;
        }

        const resizeDrag = d3
            .drag<SVGPathElement, CanvasLabel>()
            .filter(function (event, d) {
                if (event.ctrlKey || event.button !== 0) {
                    return false;
                }
                if (!context.getCanEdit()) {
                    return false;
                }
                if (!context.getCanSelect()) {
                    return false;
                }
                if (context.getDisabledLabelIds().has(d.entity.id)) {
                    return false;
                }
                return true;
            })
            .on('start', function (event, d) {
                event.sourceEvent.stopPropagation();
                d.ui.dragStartRevision = d.entity.revision;

                if (this.parentNode) {
                    d3.select(this.parentNode as Element).classed('resizing', true);
                }
            })
            .on('drag', function (event, d) {
                const snapEnabled = !event.sourceEvent.shiftKey;
                let newWidth = Math.max(CanvasConstants.LABEL_MIN.width, d.ui.dimensions.width + event.dx);
                let newHeight = Math.max(CanvasConstants.LABEL_MIN.height, d.ui.dimensions.height + event.dy);

                if (snapEnabled) {
                    newWidth =
                        Math.round(newWidth / CanvasConstants.SNAP_ALIGNMENT_PIXELS) *
                        CanvasConstants.SNAP_ALIGNMENT_PIXELS;
                    newHeight =
                        Math.round(newHeight / CanvasConstants.SNAP_ALIGNMENT_PIXELS) *
                        CanvasConstants.SNAP_ALIGNMENT_PIXELS;
                }

                d.ui.dimensions.width = newWidth;
                d.ui.dimensions.height = newHeight;

                if (this.parentNode) {
                    const label = d3.select(this.parentNode as Element);
                    label.select('rect.body').attr('width', newWidth).attr('height', newHeight);
                    label.select('rect.border').attr('width', newWidth).attr('height', newHeight);
                    label
                        .select('path.resizable-triangle')
                        .attr('transform', `translate(${newWidth - 2}, ${newHeight - 10})`);
                }
            })
            .on('end', function (event, d) {
                if (this.parentNode) {
                    d3.select(this.parentNode as Element).classed('resizing', false);
                }

                if (callbacks.onResizeEnd) {
                    callbacks.onResizeEnd(d, {
                        width: d.ui.dimensions.width,
                        height: d.ui.dimensions.height
                    });
                }
                delete d.ui.dragStartRevision;
            });

        newlyResizeable.classed('resizeable', true).select<SVGPathElement>('path.resizable-triangle').call(resizeDrag);
    }

    private static boundedMultilineEllipsis<PElement extends d3.BaseType, PDatum>(
        selection: d3.Selection<SVGTextElement, CanvasLabel, PElement, PDatum>,
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
