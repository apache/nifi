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
import { CanvasConstants } from '../canvas.constants';

/**
 * Space between the Draining label and the edge of its background.
 */
export const DRAINING_BADGE_HORIZONTAL_PADDING = 5;

/**
 * Appends a hidden Draining badge inside a process group's Queued stats row. The badge is shorter
 * than the row so it never changes the row height. Its horizontal layout is applied by
 * `updateDrainingBadge`, once the label's font is rendered and can be measured accurately.
 */
export function appendDrainingBadge<PElement extends d3.BaseType, PDatum>(
    details: d3.Selection<SVGGElement, PDatum, PElement, unknown>,
    queuedRowY: number
): d3.Selection<SVGGElement, PDatum, PElement, unknown> {
    const badgeY =
        queuedRowY +
        (CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT - CanvasConstants.PROCESS_GROUP_DRAINING_BADGE_HEIGHT) / 2;

    const badge = details.append('g').attr('class', 'process-group-draining-badge').style('visibility', 'hidden');

    badge
        .append('rect')
        .attr('class', 'process-group-draining-badge-background')
        .attr('y', badgeY)
        .attr('height', CanvasConstants.PROCESS_GROUP_DRAINING_BADGE_HEIGHT)
        .attr('rx', 2)
        .attr('ry', 2);

    badge
        .append('text')
        .attr('class', 'process-group-draining-badge-label')
        .attr('y', badgeY + CanvasConstants.PROCESS_GROUP_DRAINING_BADGE_HEIGHT / 2)
        .attr('text-anchor', 'middle')
        .attr('dominant-baseline', 'central')
        .text('Draining');

    return badge;
}

/**
 * Shows or hides the Draining badge previously appended to `details` and returns it so the
 * caller can attach or reset its tooltip. A visible badge is laid out so its label is centered in
 * the background and the label's right side sits at the value returned by `getLabelRightEdgeX`.
 * The getter is only called while draining, so non-draining groups do not measure their text.
 */
export function updateDrainingBadge<PElement extends d3.BaseType, PDatum>(
    details: d3.Selection<SVGGElement, PDatum, PElement, unknown>,
    draining: boolean,
    getLabelRightEdgeX: () => number
): d3.Selection<SVGGElement, PDatum, PElement, unknown> {
    const badge = details
        .select<SVGGElement>('g.process-group-draining-badge')
        .style('visibility', draining ? 'visible' : 'hidden');

    if (draining) {
        const label = badge.select<SVGTextElement>('text.process-group-draining-badge-label');
        const labelWidth = (label.node() as SVGTextElement).getComputedTextLength();
        const badgeWidth = labelWidth + 2 * DRAINING_BADGE_HORIZONTAL_PADDING;
        const badgeX = getLabelRightEdgeX() - labelWidth - DRAINING_BADGE_HORIZONTAL_PADDING;

        badge.select('rect.process-group-draining-badge-background').attr('x', badgeX).attr('width', badgeWidth);
        label.attr('x', badgeX + badgeWidth / 2);
    }

    return badge;
}
