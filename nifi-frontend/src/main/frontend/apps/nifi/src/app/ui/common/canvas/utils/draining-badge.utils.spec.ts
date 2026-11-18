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
import { appendDrainingBadge, DRAINING_BADGE_HORIZONTAL_PADDING, updateDrainingBadge } from './draining-badge.utils';
import { CanvasConstants } from '../canvas.constants';

describe('draining badge utils', () => {
    function setup() {
        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        document.body.appendChild(svg);
        const details = d3.select(svg).append('g').attr('class', 'process-group-details');

        return {
            details,
            cleanup: () => svg.remove()
        };
    }

    it('appends a hidden badge that fits inside the queued row', () => {
        const { details, cleanup } = setup();
        const queuedRowY = 66;

        appendDrainingBadge(details, queuedRowY);

        const badge = details.select('g.process-group-draining-badge');
        const background = badge.select('rect.process-group-draining-badge-background');
        const top = Number(background.attr('y'));
        const bottom = top + Number(background.attr('height'));

        expect(badge.style('visibility')).toBe('hidden');
        expect(badge.select('text.process-group-draining-badge-label').text()).toBe('Draining');
        expect(top).toBeGreaterThan(queuedRowY);
        expect(bottom).toBeLessThan(queuedRowY + CanvasConstants.PROCESS_GROUP_STATS_ROW_HEIGHT);

        cleanup();
    });

    it('centers the label in a background that ends the padding past the requested edge', () => {
        const { details, cleanup } = setup();
        const labelRightEdgeX = 370;

        appendDrainingBadge(details, 66);
        const badge = updateDrainingBadge(details, true, () => labelRightEdgeX);

        const label = badge.select('text.process-group-draining-badge-label');
        const background = badge.select('rect.process-group-draining-badge-background');
        const backgroundX = Number(background.attr('x'));
        const backgroundWidth = Number(background.attr('width'));

        expect(label.attr('text-anchor')).toBe('middle');
        expect(Number(label.attr('x'))).toBe(backgroundX + backgroundWidth / 2);
        expect(backgroundX + backgroundWidth).toBe(labelRightEdgeX + DRAINING_BADGE_HORIZONTAL_PADDING);

        cleanup();
    });

    it('toggles badge visibility', () => {
        const { details, cleanup } = setup();
        appendDrainingBadge(details, 66);

        expect(updateDrainingBadge(details, true, () => 370).style('visibility')).toBe('visible');
        expect(updateDrainingBadge(details, false, () => 370).style('visibility')).toBe('hidden');

        cleanup();
    });

    it('does not measure the label edge when the badge is hidden', () => {
        const { details, cleanup } = setup();
        appendDrainingBadge(details, 66);
        const getLabelRightEdgeX = vi.fn(() => 370);

        updateDrainingBadge(details, false, getLabelRightEdgeX);

        expect(getLabelRightEdgeX).not.toHaveBeenCalled();

        cleanup();
    });
});
