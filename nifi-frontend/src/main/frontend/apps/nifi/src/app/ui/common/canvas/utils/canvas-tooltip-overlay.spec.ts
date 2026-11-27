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

import { Component, Input } from '@angular/core';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { Overlay, OverlayModule } from '@angular/cdk/overlay';
import * as d3 from 'd3';
import { NiFiCommon, TextTip } from '@nifi/shared';
import { bindCanvasTooltip, CanvasTooltipSessionMap, unbindCanvasTooltip } from './canvas-tooltip-overlay';

@Component({
    selector: 'other-tip',
    template: '{{ data }}'
})
class OtherTip {
    @Input() data: string | undefined;
}

describe('canvas tooltip overlay', () => {
    const DELAY = NiFiCommon.TOOLTIP_DELAY_OPEN_MILLIS;

    let overlay: Overlay;
    let sessions: CanvasTooltipSessionMap;
    let host: HTMLDivElement;

    beforeEach(() => {
        // OverlayRef.detach observes the overlay pane, which the test DOM does not support.
        vi.stubGlobal(
            'MutationObserver',
            class {
                observe = vi.fn();
                disconnect = vi.fn();
                takeRecords = vi.fn(() => []);
            }
        );
        TestBed.configureTestingModule({
            imports: [OverlayModule]
        });
        overlay = TestBed.inject(Overlay);
        sessions = new WeakMap();
        host = document.createElement('div');
        document.body.appendChild(host);
    });

    afterEach(() => {
        host.remove();
        vi.unstubAllGlobals();
    });

    function appendTarget(): d3.Selection<HTMLDivElement, unknown, null, undefined> {
        return d3.select(host).append('div');
    }

    function textTips(): NodeListOf<Element> {
        return document.querySelectorAll('text-tip');
    }

    function openTip(selection: d3.Selection<any, any, any, any>): void {
        selection.dispatch('mouseenter');
        tick(DELAY);
    }

    it('opens after the open delay', fakeAsync(() => {
        const selection = appendTarget();
        bindCanvasTooltip(overlay, sessions, TextTip, selection, 'first');

        selection.dispatch('mouseenter');
        tick(DELAY - 1);
        expect(textTips().length).toBe(0);

        tick(1);
        expect(textTips()[0]?.textContent).toContain('first');

        unbindCanvasTooltip(sessions, selection);
    }));

    it('updates an open tooltip when bindCanvasTooltip is called again', fakeAsync(() => {
        const selection = appendTarget();
        bindCanvasTooltip(overlay, sessions, TextTip, selection, 'first');
        openTip(selection);

        bindCanvasTooltip(overlay, sessions, TextTip, selection, 'second');

        expect(textTips().length).toBe(1);
        expect(textTips()[0]?.textContent).toContain('second');

        unbindCanvasTooltip(sessions, selection);
    }));

    it('opens with the latest data when rebound during the open delay', fakeAsync(() => {
        const selection = appendTarget();
        bindCanvasTooltip(overlay, sessions, TextTip, selection, 'first');

        selection.dispatch('mouseenter');
        tick(DELAY / 2);
        bindCanvasTooltip(overlay, sessions, TextTip, selection, 'second');
        tick(DELAY / 2);

        expect(textTips()[0]?.textContent).toContain('second');

        unbindCanvasTooltip(sessions, selection);
    }));

    it('never opens when the pointer leaves before the open delay', fakeAsync(() => {
        const selection = appendTarget();
        bindCanvasTooltip(overlay, sessions, TextTip, selection, 'first');

        selection.dispatch('mouseenter');
        tick(DELAY / 2);
        selection.dispatch('mouseleave');
        tick(DELAY * 2);

        expect(textTips().length).toBe(0);
    }));

    it('closes after the pointer leaves the element', fakeAsync(() => {
        const selection = appendTarget();
        bindCanvasTooltip(overlay, sessions, TextTip, selection, 'first');
        openTip(selection);

        selection.dispatch('mouseleave');
        tick(DELAY);

        expect(textTips().length).toBe(0);
    }));

    it('stays open when the pointer returns to the element before the close delay', fakeAsync(() => {
        const selection = appendTarget();
        bindCanvasTooltip(overlay, sessions, TextTip, selection, 'first');
        openTip(selection);

        selection.dispatch('mouseleave');
        tick(DELAY / 2);
        selection.dispatch('mouseenter');
        tick(DELAY * 2);

        expect(textTips().length).toBe(1);

        unbindCanvasTooltip(sessions, selection);
    }));

    it('stays open while the pointer is over the tooltip and closes when it leaves', fakeAsync(() => {
        const selection = appendTarget();
        bindCanvasTooltip(overlay, sessions, TextTip, selection, 'first');
        openTip(selection);

        selection.dispatch('mouseleave');
        textTips()[0].dispatchEvent(new MouseEvent('mouseenter'));
        tick(DELAY * 2);
        expect(textTips().length).toBe(1);

        textTips()[0].dispatchEvent(new MouseEvent('mouseleave'));
        expect(textTips().length).toBe(0);
    }));

    it('closes an open tooltip and removes the listeners when unbound', fakeAsync(() => {
        const selection = appendTarget();
        bindCanvasTooltip(overlay, sessions, TextTip, selection, 'first');
        openTip(selection);

        unbindCanvasTooltip(sessions, selection);
        expect(textTips().length).toBe(0);

        openTip(selection);
        expect(textTips().length).toBe(0);
    }));

    it('cancels a pending open when unbound', fakeAsync(() => {
        const selection = appendTarget();
        bindCanvasTooltip(overlay, sessions, TextTip, selection, 'first');

        selection.dispatch('mouseenter');
        unbindCanvasTooltip(sessions, selection);
        tick(DELAY * 2);

        expect(textTips().length).toBe(0);
    }));

    it('closes the open tooltip when rebound with a different tooltip type', fakeAsync(() => {
        const selection = appendTarget();
        bindCanvasTooltip(overlay, sessions, TextTip, selection, 'first');
        openTip(selection);

        bindCanvasTooltip(overlay, sessions, OtherTip, selection, 'second');
        expect(textTips().length).toBe(0);

        selection.dispatch('mouseleave');
        openTip(selection);
        expect(document.querySelector('other-tip')?.textContent).toContain('second');

        unbindCanvasTooltip(sessions, selection);
    }));

    it('keeps a separate tooltip for each element in the selection', fakeAsync(() => {
        appendTarget();
        appendTarget();
        const selection = d3.select(host).selectAll<HTMLDivElement, unknown>('div');
        bindCanvasTooltip(overlay, sessions, TextTip, selection, 'shared');

        openTip(d3.select(selection.nodes()[0]));
        openTip(d3.select(selection.nodes()[1]));
        expect(textTips().length).toBe(2);

        d3.select(selection.nodes()[0]).dispatch('mouseleave');
        tick(DELAY);
        expect(textTips().length).toBe(1);

        unbindCanvasTooltip(sessions, selection);
    }));
});
