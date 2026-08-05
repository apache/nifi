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

import { Component } from '@angular/core';
import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { Overlay } from '@angular/cdk/overlay';
import { Subject } from 'rxjs';
import { NifiTooltipDirective } from './nifi-tooltip.directive';
import { NiFiCommon } from '../services/nifi-common.service';

@Component({
    selector: 'test-tip',
    standalone: true,
    template: '<div class="tooltip">tip</div>'
})
class TestTipComponent {
    data: unknown;
}

interface MockOverlayRef {
    overlayElement: HTMLElement;
    attach: ReturnType<typeof vi.fn>;
    detach: ReturnType<typeof vi.fn>;
    dispose: ReturnType<typeof vi.fn>;
    hasAttached: ReturnType<typeof vi.fn>;
    detachments: ReturnType<typeof vi.fn>;
}

@Component({
    standalone: true,
    imports: [NifiTooltipDirective],
    template: `
        <div id="a" nifiTooltip [tooltipComponentType]="tip" [delayOpen]="false" [delayClose]="true"></div>
        <div id="b" nifiTooltip [tooltipComponentType]="tip" [delayOpen]="false" [delayClose]="true"></div>
    `
})
class HostComponent {
    tip = TestTipComponent;
}

@Component({
    standalone: true,
    imports: [NifiTooltipDirective],
    template: `<div id="c" nifiTooltip [tooltipComponentType]="tip" [delayOpen]="true"></div>`
})
class DelayedHostComponent {
    tip = TestTipComponent;
}

interface SetupResult {
    fixture: ComponentFixture<HostComponent>;
    elA: HTMLElement;
    elB: HTMLElement;
    overlayRefs: MockOverlayRef[];
}

interface DelayedSetupResult {
    fixture: ComponentFixture<DelayedHostComponent>;
    elC: HTMLElement;
    overlayRefs: MockOverlayRef[];
}

function createMockOverlayRef(): MockOverlayRef {
    const detachments = new Subject<void>();
    let attached = false;

    const overlayElement = document.createElement('div');
    vi.spyOn(overlayElement, 'matches').mockReturnValue(false);

    return {
        overlayElement,
        attach: vi.fn(() => {
            attached = true;
            return { setInput: vi.fn(), location: { nativeElement: document.createElement('div') } };
        }),
        detach: vi.fn(() => {
            if (attached) {
                attached = false;
                detachments.next();
            }
        }),
        dispose: vi.fn(() => detachments.complete()),
        hasAttached: vi.fn(() => attached),
        detachments: vi.fn(() => detachments.asObservable())
    };
}

function createMockOverlay(overlayRefs: MockOverlayRef[]) {
    const positionStrategy = {
        flexibleConnectedTo: vi.fn().mockReturnThis(),
        withPositions: vi.fn().mockReturnThis(),
        withPush: vi.fn().mockReturnValue({ detach: vi.fn(), dispose: vi.fn() })
    };

    return {
        create: vi.fn(() => {
            const ref = createMockOverlayRef();
            overlayRefs.push(ref);
            return ref;
        }),
        position: vi.fn(() => positionStrategy)
    };
}

async function setup(): Promise<SetupResult> {
    const overlayRefs: MockOverlayRef[] = [];
    const mockOverlay = createMockOverlay(overlayRefs);

    await TestBed.configureTestingModule({
        imports: [HostComponent],
        providers: [{ provide: Overlay, useValue: mockOverlay }]
    }).compileComponents();

    const fixture = TestBed.createComponent(HostComponent);
    fixture.detectChanges();

    const elA = fixture.nativeElement.querySelector('#a') as HTMLElement;
    const elB = fixture.nativeElement.querySelector('#b') as HTMLElement;

    return { fixture, elA, elB, overlayRefs };
}

async function setupDelayed(): Promise<DelayedSetupResult> {
    const overlayRefs: MockOverlayRef[] = [];
    const mockOverlay = createMockOverlay(overlayRefs);

    await TestBed.configureTestingModule({
        imports: [DelayedHostComponent],
        providers: [{ provide: Overlay, useValue: mockOverlay }]
    }).compileComponents();

    const fixture = TestBed.createComponent(DelayedHostComponent);
    fixture.detectChanges();

    const elC = fixture.nativeElement.querySelector('#c') as HTMLElement;

    return { fixture, elC, overlayRefs };
}

describe('NifiTooltipDirective', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    afterEach(() => {
        // Reset the shared single-open mutex so the invariant can't leak across tests.
        (NifiTooltipDirective as unknown as { openInstance: unknown }).openInstance = null;
    });

    it('attaches a tooltip overlay on hover', () => {
        return setup().then(({ elA, overlayRefs }) => {
            elA.dispatchEvent(new MouseEvent('mouseenter'));

            expect(overlayRefs).toHaveLength(1);
            expect(overlayRefs[0].attach).toHaveBeenCalledTimes(1);
        });
    });

    it('closes the previously open tooltip when a second tooltip opens', () => {
        return setup().then(({ elA, elB, overlayRefs }) => {
            elA.dispatchEvent(new MouseEvent('mouseenter'));
            elB.dispatchEvent(new MouseEvent('mouseenter'));

            expect(overlayRefs).toHaveLength(2);
            expect(overlayRefs[0].detach).toHaveBeenCalled();
            expect(overlayRefs[1].attach).toHaveBeenCalledTimes(1);
        });
    });

    it('closes a tooltip the pointer had entered when another tooltip opens', () => {
        return setup().then(({ elA, elB, overlayRefs }) => {
            elA.dispatchEvent(new MouseEvent('mouseenter'));

            // move the pointer onto the first tooltip; in the past this pinned it open
            overlayRefs[0].overlayElement.dispatchEvent(new MouseEvent('mouseenter'));

            // opening a second tooltip must still close the first
            elB.dispatchEvent(new MouseEvent('mouseenter'));

            expect(overlayRefs[0].detach).toHaveBeenCalled();
        });
    });

    it('detaches after the close delay when the pointer is not over the tooltip', fakeAsync(async () => {
        const { elA, overlayRefs } = await setup();

        vi.spyOn(elA, 'matches').mockReturnValue(false);

        elA.dispatchEvent(new MouseEvent('mouseenter'));
        elA.dispatchEvent(new MouseEvent('mouseleave'));

        expect(overlayRefs[0].detach).not.toHaveBeenCalled();

        tick(NiFiCommon.TOOLTIP_DELAY_CLOSE_MILLIS);

        expect(overlayRefs[0].detach).toHaveBeenCalled();
    }));

    it('keeps the tooltip open while the pointer is over it, then closes on tooltip mouseleave', fakeAsync(async () => {
        const { elA, overlayRefs } = await setup();

        vi.spyOn(elA, 'matches').mockReturnValue(false);

        elA.dispatchEvent(new MouseEvent('mouseenter'));
        elA.dispatchEvent(new MouseEvent('mouseleave'));

        // the pointer is over the tooltip overlay when the close watchdog fires
        vi.mocked(overlayRefs[0].overlayElement.matches).mockReturnValue(true);
        tick(NiFiCommon.TOOLTIP_DELAY_CLOSE_MILLIS + 50);

        expect(overlayRefs[0].detach).not.toHaveBeenCalled();

        // leaving the tooltip overlay closes it
        overlayRefs[0].overlayElement.dispatchEvent(new MouseEvent('mouseleave'));

        expect(overlayRefs[0].detach).toHaveBeenCalled();
    }));

    it('self-heals and closes when the tooltip mouseleave is dropped after bridging onto it', fakeAsync(async () => {
        const { elA, overlayRefs } = await setup();

        vi.spyOn(elA, 'matches').mockReturnValue(false);

        elA.dispatchEvent(new MouseEvent('mouseenter'));
        elA.dispatchEvent(new MouseEvent('mouseleave'));

        // pointer bridges onto the tooltip; the watchdog reschedules rather than closing
        vi.mocked(overlayRefs[0].overlayElement.matches).mockReturnValue(true);
        tick(NiFiCommon.TOOLTIP_DELAY_CLOSE_MILLIS);

        expect(overlayRefs[0].detach).not.toHaveBeenCalled();

        // the pointer leaves the tooltip but its mouseleave is never delivered; the
        // next watchdog tick must still detach it
        vi.mocked(overlayRefs[0].overlayElement.matches).mockReturnValue(false);
        tick(NiFiCommon.TOOLTIP_DELAY_CLOSE_MILLIS);

        expect(overlayRefs[0].detach).toHaveBeenCalled();
    }));

    it('cancels a pending open when the pointer leaves before the tooltip attaches', fakeAsync(async () => {
        const { elC, overlayRefs } = await setupDelayed();

        elC.dispatchEvent(new MouseEvent('mouseenter'));
        elC.dispatchEvent(new MouseEvent('mouseleave'));

        tick(NiFiCommon.TOOLTIP_DELAY_OPEN_MILLIS + 50);

        expect(overlayRefs).toHaveLength(0);
    }));

    it('does not open after the delay when the pointer is no longer over the trigger', fakeAsync(async () => {
        const { elC, overlayRefs } = await setupDelayed();

        // simulate a quick pass-through whose mouseleave was dropped: the open timer
        // still fires, but the pointer is no longer over the trigger
        vi.spyOn(elC, 'matches').mockReturnValue(false);

        elC.dispatchEvent(new MouseEvent('mouseenter'));
        tick(NiFiCommon.TOOLTIP_DELAY_OPEN_MILLIS + 50);

        expect(overlayRefs).toHaveLength(0);
    }));

    it('opens after the delay when the pointer remains over the trigger', fakeAsync(async () => {
        const { elC, overlayRefs } = await setupDelayed();

        vi.spyOn(elC, 'matches').mockReturnValue(true);

        elC.dispatchEvent(new MouseEvent('mouseenter'));
        tick(NiFiCommon.TOOLTIP_DELAY_OPEN_MILLIS + 50);

        expect(overlayRefs).toHaveLength(1);
        expect(overlayRefs[0].attach).toHaveBeenCalledTimes(1);
    }));
});
