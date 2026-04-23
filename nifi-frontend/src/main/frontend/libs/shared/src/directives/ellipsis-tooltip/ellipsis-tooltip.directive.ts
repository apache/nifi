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

import { AfterViewInit, Directive, ElementRef, Input, NgZone, numberAttribute, OnDestroy, inject } from '@angular/core';
import { MatTooltip, TooltipPosition } from '@angular/material/tooltip';

/**
 * Adds a tooltip with the element's full text content when the rendered content is truncated
 * (horizontal or vertical overflow). The tooltip stays disabled while the text fits.
 *
 * Usage:
 *   <div class="truncate" ellipsisTooltip>Some long text...</div>
 */
@Directive({
    selector: '[ellipsisTooltip]',
    standalone: true,
    hostDirectives: [MatTooltip]
})
export class EllipsisTooltipDirective implements AfterViewInit, OnDestroy {
    private elementRef = inject<ElementRef<HTMLElement>>(ElementRef);
    private matTooltip = inject(MatTooltip, { self: true });
    private zone = inject(NgZone);

    private get hostElement(): HTMLElement & { __ellipsis_ro__?: ResizeObserver } {
        return this.elementRef.nativeElement as HTMLElement & { __ellipsis_ro__?: ResizeObserver };
    }
    private mutationObserver: MutationObserver | null = null;
    private hasScheduledEvaluation = false;
    private lastTooltipMessage: string | null = null;
    private lastTooltipDisabled: boolean | null = null;
    private _tooltipShowDelay = 500;
    private _tooltipHideDelay = 0;
    private tooltipPositionWasSet = false;
    private observersActive = false;

    private readonly onMouseEnter = () => this.activateObservers();
    private readonly onMouseLeave = () => this.deactivateObservers();
    private readonly onFocusIn = () => this.activateObservers();
    private readonly onFocusOut = () => this.deactivateObservers();

    @Input({ transform: numberAttribute })
    set tooltipShowDelay(value: number) {
        this._tooltipShowDelay = typeof value === 'number' ? value : 0;
        this.matTooltip.showDelay = this._tooltipShowDelay;
    }

    @Input({ transform: numberAttribute })
    set tooltipHideDelay(value: number) {
        this._tooltipHideDelay = typeof value === 'number' ? value : 0;
        this.matTooltip.hideDelay = this._tooltipHideDelay;
    }

    @Input()
    set tooltipPosition(value: TooltipPosition | undefined) {
        if (value == null) {
            this.tooltipPositionWasSet = false;
            this.matTooltip.position = 'after';
            return;
        }
        this.tooltipPositionWasSet = true;
        this.matTooltip.position = value as TooltipPosition;
    }

    ngAfterViewInit(): void {
        // Defer the first evaluation to the next microtask to avoid NG0100 (expression-changed-after-checked).
        this.scheduleEvaluateOverflow();

        this.matTooltip.showDelay = this._tooltipShowDelay;
        this.matTooltip.hideDelay = this._tooltipHideDelay;
        if (!this.tooltipPositionWasSet) {
            this.matTooltip.position = 'after';
        }

        // Lazily attach observers on interaction to avoid per-item background observers in large lists.
        const listenerOptions: AddEventListenerOptions = { passive: true, capture: true };
        this.hostElement.addEventListener('mouseenter', this.onMouseEnter, listenerOptions);
        this.hostElement.addEventListener('mouseleave', this.onMouseLeave, listenerOptions);
        this.hostElement.addEventListener('focusin', this.onFocusIn, listenerOptions);
        this.hostElement.addEventListener('focusout', this.onFocusOut, listenerOptions);
    }

    ngOnDestroy(): void {
        this.hostElement.removeEventListener('mouseenter', this.onMouseEnter);
        this.hostElement.removeEventListener('mouseleave', this.onMouseLeave);
        this.hostElement.removeEventListener('focusin', this.onFocusIn);
        this.hostElement.removeEventListener('focusout', this.onFocusOut);

        this.deactivateObservers();
    }

    private evaluateOverflow(): void {
        const element = this.elementRef.nativeElement;
        const isOverflowing = element.offsetWidth < element.scrollWidth || element.offsetHeight < element.scrollHeight;

        if (isOverflowing) {
            const text = (element.textContent || '').trim();
            const nextDisabled = text.length === 0;
            if (this.lastTooltipMessage !== text) {
                this.matTooltip.message = text;
                this.lastTooltipMessage = text;
            }
            if (this.lastTooltipDisabled !== nextDisabled) {
                this.matTooltip.disabled = nextDisabled;
                this.lastTooltipDisabled = nextDisabled;
            }
        } else {
            if (this.lastTooltipMessage !== '') {
                this.matTooltip.message = '';
                this.lastTooltipMessage = '';
            }
            if (this.lastTooltipDisabled !== true) {
                this.matTooltip.disabled = true;
                this.lastTooltipDisabled = true;
            }
        }
    }

    private scheduleEvaluateOverflow(): void {
        if (this.hasScheduledEvaluation) {
            return;
        }
        this.hasScheduledEvaluation = true;
        this.zone.runOutsideAngular(() => {
            Promise.resolve().then(() => {
                this.zone.run(() => {
                    this.hasScheduledEvaluation = false;
                    this.evaluateOverflow();
                });
            });
        });
    }

    private activateObservers(): void {
        if (this.observersActive) {
            return;
        }
        this.observersActive = true;
        this.zone.runOutsideAngular(() => {
            if (typeof window !== 'undefined' && 'MutationObserver' in window && !this.mutationObserver) {
                try {
                    const mo = new MutationObserver(() => this.scheduleEvaluateOverflow());
                    if (mo && typeof mo.observe === 'function') {
                        this.mutationObserver = mo;
                        mo.observe(this.hostElement, {
                            childList: true,
                            characterData: true,
                            subtree: true
                        });
                    }
                } catch {
                    // noop
                }
            }

            if (typeof window !== 'undefined' && 'ResizeObserver' in window && !this.hostElement.__ellipsis_ro__) {
                const ro = new ResizeObserver(() => this.scheduleEvaluateOverflow());
                this.hostElement.__ellipsis_ro__ = ro;
                ro.observe(this.hostElement);
            }
        });
        // Evaluate immediately on activation so the tooltip can show on the first interaction,
        // and schedule a follow-up microtask in case ripple/measurement work reflows the host.
        this.evaluateOverflow();
        this.scheduleEvaluateOverflow();
    }

    private deactivateObservers(): void {
        if (!this.observersActive) {
            return;
        }
        this.observersActive = false;

        if (this.mutationObserver && typeof this.mutationObserver.disconnect === 'function') {
            try {
                this.mutationObserver.disconnect();
            } catch {
                /* noop */
            }
        }
        this.mutationObserver = null;

        const ro: ResizeObserver | undefined = this.hostElement.__ellipsis_ro__;
        if (ro && typeof ro.disconnect === 'function') {
            try {
                ro.disconnect();
            } catch {
                /* noop */
            }
        }
        delete this.hostElement.__ellipsis_ro__;
    }
}
