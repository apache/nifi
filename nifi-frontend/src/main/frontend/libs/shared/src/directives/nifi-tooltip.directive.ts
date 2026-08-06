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

import { Directive, ElementRef, HostListener, Input, OnDestroy, Type, inject } from '@angular/core';
import { ConnectedPosition, Overlay, OverlayRef, PositionStrategy } from '@angular/cdk/overlay';
import { ComponentPortal } from '@angular/cdk/portal';
import { Subscription } from 'rxjs';
import { NiFiCommon } from '../services/nifi-common.service';

@Directive({
    selector: '[nifiTooltip]',
    standalone: true
})
export class NifiTooltipDirective<T> implements OnDestroy {
    /**
     * Only one tooltip should ever be visible at a time. Tracking the currently
     * attached instance lets a newly opened tooltip close any previous one, which
     * prevents overlapping overlays from stealing each other's mouse events and
     * getting stuck open.
     */
    private static openInstance: NifiTooltipDirective<unknown> | null = null;

    private element = inject<ElementRef<HTMLElement>>(ElementRef);
    private overlay = inject(Overlay);

    @Input() tooltipComponentType!: Type<T>;
    @Input() tooltipDisabled = false;
    @Input() tooltipInputData: any;
    @Input() position: ConnectedPosition | undefined;
    @Input() delayClose = true;
    @Input() delayOpen = true;

    private closeTimer = -1;
    private overlayRef: OverlayRef | null = null;
    private positionStrategy: PositionStrategy | null = null;
    private openTimer = -1;
    private detachmentsSubscription: Subscription | null = null;

    @HostListener('mouseenter')
    mouseEnter() {
        if (this.delayOpen) {
            this.openTimer = window.setTimeout(() => {
                this.openTimer = -1;

                // Only open if the pointer is genuinely still over the trigger. A quick
                // pass-through whose mouseleave was dropped or coalesced must not open the
                // tooltip once the delay elapses.
                if (this.isPointerOverTrigger() && !this.overlayRef?.hasAttached()) {
                    this.attach();
                }
            }, NiFiCommon.TOOLTIP_DELAY_OPEN_MILLIS);
        } else {
            if (!this.overlayRef?.hasAttached()) {
                this.attach();
            }
        }
    }

    @HostListener('mousemove')
    mouseMove() {
        if (this.overlayRef?.hasAttached() && this.tooltipDisabled) {
            this.detachTip();
        }
    }

    @HostListener('mouseup')
    mouseup() {
        if (!this.overlayRef?.hasAttached()) {
            this.attach();
        }
    }

    @HostListener('mouseleave')
    mouseLeave() {
        this.closeTip();
    }

    @HostListener('click')
    click() {
        this.closeTip();
    }

    private closeTip(): void {
        // cancel any pending open so a quick hover in-and-out never opens the tooltip
        if (this.openTimer > 0) {
            window.clearTimeout(this.openTimer);
            this.openTimer = -1;
        }

        if (!this.overlayRef?.hasAttached()) {
            return;
        }

        if (this.delayClose) {
            this.scheduleClose();
        } else {
            this.detachTip();
        }
    }

    /**
     * Schedule a close check. When the timer fires, close only if the pointer is over
     * neither the trigger nor the tooltip; otherwise reschedule. Re-checking against
     * the live `:hover` state (rather than cancelling the timer when the pointer
     * bridges onto the tooltip) keeps the "move onto the tooltip to scroll it"
     * behavior working while guaranteeing that a dropped or coalesced mouseleave on
     * either element can never leave the tooltip stuck open.
     */
    private scheduleClose(): void {
        if (this.closeTimer > 0) {
            window.clearTimeout(this.closeTimer);
        }

        this.closeTimer = window.setTimeout(() => {
            this.closeTimer = -1;

            if (this.isPointerOverTrigger() || this.isPointerOverTip()) {
                this.scheduleClose();
            } else {
                this.detachTip();
            }
        }, NiFiCommon.TOOLTIP_DELAY_CLOSE_MILLIS);
    }

    ngOnDestroy(): void {
        if (this.openTimer > 0) {
            window.clearTimeout(this.openTimer);
            this.openTimer = -1;
        }

        if (this.closeTimer > 0) {
            window.clearTimeout(this.closeTimer);
            this.closeTimer = -1;
        }

        if (NifiTooltipDirective.openInstance === (this as NifiTooltipDirective<unknown>)) {
            NifiTooltipDirective.openInstance = null;
        }

        this.detachmentsSubscription?.unsubscribe();
        this.overlayRef?.dispose();
        this.positionStrategy?.dispose();
    }

    private attach(): void {
        if (this.tooltipDisabled) {
            return;
        }

        // enforce a single visible tooltip across the application
        const currentlyOpen = NifiTooltipDirective.openInstance;
        if (currentlyOpen && currentlyOpen !== (this as NifiTooltipDirective<unknown>)) {
            currentlyOpen.detachTip();
        }

        if (!this.overlayRef) {
            this.positionStrategy = this.getPositionStrategy();
            this.overlayRef = this.overlay.create({ positionStrategy: this.positionStrategy });

            // Reset transient state whenever the overlay detaches for any reason so
            // a missed overlay mouseleave can't leave the instance in a state that
            // permanently blocks closing.
            this.detachmentsSubscription = this.overlayRef.detachments().subscribe(() => {
                if (this.closeTimer > 0) {
                    window.clearTimeout(this.closeTimer);
                    this.closeTimer = -1;
                }

                if (NifiTooltipDirective.openInstance === (this as NifiTooltipDirective<unknown>)) {
                    NifiTooltipDirective.openInstance = null;
                }
            });

            // Leaving the tooltip closes it immediately. This is only a fast path;
            // the scheduleClose() watchdog is the authoritative closer and self-heals
            // if this mouseleave is ever dropped or coalesced by the browser.
            this.overlayRef.overlayElement.addEventListener('mouseleave', () => {
                this.detachTip();
            });
        }

        const tooltipReference = this.overlayRef.attach(new ComponentPortal(this.tooltipComponentType));
        tooltipReference.setInput('data', this.tooltipInputData);

        NifiTooltipDirective.openInstance = this as NifiTooltipDirective<unknown>;
    }

    private detachTip(): void {
        if (this.closeTimer > 0) {
            window.clearTimeout(this.closeTimer);
            this.closeTimer = -1;
        }

        if (this.overlayRef?.hasAttached()) {
            this.overlayRef.detach();
        }

        if (this.positionStrategy?.detach) {
            this.positionStrategy.detach();
        }
    }

    private isPointerOverTrigger(): boolean {
        return this.element.nativeElement.matches(':hover');
    }

    private isPointerOverTip(): boolean {
        const overlayElement = this.overlayRef?.overlayElement;
        return !!overlayElement && overlayElement.matches(':hover');
    }

    private getPositionStrategy(): PositionStrategy {
        return this.overlay
            .position()
            .flexibleConnectedTo(this.element)
            .withPositions([
                this.position
                    ? this.position
                    : {
                          originX: 'end',
                          originY: 'bottom',
                          overlayX: 'start',
                          overlayY: 'top',
                          offsetX: 8,
                          offsetY: 8
                      }
            ])
            .withPush(true);
    }
}
