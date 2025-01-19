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

import { Directive, ElementRef, HostListener, Input, OnDestroy, Type } from '@angular/core';
import { ConnectedPosition, Overlay, OverlayRef, PositionStrategy } from '@angular/cdk/overlay';
import { ComponentPortal } from '@angular/cdk/portal';
import { NiFiCommon } from '../services/nifi-common.service';

@Directive({
    selector: '[nifiTooltip]',
    standalone: true
})
export class NifiTooltipDirective<T> implements OnDestroy {
    @Input() tooltipComponentType!: Type<T>;
    @Input() tooltipDisabled = false;
    @Input() tooltipInputData: any;
    @Input() position: ConnectedPosition | undefined;
    @Input() delayClose = true;
    @Input() delayOpen = true;

    private closeTimer = -1;
    private overlayRef: OverlayRef | null = null;
    private positionStrategy: PositionStrategy | null = null;
    private overTip = false;
    private openTimer = -1;

    constructor(
        private element: ElementRef<HTMLElement>,
        private overlay: Overlay
    ) {}

    @HostListener('mouseenter')
    mouseEnter() {
        if (this.delayOpen) {
            this.openTimer = window.setTimeout(() => {
                if (!this.overlayRef?.hasAttached()) {
                    this.attach();
                }
                this.openTimer = -1;
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
            this.overlayRef?.detach();

            if (this.positionStrategy?.detach) {
                this.positionStrategy.detach();
            }
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
        if (this.overlayRef?.hasAttached() && !this.overTip) {
            if (this.delayClose) {
                this.closeTimer = window.setTimeout(() => {
                    this.overlayRef?.detach();

                    if (this.positionStrategy?.detach) {
                        this.positionStrategy.detach();
                    }

                    this.closeTimer = -1;
                }, NiFiCommon.TOOLTIP_DELAY_CLOSE_MILLIS);
            } else {
                this.overlayRef?.detach();

                if (this.positionStrategy?.detach) {
                    this.positionStrategy.detach();
                }
            }
        }

        if (this.openTimer > 0) {
            window.clearTimeout(this.openTimer);
            this.openTimer = -1;
        }
    }

    ngOnDestroy(): void {
        this.overlayRef?.dispose();
        this.positionStrategy?.dispose();
    }

    private attach(): void {
        if (this.tooltipDisabled) {
            return;
        }

        if (!this.overlayRef) {
            this.positionStrategy = this.getPositionStrategy();
            this.overlayRef = this.overlay.create({ positionStrategy: this.positionStrategy });
        }

        const tooltipReference = this.overlayRef.attach(new ComponentPortal(this.tooltipComponentType));
        tooltipReference.setInput('data', this.tooltipInputData);

        // register mouse events
        tooltipReference.location.nativeElement.addEventListener('mouseenter', () => {
            if (this.closeTimer > 0) {
                window.clearTimeout(this.closeTimer);
                this.closeTimer = -1;
            }

            this.overTip = true;
        });
        tooltipReference.location.nativeElement.addEventListener('mouseleave', () => {
            this.overlayRef?.detach();

            if (this.positionStrategy?.detach) {
                this.positionStrategy.detach();
            }

            this.closeTimer = -1;
            this.overTip = false;
        });
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
