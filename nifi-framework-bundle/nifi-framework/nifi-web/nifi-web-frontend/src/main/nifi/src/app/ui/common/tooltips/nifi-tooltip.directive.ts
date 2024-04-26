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

@Directive({
    selector: '[nifiTooltip]',
    standalone: true
})
export class NifiTooltipDirective<T> implements OnDestroy {
    @Input() tooltipComponentType!: Type<T>;
    @Input() tooltipInputData: any;
    @Input() position: ConnectedPosition | undefined;
    @Input() delayClose = true;

    private closeTimer = -1;
    private overlayRef: OverlayRef | null = null;

    constructor(
        private element: ElementRef<HTMLElement>,
        private overlay: Overlay
    ) {}

    @HostListener('mouseenter')
    mouseEnter() {
        if (!this.overlayRef?.hasAttached()) {
            this.attach();
        }
    }

    @HostListener('mouseleave')
    mouseLeave() {
        if (this.overlayRef?.hasAttached()) {
            if (this.delayClose) {
                this.closeTimer = window.setTimeout(() => {
                    this.overlayRef?.detach();
                    this.closeTimer = -1;
                }, 400);
            } else {
                this.overlayRef?.detach();
            }
        }
    }

    ngOnDestroy(): void {
        this.overlayRef?.dispose();
    }

    private attach(): void {
        if (!this.overlayRef) {
            const positionStrategy = this.getPositionStrategy();
            this.overlayRef = this.overlay.create({ positionStrategy });
        }

        const tooltipReference = this.overlayRef.attach(new ComponentPortal(this.tooltipComponentType));
        tooltipReference.setInput('data', this.tooltipInputData);

        // register mouse events
        tooltipReference.location.nativeElement.addEventListener('mouseenter', () => {
            if (this.closeTimer > 0) {
                window.clearTimeout(this.closeTimer);
                this.closeTimer = -1;
            }
        });
        tooltipReference.location.nativeElement.addEventListener('mouseleave', () => {
            this.overlayRef?.detach();
            this.closeTimer = -1;
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
