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

import { ComponentRef, Directive, HostListener, Input, Type, ViewContainerRef } from '@angular/core';

@Directive({
    selector: '[nifiTooltip]',
    standalone: true
})
export class NifiTooltipDirective<T> {
    @Input() tooltipComponentType!: Type<T>;
    @Input() tooltipInputData: any;
    @Input() xOffset: number = 0; // TODO - replace pixel based offset with css transformY to support positioning above/below
    @Input() yOffset: number = 0;
    @Input() delayClose: boolean = true;

    private closeTimer: number = -1;
    private tooltipRef: ComponentRef<T> | undefined;

    constructor(private viewContainerRef: ViewContainerRef) {}

    @HostListener('mouseenter', ['$event'])
    mouseEnter(event: MouseEvent) {
        // @ts-ignore
        const { x, y, width, height } = event.currentTarget.getBoundingClientRect();

        // clear any existing tooltips
        this.viewContainerRef.clear();

        // create and configure the tooltip
        this.tooltipRef = this.viewContainerRef.createComponent(this.tooltipComponentType);
        this.tooltipRef.setInput('top', y + height + 8 + this.yOffset);
        this.tooltipRef.setInput('left', x + width + 8 + this.xOffset);
        this.tooltipRef.setInput('data', this.tooltipInputData);

        // register mouse events
        this.tooltipRef.location.nativeElement.addEventListener('mouseenter', () => {
            if (this.closeTimer > 0) {
                window.clearTimeout(this.closeTimer);
                this.closeTimer = -1;
            }
        });
        this.tooltipRef.location.nativeElement.addEventListener('mouseleave', () => {
            this.tooltipRef?.destroy();
            this.closeTimer = -1;
        });
    }

    @HostListener('mouseleave', ['$event'])
    mouseLeave(event: MouseEvent) {
        if (this.delayClose) {
            this.closeTimer = window.setTimeout(() => {
                this.tooltipRef?.destroy();
                this.closeTimer = -1;
            }, 400);
        } else {
            this.tooltipRef?.destroy();
        }
    }
}
