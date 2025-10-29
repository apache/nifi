/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Directive, ElementRef, HostListener, Input, NgZone, Renderer2, inject } from '@angular/core';
import { fromEvent, Subscription, switchMap, take, tap } from 'rxjs';

@Directive({
    selector: '[copy]',
    standalone: true
})
export class CopyDirective {
    private elementRef = inject<ElementRef<HTMLElement>>(ElementRef);
    private renderer = inject(Renderer2);
    private zone = inject(NgZone);

    @Input({ required: true }) copy!: string;

    private copyButton: HTMLElement | null = null;
    private subscription: Subscription | null = null;

    isClipboardAvailable(): boolean {
        // system clipboard interaction requires the browser to be in a secured context.
        return window.isSecureContext && Object.hasOwn(window, 'ClipboardItem');
    }

    @HostListener('mouseenter')
    onMouseEnter() {
        if (this.isClipboardAvailable()) {
            this.copyButton = this.renderer.createElement('i');
            if (this.copyButton) {
                const cb: HTMLElement = this.copyButton;
                cb.classList.add('copy-button', 'fa', 'fa-copy', 'ml-2', 'primary-color');

                // run outside the angular zone to prevent unnecessary change detection cycles
                this.subscription = this.zone.runOutsideAngular(() => {
                    return fromEvent<MouseEvent>(cb, 'click')
                        .pipe(
                            tap((event: MouseEvent) => {
                                // prevent copy click from triggering parent click handlers
                                event.stopPropagation();
                                event.preventDefault();
                            }),
                            switchMap(() => navigator.clipboard.writeText(this.copy)),
                            take(1)
                        )
                        .subscribe(() => {
                            cb.classList.remove('copy-button', 'fa-copy');
                            cb.classList.add('copied', 'fa-check', 'success-color-default');
                        });
                });
                this.renderer.appendChild(this.elementRef.nativeElement, this.copyButton);
            }
        }
    }

    @HostListener('mouseleave')
    onMouseLeave() {
        // if the user leaves the element without clicking, the subscription needs closed
        if (this.subscription && !this.subscription.closed) {
            this.subscription.unsubscribe();
        }
        if (this.copyButton) {
            this.renderer?.removeChild(this.elementRef.nativeElement, this.copyButton);
            this.copyButton = null;
        }
    }
}
