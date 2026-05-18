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

import { Component, ElementRef, input, viewChild } from '@angular/core';

/**
 * Layout container with a scrollable main content area and an optional collapsible side panel.
 * Projects content via named slots: mainContent, dockedContent, and panelContent.
 */
@Component({
    selector: 'shared-side-panel-container',
    standalone: true,
    template: `
        <div class="side-panel-container flex h-full">
            <div class="flex-1 min-w-0 overflow-y-auto" #scrollContainer>
                <ng-content select="[mainContent]"></ng-content>
            </div>
            @if (showDockedStrip() && !isOpen()) {
                <div class="docked-strip border-l flex flex-col items-center">
                    <ng-content select="[dockedContent]"></ng-content>
                </div>
            }
            @if (isOpen()) {
                <div class="side-panel border-l overflow-y-auto" [style.width]="panelWidth()">
                    <ng-content select="[panelContent]"></ng-content>
                </div>
            }
        </div>
    `,
    styles: [
        `
            :host {
                display: block;
                height: 100%;
            }
            .docked-strip {
                width: 48px;
            }
        `
    ],
    host: {
        class: 'block h-full'
    }
})
export class SidePanelContainerComponent {
    readonly isOpen = input(false);
    readonly showDockedStrip = input(false);
    readonly panelWidth = input('400px');

    private scrollContainer = viewChild<ElementRef>('scrollContainer');

    scrollToTop(): void {
        this.scrollContainer()?.nativeElement?.scrollTo({ top: 0, behavior: 'smooth' });
    }
}
