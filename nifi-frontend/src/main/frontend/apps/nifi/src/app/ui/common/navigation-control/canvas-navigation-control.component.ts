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

import { Component, inject, input, OnInit, output } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatExpansionPanel, MatExpansionPanelHeader, MatExpansionPanelTitle } from '@angular/material/expansion';
import { MatTooltip } from '@angular/material/tooltip';
import { Storage } from '@nifi/shared';
import { CanvasBirdseyeComponent } from '../birdseye/birdseye.component';
import { BirdseyeComponentData, BirdseyeTransform } from '../birdseye/birdseye.types';
import { Dimension, Position } from '../canvas/canvas.types';

@Component({
    selector: 'canvas-navigation-control',
    standalone: true,
    imports: [
        CanvasBirdseyeComponent,
        MatButtonModule,
        MatExpansionPanel,
        MatExpansionPanelHeader,
        MatExpansionPanelTitle,
        MatTooltip
    ],
    templateUrl: './canvas-navigation-control.component.html',
    styleUrls: ['./canvas-navigation-control.component.scss']
})
export class CanvasNavigationControl implements OnInit {
    private storage = inject(Storage);

    private static readonly CONTROL_VISIBILITY_KEY = 'graph-control-visibility';

    /**
     * Unique key for persisting the collapsed state in localStorage. Each consumer should
     * provide a distinct key to avoid state collisions when multiple instances of this
     * component exist across different pages.
     */
    storageKey = input<string>('canvas-navigation-control');

    navigationCollapsed = false;

    birdseyeComponents = input.required<BirdseyeComponentData[]>();
    birdseyeTransform = input.required<BirdseyeTransform>();
    canvasDimensions = input.required<Dimension>();
    canNavigateToParent = input<boolean>(false);

    viewportChange = output<Position>();
    birdseyeDragStart = output<void>();
    birdseyeDragEnd = output<void>();

    zoomIn = output<void>();
    zoomOut = output<void>();
    zoomFit = output<void>();
    zoomActual = output<void>();
    leaveGroup = output<void>();

    ngOnInit(): void {
        try {
            const item: { [key: string]: boolean } | null = this.storage.getItem(
                CanvasNavigationControl.CONTROL_VISIBILITY_KEY
            );
            if (item) {
                this.navigationCollapsed = item[this.storageKey()] === false;
            }
        } catch (_e) {
            // likely could not parse item... ignoring
        }
    }

    opened(): void {
        this.toggleCollapsed(false);
    }

    toggleCollapsed(collapsed: boolean): void {
        this.navigationCollapsed = collapsed;

        let item: { [key: string]: boolean } | null = this.storage.getItem(
            CanvasNavigationControl.CONTROL_VISIBILITY_KEY
        );
        if (item == null) {
            item = {};
        }

        item[this.storageKey()] = !this.navigationCollapsed;
        this.storage.setItem(CanvasNavigationControl.CONTROL_VISIBILITY_KEY, item);
    }
}
