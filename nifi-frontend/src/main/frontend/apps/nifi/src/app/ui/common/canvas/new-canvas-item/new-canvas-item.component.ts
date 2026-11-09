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

import { Component, EventEmitter, Input, Output } from '@angular/core';
import { CdkDrag, CdkDragEnd } from '@angular/cdk/drag-drop';
import { NifiTooltipDirective, TextTip } from '@nifi/shared';
import { ConnectedPosition } from '@angular/cdk/overlay';

/**
 * A draggable palette chip that callers wire to a canvas. The chip is
 * intentionally agnostic about state management: it emits dragStarted /
 * dragEnded events and lets the host (flow-designer header, connector
 * canvas, etc.) decide how to translate them into actions.
 */
@Component({
    selector: 'new-canvas-item',
    templateUrl: './new-canvas-item.component.html',
    imports: [CdkDrag, NifiTooltipDirective],
    styleUrls: ['./new-canvas-item.component.scss']
})
export class NewCanvasItem {
    @Input() iconClass = '';
    @Input() iconHoverClass = '';
    @Input() disabled = false;
    @Input() tooltip = '';

    /**
     * When true the chip suppresses hover styling while any palette item is
     * being dragged.
     */
    @Input() dragging = false;

    @Output() dragStarted = new EventEmitter<void>();
    @Output() dragEnded = new EventEmitter<CdkDragEnd>();

    protected readonly TextTip = TextTip;

    tooltipPosition: ConnectedPosition = {
        originX: 'center',
        originY: 'bottom',
        overlayX: 'center',
        overlayY: 'top',
        offsetY: 8
    };

    private hovering = false;

    mouseEnter() {
        if (!this.disabled) {
            this.hovering = true;
        }
    }

    mouseLeave() {
        if (!this.disabled) {
            this.hovering = false;
        }
    }

    isHovering(): boolean {
        return this.hovering && !this.dragging;
    }

    onDragStarted(): void {
        this.dragStarted.emit();
    }

    onDragEnded(event: CdkDragEnd): void {
        this.dragEnded.emit(event);
        event.source._dragRef.reset();
    }
}
