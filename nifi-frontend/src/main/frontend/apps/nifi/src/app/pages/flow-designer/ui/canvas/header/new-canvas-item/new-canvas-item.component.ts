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

import { Component, Input } from '@angular/core';
import { CdkDrag, CdkDragEnd } from '@angular/cdk/drag-drop';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../state';
import { createComponentRequest, setDragging } from '../../../../state/flow/flow.actions';
import { Client } from '../../../../../../service/client.service';
import { selectDragging } from '../../../../state/flow/flow.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { CanvasView } from '../../../../service/canvas-view.service';
import { ComponentType, NifiTooltipDirective, TextTip } from '@nifi/shared';
import { ConnectedPosition } from '@angular/cdk/overlay';

@Component({
    selector: 'new-canvas-item',
    templateUrl: './new-canvas-item.component.html',
    imports: [CdkDrag, NifiTooltipDirective],
    styleUrls: ['./new-canvas-item.component.scss']
})
export class NewCanvasItem {
    @Input() type!: ComponentType;
    @Input() iconClass = '';
    @Input() iconHoverClass = '';
    @Input() disabled = false;
    @Input() tooltip = '';

    protected readonly TextTip = TextTip;

    tooltipPosition: ConnectedPosition = {
        originX: 'center',
        originY: 'bottom',
        overlayX: 'center',
        overlayY: 'top',
        offsetY: 8
    };

    dragging = false;

    private hovering = false;

    constructor(
        private client: Client,
        private canvasView: CanvasView,
        private store: Store<CanvasState>
    ) {
        this.store
            .select(selectDragging)
            .pipe(takeUntilDestroyed())
            .subscribe((dragging) => {
                this.dragging = dragging;
            });
    }

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
        this.store.dispatch(
            setDragging({
                dragging: true
            })
        );
    }

    onDragEnded(event: CdkDragEnd): void {
        const dropPoint = event.dropPoint;

        const position = this.canvasView.getCanvasPosition(dropPoint);
        if (position) {
            this.store.dispatch(
                createComponentRequest({
                    request: {
                        revision: {
                            clientId: this.client.getClientId(),
                            version: 0
                        },
                        type: this.type,
                        position
                    }
                })
            );
        } else {
            this.store.dispatch(setDragging({ dragging: false }));
        }

        // reset dragging state
        event.source._dragRef.reset();
    }
}
