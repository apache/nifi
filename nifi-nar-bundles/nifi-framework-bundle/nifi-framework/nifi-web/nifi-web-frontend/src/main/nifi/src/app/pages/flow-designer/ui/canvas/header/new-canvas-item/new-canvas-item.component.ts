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

import { Component, Input, OnInit } from '@angular/core';
import { CdkDrag, CdkDragEnd, CdkDragStart } from '@angular/cdk/drag-drop';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../state';
import { INITIAL_SCALE, INITIAL_TRANSLATE } from '../../../../state/transform/transform.reducer';
import { selectTransform } from '../../../../state/transform/transform.selectors';
import { createComponentRequest, setDragging } from '../../../../state/flow/flow.actions';
import { Client } from '../../../../../../service/client.service';
import { selectDragging } from '../../../../state/flow/flow.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Position } from '../../../../state/shared';
import { ComponentType } from '../../../../../../state/shared';

@Component({
    selector: 'new-canvas-item',
    standalone: true,
    templateUrl: './new-canvas-item.component.html',
    imports: [CdkDrag],
    styleUrls: ['./new-canvas-item.component.scss']
})
export class NewCanvasItem implements OnInit {
    // @ts-ignore
    @Input() type: ComponentType;
    @Input() iconClass: string = '';
    @Input() iconHoverClass: string = '';

    dragging: boolean = false;

    private hovering: boolean = false;

    private scale: number = INITIAL_SCALE;
    private translate: Position = INITIAL_TRANSLATE;

    constructor(
        private client: Client,
        private store: Store<CanvasState>
    ) {
        this.store
            .select(selectTransform)
            .pipe(takeUntilDestroyed())
            .subscribe((transform) => {
                this.scale = transform.scale;
                this.translate = transform.translate;
            });

        this.store
            .select(selectDragging)
            .pipe(takeUntilDestroyed())
            .subscribe((dragging) => {
                this.dragging = dragging;
            });
    }

    ngOnInit(): void {}

    mouseEnter() {
        this.hovering = true;
    }

    mouseLeave() {
        this.hovering = false;
    }

    isHovering(): boolean {
        return this.hovering && !this.dragging;
    }

    onDragStarted(event: CdkDragStart): void {
        this.store.dispatch(
            setDragging({
                dragging: true
            })
        );
    }

    onDragEnded(event: CdkDragEnd): void {
        const canvasContainer: any = document.getElementById('canvas-container');
        const rect = canvasContainer.getBoundingClientRect();
        const dropPoint = event.dropPoint;

        // translate the drop point onto the canvas
        const canvasDropPoint = {
            x: dropPoint.x - rect.left,
            y: dropPoint.y - rect.top
        };

        // if the position is over the canvas fire an event to add the new item
        if (
            canvasDropPoint.x >= 0 &&
            canvasDropPoint.x < rect.width &&
            canvasDropPoint.y >= 0 &&
            canvasDropPoint.y < rect.height
        ) {
            // adjust the x and y coordinates accordingly
            const x = canvasDropPoint.x / this.scale - this.translate.x / this.scale;
            const y = canvasDropPoint.y / this.scale - this.translate.y / this.scale;

            this.store.dispatch(
                createComponentRequest({
                    request: {
                        revision: {
                            clientId: this.client.getClientId(),
                            version: 0
                        },
                        type: this.type,
                        position: { x, y }
                    }
                })
            );
        }

        // reset dragging state
        event.source._dragRef.reset();
    }

    /**
     * TODO - Improve drag boundary by computing the drag render position...
     *
     * [cdkDragConstrainPosition]="computeDragRenderPos.bind(this)"
     *
     * @param pos
     * @param dragRef
     */
    // computeDragRenderPos(pos: any, dragRef: any) {
    //     const canvasContainer: any = document.getElementById('canvas-container');
    //     const rect = canvasContainer.getBoundingClientRect();
    //     return {
    //         x: pos.x,
    //         y: pos.y < rect.y ? rect.y : pos.y
    //     }
    // }
}
