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

import { Injectable, inject } from '@angular/core';
import { DraggableBehavior } from './draggable-behavior.service';
import { CanvasUtils } from '../canvas-utils.service';
import { ConnectableBehavior } from './connectable-behavior.service';

@Injectable({
    providedIn: 'root'
})
export class EditableBehavior {
    private draggableBehavior = inject(DraggableBehavior);
    private connectableBehavior = inject(ConnectableBehavior);
    private canvasUtils = inject(CanvasUtils);

    public editable(selection: any): void {
        if (this.canvasUtils.canModify(selection)) {
            if (!selection.classed('connectable')) {
                this.connectableBehavior.activate(selection);
            }
            if (!selection.classed('moveable')) {
                this.draggableBehavior.activate(selection);
            }
        } else {
            if (selection.classed('connectable')) {
                this.connectableBehavior.deactivate(selection);
            }
            if (selection.classed('moveable')) {
                this.draggableBehavior.deactivate(selection);
            }
        }
    }
}
