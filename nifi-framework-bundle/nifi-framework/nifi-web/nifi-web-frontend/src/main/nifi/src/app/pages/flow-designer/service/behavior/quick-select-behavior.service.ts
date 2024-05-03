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

import { Injectable } from '@angular/core';
import { CanvasUtils } from '../canvas-utils.service';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../state';
import { navigateToEditComponent } from '../../state/flow/flow.actions';

@Injectable({
    providedIn: 'root'
})
export class QuickSelectBehavior {
    constructor(
        private canvasUtils: CanvasUtils,
        private store: Store<CanvasState>
    ) {}

    /**
     * Attempts to show configuration or details dialog for the specified slection.
     */
    private quickSelect(event: MouseEvent) {
        const selection: any = this.canvasUtils.getSelection();
        const selectionData: any = selection.datum();

        if (this.canvasUtils.isConfigurable(selection) || this.canvasUtils.hasDetails(selection)) {
            // show configuration dialog
            this.store.dispatch(
                navigateToEditComponent({
                    request: {
                        type: selectionData.type,
                        id: selectionData.id
                    }
                })
            );
        }

        // stop propagation and prevent default
        event.preventDefault();
        event.stopPropagation();
    }

    public activate(components: any): void {
        const self: QuickSelectBehavior = this;
        components.on('dblclick', function (event: MouseEvent) {
            self.quickSelect(event);
        });
    }
}
