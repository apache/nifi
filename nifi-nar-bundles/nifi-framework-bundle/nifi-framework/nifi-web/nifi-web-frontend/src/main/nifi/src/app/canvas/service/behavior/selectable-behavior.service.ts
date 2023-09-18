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
import * as d3 from 'd3';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../state';
import { addSelectedComponents, removeSelectedComponents, selectComponents } from '../../state/flow/flow.actions';
import { SelectedComponent } from '../../state/flow';

@Injectable({
    providedIn: 'root'
})
export class SelectableBehavior {
    constructor(private store: Store<CanvasState>) {}

    public select(event: MouseEvent, g: any): void {
        const components: SelectedComponent[] = g.data().map(function (d: any) {
            return {
                id: d.id,
                componentType: d.type
            };
        });

        if (!g.classed('selected')) {
            if (event.shiftKey) {
                this.store.dispatch(
                    addSelectedComponents({
                        request: {
                            components
                        }
                    })
                );
            } else {
                this.store.dispatch(
                    selectComponents({
                        request: {
                            components
                        }
                    })
                );
            }
        } else {
            if (event.shiftKey) {
                this.store.dispatch(
                    removeSelectedComponents({
                        request: {
                            components
                        }
                    })
                );
            }
        }

        event.stopPropagation();
    }

    public activate(components: any): void {
        const self = this;

        components.on('mousedown.selection', function (this: any, event: MouseEvent) {
            self.select(event, d3.select(this));
        });
    }
}
