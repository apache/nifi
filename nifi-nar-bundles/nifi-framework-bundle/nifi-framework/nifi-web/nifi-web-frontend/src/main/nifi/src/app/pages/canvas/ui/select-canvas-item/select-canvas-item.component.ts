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

import { Component } from '@angular/core';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../state';
import {
    selectCurrentProcessGroupId,
    selectSingleSelectedComponent,
    selectSkipTransform
} from '../../state/flow/flow.selectors';
import { filter, switchMap, withLatestFrom } from 'rxjs';
import { centerSelectedComponent, setSkipTransform } from '../../state/flow/flow.actions';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
    selector: 'select-canvas-item',
    template: '<edit-canvas-item></edit-canvas-item>'
})
export class SelectCanvasItem {
    constructor(private store: Store<CanvasState>) {
        this.store
            .select(selectCurrentProcessGroupId)
            .pipe(
                filter((processGroupId) => processGroupId != null),
                switchMap(() => this.store.select(selectSingleSelectedComponent)),
                filter((selectedComponent) => selectedComponent != null),
                withLatestFrom(this.store.select(selectSkipTransform)),
                takeUntilDestroyed()
            )
            .subscribe(([selectedComponent, skipTransform]) => {
                if (skipTransform) {
                    this.store.dispatch(setSkipTransform({ skipTransform: false }));
                } else {
                    this.store.dispatch(centerSelectedComponent());
                }
            });
    }
}
