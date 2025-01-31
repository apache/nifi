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
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../state';
import { zoomActual, zoomFit, zoomIn, zoomOut } from '../../../../state/transform/transform.actions';
import { leaveProcessGroup, setNavigationCollapsed } from '../../../../state/flow/flow.actions';
import { CanvasUtils } from '../../../../service/canvas-utils.service';
import { initialState } from '../../../../state/flow/flow.reducer';
import { Storage } from '@nifi/shared';

import { Birdseye } from './birdseye/birdseye.component';
import { MatButtonModule } from '@angular/material/button';

@Component({
    selector: 'navigation-control',
    templateUrl: './navigation-control.component.html',
    imports: [Birdseye, MatButtonModule],
    styleUrls: ['./navigation-control.component.scss']
})
export class NavigationControl {
    private static readonly CONTROL_VISIBILITY_KEY: string = 'graph-control-visibility';
    private static readonly NAVIGATION_KEY: string = 'navigation-control';

    @Input() shouldDockWhenCollapsed!: boolean;

    navigationCollapsed: boolean = initialState.navigationCollapsed;

    constructor(
        private store: Store<CanvasState>,
        private canvasUtils: CanvasUtils,
        private storage: Storage
    ) {
        try {
            const item: { [key: string]: boolean } | null = this.storage.getItem(
                NavigationControl.CONTROL_VISIBILITY_KEY
            );
            if (item) {
                this.navigationCollapsed = item[NavigationControl.NAVIGATION_KEY] === false;
                this.store.dispatch(setNavigationCollapsed({ navigationCollapsed: this.navigationCollapsed }));
            }
        } catch (e) {
            // likely could not parse item... ignoring
        }
    }

    toggleCollapsed(): void {
        this.navigationCollapsed = !this.navigationCollapsed;
        this.store.dispatch(setNavigationCollapsed({ navigationCollapsed: this.navigationCollapsed }));

        // update the current value in storage
        let item: { [key: string]: boolean } | null = this.storage.getItem(NavigationControl.CONTROL_VISIBILITY_KEY);
        if (item == null) {
            item = {};
        }

        item[NavigationControl.NAVIGATION_KEY] = !this.navigationCollapsed;
        this.storage.setItem(NavigationControl.CONTROL_VISIBILITY_KEY, item);
    }

    zoomIn(): void {
        this.store.dispatch(zoomIn());
    }

    zoomOut(): void {
        this.store.dispatch(zoomOut());
    }

    zoomFit(): void {
        this.store.dispatch(zoomFit({ transition: true }));
    }

    zoomActual(): void {
        this.store.dispatch(zoomActual());
    }

    isNotRootGroup(): boolean {
        return this.canvasUtils.isNotRootGroup();
    }

    leaveProcessGroup(): void {
        this.store.dispatch(leaveProcessGroup());
    }
}
