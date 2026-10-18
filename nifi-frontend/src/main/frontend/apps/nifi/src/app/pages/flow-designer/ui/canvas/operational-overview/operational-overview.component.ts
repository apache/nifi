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

import { Component, inject, input, output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';
import { Store } from '@ngrx/store';
import { ComponentType } from '@nifi/shared';
import { CanvasState } from '../../../state';
import { selectOperationalOverview } from '../../../state/operational-overview/operational-overview.selectors';
import { selectComponents } from '../../../state/flow/flow.actions';
import { selectClusterSummary } from '../../../../../state/cluster-summary/cluster-summary.selectors';

@Component({
    selector: 'operational-overview',
    imports: [CommonModule, MatButtonModule],
    templateUrl: './operational-overview.component.html',
    styleUrl: './operational-overview.component.scss',
    host: { class: 'block' }
})
export class OperationalOverviewComponent {
    private store = inject<Store<CanvasState>>(Store);
    expanded = input(false);
    modernWorkspace = input(false);
    workspaceChange = output<void>();
    expandedChange = output<boolean>();
    model = this.store.selectSignal(selectOperationalOverview);
    cluster = this.store.selectSignal(selectClusterSummary);

    inspectConnection(id: string): void {
        this.store.dispatch(
            selectComponents({ request: { components: [{ id, componentType: ComponentType.Connection }] } })
        );
    }
}
