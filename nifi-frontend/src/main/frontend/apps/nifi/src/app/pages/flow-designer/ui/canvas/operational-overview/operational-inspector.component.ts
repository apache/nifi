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

import { signal, Component, computed, inject, input } from '@angular/core';
import { ComponentType } from '@nifi/shared';
import { QueueNodeStatusComponent } from './queue-node-status.component';
import { CommonModule } from '@angular/common';
import { Store } from '@ngrx/store';
import { MatButtonModule } from '@angular/material/button';
import {
    deselectAllComponents,
    navigateToQueueListing,
    navigateToViewStatusHistoryForComponent
} from '../../../state/flow/flow.actions';
import { selectOperationalSelection } from '../../../state/operational-overview/operational-overview.selectors';
import { OperationalSelection } from '../../../state/operational-overview/operational-overview.selectors';

@Component({
    selector: 'operational-inspector',
    imports: [CommonModule, MatButtonModule, QueueNodeStatusComponent],
    templateUrl: './operational-inspector.component.html',
    styleUrl: './operational-inspector.component.scss',
    host: { class: 'block w-80' }
})
export class OperationalInspectorComponent {
    private store = inject(Store);
    private currentSelection = this.store.selectSignal(selectOperationalSelection);
    selection = input.required<OperationalSelection>();
    tab = signal<'details' | 'configuration' | 'comments'>('details');
    metadata = computed(() => (this.selection().readable ? this.selection().configuration : undefined));

    selectTab(event: KeyboardEvent): void {
        const tabs = ['details', 'configuration', 'comments'] as const;
        const index = tabs.indexOf(this.tab());
        const next =
            event.key === 'ArrowRight'
                ? (index + 1) % 3
                : event.key === 'ArrowLeft'
                  ? (index + 2) % 3
                  : event.key === 'Home'
                    ? 0
                    : event.key === 'End'
                      ? 2
                      : null;
        if (next !== null) {
            event.preventDefault();
            this.tab.set(tabs[next]);
            (event.currentTarget as HTMLElement).querySelectorAll<HTMLButtonElement>('[role="tab"]')[next]?.focus();
        }
    }
    utilization = computed(() => {
        const detail = this.selection();
        if (!detail.readable) return null;
        const values = [detail.percentCount, detail.percentBytes].filter(
            (value): value is number => value !== undefined && Number.isFinite(value) && value >= 0
        );
        return values.length ? Math.max(...values) : null;
    });

    close(): void {
        this.store.dispatch(deselectAllComponents());
    }

    history(): void {
        const current = this.currentSelection();
        if (current?.readable && current.id === this.selection().id) {
            this.store.dispatch(
                navigateToViewStatusHistoryForComponent({
                    request: {
                        id: current.id,
                        type: current.kind === 'connection' ? ComponentType.Connection : ComponentType.Processor
                    }
                })
            );
        }
    }

    listQueue(): void {
        const current = this.currentSelection();
        if (current?.readable && current.kind === 'connection' && current.id === this.selection().id) {
            this.store.dispatch(navigateToQueueListing({ request: { connectionId: current.id } }));
        }
    }
}
