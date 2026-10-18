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

import { Component, computed, effect, inject, input, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Subscription } from 'rxjs';
import { Store } from '@ngrx/store';
import { MatButtonModule } from '@angular/material/button';
import { selectClusterSummary } from '../../../../../state/cluster-summary/cluster-summary.selectors';
import { OperationalSelection } from '../../../state/operational-overview/operational-overview.selectors';
import { QueueStatusResponse, queueNodeRows, ThresholdObservation } from './queue-node-status.model';

@Component({
    selector: 'queue-node-status',
    imports: [CommonModule, MatButtonModule],
    templateUrl: './queue-node-status.component.html',
    host: { class: 'block' }
})
export class QueueNodeStatusComponent {
    private http = inject(HttpClient);
    private cluster = inject(Store).selectSignal(selectClusterSummary);
    selection = input.required<OperationalSelection>();
    status = signal<'idle' | 'loading' | 'ready' | 'forbidden' | 'error'>('idle');
    response = signal<QueueStatusResponse | null>(null);
    receivedAt = signal('');
    private request?: Subscription;
    rows = computed(() =>
        queueNodeRows(
            this.response() || {},
            this.cluster()?.clustered,
            this.selection().objectThreshold,
            this.selection().dataThreshold
        )
    );
    coverage = computed(() => {
        const cluster = this.cluster();
        if (!cluster) return 'Runtime coverage unavailable';
        if (!cluster.clustered) return 'Standalone instance';
        return `${this.rows().filter((row) => row.count.used !== undefined && row.bytes.used !== undefined).length} / ${cluster.totalNodeCount} complete snapshots; ${this.rows().length} node entries returned; ${cluster.connectedNodeCount} currently connected`;
    });

    private contextKey = computed(() => {
        const selection = this.selection();
        return JSON.stringify([
            selection.id,
            selection.kind,
            selection.readable,
            selection.objectThreshold,
            selection.dataThreshold
        ]);
    });

    constructor() {
        effect((onCleanup) => {
            this.contextKey();
            this.request?.unsubscribe();
            this.response.set(null);
            this.receivedAt.set('');
            this.status.set('idle');
            onCleanup(() => this.request?.unsubscribe());
        });
    }

    load(): void {
        const selected = this.selection();
        if (!selected.readable || selected.kind !== 'connection' || this.status() === 'loading') return;
        this.status.set('loading');
        this.response.set(null);
        this.receivedAt.set('');
        this.request = this.http
            .get<QueueStatusResponse>(`../nifi-api/flow/connections/${encodeURIComponent(selected.id)}/status`, {
                params: { nodewise: true }
            })
            .subscribe({
                next: (response) => {
                    if (response.canRead !== true) {
                        this.status.set('forbidden');
                        return;
                    }
                    if (response.connectionStatus?.id !== selected.id) {
                        this.status.set('error');
                        return;
                    }
                    this.response.set(response);
                    this.receivedAt.set(new Date().toISOString());
                    this.status.set('ready');
                },
                error: (error: HttpErrorResponse) =>
                    this.status.set([401, 403].includes(error.status) ? 'forbidden' : 'error')
            });
    }

    label(observation: ThresholdObservation): string {
        return {
            unavailable: 'Unavailable',
            disabled: 'Threshold disabled',
            below: 'Below threshold',
            'at-or-above': 'At or above threshold'
        }[observation.state];
    }
}
