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

import { Component, effect, inject, input, output } from '@angular/core';

import { MatButtonModule } from '@angular/material/button';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatFormField, MatLabel } from '@angular/material/form-field';
import { MatMenuModule } from '@angular/material/menu';
import { MatSelectChange, MatSelectModule } from '@angular/material/select';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';

import { NiFiCommon, SelectOption, Storage } from '@nifi/shared';
import { ProvenanceEvent } from '../../../state/shared';

@Component({
    selector: 'provenance-preview',
    templateUrl: './provenance-preview.component.html',
    imports: [
        MatButtonModule,
        MatExpansionModule,
        MatFormField,
        MatLabel,
        MatMenuModule,
        MatSelectModule,
        MatTableModule,
        NgxSkeletonLoaderModule,
        ReactiveFormsModule
    ],
    styleUrls: ['./provenance-preview.component.scss']
})
export class ProvenancePreview {
    private formBuilder = inject(FormBuilder);
    private nifiCommon = inject(NiFiCommon);
    private storage = inject(Storage);

    private static readonly CONTROL_VISIBILITY_KEY: string = 'graph-control-visibility';
    private static readonly HOSTNAME_SEPARATOR: string = '.';

    events = input.required<ProvenanceEvent[]>();
    status = input.required<'pending' | 'loading' | 'success' | 'error'>();
    error = input<string | null>(null);
    connectedToCluster = input<boolean>(false);
    contentViewerAvailable = input<boolean>(false);
    storageKey = input<string>('provenance-control');

    refresh = output<void>();
    collapsedChange = output<boolean>();
    viewDetails = output<ProvenanceEvent>();
    downloadContent = output<{ event: ProvenanceEvent; direction: 'input' | 'output' }>();
    viewContent = output<{ event: ProvenanceEvent; direction: 'input' | 'output' }>();
    replayEvent = output<ProvenanceEvent>();

    provenanceCollapsed = true;
    clusterForm: FormGroup;
    availableNodes: SelectOption[] = [];
    displayedColumns: string[] = ['eventType', 'actions'];
    dataSource: MatTableDataSource<ProvenanceEvent> = new MatTableDataSource<ProvenanceEvent>();
    selectedId: string | null = null;

    constructor() {
        this.clusterForm = this.formBuilder.group({
            nodes: null
        });

        this.restoreCollapsedState();

        effect(() => {
            const events = this.events();
            this.selectedId = null;
            this.dataSource.data = this.sortEntities(events);
            this.prepareNodeOptions();
        });

        effect(() => {
            this.connectedToCluster();
            this.prepareNodeOptions();
        });
    }

    toggleCollapsed(collapsed: boolean): void {
        this.provenanceCollapsed = collapsed;
        this.persistCollapsedState(collapsed);
        this.collapsedChange.emit(collapsed);
    }

    refreshClicked(event: MouseEvent): void {
        event.stopPropagation();
        this.refresh.emit();
    }

    filterByNode(event: MatSelectChange): void {
        this.applyFilter(event.value);
    }

    select(event: ProvenanceEvent): void {
        this.selectedId = event.id;
    }

    isSelected(event: ProvenanceEvent): boolean {
        if (this.selectedId) {
            return event.id === this.selectedId;
        }
        return false;
    }

    viewDetailsClicked(event: ProvenanceEvent): void {
        this.viewDetails.emit(event);
    }

    canDownloadContent(provenanceEvent: ProvenanceEvent, direction: 'input' | 'output'): boolean {
        if (direction === 'input') {
            return provenanceEvent.inputContentAvailable;
        } else {
            return provenanceEvent.outputContentAvailable;
        }
    }

    downloadContentClicked(event: ProvenanceEvent, direction: 'input' | 'output'): void {
        this.downloadContent.emit({ event, direction });
    }

    canViewContent(provenanceEvent: ProvenanceEvent, direction: 'input' | 'output'): boolean {
        if (this.contentViewerAvailable()) {
            return this.canDownloadContent(provenanceEvent, direction);
        }
        return false;
    }

    viewContentClicked(event: ProvenanceEvent, direction: 'input' | 'output'): void {
        this.viewContent.emit({ event, direction });
    }

    replayClicked(event: ProvenanceEvent): void {
        this.replayEvent.emit(event);
    }

    formatHostname(clusterNodeAddress?: string): string {
        if (!clusterNodeAddress) {
            return '';
        }

        const separatorIndex = clusterNodeAddress.indexOf(ProvenancePreview.HOSTNAME_SEPARATOR);
        if (separatorIndex >= 0) {
            return this.nifiCommon.substringBeforeFirst(clusterNodeAddress, ProvenancePreview.HOSTNAME_SEPARATOR);
        }
        return clusterNodeAddress;
    }

    private sortEntities(data: ProvenanceEvent[]): ProvenanceEvent[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            return (
                this.nifiCommon.compareNumber(
                    this.nifiCommon.parseDateTime(a.eventTime).getTime(),
                    this.nifiCommon.parseDateTime(b.eventTime).getTime()
                ) * -1
            );
        });
    }

    private prepareNodeOptions(): void {
        if (this.connectedToCluster() && this.dataSource.data.length > 0) {
            const nodeAddressLookup = new Map<string, string>();

            this.dataSource.data.forEach((event) => {
                nodeAddressLookup.set(event.clusterNodeId, this.formatHostname(event.clusterNodeAddress));
            });

            const nodeIds = Array.from(nodeAddressLookup.keys());
            this.availableNodes = nodeIds.map((nodeId) => {
                return {
                    text: nodeAddressLookup.get(nodeId),
                    value: nodeId
                } as SelectOption;
            });

            const selectedNodeIdFromLatestEvent = this.dataSource.data[0].clusterNodeId;
            const currentSelectedNodeId = this.clusterForm.get('nodes')?.value;

            if (!currentSelectedNodeId || !nodeAddressLookup.has(currentSelectedNodeId)) {
                this.clusterForm.get('nodes')?.setValue(selectedNodeIdFromLatestEvent);
                this.applyFilter(selectedNodeIdFromLatestEvent);
            } else {
                this.applyFilter(currentSelectedNodeId);
            }
        } else {
            this.availableNodes = [];
            this.dataSource.filter = '';
        }
    }

    private applyFilter(clusterNodeId: string): void {
        this.dataSource.filterPredicate = (provenanceEvent: ProvenanceEvent) =>
            provenanceEvent.clusterNodeId === clusterNodeId;
        this.dataSource.filter = ' ';
    }

    private restoreCollapsedState(): void {
        try {
            const item: { [key: string]: boolean } | null = this.storage.getItem(
                ProvenancePreview.CONTROL_VISIBILITY_KEY
            );
            if (item) {
                this.provenanceCollapsed = !item[this.storageKey()];
            }
        } catch (_e) {
            // likely could not parse item... ignoring
        }
    }

    private persistCollapsedState(collapsed: boolean): void {
        let item: { [key: string]: boolean } | null = this.storage.getItem(ProvenancePreview.CONTROL_VISIBILITY_KEY);
        if (item == null) {
            item = {};
        }
        item[this.storageKey()] = !collapsed;
        this.storage.setItem(ProvenancePreview.CONTROL_VISIBILITY_KEY, item);
    }
}
