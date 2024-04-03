/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { AfterViewInit, Component, DestroyRef, inject, Inject, OnInit } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { StatusHistoryService } from '../../../service/status-history.service';
import { AsyncPipe, NgStyle } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';
import {
    FieldDescriptor,
    NodeSnapshot,
    StatusHistoryEntity,
    StatusHistoryRequest,
    StatusHistoryState
} from '../../../state/status-history';
import { Store } from '@ngrx/store';
import { reloadStatusHistory } from '../../../state/status-history/status-history.actions';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import {
    selectStatusHistory,
    selectStatusHistoryComponentDetails,
    selectStatusHistoryFieldDescriptors,
    selectStatusHistoryState
} from '../../../state/status-history/status-history.selectors';
import { initialState } from '../../../state/status-history/status-history.reducer';
import { filter, take } from 'rxjs';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import * as d3 from 'd3';
import { NiFiCommon } from '../../../service/nifi-common.service';
import { TextTip } from '../tooltips/text-tip/text-tip.component';
import { NifiTooltipDirective } from '../tooltips/nifi-tooltip.directive';
import { isDefinedAndNotNull, TextTipInput } from '../../../state/shared';
import { MatCheckboxChange, MatCheckboxModule } from '@angular/material/checkbox';
import { Resizable } from '../resizable/resizable.component';
import { Instance, NIFI_NODE_CONFIG, Stats } from './index';
import { StatusHistoryChart } from './status-history-chart/status-history-chart.component';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
    selector: 'status-history',
    templateUrl: './status-history.component.html',
    standalone: true,
    imports: [
        MatDialogModule,
        AsyncPipe,
        MatButtonModule,
        NgxSkeletonLoaderModule,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatSelectModule,
        NifiTooltipDirective,
        MatCheckboxModule,
        Resizable,
        StatusHistoryChart,
        NgStyle
    ],
    styleUrls: ['./status-history.component.scss']
})
export class StatusHistory implements OnInit, AfterViewInit {
    request: StatusHistoryRequest;
    statusHistoryState$ = this.store.select(selectStatusHistoryState);
    componentDetails$ = this.store.select(selectStatusHistoryComponentDetails);
    statusHistory$ = this.store.select(selectStatusHistory);
    fieldDescriptors$ = this.store.select(selectStatusHistoryFieldDescriptors);
    fieldDescriptors: FieldDescriptor[] = [];

    details: { key: string; value: string }[] = [];

    minDate = '';
    maxDate = '';
    statusHistoryForm: FormGroup;

    nodeStats: Stats = {
        max: 'NA',
        min: 'NA',
        mean: 'NA',
        nodes: []
    };
    clusterStats: Stats = {
        max: 'NA',
        min: 'NA',
        mean: 'NA',
        nodes: []
    };

    nodes: any[] = [];

    instances: Instance[] = [];
    instanceVisibility: any = {};
    selectedDescriptor: FieldDescriptor | null = null;
    private destroyRef: DestroyRef = inject(DestroyRef);

    constructor(
        private statusHistoryService: StatusHistoryService,
        private store: Store<StatusHistoryState>,
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon,
        @Inject(MAT_DIALOG_DATA) private dialogRequest: StatusHistoryRequest
    ) {
        this.request = dialogRequest;
        this.statusHistoryForm = this.formBuilder.group({
            fieldDescriptor: ''
        });
    }

    ngOnInit(): void {
        this.statusHistory$
            .pipe(
                filter((entity) => !!entity),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe((entity: StatusHistoryEntity) => {
                if (entity) {
                    this.instances = [];
                    if (entity.statusHistory?.aggregateSnapshots?.length > 1) {
                        this.instances.push({
                            id: NIFI_NODE_CONFIG.nifiInstanceId,
                            label: NIFI_NODE_CONFIG.nifiInstanceLabel,
                            snapshots: entity.statusHistory.aggregateSnapshots
                        });
                        // if this is the first time this instance is being rendered, make it visible
                        if (this.instanceVisibility[NIFI_NODE_CONFIG.nifiInstanceId] === undefined) {
                            this.instanceVisibility[NIFI_NODE_CONFIG.nifiInstanceId] = true;
                        }
                    }

                    // get the status for each node in the cluster if applicable
                    if (entity.statusHistory?.nodeSnapshots && entity.statusHistory?.nodeSnapshots.length > 1) {
                        entity.statusHistory.nodeSnapshots.forEach((nodeSnapshot: NodeSnapshot) => {
                            this.instances.push({
                                id: nodeSnapshot.nodeId,
                                label: `${nodeSnapshot.address}:${nodeSnapshot.apiPort}`,
                                snapshots: nodeSnapshot.statusSnapshots
                            });
                            // if this is the first time this instance is being rendered, make it visible
                            if (this.instanceVisibility[nodeSnapshot.nodeId] === undefined) {
                                this.instanceVisibility[nodeSnapshot.nodeId] = true;
                            }
                        });
                    }

                    // identify all nodes and sort
                    this.nodes = this.instances
                        .filter((status) => {
                            return status.id !== NIFI_NODE_CONFIG.nifiInstanceId;
                        })
                        .sort((a: any, b: any) => {
                            return a.label < b.label ? -1 : a.label > b.label ? 1 : 0;
                        });

                    // determine the min/max date
                    const minDate: any = d3.min(this.instances, (d) => {
                        return d3.min(d.snapshots, (s) => {
                            return s.timestamp;
                        });
                    });
                    const maxDate: any = d3.max(this.instances, (d) => {
                        return d3.max(d.snapshots, (s) => {
                            return s.timestamp;
                        });
                    });
                    this.minDate = this.nifiCommon.formatDateTime(new Date(minDate));
                    this.maxDate = this.nifiCommon.formatDateTime(new Date(maxDate));
                }
            });
        this.fieldDescriptors$
            .pipe(
                filter((descriptors) => !!descriptors),
                take(1) // only need to get the descriptors once
            )
            .subscribe((descriptors) => {
                this.fieldDescriptors = descriptors;

                // select the first field description by default
                this.statusHistoryForm.get('fieldDescriptor')?.setValue(descriptors[0]);
                this.selectedDescriptor = descriptors[0];
            });

        this.componentDetails$.pipe(isDefinedAndNotNull(), take(1)).subscribe((details) => {
            this.details = Object.entries(details).map((entry) => ({ key: entry[0], value: entry[1] }));
        });
    }

    ngAfterViewInit(): void {
        // when the selected descriptor changes, update the chart
        this.statusHistoryForm
            .get('fieldDescriptor')
            ?.valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((descriptor: FieldDescriptor) => {
                if (this.instances.length > 0) {
                    this.selectedDescriptor = descriptor;
                }
            });
    }

    isInitialLoading(state: StatusHistoryState) {
        return state.loadedTimestamp === initialState.loadedTimestamp;
    }

    refresh() {
        this.store.dispatch(reloadStatusHistory({ request: this.request }));
    }

    getSelectOptionTipData(descriptor: FieldDescriptor): TextTipInput {
        return {
            text: descriptor.description
        };
    }

    clusterStatsChanged(stats: Stats) {
        this.clusterStats = stats;
    }

    nodeStatsChanged(stats: Stats) {
        this.nodeStats = stats;
    }

    protected readonly Object = Object;

    protected readonly TextTip = TextTip;

    selectNode(event: MatCheckboxChange) {
        const instanceId: string = event.source.value;
        const checked: boolean = event.checked;

        // get the line and the control points for this instance (select all for the line to update control and main charts)
        const chartLine = d3.selectAll('path.chart-line-' + instanceId);
        const markGroup = d3.select('g.mark-group-' + instanceId);

        // determine if it was hidden
        const isHidden = markGroup.classed('hidden');

        // toggle the visibility
        chartLine.classed('hidden', () => !isHidden);
        markGroup.classed('hidden', () => !isHidden);

        // record the current status so it persists across refreshes
        this.instanceVisibility = {
            ...this.instanceVisibility,
            [instanceId]: checked
        };
    }

    resized() {
        if (this.selectedDescriptor) {
            // trigger the chart to re-render by changing the selection
            this.selectedDescriptor = { ...this.selectedDescriptor };
        }
    }

    getColor(stats: Stats, nodeId: string): string {
        if (stats.nodes && stats.nodes.length > 0) {
            const nodeColor = stats.nodes?.find((c) => c.id === nodeId);
            if (nodeColor) {
                return nodeColor.color;
            }
        }
        return 'unset nifi-surface-default';
    }

    protected readonly NIFI_NODE_CONFIG = NIFI_NODE_CONFIG;
}
