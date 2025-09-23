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

import {
    AfterViewInit,
    Component,
    DestroyRef,
    HostListener,
    inject,
    OnInit,
    signal,
    WritableSignal
} from '@angular/core';
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
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import * as d3 from 'd3';
import { isDefinedAndNotNull, CloseOnEscapeDialog, NiFiCommon, NifiTooltipDirective, TextTip } from '@nifi/shared';
import { MatCheckboxChange, MatCheckboxModule } from '@angular/material/checkbox';
import { Instance, NIFI_NODE_CONFIG, StartOption, Stats } from './index';
import { StatusHistoryChart } from './status-history-chart/status-history-chart.component';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ErrorContextKey } from '../../../state/error';
import { ContextErrorBanner } from '../context-error-banner/context-error-banner.component';

@Component({
    selector: 'status-history',
    templateUrl: './status-history.component.html',
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
        StatusHistoryChart,
        NgStyle,
        ContextErrorBanner
    ],
    styleUrls: ['./status-history.component.scss']
})
export class StatusHistory extends CloseOnEscapeDialog implements OnInit, AfterViewInit {
    private statusHistoryService = inject(StatusHistoryService);
    private store = inject<Store<StatusHistoryState>>(Store);
    private formBuilder = inject(FormBuilder);
    private nifiCommon = inject(NiFiCommon);
    private dialogRequest = inject<StatusHistoryRequest>(MAT_DIALOG_DATA);

    request: StatusHistoryRequest;
    statusHistoryState$ = this.store.select(selectStatusHistoryState);
    componentDetails$ = this.store.select(selectStatusHistoryComponentDetails);
    statusHistory$ = this.store.select(selectStatusHistory);
    fieldDescriptors$ = this.store.select(selectStatusHistoryFieldDescriptors);
    fieldDescriptors: FieldDescriptor[] = [];

    dialogMaximized = false;

    details: { key: string; value: string }[] = [];

    formattedMinDate = '';
    formattedMaxDate = '';
    minDate = 0;
    maxDate = 0;
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
    filteredInstances: WritableSignal<Instance[]> = signal([]);
    startTimeOptions: StartOption[] = [];
    instanceVisibility: any = {};
    selectedDescriptor: FieldDescriptor | null = null;
    private destroyRef: DestroyRef = inject(DestroyRef);

    constructor() {
        super();
        const dialogRequest = this.dialogRequest;

        this.request = dialogRequest;
        this.statusHistoryForm = this.formBuilder.group({
            fieldDescriptor: '',
            start: new FormControl(0, Validators.required)
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
                    this.formattedMinDate = this.nifiCommon.formatDateTime(new Date(minDate));
                    this.formattedMaxDate = this.nifiCommon.formatDateTime(new Date(maxDate));

                    this.minDate = minDate;
                    this.maxDate = maxDate;
                    this.updateStartTimeOptions();
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

    maximize() {
        this.dialogMaximized = true;
        this.resized();
    }

    minimize() {
        this.dialogMaximized = false;
        this.resized();
    }

    isInitialLoading(state: StatusHistoryState) {
        return state.loadedTimestamp === initialState.loadedTimestamp;
    }

    refresh() {
        this.store.dispatch(reloadStatusHistory({ request: this.request }));
    }

    clusterStatsChanged(stats: Stats) {
        this.clusterStats = stats;
    }

    nodeStatsChanged(stats: Stats) {
        this.nodeStats = stats;
    }

    protected readonly Object = Object;

    protected readonly TextTip = TextTip;

    areAllNodesSelected(instanceVisibility: any): boolean {
        const unChecked = Object.entries(instanceVisibility)
            .filter(([node, checked]) => node !== 'nifi-instance-id' && !checked)
            .map(([, checked]) => checked);
        return unChecked.length === 0;
    }

    areAnyNodesSelected(instanceVisibility: any): boolean {
        const checked = Object.entries(instanceVisibility)
            .filter(([node, checked]) => node !== 'nifi-instance-id' && checked)
            .map(([, checked]) => checked);
        return checked.length > 0 && checked.length < this.nodes.length;
    }

    selectAllNodesChanged(event: MatCheckboxChange) {
        const checked: boolean = event.checked;
        const tmpInstanceVisibility: any = {};
        this.nodes.forEach((node: Instance) => {
            tmpInstanceVisibility[node.id] = checked;
        });
        tmpInstanceVisibility['nifi-instance-id'] = this.instanceVisibility['nifi-instance-id'];
        this.instanceVisibility = tmpInstanceVisibility;
    }

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

    @HostListener('window:resize', ['$event.target'])
    windowResized() {
        if (this.dialogMaximized) {
            this.resized();
        }
    }

    getColor(stats: Stats, nodeId: string): string {
        if (stats.nodes && stats.nodes.length > 0) {
            const nodeColor = stats.nodes?.find((c) => c.id === nodeId);
            if (nodeColor) {
                return nodeColor.color;
            }
        }
        return 'unset neutral-color';
    }

    startChanged(value: number) {
        if (value === 0) {
            this.filteredInstances.set(this.instances);
        } else {
            const filtered = this.instances.map((instance: Instance) => {
                const filteredSnapshots = instance.snapshots.filter(
                    (snapshot) => snapshot.timestamp >= this.maxDate - value
                );
                return {
                    id: instance.id,
                    label: instance.label,
                    snapshots: filteredSnapshots
                };
            });
            // only include the instances that have snapshots that meet the filter criteria
            this.filteredInstances.set(filtered.filter((instances) => instances.snapshots.length > 0));
        }
    }

    formatSelectedStartTime(value: number) {
        const selected = this.startTimeOptions.find((option) => option.value === value);
        if (selected) {
            return selected.label;
        }
        return this.startTimeOptions[0].label;
    }

    private updateStartTimeOptions() {
        const selected = this.statusHistoryForm.get('start')?.value;
        const options: StartOption[] = [{ label: 'All', value: 0, formattedDate: this.formattedMinDate }];
        const diff = this.maxDate - this.minDate;

        if (diff > this.fiveMinutes) {
            options.push({
                label: 'Last 5 minutes',
                value: this.fiveMinutes,
                formattedDate: this.nifiCommon.formatDateTime(new Date(this.maxDate - this.fiveMinutes))
            });
        }
        if (diff > this.tenMinutes) {
            options.push({
                label: 'Last 10 minutes',
                value: this.tenMinutes,
                formattedDate: this.nifiCommon.formatDateTime(new Date(this.maxDate - this.tenMinutes))
            });
        }
        if (diff > this.thirtyMinutes) {
            options.push({
                label: 'Last 30 minutes',
                value: this.thirtyMinutes,
                formattedDate: this.nifiCommon.formatDateTime(new Date(this.maxDate - this.thirtyMinutes))
            });
        }
        if (diff > this.sixtyMinutes) {
            options.push({
                label: 'Last hour',
                value: this.sixtyMinutes,
                formattedDate: this.nifiCommon.formatDateTime(new Date(this.maxDate - this.sixtyMinutes))
            });
        }
        this.startTimeOptions = options;
        // trigger the charts to update with potentially new data
        this.startChanged(selected);
    }

    private readonly fiveMinutes = 5 * NiFiCommon.MILLIS_PER_MINUTE;
    private readonly tenMinutes = 10 * NiFiCommon.MILLIS_PER_MINUTE;
    private readonly thirtyMinutes = 30 * NiFiCommon.MILLIS_PER_MINUTE;
    private readonly sixtyMinutes = NiFiCommon.MILLIS_PER_HOUR;

    protected readonly NIFI_NODE_CONFIG = NIFI_NODE_CONFIG;
    protected readonly ErrorContextKey = ErrorContextKey;
}
