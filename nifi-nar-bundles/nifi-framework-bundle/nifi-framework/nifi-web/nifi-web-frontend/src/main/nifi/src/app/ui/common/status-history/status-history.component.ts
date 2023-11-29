/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { AfterViewInit, Component, Inject, OnDestroy, OnInit } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { StatusHistoryService } from '../../../service/status-history.service';
import { AsyncPipe, NgForOf, NgIf } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';
import {
    FieldDescriptor,
    NodeSnapshot,
    StatusHistoryEntity,
    StatusHistoryRequest,
    StatusHistoryState
} from '../../../state/status-history';
import { Store } from '@ngrx/store';
import { loadStatusHistory } from '../../../state/status-history/status-history.actions';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import {
    selectStatusHistory,
    selectStatusHistoryComponentDetails,
    selectStatusHistoryFieldDescriptors,
    selectStatusHistoryState
} from '../../../state/status-history/status-history.selectors';
import { initialState } from '../../../state/status-history/status-history.reducer';
import { combineLatest, delay, filter, take } from 'rxjs';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import * as d3 from 'd3';
import { NiFiCommon } from '../../../service/nifi-common.service';
import { TextTip } from '../tooltips/text-tip/text-tip.component';
import { NifiTooltipDirective } from '../tooltips/nifi-tooltip.directive';
import { TextTipInput } from '../../../state/shared';
import { MatCheckboxChange, MatCheckboxModule } from '@angular/material/checkbox';
import { Resizable } from '../resizable/resizable.component';

interface Instance {
    id: string;
    label: string;
    snapshots: any[];
}

const config = {
    nifiInstanceId: 'nifi-instance-id',
    nifiInstanceLabel: 'NiFi'
};

interface Stats {
    min: string;
    max: string;
    mean: string;
}

@Component({
    selector: 'status-history',
    templateUrl: './status-history.component.html',
    styleUrls: ['./status-history.component.scss'],
    standalone: true,
    imports: [
        MatDialogModule,
        AsyncPipe,
        MatButtonModule,
        NgIf,
        NgxSkeletonLoaderModule,
        NgForOf,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatSelectModule,
        NifiTooltipDirective,
        MatCheckboxModule,
        Resizable
    ]
})
export class StatusHistory implements OnInit, AfterViewInit {
    request: StatusHistoryRequest;
    statusHistoryState$ = this.store.select(selectStatusHistoryState);
    componentDetails$ = this.store.select(selectStatusHistoryComponentDetails);
    statusHistory$ = this.store.select(selectStatusHistory);
    fieldDescriptors$ = this.store.select(selectStatusHistoryFieldDescriptors);
    fieldDescriptors: FieldDescriptor[] = [];

    minDate: string = '';
    maxDate: string = '';
    statusHistoryForm: FormGroup;

    nodeStats: Stats | null = null;
    clusterStats: Stats | null = null;
    nodes: any[] = [];

    private svg: any;
    instances: Instance[] = [];
    instanceVisibility: any = {};

    private formatters: any = {
        DURATION: (d: number) => {
            return this.nifiCommon.formatDuration(d);
        },
        COUNT: (d: number) => {
            // need to handle floating point number since this formatter
            // will also be used for average values
            if (d % 1 === 0) {
                return this.nifiCommon.formatInteger(d);
            } else {
                return this.nifiCommon.formatFloat(d);
            }
        },
        DATA_SIZE: (d: number) => {
            return this.nifiCommon.formatDataSize(d);
        },
        FRACTION: (d: number) => {
            return this.nifiCommon.formatFloat(d / 1000000);
        }
    };
    private brushSelection: any = null;
    private selectedDescriptor: FieldDescriptor | null = null;

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
        this.refresh();

        this.statusHistory$.pipe(filter((entity) => !!entity)).subscribe((entity: StatusHistoryEntity) => {
            if (entity) {
                this.instances = [];
                if (entity.statusHistory?.aggregateSnapshots?.length > 1) {
                    this.instances.push({
                        id: config.nifiInstanceId,
                        label: config.nifiInstanceLabel,
                        snapshots: entity.statusHistory.aggregateSnapshots
                    });
                }

                // get the status for each node in the cluster if applicable
                if (entity.statusHistory?.nodeSnapshots && entity.statusHistory?.nodeSnapshots.length > 1) {
                    entity.statusHistory.nodeSnapshots.forEach((nodeSnapshot: NodeSnapshot) => {
                        this.instances.push({
                            id: nodeSnapshot.nodeId,
                            label: `${nodeSnapshot.address}:${nodeSnapshot.apiPort}`,
                            snapshots: nodeSnapshot.statusSnapshots
                        });
                    });
                }
            }
        });
    }

    ngAfterViewInit(): void {
        this.fieldDescriptors$
            .pipe(
                filter((descriptors) => !!descriptors),
                take(1) // only need to get the descriptors once
            )
            .subscribe((descriptors) => {
                this.fieldDescriptors = descriptors;

                // select the first field description by default
                this.statusHistoryForm.get('fieldDescriptor')?.setValue(descriptors[0]);
            });

        // when the selected descriptor changes, update the chart
        this.statusHistoryForm
            .get('fieldDescriptor')
            ?.valueChanges.pipe(
                delay(100) // avoid race condition where the required DOM nodes aren't rendered yet
            )
            .subscribe((descriptor: FieldDescriptor) => {
                // clear the brush selection when the data is changed to view a different descriptor
                this.brushSelection = null;
                if (this.instances.length > 0) {
                    this.selectedDescriptor = descriptor;
                    this.updateChart(descriptor);
                }
            });
    }

    isInitialLoading(state: StatusHistoryState) {
        return state.loadedTimestamp === initialState.loadedTimestamp;
    }

    refresh() {
        this.store.dispatch(loadStatusHistory({ request: this.request }));
    }

    getSelectOptionTipData(descriptor: FieldDescriptor): TextTipInput {
        return {
            text: descriptor.description
        };
    }

    protected readonly Object = Object;

    private resizeChart() {}

    private updateChart(selectedDescriptor: FieldDescriptor) {
        const margin = {
            top: 15,
            right: 20,
            bottom: 25,
            left: 75
        };

        // -------------
        // prep the data
        // -------------

        // available colors
        const color = d3.scaleOrdinal(d3.schemeCategory10);

        // determine the available instances
        const instanceLabels = this.instances.map((instance) => instance.label);

        // specify the domain based on the detected instances
        color.domain(instanceLabels);

        // data for the chart
        const statusData = this.instances.map((instance) => {
            // if this is the first time this instance is being rendered, make it visible
            if (!this.instanceVisibility[instance.id]) {
                this.instanceVisibility[instance.id] = true;
            }

            // and convert the model
            return {
                id: instance.id,
                label: instance.label,
                values: instance.snapshots.map((snapshot) => {
                    return {
                        timestamp: snapshot.timestamp,
                        value: snapshot.statusMetrics[selectedDescriptor.field]
                    };
                }),
                visible: this.instanceVisibility[instance.id] === true
            };
        });

        const customTimeFormat = (d: any) => {
            if (d.getMilliseconds()) {
                return d3.timeFormat(':%S.%L')(d);
            } else if (d.getSeconds()) {
                return d3.timeFormat(':%S')(d);
            } else if (d.getMinutes() || d.getHours()) {
                return d3.timeFormat('%H:%M')(d);
            } else if (d.getDay() && d.getDate() !== 1) {
                return d3.timeFormat('%a %d')(d);
            } else if (d.getDate() !== 1) {
                return d3.timeFormat('%b %d')(d);
            } else if (d.getMonth()) {
                return d3.timeFormat('%B')(d);
            } else {
                return d3.timeFormat('%Y')(d);
            }
        };

        // ----------
        // main chart
        // ----------

        // the container for the main chart
        const chartContainer = document.getElementById('status-history-chart-container')!;
        // clear out the dom for the chart
        chartContainer.replaceChildren();

        // calculate the dimensions
        chartContainer.setAttribute('height', this.getChartMinHeight() + 'px');

        // determine the new width/height
        const rect = chartContainer.getBoundingClientRect();
        let width = rect.width - margin.left - margin.right;
        let height = rect.height - margin.top - margin.bottom;

        let maxWidth = chartContainer.clientWidth;
        if (width > maxWidth) {
            width = maxWidth;
        }

        let maxHeight = this.getChartMaxHeight();
        if (height > maxHeight) {
            height = maxHeight;
        }

        // define the x axis for the main chart
        const x = d3.scaleTime().range([0, width]);
        const xAxis: any = d3.axisBottom(x).ticks(5).tickFormat(customTimeFormat);

        // define the y axis
        const y = d3.scaleLinear().range([height, 0]);
        const yAxis: any = d3.axisLeft(y).tickFormat(this.formatters[selectedDescriptor.formatter]);

        // status line
        const line = d3
            .line()
            .curve(d3.curveMonotoneX)
            .x((d: any) => x(d.timestamp))
            .y((d: any) => y(d.value));

        // build the chart svg
        const chartSvg = d3
            .select('#status-history-chart-container')
            .append('svg')
            .attr('style', 'pointer-events: none;')
            .attr('width', chartContainer.clientWidth)
            .attr('height', chartContainer.clientHeight);

        // define a clip the path
        var clipPath = chartSvg
            .append('defs')
            .append('clipPath')
            .attr('id', 'clip')
            .append('rect')
            .attr('width', width)
            .attr('height', height);

        // build the chart
        const chart = chartSvg.append('g').attr('transform', 'translate(' + margin.left + ', ' + margin.top + ')');

        // determine the min/max date
        const minDate = d3.min(statusData, function (d) {
            return d3.min(d.values, function (s) {
                return s.timestamp;
            });
        });
        const maxDate = d3.max(statusData, function (d) {
            return d3.max(d.values, function (s) {
                return s.timestamp;
            });
        });
        this.minDate = this.nifiCommon.formatDateTime(new Date(minDate));
        this.maxDate = this.nifiCommon.formatDateTime(new Date(maxDate));

        // determine the x axis range
        x.domain([minDate, maxDate]);

        // determine the y axis range
        y.domain([this.getMinValue(statusData), this.getMaxValue(statusData)]);

        // build the x axis
        chart
            .append('g')
            .attr('class', 'x axis')
            .attr('transform', 'translate(0, ' + height + ')')
            .call(xAxis);

        // build the y axis
        chart
            .append('g')
            .attr('class', 'y axis')
            .call(yAxis)
            .append('text')
            .attr('transform', 'rotate(-90)')
            .attr('y', 6)
            .attr('dy', '.71em')
            .attr('text-anchor', 'end')
            .text(selectedDescriptor.label);

        // build the chart
        const status = chart
            .selectAll('.status')
            .data(statusData)
            .enter()
            .append('g')
            .attr('clip-path', 'url(#clip)')
            .attr('class', 'status');

        status
            .append('path')
            .attr('class', (d: any) => {
                return 'chart-line chart-line-' + d.id;
            })
            .attr('d', (d: any) => {
                return line(d.values);
            })
            .attr('stroke', (d: any) => {
                return color(d.label);
            })
            .classed('hidden', (d: any) => {
                return d.visible === false;
            })
            .append('title')
            .text((d: any) => {
                return d.label;
            });

        const me = this;
        // draw the control points for each line
        status.each(function (d) {
            // create a group for the control points
            const markGroup = d3
                .select(this)
                .append('g')
                .attr('class', function () {
                    return 'mark-group mark-group-' + d.id;
                })
                .classed('hidden', function (d: any) {
                    return d.visible === false;
                });

            // draw the control points
            markGroup
                .selectAll('circle.mark')
                .data(d.values)
                .enter()
                .append('circle')
                .attr('style', 'pointer-events: all;')
                .attr('class', 'mark')
                .attr('cx', (v) => {
                    return x(v.timestamp);
                })
                .attr('cy', (v) => {
                    return y(v.value);
                })
                .attr('fill', () => {
                    return color(d.label);
                })
                .attr('r', 1.5)
                .append('title')
                .text((v) => {
                    return d.label + ' -- ' + me.formatters[selectedDescriptor.formatter](v.value);
                });
        });

        // update the size of the chart
        chartSvg.attr('width', chartContainer.parentElement?.clientWidth!).attr('height', chartContainer.clientHeight);

        // update the size of the clipper
        clipPath.attr('width', width).attr('height', height);

        // update the position of the x axis
        chart.select('.x.axis').attr('transform', 'translate(0, ' + height + ')');

        // -------------
        // control chart
        // -------------

        // the container for the main chart control
        const chartControlContainer = document.getElementById('status-history-chart-control-container')!;
        chartControlContainer.replaceChildren();
        const controlHeight = chartControlContainer.clientHeight - margin.top - margin.bottom;

        const xControl = d3.scaleTime().range([0, width]);

        const xControlAxis = d3.axisBottom(xControl).ticks(5).tickFormat(customTimeFormat);

        const yControl = d3.scaleLinear().range([controlHeight, 0]);

        const yControlAxis = d3
            .axisLeft(yControl)
            .tickValues(y.domain())
            .tickFormat(this.formatters[selectedDescriptor.formatter]);

        // status line
        const controlLine = d3
            .line()
            .curve(d3.curveMonotoneX)
            .x(function (d: any) {
                return xControl(d.timestamp);
            })
            .y(function (d: any) {
                return yControl(d.value);
            });

        // build the svg
        const controlChartSvg = d3
            .select('#status-history-chart-control-container')
            .append('svg')
            .attr('width', chartContainer.clientWidth)
            .attr('height', chartControlContainer.clientHeight);

        // build the control chart
        const control = controlChartSvg
            .append('g')
            .attr('transform', 'translate(' + margin.left + ', ' + margin.top + ')');

        // define the domain for the control chart
        xControl.domain(x.domain());
        yControl.domain(y.domain());

        // build the control x axis
        control
            .append('g')
            .attr('class', 'x axis')
            .attr('transform', 'translate(0, ' + controlHeight + ')')
            .call(xControlAxis);

        // build the control y axis
        control.append('g').attr('class', 'y axis').call(yControlAxis);

        // build the control chart
        const controlStatus = control.selectAll('.status').data(statusData).enter().append('g').attr('class', 'status');

        // draw the lines
        controlStatus
            .append('path')
            .attr('class', (d: any) => {
                return 'chart-line chart-line-' + d.id;
            })
            .attr('d', (d: any) => {
                return controlLine(d.values);
            })
            .attr('stroke', (d: any) => {
                return color(d.label);
            })
            .classed('hidden', (d: any) => {
                return this.instanceVisibility[d.id] === false;
            })
            .append('title')
            .text(function (d) {
                return d.label;
            });

        const updateAggregateStatistics = () => {
            const xDomain = x.domain();
            const yDomain = y.domain();

            // locate the instances that have data points within the current brush
            const withinBrush = statusData.map((d: any) => {
                // update to only include values within the brush
                const values = d.values.filter((s: any) => {
                    return (
                        s.timestamp >= xDomain[0].getTime() &&
                        s.timestamp <= xDomain[1] &&
                        s.value >= yDomain[0] &&
                        s.value <= yDomain[1]
                    );
                });
                return {
                    ...d,
                    values
                };
            });

            // consider visible nodes with data in the brush
            const nodes: any[] = withinBrush.filter((d: any) => {
                return d.id !== config.nifiInstanceId && d.visible && d.values.length > 0;
            });

            const nodeMinValue =
                nodes.length === 0 ? 'NA' : this.formatters[selectedDescriptor.formatter](this.getMinValue(nodes));
            const nodeMeanValue =
                nodes.length === 0 ? 'NA' : this.formatters[selectedDescriptor.formatter](this.getMeanValue(nodes));
            const nodeMaxValue =
                nodes.length === 0 ? 'NA' : this.formatters[selectedDescriptor.formatter](this.getMaxValue(nodes));

            // update the currently displayed min/max/mean
            this.nodeStats = {
                min: nodeMinValue,
                max: nodeMaxValue,
                mean: nodeMeanValue
            };

            // only consider the cluster with data in the brush
            const cluster = withinBrush.filter((d) => {
                return d.id === config.nifiInstanceId && d.visible && d.values.length > 0;
            });

            // determine the cluster values
            const clusterMinValue =
                cluster.length === 0 ? 'NA' : this.formatters[selectedDescriptor.formatter](this.getMinValue(cluster));
            const clusterMeanValue =
                cluster.length === 0 ? 'NA' : this.formatters[selectedDescriptor.formatter](this.getMeanValue(cluster));
            const clusterMaxValue =
                cluster.length === 0 ? 'NA' : this.formatters[selectedDescriptor.formatter](this.getMaxValue(cluster));

            // update the cluster min/max/mean
            this.clusterStats = {
                min: clusterMinValue,
                max: clusterMaxValue,
                mean: clusterMeanValue
            };
        };

        // -------------------
        // configure the brush
        // -------------------
        /**
         * Updates the axis for the main chart.
         *
         * @param {array} xDomain   The new domain for the x axis
         * @param {array} yDomain   The new domain for the y axis
         */
        const updateAxes = (xDomain: any[], yDomain: any[]) => {
            x.domain(xDomain);
            y.domain(yDomain);

            // update the chart lines
            status.selectAll('.chart-line').attr('d', (d: any) => {
                return line(d.values);
            });
            status
                .selectAll('circle.mark')
                .attr('cx', (v: any) => {
                    return x(v.timestamp);
                })
                .attr('cy', (v: any) => {
                    return y(v.value);
                })
                .attr('r', function () {
                    return d3.brushSelection(brushNode.node()) === null ? 1.5 : 4;
                });

            // update the x axis
            chart.select('.x.axis').call(xAxis);
            chart.select('.y.axis').call(yAxis);
        };

        /**
         * Handles brush events by updating the main chart according to the context window
         * or the control domain if there is no context window.
         */
        const brushed = () => {
            this.brushSelection = d3.brushSelection(brushNode.node());
            let xContextDomain: any[];
            let yContextDomain: any[];

            // determine the new x and y domains
            if (this.brushSelection === null) {
                // get the all visible instances
                const visibleInstances: any[] = statusData.filter((d: any) => d.visible);

                if (visibleInstances.length === 0) {
                    yContextDomain = yControl.domain();
                } else {
                    yContextDomain = [
                        d3.min(visibleInstances, (d) => {
                            return d3.min(d.values, (s: any) => {
                                return s.value;
                            });
                        }),
                        d3.max(visibleInstances, (d) => {
                            return d3.max(d.values, (s: any) => {
                                return s.value;
                            });
                        })
                    ];
                }
                xContextDomain = xControl.domain();
            } else {
                xContextDomain = [this.brushSelection[0][0], this.brushSelection[1][0]].map(xControl.invert, xControl);
                yContextDomain = [this.brushSelection[1][1], this.brushSelection[0][1]].map(yControl.invert, yControl);
            }

            // update the axes accordingly
            updateAxes(xContextDomain, yContextDomain);

            // update the aggregate statistics according to the new domain
            updateAggregateStatistics();
        };

        // build the brush
        let brush: any = d3
            .brush()
            .extent([
                [xControl.range()[0], yControl.range()[1]],
                [xControl.range()[1], yControl.range()[0]]
            ])
            .on('brush', brushed);

        // context area
        const brushNode: any = control.append('g').attr('class', 'brush').on('click', brushed).call(brush);

        if (!!this.brushSelection) {
            brush = brush.move(brushNode, this.brushSelection);
        }

        // add expansion to the extent
        control
            .select('rect.selection')
            .attr('style', 'pointer-events: all;')
            .on('dblclick', () => {
                if (this.brushSelection !== null) {
                    // get the y range (this value does not change from the original y domain)
                    const yRange = yControl.range();

                    // expand the extent vertically
                    brush.move(brushNode, [
                        [this.brushSelection[0][0], yRange[1]],
                        [this.brushSelection[1][0], yRange[0]]
                    ]);
                }
            });

        // identify all nodes and sort
        this.nodes = statusData
            .filter((status) => {
                return status.id !== config.nifiInstanceId;
            })
            .sort((a: any, b: any) => {
                return a.label < b.label ? -1 : a.label > b.label ? 1 : 0;
            });

        brushed();

        // ---------------
        // handle resizing
        // ---------------
    }

    private getChartMinHeight() {
        const chartContainer = document.getElementById('status-history-chart-container')!;
        const controlContainer = document.getElementById('status-history-chart-control-container')!;

        const marginTop: any = controlContainer.computedStyleMap().get('margin-top');
        return (
            chartContainer.parentElement!.clientHeight - controlContainer.clientHeight - parseInt(marginTop.value, 10)
        );
    }

    private getChartMaxHeight() {
        const chartContainer = document.getElementById('status-history-chart-container')!;
        const controlContainer = document.getElementById('status-history-chart-control-container')!;

        const marginTop: any = controlContainer.computedStyleMap().get('margin-top');
        const statusHistory = document.getElementsByClassName('status-history')![0];
        const dialogContent = statusHistory.getElementsByClassName('dialog-content')![0];
        const dialogStyles: any = dialogContent.computedStyleMap();
        const bodyHeight = document.body.getBoundingClientRect().height;

        return (
            bodyHeight -
            controlContainer.clientHeight -
            50 -
            parseInt(marginTop.value, 10) -
            parseInt(dialogStyles.get('top')?.value) -
            parseInt(dialogStyles.get('bottom')?.value)
        );
    }

    private getChartMaxWidth() {
        const chartContainer = document.getElementById('status-history-chart-container')!;
        const controlContainer = document.getElementById('status-history-chart-control-container')!;

        const statusHistory = document.getElementsByClassName('status-history')![0];
        const dialogContent = statusHistory.getElementsByClassName('dialog-content')![0];
        const dialogContentStyles: any = dialogContent.computedStyleMap();
        const fullDialogStyles: any = statusHistory.computedStyleMap();
        const bodyWidth = document.body.getBoundingClientRect().width;

        return (
            bodyWidth -
            statusHistory.clientWidth -
            parseInt(fullDialogStyles.get('left')?.value, 10) -
            parseInt(dialogContentStyles.get('left')?.value) -
            parseInt(dialogContentStyles.get('right')?.value)
        );
    }

    private getMinValue(nodeInstances: any): any {
        return d3.min(nodeInstances, (d: any) => {
            return d3.min(d.values, (s: any) => s.value);
        });
    }

    private getMaxValue(nodeInstances: any): any {
        return d3.max(nodeInstances, (d: any) => {
            return d3.max(d.values, (s: any) => s.value);
        });
    }

    private getMeanValue(nodeInstances: any[]): any {
        let snapshotCount = 0;
        const totalValue = d3.sum(nodeInstances, (d: any) => {
            snapshotCount += d.values.length;
            return d3.sum(d.values, (s: any) => {
                return s.value;
            });
        });
        return totalValue / snapshotCount;
    }

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
        this.instanceVisibility[instanceId] = checked;
    }

    resized(event: DOMRect) {
        if (this.selectedDescriptor) {
            this.updateChart(this.selectedDescriptor);
        }
    }
}
