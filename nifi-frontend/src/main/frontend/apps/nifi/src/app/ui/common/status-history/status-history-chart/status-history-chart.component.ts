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

import { Component, EventEmitter, Input, OnDestroy, Output } from '@angular/core';

import { FieldDescriptor } from '../../../../state/status-history';
import * as d3 from 'd3';
import { NiFiCommon } from '@nifi/shared';
import { Instance, NIFI_NODE_CONFIG, Stats, VisibleInstances } from '../index';
import { debounceTime, Subject } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
    selector: 'status-history-chart',
    imports: [],
    templateUrl: './status-history-chart.component.html',
    styleUrls: ['./status-history-chart.component.scss']
})
export class StatusHistoryChart implements OnDestroy {
    private _instances!: Instance[];
    private _selectedDescriptor: FieldDescriptor | null = null;
    private _visibleInstances: VisibleInstances = {};

    @Input() set instances(nodeInstances: Instance[]) {
        this._instances = nodeInstances;
        if (this._selectedDescriptor) {
            this.updateChart(this._selectedDescriptor);
        }
    }

    get instances(): Instance[] {
        return this._instances;
    }

    @Input() set selectedFieldDescriptor(selected: FieldDescriptor | null) {
        if (this._selectedDescriptor !== selected) {
            // clear the brush selection when the data is changed to view a different descriptor
            this.brushSelection = null;
            this._selectedDescriptor = selected;
            if (selected) {
                this.updateChart(selected);
            }
        }
    }

    get selectedFieldDescriptor(): FieldDescriptor | null {
        return this._selectedDescriptor;
    }

    @Input() set visibleInstances(visibleInstances: VisibleInstances) {
        this._visibleInstances = visibleInstances;
        if (this._selectedDescriptor) {
            this.updateChart(this._selectedDescriptor);
        }
    }

    get visibleInstances(): VisibleInstances {
        return this._visibleInstances;
    }

    @Output() nodeStats: EventEmitter<Stats> = new EventEmitter<Stats>();
    @Output() clusterStats: EventEmitter<Stats> = new EventEmitter<Stats>();

    private nodeStats$: Subject<Stats> = new Subject<Stats>();
    private clusterStats$: Subject<Stats> = new Subject<Stats>();

    nodes: any[] = [];

    constructor(private nifiCommon: NiFiCommon) {
        // don't need constantly fire the stats changing as a result of brush drag/move
        this.nodeStats$.pipe(debounceTime(20), takeUntilDestroyed()).subscribe((stats: Stats) => {
            this.nodeStats.next(stats);
        });

        this.clusterStats$.pipe(debounceTime(20), takeUntilDestroyed()).subscribe((stats: Stats) => {
            this.clusterStats.next(stats);
        });
    }

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

    // private selectedDescriptor: FieldDescriptor | null = null;
    private updateChart(selectedDescriptor: FieldDescriptor) {
        const margin = {
            top: 15,
            right: 20,
            bottom: 25,
            left: 80
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
            // convert the model
            return {
                id: instance.id,
                label: instance.label,
                values: instance.snapshots.map((snapshot) => {
                    return {
                        timestamp: snapshot.timestamp,
                        value: snapshot.statusMetrics[selectedDescriptor.field]
                    };
                }),
                visible: this._visibleInstances[instance.id]
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

        // determine the new width/height
        const width = chartContainer.clientWidth - margin.left - margin.right;
        const height = chartContainer.clientHeight - margin.top - margin.bottom;

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
        const clipPath = chartSvg
            .append('defs')
            .append('clipPath')
            .attr('id', 'clip')
            .append('rect')
            .attr('width', width)
            .attr('height', height);

        // build the chart
        const chart = chartSvg.append('g').attr('transform', 'translate(' + margin.left + ', ' + margin.top + ')');

        // determine the min/max date
        const minDate = d3.min(statusData, (d) => {
            return d3.min(d.values, (s) => {
                return s.timestamp;
            });
        });
        const maxDate = d3.max(statusData, (d) => {
            return d3.max(d.values, (s) => {
                return s.timestamp;
            });
        });

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

        // draw the control points for each line
        status.each((d, index, nodes) => {
            // create a group for the control points
            const markGroup = d3
                .select(nodes[index])
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
                    return d.label + ' -- ' + this.formatters[selectedDescriptor.formatter](v.value);
                });
        });

        // update the size of the chart
        const parentElement = chartContainer.parentElement;
        if (parentElement) {
            chartSvg.attr('width', parentElement.clientWidth).attr('height', chartContainer.clientHeight);
        }

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
                return this.visibleInstances[d.id] === false;
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
                return d.id !== NIFI_NODE_CONFIG.nifiInstanceId && d.visible && d.values.length > 0;
            });

            const nodeMinValue =
                nodes.length === 0 ? 'NA' : this.formatters[selectedDescriptor.formatter](this.getMinValue(nodes));
            const nodeMeanValue =
                nodes.length === 0 ? 'NA' : this.formatters[selectedDescriptor.formatter](this.getMeanValue(nodes));
            const nodeMaxValue =
                nodes.length === 0 ? 'NA' : this.formatters[selectedDescriptor.formatter](this.getMaxValue(nodes));

            // update the currently displayed min/max/mean
            this.nodeStats$.next({
                min: nodeMinValue,
                max: nodeMaxValue,
                mean: nodeMeanValue,
                nodes: nodes.map((n) => ({
                    id: n.id,
                    label: n.label,
                    color: color(n.label)
                }))
            });

            // only consider the cluster with data in the brush
            const cluster = withinBrush.filter((d) => {
                return d.id === NIFI_NODE_CONFIG.nifiInstanceId && d.visible && d.values.length > 0;
            });

            // determine the cluster values
            const clusterMinValue =
                cluster.length === 0 ? 'NA' : this.formatters[selectedDescriptor.formatter](this.getMinValue(cluster));
            const clusterMeanValue =
                cluster.length === 0 ? 'NA' : this.formatters[selectedDescriptor.formatter](this.getMeanValue(cluster));
            const clusterMaxValue =
                cluster.length === 0 ? 'NA' : this.formatters[selectedDescriptor.formatter](this.getMaxValue(cluster));

            // update the cluster min/max/mean
            this.clusterStats$.next({
                min: clusterMinValue,
                max: clusterMaxValue,
                mean: clusterMeanValue,
                nodes: cluster.map((n) => ({
                    id: n.id,
                    label: n.label,
                    color: color(n.label)
                }))
            });
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

        if (this.brushSelection) {
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
                return status.id !== NIFI_NODE_CONFIG.nifiInstanceId;
            })
            .sort((a: any, b: any) => {
                return a.label < b.label ? -1 : a.label > b.label ? 1 : 0;
            });

        brushed();
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

    ngOnDestroy(): void {
        this.nodeStats$.complete();
        this.clusterStats$.complete();
    }
}
