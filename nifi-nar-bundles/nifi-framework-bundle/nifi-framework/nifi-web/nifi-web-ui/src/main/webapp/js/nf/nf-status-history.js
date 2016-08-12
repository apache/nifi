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

/* global nf, d3 */

nf.StatusHistory = (function () {
    var config = {
        nifiInstanceId: 'nifi-instance-id',
        nifiInstanceLabel: 'NiFi',
        type: {
            processor: 'Processor',
            inputPort: 'Input Port',
            outputPort: 'Output Port',
            processGroup: 'Process Group',
            remoteProcessGroup: 'Remote Process Group',
            connection: 'Connection',
            funnel: 'Funnel',
            template: 'Template',
            label: 'Label'
        },
        urls: {
            api: '../nifi-api'
        }
    };

    /**
     * Mapping of formatters to use for the chart's y axis.
     */
    var formatters = {
        'DURATION': function (d) {
            return nf.Common.formatDuration(d);
        },
        'COUNT': function (d) {
            // need to handle floating point number since this formatter 
            // will also be used for average values
            if (d % 1 === 0) {
                return nf.Common.formatInteger(d);
            } else {
                return nf.Common.formatFloat(d);
            }
        },
        'DATA_SIZE': function (d) {
            return nf.Common.formatDataSize(d);
        }
    };

    /**
     * The time offset of the server.
     */
    var serverTimeOffset = null;

    /**
     * The current extent of the brush.
     */
    var brushExtent = null;

    /**
     * The currently selected descriptor.
     */
    var descriptor = null;

    /**
     * The instances and whether they are visible or not
     */
    var instances = null;

    /**
     * Handles the status history response.
     *
     * @param {string} groupId
     * @param {string} id
     * @param {object} componentStatusHistory
     * @param {string} componentType
     * @param {object} selectedDescriptor
     */
    var handleStatusHistoryResponse = function (groupId, id, componentStatusHistory, componentType, selectedDescriptor) {
        // update the last refreshed
        $('#status-history-last-refreshed').text(componentStatusHistory.generated);

        // initialize the status history
        var statusHistory = {
            groupId: groupId,
            id: id,
            type: componentType,
            instances: []
        };

        // get the descriptors
        var descriptors = componentStatusHistory.fieldDescriptors;
        statusHistory.details = componentStatusHistory.componentDetails;
        statusHistory.selectedDescriptor = nf.Common.isUndefined(selectedDescriptor) ? descriptors[0] : selectedDescriptor;

        // ensure enough status snapshots
        if (nf.Common.isDefinedAndNotNull(componentStatusHistory.aggregateSnapshots) && componentStatusHistory.aggregateSnapshots.length > 1) {
            statusHistory.instances.push({
                id: config.nifiInstanceId,
                label: config.nifiInstanceLabel,
                snapshots: componentStatusHistory.aggregateSnapshots
            });
        } else {
            insufficientHistory();
            return;
        }

        // get the status for each node in the cluster if applicable
        $.each(componentStatusHistory.nodeSnapshots, function (_, nodeSnapshots) {
            // ensure enough status snapshots
            if (nf.Common.isDefinedAndNotNull(nodeSnapshots.statusSnapshots) && nodeSnapshots.statusSnapshots.length > 1) {
                statusHistory.instances.push({
                    id: nodeSnapshots.nodeId,
                    label: nodeSnapshots.address + ':' + nodeSnapshots.apiPort,
                    snapshots: nodeSnapshots.statusSnapshots
                });
            }
        });

        // ensure we found eligible status history
        if (statusHistory.instances.length > 0) {
            // store the status history
            $('#status-history-dialog').data('status-history', statusHistory);

            // chart the status history
            chart(statusHistory, descriptors);
        } else {
            insufficientHistory();
        }
    };

    /**
     * Shows an error message stating there is insufficient history available.
     */
    var insufficientHistory = function () {
        // notify the user
        nf.Dialog.showOkDialog({
            headerText: 'Status History',
            dialogContent: 'Insufficient history, please try again later.'
        });
    };

    /**
     * Builds the chart using the statusHistory and the available fields.
     *
     * @param {type} statusHistory
     * @param {type} descriptors
     */
    var chart = function (statusHistory, descriptors) {
        if (instances === null) {
            instances = {};
        }

        // go through each instance of this status history
        $.each(statusHistory.instances, function (_, instance) {
            // and convert each timestamp accordingly
            instance.snapshots.forEach(function (d) {
                // get the current user time to properly convert the server time
                var now = new Date();

                // conver the user offset to millis
                var userTimeOffset = now.getTimezoneOffset() * 60 * 1000;

                // create the proper date by adjusting by the offsets
                d.timestamp = new Date(d.timestamp + userTimeOffset + serverTimeOffset);
            });
        });

        // this will trigger the chart to be updated
        setAvailableFields(descriptors, statusHistory.selectedDescriptor);
    };

    /**
     * Sets the available fields.
     *
     * @param {type} descriptors
     * @param {type} selected
     */
    var setAvailableFields = function (descriptors, selected) {
        var options = [];
        $.each(descriptors, function (_, d) {
            options.push({
                text: d.label,
                value: d.field,
                description: nf.Common.escapeHtml(d.description)
            });
        });

        // build the combo with the available fields
        $('#status-history-metric-combo').combo({
            selectedOption: {
                value: selected.field
            },
            options: options,
            select: function (selectedOption) {
                // find the corresponding descriptor
                var selectedDescriptor = null;
                $.each(descriptors, function (_, d) {
                    if (selectedOption.value === d.field) {
                        selectedDescriptor = d;
                        return false;
                    }
                });

                // ensure the descriptor was found
                if (selectedDescriptor !== null) {
                    // clear the current extent if this is a newly selected descriptor
                    if (descriptor === null || descriptor.field !== selectedDescriptor.field) {
                        brushExtent = null;
                    }

                    // record the currently selected descriptor
                    descriptor = selectedDescriptor;

                    // update the selected chart
                    var statusHistory = $('#status-history-dialog').data('status-history');
                    statusHistory.selectedDescriptor = selectedDescriptor;
                    $('#status-history-dialog').data('status-history', statusHistory);

                    // update the chart
                    updateChart();
                }
            }
        });
    };

    var getChartMaxHeight = function () {
        return $('body').height() - $('#status-history-chart-control-container').outerHeight() -
            $('#status-history-refresh-container').outerHeight() -
            parseInt($('#status-history-chart-control-container').css('margin-top'), 10) -
            parseInt($('.dialog-content').css('top')) - parseInt($('.dialog-content').css('bottom'));
    };

    var getChartMaxWidth = function () {
        return $('body').width() - $('#status-history-details').innerWidth() -
            parseInt($('#status-history-dialog').css('left'), 10) -
            parseInt($('.dialog-content').css('left')) - parseInt($('.dialog-content').css('right'));
    };

    /**
     * Updates the chart with the specified status history and the selected field.
     */
    var updateChart = function () {
        var statusHistory = $('#status-history-dialog').data('status-history');

        // get the selected descriptor
        var selectedDescriptor = statusHistory.selectedDescriptor;

        // remove current details
        $('#status-history-details').empty();

        // add status history details
        var detailsContainer = buildDetailsContainer('Status History');
        d3.map(statusHistory.details).forEach(function (label, value) {
            addDetailItem(detailsContainer, label, value);
        });

        var margin = {
            top: 15,
            right: 20,
            bottom: 25,
            left: 75
        };

        // -------------
        // prep the data
        // -------------

        // available colors
        var color = d3.scale.category10();

        // determine the available instances
        var instanceLabels = [];
        $.each(statusHistory.instances, function (_, instance) {
            instanceLabels.push(instance.label);
        });

        // specify the domain based on the detected instances
        color.domain(instanceLabels);

        // data for the chart
        var statusData = [];

        // go through each instance of this status history
        $.each(statusHistory.instances, function (_, instance) {
            // if this is the first time this instance is being rendered, make it visible
            if (nf.Common.isUndefinedOrNull(instances[instance.id])) {
                instances[instance.id] = true;
            }

            // and convert the model
            statusData.push({
                id: instance.id,
                label: instance.label,
                values: $.map(instance.snapshots, function (d) {
                    return {
                        timestamp: d.timestamp,
                        value: d.statusMetrics[selectedDescriptor.field]
                    };
                }),
                visible: instances[instance.id] === true
            });
        });

        // --------------------------
        // custom time axis formatter
        // --------------------------

        var customTimeFormat = d3.time.format.multi([
            [':%S.%L', function (d) {
                return d.getMilliseconds();
            }],
            [':%S', function (d) {
                return d.getSeconds();
            }],
            ['%H:%M', function (d) {
                return d.getMinutes();
            }],
            ['%H:%M', function (d) {
                return d.getHours();
            }],
            ['%a %d', function (d) {
                return d.getDay() && d.getDate() !== 1;
            }],
            ['%b %d', function (d) {
                return d.getDate() !== 1;
            }],
            ['%B', function (d) {
                return d.getMonth();
            }],
            ['%Y', function () {
                return true;
            }]
        ]);

        // ----------
        // main chart
        // ----------

        var statusHistoryDialog = $('#status-history-dialog');

        // show/center the dialog if necessary
        if (!statusHistoryDialog.is(':visible')) {
            statusHistoryDialog.modal('show');
        }

        // the container for the main chart
        var chartContainer = $('#status-history-chart-container').empty();
        if (chartContainer.hasClass('ui-resizable')) {
            chartContainer.resizable('destroy');
        }

        // calculate the dimensions
        var width = chartContainer.parent().outerWidth() - margin.left - margin.right;
        var height = chartContainer.outerHeight() - margin.top - margin.bottom;

        maxWidth = getChartMaxWidth();
        if (width > maxWidth) {
            width = maxWidth;
        }

        maxHeight = getChartMaxHeight();
        if (height > maxHeight) {
            height = maxHeight;
        }

        // define the x axis for the main chart
        var x = d3.time.scale()
            .range([0, width]);

        var xAxis = d3.svg.axis()
            .scale(x)
            .ticks(5)
            .tickFormat(customTimeFormat)
            .orient('bottom');

        // define the y axis
        var y = d3.scale.linear()
            .range([height, 0]);

        var yAxis = d3.svg.axis()
            .scale(y)
            .tickFormat(formatters[selectedDescriptor.formatter])
            .orient('left');

        // status line
        var line = d3.svg.line()
            .interpolate('monotone')
            .x(function (d) {
                return x(d.timestamp);
            })
            .y(function (d) {
                return y(d.value);
            });

        // build the chart svg
        var chartSvg = d3.select('#status-history-chart-container').append('svg')
            .attr('style', 'pointer-events: none;')
            .attr('width', chartContainer.parent().width())
            .attr('height', chartContainer.innerHeight());
        // define a clip the path
        var clipPath = chartSvg.append('defs').append('clipPath')
            .attr('id', 'clip')
            .append('rect')
            .attr('width', width)
            .attr('height', height);

        // build the chart
        var chart = chartSvg.append('g')
            .attr('transform', 'translate(' + margin.left + ', ' + margin.top + ')');

        // determine the min/max date
        var minDate = d3.min(statusData, function (d) {
            return d3.min(d.values, function (s) {
                return s.timestamp;
            });
        });
        var maxDate = d3.max(statusData, function (d) {
            return d3.max(d.values, function (s) {
                return s.timestamp;
            });
        });
        addDetailItem(detailsContainer, 'Start', nf.Common.formatDateTime(minDate));
        addDetailItem(detailsContainer, 'End', nf.Common.formatDateTime(maxDate));

        // determine the x axis range
        x.domain([minDate, maxDate]);

        // determine the y axis range
        y.domain([getMinValue(statusData), getMaxValue(statusData)]);

        // build the x axis
        chart.append('g')
            .attr('class', 'x axis')
            .attr('transform', 'translate(0, ' + height + ')')
            .call(xAxis);

        // build the y axis
        chart.append('g')
            .attr('class', 'y axis')
            .call(yAxis)
            .append('text')
            .attr('transform', 'rotate(-90)')
            .attr('y', 6)
            .attr('dy', '.71em')
            .attr('text-anchor', 'end')
            .text(selectedDescriptor.label);

        // build the chart
        var status = chart.selectAll('.status')
            .data(statusData)
            .enter()
            .append('g')
            .attr('clip-path', 'url(#clip)')
            .attr('class', 'status');

        // draw the lines
        status.append('path')
            .attr('class', function (d) {
                return 'chart-line chart-line-' + d.id;
            })
            .attr('d', function (d) {
                return line(d.values);
            })
            .attr('stroke', function (d) {
                return color(d.label);
            })
            .classed('hidden', function (d) {
                return d.visible === false;
            })
            .append('title')
            .text(function (d) {
                return d.label;
            });

        // draw the control points for each line
        status.each(function (d) {
            // create a group for the control points
            var markGroup = d3.select(this).append('g')
                .attr('class', function () {
                    return 'mark-group mark-group-' + d.id;
                })
                .classed('hidden', function (d) {
                    return d.visible === false;
                });

            // draw the control points
            markGroup.selectAll('circle.mark')
                .data(d.values)
                .enter()
                .append('circle')
                .attr('style', 'pointer-events: all;')
                .attr('class', 'mark')
                .attr('cx', function (v) {
                    return x(v.timestamp);
                })
                .attr('cy', function (v) {
                    return y(v.value);
                })
                .attr('fill', function () {
                    return color(d.label);
                })
                .attr('r', 1.5)
                .append('title')
                .text(function (v) {
                    return d.label + ' -- ' + formatters[selectedDescriptor.formatter](v.value);
                });
        });

        // -------------
        // control chart
        // -------------

        // the container for the main chart control
        var chartControlContainer = $('#status-history-chart-control-container').empty();
        var controlHeight = chartControlContainer.innerHeight() - margin.top - margin.bottom;

        var xControl = d3.time.scale()
            .range([0, width]);

        var xControlAxis = d3.svg.axis()
            .scale(xControl)
            .ticks(5)
            .tickFormat(customTimeFormat)
            .orient('bottom');

        var yControl = d3.scale.linear()
            .range([controlHeight, 0]);

        var yControlAxis = d3.svg.axis()
            .scale(yControl)
            .tickValues(y.domain())
            .tickFormat(formatters[selectedDescriptor.formatter])
            .orient('left');

        // status line
        var controlLine = d3.svg.line()
            .interpolate('monotone')
            .x(function (d) {
                return xControl(d.timestamp);
            })
            .y(function (d) {
                return yControl(d.value);
            });

        // build the svg
        var controlChartSvg = d3.select('#status-history-chart-control-container').append('svg')
            .attr('width', chartContainer.parent().width())
            .attr('height', controlHeight + margin.top + margin.bottom);

        // build the control chart
        var control = controlChartSvg.append('g')
            .attr('transform', 'translate(' + margin.left + ', ' + margin.top + ')');

        // increase the y domain slightly
        var yControlDomain = y.domain();
        yControlDomain[1] *= 1.04;

        // define the domain for the control chart
        xControl.domain(x.domain());
        yControl.domain(yControlDomain);

        // build the control x axis
        control.append('g')
            .attr('class', 'x axis')
            .attr('transform', 'translate(0, ' + controlHeight + ')')
            .call(xControlAxis);

        // build the control y axis
        control.append('g')
            .attr('class', 'y axis')
            .call(yControlAxis);

        // build the control chart
        var controlStatus = control.selectAll('.status')
            .data(statusData)
            .enter()
            .append('g')
            .attr('class', 'status');

        // draw the lines
        controlStatus.append('path')
            .attr('class', function (d) {
                return 'chart-line chart-line-' + d.id;
            })
            .attr('d', function (d) {
                return controlLine(d.values);
            })
            .attr('stroke', function (d) {
                return color(d.label);
            })
            .classed('hidden', function (d) {
                return instances[d.id] === false;
            })
            .append('title')
            .text(function (d) {
                return d.label;
            });

        // -------------------
        // configure the brush
        // -------------------

        /**
         * Updates the axis for the main chart.
         *
         * @param {array} xDomain   The new domain for the x axis
         * @param {array} yDomain   The new domain for the y axis
         */
        var updateAxes = function (xDomain, yDomain) {
            // update the domain of the main chart
            x.domain(xDomain);
            y.domain(yDomain);

            // update the chart lines
            status.selectAll('.chart-line')
                .attr('d', function (d) {
                    return line(d.values);
                });
            status.selectAll('circle.mark')
                .attr('cx', function (v) {
                    return x(v.timestamp);
                })
                .attr('cy', function (v) {
                    return y(v.value);
                })
                .attr('r', function () {
                    return brush.empty() ? 1.5 : 4;
                });

            // update the x axis
            chart.select('.x.axis').call(xAxis);
            chart.select('.y.axis').call(yAxis);
        };

        /**
         * Handles brush events by updating the main chart according to the context window
         * or the control domain if there is no context window.
         */
        var brushed = function () {
            // determine the new x and y domains
            var xContextDomain, yContextDomain;
            if (brush.empty()) {
                // get the all visible instances
                var visibleInstances = $.grep(statusData, function (d) {
                    return d.visible;
                });

                // determine the appropriate y domain
                if (visibleInstances.length === 0) {
                    yContextDomain = yControl.domain();
                } else {
                    yContextDomain = [
                        d3.min(visibleInstances, function (d) {
                            return d3.min(d.values, function (s) {
                                return s.value;
                            });
                        }),
                        d3.max(visibleInstances, function (d) {
                            return d3.max(d.values, function (s) {
                                return s.value;
                            });
                        })
                    ];
                }
                xContextDomain = xControl.domain();

                // clear the current extent
                brushExtent = null;
            } else {
                var extent = brush.extent();
                xContextDomain = [extent[0][0], extent[1][0]];
                yContextDomain = [extent[0][1], extent[1][1]];

                // hold onto the current brush
                brushExtent = extent;
            }

            // update the axes accordingly
            updateAxes(xContextDomain, yContextDomain);

            // update the aggregate statistics according to the new domain
            updateAggregateStatistics();
        };

        // build the brush
        var brush = d3.svg.brush()
            .x(xControl)
            .y(yControl)
            .on('brush', brushed);

        // conditionally set the brush extent
        if (nf.Common.isDefinedAndNotNull(brushExtent)) {
            brush = brush.extent(brushExtent);
        }

        // context area
        control.append('g')
            .attr('class', 'brush')
            .call(brush);

        // add expansion to the extent
        control.select('rect.extent')
            .attr('style', 'pointer-events: all;')
            .on('dblclick', function () {
                if (!brush.empty()) {
                    // get the current extent to get the x range
                    var extent = brush.extent();

                    // get the y range (this value does not change from the original y domain)
                    var yRange = yControl.domain();

                    // expand the extent vertically
                    brush.extent([[extent[0][0], yRange[0]], [extent[1][0], yRange[1]]]);

                    // update the brush control
                    control.select('.brush').call(brush);

                    // run the brush to update the axes of the main chart
                    brushed();
                }
            });

        // --------------------
        // aggregate statistics
        // --------------------

        var updateAggregateStatistics = function () {
            // locate the instances that have data points within the current brush
            var withinBrush = $.map(statusData, function (d) {
                var xDomain = x.domain();
                var yDomain = y.domain();

                // copy to avoid modifying the original
                var copy = $.extend({}, d);

                // update the copy to only include values within the brush
                return $.extend(copy, {
                    values: $.grep(d.values, function (s) {
                        return s.timestamp.getTime() >= xDomain[0].getTime() && s.timestamp.getTime() <= xDomain[1].getTime() && s.value >= yDomain[0] && s.value <= yDomain[1];
                    })
                });
            });

            // consider visible nodes with data in the brush
            var nodes = $.grep(withinBrush, function (d) {
                return d.id !== config.nifiInstanceId && d.visible && d.values.length > 0;
            });

            var nodeMinValue = nodes.length === 0 ? 'NA' : formatters[selectedDescriptor.formatter](getMinValue(nodes));
            var nodeMeanValue = nodes.length === 0 ? 'NA' : formatters[selectedDescriptor.formatter](getMeanValue(nodes));
            var nodeMaxValue = nodes.length === 0 ? 'NA' : formatters[selectedDescriptor.formatter](getMaxValue(nodes));

            // update the currently displayed min/max/mean
            $('#node-aggregate-statistics').text(nodeMinValue + ' / ' + nodeMaxValue + ' / ' + nodeMeanValue);

            // only consider the cluster with data in the brush
            var cluster = $.grep(withinBrush, function (d) {
                return d.id === config.nifiInstanceId && d.visible && d.values.length > 0;
            });

            // determine the cluster values
            var clusterMinValue = cluster.length === 0 ? 'NA' : formatters[selectedDescriptor.formatter](getMinValue(cluster));
            var clusterMeanValue = cluster.length === 0 ? 'NA' : formatters[selectedDescriptor.formatter](getMeanValue(cluster));
            var clusterMaxValue = cluster.length === 0 ? 'NA' : formatters[selectedDescriptor.formatter](getMaxValue(cluster));

            // update the cluster min/max/mean
            $('#cluster-aggregate-statistics').text(clusterMinValue + ' / ' + clusterMaxValue + ' / ' + clusterMeanValue);
        };

        // ----------------
        // build the legend
        // ----------------

        // identify all nodes and sort
        var nodes = $.grep(statusData, function (status) {
            return status.id !== config.nifiInstanceId;
        }).sort(function (a, b) {
            return a.label < b.label ? -1 : a.label > b.label ? 1 : 0;
        });

        // adds a legend entry for the specified instance
        var addLegendEntry = function (legend, instance) {
            // create the label and the checkbox
            var instanceLabelElement = $('<div></div>').addClass('legend-label').css('color', color(instance.label)).text(instance.label).ellipsis();
            var instanceCheckboxElement = $('<div class="nf-checkbox"></div>').on('click', function () {
                // get the line and the control points for this instance (select all for the line to update control and main charts)
                var chartLine = d3.selectAll('path.chart-line-' + instance.id);
                var markGroup = d3.select('g.mark-group-' + instance.id);

                // determine if it was hidden
                var isHidden = markGroup.classed('hidden');

                // toggle the visibility
                chartLine.classed('hidden', function () {
                    return !isHidden;
                });
                markGroup.classed('hidden', function () {
                    return !isHidden;
                });

                // update whether its visible
                instance.visible = isHidden;

                // record the current status so it persists across refreshes
                instances[instance.id] = instance.visible;

                // update the brush
                brushed();
            }).addClass(instance.visible ? 'checkbox-checked' : 'checkbox-unchecked');

            // add the legend entry
            $('<div class="legend-entry"></div>').append(instanceCheckboxElement).append(instanceLabelElement).on('mouseenter', function () {
                d3.selectAll('path.chart-line-' + instance.id).classed('over', true);
            }).on('mouseleave', function () {
                d3.selectAll('path.chart-line-' + instance.id).classed('over', false);
            }).appendTo(legend);
        };

        // get the cluster instance
        var cluster = $.grep(statusData, function (status) {
            return status.id === config.nifiInstanceId;
        });

        // build the cluster container
        var clusterDetailsContainer = buildDetailsContainer('NiFi');

        // add the total cluster values
        addDetailItem(clusterDetailsContainer, 'Min / Max / Mean', '', 'cluster-aggregate-statistics');

        // build the cluster legend
        addLegendEntry(clusterDetailsContainer, cluster[0]);

        // if there are entries to render
        if (nodes.length > 0) {
            // build the cluster container
            var nodeDetailsContainer = buildDetailsContainer('Nodes');

            // add the total cluster values
            addDetailItem(nodeDetailsContainer, 'Min / Max / Mean', '', 'node-aggregate-statistics');

            // add each legend entry
            $.each(nodes, function (_, instance) {
                addLegendEntry(nodeDetailsContainer, instance);
            });
        }

        // update the brush
        brushed();

        // ---------------
        // handle resizing
        // ---------------

        var maxWidth, maxHeight, resizeExtent, dialog;
        chartContainer.append('<div class="ui-resizable-handle ui-resizable-se"></div>').resizable({
            minWidth: 425,
            minHeight: 150,
            handles: {
                'se': '.ui-resizable-se'
            },
            start: function (e, ui) {
                // record the current extent so it can be reset on stop
                if (!brush.empty()) {
                    resizeExtent = brush.extent();
                }
            },
            resize: function (e, ui) {
                // -----------
                // containment
                // -----------
                dialog = $('#status-history-dialog');
                var nfDialog = {};
                if (nf.Common.isDefinedAndNotNull(dialog.data('nf-dialog'))) {
                    nfDialog = dialog.data('nf-dialog');
                }
                nfDialog['min-width'] = (dialog.width()/$(window).width())*100 + '%';
                nfDialog['min-height'] = (dialog.height()/$(window).height())*100 + '%';
                nfDialog.responsive['fullscreen-width'] = dialog.outerWidth() + 'px';
                nfDialog.responsive['fullscreen-height'] = dialog.outerHeight() + 'px';

                maxWidth = getChartMaxWidth();
                if (ui.helper.width() > maxWidth) {
                    ui.helper.width(maxWidth);

                    nfDialog.responsive['fullscreen-width'] = $(window).width() + 'px';
                    nfDialog['min-width'] = '100%';
                }

                maxHeight = getChartMaxHeight();
                if (ui.helper.height() > maxHeight) {
                    ui.helper.height(maxHeight);

                    nfDialog.responsive['fullscreen-height'] = $(window).height() + 'px';
                    nfDialog['min-height'] = '100%';
                }

                nfDialog['min-width'] = (parseInt(nfDialog['min-width'], 10) >= 100) ? '100%': nfDialog['min-width'];
                nfDialog['min-height'] = (parseInt(nfDialog['min-height'], 10) >= 100) ? '100%': nfDialog['min-height'];

                //persist data attribute
                dialog.data('nfDialog', nfDialog);

                // ----------------------
                // status history dialog
                // ----------------------

                dialog.css('min-width', (chartContainer.outerWidth() + $('#status-history-details').outerWidth() + 40));
                dialog.css('min-height', (chartContainer.outerHeight() +
                    $('#status-history-refresh-container').outerHeight() + $('#status-history-chart-control-container').outerHeight() +
                    $('.dialog-buttons').outerHeight() + $('.dialog-header').outerHeight() + 40 + 5));

                dialog.center();
            },
            stop: function () {

                // ----------
                // main chart
                // ----------

                // determine the new width/height
                width = chartContainer.outerWidth() - margin.left - margin.right;
                height = chartContainer.outerHeight() - margin.top - margin.bottom;

                // update the range
                x.range([0, width]);
                y.range([height, 0]);

                // update the size of the chart
                chartSvg.attr('width', chartContainer.parent().width())
                    .attr('height', chartContainer.innerHeight());

                // update the size of the clipper
                clipPath.attr('width', width)
                    .attr('height', height);

                // update the position of the x axis
                chart.select('.x.axis').attr('transform', 'translate(0, ' + height + ')');

                // -------------
                // control chart
                // -------------

                // determine the new width/height
                controlHeight = chartControlContainer.height() - margin.top - margin.bottom;

                // update the range
                xControl.range([0, width]);
                yControl.range([controlHeight, 0]);

                // update the size of the control chart
                controlChartSvg.attr('width', chartContainer.parent().width())
                    .attr('height', controlHeight + margin.top + margin.bottom);

                // update the chart lines
                controlStatus.selectAll('.chart-line').attr('d', function (d) {
                    return controlLine(d.values);
                });

                // update the axes
                control.select('.x.axis').call(xControlAxis);
                control.select('.y.axis').call(yControlAxis);

                // restore the extent if necessary
                if (nf.Common.isDefinedAndNotNull(resizeExtent)) {
                    brush.extent(resizeExtent);
                }

                // update the brush
                control.select('.brush').call(brush);

                // invoking the brush will trigger appropriate redrawing of the main chart
                brushed();

                // reset the resize extent
                resizeExtent = null;

                // resize chart container
                chartContainer.width(Math.round( chartContainer.parent().width()));
                chartControlContainer.width(Math.round( chartContainer.parent().width()));

                // toggle scrollable style
                nf.Common.toggleScrollable(dialog.find('.dialog-content').get(0));
            }
        });
    };

    /**
     * Gets the minimum value from the specified instances.
     *
     * @param {type} nodeInstances
     */
    var getMinValue = function (nodeInstances) {
        return d3.min(nodeInstances, function (d) {
            return d3.min(d.values, function (s) {
                return s.value;
            });
        });
    };

    /**
     * Gets the maximum value from the specified instances.
     *
     * @param {type} nodeInstances
     */
    var getMaxValue = function (nodeInstances) {
        return d3.max(nodeInstances, function (d) {
            return d3.max(d.values, function (s) {
                return s.value;
            });
        });
    };

    /**
     * Gets the mean value from the specified instances.
     *
     * @param {type} nodeInstances
     */
    var getMeanValue = function (nodeInstances) {
        var snapshotCount = 0;
        var totalValue = d3.sum(nodeInstances, function (d) {
            snapshotCount += d.values.length;
            return d3.sum(d.values, function (s) {
                return s.value;
            });
        });
        return totalValue / snapshotCount;
    };

    /**
     * Builds a details container.
     *
     * @param {type} label
     */
    var buildDetailsContainer = function (label) {
        var detailContainer = $('<div class="status-history-detail"></div>').appendTo('#status-history-details');
        $('<div class="detail-container-label"></div>').text(label).appendTo(detailContainer);
        return $('<div></div>').appendTo(detailContainer);
    };

    /**
     * Adds a detail item and specified container.
     *
     * @param {type} container
     * @param {type} label
     * @param {type} value
     * @param {type} valueElementId
     */
    var addDetailItem = function (container, label, value, valueElementId) {
        var detailContainer = $('<div class="detail-item"></div>').appendTo(container);
        $('<div class="setting-name"></div>').text(label).appendTo(detailContainer);
        var detailElement = $('<div class="setting-field"></div>').text(value).appendTo(detailContainer);

        if (nf.Common.isDefinedAndNotNull(valueElementId)) {
            detailElement.attr('id', valueElementId);
        }
    };

    return {
        /**
         * Initializes the lineage graph.
         *
         * @param {integer} timeOffset The time offset of the server
         */
        init: function (timeOffset) {
            serverTimeOffset = timeOffset;

            $('#status-history-refresh-button').click(function () {
                var statusHistory = $('#status-history-dialog').data('status-history');
                if (statusHistory !== null) {
                    if (statusHistory.type === config.type.processor) {
                        nf.StatusHistory.showProcessorChart(statusHistory.groupId, statusHistory.id, statusHistory.selectedDescriptor);
                    } else if (statusHistory.type === config.type.processGroup) {
                        nf.StatusHistory.showProcessGroupChart(statusHistory.groupId, statusHistory.id, statusHistory.selectedDescriptor);
                    } else if (statusHistory.type === config.type.remoteProcessGroup) {
                        nf.StatusHistory.showRemoteProcessGroupChart(statusHistory.groupId, statusHistory.id, statusHistory.selectedDescriptor);
                    } else {
                        nf.StatusHistory.showConnectionChart(statusHistory.groupId, statusHistory.id, statusHistory.selectedDescriptor);
                    }
                }
            });

            // configure the dialog and make it draggable
            $('#status-history-dialog').modal({
                scrollableContentStyle: 'scrollable',
                headerText: "Status History",
                buttons: [{
                    buttonText: 'Close',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            this.modal('hide');
                        }
                    }
                }],
                handler: {
                    close: function () {
                        // remove the current status history
                        $('#status-history-dialog').removeData('status-history');

                        // reset the dom
                        $('#status-history-chart-container').empty();
                        $('#status-history-chart-control-container').empty();
                        $('#status-history-details').empty();

                        // clear the extent and selected descriptor
                        brushExtent = null;
                        descriptor = null;
                        instances = null;
                    }
                }
            });
        },

        /**
         * Shows the status history for the specified connection in this instance.
         *
         * @param {type} groupId
         * @param {type} connectionId
         * @param {type} selectedDescriptor
         */
        showConnectionChart: function (groupId, connectionId, selectedDescriptor) {
            $.ajax({
                type: 'GET',
                url: config.urls.api + '/flow/connections/' + encodeURIComponent(connectionId) + '/status/history',
                dataType: 'json'
            }).done(function (response) {
                handleStatusHistoryResponse(groupId, connectionId, response.statusHistory, config.type.connection, selectedDescriptor);
            }).fail(nf.Common.handleAjaxError);
        },

        /**
         * Shows the status history for the specified processor in this instance.
         *
         * @param {type} groupId
         * @param {type} processorId
         * @param {type} selectedDescriptor
         */
        showProcessorChart: function (groupId, processorId, selectedDescriptor) {
            $.ajax({
                type: 'GET',
                url: config.urls.api + '/flow/processors/' + encodeURIComponent(processorId) + '/status/history',
                dataType: 'json'
            }).done(function (response) {
                handleStatusHistoryResponse(groupId, processorId, response.statusHistory, config.type.processor, selectedDescriptor);
            }).fail(nf.Common.handleAjaxError);
        },

        /**
         * Shows the status history for the specified process group in this instance.
         *
         * @param {type} groupId
         * @param {type} processGroupId
         * @param {type} selectedDescriptor
         */
        showProcessGroupChart: function (groupId, processGroupId, selectedDescriptor) {
            $.ajax({
                type: 'GET',
                url: config.urls.api + '/flow/process-groups/' + encodeURIComponent(processGroupId) + '/status/history',
                dataType: 'json'
            }).done(function (response) {
                handleStatusHistoryResponse(groupId, processGroupId, response.statusHistory, config.type.processGroup, selectedDescriptor);
            }).fail(nf.Common.handleAjaxError);
        },

        /**
         * Shows the status history for the specified remote process group in this instance.
         *
         * @param {type} groupId
         * @param {type} remoteProcessGroupId
         * @param {type} selectedDescriptor
         */
        showRemoteProcessGroupChart: function (groupId, remoteProcessGroupId, selectedDescriptor) {
            $.ajax({
                type: 'GET',
                url: config.urls.api + '/flow/remote-process-groups/' + encodeURIComponent(remoteProcessGroupId) + '/status/history',
                dataType: 'json'
            }).done(function (response) {
                handleStatusHistoryResponse(groupId, remoteProcessGroupId, response.statusHistory, config.type.remoteProcessGroup, selectedDescriptor);
            }).fail(nf.Common.handleAjaxError);
        }
    };
}());