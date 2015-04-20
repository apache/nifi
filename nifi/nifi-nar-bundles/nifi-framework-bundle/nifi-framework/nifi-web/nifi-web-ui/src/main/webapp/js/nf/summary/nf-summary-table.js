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
/* global nf, top, Slick */

nf.SummaryTable = (function () {

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        filterText: 'Filter',
        styles: {
            filterList: 'summary-filter-list'
        },
        urls: {
            status: '../nifi-api/controller/process-groups/root/status',
            processGroups: '../nifi-api/controller/process-groups/',
            clusterProcessor: '../nifi-api/cluster/processors/',
            clusterConnection: '../nifi-api/cluster/connections/',
            clusterInputPort: '../nifi-api/cluster/input-ports/',
            clusterOutputPort: '../nifi-api/cluster/output-ports/',
            clusterRemoteProcessGroup: '../nifi-api/cluster/remote-process-groups/',
            systemDiagnostics: '../nifi-api/system-diagnostics',
            controllerConfig: '../nifi-api/controller/config',
            d3Script: 'js/d3/d3.min.js',
            statusHistory: 'js/nf/nf-status-history.js'
        }
    };

    /**
     * Loads the lineage capabilities when the current browser supports SVG.
     */
    var loadChartCapabilities = function () {
        return $.Deferred(function (deferred) {
            if (nf.Common.SUPPORTS_SVG) {
                nf.Common.cachedScript(config.urls.d3Script).done(function () {
                    // get the controller config to get the server offset
                    var configRequest = $.ajax({
                        type: 'GET',
                        url: config.urls.controllerConfig,
                        dataType: 'json'
                    });

                    // get the config details and load the chart script
                    $.when(configRequest, nf.Common.cachedScript(config.urls.statusHistory)).done(function (response) {
                        var configResponse = response[0];
                        var configDetails = configResponse.config;

                        // initialize the chart
                        nf.StatusHistory.init(configDetails.timeOffset);
                        deferred.resolve();
                    }).fail(function () {
                        deferred.reject();
                    });
                }).fail(function () {
                    deferred.reject();
                });
            } else {
                deferred.resolve();
            }
        }).promise();
    };

    /**
     * Goes to the specified component if possible.
     * 
     * @argument {string} groupId       The id of the group
     * @argument {string} componentId   The id of the component
     */
    var goTo = function (groupId, componentId) {
        // only attempt this if we're within a frame
        if (top !== window) {
            // and our parent has canvas utils and shell defined
            if (nf.Common.isDefinedAndNotNull(parent.nf) && nf.Common.isDefinedAndNotNull(parent.nf.CanvasUtils) && nf.Common.isDefinedAndNotNull(parent.nf.Shell)) {
                parent.nf.CanvasUtils.showComponent(groupId, componentId);
                parent.$('#shell-close-button').click();
            }
        }
    };

    /**
     * Initializes the summary table.
     * 
     * @param {type} isClustered
     */
    var initSummaryTable = function (isClustered) {
        // define the function for filtering the list
        $('#summary-filter').keyup(function () {
            applyFilter();
        }).focus(function () {
            if ($(this).hasClass(config.styles.filterList)) {
                $(this).removeClass(config.styles.filterList).val('');
            }
        }).blur(function () {
            if ($(this).val() === '') {
                $(this).addClass(config.styles.filterList).val(config.filterText);
            }
        }).addClass(config.styles.filterList).val(config.filterText);

        // initialize the summary tabs
        $('#summary-tabs').tabbs({
            tabStyle: 'summary-tab',
            selectedTabStyle: 'summary-selected-tab',
            tabs: [{
                    name: 'Processors',
                    tabContentId: 'processor-summary-tab-content'
                }, {
                    name: 'Input Ports',
                    tabContentId: 'input-port-summary-tab-content'
                }, {
                    name: 'Output Ports',
                    tabContentId: 'output-port-summary-tab-content'
                }, {
                    name: 'Remote Process Groups',
                    tabContentId: 'remote-process-group-summary-tab-content'
                }, {
                    name: 'Connections',
                    tabContentId: 'connection-summary-tab-content'
                }],
            select: function () {
                var tab = $(this).text();
                if (tab === 'Processors') {
                    // ensure the processor table is sized properly
                    var processorsGrid = $('#processor-summary-table').data('gridInstance');
                    if (nf.Common.isDefinedAndNotNull(processorsGrid)) {
                        processorsGrid.resizeCanvas();

                        // update the total number of processors
                        $('#displayed-items').text(nf.Common.formatInteger(processorsGrid.getData().getLength()));
                        $('#total-items').text(nf.Common.formatInteger(processorsGrid.getData().getLength()));
                    }

                    // update the combo for processors
                    $('#summary-filter-type').combo({
                        options: [{
                                text: 'by name',
                                value: 'name'
                            }, {
                                text: 'by type',
                                value: 'type'
                            }],
                        select: function (option) {
                            applyFilter();
                        }
                    });
                } else if (tab === 'Connections') {
                    // ensure the connection table is size properly
                    var connectionsGrid = $('#connection-summary-table').data('gridInstance');
                    if (nf.Common.isDefinedAndNotNull(connectionsGrid)) {
                        connectionsGrid.resizeCanvas();

                        // update the total number of connections
                        $('#displayed-items').text(nf.Common.formatInteger(connectionsGrid.getData().getLength()));
                        $('#total-items').text(nf.Common.formatInteger(connectionsGrid.getData().getLength()));
                    }

                    // update the combo for connections
                    $('#summary-filter-type').combo({
                        options: [{
                                text: 'by source',
                                value: 'sourceName'
                            }, {
                                text: 'by name',
                                value: 'name'
                            }, {
                                text: 'by destination',
                                value: 'destinationName'
                            }],
                        select: function (option) {
                            applyFilter();
                        }
                    });
                } else if (tab === 'Input Ports') {
                    // ensure the connection table is size properly
                    var inputPortsGrid = $('#input-port-summary-table').data('gridInstance');
                    if (nf.Common.isDefinedAndNotNull(inputPortsGrid)) {
                        inputPortsGrid.resizeCanvas();

                        // update the total number of connections
                        $('#displayed-items').text(nf.Common.formatInteger(inputPortsGrid.getData().getLength()));
                        $('#total-items').text(nf.Common.formatInteger(inputPortsGrid.getData().getLength()));
                    }

                    // update the combo for connections
                    $('#summary-filter-type').combo({
                        options: [{
                                text: 'by name',
                                value: 'name'
                            }],
                        select: function (option) {
                            applyFilter();
                        }
                    });
                } else if (tab === 'Output Ports') {
                    // ensure the connection table is size properly
                    var outputPortsGrid = $('#output-port-summary-table').data('gridInstance');
                    if (nf.Common.isDefinedAndNotNull(outputPortsGrid)) {
                        outputPortsGrid.resizeCanvas();

                        // update the total number of connections
                        $('#displayed-items').text(nf.Common.formatInteger(outputPortsGrid.getData().getLength()));
                        $('#total-items').text(nf.Common.formatInteger(outputPortsGrid.getData().getLength()));
                    }

                    // update the combo for connections
                    $('#summary-filter-type').combo({
                        options: [{
                                text: 'by name',
                                value: 'name'
                            }],
                        select: function (option) {
                            applyFilter();
                        }
                    });
                } else {
                    // ensure the connection table is size properly
                    var remoteProcessGroupsGrid = $('#remote-process-group-summary-table').data('gridInstance');
                    if (nf.Common.isDefinedAndNotNull(remoteProcessGroupsGrid)) {
                        remoteProcessGroupsGrid.resizeCanvas();

                        // update the total number of connections
                        $('#displayed-items').text(nf.Common.formatInteger(remoteProcessGroupsGrid.getData().getLength()));
                        $('#total-items').text(nf.Common.formatInteger(remoteProcessGroupsGrid.getData().getLength()));
                    }

                    // update the combo for connections
                    $('#summary-filter-type').combo({
                        options: [{
                                text: 'by name',
                                value: 'name'
                            }, {
                                text: 'by uri',
                                value: 'targetUri'
                            }],
                        select: function (option) {
                            applyFilter();
                        }
                    });
                }

                // reset the filter
                $('#summary-filter').addClass(config.styles.filterList).val(config.filterText);
                applyFilter();
            }
        });

        // listen for browser resize events to update the page size
        $(window).resize(function () {
            nf.SummaryTable.resetTableSize();
        });

        // define a custom formatter for showing more processor details
        var moreProcessorDetails = function (row, cell, value, columnDef, dataContext) {
            var markup = '<img src="images/iconDetails.png" title="View Details" class="pointer show-processor-details" style="margin-top: 5px; float: left;"/>';

            // if there are bulletins, render them on the graph
            if (!nf.Common.isEmpty(dataContext.bulletins)) {
                markup += '<img src="images/iconBulletin.png" class="has-bulletins" style="margin-top: 5px; margin-left: 5px; float: left;"/><span class="hidden row-id">' + nf.Common.escapeHtml(dataContext.id) + '</span>';
            }

            return markup;
        };

        // formatter for io
        var ioFormatter = function (row, cell, value, columnDef, dataContext) {
            return dataContext.read + ' / ' + dataContext.written;
        };

        // formatter for tasks
        var taskTimeFormatter = function (row, cell, value, columnDef, dataContext) {
            return nf.Common.formatInteger(dataContext.tasks) + ' / ' + dataContext.tasksDuration;
        };

        // function for formatting the last accessed time
        var valueFormatter = function (row, cell, value, columnDef, dataContext) {
            return nf.Common.formatValue(value);
        };

        // define a custom formatter for the run status column
        var runStatusFormatter = function (row, cell, value, columnDef, dataContext) {
            var activeThreadCount = '';
            if (nf.Common.isDefinedAndNotNull(dataContext.activeThreadCount) && dataContext.activeThreadCount > 0) {
                activeThreadCount = '(' + dataContext.activeThreadCount + ')';
            }
            var formattedValue = '<div class="' + nf.Common.escapeHtml(value.toLowerCase()) + '" style="margin-top: 3px;"></div>';
            return formattedValue + '<div class="status-text" style="margin-top: 4px;">' + nf.Common.escapeHtml(value) + '</div><div style="float: left; margin-left: 4px;">' + nf.Common.escapeHtml(activeThreadCount) + '</div>';
        };

        // define the input, read, written, and output columns (reused between both tables)
        var nameColumn = {id: 'name', field: 'name', name: 'Name', sortable: true, resizable: true};
        var runStatusColumn = {id: 'runStatus', field: 'runStatus', name: 'Run Status', formatter: runStatusFormatter, sortable: true};
        var inputColumn = {id: 'input', field: 'input', name: '<span class="input-title">In</span>&nbsp;/&nbsp;<span class="input-size-title">Size</span>&nbsp;<span style="font-weight: normal; overflow: hidden;">5 min</span>', toolTip: 'Count / data size in the last 5 min', sortable: true, defaultSortAsc: false, resizable: true};
        var ioColumn = {id: 'io', field: 'io', name: '<span class="read-title">Read</span>&nbsp;/&nbsp;<span class="written-title">Write</span>&nbsp;<span style="font-weight: normal; overflow: hidden;">5 min</span>', toolTip: 'Data size in the last 5 min', formatter: ioFormatter, sortable: true, defaultSortAsc: false, resizable: true};
        var outputColumn = {id: 'output', field: 'output', name: '<span class="output-title">Out</span>&nbsp;/&nbsp;<span class="output-size-title">Size</span>&nbsp;<span style="font-weight: normal; overflow: hidden;">5 min</span>', toolTip: 'Count / data size in the last 5 min', sortable: true, defaultSortAsc: false, resizable: true};
        var tasksTimeColumn = {id: 'tasks', field: 'tasks', name: '<span class="tasks-title">Tasks</span>&nbsp;/&nbsp;<span class="time-title">Time</span>&nbsp;<span style="font-weight: normal; overflow: hidden;">5 min</span>', toolTip: 'Count / duration in the last 5 min', formatter: taskTimeFormatter, sortable: true, defaultSortAsc: false, resizable: true};

        // define the column model for the processor summary table
        var processorsColumnModel = [
            {id: 'moreDetails', field: 'moreDetails', name: '&nbsp;', resizable: false, formatter: moreProcessorDetails, sortable: true, width: 50, maxWidth: 50},
            nameColumn,
            {id: 'type', field: 'type', name: 'Type', sortable: true, resizable: true},
            runStatusColumn,
            inputColumn,
            ioColumn,
            outputColumn,
            tasksTimeColumn
        ];

        // initialize the search field if applicable
        if (isClustered) {
            nf.ClusterSearch.init();
        }

        // determine if the this page is in the shell
        var isInShell = (top !== window);

        // add an action column if appropriate
        if (isClustered || isInShell || nf.Common.SUPPORTS_SVG) {
            // define how the column is formatted
            var processorActionFormatter = function (row, cell, value, columnDef, dataContext) {
                var markup = '';

                if (isInShell) {
                    markup += '<img src="images/iconGoTo.png" title="Go To" class="pointer go-to" style="margin-top: 2px;"/>&nbsp;';
                }

                if (nf.Common.SUPPORTS_SVG) {
                    if (isClustered) {
                        markup += '<img src="images/iconChart.png" title="Show History" class="pointer show-cluster-processor-status-history" style="margin-top: 2px;"/>&nbsp;';
                    } else {
                        markup += '<img src="images/iconChart.png" title="Show History" class="pointer show-processor-status-history" style="margin-top: 2px;"/>&nbsp;';
                    }
                }

                if (isClustered) {
                    markup += '<img src="images/iconClusterSmall.png" title="Show Details" class="pointer show-cluster-processor-summary" style="margin-top: 2px;"/>&nbsp;';
                }

                return markup;
            };

            // define the action column for clusters and within the shell
            processorsColumnModel.push({id: 'actions', name: '&nbsp;', formatter: processorActionFormatter, resizable: false, sortable: false, width: 75, maxWidth: 75});
        }

        // initialize the templates table
        var processorsOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            autoEdit: false,
            multiSelect: false
        };

        // initialize the dataview
        var processorsData = new Slick.Data.DataView({
            inlineFilters: false
        });
        processorsData.setItems([]);
        processorsData.setFilterArgs({
            searchString: '',
            property: 'name'
        });
        processorsData.setFilter(filter);

        // initialize the sort
        sort('processor-summary-table', {
            columnId: 'name',
            sortAsc: true
        }, processorsData);

        // initialize the grid
        var processorsGrid = new Slick.Grid('#processor-summary-table', processorsData, processorsColumnModel, processorsOptions);
        processorsGrid.setSelectionModel(new Slick.RowSelectionModel());
        processorsGrid.registerPlugin(new Slick.AutoTooltips());
        processorsGrid.setSortColumn('name', true);
        processorsGrid.onSort.subscribe(function (e, args) {
            sort('processor-summary-table', {
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, processorsData);
        });

        // configure a click listener
        processorsGrid.onClick.subscribe(function (e, args) {
            var target = $(e.target);

            // get the node at this row
            var item = processorsData.getItem(args.row);

            // determine the desired action
            if (processorsGrid.getColumns()[args.cell].id === 'actions') {
                if (target.hasClass('go-to')) {
                    goTo(item.groupId, item.id);
                } else if (target.hasClass('show-cluster-processor-status-history')) {
                    nf.StatusHistory.showClusterProcessorChart(item.groupId, item.id);
                } else if (target.hasClass('show-processor-status-history')) {
                    nf.StatusHistory.showStandaloneProcessorChart(item.groupId, item.id);
                } else if (target.hasClass('show-cluster-processor-summary')) {
                    // load the cluster processor summary
                    loadClusterProcessorSummary(item.id);

                    // hide the summary loading indicator
                    $('#summary-loading-container').hide();

                    // show the dialog
                    $('#cluster-processor-summary-dialog').modal('show');
                }
            } else if (processorsGrid.getColumns()[args.cell].id === 'moreDetails') {
                if (target.hasClass('show-processor-details')) {
                    nf.ProcessorDetails.showDetails(item.groupId, item.id);
                }
            }
        });

        // wire up the dataview to the grid
        processorsData.onRowCountChanged.subscribe(function (e, args) {
            processorsGrid.updateRowCount();
            processorsGrid.render();

            // update the total number of displayed processors if necessary
            if ($('#processor-summary-table').is(':visible')) {
                $('#displayed-items').text(nf.Common.formatInteger(args.current));
            }
        });
        processorsData.onRowsChanged.subscribe(function (e, args) {
            processorsGrid.invalidateRows(args.rows);
            processorsGrid.render();
        });

        // hold onto an instance of the grid
        $('#processor-summary-table').data('gridInstance', processorsGrid).on('mouseenter', 'div.slick-cell', function (e) {
            var bulletinIcon = $(this).find('img.has-bulletins');
            if (bulletinIcon.length && !bulletinIcon.data('qtip')) {
                var processorId = $(this).find('span.row-id').text();

                // get the status item
                var item = processorsData.getItemById(processorId);

                // format the tooltip
                var bulletins = nf.Common.getFormattedBulletins(item.bulletins);
                var tooltip = nf.Common.formatUnorderedList(bulletins);

                // show the tooltip
                if (nf.Common.isDefinedAndNotNull(tooltip)) {
                    bulletinIcon.qtip($.extend({
                        content: tooltip,
                        position: {
                            target: 'mouse',
                            viewport: $(window),
                            adjust: {
                                x: 8,
                                y: 8,
                                method: 'flipinvert flipinvert'
                            }
                        }
                    }, nf.Common.config.tooltipConfig));
                }
            }
        });

        // initialize the cluster processor summary dialog
        $('#cluster-processor-summary-dialog').modal({
            headerText: 'Cluster Processor Summary',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Close',
                    handler: {
                        click: function () {
                            // clear the cluster processor summary dialog
                            $('#cluster-processor-id').text('');
                            $('#cluster-processor-name').text('');

                            // close the dialog
                            this.modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // show the summary loading container
                    $('#summary-loading-container').show();
                }
            }
        });
        
        // cluster processor refresh
        nf.Common.addHoverEffect('#cluster-processor-refresh-button', 'button-refresh', 'button-refresh-hover').click(function () {
            loadClusterProcessorSummary($('#cluster-processor-id').text());
        });

        // initialize the cluster processor column model
        var clusterProcessorsColumnModel = [
            {id: 'node', field: 'node', name: 'Node', sortable: true, resizable: true},
            runStatusColumn,
            inputColumn,
            ioColumn,
            outputColumn,
            tasksTimeColumn
        ];

        // initialize the options for the cluster processors table
        var clusterProcessorsOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            autoEdit: false,
            multiSelect: false
        };

        // initialize the dataview
        var clusterProcessorsData = new Slick.Data.DataView({
            inlineFilters: false
        });
        clusterProcessorsData.setItems([]);

        // initialize the sort
        sort('cluster-processor-summary-table', {
            columnId: 'node',
            sortAsc: true
        }, clusterProcessorsData);

        // initialize the grid
        var clusterProcessorsGrid = new Slick.Grid('#cluster-processor-summary-table', clusterProcessorsData, clusterProcessorsColumnModel, clusterProcessorsOptions);
        clusterProcessorsGrid.setSelectionModel(new Slick.RowSelectionModel());
        clusterProcessorsGrid.registerPlugin(new Slick.AutoTooltips());
        clusterProcessorsGrid.setSortColumn('node', true);
        clusterProcessorsGrid.onSort.subscribe(function (e, args) {
            sort('cluster-processor-summary-table', {
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, clusterProcessorsData);
        });

        // wire up the dataview to the grid
        clusterProcessorsData.onRowCountChanged.subscribe(function (e, args) {
            clusterProcessorsGrid.updateRowCount();
            clusterProcessorsGrid.render();
        });
        clusterProcessorsData.onRowsChanged.subscribe(function (e, args) {
            clusterProcessorsGrid.invalidateRows(args.rows);
            clusterProcessorsGrid.render();
        });

        // hold onto an instance of the grid
        $('#cluster-processor-summary-table').data('gridInstance', clusterProcessorsGrid);

        // define a custom formatter for showing more processor details
        var moreConnectionDetails = function (row, cell, value, columnDef, dataContext) {
            return '<img src="images/iconDetails.png" title="View Details" class="pointer show-connection-details" style="margin-top: 5px;"/>';
        };

        // define the input, read, written, and output columns (reused between both tables)
        var queueColumn = {id: 'queued', field: 'queued', name: '<span class="queued-title">Queue</span>&nbsp;/&nbsp;<span class="queued-size-title">Size</span>', sortable: true, defaultSortAsc: false, resize: true};

        // define the column model for the summary table
        var connectionsColumnModel = [
            {id: 'moreDetails', name: '&nbsp;', sortable: false, resizable: false, formatter: moreConnectionDetails, width: 50, maxWidth: 50},
            {id: 'sourceName', field: 'sourceName', name: 'Source Name', sortable: true, resizable: true},
            {id: 'name', field: 'name', name: 'Name', sortable: true, resizable: true, formatter: valueFormatter},
            {id: 'destinationName', field: 'destinationName', name: 'Destination Name', sortable: true, resizable: true},
            inputColumn,
            queueColumn,
            outputColumn
        ];

        // add an action column if appropriate
        if (isClustered || isInShell || nf.Common.SUPPORTS_SVG) {
            // define how the column is formatted
            var connectionActionFormatter = function (row, cell, value, columnDef, dataContext) {
                var markup = '';

                if (isInShell) {
                    markup += '<img src="images/iconGoTo.png" title="Go To" class="pointer go-to" style="margin-top: 2px;"/>&nbsp;';
                }

                if (nf.Common.SUPPORTS_SVG) {
                    if (isClustered) {
                        markup += '<img src="images/iconChart.png" title="Show History" class="pointer show-cluster-connection-status-history" style="margin-top: 2px;"/>&nbsp;';
                    } else {
                        markup += '<img src="images/iconChart.png" title="Show History" class="pointer show-connection-status-history" style="margin-top: 2px;"/>&nbsp;';
                    }
                }

                if (isClustered) {
                    markup += '<img src="images/iconClusterSmall.png" title="Show Details" class="pointer show-cluster-connection-summary" style="margin-top: 2px;"/>&nbsp;';
                }

                return markup;
            };

            // define the action column for clusters and within the shell
            connectionsColumnModel.push({id: 'actions', name: '&nbsp;', formatter: connectionActionFormatter, resizable: false, sortable: false, width: 75, maxWidth: 75});
        }

        // initialize the templates table
        var connectionsOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            autoEdit: false,
            multiSelect: false
        };

        // initialize the dataview
        var connectionsData = new Slick.Data.DataView({
            inlineFilters: false
        });
        connectionsData.setItems([]);
        connectionsData.setFilterArgs({
            searchString: '',
            property: 'sourceName'
        });
        connectionsData.setFilter(filter);

        // initialize the sort
        sort('connection-summary-table', {
            columnId: 'sourceName',
            sortAsc: true
        }, connectionsData);

        // initialize the grid
        var connectionsGrid = new Slick.Grid('#connection-summary-table', connectionsData, connectionsColumnModel, connectionsOptions);
        connectionsGrid.setSelectionModel(new Slick.RowSelectionModel());
        connectionsGrid.registerPlugin(new Slick.AutoTooltips());
        connectionsGrid.setSortColumn('sourceName', true);
        connectionsGrid.onSort.subscribe(function (e, args) {
            sort('connection-summary-table', {
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, connectionsData);
        });

        // configure a click listener
        connectionsGrid.onClick.subscribe(function (e, args) {
            var target = $(e.target);

            // get the node at this row
            var item = connectionsData.getItem(args.row);

            // determine the desired action
            if (connectionsGrid.getColumns()[args.cell].id === 'actions') {
                if (target.hasClass('go-to')) {
                    goTo(item.groupId, item.id);
                } else if (target.hasClass('show-cluster-connection-status-history')) {
                    nf.StatusHistory.showClusterConnectionChart(item.groupId, item.id);
                } else if (target.hasClass('show-connection-status-history')) {
                    nf.StatusHistory.showStandaloneConnectionChart(item.groupId, item.id);
                } else if (target.hasClass('show-cluster-connection-summary')) {
                    // load the cluster processor summary
                    loadClusterConnectionSummary(item.id);

                    // hide the summary loading indicator
                    $('#summary-loading-container').hide();

                    // show the dialog
                    $('#cluster-connection-summary-dialog').modal('show');
                }
            } else if (connectionsGrid.getColumns()[args.cell].id === 'moreDetails') {
                if (target.hasClass('show-connection-details')) {
                    nf.ConnectionDetails.showDetails(item.groupId, item.id);
                }
            }
        });

        // wire up the dataview to the grid
        connectionsData.onRowCountChanged.subscribe(function (e, args) {
            connectionsGrid.updateRowCount();
            connectionsGrid.render();

            // update the total number of displayed processors, if necessary
            if ($('#connection-summary-table').is(':visible')) {
                $('#displayed-items').text(nf.Common.formatInteger(args.current));
            }
        });
        connectionsData.onRowsChanged.subscribe(function (e, args) {
            connectionsGrid.invalidateRows(args.rows);
            connectionsGrid.render();
        });

        // hold onto an instance of the grid
        $('#connection-summary-table').data('gridInstance', connectionsGrid);

        // initialize the cluster connection summary dialog
        $('#cluster-connection-summary-dialog').modal({
            headerText: 'Cluster Connection Summary',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Close',
                    handler: {
                        click: function () {
                            // clear the cluster connection summary dialog
                            $('#cluster-connection-id').text('');
                            $('#cluster-connection-name').text('');

                            // close the dialog
                            this.modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // show the summary loading container
                    $('#summary-loading-container').show();
                }
            }
        });
        
        // cluster connection refresh
        nf.Common.addHoverEffect('#cluster-connection-refresh-button', 'button-refresh', 'button-refresh-hover').click(function () {
            loadClusterConnectionSummary($('#cluster-connection-id').text());
        });

        // initialize the cluster processor column model
        var clusterConnectionsColumnModel = [
            {id: 'node', field: 'node', name: 'Node', sortable: true, resizable: true},
            inputColumn,
            queueColumn,
            outputColumn
        ];

        // initialize the options for the cluster processors table
        var clusterConnectionsOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            autoEdit: false,
            multiSelect: false
        };

        // initialize the dataview
        var clusterConnectionsData = new Slick.Data.DataView({
            inlineFilters: false
        });
        clusterConnectionsData.setItems([]);

        // initialize the sort
        sort('cluster-connection-summary-table', {
            columnId: 'node',
            sortAsc: true
        }, clusterConnectionsData);

        // initialize the grid
        var clusterConnectionsGrid = new Slick.Grid('#cluster-connection-summary-table', clusterConnectionsData, clusterConnectionsColumnModel, clusterConnectionsOptions);
        clusterConnectionsGrid.setSelectionModel(new Slick.RowSelectionModel());
        clusterConnectionsGrid.registerPlugin(new Slick.AutoTooltips());
        clusterConnectionsGrid.setSortColumn('node', true);
        clusterConnectionsGrid.onSort.subscribe(function (e, args) {
            sort('cluster-connection-summary-table', {
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, clusterConnectionsData);
        });

        // wire up the dataview to the grid
        clusterConnectionsData.onRowCountChanged.subscribe(function (e, args) {
            clusterConnectionsGrid.updateRowCount();
            clusterConnectionsGrid.render();
        });
        clusterConnectionsData.onRowsChanged.subscribe(function (e, args) {
            clusterConnectionsGrid.invalidateRows(args.rows);
            clusterConnectionsGrid.render();
        });

        // hold onto an instance of the grid
        $('#cluster-connection-summary-table').data('gridInstance', clusterConnectionsGrid);

        // define a custom formatter for showing more port details
        var moreDetails = function (row, cell, value, columnDef, dataContext) {
            var markup = '';

            // if there are bulletins, render them on the graph
            if (!nf.Common.isEmpty(dataContext.bulletins)) {
                markup += '<img src="images/iconBulletin.png" class="has-bulletins" style="margin-top: 5px; margin-left: 5px; float: left;"/><span class="hidden row-id">' + nf.Common.escapeHtml(dataContext.id) + '</span>';
            }

            return markup;
        };

        // define the column model for the summary table
        var inputPortsColumnModel = [
            {id: 'moreDetails', field: 'moreDetails', name: '&nbsp;', resizable: false, formatter: moreDetails, sortable: true, width: 50, maxWidth: 50},
            nameColumn,
            runStatusColumn,
            outputColumn
        ];

        // add an action column if appropriate
        if (isClustered || isInShell) {
            // define how the column is formatted
            var inputPortActionFormatter = function (row, cell, value, columnDef, dataContext) {
                var markup = '';

                if (isInShell) {
                    markup += '<img src="images/iconGoTo.png" title="Go To" class="pointer go-to" style="margin-top: 2px;"/>&nbsp;';
                }

                if (isClustered) {
                    markup += '<img src="images/iconClusterSmall.png" title="Show Details" class="pointer show-cluster-input-port-summary" style="margin-top: 2px;"/>&nbsp;';
                }

                return markup;
            };

            // define the action column for clusters and within the shell
            inputPortsColumnModel.push({id: 'actions', name: '&nbsp;', formatter: inputPortActionFormatter, resizable: false, sortable: false, width: 75, maxWidth: 75});
        }

        // initialize the input ports table
        var inputPortsOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            autoEdit: false,
            multiSelect: false
        };

        // initialize the dataview
        var inputPortsData = new Slick.Data.DataView({
            inlineFilters: false
        });
        inputPortsData.setItems([]);
        inputPortsData.setFilterArgs({
            searchString: '',
            property: 'name'
        });
        inputPortsData.setFilter(filter);

        // initialize the sort
        sort('input-port-summary-table', {
            columnId: 'name',
            sortAsc: true
        }, inputPortsData);

        // initialize the grid
        var inputPortsGrid = new Slick.Grid('#input-port-summary-table', inputPortsData, inputPortsColumnModel, inputPortsOptions);
        inputPortsGrid.setSelectionModel(new Slick.RowSelectionModel());
        inputPortsGrid.registerPlugin(new Slick.AutoTooltips());
        inputPortsGrid.setSortColumn('name', true);
        inputPortsGrid.onSort.subscribe(function (e, args) {
            sort('input-port-summary-table', {
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, inputPortsData);
        });

        // configure a click listener
        inputPortsGrid.onClick.subscribe(function (e, args) {
            var target = $(e.target);

            // get the node at this row
            var item = inputPortsData.getItem(args.row);

            // determine the desired action
            if (inputPortsGrid.getColumns()[args.cell].id === 'actions') {
                if (target.hasClass('go-to')) {
                    goTo(item.groupId, item.id);
                } else if (target.hasClass('show-cluster-input-port-summary')) {
                    // load the cluster processor summary
                    loadClusterInputPortSummary(item.id);

                    // hide the summary loading indicator
                    $('#summary-loading-container').hide();

                    // show the dialog
                    $('#cluster-input-port-summary-dialog').modal('show');
                }
            }
        });

        // wire up the dataview to the grid
        inputPortsData.onRowCountChanged.subscribe(function (e, args) {
            inputPortsGrid.updateRowCount();
            inputPortsGrid.render();

            // update the total number of displayed processors, if necessary
            if ($('#input-port-summary-table').is(':visible')) {
                $('#display-items').text(nf.Common.formatInteger(args.current));
            }
        });
        inputPortsData.onRowsChanged.subscribe(function (e, args) {
            inputPortsGrid.invalidateRows(args.rows);
            inputPortsGrid.render();
        });

        // hold onto an instance of the grid
        $('#input-port-summary-table').data('gridInstance', inputPortsGrid).on('mouseenter', 'div.slick-cell', function (e) {
            var bulletinIcon = $(this).find('img.has-bulletins');
            if (bulletinIcon.length && !bulletinIcon.data('qtip')) {
                var portId = $(this).find('span.row-id').text();

                // get the status item
                var item = inputPortsData.getItemById(portId);

                // format the tooltip
                var bulletins = nf.Common.getFormattedBulletins(item.bulletins);
                var tooltip = nf.Common.formatUnorderedList(bulletins);

                // show the tooltip
                if (nf.Common.isDefinedAndNotNull(tooltip)) {
                    bulletinIcon.qtip($.extend({
                        content: tooltip,
                        position: {
                            target: 'mouse',
                            viewport: $(window),
                            adjust: {
                                x: 8,
                                y: 8,
                                method: 'flipinvert flipinvert'
                            }
                        }
                    }, nf.Common.config.tooltipConfig));
                }
            }
        });

        // initialize the cluster input port summary dialog
        $('#cluster-input-port-summary-dialog').modal({
            headerText: 'Cluster Input Port Summary',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Close',
                    handler: {
                        click: function () {
                            // clear the cluster processor summary dialog
                            $('#cluster-input-port-id').text('');
                            $('#cluster-input-port-name').text('');

                            // close the dialog
                            this.modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // show the summary loading container
                    $('#summary-loading-container').show();
                }
            }
        });
        
        // cluster input port refresh
        nf.Common.addHoverEffect('#cluster-input-port-refresh-button', 'button-refresh', 'button-refresh-hover').click(function () {
            loadClusterInputPortSummary($('#cluster-input-port-id').text());
        });

        // initialize the cluster input port column model
        var clusterInputPortsColumnModel = [
            {id: 'node', field: 'node', name: 'Node', sortable: true, resizable: true},
            runStatusColumn,
            outputColumn
        ];

        // initialize the options for the cluster input port table
        var clusterInputPortsOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            autoEdit: false,
            multiSelect: false
        };

        // initialize the dataview
        var clusterInputPortsData = new Slick.Data.DataView({
            inlineFilters: false
        });
        clusterInputPortsData.setItems([]);

        // initialize the sort
        sort('cluster-input-port-summary-table', {
            columnId: 'node',
            sortAsc: true
        }, clusterInputPortsData);

        // initialize the grid
        var clusterInputPortsGrid = new Slick.Grid('#cluster-input-port-summary-table', clusterInputPortsData, clusterInputPortsColumnModel, clusterInputPortsOptions);
        clusterInputPortsGrid.setSelectionModel(new Slick.RowSelectionModel());
        clusterInputPortsGrid.registerPlugin(new Slick.AutoTooltips());
        clusterInputPortsGrid.setSortColumn('node', true);
        clusterInputPortsGrid.onSort.subscribe(function (e, args) {
            sort('cluster-input-port-summary-table', {
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, clusterInputPortsData);
        });

        // wire up the dataview to the grid
        clusterInputPortsData.onRowCountChanged.subscribe(function (e, args) {
            clusterInputPortsGrid.updateRowCount();
            clusterInputPortsGrid.render();
        });
        clusterInputPortsData.onRowsChanged.subscribe(function (e, args) {
            clusterInputPortsGrid.invalidateRows(args.rows);
            clusterInputPortsGrid.render();
        });

        // hold onto an instance of the grid
        $('#cluster-input-port-summary-table').data('gridInstance', clusterInputPortsGrid);

        // define the column model for the summary table
        var outputPortsColumnModel = [
            {id: 'moreDetails', field: 'moreDetails', name: '&nbsp;', resizable: false, formatter: moreDetails, sortable: true, width: 50, maxWidth: 50},
            nameColumn,
            runStatusColumn,
            inputColumn
        ];

        // add an action column if appropriate
        if (isClustered || isInShell) {
            // define how the column is formatted
            var outputPortActionFormatter = function (row, cell, value, columnDef, dataContext) {
                var markup = '';

                if (isInShell) {
                    markup += '<img src="images/iconGoTo.png" title="Go To" class="pointer go-to" style="margin-top: 2px;"/>&nbsp;';
                }

                if (isClustered) {
                    markup += '<img src="images/iconClusterSmall.png" title="Show Details" class="pointer show-cluster-output-port-summary" style="margin-top: 2px;"/>&nbsp;';
                }

                return markup;
            };

            // define the action column for clusters and within the shell
            outputPortsColumnModel.push({id: 'actions', name: '&nbsp;', formatter: outputPortActionFormatter, resizable: false, sortable: false, width: 75, maxWidth: 75});
        }

        // initialize the input ports table
        var outputPortsOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            autoEdit: false,
            multiSelect: false
        };

        // initialize the dataview
        var outputPortsData = new Slick.Data.DataView({
            inlineFilters: false
        });
        outputPortsData.setItems([]);
        outputPortsData.setFilterArgs({
            searchString: '',
            property: 'name'
        });
        outputPortsData.setFilter(filter);

        // initialize the sort
        sort('output-port-summary-table', {
            columnId: 'name',
            sortAsc: true
        }, outputPortsData);

        // initialize the grid
        var outputPortsGrid = new Slick.Grid('#output-port-summary-table', outputPortsData, outputPortsColumnModel, outputPortsOptions);
        outputPortsGrid.setSelectionModel(new Slick.RowSelectionModel());
        outputPortsGrid.registerPlugin(new Slick.AutoTooltips());
        outputPortsGrid.setSortColumn('name', true);
        outputPortsGrid.onSort.subscribe(function (e, args) {
            sort('output-port-summary-table', {
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, outputPortsData);
        });

        // configure a click listener
        outputPortsGrid.onClick.subscribe(function (e, args) {
            var target = $(e.target);

            // get the node at this row
            var item = outputPortsData.getItem(args.row);

            // determine the desired action
            if (outputPortsGrid.getColumns()[args.cell].id === 'actions') {
                if (target.hasClass('go-to')) {
                    goTo(item.groupId, item.id);
                } else if (target.hasClass('show-cluster-output-port-summary')) {
                    // load the cluster processor summary
                    loadClusterOutputPortSummary(item.id);

                    // hide the summary loading indicator
                    $('#summary-loading-container').hide();

                    // show the dialog
                    $('#cluster-output-port-summary-dialog').modal('show');
                }
            }
        });

        // wire up the dataview to the grid
        outputPortsData.onRowCountChanged.subscribe(function (e, args) {
            outputPortsGrid.updateRowCount();
            outputPortsGrid.render();

            // update the total number of displayed processors, if necessary
            if ($('#output-port-summary-table').is(':visible')) {
                $('#display-items').text(nf.Common.formatInteger(args.current));
            }
        });
        outputPortsData.onRowsChanged.subscribe(function (e, args) {
            outputPortsGrid.invalidateRows(args.rows);
            outputPortsGrid.render();
        });

        // hold onto an instance of the grid
        $('#output-port-summary-table').data('gridInstance', outputPortsGrid).on('mouseenter', 'div.slick-cell', function (e) {
            var bulletinIcon = $(this).find('img.has-bulletins');
            if (bulletinIcon.length && !bulletinIcon.data('qtip')) {
                var portId = $(this).find('span.row-id').text();

                // get the status item
                var item = outputPortsData.getItemById(portId);

                // format the tooltip
                var bulletins = nf.Common.getFormattedBulletins(item.bulletins);
                var tooltip = nf.Common.formatUnorderedList(bulletins);

                // show the tooltip
                if (nf.Common.isDefinedAndNotNull(tooltip)) {
                    bulletinIcon.qtip($.extend({
                        content: tooltip,
                        position: {
                            target: 'mouse',
                            viewport: $(window),
                            adjust: {
                                x: 8,
                                y: 8,
                                method: 'flipinvert flipinvert'
                            }
                        }
                    }, nf.Common.config.tooltipConfig));
                }
            }
        });

        // initialize the cluster output port summary dialog
        $('#cluster-output-port-summary-dialog').modal({
            headerText: 'Cluster Output Port Summary',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Close',
                    handler: {
                        click: function () {
                            // clear the cluster processor summary dialog
                            $('#cluster-output-port-id').text('');
                            $('#cluster-output-port-name').text('');

                            // close the dialog
                            this.modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // show the summary loading container
                    $('#summary-loading-container').show();
                }
            }
        });
        
        // cluster output port refresh
        nf.Common.addHoverEffect('#cluster-output-port-refresh-button', 'button-refresh', 'button-refresh-hover').click(function () {
            loadClusterOutputPortSummary($('#cluster-output-port-id').text());
        });

        // initialize the cluster output port column model
        var clusterOutputPortsColumnModel = [
            {id: 'node', field: 'node', name: 'Node', sortable: true, resizable: true},
            runStatusColumn,
            inputColumn
        ];

        // initialize the options for the cluster output port table
        var clusterOutputPortsOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            autoEdit: false,
            multiSelect: false
        };

        // initialize the dataview
        var clusterOutputPortsData = new Slick.Data.DataView({
            inlineFilters: false
        });
        clusterOutputPortsData.setItems([]);

        // initialize the sort
        sort('cluster-output-port-summary-table', {
            columnId: 'node',
            sortAsc: true
        }, clusterOutputPortsData);

        // initialize the grid
        var clusterOutputPortsGrid = new Slick.Grid('#cluster-output-port-summary-table', clusterOutputPortsData, clusterOutputPortsColumnModel, clusterOutputPortsOptions);
        clusterOutputPortsGrid.setSelectionModel(new Slick.RowSelectionModel());
        clusterOutputPortsGrid.registerPlugin(new Slick.AutoTooltips());
        clusterOutputPortsGrid.setSortColumn('node', true);
        clusterOutputPortsGrid.onSort.subscribe(function (e, args) {
            sort('cluster-output-port-summary-table', {
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, clusterOutputPortsData);
        });

        // wire up the dataview to the grid
        clusterOutputPortsData.onRowCountChanged.subscribe(function (e, args) {
            clusterOutputPortsGrid.updateRowCount();
            clusterOutputPortsGrid.render();
        });
        clusterOutputPortsData.onRowsChanged.subscribe(function (e, args) {
            clusterOutputPortsGrid.invalidateRows(args.rows);
            clusterOutputPortsGrid.render();
        });

        // hold onto an instance of the grid
        $('#cluster-output-port-summary-table').data('gridInstance', clusterOutputPortsGrid);

        // define a custom formatter for the run status column
        var transmissionStatusFormatter = function (row, cell, value, columnDef, dataContext) {
            var activeThreadCount = '';
            if (nf.Common.isDefinedAndNotNull(dataContext.activeThreadCount) && dataContext.activeThreadCount > 0) {
                activeThreadCount = '(' + dataContext.activeThreadCount + ')';
            }

            // determine what to put in the mark up
            var transmissionClass = 'invalid';
            var transmissionLabel = 'Invalid';
            if (nf.Common.isEmpty(dataContext.authorizationIssues)) {
                if (value === 'Transmitting') {
                    transmissionClass = 'transmitting';
                    transmissionLabel = value;
                } else {
                    transmissionClass = 'not-transmitting';
                    transmissionLabel = 'Not Transmitting';
                }
            }

            // generate the mark up
            var formattedValue = '<div class="' + transmissionClass + '" style="margin-top: 3px;"></div>';
            return formattedValue + '<div class="status-text" style="margin-top: 4px;">' + transmissionLabel + '</div><div style="float: left; margin-left: 4px;">' + nf.Common.escapeHtml(activeThreadCount) + '</div>';
        };

        var transmissionStatusColumn = {id: 'transmissionStatus', field: 'transmissionStatus', name: 'Transmitting', formatter: transmissionStatusFormatter, sortable: true, resizable: true};
        var targetUriColumn = {id: 'targetUri', field: 'targetUri', name: 'Target URI', sortable: true, resizable: true};
        var sentColumn = {id: 'sent', field: 'sent', name: '<span class="sent-title">Sent</span>&nbsp;/&nbsp;<span class="sent-size-title">Size</span>&nbsp;<span style="font-weight: normal; overflow: hidden;">5 min</span>', toolTip: 'Count / data size in the last 5 min', sortable: true, defaultSortAsc: false, resizable: true};
        var receivedColumn = {id: 'received', field: 'received', name: '<span class="received-title">Received</span>&nbsp;/&nbsp;<span class="received-size-title">Size</span>&nbsp;<span style="font-weight: normal; overflow: hidden;">5 min</span>', toolTip: 'Count / data size in the last 5 min', sortable: true, defaultSortAsc: false, resizable: true};

        // define the column model for the summary table
        var remoteProcessGroupsColumnModel = [
            {id: 'moreDetails', field: 'moreDetails', name: '&nbsp;', resizable: false, formatter: moreDetails, sortable: true, width: 50, maxWidth: 50},
            nameColumn,
            targetUriColumn,
            transmissionStatusColumn,
            sentColumn,
            receivedColumn
        ];

        // add an action column if appropriate
        if (isClustered || isInShell || nf.Common.SUPPORTS_SVG) {
            // define how the column is formatted
            var remoteProcessGroupActionFormatter = function (row, cell, value, columnDef, dataContext) {
                var markup = '';

                if (isInShell) {
                    markup += '<img src="images/iconGoTo.png" title="Go To" class="pointer go-to" style="margin-top: 2px;"/>&nbsp;';
                }

                if (nf.Common.SUPPORTS_SVG) {
                    if (isClustered) {
                        markup += '<img src="images/iconChart.png" title="Show History" class="pointer show-cluster-remote-process-group-status-history" style="margin-top: 2px;"/>&nbsp;';
                    } else {
                        markup += '<img src="images/iconChart.png" title="Show History" class="pointer show-remote-process-group-status-history" style="margin-top: 2px;"/>&nbsp;';
                    }
                }

                if (isClustered) {
                    markup += '<img src="images/iconClusterSmall.png" title="Show Details" class="pointer show-cluster-remote-process-group-summary" style="margin-top: 2px;"/>&nbsp;';
                }

                return markup;
            };

            // define the action column for clusters and within the shell
            remoteProcessGroupsColumnModel.push({id: 'actions', name: '&nbsp;', formatter: remoteProcessGroupActionFormatter, resizable: false, sortable: false, width: 75, maxWidth: 75});
        }

        // initialize the remote process groups table
        var remoteProcessGroupsOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            autoEdit: false,
            multiSelect: false
        };

        // initialize the dataview
        var remoteProcessGroupsData = new Slick.Data.DataView({
            inlineFilters: false
        });
        remoteProcessGroupsData.setItems([]);
        remoteProcessGroupsData.setFilterArgs({
            searchString: '',
            property: 'name'
        });
        remoteProcessGroupsData.setFilter(filter);

        // initialize the sort
        sort('remote-process-group-summary-table', {
            columnId: 'name',
            sortAsc: true
        }, remoteProcessGroupsData);

        // initialize the grid
        var remoteProcessGroupsGrid = new Slick.Grid('#remote-process-group-summary-table', remoteProcessGroupsData, remoteProcessGroupsColumnModel, remoteProcessGroupsOptions);
        remoteProcessGroupsGrid.setSelectionModel(new Slick.RowSelectionModel());
        remoteProcessGroupsGrid.registerPlugin(new Slick.AutoTooltips());
        remoteProcessGroupsGrid.setSortColumn('name', true);
        remoteProcessGroupsGrid.onSort.subscribe(function (e, args) {
            sort('remote-process-group-summary-table', {
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, remoteProcessGroupsData);
        });

        // configure a click listener
        remoteProcessGroupsGrid.onClick.subscribe(function (e, args) {
            var target = $(e.target);

            // get the node at this row
            var item = remoteProcessGroupsData.getItem(args.row);

            // determine the desired action
            if (remoteProcessGroupsGrid.getColumns()[args.cell].id === 'actions') {
                if (target.hasClass('go-to')) {
                    goTo(item.groupId, item.id);
                } else if (target.hasClass('show-cluster-remote-process-group-status-history')) {
                    nf.StatusHistory.showClusterRemoteProcessGroupChart(item.groupId, item.id);
                } else if (target.hasClass('show-remote-process-group-status-history')) {
                    nf.StatusHistory.showStandaloneRemoteProcessGroupChart(item.groupId, item.id);
                } else if (target.hasClass('show-cluster-remote-process-group-summary')) {
                    // load the cluster processor summary
                    loadClusterRemoteProcessGroupSummary(item.id);

                    // hide the summary loading indicator
                    $('#summary-loading-container').hide();

                    // show the dialog
                    $('#cluster-remote-process-group-summary-dialog').modal('show');
                }
            }
        });

        // wire up the dataview to the grid
        remoteProcessGroupsData.onRowCountChanged.subscribe(function (e, args) {
            remoteProcessGroupsGrid.updateRowCount();
            remoteProcessGroupsGrid.render();

            // update the total number of displayed processors, if necessary
            if ($('#remote-process-group-summary-table').is(':visible')) {
                $('#displayed-items').text(nf.Common.formatInteger(args.current));
            }
        });
        remoteProcessGroupsData.onRowsChanged.subscribe(function (e, args) {
            remoteProcessGroupsGrid.invalidateRows(args.rows);
            remoteProcessGroupsGrid.render();
        });

        // hold onto an instance of the grid
        $('#remote-process-group-summary-table').data('gridInstance', remoteProcessGroupsGrid).on('mouseenter', 'div.slick-cell', function (e) {
            var bulletinIcon = $(this).find('img.has-bulletins');
            if (bulletinIcon.length && !bulletinIcon.data('qtip')) {
                var remoteProcessGroupId = $(this).find('span.row-id').text();

                // get the status item
                var item = remoteProcessGroupsData.getItemById(remoteProcessGroupId);

                // format the tooltip
                var bulletins = nf.Common.getFormattedBulletins(item.bulletins);
                var tooltip = nf.Common.formatUnorderedList(bulletins);

                // show the tooltip
                if (nf.Common.isDefinedAndNotNull(tooltip)) {
                    bulletinIcon.qtip($.extend({
                        content: tooltip,
                        position: {
                            target: 'mouse',
                            viewport: $(window),
                            adjust: {
                                x: 8,
                                y: 8,
                                method: 'flipinvert flipinvert'
                            }
                        }
                    }, nf.Common.config.tooltipConfig));
                }
            }
        });

        // initialize the cluster remote process group summary dialog
        $('#cluster-remote-process-group-summary-dialog').modal({
            headerText: 'Cluster Remote Process Group Summary',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Close',
                    handler: {
                        click: function () {
                            // clear the cluster processor summary dialog
                            $('#cluster-remote-process-group-id').text('');
                            $('#cluster-remote-process-group-name').text('');

                            // close the dialog
                            this.modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // show the summary loading container
                    $('#summary-loading-container').show();
                }
            }
        });
        
        // cluster remote process group refresh
        nf.Common.addHoverEffect('#cluster-remote-process-group-refresh-button', 'button-refresh', 'button-refresh-hover').click(function () {
            loadClusterRemoteProcessGroupSummary($('#cluster-remote-process-group-id').text());
        });

        // initialize the cluster remote process group column model
        var clusterRemoteProcessGroupsColumnModel = [
            {id: 'node', field: 'node', name: 'Node', sortable: true, resizable: true},
            targetUriColumn,
            transmissionStatusColumn,
            sentColumn,
            receivedColumn
        ];

        // initialize the options for the cluster remote process group table
        var clusterRemoteProcessGroupsOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: false,
            enableColumnReorder: false,
            autoEdit: false
        };

        // initialize the dataview
        var clusterRemoteProcessGroupsData = new Slick.Data.DataView({
            inlineFilters: false
        });
        clusterRemoteProcessGroupsData.setItems([]);

        // initialize the sort
        sort('cluster-remote-process-group-summary-table', {
            columnId: 'node',
            sortAsc: true
        }, clusterRemoteProcessGroupsData);

        // initialize the grid
        var clusterRemoteProcessGroupsGrid = new Slick.Grid('#cluster-remote-process-group-summary-table', clusterRemoteProcessGroupsData, clusterRemoteProcessGroupsColumnModel, clusterRemoteProcessGroupsOptions);
        clusterRemoteProcessGroupsGrid.setSelectionModel(new Slick.RowSelectionModel());
        clusterRemoteProcessGroupsGrid.registerPlugin(new Slick.AutoTooltips());
        clusterRemoteProcessGroupsGrid.setSortColumn('node', true);
        clusterRemoteProcessGroupsGrid.onSort.subscribe(function (e, args) {
            sort('cluster-remote-process-group-summary-table', {
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, clusterRemoteProcessGroupsData);
        });

        // wire up the dataview to the grid
        clusterRemoteProcessGroupsData.onRowCountChanged.subscribe(function (e, args) {
            clusterRemoteProcessGroupsGrid.updateRowCount();
            clusterRemoteProcessGroupsGrid.render();
        });
        clusterRemoteProcessGroupsData.onRowsChanged.subscribe(function (e, args) {
            clusterRemoteProcessGroupsGrid.invalidateRows(args.rows);
            clusterRemoteProcessGroupsGrid.render();
        });

        // hold onto an instance of the grid
        $('#cluster-remote-process-group-summary-table').data('gridInstance', clusterRemoteProcessGroupsGrid);

        // show the dialog when clicked
        $('#system-diagnostics-link').click(function () {
            refreshSystemDiagnostics().done(function () {
                // hide the summary loading container
                $('#summary-loading-container').hide();

                // show the dialog
                $('#system-diagnostics-dialog').modal('show');
            });
        });

        // initialize the summary tabs
        $('#system-diagnostics-tabs').tabbs({
            tabStyle: 'summary-tab',
            selectedTabStyle: 'summary-selected-tab',
            tabs: [{
                    name: 'JVM',
                    tabContentId: 'jvm-tab-content'
                }, {
                    name: 'System',
                    tabContentId: 'system-tab-content'
                }]
        });

        // initialize the system diagnostics dialog
        $('#system-diagnostics-dialog').modal({
            headerText: 'System Diagnostics',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Close',
                    handler: {
                        click: function () {
                            this.modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // show the summary loading container
                    $('#summary-loading-container').show();
                }
            }
        });

        // refresh the system diagnostics when clicked
        nf.Common.addHoverEffect('#system-diagnostics-refresh-button', 'button-refresh', 'button-refresh-hover').click(function () {
            refreshSystemDiagnostics();
        });

        // initialize the total number of processors
        $('#total-items').text('0');
    };

    var sortState = {};

    /**
     * Sorts the specified data using the specified sort details.
     * 
     * @param {string} tableId
     * @param {object} sortDetails
     * @param {object} data
     */
    var sort = function (tableId, sortDetails, data) {
        // ensure there is a state object for this table
        if (nf.Common.isUndefined(sortState[tableId])) {
            sortState[tableId] = {};
        }

        // defines a function for sorting
        var comparer = function (a, b) {
            if (sortDetails.columnId === 'moreDetails') {
                var aBulletins = 0;
                if (!nf.Common.isEmpty(a.bulletins)) {
                    aBulletins = a.bulletins.length;
                }
                var bBulletins = 0;
                if (!nf.Common.isEmpty(b.bulletins)) {
                    bBulletins = b.bulletins.length;
                }
                return aBulletins - bBulletins;
            } else if (sortDetails.columnId === 'runStatus' || sortDetails.columnId === 'transmissionStatus') {
                var aString = nf.Common.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
                var bString = nf.Common.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
                if (aString === bString) {
                    return a.activeThreadCount - b.activeThreadCount;
                } else {
                    return aString === bString ? 0 : aString > bString ? 1 : -1;
                }
            } else if (sortDetails.columnId === 'queued') {
                var mod = sortState[tableId].count % 4;
                if (mod < 2) {
                    $('#' + tableId + ' span.queued-title').addClass('sorted');
                    var aQueueCount = nf.Common.parseCount(a['queuedCount']);
                    var bQueueCount = nf.Common.parseCount(b['queuedCount']);
                    return aQueueCount - bQueueCount;
                } else {
                    $('#' + tableId + ' span.queued-size-title').addClass('sorted');
                    var aQueueSize = nf.Common.parseSize(a['queuedSize']);
                    var bQueueSize = nf.Common.parseSize(b['queuedSize']);
                    return aQueueSize - bQueueSize;
                }
            } else if (sortDetails.columnId === 'sent' || sortDetails.columnId === 'received' || sortDetails.columnId === 'input' || sortDetails.columnId === 'output') {
                var aSplit = a[sortDetails.columnId].split(/ \/ /);
                var bSplit = b[sortDetails.columnId].split(/ \/ /);
                var mod = sortState[tableId].count % 4;
                if (mod < 2) {
                    $('#' + tableId + ' span.' + sortDetails.columnId + '-title').addClass('sorted');
                    var aCount = nf.Common.parseCount(aSplit[0]);
                    var bCount = nf.Common.parseCount(bSplit[0]);
                    return aCount - bCount;
                } else {
                    $('#' + tableId + ' span.' + sortDetails.columnId + '-size-title').addClass('sorted');
                    var aSize = nf.Common.parseSize(aSplit[1]);
                    var bSize = nf.Common.parseSize(bSplit[1]);
                    return aSize - bSize;
                }
            } else if (sortDetails.columnId === 'io') {
                var mod = sortState[tableId].count % 4;
                if (mod < 2) {
                    $('#' + tableId + ' span.read-title').addClass('sorted');
                    var aReadSize = nf.Common.parseSize(a['read']);
                    var bReadSize = nf.Common.parseSize(b['read']);
                    return aReadSize - bReadSize;
                } else {
                    $('#' + tableId + ' span.written-title').addClass('sorted');
                    var aWriteSize = nf.Common.parseSize(a['written']);
                    var bWriteSize = nf.Common.parseSize(b['written']);
                    return aWriteSize - bWriteSize;
                }
            } else if (sortDetails.columnId === 'tasks') {
                var mod = sortState[tableId].count % 4;
                if (mod < 2) {
                    $('#' + tableId + ' span.tasks-title').addClass('sorted');
                    var aTasks = nf.Common.parseCount(a['tasks']);
                    var bTasks = nf.Common.parseCount(b['tasks']);
                    return aTasks - bTasks;
                } else {
                    $('#' + tableId + ' span.time-title').addClass('sorted');
                    var aDuration = nf.Common.parseDuration(a['tasksDuration']);
                    var bDuration = nf.Common.parseDuration(b['tasksDuration']);
                    return aDuration - bDuration;
                }
            } else {
                var aString = nf.Common.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
                var bString = nf.Common.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
                return aString === bString ? 0 : aString > bString ? 1 : -1;
            }
        };

        // remove previous sort indicators
        $('#' + tableId + ' span.queued-title').removeClass('sorted');
        $('#' + tableId + ' span.queued-size-title').removeClass('sorted');
        $('#' + tableId + ' span.input-title').removeClass('sorted');
        $('#' + tableId + ' span.input-size-title').removeClass('sorted');
        $('#' + tableId + ' span.output-title').removeClass('sorted');
        $('#' + tableId + ' span.output-size-title').removeClass('sorted');
        $('#' + tableId + ' span.read-title').removeClass('sorted');
        $('#' + tableId + ' span.written-title').removeClass('sorted');
        $('#' + tableId + ' span.time-title').removeClass('sorted');
        $('#' + tableId + ' span.tasks-title').removeClass('sorted');
        $('#' + tableId + ' span.sent-title').removeClass('sorted');
        $('#' + tableId + ' span.sent-size-title').removeClass('sorted');
        $('#' + tableId + ' span.received-title').removeClass('sorted');
        $('#' + tableId + ' span.received-size-title').removeClass('sorted');

        // update/reset the count as appropriate
        if (sortState[tableId].prevColumn !== sortDetails.columnId) {
            sortState[tableId].count = 0;
        } else {
            sortState[tableId].count++;
        }

        // perform the sort
        data.sort(comparer, sortDetails.sortAsc);

        // record the previous table and sorted column
        sortState[tableId].prevColumn = sortDetails.columnId;
    };

    /**
     * Performs the processor filtering.
     * 
     * @param {object} item     The item subject to filtering
     * @param {object} args     Filter arguments
     * @returns {Boolean}       Whether or not to include the item
     */
    var filter = function (item, args) {
        if (args.searchString === '') {
            return true;
        }

        try {
            // perform the row filtering
            var filterExp = new RegExp(args.searchString, 'i');
        } catch (e) {
            // invalid regex
            return false;
        }

        return item[args.property].search(filterExp) >= 0;
    };

    /**
     * Refreshes the system diagnostics.
     */
    var refreshSystemDiagnostics = function () {
        return $.ajax({
            type: 'GET',
            url: nf.SummaryTable.systemDiagnosticsUrl,
            dataType: 'json'
        }).done(function (response) {
            var systemDiagnostics = response.systemDiagnostics;

            // heap
            $('#max-heap').text(systemDiagnostics.maxHeap);
            $('#total-heap').text(systemDiagnostics.totalHeap);
            $('#used-heap').text(systemDiagnostics.usedHeap);
            $('#free-heap').text(systemDiagnostics.freeHeap);
            $('#utilization-heap').text(systemDiagnostics.heapUtilization);

            // non heap
            $('#max-non-heap').text(systemDiagnostics.maxNonHeap);
            $('#total-non-heap').text(systemDiagnostics.totalNonHeap);
            $('#used-non-heap').text(systemDiagnostics.usedNonHeap);
            $('#free-non-heap').text(systemDiagnostics.freeNonHeap);
            $('#utilization-non-heap').text(systemDiagnostics.nonHeapUtilization);

            // garbage collection
            var garbageCollectionContainer = $('#garbage-collection-table tbody').empty();
            $.each(systemDiagnostics.garbageCollection, function (_, garbageCollection) {
                addGarbageCollection(garbageCollectionContainer, garbageCollection);
            });

            // load
            $('#available-processors').text(systemDiagnostics.availableProcessors);
            $('#processor-load-average').html(nf.Common.formatValue(systemDiagnostics.processorLoadAverage));

            // database storage usage
            var flowFileRepositoryStorageUsageContainer = $('#flow-file-repository-storage-usage-container').empty();
            addStorageUsage(flowFileRepositoryStorageUsageContainer, systemDiagnostics.flowFileRepositoryStorageUsage);

            // database storage usage
            var contentRepositoryUsageContainer = $('#content-repository-storage-usage-container').empty();
            $.each(systemDiagnostics.contentRepositoryStorageUsage, function (_, contentRepository) {
                addStorageUsage(contentRepositoryUsageContainer, contentRepository);
            });

            // update the stats last refreshed timestamp
            $('#system-diagnostics-last-refreshed').text(systemDiagnostics.statsLastRefreshed);
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Adds a new garbage collection to the dialog
     * @param {type} container
     * @param {type} garbageCollection
     */
    var addGarbageCollection = function (container, garbageCollection) {
        var tr = $('<tr></tr>').appendTo(container);
        $('<td></td>').append($('<b></b>').text(garbageCollection.name)).appendTo(tr);
        $('<td></td>').text(garbageCollection.collectionCount + ' times').appendTo(tr);
        $('<td></td>').text(garbageCollection.collectionTime).appendTo(tr);
    };

    /**
     * Adds a new storage usage process bar to the dialog.
     * 
     * @argument {jQuery} container     The container to add the storage usage to.
     * @argument {object} storageUsage     The storage usage.
     */
    var addStorageUsage = function (container, storageUsage) {
        var total = parseInt(storageUsage.totalSpaceBytes, 10);
        var used = parseInt(storageUsage.usedSpaceBytes, 10);
        var storageUsageContainer = $('<div class="storage-usage"></div>').appendTo(container);
        $('<div class="storage-usage-progressbar"></div>').progressbar({
            max: total,
            value: used
        }).appendTo(storageUsageContainer).children('.ui-progressbar-value').text(storageUsage.utilization);

        var storage = $('<span class="storage-identifier"></span>').text(storageUsage.identifier);
        var usageDetails = $('<span class="storage-usage-details"></span>').text(storageUsage.usedSpace + ' of ' + storageUsage.totalSpace);
        $('<div class="storage-usage-header"></div>').append(storage).append(usageDetails).append('<div class="clear"></div>').appendTo(storageUsageContainer);
    };

    /**
     * Get the text out of the filter field. If the filter field doesn't
     * have any text it will contain the text 'filter list' so this method
     * accounts for that.
     */
    var getFilterText = function () {
        var filterText = '';
        var filterField = $('#summary-filter');
        if (!filterField.hasClass(config.styles.filterList)) {
            filterText = filterField.val();
        }
        return filterText;
    };

    /**
     * Poplates the processor summary table using the specified process group.
     * 
     * @argument {array} processorItems                 The processor data
     * @argument {array} connectionItems                The connection data
     * @argument {array} inputPortItems                 The input port data
     * @argument {array} outputPortItems                The input port data
     * @argument {array} remoteProcessGroupItems        The remote process group data
     * @argument {object} processGroupStatus            The process group status
     */
    var populateProcessGroupStatus = function (processorItems, connectionItems, inputPortItems, outputPortItems, remoteProcessGroupItems, processGroupStatus) {
        // add the processors to the summary grid
        $.each(processGroupStatus.processorStatus, function (i, procStatus) {
            processorItems.push(procStatus);
        });

        // add the processors to the summary grid
        $.each(processGroupStatus.connectionStatus, function (i, connStatus) {
            connectionItems.push(connStatus);
        });

        // add the input ports to the summary grid
        $.each(processGroupStatus.inputPortStatus, function (i, portStatus) {
            inputPortItems.push(portStatus);
        });

        // add the input ports to the summary grid
        $.each(processGroupStatus.outputPortStatus, function (i, portStatus) {
            outputPortItems.push(portStatus);
        });

        // add the input ports to the summary grid
        $.each(processGroupStatus.remoteProcessGroupStatus, function (i, rpgStatus) {
            remoteProcessGroupItems.push(rpgStatus);
        });

        // add any child group's status
        $.each(processGroupStatus.processGroupStatus, function (i, childProcessGroup) {
            populateProcessGroupStatus(processorItems, connectionItems, inputPortItems, outputPortItems, remoteProcessGroupItems, childProcessGroup);
        });
    };

    /**
     * Applies the filter found in the filter expression text field.
     */
    var applyFilter = function () {
        var grid;
        if ($('#processor-summary-table').is(':visible')) {
            grid = $('#processor-summary-table').data('gridInstance');
        } else {
            grid = $('#connection-summary-table').data('gridInstance');
        }

        // ensure the grid has been initialized
        if (nf.Common.isDefinedAndNotNull(grid)) {
            var data = grid.getData();

            // update the search criteria
            data.setFilterArgs({
                searchString: getFilterText(),
                property: $('#summary-filter-type').combo('getSelectedOption').value
            });
            data.refresh();
        }
    };

    /**
     * Loads  the cluster processor details dialog for the specified processor.
     * 
     * @argument {string} rowId     The row id
     */
    var loadClusterProcessorSummary = function (rowId) {
        // get the summary
        $.ajax({
            type: 'GET',
            url: config.urls.clusterProcessor + encodeURIComponent(rowId) + '/status',
            data: {
                verbose: true
            },
            dataType: 'json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.clusterProcessorStatus)) {
                var clusterProcessorStatus = response.clusterProcessorStatus;

                var clusterProcessorsGrid = $('#cluster-processor-summary-table').data('gridInstance');
                var clusterProcessorsData = clusterProcessorsGrid.getData();

                var clusterProcessors = [];

                // populate the table
                $.each(clusterProcessorStatus.nodeProcessorStatus, function (i, nodeProcessorStatus) {
                    clusterProcessors.push({
                        id: nodeProcessorStatus.node.nodeId,
                        node: nodeProcessorStatus.node.address + ':' + nodeProcessorStatus.node.apiPort,
                        runStatus: nodeProcessorStatus.processorStatus.runStatus,
                        activeThreadCount: nodeProcessorStatus.processorStatus.activeThreadCount,
                        input: nodeProcessorStatus.processorStatus.input,
                        read: nodeProcessorStatus.processorStatus.read,
                        written: nodeProcessorStatus.processorStatus.written,
                        output: nodeProcessorStatus.processorStatus.output,
                        tasks: nodeProcessorStatus.processorStatus.tasks,
                        tasksDuration: nodeProcessorStatus.processorStatus.tasksDuration
                    });
                });

                // update the processors
                clusterProcessorsData.setItems(clusterProcessors);
                clusterProcessorsData.reSort();
                clusterProcessorsGrid.invalidate();

                // populate the processor details
                $('#cluster-processor-name').text(clusterProcessorStatus.processorName).ellipsis();
                $('#cluster-processor-id').text(clusterProcessorStatus.processorId);

                // update the stats last refreshed timestamp
                $('#cluster-processor-summary-last-refreshed').text(clusterProcessorStatus.statsLastRefreshed);
            }
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Loads the cluster connection details dialog for the specified processor.
     * 
     * @argument {string} rowId     The row id
     */
    var loadClusterConnectionSummary = function (rowId) {
        // get the summary
        $.ajax({
            type: 'GET',
            url: config.urls.clusterConnection + encodeURIComponent(rowId) + '/status',
            data: {
                verbose: true
            },
            dataType: 'json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.clusterConnectionStatus)) {
                var clusterConnectionStatus = response.clusterConnectionStatus;

                var clusterConnectionsGrid = $('#cluster-connection-summary-table').data('gridInstance');
                var clusterConnectionsData = clusterConnectionsGrid.getData();

                var clusterConnections = [];

                // populate the table
                $.each(clusterConnectionStatus.nodeConnectionStatus, function (i, nodeConnectionStatus) {
                    clusterConnections.push({
                        id: nodeConnectionStatus.node.nodeId,
                        node: nodeConnectionStatus.node.address + ':' + nodeConnectionStatus.node.apiPort,
                        input: nodeConnectionStatus.connectionStatus.input,
                        queued: nodeConnectionStatus.connectionStatus.queued,
                        queuedCount: nodeConnectionStatus.connectionStatus.queuedCount,
                        queuedSize: nodeConnectionStatus.connectionStatus.queuedSize,
                        output: nodeConnectionStatus.connectionStatus.output
                    });
                });

                // update the processors
                clusterConnectionsData.setItems(clusterConnections);
                clusterConnectionsData.reSort();
                clusterConnectionsGrid.invalidate();

                // populate the processor details
                $('#cluster-connection-name').text(clusterConnectionStatus.connectionName).ellipsis();
                $('#cluster-connection-id').text(clusterConnectionStatus.connectionId);

                // update the stats last refreshed timestamp
                $('#cluster-connection-summary-last-refreshed').text(clusterConnectionStatus.statsLastRefreshed);
            }
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Loads the cluster input port details dialog for the specified processor.
     * 
     * @argument {string} rowId     The row id
     */
    var loadClusterInputPortSummary = function (rowId) {
        // get the summary
        $.ajax({
            type: 'GET',
            url: config.urls.clusterInputPort + encodeURIComponent(rowId) + '/status',
            data: {
                verbose: true
            },
            dataType: 'json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.clusterPortStatus)) {
                var clusterInputPortStatus = response.clusterPortStatus;

                var clusterInputPortsGrid = $('#cluster-input-port-summary-table').data('gridInstance');
                var clusterInputPortsData = clusterInputPortsGrid.getData();

                var clusterInputPorts = [];

                // populate the table
                $.each(clusterInputPortStatus.nodePortStatus, function (i, nodeInputPortStatus) {
                    clusterInputPorts.push({
                        id: nodeInputPortStatus.node.nodeId,
                        node: nodeInputPortStatus.node.address + ':' + nodeInputPortStatus.node.apiPort,
                        runStatus: nodeInputPortStatus.portStatus.runStatus,
                        activeThreadCount: nodeInputPortStatus.portStatus.activeThreadCount,
                        output: nodeInputPortStatus.portStatus.output
                    });
                });

                // update the input ports
                clusterInputPortsData.setItems(clusterInputPorts);
                clusterInputPortsData.reSort();
                clusterInputPortsGrid.invalidate();

                // populate the input port details
                $('#cluster-input-port-name').text(clusterInputPortStatus.portName).ellipsis();
                $('#cluster-input-port-id').text(clusterInputPortStatus.portId);

                // update the stats last refreshed timestamp
                $('#cluster-input-port-summary-last-refreshed').text(clusterInputPortStatus.statsLastRefreshed);
            }
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Loads the cluster output port details dialog for the specified processor.
     * 
     * @argument {string} rowId     The row id
     */
    var loadClusterOutputPortSummary = function (rowId) {
        // get the summary
        $.ajax({
            type: 'GET',
            url: config.urls.clusterOutputPort + encodeURIComponent(rowId) + '/status',
            data: {
                verbose: true
            },
            dataType: 'json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.clusterPortStatus)) {
                var clusterOutputPortStatus = response.clusterPortStatus;

                var clusterOutputPortsGrid = $('#cluster-output-port-summary-table').data('gridInstance');
                var clusterOutputPortsData = clusterOutputPortsGrid.getData();

                var clusterOutputPorts = [];

                // populate the table
                $.each(clusterOutputPortStatus.nodePortStatus, function (i, nodeOutputPortStatus) {
                    clusterOutputPorts.push({
                        id: nodeOutputPortStatus.node.nodeId,
                        node: nodeOutputPortStatus.node.address + ':' + nodeOutputPortStatus.node.apiPort,
                        runStatus: nodeOutputPortStatus.portStatus.runStatus,
                        activeThreadCount: nodeOutputPortStatus.portStatus.activeThreadCount,
                        input: nodeOutputPortStatus.portStatus.input
                    });
                });

                // update the output ports
                clusterOutputPortsData.setItems(clusterOutputPorts);
                clusterOutputPortsData.reSort();
                clusterOutputPortsGrid.invalidate();

                // populate the output port details
                $('#cluster-output-port-name').text(clusterOutputPortStatus.portName).ellipsis();
                $('#cluster-output-port-id').text(clusterOutputPortStatus.portId);

                // update the stats last refreshed timestamp
                $('#cluster-output-port-summary-last-refreshed').text(clusterOutputPortStatus.statsLastRefreshed);
            }
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Loads the cluster remote process group details dialog for the specified processor.
     * 
     * @argument {string} rowId     The row id
     */
    var loadClusterRemoteProcessGroupSummary = function (rowId) {
        // get the summary
        $.ajax({
            type: 'GET',
            url: config.urls.clusterRemoteProcessGroup + encodeURIComponent(rowId) + '/status',
            data: {
                verbose: true
            },
            dataType: 'json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.clusterRemoteProcessGroupStatus)) {
                var clusterRemoteProcessGroupStatus = response.clusterRemoteProcessGroupStatus;

                var clusterRemoteProcessGroupsGrid = $('#cluster-remote-process-group-summary-table').data('gridInstance');
                var clusterRemoteProcessGroupsData = clusterRemoteProcessGroupsGrid.getData();

                var clusterRemoteProcessGroups = [];

                // populate the table
                $.each(clusterRemoteProcessGroupStatus.nodeRemoteProcessGroupStatus, function (i, nodeRemoteProcessGroupStatus) {
                    clusterRemoteProcessGroups.push({
                        id: nodeRemoteProcessGroupStatus.node.nodeId,
                        node: nodeRemoteProcessGroupStatus.node.address + ':' + nodeRemoteProcessGroupStatus.node.apiPort,
                        targetUri: nodeRemoteProcessGroupStatus.remoteProcessGroupStatus.targetUri,
                        transmissionStatus: nodeRemoteProcessGroupStatus.remoteProcessGroupStatus.transmissionStatus,
                        sent: nodeRemoteProcessGroupStatus.remoteProcessGroupStatus.sent,
                        received: nodeRemoteProcessGroupStatus.remoteProcessGroupStatus.received,
                        activeThreadCount: nodeRemoteProcessGroupStatus.remoteProcessGroupStatus.activeThreadCount,
                        authorizationIssues: nodeRemoteProcessGroupStatus.remoteProcessGroupStatus.authorizationIssues
                    });
                });

                // update the remote process groups
                clusterRemoteProcessGroupsData.setItems(clusterRemoteProcessGroups);
                clusterRemoteProcessGroupsData.reSort();
                clusterRemoteProcessGroupsGrid.invalidate();

                // populate the remote process group details
                $('#cluster-remote-process-group-name').text(clusterRemoteProcessGroupStatus.remoteProcessGroupName).ellipsis();
                $('#cluster-remote-process-group-id').text(clusterRemoteProcessGroupStatus.remoteProcessGroupId);

                // update the stats last refreshed timestamp
                $('#cluster-remote-process-group-summary-last-refreshed').text(clusterRemoteProcessGroupStatus.statsLastRefreshed);
            }
        }).fail(nf.Common.handleAjaxError);
    };

    return {
        /**
         * URL for loading system diagnostics.
         */
        systemDiagnosticsUrl: null,
        /**
         * URL for loading the summary.
         */
        url: null,
        /**
         * Initializes the status table.
         * 
         * @argument {boolean} isClustered Whether or not this NiFi is clustered.
         */
        init: function (isClustered) {
            // initialize the summary urls
            nf.SummaryTable.url = config.urls.status;
            nf.SummaryTable.systemDiagnosticsUrl = config.urls.systemDiagnostics;

            return $.Deferred(function (deferred) {
                loadChartCapabilities().done(function () {
                    // initialize the processor/connection details dialog
                    nf.ProcessorDetails.init(false);
                    nf.ConnectionDetails.init(false);
                    initSummaryTable(isClustered);

                    deferred.resolve();
                }).fail(function () {
                    deferred.reject();
                });
            }).promise();
        },
        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            var processorsGrid = $('#processor-summary-table').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(processorsGrid)) {
                processorsGrid.resizeCanvas();
            }

            var connectionsGrid = $('#connection-summary-table').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(connectionsGrid)) {
                connectionsGrid.resizeCanvas();
            }

            var inputPortGrid = $('#input-port-summary-table').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(connectionsGrid)) {
                inputPortGrid.resizeCanvas();
            }

            var outputPortGrid = $('#output-port-summary-table').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(connectionsGrid)) {
                outputPortGrid.resizeCanvas();
            }

            var remoteProcessGroupGrid = $('#remote-process-group-summary-table').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(connectionsGrid)) {
                remoteProcessGroupGrid.resizeCanvas();
            }
        },
        /**
         * Load the processor status table.
         */
        loadProcessorSummaryTable: function () {
            return $.ajax({
                type: 'GET',
                url: nf.SummaryTable.url,
                data: {
                    recursive: true
                },
                dataType: 'json'
            }).done(function (response) {
                var processGroupStatus = response.processGroupStatus;

                if (nf.Common.isDefinedAndNotNull(processGroupStatus)) {
                    // remove any tooltips from the processor table
                    var processorsGridElement = $('#processor-summary-table');
                    nf.Common.cleanUpTooltips(processorsGridElement, 'img.has-bulletins');

                    // get the processor grid/data
                    var processorsGrid = processorsGridElement.data('gridInstance');
                    var processorsData = processorsGrid.getData();

                    // get the connections grid/data (do not render bulletins)
                    var connectionsGrid = $('#connection-summary-table').data('gridInstance');
                    var connectionsData = connectionsGrid.getData();

                    // remove any tooltips from the input port table
                    var inputPortsGridElement = $('#input-port-summary-table');
                    nf.Common.cleanUpTooltips(inputPortsGridElement, 'img.has-bulletins');

                    // get the input ports grid/data
                    var inputPortsGrid = inputPortsGridElement.data('gridInstance');
                    var inputPortsData = inputPortsGrid.getData();

                    // remove any tooltips from the output port table
                    var outputPortsGridElement = $('#output-port-summary-table');
                    nf.Common.cleanUpTooltips(outputPortsGridElement, 'img.has-bulletins');

                    // get the output ports grid/data
                    var outputPortsGrid = outputPortsGridElement.data('gridInstance');
                    var outputPortsData = outputPortsGrid.getData();

                    // remove any tooltips from the remote process group table
                    var remoteProcessGroupsGridElement = $('#remote-process-group-summary-table');
                    nf.Common.cleanUpTooltips(remoteProcessGroupsGridElement, 'img.has-bulletins');

                    // get the remote process groups grid
                    var remoteProcessGroupsGrid = remoteProcessGroupsGridElement.data('gridInstance');
                    var remoteProcessGroupsData = remoteProcessGroupsGrid.getData();

                    var processorItems = [];
                    var connectionItems = [];
                    var inputPortItems = [];
                    var outputPortItems = [];
                    var remoteProcessGroupItems = [];

                    // populate the tables
                    populateProcessGroupStatus(processorItems, connectionItems, inputPortItems, outputPortItems, remoteProcessGroupItems, processGroupStatus);

                    // update the processors
                    processorsData.setItems(processorItems);
                    processorsData.reSort();
                    processorsGrid.invalidate();

                    // update the connections
                    connectionsData.setItems(connectionItems);
                    connectionsData.reSort();
                    connectionsGrid.invalidate();

                    // update the input ports
                    inputPortsData.setItems(inputPortItems);
                    inputPortsData.reSort();
                    inputPortsGrid.invalidate();

                    // update the output ports
                    outputPortsData.setItems(outputPortItems);
                    outputPortsData.reSort();
                    outputPortsGrid.invalidate();

                    // update the remote process groups
                    remoteProcessGroupsData.setItems(remoteProcessGroupItems);
                    remoteProcessGroupsData.reSort();
                    remoteProcessGroupsGrid.invalidate();

                    // update the stats last refreshed timestamp
                    $('#summary-last-refreshed').text(processGroupStatus.statsLastRefreshed);

                    // update the total number of processors
                    if ($('#processor-summary-table').is(':visible')) {
                        $('#total-items').text(nf.Common.formatInteger(processorItems.length));
                    } else if ($('#connection-summary-table').is(':visible')) {
                        $('#total-items').text(nf.Common.formatInteger(connectionItems.length));
                    } else if ($('#input-port-summary-table').is(':visible')) {
                        $('#total-items').text(nf.Common.formatInteger(inputPortItems.length));
                    } else if ($('#output-port-summary-table').is(':visible')) {
                        $('#total-items').text(nf.Common.formatInteger(outputPortItems.length));
                    } else {
                        $('#total-items').text(nf.Common.formatInteger(remoteProcessGroupItems.length));
                    }
                } else {
                    $('#total-items').text('0');
                }
            }).fail(nf.Common.handleAjaxError);
        }
    };
}());