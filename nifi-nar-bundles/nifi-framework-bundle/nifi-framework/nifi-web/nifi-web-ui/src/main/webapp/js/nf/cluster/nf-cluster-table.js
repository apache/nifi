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

/* global nf, Slick */

nf.ClusterTable = (function () {

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        primaryNode: 'Primary Node',
        clusterCoorindator: 'Cluster Coordinator',
        urls: {
            cluster: '../nifi-api/controller/cluster',
            nodes: '../nifi-api/controller/cluster/nodes'
        }
    };

    var prevColumn, count;

    /**
     * Sorts the specified data using the specified sort details.
     *
     * @param {object} sortDetails
     * @param {object} data
     */
    var sort = function (sortDetails, data) {
        // defines a function for sorting
        var comparer = function (a, b) {
            if (sortDetails.columnId === 'heartbeat' || sortDetails.columnId === 'uptime') {
                var aDate = nf.Common.parseDateTime(a[sortDetails.columnId]);
                var bDate = nf.Common.parseDateTime(b[sortDetails.columnId]);
                return aDate.getTime() - bDate.getTime();
            } else if (sortDetails.columnId === 'queued') {
                var aSplit = a[sortDetails.columnId].split(/ \/ /);
                var bSplit = b[sortDetails.columnId].split(/ \/ /);
                var mod = count % 4;
                if (mod < 2) {
                    $('#cluster-table span.queued-title').addClass('sorted');
                    var aCount = nf.Common.parseCount(aSplit[0]);
                    var bCount = nf.Common.parseCount(bSplit[0]);
                    return aCount - bCount;
                } else {
                    $('#cluster-table span.queued-size-title').addClass('sorted');
                    var aSize = nf.Common.parseSize(aSplit[1]);
                    var bSize = nf.Common.parseSize(bSplit[1]);
                    return aSize - bSize;
                }
            } else if (sortDetails.columnId === 'status') {
                var aString = nf.Common.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
                if (a.roles.includes(config.primaryNode)) {
                    aString += ', PRIMARY';
                }
                var bString = nf.Common.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
                if (b.roles.includes(config.primaryNode)) {
                    bString += ', PRIMARY';
                }
                return aString === bString ? 0 : aString > bString ? 1 : -1;
            } else if (sortDetails.columnId === 'node') {
                var aNode = formatNodeAddress(a);
                var bNode = formatNodeAddress(b);
                return aNode === bNode ? 0 : aNode > bNode ? 1 : -1;
            } else {
                var aString = nf.Common.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
                var bString = nf.Common.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
                return aString === bString ? 0 : aString > bString ? 1 : -1;
            }
        };

        // remove previous sort indicators
        $('#cluster-table span.queued-title').removeClass('sorted');
        $('#cluster-table span.queued-size-title').removeClass('sorted');

        // update/reset the count as appropriate
        if (prevColumn !== sortDetails.columnId) {
            count = 0;
        } else {
            count++;
        }

        // perform the sort
        data.sort(comparer, sortDetails.sortAsc);

        // record the previous table and sorted column
        prevColumn = sortDetails.columnId;
    };

    /**
     * Formats the address for the specified noe.
     *
     * @param {object} node
     * @returns {string}
     */
    var formatNodeAddress = function (node) {
        return nf.Common.escapeHtml(node.address) + ':' + nf.Common.escapeHtml(node.apiPort);
    };

    /**
     * Prompts to verify node connection.
     *
     * @argument {object} node     The node
     */
    var promptForConnect = function (node) {
        // prompt to connect
        nf.Dialog.showYesNoDialog({
            headerText: 'Connect Node',
            dialogContent: 'Connect \'' + formatNodeAddress(node) + '\' to this cluster?',
            yesHandler: function () {
                connect(node.nodeId);
            }
        });
    };

    /**
     * Connects the node in the specified row.
     *
     * @argument {string} nodeId     The node id
     */
    var connect = function (nodeId) {
        var entity = {
            'node': {
                'nodeId': nodeId,
                'status': 'CONNECTING'
            }
        };

        $.ajax({
            type: 'PUT',
            url: config.urls.nodes + '/' + encodeURIComponent(nodeId),
            data: JSON.stringify(entity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (response) {
            var node = response.node;

            // update the node in the table
            var clusterGrid = $('#cluster-table').data('gridInstance');
            var clusterData = clusterGrid.getData();
            clusterData.updateItem(node.nodeId, node);
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Prompts to verify node disconnection.
     *
     * @argument {object} node     The node
     */
    var promptForDisconnect = function (node) {
        // prompt for disconnect
        nf.Dialog.showYesNoDialog({
            headerText: 'Disconnect Node',
            dialogContent: 'Disconnect \'' + formatNodeAddress(node) + '\' from the cluster?',
            yesHandler: function () {
                disconnect(node.nodeId);
            }
        });
    };

    /**
     * Disconnects the node in the specified row.
     *
     * @argument {string} nodeId     The node id
     */
    var disconnect = function (nodeId) {
        var entity = {
            'node': {
                'nodeId': nodeId,
                'status': 'DISCONNECTING'
            }
        };

        $.ajax({
            type: 'PUT',
            url: config.urls.nodes + '/' + encodeURIComponent(nodeId),
            data: JSON.stringify(entity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (response) {
            var node = response.node;

            // update the node in the table
            var clusterGrid = $('#cluster-table').data('gridInstance');
            var clusterData = clusterGrid.getData();
            clusterData.updateItem(node.nodeId, node);
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Prompts to verify node disconnection.
     *
     * @argument {object} node     The node
     */
    var promptForRemoval = function (node) {
        // prompt for disconnect
        nf.Dialog.showYesNoDialog({
            headerText: 'Remove Node',
            dialogContent: 'Remove \'' + formatNodeAddress(node) + '\' from the cluster?',
            yesHandler: function () {
                remove(node.nodeId);
            }
        });
    };

    /**
     * Disconnects the node in the specified row.
     *
     * @argument {string} nodeId     The node id
     */
    var remove = function (nodeId) {
        $.ajax({
            type: 'DELETE',
            url: config.urls.nodes + '/' + encodeURIComponent(nodeId),
            dataType: 'json'
        }).done(function () {
            // get the table and update the row accordingly
            var clusterGrid = $('#cluster-table').data('gridInstance');
            var clusterData = clusterGrid.getData();
            clusterData.deleteItem(nodeId);
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Get the text out of the filter field. If the filter field doesn't
     * have any text it will contain the text 'filter list' so this method
     * accounts for that.
     */
    var getFilterText = function () {
        return $('#cluster-filter').val();
    };

    /**
     * Applies the filter found in the filter expression text field.
     */
    var applyFilter = function () {
        // get the dataview
        var clusterGrid = $('#cluster-table').data('gridInstance');

        // ensure the grid has been initialized
        if (nf.Common.isDefinedAndNotNull(clusterGrid)) {
            var clusterData = clusterGrid.getData();

            // update the search criteria
            clusterData.setFilterArgs({
                searchString: getFilterText(),
                property: $('#cluster-filter-type').combo('getSelectedOption').value
            });
            clusterData.refresh();
        }
    };

    /**
     * Performs the filtering.
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

        // perform the filter
        return item[args.property].search(filterExp) >= 0;
    };

    /**
     * Show the node details.
     *
     * @argument {object} item     The item
     */
    var showNodeDetails = function (item) {
        $.ajax({
            type: 'GET',
            url: config.urls.nodes + '/' + encodeURIComponent(item.nodeId),
            dataType: 'json'
        }).done(function (response) {
            var node = response.node;

            // update the dialog fields
            $('#node-id').text(node.nodeId);
            $('#node-address').text(formatNodeAddress(node));

            // format the events
            var events = $('#node-events');
            if ($.isArray(node.events) && node.events.length > 0) {
                var eventMessages = [];
                $.each(node.events, function (i, event) {
                    eventMessages.push(event.timestamp + ": " + event.message);
                });
                $('<div></div>').append(nf.Common.formatUnorderedList(eventMessages)).appendTo(events);
            } else {
                events.append('<div><span class="unset">None</span></div>');
            }

            // show the dialog
            $('#node-details-dialog').modal('show');
        }).fail(nf.Common.handleAjaxError);
    };
    
    return {
        /**
         * Initializes the cluster list.
         */
        init: function () {
            // initialize the user details dialog
            $('#node-details-dialog').modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'Node Details',
                buttons: [{
                    buttonText: 'Ok',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            $('#node-details-dialog').modal('hide');
                        }
                    }
                }],
                handler: {
                    close: function () {
                        // clear the details
                        $('#node-address').text('');
                        $('#node-id').text('');
                        $('#node-events').empty();
                    }
                }
            });

            // define the function for filtering the list
            $('#cluster-filter').keyup(function () {
                applyFilter();
            });

            // filter type
            $('#cluster-filter-type').combo({
                options: [{
                    text: 'by address',
                    value: 'address'
                }, {
                    text: 'by status',
                    value: 'status'
                }],
                select: function (option) {
                    applyFilter();
                }
            });

            // listen for browser resize events to update the page size
            $(window).resize(function () {
                nf.ClusterTable.resetTableSize();
            });

            // define a custom formatter for the more details column
            var moreDetailsFormatter = function (row, cell, value, columnDef, dataContext) {
                return '<div title="View Details" class="pointer show-node-details fa fa-info-circle" style="margin-top: 2px;"></div>';
            };

            // define a custom formatter for the run status column
            var nodeFormatter = function (row, cell, value, columnDef, dataContext) {
                return formatNodeAddress(dataContext);
            };

            // function for formatting the last accessed time
            var valueFormatter = function (row, cell, value, columnDef, dataContext) {
                return nf.Common.formatValue(value);
            };

            // define a custom formatter for the status column
            var statusFormatter = function (row, cell, value, columnDef, dataContext) {
                var markup = value;
                if (dataContext.roles.includes(config.primaryNode)) {
                    value += ', PRIMARY';
                }
                if (dataContext.roles.includes(config.clusterCoorindator)) {
                    value += ', COORDINATOR';
                }
                return value;
            };

            var columnModel = [
                {id: 'moreDetails', name: '&nbsp;', sortable: false, resizable: false, formatter: moreDetailsFormatter, width: 50, maxWidth: 50},
                {id: 'node', field: 'node', name: 'Node Address', formatter: nodeFormatter, resizable: true, sortable: true},
                {id: 'activeThreadCount', field: 'activeThreadCount', name: 'Active Thread Count', resizable: true, sortable: true, defaultSortAsc: false},
                {id: 'queued', field: 'queued', name: '<span class="queued-title">Queue</span>&nbsp;/&nbsp;<span class="queued-size-title">Size</span>', resizable: true, sortable: true, defaultSortAsc: false},
                {id: 'status', field: 'status', name: 'Status', formatter: statusFormatter, resizable: true, sortable: true},
                {id: 'uptime', field: 'nodeStartTime', name: 'Uptime', formatter: valueFormatter, resizable: true, sortable: true, defaultSortAsc: false},
                {id: 'heartbeat', field: 'heartbeat', name: 'Last Heartbeat', formatter: valueFormatter, resizable: true, sortable: true, defaultSortAsc: false}
            ];

            // only allow the admin to modify the cluster
            if (nf.Common.canModifyController()) {
                // function for formatting the actions column
                var actionFormatter = function (row, cell, value, columnDef, dataContext) {
                    var canDisconnect = false;
                    var canConnect = false;

                    // determine the current status
                    if (dataContext.status === 'CONNECTED' || dataContext.status === 'CONNECTING') {
                        canDisconnect = true;
                    } else if (dataContext.status === 'DISCONNECTED') {
                        canConnect = true;
                    }

                    // return the appropriate markup
                    if (canConnect) {
                        return '<div title="Connect" class="pointer prompt-for-connect fa fa-plug" style="margin-top: 2px;"></div>&nbsp;<div title="Delete" class="pointer prompt-for-removal fa fa-trash" style="margin-top: 2px;"></div>';
                    } else if (canDisconnect) {
                        return '<div title="Disconnect" class="pointer prompt-for-disconnect fa fa-power-off" style="margin-top: 2px;"></div>';
                    } else {
                        return '<div style="width: 16px; height: 16px;">&nbsp;</div>';
                    }
                };

                columnModel.push({id: 'actions', label: '&nbsp;', formatter: actionFormatter, resizable: false, sortable: false, width: 80, maxWidth: 80});
            }

            var clusterOptions = {
                forceFitColumns: true,
                enableTextSelectionOnCells: true,
                enableCellNavigation: false,
                enableColumnReorder: false,
                autoEdit: false,
                rowHeight: 24
            };

            // initialize the dataview
            var clusterData = new Slick.Data.DataView({
                inlineFilters: false
            });
            clusterData.setItems([], 'nodeId');
            clusterData.setFilterArgs({
                searchString: getFilterText(),
                property: $('#cluster-filter-type').combo('getSelectedOption').value
            });
            clusterData.setFilter(filter);

            // initialize the sort
            sort({
                columnId: 'node',
                sortAsc: true
            }, clusterData);

            // initialize the grid
            var clusterGrid = new Slick.Grid('#cluster-table', clusterData, columnModel, clusterOptions);
            clusterGrid.setSelectionModel(new Slick.RowSelectionModel());
            clusterGrid.setSortColumn('node', true);
            clusterGrid.onSort.subscribe(function (e, args) {
                sort({
                    columnId: args.sortCol.id,
                    sortAsc: args.sortAsc
                }, clusterData);
            });

            // configure a click listener
            clusterGrid.onClick.subscribe(function (e, args) {
                var target = $(e.target);

                // get the node at this row
                var item = clusterData.getItem(args.row);

                // determine the desired action
                if (clusterGrid.getColumns()[args.cell].id === 'actions') {
                    if (target.hasClass('prompt-for-connect')) {
                        promptForConnect(item);
                    } else if (target.hasClass('prompt-for-removal')) {
                        promptForRemoval(item);
                    } else if (target.hasClass('prompt-for-disconnect')) {
                        promptForDisconnect(item);
                    }
                } else if (clusterGrid.getColumns()[args.cell].id === 'moreDetails') {
                    if (target.hasClass('show-node-details')) {
                        showNodeDetails(item);
                    }
                }
            });

            // wire up the dataview to the grid
            clusterData.onRowCountChanged.subscribe(function (e, args) {
                clusterGrid.updateRowCount();
                clusterGrid.render();

                // update the total number of displayed processors
                $('#displayed-nodes').text(args.current);
            });
            clusterData.onRowsChanged.subscribe(function (e, args) {
                clusterGrid.invalidateRows(args.rows);
                clusterGrid.render();
            });

            // hold onto an instance of the grid
            $('#cluster-table').data('gridInstance', clusterGrid);

            // initialize the number of displayed items
            $('#displayed-nodes').text('0');
        },

        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            var clusterGrid = $('#cluster-table').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(clusterGrid)) {
                clusterGrid.resizeCanvas();
            }
        },

        /**
         * Load the processor cluster table.
         */
        loadClusterTable: function () {
            return $.ajax({
                type: 'GET',
                url: config.urls.cluster,
                dataType: 'json'
            }).done(function (response) {
                var cluster = response.cluster;

                // ensure there are groups specified
                if (nf.Common.isDefinedAndNotNull(cluster.nodes)) {
                    var clusterGrid = $('#cluster-table').data('gridInstance');
                    var clusterData = clusterGrid.getData();

                    // set the items
                    clusterData.setItems(cluster.nodes);
                    clusterData.reSort();
                    clusterGrid.invalidate();

                    // update the stats last refreshed timestamp
                    $('#cluster-last-refreshed').text(cluster.generated);

                    // update the total number of processors
                    $('#total-nodes').text(cluster.nodes.length);
                } else {
                    $('#total-nodes').text('0');
                }
            }).fail(nf.Common.handleAjaxError);
        }
    };
}());