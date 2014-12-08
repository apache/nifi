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
nf.ClusterTable = (function () {

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        filterText: 'Filter',
        styles: {
            filterList: 'cluster-filter-list'
        },
        urls: {
            cluster: '../nifi-api/cluster',
            nodes: '../nifi-api/cluster/nodes'
        }
    };

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
                var aCount = nf.Common.parseCount(a[sortDetails.columnId]);
                var bCount = nf.Common.parseCount(b[sortDetails.columnId]);
                return aCount - bCount;
            } else if (sortDetails.columnId === 'status') {
                var aString = nf.Common.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
                if (a.primary === true) {
                    aString += ', PRIMARY';
                }
                var bString = nf.Common.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
                if (b.primary === true) {
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

        // perform the sort
        data.sort(comparer, sortDetails.sortAsc);
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
     * Connects the node in the specified row.
     * 
     * @argument {string} nodeId     The node id
     */
    var connect = function (nodeId) {
        $.ajax({
            type: 'PUT',
            url: config.urls.nodes + '/' + encodeURIComponent(nodeId),
            data: {
                status: 'CONNECTING'
            },
            dataType: 'json'
        }).then(function (response) {
            var node = response.node;

            // update the node in the table
            var clusterGrid = $('#cluster-table').data('gridInstance');
            var clusterData = clusterGrid.getData();
            clusterData.updateItem(node.nodeId, node);
        }, nf.Common.handleAjaxError);
    };

    /**
     * Disconnects the node in the specified row.
     * 
     * @argument {string} nodeId     The node id
     */
    var disconnect = function (nodeId) {
        $.ajax({
            type: 'PUT',
            url: config.urls.nodes + '/' + encodeURIComponent(nodeId),
            data: {
                status: 'DISCONNECTING'
            },
            dataType: 'json'
        }).then(function (response) {
            var node = response.node;

            // update the node in the table
            var clusterGrid = $('#cluster-table').data('gridInstance');
            var clusterData = clusterGrid.getData();
            clusterData.updateItem(node.nodeId, node);
        }, nf.Common.handleAjaxError);
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
        }).then(function () {
            // get the table and update the row accordingly
            var clusterGrid = $('#cluster-table').data('gridInstance');
            var clusterData = clusterGrid.getData();
            clusterData.deleteItem(nodeId);
        }, nf.Common.handleAjaxError);
    };

    /**
     * Get the text out of the filter field. If the filter field doesn't
     * have any text it will contain the text 'filter list' so this method
     * accounts for that.
     */
    var getFilterText = function () {
        var filterText = '';
        var filterField = $('#cluster-filter');
        if (!filterField.hasClass(config.styles.filterList)) {
            filterText = filterField.val();
        }
        return filterText;
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

    return {
        /**
         * Initializes the cluster list.
         */
        init: function () {
            // initialize the user details dialog
            $('#node-details-dialog').modal({
                headerText: 'Node Details',
                overlayBackground: false,
                buttons: [{
                        buttonText: 'Ok',
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
            }).focus(function () {
                if ($(this).hasClass(config.styles.filterList)) {
                    $(this).removeClass(config.styles.filterList).val('');
                }
            }).blur(function () {
                if ($(this).val() === '') {
                    $(this).addClass(config.styles.filterList).val(config.filterText);
                }
            }).addClass(config.styles.filterList).val(config.filterText);

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
                return '<img src="images/iconDetails.png" title="View Details" class="pointer" style="margin-top: 4px;" onclick="javascript:nf.ClusterTable.showNodeDetails(\'' + row + '\');"/>';
            };

            // define a custom formatter for the run status column
            var nodeFormatter = function (row, cell, value, columnDef, dataContext) {
                return formatNodeAddress(dataContext);
            };

            // define a custom formatter for the status column
            var statusFormatter = function (row, cell, value, columnDef, dataContext) {
                if (dataContext.primary === true) {
                    return value + ', PRIMARY';
                } else {
                    return value;
                }
            };

            // function for formatting the last accessed time
            var valueFormatter = function (row, cell, value, columnDef, dataContext) {
                return nf.Common.formatValue(value);
            };

            var columnModel = [
                {id: 'moreDetails', name: '&nbsp;', sortable: false, resizable: false, formatter: moreDetailsFormatter, width: 50, maxWidth: 50},
                {id: 'node', field: 'node', name: 'Node Address', formatter: nodeFormatter, resizable: true, sortable: true},
                {id: 'activeThreadCount', field: 'activeThreadCount', name: 'Active Thread Count', resizable: true, sortable: true},
                {id: 'queued', field: 'queued', name: 'Queued <span style="font-weight: normal; overflow: hidden;">(count / size)</span>', resizable: true, sortable: true},
                {id: 'status', field: 'status', name: 'Status', formatter: statusFormatter, resizable: true, sortable: true},
                {id: 'uptime', field: 'nodeStartTime', name: 'Uptime', formatter: valueFormatter, resizable: true, sortable: true},
                {id: 'heartbeat', field: 'heartbeat', name: 'Last Heartbeat', formatter: valueFormatter, resizable: true, sortable: true}
            ];

            // only allow the admin to modify the cluster
            if (nf.Common.isAdmin()) {
                // function for formatting the actions column
                var actionFormatter = function (row, cell, value, columnDef, dataContext) {
                    var canDisconnect = false;
                    var canConnect = false;
                    var canBecomePrimary = false;

                    // determine if this node is already the primary
                    var isPrimary = dataContext.primary;

                    // determine the current status
                    if (dataContext.status === 'CONNECTED' || dataContext.status === 'CONNECTING') {
                        // only non-primary connected nodes can become primary
                        if (isPrimary === false && dataContext.status === 'CONNECTED') {
                            canBecomePrimary = true;
                        }
                        canDisconnect = true;
                    } else if (dataContext.status === 'DISCONNECTED') {
                        canConnect = true;
                    }

                    // return the appropriate markup
                    if (canConnect) {
                        return '<img src="images/iconConnect.png" title="Connect" class="pointer" style="margin-top: 2px;" onclick="javascript:nf.ClusterTable.promptForConnect(\'' + row + '\');"/>&nbsp;<img src="images/iconDelete.png" title="Remove" class="pointer" onclick="javascript:nf.ClusterTable.promptForRemoval(\'' + row + '\');"/>';
                    } else if (canDisconnect) {
                        var actions = '<img src="images/iconDisconnect.png" title="Disconnect" class="pointer" style="margin-top: 2px;" onclick="javascript:nf.ClusterTable.promptForDisconnect(\'' + row + '\');"/>';
                        if (canBecomePrimary) {
                            actions += '&nbsp;<img src="images/iconPrimary.png" title="Make Primary" class="pointer" style="margin-top: 2px;" onclick="javascript:nf.ClusterTable.makePrimary(\'' + row + '\');"/>';
                        }
                        return actions;
                    } else {
                        return '<div style="width: 16px; height: 16px;">&nbsp;</div>';
                    }
                };

                columnModel.push({id: 'action', label: '&nbsp;', formatter: actionFormatter, resizable: false, sortable: false, width: 80, maxWidth: 80});
            }

            var clusterOptions = {
                forceFitColumns: true,
                enableTextSelectionOnCells: true,
                enableCellNavigation: false,
                enableColumnReorder: false,
                autoEdit: false
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
                columnId: 'userName',
                sortAsc: true
            }, clusterData);

            // initialize the grid
            var clusterGrid = new Slick.Grid('#cluster-table', clusterData, columnModel, clusterOptions);
            clusterGrid.setSelectionModel(new Slick.RowSelectionModel());
            clusterGrid.setSortColumn('node', true);
            clusterGrid.onSort.subscribe(function (e, args) {
                sort({
                    columnId: args.sortCol.field,
                    sortAsc: args.sortAsc
                }, clusterData);
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
         * Prompts to verify node connection.
         * 
         * @argument {string} row     The row
         */
        promptForConnect: function (row) {
            var grid = $('#cluster-table');
            if (nf.Common.isDefinedAndNotNull(grid)) {
                var data = grid.getData();
                var node = data.getItem(row);

                // prompt to connect
                nf.Dialog.showYesNoDialog({
                    dialogContent: 'Connect \'' + formatNodeAddress(node) + '\' to this cluster?',
                    overlayBackground: false,
                    yesHandler: function () {
                        connect(node.nodeId);
                    }
                });
            }

        },
        /**
         * Prompts to verify node disconnection.
         * 
         * @argument {string} row     The row
         */
        promptForDisconnect: function (row) {
            var grid = $('#cluster-table');
            if (nf.Common.isDefinedAndNotNull(grid)) {
                var data = grid.getData();
                var node = data.getItem(row);

                // prompt for disconnect
                nf.Dialog.showYesNoDialog({
                    dialogContent: 'Disconnect \'' + formatNodeAddress(node) + '\' from the cluster?',
                    overlayBackground: false,
                    yesHandler: function () {
                        disconnect(node.nodeId);
                    }
                });
            }
        },
        /**
         * Makes the specified node the primary node of the cluster.
         * 
         * @argument {string} row     The row
         */
        makePrimary: function (row) {
            var grid = $('#cluster-table');
            if (nf.Common.isDefinedAndNotNull(grid)) {
                var data = grid.getData();
                var item = data.getItem(row);

                $.ajax({
                    type: 'PUT',
                    url: config.urls.nodes + '/' + encodeURIComponent(item.nodeId),
                    data: {
                        primary: true
                    },
                    dataType: 'json'
                }).then(function (response) {
                    var node = response.node;

                    var clusterGrid = $('#cluster-table').data('gridInstance');
                    var clusterData = clusterGrid.getData();

                    // start the update
                    clusterData.beginUpdate();
                    clusterData.updateItem(node.nodeId, node);

                    // need to find the previous primary node
                    // get the property grid data
                    var clusterItems = clusterData.getItems();
                    $.each(clusterItems, function (i, otherNode) {
                        // attempt to identify the previous primary node
                        if (node.nodeId !== otherNode.nodeId && otherNode.primary === true) {
                            // reset its primary status
                            otherNode.primary = false;
                            otherNode.status = 'CONNECTED';

                            // set the new node state
                            clusterData.updateItem(otherNode.nodeId, otherNode);

                            // no need to continue processing
                            return false;
                        }
                    });

                    // end the update
                    clusterData.endUpdate();
                }, nf.Common.handleAjaxError);
            }
        },
        /**
         * Prompts to verify node disconnection.
         * 
         * @argument {string} row     The row
         */
        promptForRemoval: function (row) {
            var grid = $('#cluster-table');
            if (nf.Common.isDefinedAndNotNull(grid)) {
                var data = grid.getData();
                var node = data.getItem(row);

                // prompt for disconnect
                nf.Dialog.showYesNoDialog({
                    dialogContent: 'Remove \'' + formatNodeAddress(node) + '\' from the cluster?',
                    overlayBackground: false,
                    yesHandler: function () {
                        remove(node.nodeId);
                    }
                });
            }
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
            }).then(function (response) {
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
            }, nf.Common.handleAjaxError);
        },
        /**
         * Populate the expanded row.
         * 
         * @argument {string} row     The row
         */
        showNodeDetails: function (row) {
            var grid = $('#cluster-table');
            if (nf.Common.isDefinedAndNotNull(grid)) {
                var data = grid.getData();
                var item = data.getItem(row);

                $.ajax({
                    type: 'GET',
                    url: config.urls.nodes + '/' + encodeURIComponent(item.nodeId),
                    dataType: 'json'
                }).then(function (response) {
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
                }, nf.Common.handleAjaxError);
            }
        }
    };
}());