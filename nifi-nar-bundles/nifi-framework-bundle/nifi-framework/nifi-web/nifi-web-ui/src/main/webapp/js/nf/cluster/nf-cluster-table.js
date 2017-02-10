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

/* global define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'Slick',
                'nf.Common',
                'nf.Dialog',
                'nf.ErrorHandler'],
            function ($, Slick, common, dialog, errorHandler) {
                return (nf.ClusterTable = factory($, Slick, common, dialog, errorHandler));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ClusterTable =
            factory(require('jquery'),
                require('Slick'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.ErrorHandler')));
    } else {
        nf.ClusterTable = factory(root.$,
            root.Slick,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.ErrorHandler);
    }
}(this, function ($, Slick, common, dialog, errorHandler) {
    'use strict';

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        primaryNode: 'Primary Node',
        clusterCoordinator: 'Cluster Coordinator',
        urls: {
            cluster: '../nifi-api/controller/cluster',
            nodes: '../nifi-api/controller/cluster/nodes',
            systemDiagnostics: '../nifi-api/system-diagnostics'
        },
        data: [{
            name: 'cluster',
            update: refreshClusterData,
            isAuthorized: common.canAccessController
        }, {
            name: 'systemDiagnostics',
            update: refreshSystemDiagnosticsData,
            isAuthorized: common.canAccessSystem
        }
        ]
    };

    var commonTableOptions = {
        forceFitColumns: true,
        enableTextSelectionOnCells: true,
        enableCellNavigation: false,
        enableColumnReorder: false,
        autoEdit: false,
        rowHeight: 24
    };

    var nodesTab = {
        name: 'Nodes',
        data: {
            dataSet: 'cluster',
            update: updateNodesTableData
        },
        tabContentId: 'cluster-nodes-tab-content',
        tableId: 'cluster-nodes-table',
        tableColumnModel: createNodeTableColumnModel,
        tableIdColumn: 'nodeId',
        tableOptions: commonTableOptions,
        tableOnClick: nodesTableOnClick,
        init: commonTableInit,
        onSort: sort,
        onTabSelected: onSelectTab,
        filterOptions: [{
            text: 'by address',
            value: 'address'
        }, {
            text: 'by status',
            value: 'status'
        }]
    };

    var jvmTab = {
        name: 'JVM',
        data: {
            dataSet: 'systemDiagnostics',
            update: updateJvmTableData
        },
        tabContentId: 'cluster-jvm-tab-content',
        tableId: 'cluster-jvm-table',
        tableColumnModel: [
            {id: 'node', field: 'node', name: 'Node Address', sortable: true, resizable: true},
            {
                id: 'heapMax',
                field: 'maxHeap',
                name: 'Heap Max',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'heapTotal',
                field: 'totalHeap',
                name: 'Heap Total',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'heapUsed',
                field: 'usedHeap',
                name: 'Heap Used',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'heapUtilPct',
                field: 'heapUtilization',
                name: 'Heap Utilization',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'nonHeapTotal',
                field: 'totalNonHeap',
                name: 'Non-Heap Total',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'nonHeapUsed',
                field: 'usedNonHeap',
                name: 'Non-Heap Used',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'gcOldGen',
                field: 'gcOldGen',
                name: 'G1 Old Generation',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'gcNewGen',
                field: 'gcNewGen',
                name: 'G1 Young Generation',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            }
        ],
        tableIdColumn: 'id',
        tableOptions: commonTableOptions,
        tableOnClick: null,
        init: commonTableInit,
        onSort: sort,
        onTabSelected: onSelectTab,
        filterOptions: [{
            text: 'by address',
            value: 'node'
        }]
    };

    var systemTab = {
        name: 'System',
        data: {
            dataSet: 'systemDiagnostics',
            update: updateSystemTableData
        },
        tabContentId: 'cluster-system-tab-content',
        tableId: 'cluster-system-table',
        tableColumnModel: [
            {id: 'node', field: 'node', name: 'Node Address', sortable: true, resizable: true},
            {
                id: 'processors',
                field: 'processors',
                name: 'Processors',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'processorLoadAverage',
                field: 'processorLoadAverage',
                name: 'Processor Load Average',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'totalThreads',
                field: 'totalThreads',
                name: 'Total Threads',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'daemonThreads',
                field: 'daemonThreads',
                name: 'Daemon Threads',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            }
        ],
        tableIdColumn: 'id',
        tableOptions: commonTableOptions,
        tableOnClick: null,
        init: commonTableInit,
        onSort: sort,
        onTabSelected: onSelectTab,
        filterOptions: [{
            text: 'by address',
            value: 'node'
        }]
    };

    var flowFileTab = {
        name: 'FlowFile Storage',
        data: {
            dataSet: 'systemDiagnostics',
            update: updateFlowFileTableData
        },
        tabContentId: 'cluster-flowfile-tab-content',
        tableId: 'cluster-flowfile-table',
        tableColumnModel: [
            {id: 'node', field: 'node', name: 'Node Address', sortable: true, resizable: true},
            {
                id: 'ffRepoTotal',
                field: 'ffRepoTotal',
                name: 'Total Space',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'ffRepoUsed',
                field: 'ffRepoUsed',
                name: 'Used Space',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'ffRepoFree',
                field: 'ffRepoFree',
                name: 'Free Space',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'ffStoreUtil',
                field: 'ffRepoUtil',
                name: 'Utilization',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            }
        ],
        tableIdColumn: 'id',
        tableOptions: commonTableOptions,
        tableOnClick: null,
        init: commonTableInit,
        onSort: sort,
        onTabSelected: onSelectTab,
        filterOptions: [{
            text: 'by address',
            value: 'node'
        }]
    };

    var contentTab = {
        name: 'Content Storage',
        data: {
            dataSet: 'systemDiagnostics',
            update: updateContentTableData
        },
        tabContentId: 'cluster-content-tab-content',
        tableId: 'cluster-content-table',
        tableColumnModel: [
            {id: 'node', field: 'node', name: 'Node Address', sortable: true, resizable: true},
            {id: 'contentRepoId', field: 'contentRepoId', name: 'Content Repository', sortable: true, resizable: true},
            {
                id: 'contentRepoTotal',
                field: 'contentRepoTotal',
                name: 'Total Space',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'contentRepoUsed',
                field: 'contentRepoUsed',
                name: 'Used Space',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'contentRepoFree',
                field: 'contentRepoFree',
                name: 'Free Space',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            },
            {
                id: 'contentRepoUtil',
                field: 'contentRepoUtil',
                name: 'Utilization',
                sortable: true,
                resizable: true,
                cssClass: 'cell-right',
                headerCssClass: 'header-right'
            }
        ],
        tableIdColumn: 'id',
        tableOptions: commonTableOptions,
        tableOnClick: null,
        init: commonTableInit,
        onSort: sort,
        onTabSelected: onSelectTab,
        filterOptions: [{
            text: 'by address',
            value: 'node'
        }, {
            text: 'by repository',
            value: 'contentRepoId'
        }]
    };

    var versionTab = {
        name: 'Versions',
        data: {
            dataSet: 'systemDiagnostics',
            update: updateVersionTableData
        },
        tabContentId: 'cluster-version-tab-content',
        tableId: 'cluster-version-table',
        tableColumnModel: [
            {id: 'node', field: 'node', name: 'Node Address', sortable: true, resizable: true},
            {id: 'version', field: 'version', name: 'NiFi Version', sortable: true, resizable: true},
            {id: 'javavendor', field: 'javaVendor', name: 'Java Vendor', sortable: true, resizable: true},
            {id: 'javaversion', field: 'javaVersion', name: 'Java Version', sortable: true, resizable: true},
            {id: 'osname', field: 'osName', name: 'OS Name', sortable: true, resizable: true},
            {id: 'osversion', field: 'osVersion', name: 'OS Version', sortable: true, resizable: true},
            {id: 'osarch', field: 'osArchitecture', name: 'OS Architecture', sortable: true, resizable: true}
        ],
        tableIdColumn: 'id',
        tableOptions: commonTableOptions,
        tableOnClick: null,
        init: commonTableInit,
        onSort: sort,
        onTabSelected: onSelectTab,
        filterOptions: [{
            text: 'by address',
            value: 'address'
        }]
    };

    var clusterTabs = [nodesTab, systemTab, jvmTab, flowFileTab, contentTab, versionTab];
    var tabsByName = {};
    var dataSetHandlers = {};

    /**
     * Click handler for the Nodes table options.
     */
    function nodesTableOnClick(e, args, target, item) {
        if (nodesTab.grid.getColumns()[args.cell].id === 'actions') {
            if (target.hasClass('prompt-for-connect')) {
                promptForConnect(item);
            } else if (target.hasClass('prompt-for-removal')) {
                promptForRemoval(item);
            } else if (target.hasClass('prompt-for-disconnect')) {
                promptForDisconnect(item);
            }
        } else if (nodesTab.grid.getColumns()[args.cell].id === 'moreDetails') {
            if (target.hasClass('show-node-details')) {
                showNodeDetails(item);
            }
        }
    }

    /**
     * Creates the Slick Grid column model for the Nodes table.
     */
    function createNodeTableColumnModel() {
        var moreDetailsFormatter = function (row, cell, value, columnDef, dataContext) {
            return '<div title="View Details" class="pointer show-node-details fa fa-info-circle" style="margin-top: 2px;"></div>';
        };

        // define a custom formatter for the run status column
        var nodeFormatter = function (row, cell, value, columnDef, dataContext) {
            return formatNodeAddress(dataContext);
        };

        // function for formatting the last accessed time
        var valueFormatter = function (row, cell, value, columnDef, dataContext) {
            return common.formatValue(value);
        };

        // define a custom formatter for the status column
        var statusFormatter = function (row, cell, value, columnDef, dataContext) {
            return formatNodeStatus(dataContext);
        };

        var columnModel = [
            {
                id: 'moreDetails',
                name: '&nbsp;',
                sortable: false,
                resizable: false,
                formatter: moreDetailsFormatter,
                width: 50,
                maxWidth: 50
            },
            {
                id: 'formattedNodeAddress',
                field: 'formattedNodeAddress',
                name: 'Node Address',
                formatter: nodeFormatter,
                resizable: true,
                sortable: true
            },
            {
                id: 'activeThreadCount',
                field: 'activeThreadCount',
                name: 'Active Thread Count',
                resizable: true,
                sortable: true,
                defaultSortAsc: false
            },
            {
                id: 'queued',
                field: 'queued',
                name: '<span class="queued-title">Queue</span>&nbsp;/&nbsp;<span class="queued-size-title">Size</span>',
                resizable: true,
                sortable: true,
                defaultSortAsc: false
            },
            {
                id: 'status',
                field: 'status',
                name: 'Status',
                formatter: statusFormatter,
                resizable: true,
                sortable: true
            },
            {
                id: 'uptime',
                field: 'nodeStartTime',
                name: 'Uptime',
                formatter: valueFormatter,
                resizable: true,
                sortable: true,
                defaultSortAsc: false
            },
            {
                id: 'heartbeat',
                field: 'heartbeat',
                name: 'Last Heartbeat',
                formatter: valueFormatter,
                resizable: true,
                sortable: true,
                defaultSortAsc: false
            }
        ];

        // only allow the admin to modify the cluster
        if (common.canModifyController()) {
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
                    return '<div title="Connect" class="pointer prompt-for-connect fa fa-plug" style="margin-top: 2px;"></div><div title="Delete" class="pointer prompt-for-removal fa fa-trash" style="margin-top: 2px;"></div>';
                } else if (canDisconnect) {
                    return '<div title="Disconnect" class="pointer prompt-for-disconnect fa fa-power-off" style="margin-top: 2px;"></div>';
                } else {
                    return '<div style="width: 16px; height: 16px;">&nbsp;</div>';
                }
            };

            columnModel.push({
                id: 'actions',
                label: '&nbsp;',
                formatter: actionFormatter,
                resizable: false,
                sortable: false,
                width: 80,
                maxWidth: 80
            });
        }

        return columnModel;
    }

    var prevColumn, count;

    /**
     * Sorts the specified data using the specified sort details.
     *
     * @param {object} sortDetails
     * @param {object} data
     */
    function sort(sortDetails, dataView, tab) {
        // defines a function for sorting
        var comparer = function (a, b) {
            if (sortDetails.columnId === 'heartbeat' || sortDetails.columnId === 'uptime') {
                var aDate = common.parseDateTime(a[sortDetails.columnId]);
                var bDate = common.parseDateTime(b[sortDetails.columnId]);
                return aDate.getTime() - bDate.getTime();
            } else if (sortDetails.columnId === 'queued') {
                var aSplit = a[sortDetails.columnId].split(/ \/ /);
                var bSplit = b[sortDetails.columnId].split(/ \/ /);
                var mod = count % 4;
                if (mod < 2) {
                    $('#cluster-nodes-table span.queued-title').addClass('sorted');
                    var aCount = common.parseCount(aSplit[0]);
                    var bCount = common.parseCount(bSplit[0]);
                    return aCount - bCount;
                } else {
                    $('#cluster-nodes-table span.queued-size-title').addClass('sorted');
                    var aSize = common.parseSize(aSplit[1]);
                    var bSize = common.parseSize(bSplit[1]);
                    return aSize - bSize;
                }
            } else if (sortDetails.columnId === 'maxHeap' || sortDetails.columnId === 'totalHeap' || sortDetails.columnId === 'usedHeap'
                || sortDetails.columnId === 'totalNonHeap' || sortDetails.columnId === 'usedNonHeap'
                || sortDetails.columnId === 'ffRepoTotal' || sortDetails.columnId === 'ffRepoUsed'
                || sortDetails.columnId === 'ffRepoFree' || sortDetails.columnId === 'contentRepoTotal'
                || sortDetails.columnId === 'contentRepoUsed' || sortDetails.columnId === 'contentRepoFree') {
                var aSize = common.parseSize(a[sortDetails.columnId]);
                var bSize = common.parseSize(b[sortDetails.columnId]);
                return aSize - bSize;
            } else if (sortDetails.columnId === 'totalThreads' || sortDetails.columnId === 'daemonThreads'
                || sortDetails.columnId === 'processors') {
                var aCount = common.parseCount(a[sortDetails.columnId]);
                var bCount = common.parseCount(b[sortDetails.columnId]);
                return aCount - bCount;
            } else if (sortDetails.columnId === 'gcOldGen' || sortDetails.columnId === 'gcNewGen') {
                var aSplit = a[sortDetails.columnId].split(/ /);
                var bSplit = b[sortDetails.columnId].split(/ /);
                var aCount = common.parseCount(aSplit[0]);
                var bCount = common.parseCount(bSplit[0]);
                return aCount - bCount;
            } else if (sortDetails.columnId === 'status') {
                var aStatus = formatNodeStatus(a);
                var bStatus = formatNodeStatus(b);
                return aStatus === bStatus ? 0 : aStatus > bStatus ? 1 : -1;
            } else if (sortDetails.columnId === 'formattedNodeAddress') {
                var aNode = formatNodeAddress(a);
                var bNode = formatNodeAddress(b);
                return aNode === bNode ? 0 : aNode > bNode ? 1 : -1;
            } else {
                var aString = common.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
                var bString = common.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
                return aString === bString ? 0 : aString > bString ? 1 : -1;
            }
        };

        // remove previous sort indicators
        $('#cluster-nodes-table span.queued-title').removeClass('sorted');
        $('#cluster-nodes-table span.queued-size-title').removeClass('sorted');

        // update/reset the count as appropriate
        if (prevColumn !== sortDetails.columnId) {
            count = 0;
        } else {
            count++;
        }

        // perform the sort
        dataView.sort(comparer, sortDetails.sortAsc);

        // record the previous table and sorted column
        prevColumn = sortDetails.columnId;
    };

    /**
     * Formats the address for the specified node.
     *
     * @param {object} node
     * @returns {string}
     */
    var formatNodeAddress = function (node) {
        return common.escapeHtml(node.address) + ':' + common.escapeHtml(node.apiPort);
    };

    /**
     * Formats the status for the specified node.
     *
     * @param {object} node
     * @returns {string}
     */
    var formatNodeStatus = function (node) {
        var markup = node.status;
        if (node.roles.includes(config.primaryNode)) {
            markup += ', PRIMARY';
        }
        if (node.roles.includes(config.clusterCoordinator)) {
            markup += ', COORDINATOR';
        }
        return markup;
    }

    /**
     * Prompts to verify node connection.
     *
     * @argument {object} node     The node
     */
    var promptForConnect = function (node) {
        // prompt to connect
        dialog.showYesNoDialog({
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
            var clusterGrid = $('#cluster-nodes-table').data('gridInstance');
            var clusterData = clusterGrid.getData();
            clusterData.updateItem(node.nodeId, node);
        }).fail(errorHandler.handleAjaxError);
    };

    /**
     * Prompts to verify node disconnection.
     *
     * @argument {object} node     The node
     */
    var promptForDisconnect = function (node) {
        // prompt for disconnect
        dialog.showYesNoDialog({
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
            var clusterGrid = $('#cluster-nodes-table').data('gridInstance');
            var clusterData = clusterGrid.getData();
            clusterData.updateItem(node.nodeId, node);
        }).fail(errorHandler.handleAjaxError);
    };

    /**
     * Prompts to verify node disconnection.
     *
     * @argument {object} node     The node
     */
    var promptForRemoval = function (node) {
        // prompt for disconnect
        dialog.showYesNoDialog({
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
            var clusterGrid = $('#cluster-nodes-table').data('gridInstance');
            var clusterData = clusterGrid.getData();
            clusterData.deleteItem(nodeId);
        }).fail(errorHandler.handleAjaxError);
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
        var visibleTab = getSelectedTab();
        if (!visibleTab) {
            return;
        }

        var grid = visibleTab.grid;

        // ensure the grid has been initialized
        if (common.isDefinedAndNotNull(grid)) {
            var gridData = grid.getData();

            // update the search criteria
            gridData.setFilterArgs({
                searchString: getFilterText(),
                property: $('#cluster-filter-type').combo('getSelectedOption').value
            });
            gridData.refresh();
        }
    };

    var getSelectedTab = function () {
        var selectedTab = null;
        clusterTabs.forEach(function (tab) {
            if ($('#' + tab.tableId).is(':visible')) {
                selectedTab = tab;
            }
        });
        return selectedTab;
    }

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

        var searchText = item[args.property];
        if (args.property === 'address') {
            searchText = formatNodeAddress(item);
        } else if (args.property === 'status') {
            searchText = formatNodeStatus(item);
        }

        // perform the filter
        return searchText.search(filterExp) >= 0;
    };

    /**
     * Updates count of displayed and total rows.
     */
    function updateFilterStats(selectedTab) {
        if (!selectedTab) {
            selectedTab = getSelectedTab();
        }
        if (selectedTab.dataView) {
            var displayedRows = selectedTab.dataView.getLength();
            var totalRows = selectedTab.rowCount;
            $('#displayed-rows').text(displayedRows);
            $('#total-rows').text(totalRows);
        }
    }

    /**
     * Clears any existing table filter.
     */
    var clearFilter = function () {
        $('#cluster-filter').val('');
        applyFilter();
    }

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
                $('<div></div>').append(common.formatUnorderedList(eventMessages)).appendTo(events);
            } else {
                events.append('<div><span class="unset">None</span></div>');
            }

            // show the dialog
            $('#node-details-dialog').modal('show');
        }).fail(errorHandler.handleAjaxError);
    };

    /**
     * Applies system diagnostics data to the JVM tab.
     */
    function updateJvmTableData(systemDiagnosticsResponse) {
        if (common.isDefinedAndNotNull(systemDiagnosticsResponse.systemDiagnostics)
            && common.isDefinedAndNotNull(systemDiagnosticsResponse.systemDiagnostics.nodeSnapshots)) {

            var jvmTableRows = [];
            systemDiagnosticsResponse.systemDiagnostics.nodeSnapshots.forEach(function (nodeSnapshot) {
                var snapshot = nodeSnapshot.snapshot;

                // sort the garbage collection
                var garbageCollection = snapshot.garbageCollection.sort(function (a, b) {
                    return a.name === b.name ? 0 : a.name > b.name ? 1 : -1;
                });

                // add the node jvm details
                jvmTableRows.push({
                    id: nodeSnapshot.nodeId,
                    node: nodeSnapshot.address + ':' + nodeSnapshot.apiPort,
                    address: nodeSnapshot.address,
                    maxHeap: snapshot.maxHeap,
                    totalHeap: snapshot.totalHeap,
                    usedHeap: snapshot.usedHeap,
                    heapUtilization: snapshot.heapUtilization,
                    maxNonHeap: snapshot.maxNonHeap,
                    totalNonHeap: snapshot.totalNonHeap,
                    usedNonHeap: snapshot.usedNonHeap,
                    gcOldGen: garbageCollection[0].collectionCount + ' times (' +
                    garbageCollection[0].collectionTime + ')',
                    gcNewGen: garbageCollection[1].collectionCount + ' times (' +
                    garbageCollection[1].collectionTime + ')'
                });
            });
            jvmTab.rowCount = jvmTableRows.length;
            jvmTab.dataView.setItems(jvmTableRows);
            jvmTab.dataView.reSort();
            jvmTab.grid.invalidate();
        } else {
            jvmTab.rowCount = 0;
        }
    }

    /**
     * Applies system diagnostics data to the System tab.
     */
    function updateSystemTableData(systemDiagnosticsResponse) {
        if (common.isDefinedAndNotNull(systemDiagnosticsResponse.systemDiagnostics)
            && common.isDefinedAndNotNull(systemDiagnosticsResponse.systemDiagnostics.nodeSnapshots)) {

            var systemTableRows = [];
            systemDiagnosticsResponse.systemDiagnostics.nodeSnapshots.forEach(function (nodeSnapshot) {
                var snapshot = nodeSnapshot.snapshot;
                systemTableRows.push({
                    id: nodeSnapshot.nodeId,
                    node: nodeSnapshot.address + ':' + nodeSnapshot.apiPort,
                    address: nodeSnapshot.address,
                    processors: snapshot.availableProcessors,
                    processorLoadAverage: snapshot.processorLoadAverage,
                    totalThreads: snapshot.totalThreads,
                    daemonThreads: snapshot.daemonThreads
                });
            });
            systemTab.rowCount = systemTableRows.length;
            systemTab.dataView.setItems(systemTableRows);
            systemTab.dataView.reSort();
            systemTab.grid.invalidate();
        } else {
            systemTab.rowCount = 0;
        }
    }

    /**
     * Applies system diagnostics data to the FlowFile Storage tab.
     */
    function updateFlowFileTableData(systemDiagnosticsResponse) {
        if (common.isDefinedAndNotNull(systemDiagnosticsResponse.systemDiagnostics)
            && common.isDefinedAndNotNull(systemDiagnosticsResponse.systemDiagnostics.nodeSnapshots)) {

            var flowFileTableRows = [];
            systemDiagnosticsResponse.systemDiagnostics.nodeSnapshots.forEach(function (nodeSnapshot) {
                var snapshot = nodeSnapshot.snapshot;
                flowFileTableRows.push({
                    id: nodeSnapshot.nodeId,
                    node: nodeSnapshot.address + ':' + nodeSnapshot.apiPort,
                    address: nodeSnapshot.address,
                    ffRepoTotal: snapshot.flowFileRepositoryStorageUsage.totalSpace,
                    ffRepoUsed: snapshot.flowFileRepositoryStorageUsage.usedSpace,
                    ffRepoFree: snapshot.flowFileRepositoryStorageUsage.freeSpace,
                    ffRepoUtil: snapshot.flowFileRepositoryStorageUsage.utilization
                });
            });
            flowFileTab.rowCount = flowFileTableRows.length;
            flowFileTab.dataView.setItems(flowFileTableRows);
            flowFileTab.dataView.reSort();
            flowFileTab.grid.invalidate();
        } else {
            flowFileTab.rowCount = 0;
        }
    }

    /**
     * Applies system diagnostics data to the Content Storage tab.
     */
    function updateContentTableData(systemDiagnosticsResponse) {
        if (common.isDefinedAndNotNull(systemDiagnosticsResponse.systemDiagnostics)
            && common.isDefinedAndNotNull(systemDiagnosticsResponse.systemDiagnostics.nodeSnapshots)) {

            var contentStorageTableRows = [];
            systemDiagnosticsResponse.systemDiagnostics.nodeSnapshots.forEach(function (nodeSnapshot) {
                var snapshot = nodeSnapshot.snapshot;
                snapshot.contentRepositoryStorageUsage.forEach(function (contentRepoUsage) {
                    contentStorageTableRows.push({
                        id: nodeSnapshot.nodeId + ':' + contentRepoUsage.identifier,
                        address: nodeSnapshot.address,
                        node: nodeSnapshot.address + ':' + nodeSnapshot.apiPort,
                        contentRepoId: contentRepoUsage.identifier,
                        contentRepoTotal: contentRepoUsage.totalSpace,
                        contentRepoUsed: contentRepoUsage.usedSpace,
                        contentRepoFree: contentRepoUsage.freeSpace,
                        contentRepoUtil: contentRepoUsage.utilization
                    });
                });
            });

            contentTab.rowCount = contentStorageTableRows.length;
            contentTab.dataView.setItems(contentStorageTableRows);
            contentTab.dataView.reSort();
            contentTab.grid.invalidate();
        } else {
            contentTab.rowCount = 0;
        }
    }

    /**
     * Applies system diagnostics data to the Versions tab.
     */
    function updateVersionTableData(systemDiagnosticsResponse) {
        if (common.isDefinedAndNotNull(systemDiagnosticsResponse.systemDiagnostics)
            && common.isDefinedAndNotNull(systemDiagnosticsResponse.systemDiagnostics.nodeSnapshots)) {

            var versionTableRows = [];
            systemDiagnosticsResponse.systemDiagnostics.nodeSnapshots.forEach(function (nodeSnapshot) {
                var snapshot = nodeSnapshot.snapshot;
                versionTableRows.push({
                    id: nodeSnapshot.nodeId,
                    address: nodeSnapshot.address,
                    node: nodeSnapshot.address + ':' + nodeSnapshot.apiPort,
                    version: snapshot.versionInfo.niFiVersion,
                    javaVendor: snapshot.versionInfo.javaVendor,
                    javaVersion: snapshot.versionInfo.javaVersion,
                    osName: snapshot.versionInfo.osName,
                    osVersion: snapshot.versionInfo.osVersion,
                    osArchitecture: snapshot.versionInfo.osArchitecture
                });
            });

            versionTab.dataView.setItems(versionTableRows);
            versionTab.dataView.reSort();
            versionTab.grid.invalidate();
        }
    }

    /**
     * Loads system diagnostics data for the cluster.
     */
    function refreshSystemDiagnosticsData() {
        var systemDiagnosticsUri = config.urls.systemDiagnostics
        var loadPromise = $.ajax({
            type: 'GET',
            url: systemDiagnosticsUri,
            data: {
                nodewise: true
            },
            dataType: 'json'
        }).done(function (systemDiagnosticsResponse) {
            var handlers = dataSetHandlers['systemDiagnostics'];
            handlers.forEach(function (handler) {
                handler(systemDiagnosticsResponse);
            });
        }).fail(errorHandler.handleAjaxError);
        return loadPromise;
    };

    /**
     * Generic initialization for Slick Grid tables
     */
    function commonTableInit(tab) {
        var dataView = new Slick.Data.DataView({
            inlineFilters: false
        });
        dataView.setItems([], tab.tableIdColumn);

        dataView.setFilterArgs({
            searchString: getFilterText(),
            property: $('#cluster-filter-type').combo('getSelectedOption').value
        });
        dataView.setFilter(filter);

        // initialize the sort
        tab.onSort({
            columnId: tab.tableIdColumn,
            sortAsc: true
        }, dataView);

        // initialize the grid
        var columnModel = tab.tableColumnModel;
        if (typeof columnModel === 'function') {
            columnModel = columnModel();
        }
        var grid = new Slick.Grid('#' + tab.tableId, dataView, columnModel, tab.tableOptions);
        grid.setSelectionModel(new Slick.RowSelectionModel());
        grid.setSortColumn(tab.tableIdColumn, true);
        grid.onSort.subscribe(function (e, args) {
            tab.onSort({
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, dataView, tab);
        });

        // wire up the dataview to the grid
        dataView.onRowCountChanged.subscribe(function (e, args) {
            grid.updateRowCount();
            grid.render();
            updateFilterStats(tab);
        });
        dataView.onRowsChanged.subscribe(function (e, args) {
            grid.invalidateRows(args.rows);
            grid.render();
        });

        // click events
        if (tab.tableOnClick) {
            grid.onClick.subscribe(function (e, args) {
                var target = $(e.target);
                var item = dataView.getItem(args.row);
                tab.tableOnClick(e, args, target, item);
            });
        }

        // hold onto an instance of the grid
        $('#' + tab.tableId).data('gridInstance', grid);
        tab.dataView = dataView;
        tab.grid = grid;
    };

    /**
     * Apply the cluster nodes data set to the table.
     */
    function updateNodesTableData(clusterResponse) {
        var cluster = clusterResponse.cluster;

        // ensure there are groups specified
        if (common.isDefinedAndNotNull(cluster.nodes)) {
            var clusterGrid = nodesTab.grid;
            var clusterData = clusterGrid.getData();

            // set the items
            nodesTab.rowCount = cluster.nodes.length;
            clusterData.setItems(cluster.nodes);
            clusterData.reSort();
            clusterGrid.invalidate();

            // update the stats last refreshed timestamp
            $('#cluster-last-refreshed').text(cluster.generated);
        } else {
            $('#total-nodes').text('0');
        }
    }

    /**
     * Refreshes cluster data sets from the server.
     */
    function refreshClusterData() {
        var clusterNodesDataPromise = $.ajax({
            type: 'GET',
            url: config.urls.cluster,
            dataType: 'json'
        }).done(function (response) {
            var handlers = dataSetHandlers['cluster'];
            handlers.forEach(function (handler) {
                handler(response);
            });
        }).fail(errorHandler.handleAjaxError);
        return clusterNodesDataPromise;
    }

    /**
     * Event handler triggered when the user switches tabs.
     */
    function onSelectTab(tab) {
        // Resize table
        var tabGrid = tab.grid;
        if (common.isDefinedAndNotNull(tabGrid)) {
            tabGrid.resizeCanvas();
        }

        // Clear filter text
        clearFilter();

        // Reset filter options
        $('#cluster-filter-type').combo({
            options: tab.filterOptions,
            select: function (option) {
                applyFilter();
            }
        });

        updateFilterStats(tab);
    }

    var nfClusterTable = {
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

            // Authorize data sets
            var dataSetAuthorized = {};
            config.data = config.data.filter(function (dataSetConfig) {
                dataSetConfig.authorized = dataSetConfig.isAuthorized();
                dataSetAuthorized[dataSetConfig.name] = dataSetConfig.authorized;
                if (dataSetConfig.authorized) {
                    return true;
                } else {
                    return false;
                }
            });

            // Filter tabs to authorized data sets
            clusterTabs = clusterTabs.filter(function (tab) {
                var tabDataSet = tab.data.dataSet;
                if (dataSetAuthorized[tabDataSet]) {
                    return true;
                } else {
                    return false;
                }
            });
            clusterTabs.forEach(function (tab) {
                tabsByName[tab.name] = tab;
                var dataSetHandlerList = dataSetHandlers[tab.data.dataSet];
                if (dataSetHandlerList) {
                    dataSetHandlers[tab.data.dataSet] = dataSetHandlerList.concat([tab.data.update]);
                } else {
                    dataSetHandlers[tab.data.dataSet] = [tab.data.update];
                }
            });

            // Initialize tab set
            $('#cluster-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                scrollableTabContentStyle: 'scrollable',
                tabs: clusterTabs,
                select: function () {
                    var tab = $(this).text();
                    var selectedTab = tabsByName[tab];
                    if (selectedTab) {
                        selectedTab.onTabSelected(selectedTab);
                    } else {
                        console.error('Failed to match tab: ', tab, tabsByName);
                    }
                }
            });

            // listen for browser resize events to update the page size
            $(window).resize(function () {
                nfClusterTable.resetTableSize();
            });

            // initialize tabs
            clusterTabs.forEach(function (tab) {
                try {
                    tab.init(tab);
                } catch (ex) {
                    console.error('Failed to initialize tab', tab, ex);
                }
            });
        },

        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            clusterTabs.forEach(function (tab) {
                if (tab && tab.grid) {
                    tab.grid.resizeCanvas();
                }
            });
        },

        /**
         * Load the processor cluster table.
         */
        loadClusterTable: function () {
            var updateDataDeferreds = config.data.map(function (dataSetSpec) {
                var dataSetDeferred = dataSetSpec.update();
                return dataSetDeferred;
            });
            var aggregateDeferred = $.when.apply($, updateDataDeferreds);
            aggregateDeferred = aggregateDeferred.done(function (aggregateResult) {
                updateFilterStats(nodesTab);
            });
            return aggregateDeferred;
        }
    };

    return nfClusterTable;
}));