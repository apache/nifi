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

/* global top, define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'Slick',
                'nf.Common',
                'nf.Dialog',
                'nf.ErrorHandler',
                'nf.HistoryModel'],
            function ($, Slick, nfCommon, nfDialog, nfErrorHandler, nfHistoryModel) {
                return (nf.HistoryTable = factory($, Slick, nfCommon, nfDialog, nfErrorHandler, nfHistoryModel));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.HistoryTable =
            factory(require('jquery'),
                require('Slick'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.ErrorHandler'),
                require('nf.HistoryModel')));
    } else {
        nf.HistoryTable = factory(root.$,
            root.Slick,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.ErrorHandler,
            root.nf.HistoryModel);
    }
}(this, function ($, Slick, nfCommon, nfDialog, nfErrorHandler, nfHistoryModel) {
    'use strict';

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        defaultStartTime: '00:00:00',
        defaultEndTime: '23:59:59',
        styles: {
            hidden: 'hidden'
        },
        urls: {
            history: '../nifi-api/controller/history'
        }
    };

    /**
     * Initializes the details dialog.
     */
    var initDetailsDialog = function () {
        $('#action-details-dialog').modal({
            scrollableContentStyle: 'scrollable',
            headerText: 'Action Details',
            buttons: [{
                buttonText: 'Ok',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        $('#action-details-dialog').modal('hide');
                    }
                }
            }],
            handler: {
                close: function () {
                    // clear the details
                    $('#action-details').empty();
                }
            }
        });
    };

    /**
     * Initializes the filter dialog.
     */
    var initFilterDialog = function () {
        // clear the filter field
        $('#history-filter').val('');

        // filter type
        $('#history-filter-type').combo({
            options: [{
                text: 'by id',
                value: 'by id',
                description: 'Filters based on the id of the component that was modified'
            }, {
                text: 'by user',
                value: 'by user',
                description: 'Filters based on the user that performed the action'
            }]
        });

        // configure the start and end date picker
        $('#history-filter-start-date, #history-filter-end-date').datepicker({
            showAnim: '',
            showOtherMonths: true,
            selectOtherMonths: true
        });
        $('#history-filter-start-date').datepicker('setDate', '-14d');
        $('#history-filter-end-date').datepicker('setDate', '+0d');

        // initialize the start and end time
        $('#history-filter-start-time').val(config.defaultStartTime);
        $('#history-filter-end-time').val(config.defaultEndTime);

        // configure the filter dialog
        $('#history-filter-dialog').modal({
            scrollableContentStyle: 'scrollable',
            headerText: 'Filter History',
            buttons: [{
                buttonText: 'Filter',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        $('#history-filter-dialog').modal('hide');

                        var filter = {};

                        // extract the filter text
                        var filterText = $('#history-filter').val();
                        if (filterText !== '') {
                            var filterType = $('#history-filter-type').combo('getSelectedOption').text;
                            if (filterType === 'by id') {
                                filter['sourceId'] = filterText;
                            } else if (filterType === 'by user') {
                                filter['userIdentity'] = filterText;
                            }
                        }

                        // extract the start date time
                        var startDate = $.trim($('#history-filter-start-date').val());
                        var startTime = $.trim($('#history-filter-start-time').val());
                        if (startDate !== '') {
                            if (startTime === '') {
                                startTime = config.defaultStartTime;
                                $('#history-filter-start-time').val(startTime);
                            }
                            filter['startDate'] = startDate + ' ' + startTime;
                        }

                        // extract the end date time
                        var endDate = $.trim($('#history-filter-end-date').val());
                        var endTime = $.trim($('#history-filter-end-time').val());
                        if (endDate !== '') {
                            if (endTime === '') {
                                endTime = config.defaultEndTime;
                                $('#history-filter-end-time').val(endTime);
                            }
                            filter['endDate'] = endDate + ' ' + endTime;
                        }

                        // set the filter
                        var historyGrid = $('#history-table').data('gridInstance');
                        var historyModel = historyGrid.getData();
                        historyModel.setFilterArgs(filter);

                        // reload the table
                        nfHistoryTable.loadHistoryTable();
                    }
                }
            },
                {
                    buttonText: 'Cancel',
                    color: {
                        base: '#E3E8EB',
                        hover: '#C7D2D7',
                        text: '#004849'
                    },
                    handler: {
                        click: function () {
                            $('#history-filter-dialog').modal('hide');
                        }
                    }
                }]
        });
    };

    /**
     * Initializes the purge dialog.
     */
    var initPurgeDialog = function () {
        // configure the start and end date picker
        $('#history-purge-end-date').datepicker({
            showAnim: '',
            showOtherMonths: true,
            selectOtherMonths: true
        });
        $('#history-purge-end-date').datepicker('setDate', '-1m');

        // initialize the start and end time
        $('#history-purge-end-time').val(config.defaultStartTime);

        // configure the filter dialog
        $('#history-purge-dialog').modal({
            scrollableContentStyle: 'scrollable',
            headerText: 'Purge History',
            buttons: [{
                buttonText: 'Purge',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        // hide the dialog
                        $('#history-purge-dialog').modal('hide');

                        // get the purge end date
                        var endDate = $.trim($('#history-purge-end-date').val());
                        var endTime = $.trim($('#history-purge-end-time').val());
                        if (endDate !== '') {
                            if (endTime === '') {
                                endTime = config.defaultStartTime;
                                $('#history-purge-end-time').val(endTime);
                            }
                            var endDateTime = endDate + ' ' + endTime;
                            var timezone = $('.timezone:first').text();
                            nfDialog.showYesNoDialog({
                                headerText: 'History',
                                dialogContent: "Are you sure you want to delete all history before '" + nfCommon.escapeHtml(endDateTime) + " " + nfCommon.escapeHtml(timezone) + "'?",
                                yesHandler: function () {
                                    purgeHistory(endDateTime);
                                }
                            });
                        } else {
                            nfDialog.showOkDialog({
                                headerText: 'History',
                                dialogContent: 'The end date must be specified.'
                            });
                        }
                    }
                }
            },
                {
                    buttonText: 'Cancel',
                    color: {
                        base: '#E3E8EB',
                        hover: '#C7D2D7',
                        text: '#004849'
                    },
                    handler: {
                        click: function () {
                            $('#history-purge-dialog').modal('hide');
                        }
                    }
                }]
        });
    };

    /**
     * Initializes the history table.
     */
    var initHistoryTable = function () {
        // clear the current filter
        $('#clear-history-filter').click(function () {
            // clear the filter dialog
            $('#history-filter').val('');

            // hide the overview
            $('#history-filter-overview').css('visibility', 'hidden');

            // clear the filter
            var historyGrid = $('#history-table').data('gridInstance');
            var historyModel = historyGrid.getData();
            historyModel.setFilterArgs({});

            // refresh the table
            nfHistoryTable.loadHistoryTable();
        });

        // add hover effect and click handler for opening the dialog
        $('#history-filter-button').click(function () {
            $('#history-filter-dialog').modal('show');
        });

        // define a custom formatter for the more details column
        var moreDetailsFormatter = function (row, cell, value, columnDef, dataContext) {
            if (dataContext.canRead === true) {
                return '<div title="View Details" class="pointer show-action-details fa fa-info-circle"></div>';
            }
            return "";
        };

        // define how general values are formatted
        var valueFormatter = function (row, cell, value, columnDef, dataContext) {
            if (dataContext.canRead !== true) {
                return '<span class="unset" style="font-size: 13px; padding-top: 2px;">Not authorized</span>';
            }
            return nfCommon.formatValue(dataContext.action[columnDef.field]);
        };

        // initialize the templates table
        var historyColumns = [
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
                id: 'timestamp',
                name: 'Date/Time',
                field: 'timestamp',
                sortable: true,
                resizable: true,
                formatter: valueFormatter
            },
            {
                id: 'sourceName',
                name: 'Name',
                field: 'sourceName',
                sortable: true,
                resizable: true,
                formatter: valueFormatter
            },
            {
                id: 'sourceType',
                name: 'Type',
                field: 'sourceType',
                sortable: true,
                resizable: true,
                formatter: valueFormatter
            },
            {
                id: 'operation',
                name: 'Operation',
                field: 'operation',
                sortable: true,
                resizable: true,
                formatter: valueFormatter
            },
            {
                id: 'userIdentity',
                name: 'User',
                field: 'userIdentity',
                sortable: true,
                resizable: true,
                formatter: valueFormatter
            }
        ];

        var historyOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: false,
            enableColumnReorder: false,
            autoEdit: false,
            rowHeight: 24
        };

        // create the remote model
        var historyModel = new nfHistoryModel();

        // initialize the grid
        var historyGrid = new Slick.Grid('#history-table', historyModel, historyColumns, historyOptions);
        historyGrid.setSelectionModel(new Slick.RowSelectionModel());
        historyGrid.registerPlugin(new Slick.AutoTooltips());

        // initialize the grid sorting
        historyGrid.onSort.subscribe(function (e, args) {
            // set the sort criteria on the model
            historyModel.setSort(args.sortCol.field, args.sortAsc ? 1 : -1);

            // reload the grid
            var vp = historyGrid.getViewport();
            historyModel.ensureData(vp.top, vp.bottom);
        });
        historyGrid.setSortColumn('timestamp', false);

        // configure a click listener
        historyGrid.onClick.subscribe(function (e, args) {
            var target = $(e.target);

            // get the node at this row
            var item = historyModel.getItem(args.row);

            // determine the desired action
            if (historyGrid.getColumns()[args.cell].id === 'moreDetails') {
                if (target.hasClass('show-action-details')) {
                    showActionDetails(item.action);
                }
            }
        });

        // listen for when the viewport changes so we can fetch the appropriate records
        historyGrid.onViewportChanged.subscribe(function (e, args) {
            var vp = historyGrid.getViewport();
            historyModel.ensureData(vp.top, vp.bottom);
        });

        // listen for when new data has been loaded
        historyModel.onDataLoaded.subscribe(function (e, args) {
            for (var i = args.from; i <= args.to; i++) {
                historyGrid.invalidateRow(i);
            }
            historyGrid.updateRowCount();
            historyGrid.render();
        });

        // hold onto an instance of the grid
        $('#history-table').data('gridInstance', historyGrid);

        // add the purge button if appropriate
        if (nfCommon.canModifyController()) {
            $('#history-purge-button').on('click', function () {
                $('#history-purge-dialog').modal('show');
            }).show();
        }
    };

    /**
     * Purges the history up to the specified end date.
     *
     * @argument {string} endDateTime       The end date time
     */
    var purgeHistory = function (endDateTime) {
        $.ajax({
            type: 'DELETE',
            url: config.urls.history + '?' + $.param({
                endDate: endDateTime
            }),
            dataType: 'json'
        }).done(function () {
            nfHistoryTable.loadHistoryTable();
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Shows the details for the specified action.
     *
     * @param {object} action
     */
    var showActionDetails = function (action) {
        // create the markup for the dialog
        var detailsMarkup = $('<div></div>').append(
            $('<div class="action-detail"><div class="history-details-name">Id</div>' + nfCommon.escapeHtml(action.sourceId) + '</div>'));

        // get any component details
        var componentDetails = action.componentDetails;

        // inspect the operation to determine if there are any component details
        if (nfCommon.isDefinedAndNotNull(componentDetails)) {
            if (action.sourceType === 'Processor' || action.sourceType === 'ControllerService' || action.sourceType === 'ReportingTask') {
                detailsMarkup.append(
                    $('<div class="action-detail"><div class="history-details-name">Type</div>' + nfCommon.escapeHtml(componentDetails.type) + '</div>'));
            } else if (action.sourceType === 'RemoteProcessGroup') {
                detailsMarkup.append(
                    $('<div class="action-detail"><div class="history-details-name">Uri</div>' + nfCommon.formatValue(componentDetails.uri) + '</div>'));
            }
        }

        // get any action details
        var actionDetails = action.actionDetails;

        // inspect the operation to determine if there are any action details
        if (nfCommon.isDefinedAndNotNull(actionDetails)) {
            if (action.operation === 'Configure') {
                detailsMarkup.append(
                    $('<div class="action-detail"><div class="history-details-name">Name</div>' + nfCommon.formatValue(actionDetails.name) + '</div>')).append(
                    $('<div class="action-detail"><div class="history-details-name">Value</div>' + nfCommon.formatValue(actionDetails.value) + '</div>')).append(
                    $('<div class="action-detail"><div class="history-details-name">Previous Value</div>' + nfCommon.formatValue(actionDetails.previousValue) + '</div>'));
            } else if (action.operation === 'Connect' || action.operation === 'Disconnect') {
                detailsMarkup.append(
                    $('<div class="action-detail"><div class="history-details-name">Source Id</div>' + nfCommon.escapeHtml(actionDetails.sourceId) + '</div>')).append(
                    $('<div class="action-detail"><div class="history-details-name">Source Name</div>' + nfCommon.formatValue(actionDetails.sourceName) + '</div>')).append(
                    $('<div class="action-detail"><div class="history-details-name">Source Type</div>' + nfCommon.escapeHtml(actionDetails.sourceType) + '</div>')).append(
                    $('<div class="action-detail"><div class="history-details-name">Relationship(s)</div>' + nfCommon.formatValue(actionDetails.relationship) + '</div>')).append(
                    $('<div class="action-detail"><div class="history-details-name">Destination Id</div>' + nfCommon.escapeHtml(actionDetails.destinationId) + '</div>')).append(
                    $('<div class="action-detail"><div class="history-details-name">Destination Name</div>' + nfCommon.formatValue(actionDetails.destinationName) + '</div>')).append(
                    $('<div class="action-detail"><div class="history-details-name">Destination Type</div>' + nfCommon.escapeHtml(actionDetails.destinationType) + '</div>'));
            } else if (action.operation === 'Move') {
                detailsMarkup.append(
                    $('<div class="action-detail"><div class="history-details-name">Group</div>' + nfCommon.formatValue(actionDetails.group) + '</div>')).append(
                    $('<div class="action-detail"><div class="history-details-name">Group Id</div>' + nfCommon.escapeHtml(actionDetails.groupId) + '</div>')).append(
                    $('<div class="action-detail"><div class="history-details-name">Previous Group</div>' + nfCommon.formatValue(actionDetails.previousGroup) + '</div>')).append(
                    $('<div class="action-detail"><div class="history-details-name">Previous Group Id</div>' + nfCommon.escapeHtml(actionDetails.previousGroupId) + '</div>'));
            } else if (action.operation === 'Purge') {
                detailsMarkup.append(
                    $('<div class="action-detail"><div class="history-details-name">End Date</div>' + nfCommon.escapeHtml(actionDetails.endDate) + '</div>'));
            }
        }

        // populate the dialog
        $('#action-details').append(detailsMarkup);

        // show the dialog
        $('#action-details-dialog').modal('show');
    };

    var nfHistoryTable = {
        init: function () {
            initDetailsDialog();
            initFilterDialog();
            initPurgeDialog();
            initHistoryTable();
        },

        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            var historyGrid = $('#history-table').data('gridInstance');
            if (nfCommon.isDefinedAndNotNull(historyGrid)) {
                historyGrid.resizeCanvas();
            }
        },

        /**
         * Load the processor status table.
         */
        loadHistoryTable: function () {
            var historyGrid = $('#history-table').data('gridInstance');

            // clear the history model
            var historyModel = historyGrid.getData();
            historyModel.clear();

            // request refresh of the current 'page'
            historyGrid.onViewportChanged.notify();
        }
    };

    return nfHistoryTable;
}));
