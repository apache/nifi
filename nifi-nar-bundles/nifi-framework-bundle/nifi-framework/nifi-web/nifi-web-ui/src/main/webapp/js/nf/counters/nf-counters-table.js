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
                'nf.ErrorHandler'],
            function ($, Slick, nfCommon, nfErrorHandler) {
                return (nf.CountersTable = factory($, Slick, nfCommon, nfErrorHandler));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.CountersTable =
            factory(require('jquery'),
                require('Slick'),
                require('nf.Common'),
                require('nf.ErrorHandler')));
    } else {
        nf.CountersTable = factory(root.$,
            root.Slick,
            root.nf.Common,
            root.nf.ErrorHandler);
    }
}(this, function ($, Slick, nfCommon, nfErrorHandler) {
    'use strict';

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        urls: {
            counters: '../nifi-api/counters'
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
            if (sortDetails.columnId === 'value') {
                var aCount = nfCommon.parseCount(a[sortDetails.columnId]);
                var bCount = nfCommon.parseCount(b[sortDetails.columnId]);
                return aCount - bCount;
            } else {
                var aString = nfCommon.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
                var bString = nfCommon.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
                return aString === bString ? 0 : aString > bString ? 1 : -1;
            }
        };

        // perform the sort
        data.sort(comparer, sortDetails.sortAsc);
    };

    /**
     * Get the text out of the filter field. If the filter field doesn't
     * have any text it will contain the text 'filter list' so this method
     * accounts for that.
     */
    var getFilterText = function () {
        return $('#counters-filter').val();
    };

    /**
     * Applies the filter found in the filter expression text field.
     */
    var applyFilter = function () {
        // get the dataview
        var countersGrid = $('#counters-table').data('gridInstance');

        // ensure the grid has been initialized
        if (nfCommon.isDefinedAndNotNull(countersGrid)) {
            var countersData = countersGrid.getData();

            // update the search criteria
            countersData.setFilterArgs({
                searchString: getFilterText(),
                property: $('#counters-filter-type').combo('getSelectedOption').value
            });
            countersData.refresh();
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
     * Resets the specified counter.
     *
     * @argument {object} item     The counter item
     */
    var resetCounter = function (item) {
        $.ajax({
            type: 'PUT',
            url: config.urls.counters + '/' + encodeURIComponent(item.id),
            dataType: 'json'
        }).done(function (response) {
            var counter = response.counter;

            // get the table and update the row accordingly
            var countersGrid = $('#counters-table').data('gridInstance');
            var countersData = countersGrid.getData();
            countersData.updateItem(counter.id, counter);
        }).fail(nfErrorHandler.handleAjaxError);
    };

    return {
        /**
         * Initializes the counters list.
         */
        init: function () {
            // define the function for filtering the list
            $('#counters-filter').keyup(function () {
                applyFilter();
            });

            // filter type
            $('#counters-filter-type').combo({
                options: [{
                    text: 'by name',
                    value: 'name'
                }, {
                    text: 'by context',
                    value: 'context'
                }],
                select: function (option) {
                    applyFilter();
                }
            });

            // initialize the templates table
            var countersColumns = [
                {
                    id: 'context',
                    name: 'Context',
                    field: 'context',
                    sortable: true,
                    resizable: true,
                    formatter: nfCommon.genericValueFormatter
                },
                {
                    id: 'name',
                    name: 'Name',
                    field: 'name',
                    sortable: true,
                    resizable: true,
                    formatter: nfCommon.genericValueFormatter
                },
                {
                    id: 'value',
                    name: 'Value',
                    field: 'value',
                    sortable: true,
                    resizable: true,
                    defaultSortAsc: false,
                    formatter: nfCommon.genericValueFormatter
                }
            ];

            // only allow dfm's to reset counters
            if (nfCommon.canModifyCounters()) {
                // function for formatting the actions column
                var actionFormatter = function (row, cell, value, columnDef, dataContext) {
                    return '<div title="Reset Counter" class="pointer reset-counter fa fa-undo" style="margin-top: 2px;"></div>';
                };

                // add the action column
                countersColumns.push({
                    id: 'actions',
                    name: '&nbsp;',
                    sortable: false,
                    resizable: false,
                    formatter: actionFormatter,
                    width: 100,
                    maxWidth: 100
                });
            }

            var countersOptions = {
                forceFitColumns: true,
                enableTextSelectionOnCells: true,
                enableCellNavigation: false,
                enableColumnReorder: false,
                autoEdit: false,
                rowHeight: 24
            };

            // initialize the dataview
            var countersData = new Slick.Data.DataView({
                inlineFilters: false
            });
            countersData.setItems([]);
            countersData.setFilterArgs({
                searchString: getFilterText(),
                property: $('#counters-filter-type').combo('getSelectedOption').value
            });
            countersData.setFilter(filter);

            // initialize the sort
            sort({
                columnId: 'context',
                sortAsc: true
            }, countersData);

            // initialize the grid
            var countersGrid = new Slick.Grid('#counters-table', countersData, countersColumns, countersOptions);
            countersGrid.setSelectionModel(new Slick.RowSelectionModel());
            countersGrid.registerPlugin(new Slick.AutoTooltips());
            countersGrid.setSortColumn('context', true);
            countersGrid.onSort.subscribe(function (e, args) {
                sort({
                    columnId: args.sortCol.field,
                    sortAsc: args.sortAsc
                }, countersData);
            });

            // configure a click listener
            countersGrid.onClick.subscribe(function (e, args) {
                var target = $(e.target);

                // get the node at this row
                var item = countersData.getItem(args.row);

                // determine the desired action
                if (countersGrid.getColumns()[args.cell].id === 'actions') {
                    if (target.hasClass('reset-counter')) {
                        resetCounter(item);
                    }
                }
            });

            // wire up the dataview to the grid
            countersData.onRowCountChanged.subscribe(function (e, args) {
                countersGrid.updateRowCount();
                countersGrid.render();

                // update the total number of displayed processors
                $('#displayed-counters').text(args.current);
            });
            countersData.onRowsChanged.subscribe(function (e, args) {
                countersGrid.invalidateRows(args.rows);
                countersGrid.render();
            });

            // hold onto an instance of the grid
            $('#counters-table').data('gridInstance', countersGrid);

            // initialize the number of display items
            $('#displayed-counters').text('0');
        },

        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            var countersGrid = $('#counters-table').data('gridInstance');
            if (nfCommon.isDefinedAndNotNull(countersGrid)) {
                countersGrid.resizeCanvas();
            }
        },

        /**
         * Load the processor counters table.
         */
        loadCountersTable: function () {
            return $.ajax({
                type: 'GET',
                url: config.urls.counters,
                dataType: 'json'
            }).done(function (response) {
                var report = response.counters;
                var aggregateSnapshot = report.aggregateSnapshot;

                // ensure there are groups specified
                if (nfCommon.isDefinedAndNotNull(aggregateSnapshot.counters)) {
                    var countersGrid = $('#counters-table').data('gridInstance');
                    var countersData = countersGrid.getData();

                    // set the items
                    countersData.setItems(aggregateSnapshot.counters);
                    countersData.reSort();
                    countersGrid.invalidate();

                    // update the stats last refreshed timestamp
                    $('#counters-last-refreshed').text(aggregateSnapshot.generated);

                    // update the total number of processors
                    $('#total-counters').text(aggregateSnapshot.counters.length);
                } else {
                    $('#total-counters').text('0');
                }
            }).fail(nfErrorHandler.handleAjaxError);
        }
    };
}));