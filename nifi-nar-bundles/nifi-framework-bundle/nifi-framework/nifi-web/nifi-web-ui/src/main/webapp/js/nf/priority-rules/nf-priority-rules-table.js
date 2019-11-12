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

(function (root, factory) {
   if(typeof define === 'function' && define.amd) {
       define(['jquery', 'Slick', 'nf.Common', 'nf.Dialog', 'nf.ErrorHandler'],
           function ($, Slick, nfCommon, nfDialog, nfErrorHandler) {
               return (nf.PriorityRulesTable = factory($, Slick, nfCommon, nfDialog, nfErrorHandler));
            });
   } else if (typeof exports === 'object' && typeof module === 'object') {
       module.exports = (nf.PriorityRulesTable =
           factory(require('jquery'),
               require('Slick'),
               require('nf.Common'),
               require('nf.Dialog'),
               require('nf.ErrorHandler') ) );
   } else {
       nf.PriorityRulesTable = factory(root.$,
           root.Slick,
           root.nf.Common,
           root.nf.Dialog,
           root.nf.ErrorHandler);
   }
}(this, function($, Slick, nfCommon, nfDialog, nfErrorHandler) {
    'use strict';

    var isDisconnectionAcknowledged = false;

    var config = {
        urls: {
            editRule: "../nifi-api/priority-rules/editRule",
            getPriorityRules: "../nifi-api/priority-rules/getRuleList"
        }
    };

    var sort = function(sortDetails, data) {
        var comparer = function(a, b) {
            var aString = nfCommon.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
            var bString = nfCommon.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
            return aString === bString ? 0 : aString > bString ? 1 : -1;
        }

        data.sort(comparer, sortDetails.sortAsc);
    };

    /*
     * Opens the access policies for the specified priorityRule
     *
     * @param priorityRuleEntity
     */
    var openAccessPolicies = function (priorityRuleEntity) {
        // only attempt this if we're within a frame
        if(top !== window) {
            // and our parent has canvas utils and shell defined
            if(nfCommon.isDefinedAndNotNull(parent.nf) && nfCommon.isDefinedAndNotNull(parent.nf.PolicyManagement) && nfCommon.isDefinedAndNotNull(parent.nf.Shell)) {
                parent.nf.PolicyManagement.showPriorityRulePolicy(priorityRuleEntity);
                parent.$('#shell-close-button').click();
            }
        }
    };

        /**
         * Applies the filter found in the filter expression text field.
         */
        var applyFilter = function () {
            // get the dataview
            var priorityRulesGrid = $('#priority-rules-table').data('gridInstance');

            // ensure the grid has been initialized
            if (nfCommon.isDefinedAndNotNull(priorityRulesGrid)) {
                var priorityRulesData = priorityRulesGrid.getData();

                // update the search criteria
                priorityRulesData.setFilterArgs({
                    searchString: getFilterText(),
                    property: $('#priority-rules-filter-type').combo('getSelectedOption').value
                });
                priorityRulesData.refresh();
                priorityRulesGrid.invalidate();
            }
        };

        /**
         * Get the text out of the filter field. If the filter field doesn't
         * have any text it will contain the text 'filter list' so this method
         * accounts for that.
         */
        var getFilterText = function () {
            return $('#priority-rules-filter').val();
        };

        /**
         * Performs the priorityRules filtering.
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

    // Load the processor PriorityRules table
    var loadPriorityRulesTable = function() {
        return $.ajax({
            type: 'GET',
            url: config.urls.getPriorityRules,
            dataType: 'json'
        }).done(function (response) {
            // ensure there are groups specified
            if(nfCommon.isDefinedAndNotNull(response.priorityRules)) {
                var priorityRulesGrid = $('#priority-rules-table').data('gridInstance');
                var priorityRulesData = priorityRulesGrid.getData();

                // set the items
                priorityRulesData.setItems(response.priorityRules);
                priorityRulesData.reSort();
                priorityRulesGrid.invalidate();

                // update the stats last refreshed timestamp
                $('#priorityRules-last-refreshed').text(response.generated);

                // update the total number of priority rules
                $('#total-priorityRules').text(response.priorityRules.length);
            } else {
                $('#total-priorityRules').text('0');
            }
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /*
     * Prompts the user before attempting to delete the specified priorityRule
     *
     * @argument {object} priorityRuleEntity The priorityRule
     */
    var promptToDeletePriorityRule = function(priorityRuleEntity) {
        // prompt for deletion
        nfDialog.showYesNoDialog({
            headerText: 'Delete PriorityRule',
            dialogContent: 'Delete priorityRule \'' + nfCommon.escapeHtml(priorityRuleEntity.label) + '(' + nfCommon.escapeHtml(priorityRuleEntity.id) + ')\'?',
            yesHandler: function() {
                deletePriorityRule(priorityRuleEntity);
            }
        });
    };

    /*
     * Deletes the priorityRule with the specified id
     *
     * @argument {string} priorityRuleEntity    The priorityRule
     */
     var deletePriorityRule = function(entity) {
         entity.expired = true;
         var priorityRuleEntity = {};
         priorityRuleEntity.priorityRule = entity;

         $.ajax({
            type: 'POST',
            url: config.urls.editRule,
            data: JSON.stringify(priorityRuleEntity),
            dataType: 'json',
            contentType: 'application/json'
         }).done(function () {
            loadPriorityRulesTable();
         }).fail(nfErrorHandler.handleAjaxError);
     };

     return {
         /**
          * Initializes the priorityRules list
          */

         init: function(disconnectionAcknowledged) {
            isDisconnectionAcknowledged = disconnectionAcknowledged;

            // define the function for filtering the list
            $('#priority-rules-filter').keyup(function() {
                applyFilter();
            });

            // filter type
            $('#priority-rules-filter-type').combo({
                options: [{
                    text: 'by label',
                    value: 'label'
                },{
                    text: 'by expression',
                    value: 'expression'
                }],
                select: function(option) {
                    applyFilter();
                }
            });

            var labelFormatter = function(row, cell, value, columnDef, dataContext) {
                return nfCommon.escapeHtml(dataContext.label);
            };

            var expressionFormatter = function(row, cell, value, columnDef, dataContext) {
                return nfCommon.escapeHtml(dataContext.expression);
            };

            var rateOfThreadUsageFormatter = function(row, cell, value, columnDef, dataContext) {
                return nfCommon.escapeHtml(dataContext.rateOfThreadUsage);
            };

            // function for formatting the actions column
            var actionFormatter = function(row, cell, value, columnDef, dataContext) {
                var markup = '';
                markup += '<div title="Edit PriorityRule" class="pointer edit-priorityRule fa fa-edit"></div>';
                markup += '<div title="Remove PriorityRule" class="pointer prompt-to-delete-priorityRule fa fa-trash"></div>';

                return markup;
            };

            var priorityRulesColumns = [
                {
                    id: 'label',
                    name: 'Label',
                    sortable: true,
                    resizable: true,
                    formatter: labelFormatter
                },{
                    id: 'expression',
                    name: 'Expression',
                    sortable: true,
                    resizable: true,
                    formatter: expressionFormatter
                },{
                    id: 'rateOfThreadUsage',
                    name: 'Priority Value (%)',
                    sortable: true,
                    resizable: true,
                    formatter: rateOfThreadUsageFormatter
                },{
                    id: 'actions',
                    name: '&nbsp;',
                    sortable: false,
                    resizable: false,
                    formatter: actionFormatter,
                    width: 100,
                    maxWidth: 100
                }
            ];

            var priorityRulesOptions = {
                forceFitColumns: true,
                enableTextSelectionOnCells: true,
                enableCellNavigation: false,
                enableColumnReorder: false,
                autoEdit: false,
                rowHeight: 24
            };

            var priorityRulesData = new Slick.Data.DataView({
                inlineFilters: false
            });
            priorityRulesData.setItems([]);
            priorityRulesData.setFilterArgs({
                searchString: '',
                property: 'label'
            });
            priorityRulesData.setFilter(filter);

            // initialize the sort
            sort({
                columnId: 'rateOfThreadUsage',
                sortAsc: false
            }, priorityRulesData);

            // initialize the grid
            var priorityRulesGrid = new Slick.Grid('#priority-rules-table', priorityRulesData, priorityRulesColumns, priorityRulesOptions);
            priorityRulesGrid.setSelectionModel(new Slick.RowSelectionModel());
            priorityRulesGrid.registerPlugin(new Slick.AutoTooltips());
            priorityRulesGrid.setSortColumn('rateOfThreadUsage', false);
            priorityRulesGrid.onSort.subscribe(function (e, args) {
                sort({
                    columnId: args.sortCol.id,
                    sortAsc: args.sortAsc
                }, priorityRulesData);
                this.invalidate();
            });

            priorityRulesGrid.onClick.subscribe(function(e, args) {
                var target = $(e.target);

                // get the node at this row
                var item = priorityRulesData.getItem(args.row);

                // determine the desired action
                if(priorityRulesGrid.getColumns()[args.cell].id === 'actions') {
                    if(target.hasClass('prompt-to-delete-priorityRule')) {
                        promptToDeletePriorityRule(item);
                    } else if(target.hasClass('edit-priorityRule')) {
                        $('#edit-priority-rule-uuid-field').val(item.id);
                        $('#edit-priority-rule-label-field').val(item.label);
                        $('#edit-priority-rule-expression-field').val(item.expression);
                        $('#edit-priority-rule-rate-field').val(item.rateOfThreadUsage);
                        $('#edit-priority-rule-dialog').modal('show');
                    }
                }
            });

            // wire up the dataview to the grid
            priorityRulesData.onRowCountChanged.subscribe(function(e, args) {
                priorityRulesGrid.updateRowCount();
                priorityRulesGrid.render();

                // update the total number of displayed processors
                $('#displayed-priority-rules').text(args.current);
            });
            priorityRulesData.onRowsChanged.subscribe(function(e, args) {
                priorityRulesGrid.invalidateRows(args.row);
                priorityRulesGrid.render();
            });

            // hold onto an instance of the grid
            $('#priority-rules-table').data('gridInstance', priorityRulesGrid);

            // initialize the number of displayed items
            $('#displayed-priority-rules').text('0');
         },

         /**
          * Update the size of the grid based on its container's current size
          */
          resetTableSize: function() {
            var priorityRulesGrid = $('#priority-rules-table').data('gridInstance');
            if(nfCommon.isDefinedAndNotNull(priorityRulesGrid)) {
                priorityRulesGrid.resizeCanvas();
            }
          },

          /**
           * Load the processor priorityRules table
           */
          loadPriorityRulesTable: function() {
            return $.ajax({
                type: 'GET',
                url: config.urls.getPriorityRules,
                dataType: 'json'
            }).done(function(response) {
                // Ensure there are groups specified
                if(nfCommon.isDefinedAndNotNull(response.priorityRules)) {
                    var priorityRulesGrid = $('#priority-rules-table').data('gridInstance');
                    var priorityRulesData = priorityRulesGrid.getData();

                    // set the items
                    priorityRulesData.setItems(response.priorityRules);
                    priorityRulesData.reSort();
                    priorityRulesGrid.invalidate();

                    // update the stats last refreshed timestamp
                    $('#priorityRules-last-refreshed').text(response.generated);

                    // update the total number of processors
                    $('#total-priority-rules').text(response.priorityRules.length);
                } else {
                    $('#total-priority-rules').text('0');
                }
            }).fail(nfErrorHandler.handleAjaxError);
          }
     };
}));