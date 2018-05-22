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
            function ($, Slick, nfCommon, nfDialog, nfErrorHandler) {
                return (nf.TemplatesTable = factory($, Slick, nfCommon, nfDialog, nfErrorHandler));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.TemplatesTable =
            factory(require('jquery'),
                require('Slick'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.ErrorHandler')));
    } else {
        nf.TemplatesTable = factory(root.$,
            root.Slick,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.ErrorHandler);
    }
}(this, function ($, Slick, nfCommon, nfDialog, nfErrorHandler) {
    'use strict';

    var isDisconnectionAcknowledged = false;

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        urls: {
            templates: '../nifi-api/flow/templates',
            downloadToken: '../nifi-api/access/download-token'
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
            if (a.permissions.canRead && b.permissions.canRead) {
                if (sortDetails.columnId === 'timestamp') {
                    var aDate = nfCommon.parseDateTime(a.template[sortDetails.columnId]);
                    var bDate = nfCommon.parseDateTime(b.template[sortDetails.columnId]);
                    return aDate.getTime() - bDate.getTime();
                } else {
                    var aString = nfCommon.isDefinedAndNotNull(a.template[sortDetails.columnId]) ? a.template[sortDetails.columnId] : '';
                    var bString = nfCommon.isDefinedAndNotNull(b.template[sortDetails.columnId]) ? b.template[sortDetails.columnId] : '';
                    return aString === bString ? 0 : aString > bString ? 1 : -1;
                }
            } else {
                if (!a.permissions.canRead && !b.permissions.canRead) {
                    return 0;
                }
                if (a.permissions.canRead) {
                    return 1;
                } else {
                    return -1;
                }
            }
        };

        // perform the sort
        data.sort(comparer, sortDetails.sortAsc);
    };

    /**
     * Prompts the user before attempting to delete the specified template.
     *
     * @argument {object} templateEntity     The template
     */
    var promptToDeleteTemplate = function (templateEntity) {
        // prompt for deletion
        nfDialog.showYesNoDialog({
            headerText: 'Delete Template',
            dialogContent: 'Delete template \'' + nfCommon.escapeHtml(templateEntity.template.name) + '\'?',
            yesHandler: function () {
                deleteTemplate(templateEntity);
            }
        });
    };

    /**
     * Opens the access policies for the specified template.
     *
     * @param templateEntity
     */
    var openAccessPolicies = function (templateEntity) {
        // only attempt this if we're within a frame
        if (top !== window) {
            // and our parent has canvas utils and shell defined
            if (nfCommon.isDefinedAndNotNull(parent.nf) && nfCommon.isDefinedAndNotNull(parent.nf.PolicyManagement) && nfCommon.isDefinedAndNotNull(parent.nf.Shell)) {
                parent.nf.PolicyManagement.showTemplatePolicy(templateEntity);
                parent.$('#shell-close-button').click();
            }
        }
    };

    /**
     * Deletes the template with the specified id.
     *
     * @argument {string} templateEntity     The template
     */
    var deleteTemplate = function (templateEntity) {
        $.ajax({
            type: 'DELETE',
            url: templateEntity.template.uri + '?' + $.param({
                'disconnectedNodeAcknowledged': isDisconnectionAcknowledged
            }),
            dataType: 'json'
        }).done(function () {
            var templatesGrid = $('#templates-table').data('gridInstance');
            var templatesData = templatesGrid.getData();
            templatesData.deleteItem(templateEntity.id);

            // update the total number of templates
            $('#total-templates').text(templatesData.getItems().length);
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Get the text out of the filter field. If the filter field doesn't
     * have any text it will contain the text 'filter list' so this method
     * accounts for that.
     */
    var getFilterText = function () {
        return $('#templates-filter').val();
    };

    /**
     * Applies the filter found in the filter expression text field.
     */
    var applyFilter = function () {
        // get the dataview
        var templatesGrid = $('#templates-table').data('gridInstance');

        // ensure the grid has been initialized
        if (nfCommon.isDefinedAndNotNull(templatesGrid)) {
            var templatesData = templatesGrid.getData();

            // update the search criteria
            templatesData.setFilterArgs({
                searchString: getFilterText(),
                property: $('#templates-filter-type').combo('getSelectedOption').value
            });
            templatesData.refresh();
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
        return item.template[args.property].search(filterExp) >= 0;
    };

    /**
     * Downloads the specified template.
     *
     * @param {object} templateEntity     The template
     */
    var downloadTemplate = function (templateEntity) {
        nfCommon.getAccessToken(config.urls.downloadToken).done(function (downloadToken) {
            var parameters = {};

            // conditionally include the download token
            if (!nfCommon.isBlank(downloadToken)) {
                parameters['access_token'] = downloadToken;
            }

            // open the url
            if ($.isEmptyObject(parameters)) {
                window.open(templateEntity.template.uri + '/download');
            } else {
                window.open(templateEntity.template.uri + '/download' + '?' + $.param(parameters));
            }
        }).fail(function () {
            nfDialog.showOkDialog({
                headerText: 'Download Template',
                dialogContent: 'Unable to generate access token for downloading content.'
            });
        });
    };

    return {
        /**
         * Initializes the templates list.
         */
        init: function (disconnectionAcknowledged) {
            isDisconnectionAcknowledged = disconnectionAcknowledged;

            // define the function for filtering the list
            $('#templates-filter').keyup(function () {
                applyFilter();
            });

            // filter type
            $('#templates-filter-type').combo({
                options: [{
                    text: 'by name',
                    value: 'name'
                }, {
                    text: 'by description',
                    value: 'description'
                }],
                select: function (option) {
                    applyFilter();
                }
            });

            var timestampFormatter = function (row, cell, value, columnDef, dataContext) {
                if (!dataContext.permissions.canRead) {
                    return '';
                }

                return nfCommon.escapeHtml(dataContext.template.timestamp);
            };

            var nameFormatter = function (row, cell, value, columnDef, dataContext) {
                if (!dataContext.permissions.canRead) {
                    return '<span class="blank">' + nfCommon.escapeHtml(dataContext.id) + '</span>';
                }

                return nfCommon.escapeHtml(dataContext.template.name);
            };

            var descriptionFormatter = function (row, cell, value, columnDef, dataContext) {
                if (!dataContext.permissions.canRead) {
                    return '';
                }

                return nfCommon.formatValue(dataContext.template.description);
            };

            var groupIdFormatter = function (row, cell, value, columnDef, dataContext) {
                if (!dataContext.permissions.canRead) {
                    return '';
                }

                return nfCommon.escapeHtml(dataContext.template.groupId);
            };

            // function for formatting the actions column
            var actionFormatter = function (row, cell, value, columnDef, dataContext) {
                var markup = '';

                if (dataContext.permissions.canRead === true) {
                    markup += '<div title="Download" class="pointer export-template icon icon-template-save"></div>';
                }

                if (dataContext.permissions.canWrite === true) {
                    markup += '<div title="Remove Template" class="pointer prompt-to-delete-template fa fa-trash"></div>';
                }

                // allow policy configuration conditionally if framed
                if (top !== window && nfCommon.canAccessTenants()) {
                    if (nfCommon.isDefinedAndNotNull(parent.nf) && nfCommon.isDefinedAndNotNull(parent.nf.CanvasUtils) && parent.nf.CanvasUtils.isManagedAuthorizer()) {
                        markup += '<div title="Access Policies" class="pointer edit-access-policies fa fa-key"></div>';
                    }
                }

                return markup;
            };

            // initialize the templates table
            var templatesColumns = [
                {
                    id: 'timestamp',
                    name: 'Date/Time',
                    sortable: true,
                    defaultSortAsc: false,
                    resizable: false,
                    formatter: timestampFormatter,
                    width: 225,
                    maxWidth: 225
                },
                {
                    id: 'name',
                    name: 'Name',
                    sortable: true,
                    resizable: true,
                    formatter: nameFormatter
                },
                {
                    id: 'description',
                    name: 'Description',
                    sortable: true,
                    resizable: true,
                    formatter: descriptionFormatter
                },
                {
                    id: 'groupId',
                    name: 'Process Group Id',
                    sortable: true,
                    resizable: true,
                    formatter: groupIdFormatter
                },
                {
                    id: 'actions',
                    name: '&nbsp;',
                    sortable: false,
                    resizable: false,
                    formatter: actionFormatter,
                    width: 100,
                    maxWidth: 100
                }
            ];

            var templatesOptions = {
                forceFitColumns: true,
                enableTextSelectionOnCells: true,
                enableCellNavigation: false,
                enableColumnReorder: false,
                autoEdit: false,
                rowHeight: 24
            };

            // initialize the dataview
            var templatesData = new Slick.Data.DataView({
                inlineFilters: false
            });
            templatesData.setItems([]);
            templatesData.setFilterArgs({
                searchString: getFilterText(),
                property: $('#templates-filter-type').combo('getSelectedOption').value
            });
            templatesData.setFilter(filter);

            // initialize the sort
            sort({
                columnId: 'timestamp',
                sortAsc: false
            }, templatesData);

            // initialize the grid
            var templatesGrid = new Slick.Grid('#templates-table', templatesData, templatesColumns, templatesOptions);
            templatesGrid.setSelectionModel(new Slick.RowSelectionModel());
            templatesGrid.registerPlugin(new Slick.AutoTooltips());
            templatesGrid.setSortColumn('timestamp', false);
            templatesGrid.onSort.subscribe(function (e, args) {
                sort({
                    columnId: args.sortCol.id,
                    sortAsc: args.sortAsc
                }, templatesData);
            });

            // configure a click listener
            templatesGrid.onClick.subscribe(function (e, args) {
                var target = $(e.target);

                // get the node at this row
                var item = templatesData.getItem(args.row);

                // determine the desired action
                if (templatesGrid.getColumns()[args.cell].id === 'actions') {
                    if (target.hasClass('export-template')) {
                        downloadTemplate(item);
                    } else if (target.hasClass('prompt-to-delete-template')) {
                        promptToDeleteTemplate(item);
                    } else if (target.hasClass('edit-access-policies')) {
                        openAccessPolicies(item);
                    }
                }
            });

            // wire up the dataview to the grid
            templatesData.onRowCountChanged.subscribe(function (e, args) {
                templatesGrid.updateRowCount();
                templatesGrid.render();

                // update the total number of displayed processors
                $('#displayed-templates').text(args.current);
            });
            templatesData.onRowsChanged.subscribe(function (e, args) {
                templatesGrid.invalidateRows(args.rows);
                templatesGrid.render();
            });

            // hold onto an instance of the grid
            $('#templates-table').data('gridInstance', templatesGrid);

            // initialize the number of displayed items
            $('#displayed-templates').text('0');
        },

        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            var templateGrid = $('#templates-table').data('gridInstance');
            if (nfCommon.isDefinedAndNotNull(templateGrid)) {
                templateGrid.resizeCanvas();
            }
        },

        /**
         * Load the processor templates table.
         */
        loadTemplatesTable: function () {
            return $.ajax({
                type: 'GET',
                url: config.urls.templates,
                dataType: 'json'
            }).done(function (response) {
                // ensure there are groups specified
                if (nfCommon.isDefinedAndNotNull(response.templates)) {
                    var templatesGrid = $('#templates-table').data('gridInstance');
                    var templatesData = templatesGrid.getData();

                    // set the items
                    templatesData.setItems(response.templates);
                    templatesData.reSort();
                    templatesGrid.invalidate();

                    // update the stats last refreshed timestamp
                    $('#templates-last-refreshed').text(response.generated);

                    // update the total number of processors
                    $('#total-templates').text(response.templates.length);
                } else {
                    $('#total-templates').text('0');
                }
            }).fail(nfErrorHandler.handleAjaxError);
        }
    };
}));