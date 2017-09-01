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
                'd3',
                'nf.Client',
                'nf.Dialog',
                'nf.Common',
                'nf.CanvasUtils',
                'nf.ControllerServices',
                'nf.ErrorHandler',
                'nf.ReportingTask',
                'nf.Shell',
                'nf.ComponentState',
                'nf.ComponentVersion',
                'nf.PolicyManagement'],
            function ($, Slick, d3, nfClient, nfDialog, nfCommon, nfCanvasUtils, nfControllerServices, nfErrorHandler, nfReportingTask, nfShell, nfComponentState, nfComponentVersion, nfPolicyManagement) {
                return (nf.Settings = factory($, Slick, d3, nfClient, nfDialog, nfCommon, nfCanvasUtils, nfControllerServices, nfErrorHandler, nfReportingTask, nfShell, nfComponentState, nfComponentVersion, nfPolicyManagement));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Settings =
            factory(require('jquery'),
                require('Slick'),
                require('d3'),
                require('nf.Client'),
                require('nf.Dialog'),
                require('nf.Common'),
                require('nf.CanvasUtils'),
                require('nf.ControllerServices'),
                require('nf.ErrorHandler'),
                require('nf.ReportingTask'),
                require('nf.Shell'),
                require('nf.ComponentState'),
                require('nf.ComponentVersion'),
                require('nf.PolicyManagement')));
    } else {
        nf.Settings = factory(root.$,
            root.Slick,
            root.d3,
            root.nf.Client,
            root.nf.Dialog,
            root.nf.Common,
            root.nf.CanvasUtils,
            root.nf.ControllerServices,
            root.nf.ErrorHandler,
            root.nf.ReportingTask,
            root.nf.Shell,
            root.nf.ComponentState,
            root.nf.ComponentVersion,
            root.nf.PolicyManagement);
    }
}(this, function ($, Slick, d3, nfClient, nfDialog, nfCommon, nfCanvasUtils, nfControllerServices, nfErrorHandler, nfReportingTask, nfShell, nfComponentState, nfComponentVersion, nfPolicyManagement) {
    'use strict';


    var config = {
        urls: {
            api: '../nifi-api',
            controllerConfig: '../nifi-api/controller/config',
            reportingTaskTypes: '../nifi-api/flow/reporting-task-types',
            createReportingTask: '../nifi-api/controller/reporting-tasks',
            reportingTasks: '../nifi-api/flow/reporting-tasks'
        }
    };

    var gridOptions = {
        forceFitColumns: true,
        enableTextSelectionOnCells: true,
        enableCellNavigation: true,
        enableColumnReorder: false,
        autoEdit: false,
        multiSelect: false,
        rowHeight: 24
    };

    /**
     * Gets the controller services table.
     *
     * @returns {*|jQuery|HTMLElement}
     */
    var getControllerServicesTable = function () {
        return $('#controller-services-table');
    };

    /**
     * Saves the settings for the controller.
     *
     * @param version
     */
    var saveSettings = function (version) {
        // marshal the configuration details
        var configuration = marshalConfiguration();
        var entity = {
            'revision': nfClient.getRevision({
                'revision': {
                    'version': version
                }
            }),
            'component': configuration
        };

        // save the new configuration details
        $.ajax({
            type: 'PUT',
            url: config.urls.controllerConfig,
            data: JSON.stringify(entity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (response) {
            // close the settings dialog
            nfDialog.showOkDialog({
                headerText: 'Settings',
                dialogContent: 'Settings successfully applied.'
            });

            // register the click listener for the save button
            $('#settings-save').off('click').on('click', function () {
                saveSettings(response.revision.version);
            });
        }).fail(nfErrorHandler.handleAjaxError);
    }

    /**
     * Initializes the general tab.
     */
    var initGeneral = function () {
    };

    /**
     * Marshals the details to include in the configuration request.
     */
    var marshalConfiguration = function () {
        // create the configuration
        var configuration = {};
        configuration['maxTimerDrivenThreadCount'] = $('#maximum-timer-driven-thread-count-field').val();
        configuration['maxEventDrivenThreadCount'] = $('#maximum-event-driven-thread-count-field').val();
        return configuration;
    };

    /**
     * Determines if the item matches the filter.
     *
     * @param {object} item     The item to filter
     * @param {object} args     The filter criteria
     * @returns {boolean}       Whether the item matches the filter
     */
    var matchesRegex = function (item, args) {
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

        // determine if the item matches the filter
        var matchesLabel = item['label'].search(filterExp) >= 0;
        var matchesTags = item['tags'].search(filterExp) >= 0;
        return matchesLabel || matchesTags;
    };

    /**
     * Determines if the specified tags match all the tags selected by the user.
     *
     * @argument {string[]} tagFilters      The tag filters
     * @argument {string} tags              The tags to test
     */
    var matchesSelectedTags = function (tagFilters, tags) {
        var selectedTags = [];
        $.each(tagFilters, function (_, filter) {
            selectedTags.push(filter);
        });

        // normalize the tags
        var normalizedTags = tags.toLowerCase();

        var matches = true;
        $.each(selectedTags, function (i, selectedTag) {
            if (normalizedTags.indexOf(selectedTag) === -1) {
                matches = false;
                return false;
            }
        });

        return matches;
    };

    /**
     * Whether the specified item is selectable.
     *
     * @param item reporting task type
     */
    var isSelectable = function (item) {
        return nfCommon.isBlank(item.usageRestriction) || nfCommon.canAccessRestrictedComponents();
    };

    /**
     * Formatter for the name column.
     *
     * @param {type} row
     * @param {type} cell
     * @param {type} value
     * @param {type} columnDef
     * @param {type} dataContext
     * @returns {String}
     */
    var nameFormatter = function (row, cell, value, columnDef, dataContext) {
        if (!dataContext.permissions.canRead) {
            return '<span class="blank">' + nfCommon.escapeHtml(dataContext.id) + '</span>';
        }

        return nfCommon.escapeHtml(dataContext.component.name);
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
                if (sortDetails.columnId === 'moreDetails') {
                    var aBulletins = 0;
                    if (!nfCommon.isEmpty(a.bulletins)) {
                        aBulletins = a.bulletins.length;
                    }
                    var bBulletins = 0;
                    if (!nfCommon.isEmpty(b.bulletins)) {
                        bBulletins = b.bulletins.length;
                    }
                    return aBulletins - bBulletins;
                } else if (sortDetails.columnId === 'type') {
                    var aType = nfCommon.isDefinedAndNotNull(a.component[sortDetails.columnId]) ? nfCommon.substringAfterLast(a.component[sortDetails.columnId], '.') : '';
                    var bType = nfCommon.isDefinedAndNotNull(b.component[sortDetails.columnId]) ? nfCommon.substringAfterLast(b.component[sortDetails.columnId], '.') : '';
                    return aType === bType ? 0 : aType > bType ? 1 : -1;
                } else if (sortDetails.columnId === 'state') {
                    var aState = 'Invalid';
                    if (nfCommon.isEmpty(a.component.validationErrors)) {
                        aState = nfCommon.isDefinedAndNotNull(a.component[sortDetails.columnId]) ? a.component[sortDetails.columnId] : '';
                    }
                    var bState = 'Invalid';
                    if (nfCommon.isEmpty(b.component.validationErrors)) {
                        bState = nfCommon.isDefinedAndNotNull(b.component[sortDetails.columnId]) ? b.component[sortDetails.columnId] : '';
                    }
                    return aState === bState ? 0 : aState > bState ? 1 : -1;
                } else {
                    var aString = nfCommon.isDefinedAndNotNull(a.component[sortDetails.columnId]) ? a.component[sortDetails.columnId] : '';
                    var bString = nfCommon.isDefinedAndNotNull(b.component[sortDetails.columnId]) ? b.component[sortDetails.columnId] : '';
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
     * Get the text out of the filter field. If the filter field doesn't
     * have any text it will contain the text 'filter list' so this method
     * accounts for that.
     */
    var getReportingTaskTypeFilterText = function () {
        return $('#reporting-task-type-filter').val();
    };

    /**
     * Filters the reporting task type table.
     */
    var applyReportingTaskTypeFilter = function () {
        // get the dataview
        var reportingTaskTypesGrid = $('#reporting-task-types-table').data('gridInstance');

        // ensure the grid has been initialized
        if (nfCommon.isDefinedAndNotNull(reportingTaskTypesGrid)) {
            var reportingTaskTypesData = reportingTaskTypesGrid.getData();

            // update the search criteria
            reportingTaskTypesData.setFilterArgs({
                searchString: getReportingTaskTypeFilterText()
            });
            reportingTaskTypesData.refresh();

            // update the buttons to possibly trigger the disabled state
            $('#new-reporting-task-dialog').modal('refreshButtons');

            // update the selection if possible
            if (reportingTaskTypesData.getLength() > 0) {
                reportingTaskTypesGrid.setSelectedRows([0]);
            }
        }
    };

    /**
     * Hides the selected reporting task.
     */
    var clearSelectedReportingTask = function () {
        $('#reporting-task-type-description').attr('title', '').text('');
        $('#reporting-task-type-name').attr('title', '').text('');
        $('#reporting-task-type-bundle').attr('title', '').text('');
        $('#selected-reporting-task-name').text('');
        $('#selected-reporting-task-type').text('').removeData('bundle');
        $('#reporting-task-description-container').hide();
    };

    /**
     * Clears the selected reporting task type.
     */
    var clearReportingTaskSelection = function () {
        // clear the selected row
        clearSelectedReportingTask();

        // clear the active cell the it can be reselected when its included
        var reportingTaskTypesGrid = $('#reporting-task-types-table').data('gridInstance');
        reportingTaskTypesGrid.resetActiveCell();
    };

    /**
     * Performs the filtering.
     *
     * @param {object} item     The item subject to filtering
     * @param {object} args     Filter arguments
     * @returns {Boolean}       Whether or not to include the item
     */
    var filterReportingTaskTypes = function (item, args) {
        // determine if the item matches the filter
        var matchesFilter = matchesRegex(item, args);

        // determine if the row matches the selected tags
        var matchesTags = true;
        if (matchesFilter) {
            var tagFilters = $('#reporting-task-tag-cloud').tagcloud('getSelectedTags');
            var hasSelectedTags = tagFilters.length > 0;
            if (hasSelectedTags) {
                matchesTags = matchesSelectedTags(tagFilters, item['tags']);
            }
        }

        // determine if the row matches the selected source group
        var matchesGroup = true;
        if (matchesFilter && matchesTags) {
            var bundleGroup = $('#reporting-task-bundle-group-combo').combo('getSelectedOption');
            if (nfCommon.isDefinedAndNotNull(bundleGroup) && bundleGroup.value !== '') {
                matchesGroup = (item.bundle.group === bundleGroup.value);
            }
        }

        // determine if this row should be visible
        var matches = matchesFilter && matchesTags && matchesGroup;

        // if this row is currently selected and its being filtered
        if (matches === false && $('#selected-reporting-task-type').text() === item['type']) {
            clearReportingTaskSelection();
        }

        return matches;
    };

    /**
     * Adds the currently selected reporting task.
     */
    var addSelectedReportingTask = function () {
        var selectedTaskType = $('#selected-reporting-task-type').text();
        var selectedTaskBundle = $('#selected-reporting-task-type').data('bundle');

        // ensure something was selected
        if (selectedTaskType === '') {
            nfDialog.showOkDialog({
                headerText: 'Settings',
                dialogContent: 'The type of reporting task to create must be selected.'
            });
        } else {
            addReportingTask(selectedTaskType, selectedTaskBundle);
        }
    };

    /**
     * Adds a new reporting task of the specified type.
     *
     * @param {string} reportingTaskType
     * @param {object} reportingTaskBundle
     */
    var addReportingTask = function (reportingTaskType, reportingTaskBundle) {
        // build the reporting task entity
        var reportingTaskEntity = {
            'revision': nfClient.getRevision({
                'revision': {
                    'version': 0
                }
            }),
            'component': {
                'type': reportingTaskType,
                'bundle': reportingTaskBundle
            }
        };

        // add the new reporting task
        var addTask = $.ajax({
            type: 'POST',
            url: config.urls.createReportingTask,
            data: JSON.stringify(reportingTaskEntity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (reportingTaskEntity) {
            // add the item
            var reportingTaskGrid = $('#reporting-tasks-table').data('gridInstance');
            var reportingTaskData = reportingTaskGrid.getData();
            reportingTaskData.addItem($.extend({
                type: 'ReportingTask',
                bulletins: []
            }, reportingTaskEntity));

            // resort
            reportingTaskData.reSort();
            reportingTaskGrid.invalidate();

            // select the new reporting task
            var row = reportingTaskData.getRowById(reportingTaskEntity.id);
            reportingTaskGrid.setSelectedRows([row]);
            reportingTaskGrid.scrollRowIntoView(row);
        }).fail(nfErrorHandler.handleAjaxError);

        // hide the dialog
        $('#new-reporting-task-dialog').modal('hide');

        return addTask;
    };

    /**
     * Initializes the new reporting task dialog.
     */
    var initNewReportingTaskDialog = function () {
        // define the function for filtering the list
        $('#reporting-task-type-filter').on('keyup', function (e) {
            var code = e.keyCode ? e.keyCode : e.which;
            if (code === $.ui.keyCode.ENTER) {
                // get the grid reference
                var grid = $('#reporting-task-types-table').data('gridInstance');
                var selected = grid.getSelectedRows();

                if (selected.length > 0) {
                    // grid configured with multi-select = false
                    var item = grid.getDataItem(selected[0]);
                    if (isSelectable(item)) {
                        addSelectedReportingTask();
                    }
                }
            } else {
                applyReportingTaskTypeFilter();
            }
        });

        // initialize the reporting task type table
        var reportingTaskTypesColumns = [
            {
                id: 'type',
                name: 'Type',
                field: 'label',
                formatter: nfCommon.typeFormatter,
                sortable: true,
                resizable: true
            },
            {
                id: 'version',
                name: 'Version',
                field: 'version',
                formatter: nfCommon.typeVersionFormatter,
                sortable: true,
                resizable: true
            },
            {
                id: 'tags',
                name: 'Tags',
                field: 'tags',
                sortable: true,
                resizable: true,
                formatter: nfCommon.genericValueFormatter
            }
        ];

        // initialize the dataview
        var reportingTaskTypesData = new Slick.Data.DataView({
            inlineFilters: false
        });
        reportingTaskTypesData.setItems([]);
        reportingTaskTypesData.setFilterArgs({
            searchString: getReportingTaskTypeFilterText()
        });
        reportingTaskTypesData.setFilter(filterReportingTaskTypes);

        // initialize the sort
        nfCommon.sortType({
            columnId: 'type',
            sortAsc: true
        }, reportingTaskTypesData);

        // initialize the grid
        var reportingTaskTypesGrid = new Slick.Grid('#reporting-task-types-table', reportingTaskTypesData, reportingTaskTypesColumns, gridOptions);
        reportingTaskTypesGrid.setSelectionModel(new Slick.RowSelectionModel());
        reportingTaskTypesGrid.registerPlugin(new Slick.AutoTooltips());
        reportingTaskTypesGrid.setSortColumn('type', true);
        reportingTaskTypesGrid.onSort.subscribe(function (e, args) {
            nfCommon.sortType({
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, reportingTaskTypesData);
        });
        reportingTaskTypesGrid.onSelectedRowsChanged.subscribe(function (e, args) {
            if ($.isArray(args.rows) && args.rows.length === 1) {
                var reportingTaskTypeIndex = args.rows[0];
                var reportingTaskType = reportingTaskTypesGrid.getDataItem(reportingTaskTypeIndex);

                // set the reporting task type description
                if (nfCommon.isDefinedAndNotNull(reportingTaskType)) {
                    // show the selected reporting task
                    $('#reporting-task-description-container').show();

                    if (nfCommon.isBlank(reportingTaskType.description)) {
                        $('#reporting-task-type-description')
                            .attr('title', '')
                            .html('<span class="unset">No description specified</span>');
                    } else {
                        $('#reporting-task-type-description')
                            .width($('#reporting-task-description-container').innerWidth() - 1)
                            .html(reportingTaskType.description)
                            .ellipsis();
                    }

                    var bundle = nfCommon.formatBundle(reportingTaskType.bundle);
                    var type = nfCommon.formatType(reportingTaskType);

                    // populate the dom
                    $('#reporting-task-type-name').text(type).attr('title', type);
                    $('#reporting-task-type-bundle').text(bundle).attr('title', bundle);
                    $('#selected-reporting-task-name').text(reportingTaskType.label);
                    $('#selected-reporting-task-type').text(reportingTaskType.type).data('bundle', reportingTaskType.bundle);

                    // refresh the buttons based on the current selection
                    $('#new-reporting-task-dialog').modal('refreshButtons');
                }
            }
        });
        reportingTaskTypesGrid.onDblClick.subscribe(function (e, args) {
            var reportingTaskType = reportingTaskTypesGrid.getDataItem(args.row);

            if (isSelectable(reportingTaskType)) {
                addReportingTask(reportingTaskType.type, reportingTaskType.bundle);
            }
        });
        reportingTaskTypesGrid.onViewportChanged.subscribe(function (e, args) {
            nfCommon.cleanUpTooltips($('#reporting-task-types-table'), 'div.view-usage-restriction');
        });

        // wire up the dataview to the grid
        reportingTaskTypesData.onRowCountChanged.subscribe(function (e, args) {
            reportingTaskTypesGrid.updateRowCount();
            reportingTaskTypesGrid.render();

            // update the total number of displayed processors
            $('#displayed-reporting-task-types').text(args.current);
        });
        reportingTaskTypesData.onRowsChanged.subscribe(function (e, args) {
            reportingTaskTypesGrid.invalidateRows(args.rows);
            reportingTaskTypesGrid.render();
        });
        reportingTaskTypesData.syncGridSelection(reportingTaskTypesGrid, true);

        // hold onto an instance of the grid
        $('#reporting-task-types-table').data('gridInstance', reportingTaskTypesGrid).on('mouseenter', 'div.slick-cell', function (e) {
            var usageRestriction = $(this).find('div.view-usage-restriction');
            if (usageRestriction.length && !usageRestriction.data('qtip')) {
                var rowId = $(this).find('span.row-id').text();

                // get the status item
                var item = reportingTaskTypesData.getItemById(rowId);

                // show the tooltip
                if (nfCommon.isDefinedAndNotNull(item.usageRestriction)) {
                    usageRestriction.qtip($.extend({}, nfCommon.config.tooltipConfig, {
                        content: item.usageRestriction,
                        position: {
                            container: $('#summary'),
                            at: 'bottom right',
                            my: 'top left',
                            adjust: {
                                x: 4,
                                y: 4
                            }
                        }
                    }));
                }
            }
        });

        // load the available reporting tasks
        $.ajax({
            type: 'GET',
            url: config.urls.reportingTaskTypes,
            dataType: 'json'
        }).done(function (response) {
            var id = 0;
            var tags = [];
            var groups = d3.set();

            // begin the update
            reportingTaskTypesData.beginUpdate();

            // go through each reporting task type
            $.each(response.reportingTaskTypes, function (i, documentedType) {
                // record the group
                groups.add(documentedType.bundle.group);

                // add the documented type
                reportingTaskTypesData.addItem({
                    id: id++,
                    label: nfCommon.substringAfterLast(documentedType.type, '.'),
                    type: documentedType.type,
                    bundle: documentedType.bundle,
                    description: nfCommon.escapeHtml(documentedType.description),
                    usageRestriction: nfCommon.escapeHtml(documentedType.usageRestriction),
                    tags: documentedType.tags.join(', ')
                });

                // count the frequency of each tag for this type
                $.each(documentedType.tags, function (i, tag) {
                    tags.push(tag.toLowerCase());
                });
            });

            // end the update
            reportingTaskTypesData.endUpdate();

            // resort
            reportingTaskTypesData.reSort();
            reportingTaskTypesGrid.invalidate();

            // set the total number of processors
            $('#total-reporting-task-types, #displayed-reporting-task-types').text(response.reportingTaskTypes.length);

            // create the tag cloud
            $('#reporting-task-tag-cloud').tagcloud({
                tags: tags,
                select: applyReportingTaskTypeFilter,
                remove: applyReportingTaskTypeFilter
            });

            // build the combo options
            var options = [{
                text: 'all groups',
                value: ''
            }];
            groups.forEach(function (group) {
                options.push({
                    text: group,
                    value: group
                });
            });

            // initialize the bundle group combo
            $('#reporting-task-bundle-group-combo').combo({
                options: options,
                select: applyReportingTaskTypeFilter
            });
        }).fail(nfErrorHandler.handleAjaxError);

        // initialize the reporting task dialog
        $('#new-reporting-task-dialog').modal({
            scrollableContentStyle: 'scrollable',
            headerText: 'Add Reporting Task',
            buttons: [{
                buttonText: 'Add',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                disabled: function () {
                    var selected = reportingTaskTypesGrid.getSelectedRows();

                    if (selected.length > 0) {
                        // grid configured with multi-select = false
                        var item = reportingTaskTypesGrid.getDataItem(selected[0]);
                        return isSelectable(item) === false;
                    } else {
                        return reportingTaskTypesGrid.getData().getLength() === 0;
                    }
                },
                handler: {
                    click: function () {
                        addSelectedReportingTask();
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
                            $(this).modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // clear the selected row
                    clearSelectedReportingTask();

                    // clear any filter strings
                    $('#reporting-task-type-filter').val('');

                    // clear the tagcloud
                    $('#reporting-task-tag-cloud').tagcloud('clearSelectedTags');

                    // reset the group combo
                    $('#reporting-task-bundle-group-combo').combo('setSelectedOption', {
                        value: ''
                    });

                    // reset the filter
                    applyReportingTaskTypeFilter();

                    // unselect any current selection
                    var reportingTaskTypesGrid = $('#reporting-task-types-table').data('gridInstance');
                    reportingTaskTypesGrid.setSelectedRows([]);
                    reportingTaskTypesGrid.resetActiveCell();
                },
                resize: function () {
                    $('#reporting-task-type-description')
                        .width($('#reporting-task-description-container').innerWidth() - 1)
                        .text($('#reporting-task-type-description').attr('title'))
                        .ellipsis();
                }
            }
        });
    };

    /**
     * Initializes the reporting tasks tab.
     */
    var initReportingTasks = function () {
        // initialize the new reporting task dialog
        initNewReportingTaskDialog();

        var moreReportingTaskDetails = function (row, cell, value, columnDef, dataContext) {
            if (!dataContext.permissions.canRead) {
                return '';
            }

            var markup = '<div title="View Details" class="pointer view-reporting-task fa fa-info-circle"></div>';

            // always include a button to view the usage
            markup += '<div title="Usage" class="pointer reporting-task-usage fa fa-book"></div>';

            var hasErrors = !nfCommon.isEmpty(dataContext.component.validationErrors);
            var hasBulletins = !nfCommon.isEmpty(dataContext.bulletins);

            if (hasErrors) {
                markup += '<div class="pointer has-errors fa fa-warning" ></div>';
            }

            if (hasBulletins) {
                markup += '<div class="has-bulletins fa fa-sticky-note-o"></div>';
            }

            if (hasErrors || hasBulletins) {
                markup += '<span class="hidden row-id">' + nfCommon.escapeHtml(dataContext.component.id) + '</span>';
            }

            return markup;
        };

        var reportingTaskRunStatusFormatter = function (row, cell, value, columnDef, dataContext) {
            if (!dataContext.permissions.canRead) {
                return '';
            }

            // determine the appropriate label
            var icon = '', label = '';
            if (!nfCommon.isEmpty(dataContext.component.validationErrors)) {
                label = 'Invalid';
                icon = 'invalid fa fa-warning';
            } else {
                if (dataContext.component.state === 'STOPPED') {
                    label = 'Stopped';
                    icon = 'fa fa-stop stopped';
                } else if (dataContext.component.state === 'RUNNING') {
                    label = 'Running';
                    icon = 'fa fa-play running';
                } else {
                    label = 'Disabled';
                    icon = 'icon icon-enable-false disabled';
                }
            }

            // include the active thread count if appropriate
            var activeThreadCount = '';
            if (nfCommon.isDefinedAndNotNull(dataContext.component.activeThreadCount) && dataContext.component.activeThreadCount > 0) {
                activeThreadCount = '(' + dataContext.component.activeThreadCount + ')';
            }

            // format the markup
            var formattedValue = '<div layout="row"><div class="' + icon + '" style="margin-top: 3px;"></div>';
            return formattedValue + '<div class="status-text" style="margin-top: 4px;">' + nfCommon.escapeHtml(label) + '</div><div style="float: left; margin-left: 4px;">' + nfCommon.escapeHtml(activeThreadCount) + '</div></div>';
        };

        var reportingTaskActionFormatter = function (row, cell, value, columnDef, dataContext) {
            var markup = '';

            if (dataContext.permissions.canRead && dataContext.permissions.canWrite) {
                if (dataContext.component.state === 'RUNNING') {
                    markup += '<div title="Stop" class="pointer stop-reporting-task fa fa-stop" style="margin-top: 2px; margin-right: 3px;" ></div>';
                } else if (dataContext.component.state === 'STOPPED' || dataContext.component.state === 'DISABLED') {
                    markup += '<div title="Edit" class="pointer edit-reporting-task fa fa-pencil" style="margin-top: 2px; margin-right: 3px;" ></div>';

                    // support starting when stopped and no validation errors
                    if (dataContext.component.state === 'STOPPED' && nfCommon.isEmpty(dataContext.component.validationErrors)) {
                        markup += '<div title="Start" class="pointer start-reporting-task fa fa-play" style="margin-top: 2px; margin-right: 3px;"></div>';
                    }

                    if (dataContext.component.multipleVersionsAvailable === true) {
                        markup += '<div title="Change Version" class="pointer change-version-reporting-task fa fa-exchange" style="margin-top: 2px; margin-right: 3px;" ></div>';
                    }

                    if (nfCommon.canModifyController()) {
                        markup += '<div title="Remove" class="pointer delete-reporting-task fa fa-trash" style="margin-top: 2px; margin-right: 3px;" ></div>';
                    }
                }

                if (dataContext.component.persistsState === true) {
                    markup += '<div title="View State" class="pointer view-state-reporting-task fa fa-tasks" style="margin-top: 2px; margin-right: 3px;" ></div>';
                }
            }

            // allow policy configuration conditionally
            if (nfCanvasUtils.isManagedAuthorizer() && nfCommon.canAccessTenants()) {
                markup += '<div title="Access Policies" class="pointer edit-access-policies fa fa-key" style="margin-top: 2px;"></div>';
            }

            return markup;
        };

        // define the column model for the reporting tasks table
        var reportingTasksColumnModel = [
            {
                id: 'moreDetails',
                name: '&nbsp;',
                resizable: false,
                formatter: moreReportingTaskDetails,
                sortable: true,
                width: 90,
                maxWidth: 90,
                toolTip: 'Sorts based on presence of bulletins'
            },
            {
                id: 'name',
                name: 'Name',
                sortable: true,
                resizable: true,
                formatter: nameFormatter
            },
            {
                id: 'type',
                name: 'Type',
                formatter: nfCommon.instanceTypeFormatter,
                sortable: true,
                resizable: true
            },
            {
                id: 'bundle',
                name: 'Bundle',
                formatter: nfCommon.instanceBundleFormatter,
                sortable: true,
                resizable: true
            },
            {
                id: 'state',
                name: 'Run Status',
                sortable: true,
                resizeable: true,
                formatter: reportingTaskRunStatusFormatter
            }
        ];

        // action column should always be last
        reportingTasksColumnModel.push({
            id: 'actions',
            name: '&nbsp;',
            resizable: false,
            formatter: reportingTaskActionFormatter,
            sortable: false,
            width: 90,
            maxWidth: 90
        });

        // initialize the dataview
        var reportingTasksData = new Slick.Data.DataView({
            inlineFilters: false
        });
        reportingTasksData.setItems([]);

        // initialize the sort
        sort({
            columnId: 'name',
            sortAsc: true
        }, reportingTasksData);

        // initialize the grid
        var reportingTasksGrid = new Slick.Grid('#reporting-tasks-table', reportingTasksData, reportingTasksColumnModel, gridOptions);
        reportingTasksGrid.setSelectionModel(new Slick.RowSelectionModel());
        reportingTasksGrid.registerPlugin(new Slick.AutoTooltips());
        reportingTasksGrid.setSortColumn('name', true);
        reportingTasksGrid.onSort.subscribe(function (e, args) {
            sort({
                columnId: args.sortCol.id,
                sortAsc: args.sortAsc
            }, reportingTasksData);
        });

        // configure a click listener
        reportingTasksGrid.onClick.subscribe(function (e, args) {
            var target = $(e.target);

            // get the service at this row
            var reportingTaskEntity = reportingTasksData.getItem(args.row);

            // determine the desired action
            if (reportingTasksGrid.getColumns()[args.cell].id === 'actions') {
                if (target.hasClass('edit-reporting-task')) {
                    nfReportingTask.showConfiguration(reportingTaskEntity);
                } else if (target.hasClass('start-reporting-task')) {
                    nfReportingTask.start(reportingTaskEntity);
                } else if (target.hasClass('stop-reporting-task')) {
                    nfReportingTask.stop(reportingTaskEntity);
                } else if (target.hasClass('delete-reporting-task')) {
                    nfReportingTask.promptToDeleteReportingTask(reportingTaskEntity);
                } else if (target.hasClass('view-state-reporting-task')) {
                    var canClear = reportingTaskEntity.component.state === 'STOPPED' && reportingTaskEntity.component.activeThreadCount === 0;
                    nfComponentState.showState(reportingTaskEntity, canClear);
                } else if (target.hasClass('change-version-reporting-task')) {
                    nfComponentVersion.promptForVersionChange(reportingTaskEntity);
                } else if (target.hasClass('edit-access-policies')) {
                    // show the policies for this service
                    nfPolicyManagement.showReportingTaskPolicy(reportingTaskEntity);

                    // close the settings dialog
                    $('#shell-close-button').click();
                }
            } else if (reportingTasksGrid.getColumns()[args.cell].id === 'moreDetails') {
                if (target.hasClass('view-reporting-task')) {
                    nfReportingTask.showDetails(reportingTaskEntity);
                } else if (target.hasClass('reporting-task-usage')) {
                    // close the settings dialog
                    $('#shell-close-button').click();

                    // open the documentation for this reporting task
                    nfShell.showPage('../nifi-docs/documentation?' + $.param({
                            select: reportingTaskEntity.component.type,
                            group: reportingTaskEntity.component.bundle.group,
                            artifact: reportingTaskEntity.component.bundle.artifact,
                            version: reportingTaskEntity.component.bundle.version
                        })).done(function () {
                        nfSettings.showSettings();
                    });
                }
            }
        });

        // wire up the dataview to the grid
        reportingTasksData.onRowCountChanged.subscribe(function (e, args) {
            reportingTasksGrid.updateRowCount();
            reportingTasksGrid.render();
        });
        reportingTasksData.onRowsChanged.subscribe(function (e, args) {
            reportingTasksGrid.invalidateRows(args.rows);
            reportingTasksGrid.render();
        });
        reportingTasksData.syncGridSelection(reportingTasksGrid, true);

        // hold onto an instance of the grid
        $('#reporting-tasks-table').data('gridInstance', reportingTasksGrid).on('mouseenter', 'div.slick-cell', function (e) {
            var errorIcon = $(this).find('div.has-errors');
            if (errorIcon.length && !errorIcon.data('qtip')) {
                var taskId = $(this).find('span.row-id').text();

                // get the task item
                var reportingTaskEntity = reportingTasksData.getItemById(taskId);

                // format the errors
                var tooltip = nfCommon.formatUnorderedList(reportingTaskEntity.component.validationErrors);

                // show the tooltip
                if (nfCommon.isDefinedAndNotNull(tooltip)) {
                    errorIcon.qtip($.extend({},
                        nfCommon.config.tooltipConfig,
                        {
                            content: tooltip,
                            position: {
                                target: 'mouse',
                                viewport: $('#shell-container'),
                                adjust: {
                                    x: 8,
                                    y: 8,
                                    method: 'flipinvert flipinvert'
                                }
                            }
                        }));
                }
            }

            var bulletinIcon = $(this).find('div.has-bulletins');
            if (bulletinIcon.length && !bulletinIcon.data('qtip')) {
                var taskId = $(this).find('span.row-id').text();

                // get the task item
                var reportingTaskEntity = reportingTasksData.getItemById(taskId);

                // format the tooltip
                var bulletins = nfCommon.getFormattedBulletins(reportingTaskEntity.bulletins);
                var tooltip = nfCommon.formatUnorderedList(bulletins);

                // show the tooltip
                if (nfCommon.isDefinedAndNotNull(tooltip)) {
                    bulletinIcon.qtip($.extend({},
                        nfCommon.config.tooltipConfig,
                        {
                            content: tooltip,
                            position: {
                                target: 'mouse',
                                viewport: $('#shell-container'),
                                adjust: {
                                    x: 8,
                                    y: 8,
                                    method: 'flipinvert flipinvert'
                                }
                            }
                        }));
                }
            }
        });
    };

    /**
     * Loads the settings.
     */
    var loadSettings = function () {
        var setUnauthorizedText = function () {
            $('#read-only-maximum-timer-driven-thread-count-field').addClass('unset').text('Unauthorized');
            $('#read-only-maximum-event-driven-thread-count-field').addClass('unset').text('Unauthorized');
        };

        var setEditable = function (editable) {
            if (editable) {
                $('#general-settings div.editable').show();
                $('#general-settings div.read-only').hide();
                $('#settings-save').show();
            } else {
                $('#general-settings div.editable').hide();
                $('#general-settings div.read-only').show();
                $('#settings-save').hide();
            }
        };

        var settings = $.Deferred(function (deferred) {
            $.ajax({
                type: 'GET',
                url: config.urls.controllerConfig,
                dataType: 'json'
            }).done(function (response) {
                if (response.permissions.canWrite) {
                    // populate the settings
                    $('#maximum-timer-driven-thread-count-field').removeClass('unset').val(response.component.maxTimerDrivenThreadCount);
                    $('#maximum-event-driven-thread-count-field').removeClass('unset').val(response.component.maxEventDrivenThreadCount);

                    setEditable(true);

                    // register the click listener for the save button
                    $('#settings-save').off('click').on('click', function () {
                        saveSettings(response.revision.version);
                    });
                } else {
                    if (response.permissions.canRead) {
                        // populate the settings
                        $('#read-only-maximum-timer-driven-thread-count-field').removeClass('unset').text(response.component.maxTimerDrivenThreadCount);
                        $('#read-only-maximum-event-driven-thread-count-field').removeClass('unset').text(response.component.maxEventDrivenThreadCount);
                    } else {
                        setUnauthorizedText();
                    }

                    setEditable(false);
                }
                deferred.resolve();
            }).fail(function (xhr, status, error) {
                if (xhr.status === 403) {
                    setUnauthorizedText();
                    setEditable(false);
                    deferred.resolve();
                } else {
                    deferred.reject(xhr, status, error);
                }
            });
        }).promise();

        // load the controller services
        var controllerServicesUri = config.urls.api + '/flow/controller/controller-services';
        var controllerServicesXhr = nfControllerServices.loadControllerServices(controllerServicesUri, getControllerServicesTable());

        // load the reporting tasks
        var reportingTasks = loadReportingTasks();

        // return a deferred for all parts of the settings
        return $.when(settings, controllerServicesXhr, reportingTasks).done(function (settingsResult, controllerServicesResult) {
            var controllerServicesResponse = controllerServicesResult[0];

            // update the current time
            $('#settings-last-refreshed').text(controllerServicesResponse.currentTime);
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Loads the reporting tasks.
     */
    var loadReportingTasks = function () {
        return $.ajax({
            type: 'GET',
            url: config.urls.reportingTasks,
            dataType: 'json'
        }).done(function (response) {
            var tasks = [];
            $.each(response.reportingTasks, function (_, task) {
                tasks.push($.extend({
                    type: 'ReportingTask',
                    bulletins: []
                }, task));
            });

            var reportingTasksElement = $('#reporting-tasks-table');
            nfCommon.cleanUpTooltips(reportingTasksElement, 'div.has-errors');
            nfCommon.cleanUpTooltips(reportingTasksElement, 'div.has-bulletins');

            var reportingTasksGrid = reportingTasksElement.data('gridInstance');
            var reportingTasksData = reportingTasksGrid.getData();

            // update the reporting tasks
            reportingTasksData.setItems(tasks);
            reportingTasksData.reSort();
            reportingTasksGrid.invalidate();
        });
    };

    /**
     * Shows the process group configuration.
     */
    var showSettings = function () {
        // show the settings dialog
        nfShell.showContent('#settings').done(function () {
            reset();
        });

        //reset content to account for possible policy changes
        $('#settings-tabs').find('.selected-tab').click();

        // adjust the table size
        nfSettings.resetTableSize();
    };

    /**
     * Reset state of this dialog.
     */
    var reset = function () {
        // reset button state
        $('#settings-save').mouseout();
    };

    var nfSettings = {
        /**
         * Initializes the settings page.
         */
        init: function () {
            // initialize the settings tabs
            $('#settings-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                scrollableTabContentStyle: 'scrollable',
                tabs: [{
                    name: 'General',
                    tabContentId: 'general-settings-tab-content'
                }, {
                    name: 'Reporting Task Controller Services',
                    tabContentId: 'controller-services-tab-content'
                }, {
                    name: 'Reporting Tasks',
                    tabContentId: 'reporting-tasks-tab-content'
                }],
                select: function () {
                    var tab = $(this).text();
                    if (tab === 'General') {
                        $('#controller-cs-availability').hide();
                        $('#new-service-or-task').hide();
                        $('#settings-save').show();
                    } else {
                        var canModifyController = false;
                        if (nfCommon.isDefinedAndNotNull(nfCommon.currentUser)) {
                            // only consider write permissions for creating new controller services/reporting tasks
                            canModifyController = nfCommon.currentUser.controllerPermissions.canWrite === true;
                        }

                        if (canModifyController) {
                            $('#new-service-or-task').show();
                            $('div.controller-settings-table').css('top', '32px');

                            // update the tooltip on the button
                            $('#new-service-or-task').attr('title', function () {
                                if (tab === 'Reporting Task Controller Services') {
                                    $('#settings-save').hide();
                                    return 'Create a new reporting task controller service';
                                } else if (tab === 'Reporting Tasks') {
                                    $('#settings-save').hide();
                                    return 'Create a new reporting task';
                                }
                            });
                        } else {
                            $('#new-service-or-task').hide();
                            $('div.controller-settings-table').css('top', '0');
                        }

                        if (tab === 'Reporting Task Controller Services') {
                            $('#controller-cs-availability').show();
                        } else if (tab === 'Reporting Tasks') {
                            $('#controller-cs-availability').hide();
                        }

                        // resize the table
                        nfSettings.resetTableSize();
                    }
                }
            });

            // settings refresh button
            $('#settings-refresh-button').click(function () {
                loadSettings();
            });

            // create a new controller service or reporting task
            $('#new-service-or-task').on('click', function () {
                var selectedTab = $('#settings-tabs li.selected-tab').text();
                if (selectedTab === 'Reporting Task Controller Services') {
                    var controllerServicesUri = config.urls.api + '/controller/controller-services';
                    nfControllerServices.promptNewControllerService(controllerServicesUri, getControllerServicesTable());
                } else if (selectedTab === 'Reporting Tasks') {
                    $('#new-reporting-task-dialog').modal('show');

                    var reportingTaskTypesGrid = $('#reporting-task-types-table').data('gridInstance');
                    if (nfCommon.isDefinedAndNotNull(reportingTaskTypesGrid)) {
                        var reportingTaskTypesData = reportingTaskTypesGrid.getData();

                        // reset the canvas size after the dialog is shown
                        reportingTaskTypesGrid.resizeCanvas();

                        // select the first row if possible
                        if (reportingTaskTypesData.getLength() > 0) {
                            reportingTaskTypesGrid.setSelectedRows([0]);
                        }
                    }

                    // set the initial focus
                    $('#reporting-task-type-filter').focus();
                }
            });

            // initialize each tab
            initGeneral();
            nfControllerServices.init(getControllerServicesTable(), nfSettings.showSettings);
            initReportingTasks();
        },

        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            nfControllerServices.resetTableSize(getControllerServicesTable());

            var reportingTasksGrid = $('#reporting-tasks-table').data('gridInstance');
            if (nfCommon.isDefinedAndNotNull(reportingTasksGrid)) {
                reportingTasksGrid.resizeCanvas();
            }
        },

        /**
         * Shows the settings dialog.
         */
        showSettings: function () {
            return loadSettings().done(showSettings);
        },

        /**
         * Loads the settings dialogs.
         */
        loadSettings: function () {
            return loadSettings();
        },

        /**
         * Selects the specified controller service.
         *
         * @param {string} controllerServiceId
         */
        selectControllerService: function (controllerServiceId) {
            var controllerServiceGrid = getControllerServicesTable().data('gridInstance');
            var controllerServiceData = controllerServiceGrid.getData();

            // select the desired service
            var row = controllerServiceData.getRowById(controllerServiceId);
            controllerServiceGrid.setSelectedRows([row]);
            controllerServiceGrid.scrollRowIntoView(row);

            // select the controller services tab
            $('#settings-tabs').find('li:eq(1)').click();
        },

        /**
         * Selects the specified reporting task.
         *
         * @param {string} reportingTaskId
         */
        selectReportingTask: function (reportingTaskId) {
            var reportingTaskGrid = $('#reporting-tasks-table').data('gridInstance');
            var reportingTaskData = reportingTaskGrid.getData();

            // select the desired service
            var row = reportingTaskData.getRowById(reportingTaskId);
            reportingTaskGrid.setSelectedRows([row]);
            reportingTaskGrid.scrollRowIntoView(row);

            // select the controller services tab
            $('#settings-tabs').find('li:eq(2)').click();
        },

        /**
         * Sets the controller service and reporting task bulletins in their respective tables.
         *
         * @param {object} controllerServiceBulletins
         * @param {object} reportingTaskBulletins
         */
        setBulletins: function (controllerServiceBulletins, reportingTaskBulletins) {
            if ($('#controller-services-table').data('gridInstance')) {
                nfControllerServices.setBulletins(getControllerServicesTable(), controllerServiceBulletins);
            }

            // reporting tasks
            var reportingTasksGrid = $('#reporting-tasks-table').data('gridInstance');
            var reportingTasksData = reportingTasksGrid.getData();
            reportingTasksData.beginUpdate();

            // if there are some bulletins process them
            if (!nfCommon.isEmpty(reportingTaskBulletins)) {
                var reportingTaskBulletinsBySource = d3.nest()
                    .key(function (d) {
                        return d.sourceId;
                    })
                    .map(reportingTaskBulletins, d3.map);

                reportingTaskBulletinsBySource.forEach(function (sourceId, sourceBulletins) {
                    var reportingTask = reportingTasksData.getItemById(sourceId);
                    if (nfCommon.isDefinedAndNotNull(reportingTask)) {
                        reportingTasksData.updateItem(sourceId, $.extend(reportingTask, {
                            bulletins: sourceBulletins
                        }));
                    }
                });
            } else {
                // if there are no bulletins clear all
                var reportingTasks = reportingTasksData.getItems();
                $.each(reportingTasks, function (_, reportingTask) {
                    reportingTasksData.updateItem(reportingTask.id, $.extend(reportingTask, {
                        bulletins: []
                    }));
                });
            }
            reportingTasksData.endUpdate();
        }
    };

    return nfSettings;
}));