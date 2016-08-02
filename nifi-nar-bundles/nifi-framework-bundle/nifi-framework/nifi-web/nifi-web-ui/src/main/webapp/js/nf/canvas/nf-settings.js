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

/* global nf, Slick, d3 */

nf.Settings = (function () {

    var config = {
        filterText: 'Filter',
        styles: {
            filterList: 'filter-list'
        },
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
            'revision': nf.Client.getRevision({
                'revision': {
                    'version': version
                }
            }),
            'controllerConfiguration': configuration
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
            nf.Dialog.showOkDialog({
                headerText: 'Settings',
                dialogContent: 'Settings successfully applied.'
            });

            // register the click listener for the save button
            $('#settings-save').off('click').on('click', function () {
                saveSettings(response.revision.version);
            });
        }).fail(nf.Common.handleAjaxError);
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
            return '<span class="blank">' + dataContext.id + '</span>';
        }

        return dataContext.component.name;
    };

    /**
     * Formatter for the type column.
     *
     * @param {type} row
     * @param {type} cell
     * @param {type} value
     * @param {type} columnDef
     * @param {type} dataContext
     * @returns {String}
     */
    var typeFormatter = function (row, cell, value, columnDef, dataContext) {
        if (!dataContext.permissions.canRead) {
            return '';
        }

        return nf.Common.substringAfterLast(dataContext.component.type, '.');
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
            if(a.permissions.canRead && b.permissions.canRead) {
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
                } else if (sortDetails.columnId === 'type') {
                    var aType = nf.Common.isDefinedAndNotNull(a.component[sortDetails.columnId]) ? nf.Common.substringAfterLast(a.component[sortDetails.columnId], '.') : '';
                    var bType = nf.Common.isDefinedAndNotNull(b.component[sortDetails.columnId]) ? nf.Common.substringAfterLast(b.component[sortDetails.columnId], '.') : '';
                    return aType === bType ? 0 : aType > bType ? 1 : -1;
                } else if (sortDetails.columnId === 'state') {
                    var aState = 'Invalid';
                    if (nf.Common.isEmpty(a.component.validationErrors)) {
                        aState = nf.Common.isDefinedAndNotNull(a.component[sortDetails.columnId]) ? a.component[sortDetails.columnId] : '';
                    }
                    var bState = 'Invalid';
                    if (nf.Common.isEmpty(b.component.validationErrors)) {
                        bState = nf.Common.isDefinedAndNotNull(b.component[sortDetails.columnId]) ? b.component[sortDetails.columnId] : '';
                    }
                    return aState === bState ? 0 : aState > bState ? 1 : -1;
                } else {
                    var aString = nf.Common.isDefinedAndNotNull(a.component[sortDetails.columnId]) ? a.component[sortDetails.columnId] : '';
                    var bString = nf.Common.isDefinedAndNotNull(b.component[sortDetails.columnId]) ? b.component[sortDetails.columnId] : '';
                    return aString === bString ? 0 : aString > bString ? 1 : -1;
                }
            } else {
                if (!a.permissions.canRead && !b.permissions.canRead){
                    return 0;
                }
                if(a.permissions.canRead){
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
        var filterText = '';
        var filterField = $('#reporting-task-type-filter');
        if (!filterField.hasClass(config.styles.filterList)) {
            filterText = filterField.val();
        }
        return filterText;
    };

    /**
     * Filters the reporting task type table.
     */
    var applyReportingTaskTypeFilter = function () {
        // get the dataview
        var reportingTaskTypesGrid = $('#reporting-task-types-table').data('gridInstance');

        // ensure the grid has been initialized
        if (nf.Common.isDefinedAndNotNull(reportingTaskTypesGrid)) {
            var reportingTaskTypesData = reportingTaskTypesGrid.getData();

            // update the search criteria
            reportingTaskTypesData.setFilterArgs({
                searchString: getReportingTaskTypeFilterText()
            });
            reportingTaskTypesData.refresh();

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
        $('#reporting-task-type-description').text('');
        $('#reporting-task-type-name').text('');
        $('#selected-reporting-task-name').text('');
        $('#selected-reporting-task-type').text('');
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

        // determine if this row should be visible
        var matches = matchesFilter && matchesTags;

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

        // ensure something was selected
        if (selectedTaskType === '') {
            nf.Dialog.showOkDialog({
                headerText: 'Settings',
                dialogContent: 'The type of reporting task to create must be selected.'
            });
        } else {
            addReportingTask(selectedTaskType);
        }
    };

    /**
     * Adds a new reporting task of the specified type.
     *
     * @param {string} reportingTaskType
     */
    var addReportingTask = function (reportingTaskType) {
        // build the reporting task entity
        var reportingTaskEntity = {
            'revision': nf.Client.getRevision({
                'revision': {
                    'version': 0
                }
            }),
            'component': {
                'type': reportingTaskType
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
            reportingTaskData.addItem(reportingTaskEntity);

            // resort
            reportingTaskData.reSort();
            reportingTaskGrid.invalidate();

            // select the new reporting task
            var row = reportingTaskData.getRowById(reportingTaskEntity.id);
            reportingTaskGrid.setSelectedRows([row]);
            reportingTaskGrid.scrollRowIntoView(row);
        }).fail(nf.Common.handleAjaxError);

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
                addSelectedReportingTask();
            } else {
                applyReportingTaskTypeFilter();
            }
        }).focus(function () {
            if ($(this).hasClass(config.styles.filterList)) {
                $(this).removeClass(config.styles.filterList).val('');
            }
        }).blur(function () {
            if ($(this).val() === '') {
                $(this).addClass(config.styles.filterList).val(config.filterText);
            }
        }).addClass(config.styles.filterList).val(config.filterText);

        // initialize the processor type table
        var reportingTaskTypesColumns = [
            {id: 'type', name: 'Type', field: 'label', sortable: false, resizable: true},
            {id: 'tags', name: 'Tags', field: 'tags', sortable: false, resizable: true}
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

        // initialize the grid
        var reportingTaskTypesGrid = new Slick.Grid('#reporting-task-types-table', reportingTaskTypesData, reportingTaskTypesColumns, gridOptions);
        reportingTaskTypesGrid.setSelectionModel(new Slick.RowSelectionModel());
        reportingTaskTypesGrid.registerPlugin(new Slick.AutoTooltips());
        reportingTaskTypesGrid.setSortColumn('type', true);
        reportingTaskTypesGrid.onSelectedRowsChanged.subscribe(function (e, args) {
            if ($.isArray(args.rows) && args.rows.length === 1) {
                var reportingTaskTypeIndex = args.rows[0];
                var reportingTaskType = reportingTaskTypesGrid.getDataItem(reportingTaskTypeIndex);

                // set the reporting task type description
                if (nf.Common.isDefinedAndNotNull(reportingTaskType)) {
                    if (nf.Common.isBlank(reportingTaskType.description)) {
                        $('#reporting-task-type-description').attr('title', '').html('<span class="unset">No description specified</span>');
                    } else {
                        $('#reporting-task-type-description').html(reportingTaskType.description).ellipsis();
                    }

                    // populate the dom
                    $('#reporting-task-type-name').text(reportingTaskType.label).ellipsis();
                    $('#selected-reporting-task-name').text(reportingTaskType.label);
                    $('#selected-reporting-task-type').text(reportingTaskType.type);

                    // show the selected reporting task
                    $('#reporting-task-description-container').show();
                }
            }
        });
        reportingTaskTypesGrid.onDblClick.subscribe(function (e, args) {
            var reportingTaskType = reportingTaskTypesGrid.getDataItem(args.row);
            addReportingTask(reportingTaskType.type);
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
        $('#reporting-task-types-table').data('gridInstance', reportingTaskTypesGrid);

        // load the available reporting tasks
        $.ajax({
            type: 'GET',
            url: config.urls.reportingTaskTypes,
            dataType: 'json'
        }).done(function (response) {
            var id = 0;
            var tags = [];

            // begin the update
            reportingTaskTypesData.beginUpdate();

            // go through each reporting task type
            $.each(response.reportingTaskTypes, function (i, documentedType) {
                // add the documented type
                reportingTaskTypesData.addItem({
                    id: id++,
                    label: nf.Common.substringAfterLast(documentedType.type, '.'),
                    type: documentedType.type,
                    description: nf.Common.escapeHtml(documentedType.description),
                    tags: documentedType.tags.join(', ')
                });

                // count the frequency of each tag for this type
                $.each(documentedType.tags, function (i, tag) {
                    tags.push(tag.toLowerCase());
                });
            });

            // end the udpate
            reportingTaskTypesData.endUpdate();

            // set the total number of processors
            $('#total-reporting-task-types, #displayed-reporting-task-types').text(response.reportingTaskTypes.length);

            // create the tag cloud
            $('#reporting-task-tag-cloud').tagcloud({
                tags: tags,
                select: applyReportingTaskTypeFilter,
                remove: applyReportingTaskTypeFilter
            });
        }).fail(nf.Common.handleAjaxError);

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
                    $('#reporting-task-type-filter').addClass(config.styles.filterList).val(config.filterText);

                    // clear the tagcloud
                    $('#reporting-task-tag-cloud').tagcloud('clearSelectedTags');

                    // reset the filter
                    applyReportingTaskTypeFilter();

                    // unselect any current selection
                    var reportingTaskTypesGrid = $('#reporting-task-types-table').data('gridInstance');
                    reportingTaskTypesGrid.setSelectedRows([]);
                    reportingTaskTypesGrid.resetActiveCell();
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

            var markup = '<div title="View Details" class="pointer view-reporting-task fa fa-info-circle" style="margin-top: 5px; float: left;" ></div>';

            // always include a button to view the usage
            markup += '<div title="Usage" class="pointer reporting-task-usage fa fa-book" style="margin-left: 6px; margin-top: 5px; float: left;"></div>';

            var hasErrors = !nf.Common.isEmpty(dataContext.component.validationErrors);
            var hasBulletins = !nf.Common.isEmpty(dataContext.bulletins);

            if (hasErrors) {
                markup += '<div class="pointer has-errors fa fa-warning" style="margin-top: 4px; margin-left: 3px; float: left;" ></div>';
            }

            if (hasBulletins) {
                markup += '<div class="has-bulletins fa fa-sticky-note-o" style="margin-top: 5px; margin-left: 5px; float: left;"></div>';
            }

            if (hasErrors || hasBulletins) {
                markup += '<span class="hidden row-id">' + nf.Common.escapeHtml(dataContext.component.id) + '</span>';
            }

            return markup;
        };

        var reportingTaskRunStatusFormatter = function (row, cell, value, columnDef, dataContext) {
            if (!dataContext.permissions.canRead) {
                return '';
            }

            // determine the appropriate label
            var icon = '', label = '';
            if (!nf.Common.isEmpty(dataContext.component.validationErrors)) {
                label = 'Invalid';
                icon = 'invalid fa fa-warning';
            } else {
                if (dataContext.component.state === 'STOPPED') {
                    label = 'Stopped';
                    icon = 'fa fa-stop';
                } else if (dataContext.component.state === 'RUNNING') {
                    label = 'Running';
                    icon = 'fa fa-play';
                } else {
                    label = 'Disabled';
                    icon = 'icon icon-enable-false';
                }
            }

            // include the active thread count if appropriate
            var activeThreadCount = '';
            if (nf.Common.isDefinedAndNotNull(dataContext.component.activeThreadCount) && dataContext.component.activeThreadCount > 0) {
                activeThreadCount = '(' + dataContext.component.activeThreadCount + ')';
            }

            // format the markup
            var formattedValue = '<div layout="row"><div class="' + icon + '" style="margin-top: 3px;"></div>';
            return formattedValue + '<div class="status-text" style="margin-top: 4px;">' + nf.Common.escapeHtml(label) + '</div><div style="float: left; margin-left: 4px;">' + nf.Common.escapeHtml(activeThreadCount) + '</div></div>';
        };

        var reportingTaskActionFormatter = function (row, cell, value, columnDef, dataContext) {
            var markup = '';

            if (dataContext.permissions.canRead && dataContext.permissions.canWrite) {
                if (dataContext.component.state === 'RUNNING') {
                    markup += '<div title="Stop" class="pointer stop-reporting-task fa fa-stop" style="margin-top: 2px; margin-right: 3px;" ></div>';
                } else if (dataContext.component.state === 'STOPPED' || dataContext.component.state === 'DISABLED') {
                    markup += '<div title="Edit" class="pointer edit-reporting-task fa fa-pencil" style="margin-top: 2px; margin-right: 3px;" ></div>';

                    // support starting when stopped and no validation errors
                    if (dataContext.component.state === 'STOPPED' && nf.Common.isEmpty(dataContext.component.validationErrors)) {
                        markup += '<div title="Start" class="pointer start-reporting-task fa fa-play" style="margin-top: 2px; margin-right: 3px;"></div>';
                    }

                    markup += '<div title="Remove" class="pointer delete-reporting-task fa fa-trash" style="margin-top: 2px; margin-right: 3px;" ></div>';
                }

                if (dataContext.component.persistsState === true) {
                    markup += '<div title="View State" class="pointer view-state-reporting-task fa fa-tasks" style="margin-top: 2px; margin-right: 3px;" ></div>';
                }
            }

            // allow policy configuration conditionally
            if (nf.Canvas.isConfigurableAuthorizer() && nf.Common.canAccessTenants()) {
                markup += '<div title="Access Policies" class="pointer edit-access-policies fa fa-key" style="margin-top: 2px;"></div>';
            }

            return markup;
        };

        // define the column model for the reporting tasks table
        var reportingTasksColumnModel = [
            {id: 'moreDetails', name: '&nbsp;', resizable: false, formatter: moreReportingTaskDetails, sortable: true, width: 90, maxWidth: 90, toolTip: 'Sorts based on presence of bulletins'},
            {id: 'name', name: 'Name', sortable: true, resizable: true, formatter: nameFormatter},
            {id: 'type', name: 'Type', sortable: true, resizable: true, formatter: typeFormatter},
            {id: 'state', name: 'Run Status', sortable: true, resizeable: true, formatter: reportingTaskRunStatusFormatter}
        ];

        // action column should always be last
        reportingTasksColumnModel.push({id: 'actions', name: '&nbsp;', resizable: false, formatter: reportingTaskActionFormatter, sortable: false, width: 90, maxWidth: 90});

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
                    nf.ReportingTask.showConfiguration(reportingTaskEntity);
                } else if (target.hasClass('start-reporting-task')) {
                    nf.ReportingTask.start(reportingTaskEntity);
                } else if (target.hasClass('stop-reporting-task')) {
                    nf.ReportingTask.stop(reportingTaskEntity);
                } else if (target.hasClass('delete-reporting-task')) {
                    nf.ReportingTask.remove(reportingTaskEntity);
                } else if (target.hasClass('view-state-reporting-task')) {
                    var canClear = reportingTaskEntity.component.state === 'STOPPED' && reportingTaskEntity.component.activeThreadCount === 0;
                    nf.ComponentState.showState(reportingTaskEntity, canClear);
                } else if (target.hasClass('edit-access-policies')) {
                    // show the policies for this service
                    nf.PolicyManagement.showReportingTaskPolicy(reportingTaskEntity);

                    // close the settings dialog
                    $('#shell-close-button').click();
                }
            } else if (reportingTasksGrid.getColumns()[args.cell].id === 'moreDetails') {
                if (target.hasClass('view-reporting-task')) {
                    nf.ReportingTask.showDetails(reportingTaskEntity);
                } else if (target.hasClass('reporting-task-usage')) {
                    // close the settings dialog
                    $('#shell-close-button').click();

                    // open the documentation for this reporting task
                    nf.Shell.showPage('../nifi-docs/documentation?' + $.param({
                            select: nf.Common.substringAfterLast(reportingTaskEntity.component.type, '.')
                        })).done(function () {
                        nf.Settings.showSettings();
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
                var tooltip = nf.Common.formatUnorderedList(reportingTaskEntity.component.validationErrors);

                // show the tooltip
                if (nf.Common.isDefinedAndNotNull(tooltip)) {
                    errorIcon.qtip($.extend({},
                        nf.Common.config.tooltipConfig,
                        {
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
                        }));
                }
            }

            var bulletinIcon = $(this).find('div.has-bulletins');
            if (bulletinIcon.length && !bulletinIcon.data('qtip')) {
                var taskId = $(this).find('span.row-id').text();

                // get the task item
                var reportingTaskEntity = reportingTasksData.getItemById(taskId);

                // format the tooltip
                var bulletins = nf.Common.getFormattedBulletins(reportingTaskEntity.bulletins);
                var tooltip = nf.Common.formatUnorderedList(bulletins);

                // show the tooltip
                if (nf.Common.isDefinedAndNotNull(tooltip)) {
                    bulletinIcon.qtip($.extend({},
                        nf.Common.config.tooltipConfig,
                        {
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
                // update the current time
                $('#settings-last-refreshed').text(response.currentTime);

                if (response.permissions.canWrite) {
                    // populate the settings
                    $('#maximum-timer-driven-thread-count-field').removeClass('unset').val(response.controllerConfiguration.maxTimerDrivenThreadCount);
                    $('#maximum-event-driven-thread-count-field').removeClass('unset').val(response.controllerConfiguration.maxEventDrivenThreadCount);

                    setEditable(true);

                    // register the click listener for the save button
                    $('#settings-save').off('click').on('click', function () {
                        saveSettings(response.revision.version);
                    });
                } else {
                    if (response.permissions.canRead) {
                        // populate the settings
                        $('#read-only-maximum-timer-driven-thread-count-field').removeClass('unset').text(response.controllerConfiguration.maxTimerDrivenThreadCount);
                        $('#read-only-maximum-event-driven-thread-count-field').removeClass('unset').text(response.controllerConfiguration.maxEventDrivenThreadCount);
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
        var controllerServices = nf.ControllerServices.loadControllerServices(controllerServicesUri, getControllerServicesTable());

        // load the reporting tasks
        var reportingTasks = loadReportingTasks();

        // return a deferred for all parts of the settings
        return $.when(settings, controllerServices, reportingTasks).fail(nf.Common.handleAjaxError);
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
                    bulletins: []
                }, task));
            });

            var reportingTasksElement = $('#reporting-tasks-table');
            nf.Common.cleanUpTooltips(reportingTasksElement, 'div.has-errors');
            nf.Common.cleanUpTooltips(reportingTasksElement, 'div.has-bulletins');

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
        nf.Shell.showContent('#settings').done(function () {
            reset();
        });

        //reset content to account for possible policy changes
        $('#settings-tabs').find('.selected-tab').click();

        // adjust the table size
        nf.Settings.resetTableSize();
    };

    /**
     * Reset state of this dialog.
     */
    var reset = function () {
        // reset button state
        $('#settings-save').mouseout();
    };

    return {
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
                    name: 'Controller Services',
                    tabContentId: 'controller-services-tab-content'
                }, {
                    name: 'Reporting Tasks',
                    tabContentId: 'reporting-tasks-tab-content'
                }],
                select: function () {
                    var tab = $(this).text();
                    if (tab === 'General') {
                        $('#new-service-or-task').hide();
                        $('#settings-save').show();
                    } else {
                        if (nf.Common.canModifyController()) {
                            $('#new-service-or-task').show();

                            // update the tooltip on the button
                            $('#new-service-or-task').attr('title', function () {
                                if (tab === 'Controller Services') {
                                    $('#settings-save').hide();
                                    return 'Create a new controller service';
                                } else if (tab === 'Reporting Tasks') {
                                    $('#settings-save').hide();
                                    return 'Create a new reporting task';
                                }
                            });
                        } else {
                            $('#new-service-or-task').hide();
                        }

                        // resize the table
                        nf.Settings.resetTableSize();
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
                if (selectedTab === 'Controller Services') {
                    var controllerServicesUri = config.urls.api + '/controller/controller-services';
                    nf.ControllerServices.promptNewControllerService(controllerServicesUri, getControllerServicesTable());
                } else if (selectedTab === 'Reporting Tasks') {
                    $('#new-reporting-task-dialog').modal('show');

                    // reset the canvas size after the dialog is shown
                    var reportingTaskTypesGrid = $('#reporting-task-types-table').data('gridInstance');
                    if (nf.Common.isDefinedAndNotNull(reportingTaskTypesGrid)) {
                        reportingTaskTypesGrid.setSelectedRows([0]);
                        reportingTaskTypesGrid.resizeCanvas();
                    }

                    // set the initial focus
                    $('#reporting-task-type-filter').focus();
                }
            });
            
            // initialize each tab
            initGeneral();
            nf.ControllerServices.init(getControllerServicesTable());
            initReportingTasks();
        },

        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            nf.ControllerServices.resetTableSize(getControllerServicesTable());

            var reportingTasksGrid = $('#reporting-tasks-table').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(reportingTasksGrid)) {
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
         * Sets the controller service and reporting task bulletins in their respective tables.
         *
         * @param {object} controllerServiceBulletins
         * @param {object} reportingTaskBulletins
         */
        setBulletins: function(controllerServiceBulletins, reportingTaskBulletins) {
            if ($('#controller-services-table').data('gridInstance')) {
                nf.ControllerServices.setBulletins(getControllerServicesTable(), controllerServiceBulletins);
            }

            // reporting tasks
            var reportingTasksGrid = $('#reporting-tasks-table').data('gridInstance');
            var reportingTasksData = reportingTasksGrid.getData();
            reportingTasksData.beginUpdate();

            // if there are some bulletins process them
            if (!nf.Common.isEmpty(reportingTaskBulletins)) {
                var reportingTaskBulletinsBySource = d3.nest()
                    .key(function (d) {
                        return d.sourceId;
                    })
                    .map(reportingTaskBulletins, d3.map);

                reportingTaskBulletinsBySource.forEach(function (sourceId, sourceBulletins) {
                    var reportingTask = reportingTasksData.getItemById(sourceId);
                    if (nf.Common.isDefinedAndNotNull(reportingTask)) {
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
}());