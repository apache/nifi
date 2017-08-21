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
                'd3',
                'Slick',
                'nf.Client',
                'nf.Shell',
                'nf.ProcessGroupConfiguration',
                'nf.CanvasUtils',
                'nf.ErrorHandler',
                'nf.Dialog',
                'nf.Common',
                'nf.ControllerService',
                'nf.ProcessGroup',
                'nf.PolicyManagement',
                'nf.ComponentState',
                'nf.ComponentVersion',
                'nf.ng.Bridge'],
            function ($, d3, Slick, nfClient, nfShell, nfProcessGroupConfiguration, nfCanvasUtils, nfErrorHandler, nfDialog, nfCommon, nfControllerService, nfProcessGroup, nfPolicyManagement, nfComponentState, nfComponentVersion, nfNgBridge) {
                return (nf.ControllerServices = factory($, d3, Slick, nfClient, nfShell, nfProcessGroupConfiguration, nfCanvasUtils, nfErrorHandler, nfDialog, nfCommon, nfControllerService, nfProcessGroup, nfPolicyManagement, nfComponentState, nfComponentVersion, nfNgBridge));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ControllerServices =
            factory(require('jquery'),
                require('d3'),
                require('Slick'),
                require('nf.Client'),
                require('nf.Shell'),
                require('nf.ProcessGroupConfiguration'),
                require('nf.CanvasUtils'),
                require('nf.ErrorHandler'),
                require('nf.Dialog'),
                require('nf.Common'),
                require('nf.ControllerService'),
                require('nf.ProcessGroup'),
                require('nf.PolicyManagement'),
                require('nf.ComponentState'),
                require('nf.ComponentVersion'),
                require('nf.ng.Bridge')));
    } else {
        nf.ControllerServices = factory(root.$,
            root.d3,
            root.Slick,
            root.nf.Client,
            root.nf.Shell,
            root.nf.ProcessGroupConfiguration,
            root.nf.CanvasUtils,
            root.nf.ErrorHandler,
            root.nf.Dialog,
            root.nf.Common,
            root.nf.ControllerService,
            root.nf.ProcessGroup,
            root.nf.PolicyManagement,
            root.nf.ComponentState,
            root.nf.ComponentVersion,
            root.nf.ng.Bridge);
    }
}(this, function ($, d3, Slick, nfClient, nfShell, nfProcessGroupConfiguration, nfCanvasUtils, nfErrorHandler, nfDialog, nfCommon, nfControllerService, nfProcessGroup, nfPolicyManagement, nfComponentState, nfComponentVersion, nfNgBridge) {
    'use strict';

    var dblClick = null;
    var initialized = false;

    var config = {
        urls: {
            api: '../nifi-api',
            controllerServiceTypes: '../nifi-api/flow/controller-service-types'
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
     * Whether the specified item is selectable.
     *
     * @param item controller service type
     */
    var isSelectable = function (item) {
        return nfCommon.isBlank(item.usageRestriction) || nfCommon.canAccessRestrictedComponents();
    };

    /**
     * Get the text out of the filter field. If the filter field doesn't
     * have any text it will contain the text 'filter list' so this method
     * accounts for that.
     */
    var getControllerServiceTypeFilterText = function () {
        return $('#controller-service-type-filter').val();
    };

    /**
     * Filters the processor type table.
     */
    var applyControllerServiceTypeFilter = function () {
        // get the dataview
        var controllerServiceTypesGrid = $('#controller-service-types-table').data('gridInstance');

        // ensure the grid has been initialized
        if (nfCommon.isDefinedAndNotNull(controllerServiceTypesGrid)) {
            var controllerServiceTypesData = controllerServiceTypesGrid.getData();

            // update the search criteria
            controllerServiceTypesData.setFilterArgs({
                searchString: getControllerServiceTypeFilterText()
            });
            controllerServiceTypesData.refresh();

            // update the buttons to possibly trigger the disabled state
            $('#new-controller-service-dialog').modal('refreshButtons');

            // update the selection if possible
            if (controllerServiceTypesData.getLength() > 0) {
                controllerServiceTypesGrid.setSelectedRows([0]);
            }
        }
    };

    /**
     * Hides the selected controller service.
     */
    var clearSelectedControllerService = function () {
        $('#controller-service-type-description').attr('title', '').text('');
        $('#controller-service-type-name').attr('title', '').text('');
        $('#controller-service-type-bundle').attr('title', '').text('');
        $('#selected-controller-service-name').text('');
        $('#selected-controller-service-type').text('').removeData('bundle');
        $('#controller-service-description-container').hide();
    };

    /**
     * Clears the selected controller service type.
     */
    var clearControllerServiceSelection = function () {
        // clear the selected row
        clearSelectedControllerService();

        // clear the active cell the it can be reselected when its included
        var controllerServiceTypesGrid = $('#controller-service-types-table').data('gridInstance');
        controllerServiceTypesGrid.resetActiveCell();
    };

    /**
     * Performs the filtering.
     *
     * @param {object} item     The item subject to filtering
     * @param {object} args     Filter arguments
     * @returns {Boolean}       Whether or not to include the item
     */
    var filterControllerServiceTypes = function (item, args) {
        // determine if the item matches the filter
        var matchesFilter = matchesRegex(item, args);

        // determine if the row matches the selected tags
        var matchesTags = true;
        if (matchesFilter) {
            var tagFilters = $('#controller-service-tag-cloud').tagcloud('getSelectedTags');
            var hasSelectedTags = tagFilters.length > 0;
            if (hasSelectedTags) {
                matchesTags = matchesSelectedTags(tagFilters, item['tags']);
            }
        }

        // determine if the row matches the selected source group
        var matchesGroup = true;
        if (matchesFilter && matchesTags) {
            var bundleGroup = $('#controller-service-bundle-group-combo').combo('getSelectedOption');
            if (nfCommon.isDefinedAndNotNull(bundleGroup) && bundleGroup.value !== '') {
                matchesGroup = (item.bundle.group === bundleGroup.value);
            }
        }

        // determine if this row should be visible
        var matches = matchesFilter && matchesTags && matchesGroup;

        // if this row is currently selected and its being filtered
        if (matches === false && $('#selected-controller-service-type').text() === item['type']) {
            clearControllerServiceSelection();
        }

        return matches;
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
     * Adds the currently selected controller service.
     *
     * @param {string} controllerServicesUri
     * @param {jQuery} serviceTable
     */
    var addSelectedControllerService = function (controllerServicesUri, serviceTable) {
        var selectedServiceType = $('#selected-controller-service-type').text();
        var selectedServiceBundle = $('#selected-controller-service-type').data('bundle');

        // ensure something was selected
        if (selectedServiceType === '') {
            nfDialog.showOkDialog({
                headerText: 'Controller Service',
                dialogContent: 'The type of controller service to create must be selected.'
            });
        } else {
            addControllerService(controllerServicesUri, serviceTable, selectedServiceType, selectedServiceBundle);
        }
    };

    /**
     * Adds a new controller service of the specified type.
     *
     * @param {string} controllerServicesUri
     * @param {jQuery} serviceTable
     * @param {string} controllerServiceType
     * @param {object} controllerServiceBundle
     */
    var addControllerService = function (controllerServicesUri, serviceTable, controllerServiceType, controllerServiceBundle) {
        // build the controller service entity
        var controllerServiceEntity = {
            'revision': nfClient.getRevision({
                'revision': {
                    'version': 0
                }
            }),
            'component': {
                'type': controllerServiceType,
                'bundle': controllerServiceBundle
            }
        };

        // add the new controller service
        var addService = $.ajax({
            type: 'POST',
            url: controllerServicesUri,
            data: JSON.stringify(controllerServiceEntity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (controllerServiceEntity) {
            // add the item
            var controllerServicesGrid = serviceTable.data('gridInstance');
            var controllerServicesData = controllerServicesGrid.getData();
            controllerServicesData.addItem($.extend({
                type: 'ControllerService',
                bulletins: []
            }, controllerServiceEntity));

            // resort
            controllerServicesData.reSort();
            controllerServicesGrid.invalidate();

            // select the new controller service
            var row = controllerServicesData.getRowById(controllerServiceEntity.id);
            controllerServicesGrid.setSelectedRows([row]);
            controllerServicesGrid.scrollRowIntoView(row);
        }).fail(nfErrorHandler.handleAjaxError);

        // hide the dialog
        $('#new-controller-service-dialog').modal('hide');

        return addService;
    };

    /**
     * Initializes the new controller service dialog.
     */
    var initNewControllerServiceDialog = function () {
        // initialize the controller service type table
        var controllerServiceTypesColumns = [
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
        var controllerServiceTypesData = new Slick.Data.DataView({
            inlineFilters: false
        });
        controllerServiceTypesData.setItems([]);
        controllerServiceTypesData.setFilterArgs({
            searchString: getControllerServiceTypeFilterText()
        });
        controllerServiceTypesData.setFilter(filterControllerServiceTypes);

        // initialize the sort
        nfCommon.sortType({
            columnId: 'type',
            sortAsc: true
        }, controllerServiceTypesData);

        // initialize the grid
        var controllerServiceTypesGrid = new Slick.Grid('#controller-service-types-table', controllerServiceTypesData, controllerServiceTypesColumns, gridOptions);
        controllerServiceTypesGrid.setSelectionModel(new Slick.RowSelectionModel());
        controllerServiceTypesGrid.registerPlugin(new Slick.AutoTooltips());
        controllerServiceTypesGrid.setSortColumn('type', true);
        controllerServiceTypesGrid.onSort.subscribe(function (e, args) {
            nfCommon.sortType({
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, controllerServiceTypesData);
        });
        controllerServiceTypesGrid.onSelectedRowsChanged.subscribe(function (e, args) {
            if ($.isArray(args.rows) && args.rows.length === 1) {
                var controllerServiceTypeIndex = args.rows[0];
                var controllerServiceType = controllerServiceTypesGrid.getDataItem(controllerServiceTypeIndex);

                // set the controller service type description
                if (nfCommon.isDefinedAndNotNull(controllerServiceType)) {
                    // show the selected controller service
                    $('#controller-service-description-container').show();

                    if (nfCommon.isBlank(controllerServiceType.description)) {
                        $('#controller-service-type-description')
                            .attr('title', '')
                            .html('<span class="unset">No description specified</span>');
                    } else {
                        $('#controller-service-type-description')
                            .width($('#controller-service-description-container').innerWidth() - 1)
                            .html(controllerServiceType.description)
                            .ellipsis();
                    }

                    var bundle = nfCommon.formatBundle(controllerServiceType.bundle);
                    var type = nfCommon.formatType(controllerServiceType);

                    // populate the dom
                    $('#controller-service-type-name').text(type).attr('title', type);
                    $('#controller-service-type-bundle').text(bundle).attr('title', bundle);
                    $('#selected-controller-service-name').text(controllerServiceType.label);
                    $('#selected-controller-service-type').text(controllerServiceType.type).data('bundle', controllerServiceType.bundle);

                    // refresh the buttons based on the current selection
                    $('#new-controller-service-dialog').modal('refreshButtons');
                }
            }
        });
        controllerServiceTypesGrid.onViewportChanged.subscribe(function (e, args) {
            nfCommon.cleanUpTooltips($('#controller-service-types-table'), 'div.view-usage-restriction');
            nfCommon.cleanUpTooltips($('#controller-service-types-table'), 'div.controller-service-apis');
        });

        // wire up the dataview to the grid
        controllerServiceTypesData.onRowCountChanged.subscribe(function (e, args) {
            controllerServiceTypesGrid.updateRowCount();
            controllerServiceTypesGrid.render();

            // update the total number of displayed processors
            $('#displayed-controller-service-types').text(args.current);
        });
        controllerServiceTypesData.onRowsChanged.subscribe(function (e, args) {
            controllerServiceTypesGrid.invalidateRows(args.rows);
            controllerServiceTypesGrid.render();
        });
        controllerServiceTypesData.syncGridSelection(controllerServiceTypesGrid, true);

        // hold onto an instance of the grid
        $('#controller-service-types-table').data('gridInstance', controllerServiceTypesGrid).on('mouseenter', 'div.slick-cell', function (e) {
            var usageRestriction = $(this).find('div.view-usage-restriction');
            if (usageRestriction.length && !usageRestriction.data('qtip')) {
                var rowId = $(this).find('span.row-id').text();

                // get the status item
                var item = controllerServiceTypesData.getItemById(rowId);

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

            var serviceApis = $(this).find('div.controller-service-apis');
            if (serviceApis.length && !serviceApis.data('qtip')) {
                var rowId = $(this).find('span.row-id').text();

                // get the status item
                var item = controllerServiceTypesData.getItemById(rowId);

                // show the tooltip
                if (!nfCommon.isEmpty(item.controllerServiceApis)) {
                    var formattedControllerServiceApis = nfCommon.getFormattedServiceApis(item.controllerServiceApis);
                    var serviceTips = nfCommon.formatUnorderedList(formattedControllerServiceApis);

                    var tipContent = $('<div style="padding: 4px;"><p>Supports Controller Services</p><br/></div>').append(serviceTips);

                    serviceApis.qtip($.extend({}, nfCommon.config.tooltipConfig, {
                        content: tipContent,
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

        // load the available controller services
        $.ajax({
            type: 'GET',
            url: config.urls.controllerServiceTypes,
            dataType: 'json'
        }).done(function (response) {
            var id = 0;
            var tags = [];
            var groups = d3.set();

            // begin the update
            controllerServiceTypesData.beginUpdate();

            // go through each controller service type
            $.each(response.controllerServiceTypes, function (i, documentedType) {
                // record the group
                groups.add(documentedType.bundle.group);

                // add the documented type
                controllerServiceTypesData.addItem({
                    id: id++,
                    label: nfCommon.substringAfterLast(documentedType.type, '.'),
                    type: documentedType.type,
                    bundle: documentedType.bundle,
                    controllerServiceApis: documentedType.controllerServiceApis,
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
            controllerServiceTypesData.endUpdate();

            // resort
            controllerServiceTypesData.reSort();
            controllerServiceTypesGrid.invalidate();

            // set the total number of processors
            $('#total-controller-service-types, #displayed-controller-service-types').text(response.controllerServiceTypes.length);

            // create the tag cloud
            $('#controller-service-tag-cloud').tagcloud({
                tags: tags,
                select: applyControllerServiceTypeFilter,
                remove: applyControllerServiceTypeFilter
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
            $('#controller-service-bundle-group-combo').combo({
                options: options,
                select: applyControllerServiceTypeFilter
            });
        }).fail(nfErrorHandler.handleAjaxError);

        // initialize the controller service dialog
        $('#new-controller-service-dialog').modal({
            headerText: 'Add Controller Service',
            scrollableContentStyle: 'scrollable',
            handler: {
                close: function () {
                    // clear the selected row
                    clearSelectedControllerService();

                    // clear any filter strings
                    $('#controller-service-type-filter').val('');

                    // clear the tagcloud
                    $('#controller-service-tag-cloud').tagcloud('clearSelectedTags');

                    // reset the group combo
                    $('#controller-service-bundle-group-combo').combo('setSelectedOption', {
                        value: ''
                    });

                    // reset the filter
                    applyControllerServiceTypeFilter();

                    // unselect any current selection
                    var controllerServiceTypesGrid = $('#controller-service-types-table').data('gridInstance');
                    controllerServiceTypesGrid.setSelectedRows([]);
                    controllerServiceTypesGrid.resetActiveCell();
                },
                resize: function () {
                    $('#controller-service-type-description')
                        .width($('#controller-service-description-container').innerWidth() - 1)
                        .text($('#controller-service-type-description').attr('title'))
                        .ellipsis();
                }
            }
        });
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
     * Formatter for the name column.
     *
     * @param {type} row
     * @param {type} cell
     * @param {type} value
     * @param {type} columnDef
     * @param {type} dataContext
     * @returns {String}
     */
    var groupIdFormatter = function (row, cell, value, columnDef, dataContext) {
        if (!dataContext.permissions.canRead) {
            return '';
        }

        if (nfCommon.isDefinedAndNotNull(dataContext.component.parentGroupId)) {
            // see if this listing is based off a selected process group
            var selection = nfCanvasUtils.getSelection();
            if (selection.empty() === false) {
                var selectedData = selection.datum();
                if (selectedData.id === dataContext.component.parentGroupId) {
                    if (selectedData.permissions.canRead) {
                        return nfCommon.escapeHtml(selectedData.component.name);
                    } else {
                        return nfCommon.escapeHtml(selectedData.id);
                    }
                }
            }

            // there's either no selection or the service is defined in an ancestor component
            var breadcrumbs = nfNgBridge.injector.get('breadcrumbsCtrl').getBreadcrumbs();

            var processGroupLabel = dataContext.component.parentGroupId;
            $.each(breadcrumbs, function (_, breadcrumbEntity) {
                if (breadcrumbEntity.id === dataContext.component.parentGroupId) {
                    processGroupLabel = breadcrumbEntity.label;
                    return false;
                }
            });

            return nfCommon.escapeHtml(processGroupLabel);
        } else {
            return 'Controller';
        }
    };

    /**
     * Determines if the user has write permissions for the parent of the specified controller service.
     *
     * @param dataContext
     * @returns {boolean} whether the user has write permissions for the parent of the controller service
     */
    var canWriteControllerServiceParent = function (dataContext) {
        // we know the process group for this controller service is part of the current breadcrumb trail
        var canWriteProcessGroupParent = function (processGroupId) {
            // see if this listing is based off a selected process group
            var selection = nfCanvasUtils.getSelection();
            if (selection.empty() === false) {
                var datum = selection.datum();
                if (datum.id === processGroupId) {
                    return datum.permissions.canWrite;
                }
            }

            // there's either no selection or the service is defined in an ancestor component
            var breadcrumbs = nfNgBridge.injector.get('breadcrumbsCtrl').getBreadcrumbs();

            var isAuthorized = false;
            $.each(breadcrumbs, function (_, breadcrumbEntity) {
                if (breadcrumbEntity.id === processGroupId) {
                    isAuthorized = breadcrumbEntity.permissions.canWrite;
                    return false;
                }
            });

            return isAuthorized;
        };

        if (nfCommon.isDefinedAndNotNull(dataContext.component.parentGroupId)) {
            return canWriteProcessGroupParent(dataContext.component.parentGroupId);
        } else {
            return nfCommon.canModifyController();
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
            if(a.permissions.canRead && b.permissions.canRead) {
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
     * Initializes the controller services tab.
     * 
     * @param {jQuery} serviceTable
     * @param {function} showSettings
     */
    var initControllerServices = function (serviceTable, showSettings) {
        // more details formatter
        var moreControllerServiceDetails = function (row, cell, value, columnDef, dataContext) {
            if (!dataContext.permissions.canRead) {
                return '';
            }

            // always include a button to view the usage
            var markup = '<div title="Usage" class="pointer controller-service-usage fa fa-book" style="margin-top: 5px; margin-right: 3px;" ></div>';

            var hasErrors = !nfCommon.isEmpty(dataContext.component.validationErrors);
            var hasBulletins = !nfCommon.isEmpty(dataContext.bulletins);

            if (hasErrors) {
                markup += '<div class="pointer has-errors fa fa-warning" style="margin-top: 4px; margin-right: 3px; float: left;" ></div>';
            }

            if (hasBulletins) {
                markup += '<div class="has-bulletins fa fa-sticky-note-o" style="margin-top: 5px; margin-right: 3px; float: left;"></div>';
            }

            if (hasErrors || hasBulletins) {
                markup += '<span class="hidden row-id">' + nfCommon.escapeHtml(dataContext.id) + '</span>';
            }

            return markup;
        };

        var controllerServiceStateFormatter = function (row, cell, value, columnDef, dataContext) {
            if (!dataContext.permissions.canRead) {
                return '';
            }
            
            // determine the appropriate label
            var icon = '', label = '';
            if (!nfCommon.isEmpty(dataContext.component.validationErrors)) {
                icon = 'invalid fa fa-warning';
                label = 'Invalid';
            } else {
                if (dataContext.component.state === 'DISABLED') {
                    icon = 'disabled icon icon-enable-false"';
                    label = 'Disabled';
                } else if (dataContext.component.state === 'DISABLING') {
                    icon = 'disabled icon icon-enable-false"';
                    label = 'Disabling';
                } else if (dataContext.component.state === 'ENABLED') {
                    icon = 'enabled fa fa-flash';
                    label = 'Enabled';
                } else if (dataContext.component.state === 'ENABLING') {
                    icon = 'enabled fa fa-flash';
                    label = 'Enabling';
                }
            }

            // format the markup
            var formattedValue = '<div layout="row"><div class="' + icon + '" style="margin-top: 5px;"></div>';
            return formattedValue + '<div class="status-text" style="margin-top: 4px;">' + label + '</div></div>';
        };

        var controllerServiceActionFormatter = function (row, cell, value, columnDef, dataContext) {
            var markup = '';

            if (dataContext.permissions.canRead && dataContext.permissions.canWrite) {
                var definedByCurrentGroup = false;
                if (nfCommon.isDefinedAndNotNull(dataContext.component.parentGroupId)) {
                    // when opened in the process group context, the current group is store in #process-group-id
                    if (dataContext.component.parentGroupId === $('#process-group-id').text()) {
                        definedByCurrentGroup = true;
                    }
                } else {
                    // when there is no parent group, the service is defined at the controller level and should be editable
                    definedByCurrentGroup = true;
                }

                if (definedByCurrentGroup === true) {
                    if (dataContext.component.state === 'ENABLED' || dataContext.component.state === 'ENABLING') {
                        markup += '<div class="pointer view-controller-service fa fa-gear" title="View Configuration" style="margin-top: 2px; margin-right: 3px;" ></div>';
                        markup += '<div class="pointer disable-controller-service icon icon-enable-false" title="Disable" style="margin-top: 2px; margin-right: 3px;" ></div>';
                    } else if (dataContext.component.state === 'DISABLED') {
                        markup += '<div class="pointer edit-controller-service fa fa-gear" title="Configure" style="margin-top: 2px; margin-right: 3px;" ></div>';

                        // if there are no validation errors allow enabling
                        if (nfCommon.isEmpty(dataContext.component.validationErrors)) {
                            markup += '<div class="pointer enable-controller-service fa fa-flash" title="Enable" style="margin-top: 2px; margin-right: 3px;"></div>';
                        }

                        if (dataContext.component.multipleVersionsAvailable === true) {
                            markup += '<div title="Change Version" class="pointer change-version-controller-service fa fa-exchange" style="margin-top: 2px; margin-right: 3px;" ></div>';
                        }

                        if (canWriteControllerServiceParent(dataContext)) {
                            markup += '<div class="pointer delete-controller-service fa fa-trash" title="Remove" style="margin-top: 2px; margin-right: 3px;" ></div>';
                        }
                    }

                    if (dataContext.component.persistsState === true) {
                        markup += '<div title="View State" class="pointer view-state-controller-service fa fa-tasks" style="margin-top: 2px; margin-right: 3px;" ></div>';
                    }
                } else {
                    markup += '<div class="pointer go-to-controller-service fa fa-long-arrow-right" title="Go To" style="margin-top: 2px; margin-right: 3px;" ></div>';
                }
            }

            // allow policy configuration conditionally
            if (nfCanvasUtils.isManagedAuthorizer() && nfCommon.canAccessTenants()) {
                markup += '<div title="Access Policies" class="pointer edit-access-policies fa fa-key" style="margin-top: 2px;"></div>';
            }

            return markup;
        };

        // define the column model for the controller services table
        var controllerServicesColumns = [
            {
                id: 'moreDetails',
                name: '&nbsp;',
                resizable: false,
                formatter: moreControllerServiceDetails,
                sortable: true,
                width: 90,
                maxWidth: 90,
                toolTip: 'Sorts based on presence of bulletins'
            },
            {
                id: 'name',
                name: 'Name',
                formatter: nameFormatter,
                sortable: true,
                resizable: true
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
                name: 'State',
                formatter: controllerServiceStateFormatter,
                sortable: true,
                resizeable: true
            },
            {
                id: 'parentGroupId',
                name: 'Scope',
                formatter: groupIdFormatter,
                sortable: true,
                resizeable: true
            }
        ];

        // action column should always be last
        controllerServicesColumns.push(
            {
                id: 'actions',
                name: '&nbsp;',
                resizable: false,
                formatter: controllerServiceActionFormatter,
                sortable: false,
                width: 90,
                maxWidth: 90
            });

        // initialize the dataview
        var controllerServicesData = new Slick.Data.DataView({
            inlineFilters: false
        });
        controllerServicesData.setItems([]);

        // initialize the sort
        sort({
            columnId: 'name',
            sortAsc: true
        }, controllerServicesData);

        // initialize the grid
        var controllerServicesGrid = new Slick.Grid(serviceTable, controllerServicesData, controllerServicesColumns, gridOptions);
        controllerServicesGrid.setSelectionModel(new Slick.RowSelectionModel());
        controllerServicesGrid.registerPlugin(new Slick.AutoTooltips());
        controllerServicesGrid.setSortColumn('name', true);
        controllerServicesGrid.onSort.subscribe(function (e, args) {
            sort({
                columnId: args.sortCol.id,
                sortAsc: args.sortAsc
            }, controllerServicesData);
        });

        // configure a click listener
        controllerServicesGrid.onClick.subscribe(function (e, args) {
            var target = $(e.target);

            // get the service at this row
            var controllerServiceEntity = controllerServicesData.getItem(args.row);

            // determine the desired action
            if (controllerServicesGrid.getColumns()[args.cell].id === 'actions') {
                if (target.hasClass('edit-controller-service')) {
                    nfControllerService.showConfiguration(serviceTable, controllerServiceEntity);
                } else if (target.hasClass('view-controller-service')) {
                    nfControllerService.showDetails(serviceTable, controllerServiceEntity);
                } else if (target.hasClass('enable-controller-service')) {
                    nfControllerService.enable(serviceTable, controllerServiceEntity);
                } else if (target.hasClass('disable-controller-service')) {
                    nfControllerService.disable(serviceTable, controllerServiceEntity);
                } else if (target.hasClass('delete-controller-service')) {
                    nfControllerService.promptToDeleteController(serviceTable, controllerServiceEntity);
                } else if (target.hasClass('view-state-controller-service')) {
                    nfComponentState.showState(controllerServiceEntity, controllerServiceEntity.state === 'DISABLED');
                } else if (target.hasClass('change-version-controller-service')) {
                    nfComponentVersion.promptForVersionChange(controllerServiceEntity);
                } else if (target.hasClass('edit-access-policies')) {
                    // show the policies for this service
                    nfPolicyManagement.showControllerServicePolicy(controllerServiceEntity);

                    // close the settings dialog
                    $('#shell-close-button').click();
                } else if (target.hasClass('go-to-controller-service')) {
                    // load the parent group of the selected service
                    nfProcessGroup.enterGroup(controllerServiceEntity.component.parentGroupId);

                    // open/select the specific service
                    $.Deferred(function (deferred) {
                        if ($('#process-group-configuration').is(':visible')) {
                            nfProcessGroupConfiguration.loadConfiguration(controllerServiceEntity.component.parentGroupId).done(function () {
                                deferred.resolve();
                            });
                        } else {
                            nfProcessGroupConfiguration.showConfiguration(controllerServiceEntity.component.parentGroupId).done(function () {
                                deferred.resolve();
                            });
                        }
                    }).done(function () {
                        nfProcessGroupConfiguration.selectControllerService(controllerServiceEntity.id);
                    });
                }
            } else if (controllerServicesGrid.getColumns()[args.cell].id === 'moreDetails') {
                if (target.hasClass('controller-service-usage')) {
                     // close the settings dialog
                     $('#shell-close-button').click();

                     // open the documentation for this controller service
                    nfShell.showPage('../nifi-docs/documentation?' + $.param({
                            select: controllerServiceEntity.component.type,
                            group: controllerServiceEntity.component.bundle.group,
                            artifact: controllerServiceEntity.component.bundle.artifact,
                            version: controllerServiceEntity.component.bundle.version
                    })).done(function() {
                         if (nfCommon.isDefinedAndNotNull(controllerServiceEntity.component.parentGroupId)) {
                             var groupId;
                             var processGroup = nfProcessGroup.get(controllerServiceEntity.component.parentGroupId);
                             if (nfCommon.isDefinedAndNotNull(processGroup)) {
                                 groupId = processGroup.id;
                             } else {
                                 groupId = nfCanvasUtils.getGroupId();
                             }

                             // reload the corresponding group
                             nfProcessGroupConfiguration.showConfiguration(groupId);
                         } else {
                             showSettings();
                         }
                    });
                }
            }
        });

        // wire up the dataview to the grid
        controllerServicesData.onRowCountChanged.subscribe(function (e, args) {
            controllerServicesGrid.updateRowCount();
            controllerServicesGrid.render();
        });
        controllerServicesData.onRowsChanged.subscribe(function (e, args) {
            controllerServicesGrid.invalidateRows(args.rows);
            controllerServicesGrid.render();
        });
        controllerServicesData.syncGridSelection(controllerServicesGrid, true);

        // hold onto an instance of the grid
        serviceTable.data('gridInstance', controllerServicesGrid).on('mouseenter', 'div.slick-cell', function (e) {
            var errorIcon = $(this).find('div.has-errors');
            if (errorIcon.length && !errorIcon.data('qtip')) {
                var serviceId = $(this).find('span.row-id').text();

                // get the service item
                var controllerServiceEntity = controllerServicesData.getItemById(serviceId);

                // format the errors
                var tooltip = nfCommon.formatUnorderedList(controllerServiceEntity.component.validationErrors);

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
                var controllerServiceEntity = controllerServicesData.getItemById(taskId);

                // format the tooltip
                var bulletins = nfCommon.getFormattedBulletins(controllerServiceEntity.bulletins);
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
     * Loads the controller services.
     *
     * @param {string} controllerServicesUri
     * @param {jQuery} serviceTable
     */
    var loadControllerServices = function (controllerServicesUri, serviceTable) {
        return $.ajax({
            type: 'GET',
            url: controllerServicesUri,
            dataType: 'json'
        }).done(function (response) {
            var services = [];
            $.each(response.controllerServices, function (_, service) {
                services.push($.extend({
                    type: 'ControllerService',
                    bulletins: []
                }, service));
            });

            nfCommon.cleanUpTooltips(serviceTable, 'div.has-errors');
            nfCommon.cleanUpTooltips(serviceTable, 'div.has-bulletins');

            var controllerServicesGrid = serviceTable.data('gridInstance');
            var controllerServicesData = controllerServicesGrid.getData();

            // update the controller services
            controllerServicesData.setItems(services);
            controllerServicesData.reSort();
            controllerServicesGrid.invalidate();
            controllerServicesGrid.render();
        });
    };

    return {
        /**
         * Initializes the status page.
         *
         * @param {jQuery} serviceTable
         * @param {function} showSettings
         */
        init: function (serviceTable, showSettings) {
            if (!initialized) {
                // initialize the new controller service dialog
                initNewControllerServiceDialog();

                // don't run this again
                initialized = true;
            }

            // initialize the controller service table
            initControllerServices(serviceTable, showSettings);
        },

        /**
         * Prompts for a new controller service.
         *
         * @param {string} controllerServicesUri
         * @param {jQuery} serviceTable
         */
        promptNewControllerService: function (controllerServicesUri, serviceTable) {
            // get the grid reference
            var grid = $('#controller-service-types-table').data('gridInstance');
            var dataview = grid.getData();

            // update the keyhandler
            $('#controller-service-type-filter').off('keyup').on('keyup', function (e) {
                var code = e.keyCode ? e.keyCode : e.which;
                if (code === $.ui.keyCode.ENTER) {
                    var selected = grid.getSelectedRows();

                    if (selected.length > 0) {
                        // grid configured with multi-select = false
                        var item = grid.getDataItem(selected[0]);
                        if (isSelectable(item)) {
                            addSelectedControllerService(controllerServicesUri, serviceTable);
                        }
                    }
                } else {
                    applyControllerServiceTypeFilter();
                }
            });

            // update the button model and show the dialog
            $('#new-controller-service-dialog').modal('setButtonModel', [{
                buttonText: 'Add',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                disabled: function () {
                    var selected = grid.getSelectedRows();

                    if (selected.length > 0) {
                        // grid configured with multi-select = false
                        var item = grid.getDataItem(selected[0]);
                        return isSelectable(item) === false;
                    } else {
                        return dataview.getLength() === 0;
                    }
                },
                handler: {
                    click: function () {
                        addSelectedControllerService(controllerServicesUri, serviceTable);
                    }
                }
            }, {
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
            }]).modal('show');

            // remove previous dbl click handler
            if (dblClick !== null) {
                grid.onDblClick.unsubscribe(dblClick);
            }

            // update the dbl click handler and subsrcibe
            dblClick = function(e, args) {
                var controllerServiceType = grid.getDataItem(args.row);

                if (isSelectable(controllerServiceType)) {
                    addControllerService(controllerServicesUri, serviceTable, controllerServiceType.type, controllerServiceType.bundle);
                }
            };
            grid.onDblClick.subscribe(dblClick);

            // reset the canvas size after the dialog is shown
            grid.resizeCanvas();

            // auto select the first row if possible
            if (dataview.getLength() > 0) {
                grid.setSelectedRows([0]);
            }

            // set the initial focus
            $('#controller-service-type-filter').focus();
        },

        /**
         * Update the size of the grid based on its container's current size.
         * 
         * @param {jQuery} serviceTable
         */
        resetTableSize: function (serviceTable) {
            var controllerServicesGrid = serviceTable.data('gridInstance');
            if (nfCommon.isDefinedAndNotNull(controllerServicesGrid)) {
                controllerServicesGrid.resizeCanvas();
            }
        },

        /**
         * Loads the settings.
         *
         * @param {string} controllerServicesUri
         * @param {jQuery} serviceTable
         */
        loadControllerServices: function (controllerServicesUri, serviceTable) {
            return loadControllerServices(controllerServicesUri, serviceTable);
        },

        /**
         * Sets the controller service and reporting task bulletins in their respective tables.
         *
         * @param {jQuery} serviceTable
         * @param {object} controllerServiceBulletins
         */
        setBulletins: function(serviceTable, controllerServiceBulletins) {
            // controller services
            var controllerServicesGrid = serviceTable.data('gridInstance');
            var controllerServicesData = controllerServicesGrid.getData();
            controllerServicesData.beginUpdate();

            // if there are some bulletins process them
            if (!nfCommon.isEmpty(controllerServiceBulletins)) {
                var controllerServiceBulletinsBySource = d3.nest()
                    .key(function(d) { return d.sourceId; })
                    .map(controllerServiceBulletins, d3.map);

                controllerServiceBulletinsBySource.forEach(function(sourceId, sourceBulletins) {
                    var controllerService = controllerServicesData.getItemById(sourceId);
                    if (nfCommon.isDefinedAndNotNull(controllerService)) {
                        controllerServicesData.updateItem(sourceId, $.extend(controllerService, {
                            bulletins: sourceBulletins
                        }));
                    }
                });
            } else {
                // if there are no bulletins clear all
                var controllerServices = controllerServicesData.getItems();
                $.each(controllerServices, function(_, controllerService) {
                    controllerServicesData.updateItem(controllerService.id, $.extend(controllerService, {
                        bulletins: []
                    }));
                });
            }
            controllerServicesData.endUpdate();
        }
    };
}));