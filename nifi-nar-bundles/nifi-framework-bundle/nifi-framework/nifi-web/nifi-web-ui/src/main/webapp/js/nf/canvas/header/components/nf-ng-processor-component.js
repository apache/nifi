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
                'nf.Client',
                'nf.Birdseye',
                'nf.Storage',
                'nf.Graph',
                'nf.CanvasUtils',
                'nf.ErrorHandler',
                'nf.FilteredDialogCommon',
                'nf.Dialog',
                'nf.Common'],
            function ($, Slick, nfClient, nfBirdseye, nfStorage, nfGraph, nfCanvasUtils, nfErrorHandler, nfFilteredDialogCommon, nfDialog, nfCommon) {
                return (nf.ng.ProcessorComponent = factory($, Slick, nfClient, nfBirdseye, nfStorage, nfGraph, nfCanvasUtils, nfErrorHandler, nfFilteredDialogCommon, nfDialog, nfCommon));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.ProcessorComponent =
            factory(require('jquery'),
                require('Slick'),
                require('nf.Client'),
                require('nf.Birdseye'),
                require('nf.Storage'),
                require('nf.Graph'),
                require('nf.CanvasUtils'),
                require('nf.ErrorHandler'),
                require('nf.FilteredDialogCommon'),
                require('nf.Dialog'),
                require('nf.Common')));
    } else {
        nf.ng.ProcessorComponent = factory(root.$,
            root.Slick,
            root.nf.Client,
            root.nf.Birdseye,
            root.nf.Storage,
            root.nf.Graph,
            root.nf.CanvasUtils,
            root.nf.ErrorHandler,
            root.nf.FilteredDialogCommon,
            root.nf.Dialog,
            root.nf.Common);
    }
}(this, function ($, Slick, nfClient, nfBirdseye, nfStorage, nfGraph, nfCanvasUtils, nfErrorHandler, nfFilteredDialogCommon, nfDialog, nfCommon) {
    'use strict';

    return function (serviceProvider) {
        'use strict';

        /**
         * Filters the processor type table.
         */
        var applyFilter = function () {
            // get the dataview
            var processorTypesGrid = $('#processor-types-table').data('gridInstance');

            // ensure the grid has been initialized
            if (nfCommon.isDefinedAndNotNull(processorTypesGrid)) {
                var processorTypesData = processorTypesGrid.getData();

                // update the search criteria
                processorTypesData.setFilterArgs({
                    searchString: getFilterText()
                });
                processorTypesData.refresh();

                // update the buttons to possibly trigger the disabled state
                $('#new-processor-dialog').modal('refreshButtons');

                // update the selection if possible
                if (processorTypesData.getLength() > 0) {
                    nfFilteredDialogCommon.choseFirstRow(processorTypesGrid);
                    // make the first row visible
                    processorTypesGrid.scrollRowToTop(0);
                }
            }
        };

        /**
         * Determines if the item matches the filter.
         *
         * @param {object} item     The item to filter.
         * @param {object} args     The filter criteria.
         * @returns {boolean}       Whether the item matches the filter.
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
         * Performs the filtering.
         *
         * @param {object} item     The item subject to filtering.
         * @param {object} args     Filter arguments.
         * @returns {Boolean}       Whether or not to include the item.
         */
        var filter = function (item, args) {
            // determine if the item matches the filter
            var matchesFilter = matchesRegex(item, args);

            // determine if the row matches the selected tags
            var matchesTags = true;
            if (matchesFilter) {
                var tagFilters = $('#processor-tag-cloud').tagcloud('getSelectedTags');
                var hasSelectedTags = tagFilters.length > 0;
                if (hasSelectedTags) {
                    matchesTags = matchesSelectedTags(tagFilters, item['tags']);
                }
            }

            // determine if the row matches the selected source group
            var matchesGroup = true;
            if (matchesFilter && matchesTags) {
                var bundleGroup = $('#processor-bundle-group-combo').combo('getSelectedOption');
                if (nfCommon.isDefinedAndNotNull(bundleGroup) && bundleGroup.value !== '') {
                    matchesGroup = (item.bundle.group === bundleGroup.value);
                }
            }

            // determine if this row should be visible
            var matches = matchesFilter && matchesTags && matchesGroup;

            // if this row is currently selected and its being filtered
            if (matches === false && $('#selected-processor-type').text() === item['type']) {
                // clear the selected row
                $('#processor-type-description').attr('title', '').text('');
                $('#processor-type-name').attr('title', '').text('');
                $('#processor-type-bundle').attr('title', '').text('');
                $('#selected-processor-name').text('');
                $('#selected-processor-type').text('').removeData('bundle');

                // clear the active cell the it can be reselected when its included
                var processTypesGrid = $('#processor-types-table').data('gridInstance');
                processTypesGrid.resetActiveCell();
            }

            return matches;
        };

        /**
         * Determines if the specified tags match all the tags selected by the user.
         *
         * @argument {string[]} tagFilters      The tag filters.
         * @argument {string} tags              The tags to test.
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
         * Get the text out of the filter field. If the filter field doesn't
         * have any text it will contain the text 'filter list' so this method
         * accounts for that.
         */
        var getFilterText = function () {
            return $('#processor-type-filter').val();
        };

        /**
         * Resets the filtered processor types.
         */
        var resetProcessorDialog = function () {
            // clear the selected tag cloud
            $('#processor-tag-cloud').tagcloud('clearSelectedTags');

            // reset the group combo
            $('#processor-bundle-group-combo').combo('setSelectedOption', {
                value: ''
            });

            // clear any filter strings
            $('#processor-type-filter').val('');

            // reapply the filter
            applyFilter();

            // clear the selected row
            $('#processor-type-description').attr('title', '').text('');
            $('#processor-type-name').attr('title', '').text('');
            $('#processor-type-bundle').attr('title', '').text('');
            $('#selected-processor-name').text('');
            $('#selected-processor-type').text('').removeData('bundle');

            // unselect any current selection
            var processTypesGrid = $('#processor-types-table').data('gridInstance');
            processTypesGrid.setSelectedRows([]);
            processTypesGrid.resetActiveCell();
        };

        /**
         * Create the processor and add to the graph.
         *
         * @argument {string} name              The processor name.
         * @argument {string} processorType     The processor type.
         * @argument {object} bundle            The processor bundle.
         * @argument {object} pt                The point that the processor was dropped.
         */
        var createProcessor = function (name, processorType, bundle, pt) {
            var processorEntity = {
                'revision': nfClient.getRevision({
                    'revision': {
                        'version': 0
                    }
                }),
                'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
                'component': {
                    'type': processorType,
                    'bundle': bundle,
                    'name': name,
                    'position': {
                        'x': pt.x,
                        'y': pt.y
                    }
                }
            };

            // create a new processor of the defined type
            $.ajax({
                type: 'POST',
                url: serviceProvider.headerCtrl.toolboxCtrl.config.urls.api + '/process-groups/' + encodeURIComponent(nfCanvasUtils.getGroupId()) + '/processors',
                data: JSON.stringify(processorEntity),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // add the processor to the graph
                nfGraph.add({
                    'processors': [response]
                }, {
                    'selectAll': true
                });

                // update component visibility
                nfGraph.updateVisibility();

                // update the birdseye
                nfBirdseye.refresh();
            }).fail(nfErrorHandler.handleAjaxError);
        };

        /**
         * Whether the specified item is selectable.
         *
         * @param item process type
         */
        var isSelectable = function (item) {
            return item.restricted === false || nfCommon.canAccessComponentRestrictions(item.explicitRestrictions);
        };

        function ProcessorComponent() {

            this.icon = 'icon icon-processor';

            this.hoverIcon = 'icon icon-processor-add';

            /**
             * The processor component's modal.
             */
            this.modal = {

                /**
                 * The processor component modal's filter.
                 */
                filter: {

                    /**
                     * Initialize the filter.
                     */
                    init: function () {
                        // initialize the processor type table
                        var processorTypesColumns = [
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

                        var processorTypesOptions = {
                            forceFitColumns: true,
                            enableTextSelectionOnCells: true,
                            enableCellNavigation: true,
                            enableColumnReorder: false,
                            autoEdit: false,
                            multiSelect: false,
                            rowHeight: 24
                        };

                        // initialize the dataview
                        var processorTypesData = new Slick.Data.DataView({
                            inlineFilters: false
                        });
                        processorTypesData.setItems([]);
                        processorTypesData.setFilterArgs({
                            searchString: getFilterText()
                        });
                        processorTypesData.setFilter(filter);

                        // initialize the sort
                        nfCommon.sortType({
                            columnId: 'type',
                            sortAsc: true
                        }, processorTypesData);

                        // initialize the grid
                        var processorTypesGrid = new Slick.Grid('#processor-types-table', processorTypesData, processorTypesColumns, processorTypesOptions);
                        processorTypesGrid.setSelectionModel(new Slick.RowSelectionModel());
                        processorTypesGrid.registerPlugin(new Slick.AutoTooltips());
                        processorTypesGrid.setSortColumn('type', true);
                        processorTypesGrid.onSort.subscribe(function (e, args) {
                            nfCommon.sortType({
                                columnId: args.sortCol.field,
                                sortAsc: args.sortAsc
                            }, processorTypesData);
                        });
                        processorTypesGrid.onSelectedRowsChanged.subscribe(function (e, args) {
                            if ($.isArray(args.rows) && args.rows.length === 1) {
                                var processorTypeIndex = args.rows[0];
                                var processorType = processorTypesGrid.getDataItem(processorTypeIndex);

                                // set the processor type description
                                if (nfCommon.isDefinedAndNotNull(processorType)) {
                                    if (nfCommon.isBlank(processorType.description)) {
                                        $('#processor-type-description')
                                            .attr('title', '')
                                            .html('<span class="unset">No description specified</span>');
                                    } else {
                                        $('#processor-type-description')
                                            .width($('#processor-description-container').innerWidth() - 1)
                                            .html(processorType.description)
                                            .ellipsis();
                                    }

                                    var bundle = nfCommon.formatBundle(processorType.bundle);
                                    var type = nfCommon.formatType(processorType);

                                    // populate the dom
                                    $('#processor-type-name').text(type).attr('title', type);
                                    $('#processor-type-bundle').text(bundle).attr('title', bundle);
                                    $('#selected-processor-name').text(processorType.label);
                                    $('#selected-processor-type').text(processorType.type).data('bundle', processorType.bundle);

                                    // refresh the buttons based on the current selection
                                    $('#new-processor-dialog').modal('refreshButtons');
                                }
                            }
                        });
                        processorTypesGrid.onViewportChanged.subscribe(function (e, args) {
                            nfCommon.cleanUpTooltips($('#processor-types-table'), 'div.view-usage-restriction');
                        });

                        // wire up the dataview to the grid
                        processorTypesData.onRowCountChanged.subscribe(function (e, args) {
                            processorTypesGrid.updateRowCount();
                            processorTypesGrid.render();

                            // update the total number of displayed processors
                            $('#displayed-processor-types').text(args.current);
                        });
                        processorTypesData.onRowsChanged.subscribe(function (e, args) {
                            processorTypesGrid.invalidateRows(args.rows);
                            processorTypesGrid.render();
                        });
                        processorTypesData.syncGridSelection(processorTypesGrid, false);

                        // hold onto an instance of the grid
                        $('#processor-types-table').data('gridInstance', processorTypesGrid).on('mouseenter', 'div.slick-cell', function (e) {
                            var usageRestriction = $(this).find('div.view-usage-restriction');
                            if (usageRestriction.length && !usageRestriction.data('qtip')) {
                                var rowId = $(this).find('span.row-id').text();

                                // get the status item
                                var item = processorTypesData.getItemById(rowId);

                                // show the tooltip
                                if (item.restricted === true) {
                                    var restrictionTip = $('<div></div>');

                                    if (nfCommon.isBlank(item.usageRestriction)) {
                                        restrictionTip.append($('<p style="margin-bottom: 3px;"></p>').text('Requires the following permissions:'));
                                    } else {
                                        restrictionTip.append($('<p style="margin-bottom: 3px;"></p>').text(item.usageRestriction + ' Requires the following permissions:'));
                                    }

                                    var restrictions = [];
                                    if (nfCommon.isDefinedAndNotNull(item.explicitRestrictions)) {
                                        $.each(item.explicitRestrictions, function (_, explicitRestriction) {
                                            var requiredPermission = explicitRestriction.requiredPermission;
                                            restrictions.push("'" + requiredPermission.label + "' - " + nfCommon.escapeHtml(explicitRestriction.explanation));
                                        });
                                    } else {
                                        restrictions.push('Access to restricted components regardless of restrictions.');
                                    }
                                    restrictionTip.append(nfCommon.formatUnorderedList(restrictions));

                                    usageRestriction.qtip($.extend({}, nfCommon.config.tooltipConfig, {
                                        content: restrictionTip,
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

                        var generalRestriction = nfCommon.getPolicyTypeListing('restricted-components');

                        // load the available processor types, this select is shown in the
                        // new processor dialog when a processor is dragged onto the screen
                        $.ajax({
                            type: 'GET',
                            url: serviceProvider.headerCtrl.toolboxCtrl.config.urls.processorTypes,
                            dataType: 'json'
                        }).done(function (response) {
                            var tags = [];
                            var groups = d3.set();
                            var restrictedUsage = d3.map();
                            var requiredPermissions = d3.map();

                            // begin the update
                            processorTypesData.beginUpdate();

                            // go through each processor type
                            $.each(response.processorTypes, function (i, documentedType) {
                                var type = documentedType.type;

                                if (documentedType.restricted === true) {
                                    if (nfCommon.isDefinedAndNotNull(documentedType.explicitRestrictions)) {
                                        $.each(documentedType.explicitRestrictions, function (_, explicitRestriction) {
                                            var requiredPermission = explicitRestriction.requiredPermission;

                                            // update required permissions
                                            if (!requiredPermissions.has(requiredPermission.id)) {
                                                requiredPermissions.set(requiredPermission.id, requiredPermission.label);
                                            }

                                            // update component restrictions
                                            if (!restrictedUsage.has(requiredPermission.id)) {
                                                restrictedUsage.set(requiredPermission.id, []);
                                            }

                                            restrictedUsage.get(requiredPermission.id).push({
                                                type: nfCommon.formatType(documentedType),
                                                bundle: nfCommon.formatBundle(documentedType.bundle),
                                                explanation: nfCommon.escapeHtml(explicitRestriction.explanation)
                                            })
                                        });
                                    } else {
                                        // update required permissions
                                        if (!requiredPermissions.has(generalRestriction.value)) {
                                            requiredPermissions.set(generalRestriction.value, generalRestriction.text);
                                        }

                                        // update component restrictions
                                        if (!restrictedUsage.has(generalRestriction.value)) {
                                            restrictedUsage.set(generalRestriction.value, []);
                                        }

                                        restrictedUsage.get(generalRestriction.value).push({
                                            type: nfCommon.formatType(documentedType),
                                            bundle: nfCommon.formatBundle(documentedType.bundle),
                                            explanation: nfCommon.escapeHtml(documentedType.usageRestriction)
                                        });
                                    }
                                }

                                // record the group
                                groups.add(documentedType.bundle.group);

                                // create the row for the processor type
                                processorTypesData.addItem({
                                    id: i,
                                    label: nfCommon.substringAfterLast(type, '.'),
                                    type: type,
                                    bundle: documentedType.bundle,
                                    description: nfCommon.escapeHtml(documentedType.description),
                                    restricted:  documentedType.restricted,
                                    usageRestriction: nfCommon.escapeHtml(documentedType.usageRestriction),
                                    explicitRestrictions: documentedType.explicitRestrictions,
                                    tags: documentedType.tags.join(', ')
                                });

                                // count the frequency of each tag for this type
                                $.each(documentedType.tags, function (i, tag) {
                                    tags.push(tag.toLowerCase());
                                });
                            });

                            // end the update
                            processorTypesData.endUpdate();

                            // resort
                            processorTypesData.reSort();
                            processorTypesGrid.invalidate();

                            // set the component restrictions and the corresponding required permissions
                            nfCanvasUtils.addComponentRestrictions(restrictedUsage, requiredPermissions);

                            // set the total number of processors
                            $('#total-processor-types, #displayed-processor-types').text(response.processorTypes.length);

                            // create the tag cloud
                            $('#processor-tag-cloud').tagcloud({
                                tags: tags,
                                select: applyFilter,
                                remove: applyFilter
                            });

                            // build the combo options
                            var options = [{
                                text: 'all groups',
                                value: ''
                            }];
                            groups.each(function (group) {
                                options.push({
                                    text: group,
                                    value: group
                                });
                            });

                            // initialize the bundle group combo
                            $('#processor-bundle-group-combo').combo({
                                options: options,
                                select: applyFilter
                            });
                        }).fail(nfErrorHandler.handleAjaxError);
                    }
                },

                /**
                 * Gets the modal element.
                 *
                 * @returns {*|jQuery|HTMLElement}
                 */
                getElement: function () {
                    return $('#new-processor-dialog');
                },

                /**
                 * Initialize the modal.
                 */
                init: function () {
                    this.filter.init();

                    // configure the new processor dialog
                    this.getElement().modal({
                        scrollableContentStyle: 'scrollable',
                        headerText: 'Add Processor',
                        handler: {
                            resize: function () {
                                $('#processor-type-description')
                                    .width($('#processor-description-container').innerWidth() - 1)
                                    .text($('#processor-type-description').attr('title'))
                                    .ellipsis();
                            }
                        }
                    });
                },

                /**
                 * Updates the modal config.
                 *
                 * @param {string} name             The name of the property to update.
                 * @param {object|array} config     The config for the `name`.
                 */
                update: function (name, config) {
                    this.getElement().modal(name, config);
                },

                /**
                 * Show the modal
                 */
                show: function () {
                    this.getElement().modal('show');
                },

                /**
                 * Hide the modal
                 */
                hide: function () {
                    this.getElement().modal('hide');
                }
            };
        }

        ProcessorComponent.prototype = {
            constructor: ProcessorComponent,

            /**
             * Gets the component.
             *
             * @returns {*|jQuery|HTMLElement}
             */
            getElement: function () {
                return $('#processor-component');
            },

            /**
             * Enable the component.
             */
            enabled: function () {
                this.getElement().attr('disabled', false);
            },

            /**
             * Disable the component.
             */
            disabled: function () {
                this.getElement().attr('disabled', true);
            },

            /**
             * Handler function for when component is dropped on the canvas.
             *
             * @argument {object} pt        The point that the component was dropped
             */
            dropHandler: function (pt) {
                this.promptForProcessorType(pt);
            },

            /**
             * The drag icon for the toolbox component.
             *
             * @param event
             * @returns {*|jQuery|HTMLElement}
             */
            dragIcon: function (event) {
                return $('<div class="icon icon-processor-add"></div>');
            },

            /**
             * Prompts the user to select the type of new processor to create.
             *
             * @argument {object} pt        The point that the processor was dropped
             */
            promptForProcessorType: function (pt) {
                var processorComponent = this;

                // handles adding the selected processor at the specified point
                var addProcessor = function () {
                    // get the type of processor currently selected
                    var name = $('#selected-processor-name').text();
                    var processorType = $('#selected-processor-type').text();
                    var bundle = $('#selected-processor-type').data('bundle');

                    // ensure something was selected
                    if (name === '' || processorType === '') {
                        nfDialog.showOkDialog({
                            headerText: 'Add Processor',
                            dialogContent: 'The type of processor to create must be selected.'
                        });
                    } else {
                        // create the new processor
                        createProcessor(name, processorType, bundle, pt);
                    }

                    // hide the dialog
                    processorComponent.modal.hide();
                };

                // get the grid reference
                var grid = $('#processor-types-table').data('gridInstance');
                var dataview = grid.getData();

                // add the processor when its double clicked in the table
                var gridDoubleClick = function (e, args) {
                    var processorType = grid.getDataItem(args.row);

                    if (isSelectable(processorType)) {
                        $('#selected-processor-name').text(processorType.label);
                        $('#selected-processor-type').text(processorType.type).data('bundle', processorType.bundle);

                        addProcessor();
                    }
                };

                // register a handler for double click events
                grid.onDblClick.subscribe(gridDoubleClick);

                // update the button model
                this.modal.update('setButtonModel', [{
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
                        click: addProcessor
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
                                $('#new-processor-dialog').modal('hide');
                            }
                        }
                    }]);

                // set a new handler for closing the the dialog
                this.modal.update('setCloseHandler', function () {
                    // remove the handler
                    grid.onDblClick.unsubscribe(gridDoubleClick);

                    // clear the current filters
                    resetProcessorDialog();
                });

                // show the dialog
                this.modal.show();

                var navigationKeys = [$.ui.keyCode.UP, $.ui.keyCode.PAGE_UP, $.ui.keyCode.DOWN, $.ui.keyCode.PAGE_DOWN];

                // setup the filter
                $('#processor-type-filter').off('keyup').on('keyup', function (e) {
                    var code = e.keyCode ? e.keyCode : e.which;

                    // ignore navigation keys
                    if ($.inArray(code, navigationKeys) !== -1) {
                        return;
                    }

                    if (code === $.ui.keyCode.ENTER) {
                        var selected = grid.getSelectedRows();

                        if (selected.length > 0) {
                            // grid configured with multi-select = false
                            var item = grid.getDataItem(selected[0]);
                            if (isSelectable(item)) {
                                addProcessor();
                            }
                        }
                    } else {
                        applyFilter();
                    }
                });

                // setup row navigation
                nfFilteredDialogCommon.addKeydownListener('#processor-type-filter', grid, dataview);

                // adjust the grid canvas now that its been rendered
                grid.resizeCanvas();

                // auto select the first row if possible
                if (dataview.getLength() > 0) {
                    nfFilteredDialogCommon.choseFirstRow(grid);
                }

                // set the initial focus
                $('#processor-type-filter').focus()
            }
        };

        var processorComponent = new ProcessorComponent();
        return processorComponent;
    };
}));