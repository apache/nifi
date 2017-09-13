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

/**
 * Opens the variable registry for a given Process Group.
 */
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'd3',
                'Slick',
                'nf.Canvas',
                'nf.CanvasUtils',
                'nf.ErrorHandler',
                'nf.Dialog',
                'nf.Client',
                'nf.Common',
                'nf.ng.Bridge',
                'nf.Processor',
                'nf.ProcessGroup',
                'nf.ProcessGroupConfiguration'],
            function ($, d3, Slick, nfCanvas, nfCanvasUtils, nfErrorHandler, nfDialog, nfClient, nfCommon, nfNgBridge, nfProcessor, nfProcessGroup, nfProcessGroupConfiguration) {
                return (nf.ComponentState = factory($, d3, Slick, nfCanvas, nfCanvasUtils, nfErrorHandler, nfDialog, nfClient, nfCommon, nfNgBridge, nfProcessor, nfProcessGroup, nfProcessGroupConfiguration));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ComponentState =
            factory(require('jquery'),
                require('d3'),
                require('Slick'),
                require('nf.Canvas'),
                require('nf.CanvasUtils'),
                require('nf.ErrorHandler'),
                require('nf.Dialog'),
                require('nf.Client'),
                require('nf.Common'),
                require('nf.ng.Bridge'),
                require('nf.Processor'),
                require('nf.ProcessGroup'),
                require('nf.ProcessGroupConfiguration')));
    } else {
        nf.VariableRegistry = factory(root.$,
            root.d3,
            root.Slick,
            root.nf.Canvas,
            root.nf.CanvasUtils,
            root.nf.ErrorHandler,
            root.nf.Dialog,
            root.nf.Client,
            root.nf.Common,
            root.nf.ng.Bridge,
            root.nf.Processor,
            root.nf.ProcessGroup,
            root.nf.ProcessGroupConfiguration);
    }
}(this, function ($, d3, Slick, nfCanvas, nfCanvasUtils, nfErrorHandler, nfDialog, nfClient, nfCommon, nfNgBridge, nfProcessor, nfProcessGroup, nfProcessGroupConfiguration) {
    'use strict';

    // text editor
    var textEditor = function (args) {
        var scope = this;
        var initialValue = '';
        var previousValue;
        var wrapper;
        var isEmpty;
        var input;

        this.init = function () {
            var container = $('body');

            // record the previous value
            previousValue = args.item[args.column.field];

            // create the wrapper
            wrapper = $('<div></div>').addClass('slickgrid-editor').css({
                'z-index': 100000,
                'position': 'absolute',
                'border-radius': '2px',
                'box-shadow': 'rgba(0, 0, 0, 0.247059) 0px 2px 5px',
                'background-color': 'rgb(255, 255, 255)',
                'overflow': 'hidden',
                'padding': '10px 20px',
                'cursor': 'move',
                'transform': 'translate3d(0px, 0px, 0px)'
            }).appendTo(container);

            // create the input field
            input = $('<textarea hidefocus rows="5"/>').css({
                'height': '80px',
                'width': args.position.width + 'px',
                'min-width': '212px',
                'margin-bottom': '5px',
                'margin-top': '10px',
                'white-space': 'pre'
            }).tab().on('keydown', scope.handleKeyDown).appendTo(wrapper);

            wrapper.draggable({
                cancel: '.button, textarea, .nf-checkbox',
                containment: 'parent'
            });

            // create the button panel
            var stringCheckPanel = $('<div class="string-check-container">');
            stringCheckPanel.appendTo(wrapper);

            // build the custom checkbox
            isEmpty = $('<div class="nf-checkbox string-check"/>').appendTo(stringCheckPanel);
            $('<span class="string-check-label nf-checkbox-label">&nbsp;Set empty string</span>').appendTo(stringCheckPanel);

            var ok = $('<div class="button">Ok</div>').css({
                'color': '#fff',
                'background': '#728E9B'
            }).hover(
                function () {
                    $(this).css('background', '#004849');
                }, function () {
                    $(this).css('background', '#728E9B');
                }).on('click', scope.save);
            var cancel = $('<div class="secondary-button">Cancel</div>').css({
                'color': '#004849',
                'background': '#E3E8EB'
            }).hover(
                function () {
                    $(this).css('background', '#C7D2D7');
                }, function () {
                    $(this).css('background', '#E3E8EB');
                }).on('click', scope.cancel);
            $('<div></div>').css({
                'position': 'relative',
                'top': '10px',
                'left': '20px',
                'width': '212px',
                'clear': 'both',
                'float': 'right'
            }).append(ok).append(cancel).append('<div class="clear"></div>').appendTo(wrapper);

            // position and focus
            scope.position(args.position);
            input.focus().select();
        };

        this.handleKeyDown = function (e) {
            if (e.which === $.ui.keyCode.ENTER && !e.shiftKey) {
                scope.save();
            } else if (e.which === $.ui.keyCode.ESCAPE) {
                scope.cancel();

                // prevent further propagation or escape press and prevent default behavior
                e.stopImmediatePropagation();
                e.preventDefault();
            }
        };

        this.save = function () {
            args.commitChanges();
        };

        this.cancel = function () {
            input.val(initialValue);
            args.cancelChanges();
        };

        this.hide = function () {
            wrapper.hide();
        };

        this.show = function () {
            wrapper.show();
        };

        this.position = function (position) {
            wrapper.css({
                'top': position.top - 27,
                'left': position.left - 20
            });
        };

        this.destroy = function () {
            wrapper.remove();
        };

        this.focus = function () {
            input.focus();
        };

        this.loadValue = function (item) {
            var isEmptyChecked = false;

            // determine the value to use when populating the text field
            if (nfCommon.isDefinedAndNotNull(item[args.column.field])) {
                initialValue = item[args.column.field];
                isEmptyChecked = initialValue === '';
            }

            // determine if its an empty string
            var checkboxStyle = isEmptyChecked ? 'checkbox-checked' : 'checkbox-unchecked';
            isEmpty.addClass(checkboxStyle);

            input.val(initialValue);
            input.select();
        };

        this.serializeValue = function () {
            // if the field has been cleared, set the value accordingly
            if (input.val() === '') {
                // if the user has checked the empty string checkbox, use emtpy string
                if (isEmpty.hasClass('checkbox-checked')) {
                    return '';
                } else {
                    return null;
                }
            } else {
                // if there is text specified, use that value
                return input.val();
            }
        };

        this.applyValue = function (item, state) {
            item[args.column.field] = state;
        };

        this.isValueChanged = function () {
            return scope.serializeValue() !== previousValue;
        };

        this.validate = function () {
            return {
                valid: true,
                msg: null
            };
        };

        // initialize the custom long text editor
        this.init();
    };

    /**
     * Shows the variable in a read only property detail winder.
     *
     * @param {object} variable
     * @param {slickgrid} variableGrid
     * @param {integer} row
     * @param {integer} cell
     */
    var showVariableValue = function (variable, variableGrid, row, cell) {
        var cellNode = $(variableGrid.getCellNode(row, cell));
        var offset = cellNode.offset();

        var wrapper = $('<div class="property-detail"></div>').css({
            'z-index': 1999,
            'position': 'absolute',
            'padding': '10px 20px',
            'overflow': 'hidden',
            'border-radius': '2px',
            'box-shadow': 'rgba(0, 0, 0, 0.247059) 0px 2px 5px',
            'background-color': 'rgb(255, 255, 255)',
            'cursor': 'move',
            'transform': 'translate3d(0px, 0px, 0px)',
            'top': offset.top - 26,
            'left': offset.left - 20
        }).draggable({
            containment: 'parent'
        }).appendTo('body');

        // create the input field
        $('<textarea hidefocus rows="5" readonly="readonly"/>').css({
            'height': '80px',
            'resize': 'both',
            'width': cellNode.width() + 'px',
            'margin': '10px 0px',
            'white-space': 'pre'
        }).text(variable.value).on('keydown', function (evt) {
            if (evt.which === $.ui.keyCode.ESCAPE) {
                cleanUp();

                evt.stopImmediatePropagation();
                evt.preventDefault();
            }
        }).appendTo(wrapper);

        var cleanUp = function () {
            wrapper.hide().remove();
        };

        // add an ok button that will remove the entire pop up
        var ok = $('<div class="button">Ok</div>').css({
            'position': 'relative',
            'top': '10px',
            'left': '20px'
        }).hover(
            function () {
                $(this).css('background', '#004849');
            }, function () {
                $(this).css('background', '#728E9B');
            }).on('click', function () {
            cleanUp();
        });

        $('<div></div>').append(ok).append('<div class="clear"></div>').appendTo(wrapper);
    };

    var gridOptions = {
        forceFitColumns: true,
        enableTextSelectionOnCells: true,
        enableCellNavigation: true,
        enableColumnReorder: false,
        editable: true,
        enableAddRow: false,
        autoEdit: false,
        multiSelect: false,
        rowHeight: 24
    };

    /**
     * Gets the scope label for the specified Process Group.
     *
     * @param {string} processGroupId
     * @returns {string} the label for the specified Process Group
     */
    var getScopeLabel = function (processGroupId) {
        // see if this listing is based off a selected process group
        var selection = nfCanvasUtils.getSelection();
        if (selection.empty() === false) {
            var selectedData = selection.datum();
            if (selectedData.id === processGroupId) {
                if (selectedData.permissions.canRead) {
                    return nfCommon.escapeHtml(selectedData.component.name);
                } else {
                    return nfCommon.escapeHtml(selectedData.id);
                }
            }
        }

        // there's either no selection or the variable is defined in an ancestor component
        var breadcrumbs = nfNgBridge.injector.get('breadcrumbsCtrl').getBreadcrumbs();

        var processGroupLabel = processGroupId;
        $.each(breadcrumbs, function (_, breadcrumbEntity) {
            if (breadcrumbEntity.id === processGroupId) {
                processGroupLabel = breadcrumbEntity.label;
                return false;
            }
        });

        return processGroupLabel;
    };

    /**
     * Initializes the variable table
     */
    var initVariableTable = function () {
        var variableTable = $('#variable-registry-table');

        var nameFormatter = function (row, cell, value, columnDef, dataContext) {
            return nfCommon.escapeHtml(value);
        };

        var valueFormatter = function (row, cell, value, columnDef, dataContext) {
            if (dataContext.isOverridden) {
                return '<div class="overridden" title="This value has been overridden by another variable in a descendant Process Group">' + nfCommon.escapeHtml(value) + '</div>';
            } else {
                if (value === '') {
                    return '<span class="table-cell blank">Empty string set</span>';
                } else if (value === null) {
                    return '<span class="unset">No value set</span>';
                } else {
                    return nfCommon.escapeHtml(value);
                }
            }
        };

        var scopeFormatter = function (row, cell, value, columnDef, dataContext) {
            if (nfCommon.isDefinedAndNotNull(value)) {
                return nfCommon.escapeHtml(getScopeLabel(value));
            } else {
                return 'Controller';
            }
        };

        var variableActionFormatter = function (row, cell, value, columnDef, dataContext) {
            var markup = '';

            if (dataContext.isEditable === true) {
                markup += '<div title="Delete" class="delete-variable pointer fa fa-trash" style="margin-top: 2px;" ></div>';
            } else {
                var currentProcessGroupId = $('#variable-registry-process-group-id').text();

                if (dataContext.processGroupId !== currentProcessGroupId) {
                    markup += '<div title="Go To" class="go-to-variable pointer fa fa-long-arrow-right" style="margin-top: 2px;" ></div>';
                }
            }

            return markup;
        };

        // define the column model for the controller services table
        var variableColumns = [
            {
                id: 'scope',
                name: 'Scope',
                field: 'processGroupId',
                formatter: scopeFormatter,
                sortable: true,
                resizable: true
            },
            {
                id: 'name',
                name: 'Name',
                field: 'name',
                formatter: nameFormatter,
                sortable: true,
                resizable: true
            },
            {
                id: 'value',
                name: 'Value',
                field: 'value',
                formatter: valueFormatter,
                sortable: true,
                resizable: true,
                cssClass: 'pointer'
            },
            {
                id: 'actions',
                name: '&nbsp;',
                resizable: false,
                formatter: variableActionFormatter,
                sortable: false,
                width: 45,
                maxWidth: 45
            }
        ];

        // initialize the dataview
        var variableData = new Slick.Data.DataView({
            inlineFilters: false
        });
        variableData.setFilterArgs({
            searchString: '',
            property: 'hidden'
        });
        variableData.setFilter(function (item, args) {
            return item.hidden === false;
        });
        variableData.getItemMetadata = function (index) {
            return {
                columns: {
                    value: {
                        editor: textEditor
                    }
                }
            };
        };

        // initialize the sort
        sortVariables({
            columnId: 'name',
            sortAsc: true
        }, variableData);

        // initialize the grid
        var variablesGrid = new Slick.Grid(variableTable, variableData, variableColumns, gridOptions);
        variablesGrid.setSelectionModel(new Slick.RowSelectionModel());
        variablesGrid.registerPlugin(new Slick.AutoTooltips());
        variablesGrid.setSortColumn('name', true);
        variablesGrid.onSort.subscribe(function (e, args) {
            sortVariables({
                columnId: args.sortCol.id,
                sortAsc: args.sortAsc
            }, variableData);
        });
        variablesGrid.onClick.subscribe(function (e, args) {
            // get the variable at this row
            var variable = variableData.getItem(args.row);

            if (variablesGrid.getColumns()[args.cell].id === 'value') {
                if (variable.isEditable === true) {
                    variablesGrid.gotoCell(args.row, args.cell, true);
                } else {
                    // ensure the row is selected
                    variablesGrid.setSelectedRows([args.row]);

                    // show the variable
                    showVariableValue(variable, variablesGrid, args.row, args.cell);
                }

                // prevents standard edit logic
                e.stopImmediatePropagation();
            } else if (variablesGrid.getColumns()[args.cell].id === 'actions') {
                var target = $(e.target);

                // determine the desired action
                if (target.hasClass('delete-variable')) {
                    // mark the property in question for removal and refresh the table
                    variableData.updateItem(variable.id, $.extend(variable, {
                        hidden: true
                    }));

                    // look if this variable that was just 'removed'
                    var variables = variableData.getItems();
                    $.each(variables, function (_, item) {
                        if (item.isOverridden === true && !isOverridden(variables, item)) {
                            variableData.updateItem(item.id, $.extend(item, {
                                isOverridden: false
                            }));
                        }
                    });

                    // reset the selection if necessary
                    var selectedRows = variablesGrid.getSelectedRows();
                    if (selectedRows.length === 0) {
                        variablesGrid.setSelectedRows([0]);
                    }

                    // prevents standard edit logic
                    e.stopImmediatePropagation();
                } else if (target.hasClass('go-to-variable')) {
                    // check if there are outstanding changes
                    handleOutstandingChanges().done(function () {
                        // go to the process group that this variable belongs to
                        var breadcrumbs = nfNgBridge.injector.get('breadcrumbsCtrl').getBreadcrumbs();
                        $.each(breadcrumbs, function (_, breadcrumbEntity) {
                            // find the breadcrumb for the process group of the variable
                            if (breadcrumbEntity.id === variable.processGroupId) {
                                // if that breadcrumb has a parent breadcrumb, navigate to the parent group and select the PG
                                if (nfCommon.isDefinedAndNotNull(breadcrumbEntity.parentBreadcrumb)) {
                                    nfCanvasUtils.showComponent(breadcrumbEntity.parentBreadcrumb.id, breadcrumbEntity.id).done(function () {
                                        setTimeout(function () {
                                            // open the variable dialog for the process group of this variable
                                            showVariables(variable.processGroupId, variable.name);
                                        }, 500);
                                    });
                                } else {
                                    nfCanvasUtils.getComponentByType('ProcessGroup').enterGroup(breadcrumbEntity.id).done(function () {
                                        setTimeout(function () {
                                            // open the variable dialog for the process group of this variable
                                            showVariables(variable.processGroupId, variable.name);
                                        }, 500);
                                    });
                                }

                                return false;
                            }
                        });
                    });
                }
            }
        });
        variablesGrid.onSelectedRowsChanged.subscribe(function (e, args) {
            if ($.isArray(args.rows) && args.rows.length === 1) {
                // show the affected components for the selected variable
                if (variablesGrid.getDataLength() > 0) {
                    var variableIndex = args.rows[0];
                    var variable = variablesGrid.getDataItem(variableIndex);

                    // update the details for this variable
                    $('#affected-components-context').removeClass('unset').text(variable.name);
                    populateAffectedComponents(variable.affectedComponents);
                }
            }
        });

        // wire up the dataview to the grid
        variableData.onRowCountChanged.subscribe(function (e, args) {
            variablesGrid.updateRowCount();
            variablesGrid.render();
        });
        variableData.onRowsChanged.subscribe(function (e, args) {
            variablesGrid.invalidateRows(args.rows);
            variablesGrid.render();
        });
        variableData.syncGridSelection(variablesGrid, true);

        // hold onto an instance of the grid
        variableTable.data('gridInstance', variablesGrid);
    };

    /**
     * Handles outstanding changes.
     *
     * @returns {deferred}
     */
    var handleOutstandingChanges = function () {
        var variableGrid = $('#variable-registry-table').data('gridInstance');
        if (nfCommon.isDefinedAndNotNull(variableGrid)) {
            // get the property grid to commit the current edit
            var editController = variableGrid.getEditController();
            editController.commitCurrentEdit();
        }

        return $.Deferred(function (deferred) {
            if ($('#variable-update-status').is(':visible')) {
                close();
                deferred.resolve();
            } else {
                var variables = marshalVariables();

                // if there are no variables there is nothing to save
                if ($.isEmptyObject(variables)) {
                    close();
                    deferred.resolve();
                } else {
                    // see if those changes should be saved
                    nfDialog.showYesNoDialog({
                        headerText: 'Variables',
                        dialogContent: 'Save changes before leaving variable configuration?',
                        noHandler: function () {
                            close();
                            deferred.resolve();
                        },
                        yesHandler: function () {
                            updateVariables().done(function () {
                                deferred.resolve();
                            }).fail(function () {
                                deferred.reject();
                            });
                        }
                    });
                }
            }

        }).promise();
    };

    /**
     * Sorts the specified data using the specified sort details.
     *
     * @param {object} sortDetails
     * @param {object} data
     */
    var sortVariables = function (sortDetails, data) {
        // defines a function for sorting
        var comparer = function (a, b) {
            if (sortDetails.columnId === 'scope') {
                var aScope = nfCommon.isDefinedAndNotNull(a.processGroupId) ? getScopeLabel(a.processGroupId) : '';
                var bScope = nfCommon.isDefinedAndNotNull(b.processGroupId) ? getScopeLabel(b.processGroupId) : '';
                return aScope === bScope ? 0 : aScope > bScope ? 1 : -1;
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
     * Sorts the specified entities based on the name.
     *
     * @param {object} a
     * @param {pbject} b
     * @returns {number}
     */
    var nameComparator = function (a, b) {
        return a.component.name.localeCompare(b.component.name);
    };

    /**
     * Renders the specified affected component.
     *
     * @param {object} affectedProcessorEntity
     * @param {jQuery} container
     */
    var renderAffectedProcessor = function (affectedProcessorEntity, container) {
        var affectedProcessorContainer = $('<li class="affected-component-container"></li>').appendTo(container);
        var affectedProcessor = affectedProcessorEntity.component;

        // processor state
        $('<div class="referencing-component-state"></div>').addClass(function () {
            if (nfCommon.isDefinedAndNotNull(affectedProcessor.state)) {
                var icon = $(this);

                var state = affectedProcessor.state.toLowerCase();
                if (state === 'stopped' && !nfCommon.isEmpty(affectedProcessor.validationErrors)) {
                    state = 'invalid';

                    // build the validation error listing
                    var list = nfCommon.formatUnorderedList(affectedProcessor.validationErrors);

                    // add tooltip for the warnings
                    icon.qtip($.extend({},
                        nfCanvasUtils.config.systemTooltipConfig,
                        {
                            content: list
                        }));
                }

                return state;
            } else {
                return '';
            }
        }).appendTo(affectedProcessorContainer);


        // processor name
        $('<span class="referencing-component-name link"></span>').text(affectedProcessor.name).on('click', function () {
            // check if there are outstanding changes
            handleOutstandingChanges().done(function () {
                // show the component in question
                nfCanvasUtils.showComponent(affectedProcessor.processGroupId, affectedProcessor.id);
            });
        }).appendTo(affectedProcessorContainer);

        // bulletin
        $('<div class="referencing-component-bulletins"></div>').addClass(affectedProcessor.id + '-affected-bulletins').appendTo(affectedProcessorContainer);

        // processor active threads
        $('<span class="referencing-component-active-thread-count"></span>').text(function () {
            if (nfCommon.isDefinedAndNotNull(affectedProcessor.activeThreadCount) && affectedProcessor.activeThreadCount > 0) {
                return '(' + affectedProcessor.activeThreadCount + ')';
            } else {
                return '';
            }
        }).appendTo(affectedProcessorContainer);
    };

    /**
     * Renders the specified affect controller service.
     *
     * @param {object} affectedControllerServiceEntity
     * @param {jQuery} container
     */
    var renderAffectedControllerService = function (affectedControllerServiceEntity, container) {
        var affectedControllerServiceContainer = $('<li class="affected-component-container"></li>').appendTo(container);
        var affectedControllerService = affectedControllerServiceEntity.component;

        // controller service state
        $('<div class="referencing-component-state"></div>').addClass(function () {
            if (nfCommon.isDefinedAndNotNull(affectedControllerService.state)) {
                var icon = $(this);

                var state = affectedControllerService.state === 'ENABLED' ? 'enabled' : 'disabled';
                if (state === 'disabled' && !nfCommon.isEmpty(affectedControllerService.validationErrors)) {
                    state = 'invalid';

                    // build the error listing
                    var list = nfCommon.formatUnorderedList(affectedControllerService.validationErrors);

                    // add tooltip for the warnings
                    icon.qtip($.extend({},
                        nfCanvasUtils.config.systemTooltipConfig,
                        {
                            content: list
                        }));
                }
                return state;
            } else {
                return '';
            }
        }).appendTo(affectedControllerServiceContainer);

        // bulletin
        $('<div class="referencing-component-bulletins"></div>').addClass(affectedControllerService.id + '-affected-bulletins').appendTo(affectedControllerServiceContainer);

        // controller service name
        $('<span class="link"></span>').text(affectedControllerService.name).on('click', function () {
            // check if there are outstanding changes
            handleOutstandingChanges().done(function () {
                // show the component in question
                nfProcessGroupConfiguration.showConfiguration(affectedControllerService.processGroupId).done(function () {
                    nfProcessGroupConfiguration.selectControllerService(affectedControllerService.id);
                });
            });
        }).appendTo(affectedControllerServiceContainer);
    };

    /**
     * Populates the affected components for the specified variable.
     *
     * @param {object} affectedComponents
     */
    var populateAffectedComponents = function (affectedComponents) {
        var affectedProcessors = [];
        var affectedControllerServices = [];
        var unauthorizedAffectedComponents = [];

        // clear the affected components from the previous selection
        var processorContainer = $('#variable-registry-affected-processors');
        nfCommon.cleanUpTooltips(processorContainer, 'div.referencing-component-state');
        nfCommon.cleanUpTooltips(processorContainer, 'div.referencing-component-bulletins');
        processorContainer.empty();

        var controllerServiceContainer = $('#variable-registry-affected-controller-services');
        nfCommon.cleanUpTooltips(controllerServiceContainer, 'div.referencing-component-state');
        nfCommon.cleanUpTooltips(controllerServiceContainer, 'div.referencing-component-bulletins');
        controllerServiceContainer.empty();

        var unauthorizedComponentsContainer = $('#variable-registry-affected-unauthorized-components').empty();

        // affected component will be undefined when a new variable is added
        if (nfCommon.isUndefined(affectedComponents)) {
            $('<li class="affected-component-container"><span class="unset">Pending Apply</span></li>').appendTo(processorContainer);
            $('<li class="affected-component-container"><span class="unset">Pending Apply</span></li>').appendTo(controllerServiceContainer);
            $('<li class="affected-component-container"><span class="unset">Pending Apply</span></li>').appendTo(unauthorizedComponentsContainer);
        } else {
            var referencingComponentsForBulletinRetrieval = [];

            // bin the affected components according to their type
            $.each(affectedComponents, function (_, affectedComponentEntity) {
                if (affectedComponentEntity.permissions.canRead === true && affectedComponentEntity.permissions.canWrite === true) {
                    referencingComponentsForBulletinRetrieval.push(affectedComponentEntity.id);

                    if (affectedComponentEntity.component.referenceType === 'PROCESSOR') {
                        affectedProcessors.push(affectedComponentEntity);
                    } else {
                        affectedControllerServices.push(affectedComponentEntity);
                    }
                } else {
                    // if we're unauthorized only because the user is lacking write permissions, we can still query for bulletins
                    if (affectedComponentEntity.permissions.canRead === true) {
                        referencingComponentsForBulletinRetrieval.push(affectedComponentEntity.id);
                    }

                    unauthorizedAffectedComponents.push(affectedComponentEntity);
                }
            });

            if (affectedProcessors.length === 0) {
                $('<li class="affected-component-container"><span class="unset">None</span></li>').appendTo(processorContainer);
            } else {
                // sort the affected processors
                affectedProcessors.sort(nameComparator);

                // render each and register a click handler
                $.each(affectedProcessors, function (_, affectedProcessorEntity) {
                    renderAffectedProcessor(affectedProcessorEntity, processorContainer);
                });
            }

            if (affectedControllerServices.length === 0) {
                $('<li class="affected-component-container"><span class="unset">None</span></li>').appendTo(controllerServiceContainer);
            } else {
                // sort the affected controller services
                affectedControllerServices.sort(nameComparator);

                // render each and register a click handler
                $.each(affectedControllerServices, function (_, affectedControllerServiceEntity) {
                    renderAffectedControllerService(affectedControllerServiceEntity, controllerServiceContainer);
                });
            }

            if (unauthorizedAffectedComponents.length === 0) {
                $('<li class="affected-component-container"><span class="unset">None</span></li>').appendTo(unauthorizedComponentsContainer);
            } else {
                // sort the unauthorized affected components
                unauthorizedAffectedComponents.sort(function (a, b) {
                    if (a.permissions.canRead === true && b.permissions.canRead === true) {
                        // processors before controller services
                        var sortVal = a.component.referenceType === b.component.referenceType ? 0 : a.component.referenceType > b.component.referenceType ? -1 : 1;

                        // if a and b are the same type, then sort by name
                        if (sortVal === 0) {
                            sortVal = a.component.name === b.component.name ? 0 : a.component.name > b.component.name ? 1 : -1;
                        }

                        return sortVal;
                    } else {

                        // if lacking read and write perms on both, sort by id
                        if (a.permissions.canRead === false && b.permissions.canRead === false) {
                            return a.id > b.id ? 1 : -1;
                        } else {
                            // if only one has read perms, then let it come first
                            if (a.permissions.canRead === true) {
                                return -1;
                            } else {
                                return 1;
                            }
                        }
                    }
                });

                $.each(unauthorizedAffectedComponents, function (_, unauthorizedAffectedComponentEntity) {
                    if (unauthorizedAffectedComponentEntity.permissions.canRead === true) {
                        if (unauthorizedAffectedComponentEntity.component.referenceType === 'PROCESSOR') {
                            renderAffectedProcessor(unauthorizedAffectedComponentEntity, unauthorizedComponentsContainer);
                        } else {
                            renderAffectedControllerService(unauthorizedAffectedComponentEntity, unauthorizedComponentsContainer);
                        }
                    } else {
                        var affectedUnauthorizedComponentContainer = $('<li class="affected-component-container"></li>').appendTo(unauthorizedComponentsContainer);
                        $('<span class="unset"></span>').text(unauthorizedAffectedComponentEntity.id).appendTo(affectedUnauthorizedComponentContainer);
                    }
                });
            }

            // query for the bulletins
            if (referencingComponentsForBulletinRetrieval.length > 0) {
                nfCanvasUtils.queryBulletins(referencingComponentsForBulletinRetrieval).done(function (response) {
                    var bulletins = response.bulletinBoard.bulletins;

                    var bulletinsBySource = d3.nest()
                        .key(function (d) {
                            return d.sourceId;
                        })
                        .map(bulletins, d3.map);

                    bulletinsBySource.forEach(function (sourceId, sourceBulletins) {
                        $('div.' + sourceId + '-affected-bulletins').each(function () {
                            var bulletinIcon = $(this);

                            // if there are bulletins update them
                            if (sourceBulletins.length > 0) {
                                // format the new bulletins
                                var formattedBulletins = nfCommon.getFormattedBulletins(sourceBulletins);

                                var list = nfCommon.formatUnorderedList(formattedBulletins);

                                // update existing tooltip or initialize a new one if appropriate
                                bulletinIcon.addClass('has-bulletins').show().qtip($.extend({},
                                    nfCanvasUtils.config.systemTooltipConfig,
                                    {
                                        content: list
                                    }));
                            }
                        });
                    });
                });
            }
        }
    };

    /**
     * Shows the variable for the specified processGroupId.
     *
     * @param {string} processGroupId
     * @param {string} variableToSelect to select
     */
    var showVariables = function (processGroupId, variableToSelect) {
        return $.ajax({
            type: 'GET',
            url: '../nifi-api/process-groups/' + encodeURIComponent(processGroupId) + '/variable-registry',
            dataType: 'json'
        }).done(function (response) {
            $('#process-group-variable-registry').text(getScopeLabel(processGroupId));
            $('#variable-registry-process-group-id').text(processGroupId).data('revision', response.processGroupRevision);

            // load the variables
            loadVariables(response.variableRegistry, variableToSelect);

            // show the dialog
            $('#variable-registry-dialog').modal('show');
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Returns whether the currentVariable is overridden in the specified variables.
     *
     * @param {array} variables
     * @param {object} currentVariable
     * @returns {boolean} whether currentVariable is overridden
     */
    var isOverridden = function(variables, currentVariable) {
        // identify any variables conflicting with the current variable
        var conflictingVariables = [];
        $.each(variables, function (_, variable) {
            if (currentVariable.name === variable.name && variable.hidden === false) {
                conflictingVariables.push(variable);
            }
        });

        var isOverridden = false;

        // if there are any variables conflicting
        if (conflictingVariables.length > 1) {
            var ancestry = [];

            // get the breadcrumbs to walk the ancestry
            var breadcrumbs = nfNgBridge.injector.get('breadcrumbsCtrl').getBreadcrumbs();
            $.each(breadcrumbs, function (_, breadcrumbEntity) {
                ancestry.push(breadcrumbEntity.id);
            });

            // check to see if the current process group is not part of the ancestry
            var currentProcessGroupId = $('#variable-registry-process-group-id').text();
            if (ancestry.indexOf(currentProcessGroupId) === -1) {
                ancestry.push(currentProcessGroupId);
            }

            // go through each group in the ancestry
            $.each(ancestry, function (_, processGroupId) {
                // for each breadcrumb go through each variable
                for (var i = 0; i < conflictingVariables.length; i++) {

                    // if this breadcrumb represents the process group for the conflicting variable
                    if (processGroupId === conflictingVariables[i].processGroupId) {

                        // if this conflicting variable is the current variable, mark as overridden as we
                        // know there is at least one more conflicting variable
                        if (currentVariable === conflictingVariables[i]) {
                            isOverridden = true;
                        }

                        conflictingVariables.splice(i, 1);

                        // if we are left with only a single variable break out of the breadcrumb iteration
                        if (conflictingVariables.length === 1) {
                            return false;
                        }
                    }
                }
            });
        }

        return isOverridden;
    };

    /**
     * Returns whether the specified variable is editable.
     *
     * @param {object} variable
     * @returns {boolean} if variable is editable
     */
    var isEditable = function (variable) {
        // if the variable can be written based on the perms of the affected components
        if (variable.canWrite === true) {
            var currentProcessGroupId = $('#variable-registry-process-group-id').text();

            // only support configuration if the variable belongs to the current group
            if (variable.processGroupId === currentProcessGroupId) {

                // verify the permissions of the group
                var selection = nfCanvasUtils.getSelection();
                if (selection.empty() === false && nf.CanvasUtils.isProcessGroup(selection)) {
                    var selectedData = selection.datum();
                    if (selectedData.id === currentProcessGroupId) {
                        return selectedData.permissions.canWrite === true;
                    }
                }

                var canWrite = false;
                var breadcrumbs = nfNgBridge.injector.get('breadcrumbsCtrl').getBreadcrumbs();
                $.each(breadcrumbs, function (_, breadcrumbEntity) {
                    if (breadcrumbEntity.id === currentProcessGroupId) {
                        canWrite = breadcrumbEntity.permissions.canWrite === true;
                        return false;
                    }
                });

                return canWrite;
            }
        }

        return false;
    };

    /**
     * Loads the specified variable registry.
     *
     * @param {object} variableRegistry
     * @param {string} variableToSelect to select
     */
    var loadVariables = function (variableRegistry, variableToSelect) {
        if (nfCommon.isDefinedAndNotNull(variableRegistry)) {
            var count = 0;
            var index = 0;

            var variableGrid = $('#variable-registry-table').data('gridInstance');
            var variableData = variableGrid.getData();

            // begin the update
            variableData.beginUpdate();

            var variables = [];
            $.each(variableRegistry.variables, function (i, variableEntity) {
                var variable = variableEntity.variable;
                variables.push({
                    id: count++,
                    hidden: false,
                    canWrite: variableEntity.canWrite,
                    name: variable.name,
                    value: variable.value,
                    previousValue: variable.value,
                    processGroupId: variable.processGroupId,
                    affectedComponents: variable.affectedComponents
                });
            });

            $.each(variables, function (i, variable) {
                variableData.addItem($.extend({
                    isOverridden: isOverridden(variables, variable),
                    isEditable: isEditable(variable)
                }, variable));
            });

            // complete the update
            variableData.endUpdate();
            variableData.reSort();

            // if we are pre-selecting a specific variable, get it's index
            if (nfCommon.isDefinedAndNotNull(variableToSelect)) {
                $.each(variables, function (i, variable) {
                    if (variableRegistry.processGroupId === variable.processGroupId && variable.name === variableToSelect) {
                        index = variableData.getRowById(variable.id);
                    }
                });
            }

            if (variables.length === 0) {
                // empty the containers
                var processorContainer = $('#variable-registry-affected-processors');
                nfCommon.cleanUpTooltips(processorContainer, 'div.referencing-component-state');
                nfCommon.cleanUpTooltips(processorContainer, 'div.referencing-component-bulletins');
                processorContainer.empty();

                var controllerServiceContainer = $('#variable-registry-affected-controller-services');
                nfCommon.cleanUpTooltips(controllerServiceContainer, 'div.referencing-component-state');
                nfCommon.cleanUpTooltips(controllerServiceContainer, 'div.referencing-component-bulletins');
                controllerServiceContainer.empty();

                var unauthorizedComponentsContainer = $('#variable-registry-affected-unauthorized-components').empty();

                // indicate no affected components
                $('<li class="affected-component-container"><span class="unset">None</span></li>').appendTo(processorContainer);
                $('<li class="affected-component-container"><span class="unset">None</span></li>').appendTo(controllerServiceContainer);
                $('<li class="affected-component-container"><span class="unset">None</span></li>').appendTo(unauthorizedComponentsContainer);

                // update the selection context
                $('#affected-components-context').addClass('unset').text('None');
            } else {
                // select the desired row
                variableGrid.setSelectedRows([index]);
            }
        }
    };

    /**
     * Populates the variable update steps.
     *
     * @param {array} updateSteps
     * @param {boolean} whether this request has been cancelled
     * @param {boolean} whether this request has errored
     */
    var populateVariableUpdateStep = function (updateSteps, cancelled, errored) {
        var updateStatusContainer = $('#variable-update-steps').empty();

        // go through each step
        $.each(updateSteps, function (_, updateStep) {
            var stepItem = $('<li></li>').text(updateStep.description).appendTo(updateStatusContainer);

            $('<div class="variable-step"></div>').addClass(function () {
                 if (nfCommon.isDefinedAndNotNull(updateStep.failureReason)) {
                     return 'ajax-error';
                 } else {
                     if (updateStep.complete === true) {
                         return 'ajax-complete';
                     } else {
                         return cancelled === true || errored === true ? 'ajax-error' : 'ajax-loading';
                     }
                 }
            }).appendTo(stepItem);

            $('<div class="clear"></div>').appendTo(stepItem);
        });
    };

    /**
     * Updates variables by issuing an update request and polling until it's completion.
     */
    var updateVariables = function () {
        var variables = marshalVariables();
        if (variables.length === 0) {
            close();
            return;
        }

        // update the variables context
        var variableNames = variables.map(function (v) {
             return v.variable.name;
        });
        $('#affected-components-context').removeClass('unset').text(variableNames.join(', '));

        // get the current group id
        var processGroupId = $('#variable-registry-process-group-id').text();

        return $.Deferred(function (deferred) {
            // updates the button model to show the close button
            var updateToCloseButtonModel = function () {
                $('#variable-registry-dialog').modal('setButtonModel', [{
                    buttonText: 'Close',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            deferred.resolve();
                            close();
                        }
                    }
                }]);
            };

            var cancelled = false;

            // update the button model to show the cancel button
            $('#variable-registry-dialog').modal('setButtonModel', [{
                buttonText: 'Cancel',
                color: {
                    base: '#E3E8EB',
                    hover: '#C7D2D7',
                    text: '#004849'
                },
                handler: {
                    click: function () {
                        cancelled = true;
                        updateToCloseButtonModel()
                    }
                }
            }]);

            var requestId;
            var handleAjaxFailure = function (xhr, status, error) {
                // delete the request if possible
                if (nfCommon.isDefinedAndNotNull(requestId)) {
                    deleteUpdateRequest(processGroupId, requestId);
                }

                // update the step status
                $('#variable-update-steps').find('div.variable-step.ajax-loading').removeClass('ajax-loading').addClass('ajax-error');

                // update the button model
                updateToCloseButtonModel();
            };

            submitUpdateRequest(processGroupId, variables).done(function (response) {
                var pollUpdateRequest = function (updateRequestEntity) {
                    var updateRequest = updateRequestEntity.request;
                    var errored = nfCommon.isDefinedAndNotNull(updateRequest.failureReason);

                    // get the request id
                    requestId = updateRequest.requestId;

                    // update the affected components
                    populateAffectedComponents(updateRequest.affectedComponents);

                    // update the progress/steps
                    populateVariableUpdateStep(updateRequest.updateSteps, cancelled, errored);

                    // if this request was cancelled, remove the update request
                    if (cancelled) {
                        deleteUpdateRequest(updateRequest.processGroupId, requestId);
                    } else {
                        if (updateRequest.complete === true) {
                            if (errored) {
                                nfDialog.showOkDialog({
                                    headerText: 'Variable Update Error',
                                    dialogContent: 'Unable to complete variable update request: ' + nfCommon.escapeHtml(updateRequest.failureReason)
                                });
                            }

                            // reload affected processors
                            $.each(updateRequest.affectedComponents, function (_, affectedComponentEntity) {
                                if (affectedComponentEntity.permissions.canRead === true) {
                                    var affectedComponent = affectedComponentEntity.component;

                                    // reload the process if it's in the current group
                                    if (affectedComponent.referenceType === 'PROCESSOR' && nfCanvasUtils.getGroupId() === affectedComponent.processGroupId) {
                                        nfProcessor.reload(affectedComponent.id);
                                    }
                                }
                            });

                            // reload the process group if the context of the update is not the current group (meaning its a child of the current group)
                            if (nfCanvasUtils.getGroupId() !== updateRequest.processGroupId) {
                                nfProcessGroup.reload(updateRequest.processGroupId);
                            }

                            // delete the update request
                            deleteUpdateRequest(updateRequest.processGroupId, requestId);

                            // update the button model
                            updateToCloseButtonModel();
                        } else {
                            // wait to get an updated status
                            setTimeout(function () {
                                getUpdateRequest(updateRequest.processGroupId, requestId).done(function (getResponse) {
                                    pollUpdateRequest(getResponse);
                                }).fail(handleAjaxFailure);
                            }, 2000);
                        }
                    }
                };

                // update the visibility
                $('#variable-registry-table, #add-variable').hide();
                $('#variable-update-status').show();

                pollUpdateRequest(response);
            }).fail(handleAjaxFailure);
        }).promise();
    };

    /**
     * Submits an variable update request.
     *
     * @param {string} processGroupId
     * @param {object} variables
     * @returns {deferred} update request xhr
     */
    var submitUpdateRequest = function (processGroupId, variables) {
        var processGroupRevision = $('#variable-registry-process-group-id').data('revision');

        var updateRequestEntity = {
            processGroupRevision: nfClient.getRevision({
                revision: {
                    version: processGroupRevision.version
                }
            }),
            variableRegistry: {
                processGroupId: processGroupId,
                variables: variables
            }
        };

        return $.ajax({
            type: 'POST',
            data: JSON.stringify(updateRequestEntity),
            url: '../nifi-api/process-groups/' + encodeURIComponent(processGroupId) + '/variable-registry/update-requests',
            dataType: 'json',
            contentType: 'application/json'
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Obtains the current state of the updateRequest using the specified process group id and update request id.
     *
     * @param {string} processGroupId
     * @param {string} updateRequestId
     * @returns {deferred} update request xhr
     */
    var getUpdateRequest = function (processGroupId, updateRequestId) {
        return $.ajax({
            type: 'GET',
            url: '../nifi-api/process-groups/' + encodeURIComponent(processGroupId) + '/variable-registry/update-requests/' + encodeURIComponent(updateRequestId),
            dataType: 'json'
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Deletes an updateRequest using the specified process group id and update request id.
     *
     * @param {string} processGroupId
     * @param {string} updateRequestId
     * @returns {deferred} update request xhr
     */
    var deleteUpdateRequest = function (processGroupId, updateRequestId) {
        return $.ajax({
            type: 'DELETE',
            url: '../nifi-api/process-groups/' + encodeURIComponent(processGroupId) + '/variable-registry/update-requests/' + encodeURIComponent(updateRequestId),
            dataType: 'json'
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Marshals the variables in the table.
     */
    var marshalVariables = function () {
        var variables = [];

        var variableGrid = $('#variable-registry-table').data('gridInstance');
        var variableData = variableGrid.getData();

        $.each(variableData.getItems(), function () {
            var variable = {
                'name': this.name
            };

            var modified = false;
            if (this.hidden === true && this.previousValue !== null) {
                // hidden variables were removed by the user, clear the value
                variable['value'] = null;
                modified = true;
            } else if (this.value !== this.previousValue) {
                // the value has changed
                variable['value'] = this.value;
                modified = true;
            }

            if (modified) {
                variables.push({
                    'variable': variable
                });
            }
        });

        return variables;
    };

    /**
     * Adds a new variable.
     */
    var addNewVariable = function () {
        var currentProcessGroupId = $('#variable-registry-process-group-id').text();
        var variableName = $.trim($('#new-variable-name').val());

        // ensure the property name is specified
        if (variableName !== '') {
            var variableGrid = $('#variable-registry-table').data('gridInstance');
            var variableData = variableGrid.getData();

            // ensure the property name is unique
            var conflictingVariables = [];
            var matchingVariable = null;
            $.each(variableData.getItems(), function (_, item) {
                if (variableName === item.name) {
                    // if the scope is same, this is an exact match otherwise we've identified a conflicting variable
                    if (currentProcessGroupId === item.processGroupId) {
                        matchingVariable = item;
                    } else {
                        conflictingVariables.push(item);
                    }
                }
            });

            if (matchingVariable === null) {
                // add a row for the new variable
                var id = variableData.getLength();
                variableData.addItem({
                    id: id,
                    hidden: false,
                    canWrite: true,
                    name: variableName,
                    value: null,
                    previousValue: null,
                    processGroupId: currentProcessGroupId,
                    isEditable: true,
                    isOverridden: false
                });

                // we've just added a new variable, mark any conflicting variables as overridden
                $.each(conflictingVariables, function (_, conflictingVariable) {
                    variableData.updateItem(conflictingVariable.id, $.extend(conflictingVariable, {
                        isOverridden: true
                    }));
                });

                // sort the data
                variableData.reSort();

                // select the new variable row
                var row = variableData.getRowById(id);
                variableGrid.setActiveCell(row, variableGrid.getColumnIndex('value'));
                variableGrid.editActiveCell();
            } else {
                // if this row is currently hidden, clear the value and show it
                if (matchingVariable.hidden === true) {
                    variableData.updateItem(matchingVariable.id, $.extend(matchingVariable, {
                        hidden: false,
                        value: null
                    }));

                    // select the new properties row
                    var editableMatchingRow = variableData.getRowById(matchingVariable.id);
                    variableGrid.setActiveCell(editableMatchingRow, variableGrid.getColumnIndex('value'));
                    variableGrid.editActiveCell();
                } else {
                    nfDialog.showOkDialog({
                        headerText: 'Variable Exists',
                        dialogContent: 'A variable with this name already exists.'
                    });

                    // select the existing properties row
                    var matchingRow = variableData.getRowById(matchingVariable.id);
                    variableGrid.setSelectedRows([matchingRow]);
                    variableGrid.scrollRowIntoView(matchingRow);
                }
            }
        } else {
            nfDialog.showOkDialog({
                headerText: 'Variable Name',
                dialogContent: 'Variable name must be specified.'
            });
        }

        // close the new variable dialog
        $('#new-variable-dialog').modal('hide');
    };

    /**
     * Cancels adding a new variable.
     */
    var close = function () {
        $('#variable-registry-dialog').modal('hide');
    };

    /**
     * Reset the dialog.
     */
    var resetDialog = function () {
        $('#variable-registry-table, #add-variable').show();
        $('#variable-update-status').hide();

        $('#process-group-variable-registry').text('');
        $('#variable-registry-process-group-id').text('').removeData('revision');
        $('#affected-components-context').removeClass('unset').text('');

        var variableGrid = $('#variable-registry-table').data('gridInstance');
        var variableData = variableGrid.getData();
        variableData.setItems([]);

        var affectedProcessorContainer = $('#variable-registry-affected-processors');
        nfCommon.cleanUpTooltips(affectedProcessorContainer, 'div.referencing-component-state');
        nfCommon.cleanUpTooltips(affectedProcessorContainer, 'div.referencing-component-bulletins');
        affectedProcessorContainer.empty();

        var affectedControllerServicesContainer = $('#variable-registry-affected-controller-services');
        nfCommon.cleanUpTooltips(affectedControllerServicesContainer, 'div.referencing-component-state');
        nfCommon.cleanUpTooltips(affectedControllerServicesContainer, 'div.referencing-component-bulletins');
        affectedControllerServicesContainer.empty();

        $('#variable-registry-affected-unauthorized-components').empty();
    };

    return {
        /**
         * Initializes the variable registry dialogs.
         */
        init: function () {
            $('#variable-registry-dialog').modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'Variables',
                handler: {
                    close: function () {
                        resetDialog();
                    },
                    open: function () {
                        var variableGrid = $('#variable-registry-table').data('gridInstance');
                        if (nfCommon.isDefinedAndNotNull(variableGrid)) {
                            variableGrid.resizeCanvas();
                        }
                    }
                }
            });

            $('#new-variable-dialog').modal({
                headerText: 'New Variable',
                buttons: [{
                    buttonText: 'Ok',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            addNewVariable();
                        }
                    }
                }],
                handler: {
                    close: function () {
                        $('#new-variable-name').val('');
                    },
                    open: function () {
                        $('#new-variable-name').focus();
                    }
                }
            });

            $('#new-variable-name').on('keydown', function (e) {
                var code = e.keyCode ? e.keyCode : e.which;
                if (code === $.ui.keyCode.ENTER) {
                    addNewVariable();

                    // prevents the enter from propagating into the field for editing the new property value
                    e.stopImmediatePropagation();
                    e.preventDefault();
                }
            });

            $('#add-variable').on('click', function () {
                $('#new-variable-dialog').modal('show');
            });

            initVariableTable();
        },

        /**
         * Shows the variables for the specified process group.
         *
         * @param {string} processGroupId
         */
        showVariables: function (processGroupId) {
            // restore the button model
            $('#variable-registry-dialog').modal('setButtonModel', [{
                buttonText: 'Apply',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        updateVariables();
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
                        close();
                    }
                }
            }]);

            return showVariables(processGroupId);
        }
    };
}));