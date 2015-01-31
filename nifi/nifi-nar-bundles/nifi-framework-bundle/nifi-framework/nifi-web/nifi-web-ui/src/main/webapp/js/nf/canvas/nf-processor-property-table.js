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
nf.ProcessorPropertyTable = (function () {

    /**
     * Initialize the new property dialog.
     */
    var initNewPropertyDialog = function () {
        var languageId = 'nfel';
        var editorClass = languageId + '-editor';

        // add the over state for the new property button
        nf.Common.addHoverEffect('#add-property-icon', 'add-icon-bg', 'add-icon-bg-hover');

        var add = function () {
            var propertyName = $.trim($('#new-property-name').val());
            var propertyValue = $('#new-property-value').nfeditor('getValue');

            // ensure the property name and value is specified
            if (propertyName !== '') {
                // add a row for the new property
                var propertyGrid = $('#processor-properties').data('gridInstance');
                var propertyData = propertyGrid.getData();
                propertyData.addItem({
                    id: propertyData.getLength(),
                    hidden: false,
                    property: propertyName,
                    displayName: propertyName,
                    previousValue: null,
                    value: propertyValue,
                    type: 'userDefined'
                });
            } else {
                nf.Dialog.showOkDialog({
                    dialogContent: 'Property name must be specified.',
                    overlayBackground: false
                });
            }

            // close the dialog
            $('#processor-property-dialog').hide();
        };

        var cancel = function () {
            $('#processor-property-dialog').hide();
        };

        // create the editor
        $('#new-property-value').addClass(editorClass).nfeditor({
            languageId: languageId,
            width: 318,
            minWidth: 318,
            height: 106,
            minHeight: 106,
            resizable: true,
            escape: cancel,
            enter: add
        });

        // add a click listener to display the new property dialog
        $('#add-property-icon').on('click', function () {
            // close all fields currently being edited
            nf.ProcessorPropertyTable.saveRow();

            // clear the dialog
            $('#new-property-name').val('');
            $('#new-property-value').nfeditor('setValue', '');

            // reset the add property dialog position/size
            $('#new-property-value').nfeditor('setSize', 318, 106);

            // open the new property dialog
            $('#processor-property-dialog').center().show();

            // give the property name focus
            $('#new-property-value').nfeditor('refresh');
            $('#new-property-name').focus();
        });

        // make the new property dialog draggable
        $('#processor-property-dialog').draggable({
            cancel: 'input, textarea, pre, .button, .' + editorClass,
            containment: 'parent'
        }).on('click', '#new-property-ok', add).on('click', '#new-property-cancel', cancel);

        // enable tabs in the property value
        $('#new-property-name').on('keydown', function (e) {
            if (e.which === $.ui.keyCode.ENTER && !e.shiftKey) {
                add();
            } else if (e.which === $.ui.keyCode.ESCAPE) {
                e.preventDefault();
                cancel();
            }
        });
    };

    /**
     * Initializes the processor property table.
     */
    var initProcessorPropertiesTable = function () {
        // function for formatting the property name
        var nameFormatter = function (row, cell, value, columnDef, dataContext) {
            var nameWidthOffset = 10;
            var cellContent = $('<div></div>');

            // format the contents
            var formattedValue = $('<span/>').addClass('table-cell').text(value).appendTo(cellContent);
            if (dataContext.type === 'required') {
                formattedValue.addClass('required');
            }

            // get the processor details to insert the description tooltip
            var details = $('#processor-configuration').data('processorDetails');
            var propertyDescriptor = details.config.descriptors[dataContext.property];

            // show the property description if applicable
            if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
                if (!nf.Common.isBlank(propertyDescriptor.description) || !nf.Common.isBlank(propertyDescriptor.defaultValue) || !nf.Common.isBlank(propertyDescriptor.supportsEl)) {
                    $('<img class="icon-info" src="images/iconInfo.png" alt="Info" title="" style="float: right; margin-right: 6px; margin-top: 4px;" />').appendTo(cellContent);
                    $('<span class="hidden property-descriptor-name"></span>').text(dataContext.property).appendTo(cellContent);
                    nameWidthOffset = 26; // 10 + icon width (10) + icon margin (6)
                }
            }

            // adjust the width accordingly
            formattedValue.width(columnDef.width - nameWidthOffset).ellipsis();

            // return the cell content
            return cellContent.html();
        };

        // function for formatting the property value
        var valueFormatter = function (row, cell, value, columnDef, dataContext) {
            var valueMarkup;
            if (nf.Common.isDefinedAndNotNull(value)) {
                // identify the property descriptor
                var processorDetails = $('#processor-configuration').data('processorDetails');
                var propertyDescriptor = processorDetails.config.descriptors[dataContext.property];

                // determine if the property is sensitive
                if (nf.ProcessorPropertyTable.isSensitiveProperty(propertyDescriptor)) {
                    valueMarkup = '<span class="table-cell sensitive">Sensitive value set</span>';
                } else {
                    // if there are allowable values, attempt to swap out for the display name
                    var allowableValues = nf.ProcessorPropertyTable.getAllowableValues(propertyDescriptor);
                    if ($.isArray(allowableValues)) {
                        $.each(allowableValues, function (_, allowableValue) {
                            if (value === allowableValue.value) {
                                value = allowableValue.displayName;
                                return false;
                            }
                        });
                    }

                    if (value === '') {
                        valueMarkup = '<span class="table-cell blank">Empty string set</span>';
                    } else {
                        valueMarkup = '<div class="table-cell value"><pre class="ellipsis">' + nf.Common.escapeHtml(value) + '</pre></div>';
                    }
                }
            } else {
                valueMarkup = '<span class="unset">No value set</span>';
            }

            // format the contents
            var content = $(valueMarkup);
            if (dataContext.type === 'required') {
                content.addClass('required');
            }
            content.find('.ellipsis').width(columnDef.width - 10).ellipsis();

            // return the appropriate markup
            return $('<div/>').append(content).html();
        };

        // custom formatter for the actions column
        var actionFormatter = function (row, cell, value, columnDef, dataContext) {
            var markup = '';

            // allow user defined properties to be removed
            if (dataContext.type === 'userDefined') {
                markup = '<img src="images/iconDelete.png" title="Delete" class="pointer" style="margin-top: 2px" onclick="javascript:nf.ProcessorPropertyTable.deleteProperty(\'' + row + '\');"/>';
            }

            return markup;
        };

        var processorConfigurationColumns = [
            {id: 'property', field: 'displayName', name: 'Property', sortable: false, resizable: true, rerenderOnResize: true, formatter: nameFormatter},
            {id: 'value', field: 'value', name: 'Value', sortable: false, resizable: true, cssClass: 'pointer', rerenderOnResize: true, formatter: valueFormatter},
            {id: "actions", name: "&nbsp;", minWidth: 20, width: 20, formatter: actionFormatter}
        ];
        var processorConfigurationOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            editable: true,
            enableAddRow: false,
            autoEdit: false
        };

        // initialize the dataview
        var processorConfigurationData = new Slick.Data.DataView({
            inlineFilters: false
        });
        processorConfigurationData.setItems([]);
        processorConfigurationData.setFilterArgs({
            searchString: '',
            property: 'hidden'
        });
        processorConfigurationData.setFilter(filter);
        processorConfigurationData.getItemMetadata = function (index) {
            var item = processorConfigurationData.getItem(index);

            // identify the property descriptor
            var processorDetails = $('#processor-configuration').data('processorDetails');
            var propertyDescriptor = processorDetails.config.descriptors[item.property];

            // support el if specified or unsure yet (likely a dynamic property)
            if (nf.Common.isUndefinedOrNull(propertyDescriptor) || nf.ProcessorPropertyTable.supportsEl(propertyDescriptor)) {
                return {
                    columns: {
                        value: {
                            editor: nf.ProcessorPropertyNfelEditor
                        }
                    }
                };
            } else {
                // check for allowable values which will drive which editor to use
                var allowableValues = nf.ProcessorPropertyTable.getAllowableValues(propertyDescriptor);
                if ($.isArray(allowableValues)) {
                    return {
                        columns: {
                            value: {
                                editor: nf.ProcessorPropertyComboEditor
                            }
                        }
                    };
                } else {
                    return {
                        columns: {
                            value: {
                                editor: nf.ProcessorPropertyTextEditor
                            }
                        }
                    };
                }
            }
        };

        // initialize the grid
        var processorConfigurationGrid = new Slick.Grid('#processor-properties', processorConfigurationData, processorConfigurationColumns, processorConfigurationOptions);
        processorConfigurationGrid.setSelectionModel(new Slick.RowSelectionModel());
        processorConfigurationGrid.onClick.subscribe(function (e, args) {
            // edits the clicked cell
            processorConfigurationGrid.gotoCell(args.row, args.cell, true);

            // prevents standard edit logic
            e.stopImmediatePropagation();
        });

        // wire up the dataview to the grid
        processorConfigurationData.onRowCountChanged.subscribe(function (e, args) {
            processorConfigurationGrid.updateRowCount();
            processorConfigurationGrid.render();
        });
        processorConfigurationData.onRowsChanged.subscribe(function (e, args) {
            processorConfigurationGrid.invalidateRows(args.rows);
            processorConfigurationGrid.render();
        });

        // hold onto an instance of the grid and listen for mouse events to add tooltips where appropriate
        $('#processor-properties').data('gridInstance', processorConfigurationGrid).on('mouseenter', 'div.slick-cell', function (e) {
            var infoIcon = $(this).find('img.icon-info');
            if (infoIcon.length && !infoIcon.data('qtip')) {
                var property = $(this).find('span.property-descriptor-name').text();

                // get the processor details to insert the description tooltip
                var details = $('#processor-configuration').data('processorDetails');
                var propertyDescriptor = details.config.descriptors[property];

                // get the processor history
                var processorHistory = $('#processor-configuration').data('processorHistory');
                var propertyHistory = processorHistory.propertyHistory[property];

                // format the tooltip
                var tooltip = nf.Common.formatPropertyTooltip(propertyDescriptor, propertyHistory);

                if (nf.Common.isDefinedAndNotNull(tooltip)) {
                    infoIcon.qtip($.extend({
                        content: tooltip
                    }, nf.Common.config.tooltipConfig));
                }
            }
        });
    };

    /**
     * Performs the filtering.
     * 
     * @param {object} item     The item subject to filtering
     * @param {object} args     Filter arguments
     * @returns {Boolean}       Whether or not to include the item
     */
    var filter = function (item, args) {
        return item.hidden === false;
    };

    return {
        config: {
            sensitiveText: 'Sensitive value set'
        },
        
        /**
         * Initializes the property table.
         */
        init: function () {
            initNewPropertyDialog();
            initProcessorPropertiesTable();
        },
        
        /**
         * Determines if the specified property is sensitive.
         * 
         * @argument {object} propertyDescriptor        The property descriptor
         */
        isSensitiveProperty: function (propertyDescriptor) {
            if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
                return propertyDescriptor.sensitive === true;
            } else {
                return false;
            }
        },
        
        /**
         * Determines if the specified property is required.
         * 
         * @param {object} propertyDescriptor           The property descriptor
         */
        isRequiredProperty: function (propertyDescriptor) {
            if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
                return propertyDescriptor.required === true;
            } else {
                return false;
            }
        },
        
        /**
         * Determines if the specified property is required.
         * 
         * @param {object} propertyDescriptor           The property descriptor
         */
        isDynamicProperty: function (propertyDescriptor) {
            if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
                return propertyDescriptor.dynamic === true;
            } else {
                return false;
            }
        },
        
        /**
         * Gets the allowable values for the specified property.
         * 
         * @argument {object} propertyDescriptor        The property descriptor
         */
        getAllowableValues: function (propertyDescriptor) {
            if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
                return propertyDescriptor.allowableValues;
            } else {
                return null;
            }
        },
        
        /**
         * Returns whether the specified property supports EL.
         * 
         * @param {object} propertyDescriptor           The property descriptor
         */
        supportsEl: function (propertyDescriptor) {
            if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
                return propertyDescriptor.supportsEl === true;
            } else {
                return false;
            }
        },
        
        /**
         * Saves the last edited row in the specified grid.
         */
        saveRow: function () {
            // get the property grid to commit the current edit
            var propertyGrid = $('#processor-properties').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(propertyGrid)) {
                var editController = propertyGrid.getEditController();
                editController.commitCurrentEdit();
            }
        },
        
        /**
         * Cancels the edit in the specified row.
         */
        cancelEdit: function () {
            // get the property grid to reset the current edit
            var propertyGrid = $('#processor-properties').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(propertyGrid)) {
                var editController = propertyGrid.getEditController();
                editController.cancelCurrentEdit();
            }
        },
        
        /**
         * Deletes the property in the specified row.
         * 
         * @argument {string} row         The row
         */
        deleteProperty: function (row) {
            var propertyGrid = $('#processor-properties').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(propertyGrid)) {
                var propertyData = propertyGrid.getData();

                // update the property in question
                var property = propertyData.getItem(row);
                property.hidden = true;

                // refresh the table
                propertyData.updateItem(property.id, property);
            }
        },
        
        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            var propertyGrid = $('#processor-properties').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(propertyGrid)) {
                propertyGrid.resizeCanvas();
            }
        },
        
        /**
         * Loads the specified properties.
         * 
         * @argument {object} properties        The properties
         * @argument {map} descriptors          The property descriptors (property name -> property descriptor)
         */
        loadProperties: function (properties, descriptors) {
            // get the property grid
            var propertyGrid = $('#processor-properties').data('gridInstance');
            var propertyData = propertyGrid.getData();

            // generate the processor properties
            if (nf.Common.isDefinedAndNotNull(properties)) {
                propertyData.beginUpdate();

                var i = 0;
                $.each(properties, function (name, value) {
                    // get the property descriptor
                    var descriptor = descriptors[name];

                    // determine the property type
                    var type = 'userDefined';
                    var displayName = name;
                    if (nf.Common.isDefinedAndNotNull(descriptor)) {
                        if (nf.ProcessorPropertyTable.isRequiredProperty(descriptor)) {
                            type = 'required';
                        } else if (nf.ProcessorPropertyTable.isDynamicProperty(descriptor)) {
                            type = 'userDefined';
                        } else {
                            type = 'optional';
                        }

                        // use the display name if possible
                        displayName = descriptor.displayName;
                        
                        // determine the value
                        if (nf.Common.isNull(value) && nf.Common.isDefinedAndNotNull(descriptor.defaultValue)) {
                            value = descriptor.defaultValue;
                        }
                    }

                    // add the row
                    propertyData.addItem({
                        id: i++,
                        hidden: false,
                        property: name,
                        displayName: displayName,
                        previousValue: value,
                        value: value,
                        type: type
                    });
                });

                propertyData.endUpdate();
            }
        },
        
        /**
         * Determines if a save is required.
         */
        isSaveRequired: function () {
            // get the property grid
            var propertyGrid = $('#processor-properties').data('gridInstance');
            var propertyData = propertyGrid.getData();

            // determine if any of the processor properties have changed
            var isSaveRequired = false;
            $.each(propertyData.getItems(), function () {
                if (this.value !== this.previousValue) {
                    isSaveRequired = true;
                    return false;
                }
            });
            return isSaveRequired;
        },
        
        /**
         * Marshal's the properties to send to the server.
         */
        marshalProperties: function () {
            // properties
            var properties = {};

            // get the property grid data
            var propertyGrid = $('#processor-properties').data('gridInstance');
            var propertyData = propertyGrid.getData();
            $.each(propertyData.getItems(), function () {
                if (this.hidden === true) {
                    properties[this.property] = null;
                } else if (this.value !== this.previousValue) {
                    properties[this.property] = this.value;
                }
            });

            return properties;
        },
        
        /**
         * Clears the property table.
         */
        clear: function () {
            var propertyGridElement = $('#processor-properties');
            
            // clean up any tooltips that may have been generated
            nf.Common.cleanUpTooltips(propertyGridElement, 'img.icon-info');
            
            // clear the data in the grid
            var propertyGrid = propertyGridElement.data('gridInstance');
            var propertyData = propertyGrid.getData();
            propertyData.setItems([]);
        }
    };
}());