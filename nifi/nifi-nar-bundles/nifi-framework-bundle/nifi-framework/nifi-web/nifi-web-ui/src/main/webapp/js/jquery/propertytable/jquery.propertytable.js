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

/* global nf, Slick */

/**
 * Create a property table. The options are specified in the following
 * format:
 *
 * {
 *   readOnly: true,
 *   newPropertyDialogContainer: 'body'
 * }
 */

/**
 * jQuery plugin for a property table.
 * 
 * @param {type} $
 */
(function ($) {
    var languageId = 'nfel';
    var editorClass = languageId + '-editor';

    // text editor
    var textEditor = function (args) {
        var scope = this;
        var initialValue = '';
        var previousValue;
        var propertyDescriptor;
        var wrapper;
        var isEmpty;
        var input;

        this.init = function () {
            var container = $('body');

            // get the property descriptor
            var gridContainer = $(args.grid.getContainerNode());
            var descriptors = gridContainer.data('descriptors');
            propertyDescriptor = descriptors[args.item.property];

            // record the previous value
            previousValue = args.item[args.column.field];

            // create the wrapper
            wrapper = $('<div></div>').addClass('slickgrid-editor').css({
                'z-index': 100000,
                'position': 'absolute',
                'background': 'white',
                'padding': '5px',
                'overflow': 'hidden',
                'border': '3px solid #365C6A',
                'box-shadow': '4px 4px 6px rgba(0, 0, 0, 0.9)',
                'cursor': 'move'
            }).draggable({
                cancel: '.button, textarea, .nf-checkbox',
                containment: 'parent'
            }).appendTo(container);

            // create the input field
            input = $('<textarea hidefocus rows="5"/>').css({
                'background': 'white',
                'width': args.position.width + 'px',
                'min-width': '150px',
                'height': '80px',
                'border-width': '0',
                'outline': '0',
                'overflow-y': 'auto',
                'resize': 'both',
                'margin-bottom': '28px'
            }).tab().on('keydown', scope.handleKeyDown).appendTo(wrapper);

            // create the button panel
            var stringCheckPanel = $('<div class="string-check-container">');

            // build the custom checkbox
            isEmpty = $('<div class="nf-checkbox string-check"/>').appendTo(stringCheckPanel);
            $('<span class="string-check-label">&nbsp;Empty</span>').appendTo(stringCheckPanel);

            var ok = $('<div class="button button-normal">Ok</div>').on('click', scope.save);
            var cancel = $('<div class="button button-normal">Cancel</div>').on('click', scope.cancel);
            $('<div></div>').css({
                'position': 'absolute',
                'bottom': '0',
                'left': '0',
                'right': '0',
                'padding': '0 3px 5px'
            }).append(stringCheckPanel).append(ok).append(cancel).append('<div class="clear"></div>').appendTo(wrapper);

            // position and focus
            scope.position(args.position);
            input.focus().select();
        };

        this.handleKeyDown = function (e) {
            if (e.which === $.ui.keyCode.ENTER && !e.shiftKey) {
                scope.save();
            } else if (e.which === $.ui.keyCode.ESCAPE) {
                scope.cancel();
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
                'top': position.top - 5,
                'left': position.left - 5
            });
        };

        this.destroy = function () {
            wrapper.remove();
        };

        this.focus = function () {
            input.focus();
        };

        this.loadValue = function (item) {
            // determine if this is a sensitive property
            var isEmptyChecked = false;
            var sensitive = nf.Common.isSensitiveProperty(propertyDescriptor);

            // determine the value to use when populating the text field
            if (nf.Common.isDefinedAndNotNull(item[args.column.field])) {
                if (sensitive) {
                    initialValue = nf.Common.config.sensitiveText;
                } else {
                    initialValue = item[args.column.field];
                    isEmptyChecked = initialValue === '';
                }
            }

            // determine if its an empty string
            var checkboxStyle = isEmptyChecked ? 'checkbox-checked' : 'checkbox-unchecked';
            isEmpty.addClass(checkboxStyle);

            // style sensitive properties differently
            if (sensitive) {
                input.addClass('sensitive').keydown(function () {
                    var sensitiveInput = $(this);
                    if (sensitiveInput.hasClass('sensitive')) {
                        sensitiveInput.removeClass('sensitive');
                        if (sensitiveInput.val() === nf.Common.config.sensitiveText) {
                            sensitiveInput.val('');
                        }
                    }
                });
            }

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
                    // otherwise if the property is required
                    if (nf.Common.isRequiredProperty(propertyDescriptor)) {
                        if (nf.Common.isBlank(propertyDescriptor.defaultValue)) {
                            return previousValue;
                        } else {
                            return propertyDescriptor.defaultValue;
                        }
                    } else {
                        // if the property is not required, clear the value
                        return null;
                    }
                }
            } else {
                // if the field still has the sensitive class it means a property
                // was edited but never modified so we should restore the previous
                // value instead of setting it to the 'sensitive value set' string
                if (input.hasClass('sensitive')) {
                    return previousValue;
                } else {
                    // if there is text specified, use that value
                    return input.val();
                }
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

    // nfel editor
    var nfelEditor = function (args) {
        var scope = this;
        var initialValue = '';
        var previousValue;
        var propertyDescriptor;
        var isEmpty;
        var wrapper;
        var editor;

        this.init = function () {
            var container = $('body');

            // get the property descriptor
            var gridContainer = $(args.grid.getContainerNode());
            var descriptors = gridContainer.data('descriptors');
            propertyDescriptor = descriptors[args.item.property];

            // determine if this is a sensitive property
            var sensitive = nf.Common.isSensitiveProperty(propertyDescriptor);

            // record the previous value
            previousValue = args.item[args.column.field];

            var languageId = 'nfel';
            var editorClass = languageId + '-editor';

            // create the wrapper
            wrapper = $('<div></div>').addClass('slickgrid-nfel-editor').css({
                'z-index': 14000,
                'position': 'absolute',
                'background': 'white',
                'padding': '5px',
                'overflow': 'hidden',
                'border': '3px solid #365C6A',
                'box-shadow': '4px 4px 6px rgba(0, 0, 0, 0.9)',
                'cursor': 'move'
            }).draggable({
                cancel: 'input, textarea, pre, .nf-checkbox, .button, .' + editorClass,
                containment: 'parent'
            }).appendTo(container);

            // create the editor
            editor = $('<div></div>').addClass(editorClass).appendTo(wrapper).nfeditor({
                languageId: languageId,
                width: args.position.width,
                minWidth: 175,
                minHeight: 100,
                resizable: true,
                sensitive: sensitive,
                escape: function () {
                    scope.cancel();
                },
                enter: function () {
                    scope.save();
                }
            });

            // create the button panel
            var stringCheckPanel = $('<div class="string-check-container">');

            // build the custom checkbox
            isEmpty = $('<div class="nf-checkbox string-check"/>').appendTo(stringCheckPanel);
            $('<span class="string-check-label">&nbsp;Empty</span>').appendTo(stringCheckPanel);

            var ok = $('<div class="button button-normal">Ok</div>').on('click', scope.save);
            var cancel = $('<div class="button button-normal">Cancel</div>').on('click', scope.cancel);
            $('<div></div>').css({
                'position': 'absolute',
                'bottom': '0',
                'left': '0',
                'right': '0',
                'padding': '0 3px 5px 1px'
            }).append(stringCheckPanel).append(ok).append(cancel).append('<div class="clear"></div>').appendTo(wrapper);

            // position and focus
            scope.position(args.position);
            editor.nfeditor('focus').nfeditor('selectAll');
        };

        this.save = function () {
            args.commitChanges();
        };

        this.cancel = function () {
            editor.nfeditor('setValue', initialValue);
            args.cancelChanges();
        };

        this.hide = function () {
            wrapper.hide();
        };

        this.show = function () {
            wrapper.show();
            editor.nfeditor('setSize', args.position.width, null).nfeditor('refresh');
        };

        this.position = function (position) {
            wrapper.css({
                'top': position.top - 5,
                'left': position.left - 5
            });
        };

        this.destroy = function () {
            editor.nfeditor('destroy');
            wrapper.remove();
        };

        this.focus = function () {
            editor.nfeditor('focus');
        };

        this.loadValue = function (item) {
            // determine if this is a sensitive property
            var isEmptyChecked = false;
            var sensitive = nf.Common.isSensitiveProperty(propertyDescriptor);

            // determine the value to use when populating the text field
            if (nf.Common.isDefinedAndNotNull(item[args.column.field])) {
                if (sensitive) {
                    initialValue = nf.Common.config.sensitiveText;
                } else {
                    initialValue = item[args.column.field];
                    isEmptyChecked = initialValue === '';
                }
            }

            // determine if its an empty string
            var checkboxStyle = isEmptyChecked ? 'checkbox-checked' : 'checkbox-unchecked';
            isEmpty.addClass(checkboxStyle);

            editor.nfeditor('setValue', initialValue).nfeditor('selectAll');
        };

        this.serializeValue = function () {
            var value = editor.nfeditor('getValue');

            // if the field has been cleared, set the value accordingly
            if (value === '') {
                // if the user has checked the empty string checkbox, use emtpy string
                if (isEmpty.hasClass('checkbox-checked')) {
                    return '';
                } else {
                    // otherwise if the property is required
                    if (nf.Common.isRequiredProperty(propertyDescriptor)) {
                        if (nf.Common.isBlank(propertyDescriptor.defaultValue)) {
                            return previousValue;
                        } else {
                            return propertyDescriptor.defaultValue;
                        }
                    } else {
                        // if the property is not required, clear the value
                        return null;
                    }
                }
            } else {
                // if the field still has the sensitive class it means a property
                // was edited but never modified so we should restore the previous
                // value instead of setting it to the 'sensitive value set' string

                // if the field hasn't been modified return the previous value... this
                // is important because sensitive properties contain the text 'sensitive
                // value set' which is cleared when the value is edited. we do not 
                // want to actually use this value
                if (editor.nfeditor('isModified') === false) {
                    return previousValue;
                } else {
                    // if there is text specified, use that value
                    return value;
                }
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

        // initialize the custom long nfel editor
        this.init();
    };

    // combo editor
    var comboEditor = function (args) {
        var scope = this;
        var initialValue = null;
        var wrapper;
        var combo;
        var propertyDescriptor;

        this.init = function () {
            var container = $('body');

            // get the property descriptor
            var gridContainer = $(args.grid.getContainerNode());
            var descriptors = gridContainer.data('descriptors');
            propertyDescriptor = descriptors[args.item.property];

            // create the wrapper
            wrapper = $('<div></div>').css({
                'z-index': 1999,
                'position': 'absolute',
                'background': 'white',
                'padding': '5px',
                'overflow': 'hidden',
                'border': '3px solid #365C6A',
                'box-shadow': '4px 4px 6px rgba(0, 0, 0, 0.9)',
                'cursor': 'move'
            }).draggable({
                cancel: '.button, .combo',
                containment: 'parent'
            }).appendTo(container);

            // check for allowable values which will drive which editor to use
            var allowableValues = nf.Common.getAllowableValues(propertyDescriptor);

            // show the output port options
            var options = [];
            if (propertyDescriptor.required === false) {
                options.push({
                    text: 'No value',
                    value: null,
                    optionClass: 'unset'
                });
            }
            if ($.isArray(allowableValues)) {
                $.each(allowableValues, function (i, allowableValue) {
                    options.push({
                        text: allowableValue.displayName,
                        value: allowableValue.value,
                        description: nf.Common.escapeHtml(allowableValue.description)
                    });
                });
            }

            // ensure the options there is at least one option
            if (options.length === 0) {
                options.push({
                    text: 'No value',
                    value: null,
                    optionClass: 'unset',
                    disabled: true
                });
            }

            // determine the max height
            var position = args.position;
            var windowHeight = $(window).height();
            var maxHeight = windowHeight - position.bottom - 16;

            // build the combo field
            combo = $('<div class="value-combo combo"></div>').combo({
                options: options,
                maxHeight: maxHeight
            }).width(position.width - 16).appendTo(wrapper);

            // add buttons for handling user input
            $('<div class="button button-normal">Cancel</div>').css({
                'margin': '0 0 0 5px',
                'float': 'left'
            }).on('click', scope.cancel).appendTo(wrapper);
            $('<div class="button button-normal">Ok</div>').css({
                'margin': '0 0 0 5px',
                'float': 'left'
            }).on('click', scope.save).appendTo(wrapper);

            // position and focus
            scope.position(position);
        };

        this.save = function () {
            args.commitChanges();
        };

        this.cancel = function () {
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
                'top': position.top - 5,
                'left': position.left - 5
            });
        };

        this.destroy = function () {
            wrapper.remove();
        };

        this.focus = function () {
        };

        this.loadValue = function (item) {
            // select as appropriate
            if (!nf.Common.isUndefined(item.value)) {
                initialValue = item.value;

                combo.combo('setSelectedOption', {
                    value: item.value
                });
            } else if (nf.Common.isDefinedAndNotNull(propertyDescriptor.defaultValue)) {
                initialValue = propertyDescriptor.defaultValue;

                combo.combo('setSelectedOption', {
                    value: propertyDescriptor.defaultValue
                });
            }
        };

        this.serializeValue = function () {
            var selectedOption = combo.combo('getSelectedOption');
            return selectedOption.value;
        };

        this.applyValue = function (item, state) {
            item[args.column.field] = state;
        };

        this.isValueChanged = function () {
            var selectedOption = combo.combo('getSelectedOption');
            return (!(selectedOption.value === "" && initialValue === null)) && (selectedOption.value !== initialValue);
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
     * Shows the property value for the specified row and cell.
     * 
     * @param {type} propertyGrid
     * @param {type} descriptors 
     * @param {type} row
     * @param {type} cell
     */
    var showPropertyValue = function (propertyGrid, descriptors, row, cell) {
        // remove any currently open detail dialogs
        nf.Common.removeAllPropertyDetailDialogs();

        // get the property in question
        var propertyData = propertyGrid.getData();
        var property = propertyData.getItem(row);

        // ensure there is a value
        if (nf.Common.isDefinedAndNotNull(property.value)) {

            // get the descriptor to insert the description tooltip
            var propertyDescriptor = descriptors[property.property];

            // ensure we're not dealing with a sensitive property
            if (!nf.Common.isSensitiveProperty(propertyDescriptor)) {

                // get details about the location of the cell
                var cellNode = $(propertyGrid.getCellNode(row, cell));
                var offset = cellNode.offset();

                // create the wrapper
                var wrapper = $('<div class="property-detail"></div>').css({
                    'z-index': 1999,
                    'position': 'absolute',
                    'background': 'white',
                    'padding': '5px',
                    'overflow': 'hidden',
                    'border': '3px solid #365C6A',
                    'box-shadow': '4px 4px 6px rgba(0, 0, 0, 0.9)',
                    'cursor': 'move',
                    'top': offset.top - 5,
                    'left': offset.left - 5
                }).appendTo('body');

                var allowableValues = nf.Common.getAllowableValues(propertyDescriptor);
                if ($.isArray(allowableValues)) {
                    // prevent dragging over the combo
                    wrapper.draggable({
                        cancel: '.button, .combo',
                        containment: 'parent'
                    });

                    // create the read only options
                    var options = [];
                    $.each(allowableValues, function (i, allowableValue) {
                        options.push({
                            text: allowableValue.displayName,
                            value: allowableValue.value,
                            description: nf.Common.escapeHtml(allowableValue.description),
                            disabled: true
                        });
                    });

                    // ensure the options there is at least one option
                    if (options.length === 0) {
                        options.push({
                            text: 'No value',
                            value: null,
                            optionClass: 'unset',
                            disabled: true
                        });
                    }

                    // determine the max height
                    var windowHeight = $(window).height();
                    var maxHeight = windowHeight - (offset.top + cellNode.height()) - 16;
                    var width = cellNode.width() - 16;

                    // build the combo field
                    $('<div class="value-combo combo"></div>').width(width).combo({
                        options: options,
                        maxHeight: maxHeight,
                        selectedOption: {
                            value: property.value
                        }
                    }).appendTo(wrapper);
                    
                    $('<div class="button button-normal">Ok</div>').css({
                        'margin': '0 0 0 5px',
                        'float': 'left'
                    }).on('click', function () {
                        wrapper.hide().remove();
                    }).appendTo(wrapper);
                } else {
                    var editor = null;

                    // so the nfel editor is appropriate
                    if (nf.Common.supportsEl(propertyDescriptor)) {
                        var languageId = 'nfel';
                        var editorClass = languageId + '-editor';

                        // prevent dragging over the nf editor
                        wrapper.draggable({
                            cancel: 'input, textarea, pre, .button, .' + editorClass,
                            containment: 'parent'
                        });

                        // create the editor
                        editor = $('<div></div>').addClass(editorClass).appendTo(wrapper).nfeditor({
                            languageId: languageId,
                            width: cellNode.width(),
                            content: property.value,
                            minWidth: 175,
                            minHeight: 100,
                            readOnly: true,
                            resizable: true
                        });
                    } else {
                        // prevent dragging over standard components
                        wrapper.draggable({
                            containment: 'parent'
                        });

                        // create the input field
                        $('<textarea hidefocus rows="5" readonly="readonly"/>').css({
                            'background': 'white',
                            'width': cellNode.width() + 'px',
                            'height': '80px',
                            'border-width': '0',
                            'outline': '0',
                            'overflow-y': 'auto',
                            'resize': 'both',
                            'margin-bottom': '28px'
                        }).text(property.value).appendTo(wrapper);
                    }

                    // add an ok button that will remove the entire pop up
                    var ok = $('<div class="button button-normal">Ok</div>').on('click', function () {
                        // clean up the editor
                        if (editor !== null) {
                            editor.nfeditor('destroy');
                        }

                        // clean up the rest
                        wrapper.hide().remove();
                    });
                    $('<div></div>').css({
                        'position': 'absolute',
                        'bottom': '0',
                        'left': '0',
                        'right': '0',
                        'padding': '0 3px 5px'
                    }).append(ok).append('<div class="clear"></div>').appendTo(wrapper);
                }
            }
        }
    };

    var initPropertiesTable = function (table, options) {
        // function for formatting the property name
        var nameFormatter = function (row, cell, value, columnDef, dataContext) {
            var nameWidthOffset = 10;
            var cellContent = $('<div></div>');

            // format the contents
            var formattedValue = $('<span/>').addClass('table-cell').text(value).appendTo(cellContent);
            if (dataContext.type === 'required') {
                formattedValue.addClass('required');
            }

            // get the property descriptor
            var descriptors = table.data('descriptors');
            var propertyDescriptor = descriptors[dataContext.property];

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
                // get the property descriptor
                var descriptors = table.data('descriptors');
                var propertyDescriptor = descriptors[dataContext.property];

                // determine if the property is sensitive
                if (nf.Common.isSensitiveProperty(propertyDescriptor)) {
                    valueMarkup = '<span class="table-cell sensitive">Sensitive value set</span>';
                } else {
                    // if there are allowable values, attempt to swap out for the display name
                    var allowableValues = nf.Common.getAllowableValues(propertyDescriptor);
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

        var propertyColumns = [
            {id: 'property', field: 'displayName', name: 'Property', sortable: false, resizable: true, rerenderOnResize: true, formatter: nameFormatter},
            {id: 'value', field: 'value', name: 'Value', sortable: false, resizable: true, cssClass: 'pointer', rerenderOnResize: true, formatter: valueFormatter}
        ];

        if (options.readOnly !== true) {
            // custom formatter for the actions column
            var actionFormatter = function (row, cell, value, columnDef, dataContext) {
                var markup = '';

                // allow user defined properties to be removed
                if (dataContext.type === 'userDefined') {
                    markup = '<img src="images/iconDelete.png" title="Delete" class="delete-property pointer" style="margin-top: 2px" />';
                }

                return markup;
            };
            propertyColumns.push({id: "actions", name: "&nbsp;", minWidth: 20, width: 20, formatter: actionFormatter});
        }

        var propertyConfigurationOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            editable: options.readOnly !== true,
            enableAddRow: false,
            autoEdit: false
        };

        // initialize the dataview
        var propertyData = new Slick.Data.DataView({
            inlineFilters: false
        });
        propertyData.setItems([]);
        propertyData.setFilterArgs({
            searchString: '',
            property: 'hidden'
        });
        propertyData.setFilter(filter);
        propertyData.getItemMetadata = function (index) {
            var item = propertyData.getItem(index);

            // get the property descriptor
            var descriptors = table.data('descriptors');
            var propertyDescriptor = descriptors[item.property];

            // support el if specified or unsure yet (likely a dynamic property)
            if (nf.Common.isUndefinedOrNull(propertyDescriptor) || nf.Common.supportsEl(propertyDescriptor)) {
                return {
                    columns: {
                        value: {
                            editor: nfelEditor
                        }
                    }
                };
            } else {
                // check for allowable values which will drive which editor to use
                var allowableValues = nf.Common.getAllowableValues(propertyDescriptor);
                if ($.isArray(allowableValues)) {
                    return {
                        columns: {
                            value: {
                                editor: comboEditor
                            }
                        }
                    };
                } else {
                    return {
                        columns: {
                            value: {
                                editor: textEditor
                            }
                        }
                    };
                }
            }
        };

        // initialize the grid
        var propertyGrid = new Slick.Grid(table, propertyData, propertyColumns, propertyConfigurationOptions);
        propertyGrid.setSelectionModel(new Slick.RowSelectionModel());
        propertyGrid.onClick.subscribe(function (e, args) {
            if (propertyGrid.getColumns()[args.cell].id === 'value') {
                if (options.readOnly === true) {
                    var descriptors = table.data('descriptors');
                    showPropertyValue(propertyGrid, descriptors, args.row, args.cell);
                } else {
                    // edits the clicked cell
                    propertyGrid.gotoCell(args.row, args.cell, true);
                }

                // prevents standard edit logic
                e.stopImmediatePropagation();
            } else if (propertyGrid.getColumns()[args.cell].id === 'actions') {
                var target = $(e.target);
                if (target.hasClass('delete-property')) {
                    // mark the property in question for removal
                    var property = propertyData.getItem(args.row);
                    property.hidden = true;

                    // refresh the table
                    propertyData.updateItem(property.id, property);

                    // prevents standard edit logic
                    e.stopImmediatePropagation();
                }
            }
        });
        propertyGrid.onKeyDown.subscribe(function(e, args) {
            if (e.which === $.ui.keyCode.ESCAPE) {
                var editorLock = propertyGrid.getEditorLock();
                if (editorLock.isActive()) {
                    editorLock.cancelCurrentEdit();
                    
                    // prevents standard cancel logic - standard logic does
                    // not stop propagation when escape is pressed
                    e.stopImmediatePropagation();
                    e.preventDefault();
                }
            }
        });

        // wire up the dataview to the grid
        propertyData.onRowCountChanged.subscribe(function (e, args) {
            propertyGrid.updateRowCount();
            propertyGrid.render();
        });
        propertyData.onRowsChanged.subscribe(function (e, args) {
            propertyGrid.invalidateRows(args.rows);
            propertyGrid.render();
        });

        // hold onto an instance of the grid and listen for mouse events to add tooltips where appropriate
        table.data('gridInstance', propertyGrid).on('mouseenter', 'div.slick-cell', function (e) {
            var infoIcon = $(this).find('img.icon-info');
            if (infoIcon.length && !infoIcon.data('qtip')) {
                var property = $(this).find('span.property-descriptor-name').text();

                // get the property descriptor
                var descriptors = table.data('descriptors');
                var propertyDescriptor = descriptors[property];

                // get the history
                var history = table.data('history');
                var propertyHistory = history[property];

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

    var saveRow = function (table) {
        // get the property grid to commit the current edit
        var propertyGrid = table.data('gridInstance');
        if (nf.Common.isDefinedAndNotNull(propertyGrid)) {
            var editController = propertyGrid.getEditController();
            editController.commitCurrentEdit();
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
        return item.hidden === false;
    };

    /**
     * Loads the specified properties.
     * 
     * @param {type} table 
     * @param {type} properties
     * @param {type} descriptors
     * @param {type} history
     */
    var loadProperties = function (table, properties, descriptors, history) {
        // save the descriptors and history
        table.data({
            'descriptors': descriptors,
            'history': history
        });

        // get the grid
        var propertyGrid = table.data('gridInstance');
        var propertyData = propertyGrid.getData();

        // generate the properties
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
                    if (nf.Common.isRequiredProperty(descriptor)) {
                        type = 'required';
                    } else if (nf.Common.isDynamicProperty(descriptor)) {
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
    };

    /**
     * Clears the property table container.
     * 
     * @param {jQuery} propertyTableContainer
     */
    var clear = function (propertyTableContainer) {
        var options = propertyTableContainer.data('options');
        if (options.readOnly === true) {
            nf.Common.removeAllPropertyDetailDialogs();
        } else {
            // clear any existing new property dialogs
            if (nf.Common.isDefinedAndNotNull(options.newPropertyDialogContainer)) {
                $(options.newPropertyDialogContainer).children('div.new-property-dialog').hide();
            }
        }

        // clean up data
        var table = propertyTableContainer.find('div.property-table');
        table.removeData('descriptors history');

        // clean up any tooltips that may have been generated
        nf.Common.cleanUpTooltips(table, 'img.icon-info');

        // clear the data in the grid
        var propertyGrid = table.data('gridInstance');
        var propertyData = propertyGrid.getData();
        propertyData.setItems([]);
    };

    var methods = {
        /**
         * Initializes the tag cloud.
         * 
         * @argument {object} options The options for the tag cloud
         */
        init: function (options) {
            return this.each(function () {
                // ensure the options have been properly specified
                if (nf.Common.isDefinedAndNotNull(options)) {
                    // get the tag cloud
                    var propertyTableContainer = $(this);

                    // clear any current contents, remote events, and store options
                    propertyTableContainer.empty().unbind().data('options', options);

                    // build the component
                    var header = $('<div class="properties-header"></div>').appendTo(propertyTableContainer);
                    $('<div class="required-property-note">Required field</div>').appendTo(header);

                    // build the table
                    var table = $('<div class="property-table"></div>').appendTo(propertyTableContainer);

                    // optionally add a add new property button
                    if (options.readOnly !== true && nf.Common.isDefinedAndNotNull(options.newPropertyDialogContainer)) {
                        // build the new property dialog
                        var newPropertyDialogMarkup = 
                                '<div class="new-property-dialog dialog cancellable">' +
                                    '<div>' +
                                        '<div class="setting-name">Property name</div>' +
                                        '<div class="setting-field new-property-name-container">' +
                                            '<input class="new-property-name" type="text"/>' +
                                        '</div>' +
                                    '</div>' +
                                    '<div class="new-property-button-container">' +
                                        '<div class="new-property-ok button button-normal">Ok</div>' +
                                        '<div class="new-property-cancel button button-normal">Cancel</div>' +
                                        '<div class="clear"></div>' +
                                    '</div>' +
                                '</div>';

                        var newPropertyDialog = $(newPropertyDialogMarkup).appendTo(options.newPropertyDialogContainer);
                        var newPropertyNameField = newPropertyDialog.find('input.new-property-name');

                        var add = function () {
                            var propertyName = $.trim(newPropertyNameField.val());

                            // ensure the property name and value is specified
                            if (propertyName !== '') {
                                // load the property descriptor if possible
                                if (typeof options.descriptorDeferred === 'function') {
                                    options.descriptorDeferred(propertyName).done(function(response) {
                                        var descriptor = response.propertyDescriptor;
                                        
                                        // store the descriptor for use later
                                        var descriptors = table.data('descriptors');
                                        if (!nf.Common.isUndefined(descriptors)) {
                                            descriptors[descriptor.name] = descriptor;
                                        }
                                    });
                                }
                                
                                // add a row for the new property
                                var propertyGrid = table.data('gridInstance');
                                var propertyData = propertyGrid.getData();
                                var id = propertyData.getLength(); 
                                propertyData.addItem({
                                    id: id,
                                    hidden: false,
                                    property: propertyName,
                                    displayName: propertyName,
                                    previousValue: null,
                                    value: null,
                                    type: 'userDefined'
                                });
                                
                                // select the new properties row
                                var row = propertyData.getRowById(id);
                                propertyGrid.setSelectedRows([row]);
                                propertyGrid.scrollRowIntoView(row);
                            } else {
                                nf.Dialog.showOkDialog({
                                    dialogContent: 'Property name must be specified.',
                                    overlayBackground: false
                                });
                            }

                            // close the dialog
                            newPropertyDialog.hide();
                        };

                        var cancel = function () {
                            newPropertyDialog.hide();
                        };
                        
                        // enable enter to add
                        newPropertyNameField.on('keydown', function (e) {
                            var code = e.keyCode ? e.keyCode : e.which;
                            if (code === $.ui.keyCode.ENTER) {
                                add();
                            }
                        });

                        // make the new property dialog draggable
                        newPropertyDialog.draggable({
                            cancel: 'input, textarea, pre, .button, .' + editorClass,
                            containment: 'body'
                        }).on('click', 'div.new-property-ok', add).on('click', 'div.new-property-cancel', cancel);

                        // build the control to open the new property dialog
                        var addProperty = $('<div class="add-property"></div>').appendTo(header);
                        $('<div class="add-property-icon add-icon-bg"></div>').on('click', function () {
                            // close all fields currently being edited
                            saveRow(table);

                            // clear the dialog
                            newPropertyNameField.val('');

                            // open the new property dialog
                            newPropertyDialog.center().show();

                            // set the initial focus
                            newPropertyNameField.focus();
                        }).on('mouseenter', function () {
                            $(this).removeClass('add-icon-bg').addClass('add-icon-bg-hover');
                        }).on('mouseleave', function () {
                            $(this).removeClass('add-icon-bg-hover').addClass('add-icon-bg');
                        }).appendTo(addProperty);
                        $('<div class="add-property-text">New property</div>').appendTo(addProperty);
                    }
                    $('<div class="clear"></div>').appendTo(header);

                    // initializes the properties table
                    initPropertiesTable(table, options);
                }
            });
        },
        
        /**
         * Loads the specified properties.
         * 
         * @argument {object} properties        The properties
         * @argument {map} descriptors          The property descriptors (property name -> property descriptor)
         * @argument {map} history
         */
        loadProperties: function (properties, descriptors, history) {
            return this.each(function () {
                var table = $(this).find('div.property-table');
                loadProperties(table, properties, descriptors, history);
            });
        },
        
        /**
         * Saves the last edited row in the specified grid.
         */
        saveRow: function () {
            return this.each(function () {
                var table = $(this).find('div.property-table');
                saveRow(table);
            });
        },
        
        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            return this.each(function () {
                var table = $(this).find('div.property-table');
                var propertyGrid = table.data('gridInstance');
                if (nf.Common.isDefinedAndNotNull(propertyGrid)) {
                    propertyGrid.resizeCanvas();
                }
            });
        },
        
        /**
         * Cancels the edit in the specified row.
         */
        cancelEdit: function () {
            return this.each(function () {
                var table = $(this).find('div.property-table');
                var propertyGrid = table.data('gridInstance');
                if (nf.Common.isDefinedAndNotNull(propertyGrid)) {
                    var editController = propertyGrid.getEditController();
                    editController.cancelCurrentEdit();
                }
            });
        },
        
        /**
         * Destroys the property table.
         */
        destroy: function () {
            return this.each(function () {
                var propertyTableContainer = $(this);
                var options = propertyTableContainer.data('options');
                
                // clear the property table container
                clear(propertyTableContainer);
                
                // clear any existing new property dialogs
                if (nf.Common.isDefinedAndNotNull(options.newPropertyDialogContainer)) {
                    $(options.newPropertyDialogContainer).children('div.new-property-dialog').remove();
                }
            });
        },
        
        /**
         * Clears the property table.
         */
        clear: function () {
            return this.each(function () {
                clear($(this));
            });
        },
        
        /**
         * Determines if a save is required for the first matching element.
         */
        isSaveRequired: function () {
            var isSaveRequired = false;

            this.each(function () {
                // get the property grid
                var table = $(this).find('div.property-table');
                var propertyGrid = table.data('gridInstance');
                var propertyData = propertyGrid.getData();

                // determine if any of the properties have changed
                $.each(propertyData.getItems(), function () {
                    if (this.value !== this.previousValue) {
                        isSaveRequired = true;
                        return false;
                    }
                });

                return false;
            });

            return isSaveRequired;
        },
        
        /**
         * Marshalls the properties for the first matching element.
         */
        marshalProperties: function () {
            // properties
            var properties = {};

            this.each(function () {
                // get the property grid data
                var table = $(this).find('div.property-table');
                var propertyGrid = table.data('gridInstance');
                var propertyData = propertyGrid.getData();
                $.each(propertyData.getItems(), function () {
                    if (this.hidden === true) {
                        // hidden properties were removed by the user, clear the value
                        properties[this.property] = null;
                    } else if (this.value !== this.previousValue) {
                        // the value has changed
                        properties[this.property] = this.value;
                    }
                });

                return false;
            });

            return properties;
        }
    };

    $.fn.propertytable = function (method) {
        if (methods[method]) {
            return methods[method].apply(this, Array.prototype.slice.call(arguments, 1));
        } else {
            return methods.init.apply(this, arguments);
        }
    };
})(jQuery);