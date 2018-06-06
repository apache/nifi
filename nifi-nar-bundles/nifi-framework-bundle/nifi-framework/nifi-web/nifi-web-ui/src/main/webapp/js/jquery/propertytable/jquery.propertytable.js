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

/* requires modal, combo, qtip, and nfeditor plugins to be loaded first*/

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'Slick',
                'nf.Common',
                'nf.UniversalCapture',
                'nf.Dialog',
                'nf.Storage',
                'nf.Client',
                'nf.ErrorHandler',
                'nf.ProcessGroupConfiguration',
                'nf.Settings'],
            function ($,
                      Slick,
                      nfCommon,
                      nfUniversalCapture,
                      nfDialog,
                      nfStorage,
                      nfClient,
                      nfErrorHandler,
                      nfProcessGroupConfiguration,
                      nfSettings) {
                factory($,
                    Slick,
                    nfCommon,
                    nfUniversalCapture,
                    nfDialog,
                    nfStorage,
                    nfClient,
                    nfErrorHandler,
                    nfProcessGroupConfiguration,
                    nfSettings);
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        factory(require('jquery'),
            require('Slick'),
            require('nf.Common'),
            require('nf.UniversalCapture'),
            require('nf.Dialog'),
            require('nf.Storage'),
            require('nf.Client'),
            require('nf.ErrorHandler'),
            require('nf.ProcessGroupConfiguration'),
            require('nf.Settings'));
    } else {
        factory(root.$,
            root.Slick,
            root.nf.Common,
            root.nf.UniversalCapture,
            root.nf.Dialog,
            root.nf.Storage,
            root.nf.Client,
            root.nf.ErrorHandler,
            root.nf.ProcessGroupConfiguration,
            root.nf.Settings);
    }
}(this, function ($,
                  Slick,
                  nfCommon,
                  nfUniversalCapture,
                  nfDialog,
                  nfStorage,
                  nfClient,
                  nfErrorHandler,
                  nfProcessGroupConfiguration,
                  nfSettings) {

    var languageId = 'nfel';
    var editorClass = languageId + '-editor';
    var groupId = null;

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
            // determine if this is a sensitive property
            var isEmptyChecked = false;
            var sensitive = nfCommon.isSensitiveProperty(propertyDescriptor);

            // determine the value to use when populating the text field
            if (nfCommon.isDefinedAndNotNull(item[args.column.field])) {
                if (sensitive) {
                    initialValue = nfCommon.config.sensitiveText;
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
                        if (sensitiveInput.val() === nfCommon.config.sensitiveText) {
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
                    if (nfCommon.isRequiredProperty(propertyDescriptor)) {
                        if (nfCommon.isBlank(propertyDescriptor.defaultValue)) {
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
            var sensitive = nfCommon.isSensitiveProperty(propertyDescriptor);

            // record the previous value
            previousValue = args.item[args.column.field];

            var languageId = 'nfel';
            var editorClass = languageId + '-editor';

            // create the wrapper
            wrapper = $('<div></div>').addClass('slickgrid-nfel-editor').css({
                'z-index': 14000,
                'position': 'absolute',
                'padding': '10px 20px',
                'overflow': 'hidden',
                'border-radius': '2px',
                'box-shadow': 'rgba(0, 0, 0, 0.247059) 0px 2px 5px',
                'background-color': 'rgb(255, 255, 255)',
                'cursor': 'move',
                'transform': 'translate3d(0px, 0px, 0px)'
            }).draggable({
                cancel: 'input, textarea, pre, .nf-checkbox, .button, .' + editorClass,
                containment: 'parent'
            }).appendTo(container);

            // create the editor
            editor = $('<div></div>').addClass(editorClass).appendTo(wrapper).nfeditor({
                languageId: languageId,
                width: (args.position.width < 212) ? 212 : args.position.width,
                minWidth: 212,
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
            editor.nfeditor('refresh');
        };

        this.position = function (position) {
            wrapper.css({
                'top': position.top - 21,
                'left': position.left - 43
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
            var sensitive = nfCommon.isSensitiveProperty(propertyDescriptor);

            // determine the value to use when populating the text field
            if (nfCommon.isDefinedAndNotNull(item[args.column.field])) {
                if (sensitive) {
                    initialValue = nfCommon.config.sensitiveText;
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
                    if (nfCommon.isRequiredProperty(propertyDescriptor)) {
                        if (nfCommon.isBlank(propertyDescriptor.defaultValue)) {
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

            // get the options
            var propertyContainer = gridContainer.closest('.property-container');
            var configurationOptions = propertyContainer.data('options');

            // create the wrapper
            wrapper = $('<div class="combo-editor"></div>').css({
                'z-index': 1999,
                'position': 'absolute',
                'padding': '10px 20px',
                'overflow': 'hidden',
                'border-radius': '2px',
                'box-shadow': 'rgba(0, 0, 0, 0.247059) 0px 2px 5px',
                'background-color': 'rgb(255, 255, 255)',
                'cursor': 'move',
                'transform': 'translate3d(0px, 0px, 0px)'
            }).draggable({
                cancel: '.button, .combo',
                containment: 'parent'
            }).appendTo(container);

            // check for allowable values which will drive which editor to use
            var allowableValues = nfCommon.getAllowableValues(propertyDescriptor);

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
                $.each(allowableValues, function (i, allowableValueEntity) {
                    var allowableValue = allowableValueEntity.allowableValue;
                    options.push({
                        text: allowableValue.displayName,
                        value: allowableValue.value,
                        disabled: allowableValueEntity.canRead === false && allowableValue.value !== args.item['previousValue'],
                        description: nfCommon.escapeHtml(allowableValue.description)
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

            // if this descriptor identifies a controller service, provide a way to create one
            if (nfCommon.isDefinedAndNotNull(propertyDescriptor.identifiesControllerService)) {
                options.push({
                    text: 'Create new service...',
                    value: undefined,
                    optionClass: 'unset'
                });
            }

            // determine the max height
            var position = args.position;
            var windowHeight = $(window).height();
            var maxHeight = windowHeight - position.bottom - 16;

            // build the combo field
            combo = $('<div class="value-combo combo"></div>').combo({
                options: options,
                maxHeight: maxHeight,
                select: function (option) {
                    if (typeof option.value === 'undefined') {
                        // cancel the current edit
                        scope.cancel();

                        // prompt for the new service type
                        promptForNewControllerService(gridContainer, args.grid, args.item, propertyDescriptor.identifiesControllerService, propertyDescriptor.identifiesControllerServiceBundle, configurationOptions);
                    }
                }
            }).css({
                'margin-top': '10px',
                'margin-bottom': '10px',
                'width': ((position.width - 16) < 212) ? 212 : (position.width - 16) + 'px'
            }).appendTo(wrapper);

            // add buttons for handling user input
            var cancel = $('<div class="secondary-button">Cancel</div>').css({
                'color': '#004849',
                'background': '#E3E8EB'
            }).hover(
                function () {
                    $(this).css('background', '#C7D2D7');
                }, function () {
                    $(this).css('background', '#E3E8EB');
                }).on('click', scope.cancel);
            var ok = $('<div class="button">Ok</div>').css({
                'color': '#fff',
                'background': '#728E9B'
            }).hover(
                function () {
                    $(this).css('background', '#004849');
                }, function () {
                    $(this).css('background', '#728E9B');
                }).on('click', scope.save);

            $('<div></div>').css({
                'position': 'relative',
                'top': '10px',
                'left': '20px',
                'width': '212px',
                'clear': 'both',
                'float': 'right'
            }).append(ok).append(cancel).appendTo(wrapper);

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
                'top': position.top - 24,
                'left': position.left - 20
            });
        };

        this.destroy = function () {
            combo.combo('destroy');
            wrapper.remove();
        };

        this.focus = function () {
        };

        this.loadValue = function (item) {
            // select as appropriate
            if (!nfCommon.isUndefined(item.value)) {
                initialValue = item.value;

                combo.combo('setSelectedOption', {
                    value: item.value
                });
            } else if (nfCommon.isDefinedAndNotNull(propertyDescriptor.defaultValue)) {
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
        nfUniversalCapture.removeAllPropertyDetailDialogs();

        // get the property in question
        var propertyData = propertyGrid.getData();
        var property = propertyData.getItem(row);

        // ensure there is a value
        if (nfCommon.isDefinedAndNotNull(property.value)) {

            // get the descriptor to insert the description tooltip
            var propertyDescriptor = descriptors[property.property];

            // ensure we're not dealing with a sensitive property
            if (!nfCommon.isSensitiveProperty(propertyDescriptor)) {

                // get details about the location of the cell
                var cellNode = $(propertyGrid.getCellNode(row, cell));
                var offset = cellNode.offset();

                // create the wrapper
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
                    'top': offset.top - 24,
                    'left': offset.left - 20
                }).appendTo('body');

                var allowableValues = nfCommon.getAllowableValues(propertyDescriptor);
                if ($.isArray(allowableValues)) {
                    // prevent dragging over the combo
                    wrapper.draggable({
                        cancel: '.button, .combo',
                        containment: 'parent'
                    });

                    // create the read only options
                    var options = [];
                    $.each(allowableValues, function (i, allowableValueEntity) {
                        var allowableValue = allowableValueEntity.allowableValue;
                        options.push({
                            text: allowableValue.displayName,
                            value: allowableValue.value,
                            description: nfCommon.escapeHtml(allowableValue.description),
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
                    $('<div class="value-combo combo"></div>').css({
                        'width': width,
                        'margin-top': '10px',
                        'margin-bottom': '10px'
                    }).combo({
                        options: options,
                        maxHeight: maxHeight,
                        selectedOption: {
                            value: property.value
                        }
                    }).appendTo(wrapper);

                    $('<div class="button">Ok</div>').css({
                        'position': 'relative',
                        'top': '10px',
                        'left': '20px'
                    }).hover(
                        function () {
                            $(this).css('background', '#004849');
                        }, function () {
                            $(this).css('background', '#728E9B');
                        }).on('click', function () {
                        wrapper.hide().remove();
                    }).appendTo(wrapper);
                } else {
                    var editor = null;

                    // so the nfel editor is appropriate
                    if (nfCommon.supportsEl(propertyDescriptor)) {
                        var languageId = 'nfel';
                        var editorClass = languageId + '-editor';

                        // prevent dragging over the nf editor
                        wrapper.css({
                            'z-index': 1999,
                            'position': 'absolute',
                            'padding': '10px 20px',
                            'overflow': 'hidden',
                            'border-radius': '2px',
                            'box-shadow': 'rgba(0, 0, 0, 0.247059) 0px 2px 5px',
                            'background-color': 'rgb(255, 255, 255)',
                            'cursor': 'move',
                            'transform': 'translate3d(0px, 0px, 0px)',
                            'top': offset.top - 22,
                            'left': offset.left - 43
                        }).draggable({
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
                            resizable: true,
                            escape: function () {
                                cleanUp();
                            }
                        });
                    } else {
                        wrapper.css({
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
                        });

                        // create the input field
                        $('<textarea hidefocus rows="5" readonly="readonly"/>').css({
                            'height': '80px',
                            'resize': 'both',
                            'width': cellNode.width() + 'px',
                            'margin': '10px 0px',
                            'white-space': 'pre'
                        }).text(property.value).on('keydown', function (evt) {
                            if (evt.which === $.ui.keyCode.ESCAPE) {
                                cleanUp();

                                evt.stopImmediatePropagation();
                                evt.preventDefault();
                            }
                        }).appendTo(wrapper);

                        // prevent dragging over standard components
                        wrapper.draggable({
                            containment: 'parent'
                        });
                    }

                    var cleanUp = function () {
                        // clean up the editor
                        if (editor !== null) {
                            editor.nfeditor('destroy');
                        }

                        // clean up the rest
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
                }
            }
        }
    };

    /**
     * Gets the available controller services that implement the specified type and
     * prompts the user to create one.
     *
     * @param {jQuery} gridContainer The grid container
     * @param {slickgrid} grid The grid
     * @param {object} item The item
     * @param {string} serviceType The type of service to create
     * @param {object} bundle The bundle the service impl should implement
     * @param {object} configurationOptions The configuration options
     */
    var promptForNewControllerService = function (gridContainer, grid, item, serviceType, bundle, configurationOptions) {
        $.ajax({
            type: 'GET',
            url: '../nifi-api/flow/controller-service-types',
            data: {
                'serviceType': serviceType,
                'serviceBundleGroup': bundle.group,
                'serviceBundleArtifact': bundle.artifact,
                'serviceBundleVersion': bundle.version
            },
            dataType: 'json'
        }).done(function (response) {
            // get the property descriptor
            var descriptors = gridContainer.data('descriptors');
            var propertyDescriptor = descriptors[item.property];

            var controllerServiceLookup = d3.map();
            var options = [];
            $.each(response.controllerServiceTypes, function (i, controllerServiceType) {
                controllerServiceLookup.set(i, controllerServiceType);
                options.push({
                    text: nfCommon.formatType(controllerServiceType),
                    value: i,
                    description: nfCommon.escapeHtml(controllerServiceType.description)
                });
            });

            // ensure there are some applicable controller services
            if (options.length === 0) {
                nfDialog.showOkDialog({
                    headerText: 'Controller Service',
                    dialogContent: 'No controller service types found that are applicable for this property.'
                });
            } else {
                var newControllerServiceDialogMarkup =
                    '<div id="new-inline-controller-service-dialog" class="hidden dialog medium-dialog cancellable">' +
                        '<div class="dialog-content">' +
                            '<div>' +
                                '<div class="setting-name">Requires Controller Service</div>' +
                                '<div class="setting-field">' +
                                    '<div class="new-inline-controller-service-requirement"></div>' +
                                '</div>' +
                            '</div>' +
                            '<div>' +
                                '<div class="setting-name">Compatible Controller Services</div>' +
                                '<div class="setting-field">' +
                                    '<div class="new-inline-controller-service-combo"></div>' +
                                '</div>' +
                            '</div>' +
                            '<div>' +
                                '<div class="setting-name">Controller Service Name</div>' +
                                '<div class="setting-field">' +
                                    '<input type="text" class="new-inline-controller-service-name"/>' +
                                '</div>' +
                            '</div>' +
                            '<div>' +
                                '<div class="setting-name">Bundle</div>' +
                                '<div class="setting-field">' +
                                    '<div class="new-inline-controller-service-bundle"></div>' +
                                '</div>' +
                            '</div>' +
                            '<div>' +
                                '<div class="setting-name">Tags</div>' +
                                '<div class="setting-field">' +
                                    '<div class="new-inline-controller-service-tags"></div>' +
                                '</div>' +
                            '</div>' +
                            '<div>' +
                                '<div class="setting-name">Description</div>' +
                                '<div class="setting-field">' +
                                    '<div class="new-inline-controller-service-description"></div>' +
                                '</div>' +
                            '</div>' +
                        '</div>' +
                    '</div>';

                var newControllerServiceDialog = $(newControllerServiceDialogMarkup).appendTo(configurationOptions.dialogContainer);
                var newControllerServiceRequirement = newControllerServiceDialog.find('div.new-inline-controller-service-requirement');
                var newControllerServiceCombo = newControllerServiceDialog.find('div.new-inline-controller-service-combo');
                var newControllerServiceNameInput = newControllerServiceDialog.find('input.new-inline-controller-service-name');
                var newControllerServiceBundle = newControllerServiceDialog.find('div.new-inline-controller-service-bundle');
                var newControllerServiceTags = newControllerServiceDialog.find('div.new-inline-controller-service-tags');
                var newControllerServiceDescription = newControllerServiceDialog.find('div.new-inline-controller-service-description');

                // include the required service
                var formattedType = nfCommon.formatType({
                    'type': propertyDescriptor.identifiesControllerService,
                    'bundle': propertyDescriptor.identifiesControllerServiceBundle
                });
                var formattedBundle = nfCommon.formatBundle(propertyDescriptor.identifiesControllerServiceBundle);
                newControllerServiceRequirement.text(formattedType + ' from ' + formattedBundle);

                // sort the text version visible to the user
                options.sort(function (a, b) {
                    var aCS = controllerServiceLookup.get(a.value);
                    var bCS = controllerServiceLookup.get(b.value);

                    var aName = nfCommon.substringAfterLast(aCS.type, '.');
                    var bName = nfCommon.substringAfterLast(bCS.type, '.');

                    return aName === bName ? -nfCommon.sortVersion(aCS.bundle.version, bCS.bundle.version) : aName > bName ? 1 : -1;
                });

                // default to the first service
                var newControllerServiceNameDefault = nfCommon.formatClassName(controllerServiceLookup.get(0));
                newControllerServiceNameInput.val(newControllerServiceNameDefault);

                // build the combo field
                newControllerServiceCombo.combo({
                    options: options,
                    select: function (option) {
                        // set the service details
                        var service = controllerServiceLookup.get(option.value);
                        newControllerServiceBundle.text(nfCommon.formatBundle(service.bundle));
                        newControllerServiceTags.text(service.tags.join(', '));
                        newControllerServiceDescription.text(service.description);

                        // update default when no edits were made
                        if (newControllerServiceNameDefault === newControllerServiceNameInput.val().trim()) {
                            newControllerServiceNameDefault = nfCommon.formatClassName(service);
                            newControllerServiceNameInput.val(newControllerServiceNameDefault);
                        }
                    }
                });

                newControllerServiceDialog.modal({
                    headerText: 'Add Controller Service',
                    scrollableContentStyle: 'scrollable',
                    buttons: [{
                        buttonText: 'Create',
                        color: {
                            base: '#728E9B',
                            hover: '#004849',
                            text: '#ffffff'
                        },
                        handler: {
                            click: function () {
                                create();
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
                                    cancel();
                                }
                            }
                        }]
                });

                var create = function () {
                    var newControllerServiceKey = newControllerServiceCombo.combo('getSelectedOption').value;
                    var newControllerServiceType = controllerServiceLookup.get(newControllerServiceKey);
                    var newControllerServiceName = newControllerServiceNameInput.val();

                    // build the controller service entity
                    var controllerServiceEntity = {
                        'revision': nfClient.getRevision({
                            'revision': {
                                'version': 0,
                            }
                        }),
                        'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
                        'component': {
                            'type': newControllerServiceType.type,
                            'bundle': newControllerServiceType.bundle
                        }
                    };

                    // set custom name when specified
                    if (newControllerServiceName.trim() !== '') {
                        controllerServiceEntity.component.name = newControllerServiceName.trim();
                    }

                    // determine the appropriate uri for creating the controller service
                    var uri = '../nifi-api/controller/controller-services';
                    if (nfCommon.isDefinedAndNotNull(groupId)) {
                        uri = '../nifi-api/process-groups/' + encodeURIComponent(groupId) + '/controller-services';
                    }

                    // add the new controller service
                    $.ajax({
                        type: 'POST',
                        url: uri,
                        data: JSON.stringify(controllerServiceEntity),
                        dataType: 'json',
                        contentType: 'application/json'
                    }).done(function (response) {
                        // load the descriptor and update the property
                        configurationOptions.descriptorDeferred(item.property).done(function (descriptorResponse) {
                            var descriptor = descriptorResponse.propertyDescriptor;

                            // store the descriptor for use later
                            var descriptors = gridContainer.data('descriptors');
                            if (!nfCommon.isUndefined(descriptors)) {
                                descriptors[descriptor.name] = descriptor;
                            }

                            // add a row for the new property
                            var data = grid.getData();
                            data.updateItem(item.id, $.extend(item, {
                                value: response.component.id
                            }));

                            // close the dialog
                            newControllerServiceDialog.modal('hide');
                        });

                        // invoke callback if necessary
                        if (typeof configurationOptions.controllerServiceCreatedDeferred === 'function') {
                            configurationOptions.controllerServiceCreatedDeferred(response);
                        }
                    }).fail(nfErrorHandler.handleAjaxError);
                };

                var cancel = function () {
                    newControllerServiceDialog.modal('hide');
                };

                newControllerServiceDialog.modal('show');
            }
        }).fail(nfErrorHandler.handleAjaxError);
    };

    var initPropertiesTable = function (table, options) {
        // function for formatting the property name
        var nameFormatter = function (row, cell, value, columnDef, dataContext) {
            var nameWidthOffset = 30;
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
            if (nfCommon.isDefinedAndNotNull(propertyDescriptor)) {
                if (!nfCommon.isBlank(propertyDescriptor.description) || !nfCommon.isBlank(propertyDescriptor.defaultValue) || !nfCommon.isBlank(propertyDescriptor.supportsEl)) {
                    $('<div class="fa fa-question-circle" alt="Info" style="float: right;"></div>').appendTo(cellContent);
                    $('<span class="hidden property-descriptor-name"></span>').text(dataContext.property).appendTo(cellContent);
                    nameWidthOffset = 46; // 10 + icon width (10) + icon margin (6) + padding (20)
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
            if (nfCommon.isDefinedAndNotNull(value)) {
                // get the property descriptor
                var descriptors = table.data('descriptors');
                var propertyDescriptor = descriptors[dataContext.property];

                // determine if the property is sensitive
                if (nfCommon.isSensitiveProperty(propertyDescriptor)) {
                    valueMarkup = '<span class="table-cell sensitive">Sensitive value set</span>';
                } else {
                    var resolvedAllowableValue = false;

                    // if there are allowable values, attempt to swap out for the display name
                    var allowableValues = nfCommon.getAllowableValues(propertyDescriptor);
                    if ($.isArray(allowableValues)) {
                        $.each(allowableValues, function (_, allowableValueEntity) {
                            var allowableValue = allowableValueEntity.allowableValue;
                            if (value === allowableValue.value) {
                                value = allowableValue.displayName;
                                resolvedAllowableValue = true;
                                return false;
                            }
                        });
                    }

                    if (value === '') {
                        valueMarkup = '<span class="table-cell blank">Empty string set</span>';
                    } else {
                        if (!resolvedAllowableValue && nfCommon.isDefinedAndNotNull(propertyDescriptor.identifiesControllerService)) {
                            valueMarkup = '<span class="table-cell blank">Incompatible Controller Service Configured</div>';
                        } else {
                            valueMarkup = '<div class="table-cell value"><pre class="ellipsis">' + nfCommon.escapeHtml(value) + '</pre></div>';
                        }
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
            {
                id: 'property',
                field: 'displayName',
                name: 'Property',
                sortable: false,
                resizable: true,
                rerenderOnResize: true,
                formatter: nameFormatter
            },
            {
                id: 'value',
                field: 'value',
                name: 'Value',
                sortable: false,
                resizable: true,
                cssClass: 'pointer',
                rerenderOnResize: true,
                formatter: valueFormatter
            }
        ];

        // custom formatter for the actions column
        var actionFormatter = function (row, cell, value, columnDef, dataContext) {
            var markup = '';

            // get the property descriptor
            var descriptors = table.data('descriptors');
            var propertyDescriptor = descriptors[dataContext.property];

            var identifiesControllerService = nfCommon.isDefinedAndNotNull(propertyDescriptor.identifiesControllerService);
            var isConfigured = nfCommon.isDefinedAndNotNull(dataContext.value);

            // check to see if we should provide a button for going to a controller service
            if (identifiesControllerService && isConfigured && (options.supportsGoTo === true)) {
                // ensure the configured value is referencing a valid service
                $.each(propertyDescriptor.allowableValues, function (_, allowableValueEntity) {
                    var allowableValue = allowableValueEntity.allowableValue;
                    if (allowableValue.value === dataContext.value) {
                        markup += '<div class="pointer go-to-service fa fa-long-arrow-right" title="Go To"></div>';
                        return false;
                    }
                });
            }

            // allow user defined properties to be removed
            if (options.readOnly !== true && dataContext.type === 'userDefined') {
                markup += '<div title="Delete" class="delete-property pointer fa fa-trash"></div>';
            }

            return markup;
        };
        propertyColumns.push(
            {
                id: "actions",
                name: "&nbsp;",
                minWidth: 20,
                width: 20,
                formatter: actionFormatter
            });

        var propertyConfigurationOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            editable: options.readOnly !== true,
            enableAddRow: false,
            autoEdit: false,
            rowHeight: 24
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
            if (nfCommon.isUndefinedOrNull(propertyDescriptor) || nfCommon.supportsEl(propertyDescriptor)) {
                return {
                    columns: {
                        value: {
                            editor: nfelEditor
                        }
                    }
                };
            } else {
                // check for allowable values which will drive which editor to use
                var allowableValues = nfCommon.getAllowableValues(propertyDescriptor);
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

        var goToControllerService = function (property) {
            $.ajax({
                type: 'GET',
                url: '../nifi-api/controller-services/' + encodeURIComponent(property.value),
                dataType: 'json'
            }).done(function (controllerServiceEntity) {
                // close the dialog
                var dialog = table.closest('.dialog');
                if (dialog.hasClass('modal')) {
                    dialog.modal('hide');
                } else {
                    dialog.hide();
                }

                var controllerService = controllerServiceEntity.component;
                $.Deferred(function (deferred) {
                    if (nfCommon.isDefinedAndNotNull(controllerService.parentGroupId)) {
                        if ($('#process-group-configuration').is(':visible')) {
                            nfProcessGroupConfiguration.loadConfiguration(controllerService.parentGroupId).done(function () {
                                deferred.resolve();
                            });
                        } else {
                            nfProcessGroupConfiguration.showConfiguration(controllerService.parentGroupId).done(function () {
                                deferred.resolve();
                            });
                        }
                    } else {
                        if ($('#settings').is(':visible')) {
                            // reload the settings
                            nfSettings.loadSettings().done(function () {
                                deferred.resolve();
                            });
                        } else {
                            // reload the settings and show
                            nfSettings.showSettings().done(function () {
                                deferred.resolve();
                            });
                        }
                    }
                }).done(function () {
                    if (nfCommon.isDefinedAndNotNull(controllerService.parentGroupId)) {
                        nfProcessGroupConfiguration.selectControllerService(property.value);
                    } else {
                        nfSettings.selectControllerService(property.value);
                    }
                });
            }).fail(nfErrorHandler.handleAjaxError);
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
                var property = propertyData.getItem(args.row);

                var target = $(e.target);
                if (target.hasClass('delete-property')) {
                    // mark the property in question for removal and refresh the table
                    propertyData.updateItem(property.id, $.extend(property, {
                        hidden: true
                    }));

                    // prevents standard edit logic
                    e.stopImmediatePropagation();
                } else if (target.hasClass('go-to-service')) {
                    if (options.readOnly === true) {
                        goToControllerService(property);
                    } else {
                        // load the property descriptor if possible
                        if (typeof options.goToServiceDeferred === 'function') {
                            options.goToServiceDeferred().done(function () {
                                goToControllerService(property);
                            });
                        }
                    }
                }
            }
        });
        propertyGrid.onKeyDown.subscribe(function (e, args) {
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

        if (options.readOnly !== true) {
            propertyGrid.onBeforeCellEditorDestroy.subscribe(function (e, args) {
                setTimeout(function() {
                    propertyGrid.resizeCanvas();
                }, 50);
            });
        }

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
            var infoIcon = $(this).find('div.fa-question-circle');
            if (infoIcon.length && !infoIcon.data('qtip')) {
                var property = $(this).find('span.property-descriptor-name').text();

                // get the property descriptor
                var descriptors = table.data('descriptors');
                var propertyDescriptor = descriptors[property];

                // get the history
                var history = table.data('history');
                var propertyHistory = history[property];

                // format the tooltip
                var tooltip = nfCommon.formatPropertyTooltip(propertyDescriptor, propertyHistory);

                if (nfCommon.isDefinedAndNotNull(tooltip)) {
                    infoIcon.qtip($.extend({},
                        nfCommon.config.tooltipConfig,
                        {
                            content: tooltip
                        }));
                }
            }
        });
    };

    var saveRow = function (table) {
        // get the property grid to commit the current edit
        var propertyGrid = table.data('gridInstance');
        if (nfCommon.isDefinedAndNotNull(propertyGrid)) {
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
        if (nfCommon.isDefinedAndNotNull(properties)) {
            propertyData.beginUpdate();

            var i = 0;
            $.each(properties, function (name, value) {
                // get the property descriptor
                var descriptor = descriptors[name];

                // determine the property type
                var type = 'userDefined';
                var displayName = name;
                if (nfCommon.isDefinedAndNotNull(descriptor)) {
                    if (nfCommon.isRequiredProperty(descriptor)) {
                        type = 'required';
                    } else if (nfCommon.isDynamicProperty(descriptor)) {
                        type = 'userDefined';
                    } else {
                        type = 'optional';
                    }

                    // use the display name if possible
                    displayName = descriptor.displayName;

                    // determine the value
                    if (nfCommon.isNull(value) && nfCommon.isDefinedAndNotNull(descriptor.defaultValue)) {
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
            nfUniversalCapture.removeAllPropertyDetailDialogs();
        } else {
            // clear any existing new property dialogs
            if (nfCommon.isDefinedAndNotNull(options.dialogContainer)) {
                $('#new-property-dialog').modal("hide");
            }
        }

        // clean up data
        var table = propertyTableContainer.find('div.property-table');
        table.removeData('descriptors history');

        // clean up any tooltips that may have been generated
        nfCommon.cleanUpTooltips(table, 'div.fa-question-circle');

        // clear the data in the grid
        var propertyGrid = table.data('gridInstance');
        var propertyData = propertyGrid.getData();
        propertyData.setItems([]);
    };

    var methods = {
        /**
         * Create a property table. The options are specified in the following
         * format:
         *
         * {
         *   readOnly: true,
         *   dialogContainer: 'body',
         *   descriptorDeferred: function () {
         *      return $.Deferred(function (deferred) {
         *          deferred.resolve();
         *      }).promise;
         *   },
         *   supportsGoTo: true,
         *   goToServiceDeferred: function () {
         *      return $.Deferred(function (deferred) {
         *          deferred.resolve();
         *      }).promise;
         *   }
         * }
         *
         * @argument {object} options The options for the tag cloud
         */
        init: function (options) {
            return this.each(function () {
                // ensure the options have been properly specified
                if (nfCommon.isDefinedAndNotNull(options)) {
                    // get the tag cloud
                    var propertyTableContainer = $(this);

                    // clear any current contents, remote events, and store options
                    propertyTableContainer.empty().unbind().addClass('property-container').data('options', options);

                    // build the component
                    var header = $('<div class="properties-header"></div>').appendTo(propertyTableContainer);
                    $('<div class="required-property-note">Required field</div>').appendTo(header);

                    // build the table
                    var table = $('<div class="property-table"></div>').appendTo(propertyTableContainer);

                    // optionally add a add new property button
                    if (options.readOnly !== true && nfCommon.isDefinedAndNotNull(options.dialogContainer)) {
                        // build the new property dialog
                        var newPropertyDialogMarkup =
                            '<div id="new-property-dialog" class="dialog cancellable small-dialog hidden">' +
                                '<div class="dialog-content">' +
                                    '<div>' +
                                    '<div class="setting-name">Property name</div>' +
                                        '<div class="setting-field new-property-name-container">' +
                                            '<input class="new-property-name" type="text"/>' +
                                        '</div>' +
                                    '</div>' +
                                '</div>' +
                            '</div>';

                        var newPropertyDialog = $(newPropertyDialogMarkup).appendTo(options.dialogContainer);
                        var newPropertyNameField = newPropertyDialog.find('input.new-property-name');

                        newPropertyDialog.modal({
                            headerText: 'Add Property',
                            scrollableContentStyle: 'scrollable',
                            buttons: [{
                                buttonText: 'Ok',
                                color: {
                                    base: '#728E9B',
                                    hover: '#004849',
                                    text: '#ffffff'
                                },
                                handler: {
                                    click: function () {
                                        add();
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
                                            cancel();
                                        }
                                    }
                                }]
                        });

                        var add = function () {
                            var propertyName = $.trim(newPropertyNameField.val());

                            // ensure the property name is specified
                            if (propertyName !== '') {
                                var propertyGrid = table.data('gridInstance');
                                var propertyData = propertyGrid.getData();

                                // ensure the property name is unique
                                var existingItem = null;
                                $.each(propertyData.getItems(), function (_, item) {
                                    if (propertyName === item.property) {
                                        existingItem = item;
                                        return false;
                                    }
                                });

                                if (existingItem === null) {
                                    // load the descriptor and add the property
                                    options.descriptorDeferred(propertyName).done(function (response) {
                                        var descriptor = response.propertyDescriptor;

                                        // store the descriptor for use later
                                        var descriptors = table.data('descriptors');
                                        if (!nfCommon.isUndefined(descriptors)) {
                                            descriptors[descriptor.name] = descriptor;
                                        }

                                        // add a row for the new property
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
                                        propertyGrid.setActiveCell(row, propertyGrid.getColumnIndex('value'));
                                        propertyGrid.editActiveCell();
                                    });
                                } else {
                                    // if this row is currently hidden, clear the value and show it
                                    if (existingItem.hidden === true) {
                                        propertyData.updateItem(existingItem.id, $.extend(existingItem, {
                                            hidden: false,
                                            previousValue: null,
                                            value: null
                                        }));

                                        // select the new properties row
                                        var row = propertyData.getRowById(existingItem.id);
                                        propertyGrid.setActiveCell(row, propertyGrid.getColumnIndex('value'));
                                        propertyGrid.editActiveCell();
                                    } else {
                                        nfDialog.showOkDialog({
                                            headerText: 'Property Exists',
                                            dialogContent: 'A property with this name already exists.'
                                        });

                                        // select the existing properties row
                                        var row = propertyData.getRowById(existingItem.id);
                                        propertyGrid.setSelectedRows([row]);
                                        propertyGrid.scrollRowIntoView(row);
                                    }
                                }
                            } else {
                                nfDialog.showOkDialog({
                                    headerText: 'Property Name',
                                    dialogContent: 'Property name must be specified.'
                                });
                            }

                            // close the dialog
                            newPropertyDialog.modal('hide');
                        };

                        var cancel = function () {
                            newPropertyDialog.modal('hide');
                        };

                        // enable enter to add
                        newPropertyNameField.on('keydown', function (e) {
                            var code = e.keyCode ? e.keyCode : e.which;
                            if (code === $.ui.keyCode.ENTER) {
                                add();

                                // prevents the enter from propagating into the field for editing the new property value
                                e.stopImmediatePropagation();
                                e.preventDefault();
                            }
                        });

                        newPropertyDialog.on('click', 'div.new-property-ok', add).on('click', 'div.new-property-cancel', cancel);

                        // build the control to open the new property dialog
                        var addProperty = $('<div class="add-property"></div>').appendTo(header);
                        $('<button class="button fa fa-plus"></button>').on('click', function () {
                            // close all fields currently being edited
                            saveRow(table);

                            // clear the dialog
                            newPropertyNameField.val('');

                            // open the new property dialog
                            newPropertyDialog.modal('show');

                            // set the initial focus
                            newPropertyNameField.focus();
                        }).appendTo(addProperty);
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
                if (nfCommon.isDefinedAndNotNull(propertyGrid)) {
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
                if (nfCommon.isDefinedAndNotNull(propertyGrid)) {
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

                if (nfCommon.isDefinedAndNotNull(options)) {
                    // clear the property table container
                    clear(propertyTableContainer);

                    // clear any existing new property dialogs
                    if (nfCommon.isDefinedAndNotNull(options.dialogContainer)) {
                        $('#new-property-dialog').modal("hide");
                        $(options.dialogContainer).children('div.new-inline-controller-service-dialog').remove();
                    }
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
        },

        /**
         * Sets the current group id.
         */
        setGroupId: function (currentGroupId) {
            return this.each(function () {
                groupId = currentGroupId;
            });
        }
    };

    $.fn.propertytable = function (method) {
        if (methods[method]) {
            return methods[method].apply(this, Array.prototype.slice.call(arguments, 1));
        } else {
            return methods.init.apply(this, arguments);
        }
    };
}));