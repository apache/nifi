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
nf.ProcessorPropertyComboEditor = function (args) {
    var scope = this;
    var initialValue = null;
    var wrapper;
    var combo;

    this.init = function () {
        var container = $('body');

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

        // identify the property descriptor - property descriptor is never null/undefined here... in order
        // to use this editor, the property descriptor would have had to indicate a set of allowable values
        var processorDetails = $('#processor-configuration').data('processorDetails');
        var propertyDescriptor = processorDetails.config.descriptors[args.item.property];

        // check for allowable values which will drive which editor to use
        var allowableValues = nf.ProcessorPropertyTable.getAllowableValues(propertyDescriptor);

        // show the output port options
        var options = [];
        if (propertyDescriptor.required === false) {
            options.push({
                text: 'No value',
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
        // identify the property descriptor
        var processorDetails = $('#processor-configuration').data('processorDetails');
        var propertyDescriptor = processorDetails.config.descriptors[item.property];

        // select as appropriate
        if (nf.Common.isDefinedAndNotNull(item.value)) {
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