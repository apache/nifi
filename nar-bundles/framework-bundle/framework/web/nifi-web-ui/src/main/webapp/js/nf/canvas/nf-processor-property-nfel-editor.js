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
nf.ProcessorPropertyNfelEditor = function (args) {
    var scope = this;
    var initialValue = '';
    var previousValue;
    var propertyDescriptor;
    var isEmpty;
    var wrapper;
    var editor;

    this.init = function () {
        var container = $('body');

        // get the property descriptor for this property
        var details = $('#processor-configuration').data('processorDetails');
        propertyDescriptor = details.config.descriptors[args.item.property];

        // determine if this is a sensitive property
        var sensitive = nf.ProcessorPropertyTable.isSensitiveProperty(propertyDescriptor);

        // record the previous value
        previousValue = args.item[args.column.field];

        var languageId = 'nfel';
        var editorClass = languageId + '-editor';

        // create the wrapper
        wrapper = $('<div></div>').addClass('slickgrid-nfel-editor').css({
            'z-index': 100000,
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
        var sensitive = nf.ProcessorPropertyTable.isSensitiveProperty(propertyDescriptor);

        // determine the value to use when populating the text field
        if (nf.Common.isDefinedAndNotNull(item[args.column.field])) {
            if (sensitive) {
                initialValue = nf.ProcessorPropertyTable.config.sensitiveText;
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
                if (nf.ProcessorPropertyTable.isRequiredProperty(propertyDescriptor)) {
                    if (nf.Common.isBlank(propertyDescriptor.defaultValue)) {
                        // reset to the previous value if available
                        if (nf.Common.isDefinedAndNotNull(previousValue)) {
                            return previousValue;
                        } else {
                            return undefined;
                        }
                    } else {
                        return propertyDescriptor.defaultValue;
                    }
                } else {
                    // if the property is not required, clear the value
                    return undefined;
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