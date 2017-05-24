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

/* requires qtip plugin to be loaded first*/

/**
 * Create a new combo box. The options are specified in the following
 * format:
 *
 * {
 *   selectedOption: {
 *      value: 'option 2'
 *   },
 *   options: [{
 *      text: 'option 1',
 *      value: 'option 1',
 *      disabled: true,
 *      description: 'Option description...'
 *   }, {
 *      text: 'option 2',
 *      value: 'option 2',
 *      optionClass: 'my-style'
 *   }],
 *   maxHeight: 300,
 *   select: selectHandler
 * }
 *
 * Options have a label (specified as the text property that is rendered
 * to users) and a value which is not. Additionally, options can be marked
 * as disabled. A disabled option cannot be selected by a user but may be
 * programmatically selected (supporting the restoration of options that have
 * become invalid). It is up to the developer to ensure that the selected
 * option is not disabled.
 *
 * The optionClass option supports specifying a class to apply to the
 * option element.
 */

/**
 * jQuery plugin for a NiFi style combo box.
 *
 * @param {type} $
 * @returns {undefined}
 */
(function ($) {

    var isUndefined = function (obj) {
        return typeof obj === 'undefined';
    };

    var isNull = function (obj) {
        return obj === null;
    };

    var isDefinedAndNotNull = function (obj) {
        return !isUndefined(obj) && !isNull(obj);
    };

    var isBlank = function (str) {
        return isUndefined(str) || isNull(str) || str === '';
    };

    var selectOption = function (combo, text, value) {
        var config = combo.data('options');
        var comboText = combo.children('div.combo-text');
        var selectedOption;

        // attempt to match on value first
        if (isDefinedAndNotNull(value)) {
            $.each(config.options, function (i, option) {
                if (value === option.value) {
                    selectedOption = option;
                }
            });
        }

        // if no option values matched, try the text
        if (isUndefined(selectedOption)) {
            $.each(config.options, function (i, option) {
                if (isUndefined(selectedOption)) {
                    selectedOption = option;
                }

                if (text === option.text) {
                    selectedOption = option;
                }
            });
        }

        // ensure we found the selected option
        if (isDefinedAndNotNull(selectedOption)) {
            comboText.removeClass('selected-disabled-option').attr('title', selectedOption.text).text(selectedOption.text).data('text', selectedOption.text);

            // if the selected option is disabled show it
            if (selectedOption.disabled === true) {
                comboText.addClass('selected-disabled-option');
            }

            // store the selected option
            combo.data('selected-option', selectedOption);

            // fire a select event if applicable
            if (isDefinedAndNotNull(config.select)) {
                config.select.call(combo, selectedOption);
            }
        }

    };

    var methods = {

        /**
         * Initializes the combo box.
         *
         * @argument {object} options The options for the combo box
         */
        init: function (options) {
            return this.each(function () {
                // ensure the options have been properly specified
                if (isDefinedAndNotNull(options) &&
                    isDefinedAndNotNull(options.options)) {

                    // get the combo 
                    var combo = $(this);

                    // clear any current contents, remote events, and store options
                    combo.empty().unbind().data('options', options);

                    // add a div to hold the text
                    $('<div class="combo-text"></div>').appendTo(combo);

                    // add hover effect and handle a combo click
                    combo.addClass('combo-button-normal pointer combo').hover(function () {
                        combo.removeClass('button-normal').addClass('combo-button-over');
                    }, function () {
                        combo.removeClass('button-over').addClass('combo-button-normal');
                    }).click(function (event) {
                        var comboConfigOptions = combo.data('options');
                        
                        //add active styles
                        $(this).addClass('combo-open');

                        // determine the position of the element in question
                        var position = combo.offset();

                        // create the combo box options beneath it
                        var comboOptions = $('<div></div>').addClass('combo-options').css({
                            'position': 'absolute',
                            'left': position.left + 'px',
                            'top': (position.top + Math.round(combo.outerHeight()) - 1) + 'px',
                            'width': (Math.round(combo.outerWidth()) - 2) + 'px',
                            'overflow-y': 'auto'
                        });

                        // keep track of the overall height
                        var actualHeight = 0;

                        // set the max height if necessary
                        var maxHeight = -1;
                        if (isDefinedAndNotNull(comboConfigOptions.maxHeight)) {
                            maxHeight = parseInt(comboConfigOptions.maxHeight);
                            if (maxHeight > 0) {
                                comboOptions.css('max-height', maxHeight + 'px');
                            }
                        }

                        // create the list that will contain the options
                        var optionList = $('<ul></ul>').appendTo(comboOptions);

                        // process the options
                        $.each(comboConfigOptions.options, function (i, option) {
                            var optionText = $('<span class="combo-option-text"></span>').attr('title', option.text).text(option.text);
                            var optionValue = $('<span class="hidden"></span>').text(option.value);

                            // create each option
                            var optionElement = $('<li></li>').addClass(option.optionClass).append(optionText).append(optionValue).appendTo(optionList);

                            // this is option is enabled register appropriate listeners
                            if (option.disabled === true) {
                                optionElement.addClass('unset disabled-option');
                            } else {
                                optionElement.click(function () {
                                    //remove active styles
                                    $('.combo').removeClass('combo-open');

                                    // select the option
                                    selectOption(combo, option.text, option.value);

                                    // click the glass pane which will hide the options
                                    $('.combo-glass-pane').click();
                                }).hover(function () {
                                    $(this).addClass('pointer').css('background', '#eaeef0');
                                }, function () {
                                    $(this).removeClass('pointer').css('background', '#ffffff');
                                });
                            }

                            if (!isBlank(option.description)) {
                                $('<div style="float: right; line-height: 32px;" class="fa fa-question-circle"></div>').appendTo(optionElement).qtip({
                                        content: option.description,
                                        position: {
                                            at: 'top center',
                                            my: 'bottom center',
                                            adjust: {
                                                y: 5
                                            }
                                        },
                                        style: {
                                            classes: 'nifi-tooltip'
                                        },
                                        show: {
                                            solo: true,
                                            effect: function(offset) {
                                                $(this).slideDown(100);
                                            }
                                        },
                                        hide: {
                                            effect: function(offset) {
                                                $(this).slideUp(100);
                                            }
                                        }
                                    }
                                );
                            } else {
                                optionText.css('width', '100%');
                            }

                            actualHeight += 16;
                        });

                        // show the glass pane to catch the click events
                        var comboGlassPane = $('<div class="combo-glass-pane"></div>').one('click', function () {
                            if (comboOptions.length !== 0) {
                                // clean up tooltips
                                comboOptions.find('.fa').each(function () {
                                    var tip = $(this);
                                    if (tip.data('qtip')) {
                                        var api = tip.qtip('api');
                                        api.destroy(true);
                                    }
                                });
                                // remove the options
                                comboOptions.remove();

                                //remove active styles
                                $('.combo').removeClass('combo-open');
                            }

                            // remove the glass pane
                            $(this).remove();
                        });

                        // add the combo to the dom
                        $('body').append(comboOptions).append(comboGlassPane);

                        // stop the event propagation so the body click event isn't fired
                        event.stopPropagation();
                    });

                    // add the drop down arrow
                    $('<div class="combo-arrow fa fa-chevron-down"></div>').appendTo(combo);

                    // set the selection
                    if (isDefinedAndNotNull(options.selectedOption)) {
                        selectOption(combo, options.selectedOption.text, options.selectedOption.value);
                    } else {
                        selectOption(combo);
                    }
                }
            });
        },

        /**
         * Returns the selected option of the first matching element.
         */
        getSelectedOption: function () {
            var value;

            this.each(function () {
                value = $(this).data('selected-option');
                return false;
            });

            return value;
        },

        /**
         * Sets the selected option.
         *
         * @argument {object} option The option to select
         */
        setSelectedOption: function (option) {
            return this.each(function () {
                selectOption($(this), option.text, option.value);
            });
        },

        /**
         * Sets whether the specified option is enabled or disabled.
         * 
         * @param option
         * @param enabled
         */
        setOptionEnabled: function (option, enabled) {
            return this.each(function () {
                var combo = $(this);
                var comboConfigOptions = combo.data('options');

                $.each(comboConfigOptions.options, function (i, configOption) {
                     if (configOption.value === option.value) {
                        configOption.disabled = !enabled;   
                     }
                });
            });
        },

        /**
         * Destroys the combo.
         */
        destroy: function () {
            return this.each(function () {
                $(this).empty().unbind().removeData();

                // remove the options if open
                $('div.combo-glass-pane').click();
            });
        },

        /**
         * Closes the combo.
         */
        close: function () {
            $('div.combo-glass-pane').click();
        }
    };

    $.fn.combo = function (method) {
        if (methods[method]) {
            return methods[method].apply(this, Array.prototype.slice.call(arguments, 1));
        } else {
            return methods.init.apply(this, arguments);
        }
    };
})(jQuery);