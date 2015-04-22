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

/**
 * Create a new dialog. The options are specified in the following
 * format:
 *
 * {
 *   headerText: 'Dialog Header',
 *   overlayBackground: true,
 *   buttons: [{
 *      buttonText: 'Cancel',
 *      handler: {
 *          click: cancelHandler
 *      }
 *   }, {
 *      buttonText: 'Apply',
 *      handler: {
 *          click: applyHandler
 *      }
 *   }],
 *   handler: {
 *      close: closeHandler
 *   }
 * }
 * 
 * The content of the dialog should be in a element with the class dialog-content
 * directly under the dialog.
 * 
 * @argument {jQuery} $
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

    // private function for adding buttons
    var addButtons = function (dialog, buttonModel) {
        if (isDefinedAndNotNull(buttonModel)) {
            var buttonWrapper = $('<div class="dialog-buttons"></div>');
            $.each(buttonModel, function (i, buttonConfig) {
                $('<div class="button button-normal"></div>').text(buttonConfig.buttonText).click(function () {
                    var handler = $(this).data('handler');
                    if (isDefinedAndNotNull(handler) && typeof handler.click === 'function') {
                        handler.click.call(dialog);
                    }
                }).data('handler', buttonConfig.handler).appendTo(buttonWrapper);
            });
            buttonWrapper.append('<div class="clear"></div>').appendTo(dialog);
        }
    };

    var methods = {
        
        /**
         * Initializes the dialog.
         * 
         * @argument {object} options The options for the plugin
         */
        init: function (options) {
            return this.each(function () {
                // ensure the options have been properly specified
                if (isDefinedAndNotNull(options)) {

                    // get the combo
                    var dialog = $(this).addClass('dialog cancellable modal');
                    var dialogHeaderText = $('<span class="dialog-header-text"></span>');
                    var dialogHeader = $('<div class="dialog-header"></div>').append(dialogHeaderText);

                    // save the close handler
                    if (isDefinedAndNotNull(options.handler)) {
                        dialog.data('handler', options.handler);
                    }

                    // determine if the specified header text is null
                    if (!isBlank(options.headerText)) {
                        dialogHeaderText.text(options.headerText);
                    }
                    dialog.prepend(dialogHeader);

                    // determine whether a dialog border is necessary
                    if (options.overlayBackground === true) {
                        dialog.addClass('overlay');
                    } else {
                        dialog.addClass('show-border');
                    }

                    // add the buttons
                    addButtons(dialog, options.buttons);
                }
            });
        },
        
        /**
         * Sets the handler that is used when the dialog is closed.
         * 
         * @argument {function} handler The function to call when hiding the dialog
         */
        setHandler: function (handler) {
            return this.each(function () {
                $(this).data('handler', handler);
            });
        },
        
        /**
         * Sets whether to overlay the background or if a border should be shown around the dialog.
         * 
         * @argument {boolean} overlayBackground Whether or not to overlay the background
         */
        setOverlayBackground: function (overlayBackground) {
            return this.each(function () {
                if (isDefinedAndNotNull(overlayBackground)) {
                    var dialog = $(this);

                    // if we should overlay the background
                    if (overlayBackground === true) {
                        dialog.addClass('overlay');
                        dialog.removeClass('show-border');
                    } else {
                        dialog.addClass('show-border');
                        dialog.removeClass('overlay');
                    }
                }
            });
        },
        
        /**
         * Updates the button model for the selected dialog.
         * 
         * @argument {array} buttons The new button model
         */
        setButtonModel: function (buttons) {
            return this.each(function () {
                if (isDefinedAndNotNull(buttons)) {
                    var dialog = $(this);

                    // remove the current buttons
                    dialog.children('.dialog-buttons').remove();

                    // add the new buttons
                    addButtons(dialog, buttons);
                }
            });
        },
        
        /**
         * Sets the header text of the dialog.
         * 
         * @argument {string} text Text to use a as a header
         */
        setHeaderText: function (text) {
            return this.each(function () {
                $(this).find('span.dialog-header-text').text(text);
            });
        },
        
        /**
         * Shows the dialog.
         */
        show: function () {
            return this.each(function () {
                // show the dialog
                var dialog = $(this);
                if (!dialog.is(':visible')) {
                    var title = dialog.find('span.dialog-header-text').text();
                    if (isBlank(title)) {
                        dialog.find('.dialog-content').css('margin-top', '-20px');
                    } else {
                        dialog.find('.dialog-content').css('margin-top', '0');
                    }
                    
                    // show the appropriate background
                    if (dialog.hasClass('show-border')) {
                        $('#glass-pane').show();
                    } else {
                        $('#faded-background').show();
                    }

                    // show the centered dialog - performing these operation in the
                    // opposite order one might expect. for some reason, the call to
                    // center is causeing the graph to flicker in firefox (3.6). displaying
                    // the dialog container before centering seems to address the issue.
                    dialog.show().center();
                }
            });
        },
        
        /**
         * Hides the dialog.
         */
        hide: function () {
            return this.each(function () {
                var dialog = $(this);
                if (dialog.is(':visible')) {
                    // hide the appropriate backgroun
                    if (dialog.hasClass('show-border')) {
                        $('#glass-pane').hide();
                    } else {
                        $('#faded-background').hide();
                    }

                    // hide the dialog
                    dialog.hide();
                    
                    // invoke the handler
                    var handler = dialog.data('handler');
                    if (isDefinedAndNotNull(handler) && typeof handler.close === 'function') {
                        handler.close.call(dialog);
                    }
                }
            });
        }
    };

    $.fn.modal = function (method) {
        if (methods[method]) {
            return methods[method].apply(this, Array.prototype.slice.call(arguments, 1));
        } else {
            return methods.init.apply(this, arguments);
        }
    };
})(jQuery);