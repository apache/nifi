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
 */
(function ($) {

    var overlayBackground = 'overlay-background';

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

    // private function for adding the border
    var addBorder = function (dialog) {
        var dialogParent = dialog.parent();

        // detach the dialog
        dialog.detach();

        // create the wrapper
        $('<div/>').append('<div class="dialog-border"></div>').append(dialog).appendTo(dialogParent);
    };

    var methods = {
        /**
         * Initializes the dialog.
         */
        init: function (options) {
            return this.each(function () {
                // ensure the options have been properly specified
                if (isDefinedAndNotNull(options)) {

                    // get the combo
                    var dialog = $(this).addClass('dialog cancellable');
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
                    if (options.overlayBackground === false) {
                        addBorder(dialog);
                    }

                    // add the buttons
                    addButtons(dialog, options.buttons);
                }
            });
        },
        /**
         * Sets the handler that is used when the dialog is closed.
         */
        setHandler: function (handler) {
            return this.each(function () {
                $(this).data('handler', handler);
            });
        },
        /**
         * Sets whether to overlay the background or if a border should be shown around the dialog.
         */
        setOverlayBackground: function (overlayBackground) {
            return this.each(function () {
                if (isDefinedAndNotNull(overlayBackground)) {
                    var dialog = $(this);
                    var prev = dialog.prev();

                    // if we should overlay the background
                    if (overlayBackground === true) {
                        // if we're currently configured for a border
                        if (prev.hasClass('dialog-border')) {
                            // detach the dialog
                            dialog.detach();

                            // reinsert appropriately
                            prev.parent().replaceWith(dialog);
                        }
                    } else {
                        // if we're currently configured to overlay the background
                        if (!prev.hasClass('dialog-border')) {
                            addBorder(dialog);
                        }
                    }
                }
            });
        },
        /**
         * Updates the button model for the selected dialog.
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
                    // position the background/border
                    var background;
                    var prev = dialog.prev();
                    if (!prev.hasClass('dialog-border')) {
                        // add a marker style so we know how to remove the style when hiding the dialog
                        dialog.addClass(overlayBackground);

                        // get the faded background
                        background = $('#faded-background');
                    } else {
                        // get the dialog border
                        background = prev;

                        // get the width and the height of the dialog container
                        var width = parseInt(dialog.css('width'));
                        var height = parseInt(dialog.css('height'));

                        // set the width and height and center the dialog border
                        background.css({
                            'width': (width + 20) + 'px',
                            'height': (height + 20) + 'px'
                        }).center();

                        // show the glass pane
                        $('#glass-pane').show();
                    }
                    background.show();

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
                    // determine how to hide the dialog
                    if (dialog.hasClass(overlayBackground)) {
                        dialog.removeClass(overlayBackground);

                        // hide the faded background
                        $('#faded-background').hide();
                    } else {
                        // hide the dialog border
                        dialog.prev().hide();

                        // hide the glass pane
                        $('#glass-pane').hide();
                    }

                    var handler = dialog.data('handler');
                    if (isDefinedAndNotNull(handler) && typeof handler.close === 'function') {
                        handler.close.call(dialog);
                    }

                    // hide the dialog
                    dialog.hide();
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