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
 * Create a new dialog.
 *
 * If the height, width, and fullscreen breakpoints options are not set this plugin will look for (and use)
 * any CSS defined styles for height, min/max-height, width, min/max-width. If no CSS styles are defined then the
 * plugin will attempt to calculate these values when the dialog is first opened (based on screen size).
 *
 * The options are specified in the following format:
 *
 * {
 *   header: true,
 *   footer: true,
 *   headerText: 'Dialog Header',
 *   scrollableContentStyle: 'scrollable',
 *   buttons: [{
 *      buttonText: 'Cancel',
 *          color: {
 *              base: '#728E9B',
 *              hover: '#004849',
 *              text: '#ffffff'
 *          },
 *          disabled: isDisabledFunction,
 *      handler: {
 *          click: cancelHandler
 *      }
 *   }, {
 *      buttonText: 'Apply',
 *          color: {
 *              base: '#E3E8EB',
 *              hover: '#C7D2D7',
 *              text: '#004849'
 *          },
 *          disabled: isDisabledFunction,
 *      handler: {
 *          click: applyHandler
 *      }
 *   }],
 *   handler: {
 *      close: closeHandler,
 *      open: openHandler,
 *      resize: resizeHandler
 *   },
 *   height: "55%", //optional. Property can also be set with css (accepts 'px' or '%' values)
 *   width: "34%", //optional. Property can also be set with css (accepts 'px' or '%' values)
 *   min-height: "420px", //optional, defaults to 'height'. Property  can also be set with css (accepts 'px' values)
 *   min-width: "470px" //optional, defaults to 'width'. Property can also be set with css (accepts 'px' values)
 *   responsive: {
 *       x: "true", //optional, default true
 *       y: "true", //optional, default true
 *       fullscreen-height: "420px", //optional, default is original dialog height (accepts 'px' values)
 *       fullscreen-width: "470px", //optional, default is original dialog width (accepts 'px' values)
 *      },
 *      glasspane: "#728E9B" //optional, sets the color of modal glasspane...default if unset is the dialog header color
 * }
 *
 * The content of the dialog MUST be contained in an element with the class `dialog-content`
 * directly under the dialog element.
 *
 * <div id="dialogId">
 *  <div class="dialog-content">
 *      //Dialog Content....
 *  </div>
 * </div>
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
                var isDisabled = function () {
                    return typeof buttonConfig.disabled === 'function' && buttonConfig.disabled.call() === true;
                };

                // create the button
                var button = $('<div class="button"></div>').append($('<span></span>').text(buttonConfig.buttonText));

                // add the class if specified
                if (isDefinedAndNotNull(buttonConfig.clazz)) {
                    button.addClass(buttonConfig.clazz);
                }

                // set the color if specified
                if (isDefinedAndNotNull(buttonConfig.color)) {
                    button.css({
                        'background': buttonConfig.color.base,
                        'color': buttonConfig.color.text
                    });
                }

                // check if the button should be disabled
                if (isDisabled()) {
                    button.addClass('disabled-button');
                } else {
                    // enable custom hover if specified
                    if (isDefinedAndNotNull(buttonConfig.color)) {
                        button.hover(function () {
                            $(this).css("background-color", buttonConfig.color.hover);
                        }, function () {
                            $(this).css("background-color", buttonConfig.color.base);
                        });
                    }

                    button.click(function () {
                        var handler = $(this).data('handler');
                        if (isDefinedAndNotNull(handler) && typeof handler.click === 'function') {
                            handler.click.call(dialog);
                        }
                    });
                }

                // add the button to the wrapper
                button.data('handler', buttonConfig.handler).appendTo(buttonWrapper);
            });

            // store the button model to refresh later
            dialog.append(buttonWrapper).data('buttonModel', buttonModel);
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
                // get the combo
                var dialog = $(this).addClass('dialog cancellable modal');
                dialog.css('display', 'none');

                var nfDialogData = {};
                if (isDefinedAndNotNull(dialog.data('nf-dialog'))) {
                    nfDialogData = dialog.data('nf-dialog');
                }

                // ensure the options have been properly specified
                if (isDefinedAndNotNull(options)) {

                    $.extend(nfDialogData, options);

                    //persist data attribute
                    dialog.data('nfDialog', nfDialogData);
                }

                // determine if dialog needs a header
                if (!isDefinedAndNotNull(nfDialogData.header) || nfDialogData.header) {
                    var dialogHeaderText = $('<span class="dialog-header-text"></span>');
                    var dialogHeader = $('<div class="dialog-header"></div>').prepend(dialogHeaderText);

                    // determine if the specified header text is null
                    if (!isBlank(nfDialogData.headerText)) {
                        dialogHeaderText.text(nfDialogData.headerText);
                    }

                    dialog.prepend(dialogHeader);
                }

                // determine if dialog needs footer/buttons
                if (!isDefinedAndNotNull(nfDialogData.footer) || nfDialogData.footer) {
                    // add the buttons
                    addButtons(dialog, nfDialogData.buttons);
                }
            });
        },

        /**
         * Sets the handler that is used when the dialog is closed.
         *
         * @argument {function} handler The function to call when hiding the dialog
         */
        setCloseHandler: function (handler) {
            return this.each(function (index, dialog) {

                var nfDialogData = {};
                if (isDefinedAndNotNull($(this).data('nf-dialog'))) {
                    nfDialogData = $(dialog).data('nf-dialog');
                }
                if (!isDefinedAndNotNull(nfDialogData.handler)){
                    nfDialogData.handler = {};
                }
                nfDialogData.handler.close = handler;

                //persist data attribute
                $(dialog).data('nfDialog', nfDialogData);
            });
        },

        /**
         * Sets the handler that is used when the dialog is opened.
         *
         * @argument {function} handler The function to call when showing the dialog
         */
        setOpenHandler: function (handler) {
            return this.each(function (index, dialog) {

                var nfDialogData = {};
                if (isDefinedAndNotNull($(this).data('nf-dialog'))) {
                    nfDialogData = $(dialog).data('nf-dialog');
                }
                if (!isDefinedAndNotNull(nfDialogData.handler)){
                    nfDialogData.handler = {};
                }
                nfDialogData.handler.open = handler;

                //persist data attribute
                $(dialog).data('nfDialog', nfDialogData);
            });
        },

        /**
         * Sets the handler that is used when the dialog is resized.
         *
         * @argument {function} handler The function to call when resizing the dialog
         */
        setResizeHandler: function (handler) {
            return this.each(function (index, dialog) {

                var nfDialogData = {};
                if (isDefinedAndNotNull($(this).data('nf-dialog'))) {
                    nfDialogData = $(dialog).data('nf-dialog');
                }
                if (!isDefinedAndNotNull(nfDialogData.handler)){
                    nfDialogData.handler = {};
                }
                nfDialogData.handler.resize = handler;

                //persist data attribute
                $(dialog).data('nfDialog', nfDialogData);
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
         * Refreshes the buttons with the existing model.
         */
        refreshButtons: function () {
            return this.each(function () {
                var dialog = $(this);
                var buttons = dialog.data('buttonModel');

                // remove the current buttons
                dialog.children('.dialog-buttons').remove();

                // add the new buttons
                addButtons(dialog, buttons);
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

        resize: function () {
            var dialog = $(this);
            var dialogContent = dialog.find('.dialog-content');

            var nfDialogData = {};
            if (isDefinedAndNotNull(dialog.data('nf-dialog'))) {
                nfDialogData = dialog.data('nf-dialog');
            }

            //initialize responsive properties
            if (!isDefinedAndNotNull(nfDialogData.responsive)) {
                nfDialogData.responsive = {};

                if (!isDefinedAndNotNull(nfDialogData.responsive.x)) {
                    nfDialogData.responsive.x = true;
                }

                if (!isDefinedAndNotNull(nfDialogData.responsive.y)) {
                    nfDialogData.responsive.y = true;
                }
            } else {
                if (!isDefinedAndNotNull(nfDialogData.responsive.x)) {
                    nfDialogData.responsive.x = true;
                } else {
                    nfDialogData.responsive.x = (nfDialogData.responsive.x == "true" || nfDialogData.responsive.x == true) ? true : false;
                }

                if (!isDefinedAndNotNull(nfDialogData.responsive.y)) {
                    nfDialogData.responsive.y = true;
                } else {
                    nfDialogData.responsive.y = (nfDialogData.responsive.y == "true" || nfDialogData.responsive.y == true) ? true : false;
                }
            }

            if (nfDialogData.responsive.y || nfDialogData.responsive.x) {

                var fullscreenHeight;
                var fullscreenWidth;

                if (isDefinedAndNotNull(nfDialogData.responsive['fullscreen-height'])) {
                    fullscreenHeight = parseInt(nfDialogData.responsive['fullscreen-height'], 10);
                } else {
                    nfDialogData.responsive['fullscreen-height'] = dialog.height() + 'px';

                    fullscreenHeight = parseInt(nfDialogData.responsive['fullscreen-height'], 10);
                }

                if (isDefinedAndNotNull(nfDialogData.responsive['fullscreen-width'])) {
                    fullscreenWidth = parseInt(nfDialogData.responsive['fullscreen-width'], 10);
                } else {
                    nfDialogData.responsive['fullscreen-width'] = dialog.width() + 'px';

                    fullscreenWidth = parseInt(nfDialogData.responsive['fullscreen-width'], 10);
                }

                if (!isDefinedAndNotNull(nfDialogData.width)) {
                    nfDialogData.width = dialog.css('width');
                }

                if (!isDefinedAndNotNull(nfDialogData['min-width'])) {
                    if (parseInt(dialog.css('min-width'), 10) > 0) {
                        nfDialogData['min-width'] = dialog.css('min-width');
                    } else {
                        nfDialogData['min-width'] = nfDialogData.width;
                    }
                }

                //min-width should always be set in terms of px
                if (nfDialogData['min-width'].indexOf("%") > 0) {
                    nfDialogData['min-width'] = ($(window).width() * (parseInt(nfDialogData['min-width'], 10) / 100)) + 'px';
                }

                if (!isDefinedAndNotNull(nfDialogData.height)) {
                    nfDialogData.height = dialog.css('height');
                }

                if (!isDefinedAndNotNull(nfDialogData['min-height'])) {
                    if (parseInt(dialog.css('min-height'), 10) > 0) {
                        nfDialogData['min-height'] = dialog.css('min-height');
                    } else {
                        nfDialogData['min-height'] = nfDialogData.height;
                    }
                }

                //min-height should always be set in terms of px
                if (nfDialogData['min-height'].indexOf("%") > 0) {
                    nfDialogData['min-height'] = ($(window).height() * (parseInt(nfDialogData['min-height'], 10) / 100)) + 'px';
                }

                //resize dialog
                if ($(window).height() < fullscreenHeight) {
                    if (nfDialogData.responsive.y) {
                        dialog.css('height', '100%');
                        dialog.css('min-height', '100%');
                    }
                } else {
                    //set the dialog min-height
                    dialog.css('min-height', nfDialogData['min-height']);
                    if (nfDialogData.responsive.y) {
                        //make sure nfDialogData.height is in terms of %
                        if (nfDialogData.height.indexOf("px") > 0) {
                            nfDialogData.height = (parseInt(nfDialogData.height, 10) / $(window).height() * 100) + '%';
                        }
                        dialog.css('height', nfDialogData.height);
                    }
                }

                if ($(window).width() < fullscreenWidth) {
                    if (nfDialogData.responsive.x) {
                        dialog.css('width', '100%');
                        dialog.css('min-width', '100%');
                    }
                } else {
                    //set the dialog width
                    dialog.css('min-width', nfDialogData['min-width']);
                    if (nfDialogData.responsive.x) {
                        //make sure nfDialogData.width is in terms of %
                        if (nfDialogData.width.indexOf("px") > 0) {
                            nfDialogData.width = (parseInt(nfDialogData.width, 10) / $(window).width() * 100) + '%';
                        }
                        dialog.css('width', nfDialogData.width);
                    }
                }

                dialog.center();

                //persist data attribute
                dialog.data('nfDialog', nfDialogData);
            }

            //apply scrollable style if applicable
            if (dialogContent[0].offsetHeight < dialogContent[0].scrollHeight) {
                // your element has overflow
                if (isDefinedAndNotNull(nfDialogData.scrollableContentStyle)) {
                    dialogContent.addClass(nfDialogData.scrollableContentStyle);
                }
            } else {
                // your element doesn't have overflow
                if (isDefinedAndNotNull(nfDialogData.scrollableContentStyle)) {
                    dialogContent.removeClass(nfDialogData.scrollableContentStyle);
                }
            }

            if (isDefinedAndNotNull(nfDialogData.handler)) {
                var handler = nfDialogData.handler.resize;
                if (isDefinedAndNotNull(handler) && typeof handler === 'function') {
                    // invoke the handler
                    handler.call(dialog);
                }
            }
        },

        /**
         * Shows the dialog.
         */
        show: function () {
            var dialog = $(this);

            var zIndex = dialog.css('z-index');
            if (zIndex === 'auto') {
                if (isDefinedAndNotNull(dialog.data('nf-dialog'))) {
                    zIndex = (isDefinedAndNotNull(dialog.data('nf-dialog')['z-index'])) ?
                        dialog.data('nf-dialog')['z-index'] : 1301;
                } else {
                    zIndex = 1301;
                }
            }
            var openDialogs = $.makeArray($('.dialog:visible'));
            if (openDialogs.length >= 1){
                var zVals = openDialogs.map(function(openDialog){
                    var index;
                    return isNaN(index = parseInt($(openDialog).css("z-index"), 10)) ? 0 : index;
                });
                //Add 2 so that we have room for the glass pane overlay of the new dialog
                zIndex = Math.max.apply(null, zVals) + 2;
            }
            dialog.css('z-index', zIndex);

            var nfDialogData = {};
            if (isDefinedAndNotNull(dialog.data('nf-dialog'))) {
                nfDialogData = dialog.data('nf-dialog');
            }

            var glasspane;
            if (isDefinedAndNotNull(nfDialogData.glasspane)) {
                glasspane = nfDialogData.glasspane;
            } else {
                nfDialogData.glasspane = glasspane = dialog.find('.dialog-header').css('background-color'); //default to header color
            }

            if(top !== window || !isDefinedAndNotNull(nfDialogData.glasspane)) {
                nfDialogData.glasspane = glasspane = 'transparent';
            }

            if (!$('body').find("[data-nf-dialog-parent='" + dialog.attr('id') + "']").is(':visible')) {
                //create glass pane overlay
                $('<div></div>').attr('data-nf-dialog-parent', dialog.attr('id')).addClass("modal-glass").css({
                    "background-color": glasspane,
                    "z-index": zIndex - 1
                }).appendTo($('body'));
            }

            //persist data attribute
            dialog.data('nfDialog', nfDialogData);

            return this.each(function () {
                // show the dialog
                if (!dialog.is(':visible')) {
                    dialog.show();
                    dialog.modal('resize');
                    dialog.center();

                    if (isDefinedAndNotNull(nfDialogData.handler)) {
                        var handler = nfDialogData.handler.open;
                        if (isDefinedAndNotNull(handler) && typeof handler === 'function') {
                            // invoke the handler
                            handler.call(dialog);
                        }
                    }
                }
            });
        },

        /**
         * Hides the dialog.
         */
        hide: function () {
            return this.each(function () {
                var dialog = $(this);

                var nfDialogData = {};
                if (isDefinedAndNotNull(dialog.data('nf-dialog'))) {
                    nfDialogData = dialog.data('nf-dialog');
                }

                if (isDefinedAndNotNull(nfDialogData.handler)) {
                    var handler = nfDialogData.handler.close;
                    if (isDefinedAndNotNull(handler) && typeof handler === 'function') {
                        // invoke the handler
                        handler.call(dialog);
                    }
                }

                // remove the modal glass pane overlay
                $('body').find("[data-nf-dialog-parent='" + dialog.attr('id') + "']").remove();

                if (dialog.is(':visible')) {
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