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
            var button;
            $.each(buttonModel, function (i, buttonConfig) {
                var clazz = isDefinedAndNotNull(buttonConfig.clazz) ? buttonConfig.clazz : '';
                if (buttonConfig.color) {
                    button = $('<div class="button ' + clazz + '" style="color:' + buttonConfig.color.text + '; background:' + buttonConfig.color.base + ';"><span>' + buttonConfig.buttonText + '</span></div>');
                    button.hover(function () {
                        $(this).css("background-color", buttonConfig.color.hover);
                    }, function () {
                        $(this).css("background-color", buttonConfig.color.base);
                    });
                } else {
                    button = $('<div class="button ' + clazz + '"><span>' + buttonConfig.buttonText + '</span></div>');
                }
                button.click(function () {
                    var handler = $(this).data('handler');
                    if (isDefinedAndNotNull(handler) && typeof handler.click === 'function') {
                        handler.click.call(dialog);
                    }
                }).data('handler', buttonConfig.handler).appendTo(buttonWrapper);
            });
            buttonWrapper.appendTo(dialog);
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

                var nfDialog = {};
                if (isDefinedAndNotNull(dialog.data('nf-dialog'))) {
                    nfDialog = dialog.data('nf-dialog');
                }

                // ensure the options have been properly specified
                if (isDefinedAndNotNull(options)) {

                    $.extend(nfDialog, options);

                    //persist data attribute
                    dialog.data('nfDialog', nfDialog);
                }

                // determine if dialog needs a header
                if (!isDefinedAndNotNull(nfDialog.header) || nfDialog.header) {
                    var dialogHeaderText = $('<span class="dialog-header-text"></span>');
                    var dialogHeader = $('<div class="dialog-header"></div>').prepend(dialogHeaderText);

                    // determine if the specified header text is null
                    if (!isBlank(nfDialog.headerText)) {
                        dialogHeaderText.text(nfDialog.headerText);
                    }

                    dialog.prepend(dialogHeader);
                }

                // determine if dialog needs footer/buttons
                if (!isDefinedAndNotNull(nfDialog.footer) || nfDialog.footer) {
                    // add the buttons
                    addButtons(dialog, nfDialog.buttons);
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

                var nfDialog = {};
                if (isDefinedAndNotNull($(this).data('nf-dialog'))) {
                    nfDialog = $(dialog).data('nf-dialog');
                }
                if (!isDefinedAndNotNull(nfDialog.handler)){
                    nfDialog.handler = {};
                }
                nfDialog.handler.close = handler;

                //persist data attribute
                $(dialog).data('nfDialog', nfDialog);
            });
        },

        /**
         * Sets the handler that is used when the dialog is opened.
         *
         * @argument {function} handler The function to call when showing the dialog
         */
        setOpenHandler: function (handler) {
            return this.each(function (index, dialog) {

                var nfDialog = {};
                if (isDefinedAndNotNull($(this).data('nf-dialog'))) {
                    nfDialog = $(dialog).data('nf-dialog');
                }
                if (!isDefinedAndNotNull(nfDialog.handler)){
                    nfDialog.handler = {};
                }
                nfDialog.handler.open = handler;

                //persist data attribute
                $(dialog).data('nfDialog', nfDialog);
            });
        },

        /**
         * Sets the handler that is used when the dialog is resized.
         *
         * @argument {function} handler The function to call when resizing the dialog
         */
        setResizeHandler: function (handler) {
            return this.each(function (index, dialog) {

                var nfDialog = {};
                if (isDefinedAndNotNull($(this).data('nf-dialog'))) {
                    nfDialog = $(dialog).data('nf-dialog');
                }
                if (!isDefinedAndNotNull(nfDialog.handler)){
                    nfDialog.handler = {};
                }
                nfDialog.handler.resize = handler;

                //persist data attribute
                $(dialog).data('nfDialog', nfDialog);
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

        resize: function () {
            var dialog = $(this);
            var dialogContent = dialog.find('.dialog-content');

            var nfDialog = {};
            if (isDefinedAndNotNull(dialog.data('nf-dialog'))) {
                nfDialog = dialog.data('nf-dialog');
            }

            //initialize responsive properties
            if (!isDefinedAndNotNull(nfDialog.responsive)) {
                nfDialog.responsive = {};

                if (!isDefinedAndNotNull(nfDialog.responsive.x)) {
                    nfDialog.responsive.x = true;
                }

                if (!isDefinedAndNotNull(nfDialog.responsive.y)) {
                    nfDialog.responsive.y = true;
                }
            } else {
                if (!isDefinedAndNotNull(nfDialog.responsive.x)) {
                    nfDialog.responsive.x = true;
                } else {
                    nfDialog.responsive.x = (nfDialog.responsive.x == "true" || nfDialog.responsive.x == true) ? true : false;
                }

                if (!isDefinedAndNotNull(nfDialog.responsive.y)) {
                    nfDialog.responsive.y = true;
                } else {
                    nfDialog.responsive.y = (nfDialog.responsive.y == "true" || nfDialog.responsive.y == true) ? true : false;
                }
            }

            if (nfDialog.responsive.y || nfDialog.responsive.x) {

                var fullscreenHeight;
                var fullscreenWidth;

                if (isDefinedAndNotNull(nfDialog.responsive['fullscreen-height'])) {
                    fullscreenHeight = parseInt(nfDialog.responsive['fullscreen-height'], 10);
                } else {
                    nfDialog.responsive['fullscreen-height'] = dialog.height() + 'px';

                    fullscreenHeight = parseInt(nfDialog.responsive['fullscreen-height'], 10);
                }

                if (isDefinedAndNotNull(nfDialog.responsive['fullscreen-width'])) {
                    fullscreenWidth = parseInt(nfDialog.responsive['fullscreen-width'], 10);
                } else {
                    nfDialog.responsive['fullscreen-width'] = dialog.width() + 'px';

                    fullscreenWidth = parseInt(nfDialog.responsive['fullscreen-width'], 10);
                }

                if (!isDefinedAndNotNull(nfDialog.width)) {
                    nfDialog.width = dialog.css('width');
                }

                if (!isDefinedAndNotNull(nfDialog['min-width'])) {
                    if (parseInt(dialog.css('min-width'), 10) > 0) {
                        nfDialog['min-width'] = dialog.css('min-width');
                    } else {
                        nfDialog['min-width'] = nfDialog.width;
                    }
                }

                //min-width should always be set in terms of px
                if (nfDialog['min-width'].indexOf("%") > 0) {
                    nfDialog['min-width'] = ($(window).width() * (parseInt(nfDialog['min-width'], 10) / 100)) + 'px';
                }

                if (!isDefinedAndNotNull(nfDialog.height)) {
                    nfDialog.height = dialog.css('height');
                }

                if (!isDefinedAndNotNull(nfDialog['min-height'])) {
                    if (parseInt(dialog.css('min-height'), 10) > 0) {
                        nfDialog['min-height'] = dialog.css('min-height');
                    } else {
                        nfDialog['min-height'] = nfDialog.height;
                    }
                }

                //min-height should always be set in terms of px
                if (nfDialog['min-height'].indexOf("%") > 0) {
                    nfDialog['min-height'] = ($(window).height() * (parseInt(nfDialog['min-height'], 10) / 100)) + 'px';
                }

                //resize dialog
                if ($(window).height() < fullscreenHeight) {
                    if (nfDialog.responsive.y) {
                        dialog.css('height', '100%');
                        dialog.css('min-height', '100%');
                    }
                } else {
                    //set the dialog min-height
                    dialog.css('min-height', nfDialog['min-height']);
                    if (nfDialog.responsive.y) {
                        //make sure nfDialog.height is in terms of %
                        if (nfDialog.height.indexOf("px") > 0) {
                            nfDialog.height = (parseInt(nfDialog.height, 10) / $(window).height() * 100) + '%';
                        }
                        dialog.css('height', nfDialog.height);
                    }
                }

                if ($(window).width() < fullscreenWidth) {
                    if (nfDialog.responsive.x) {
                        dialog.css('width', '100%');
                        dialog.css('min-width', '100%');
                    }
                } else {
                    //set the dialog width
                    dialog.css('min-width', nfDialog['min-width']);
                    if (nfDialog.responsive.x) {
                        //make sure nfDialog.width is in terms of %
                        if (nfDialog.width.indexOf("px") > 0) {
                            nfDialog.width = (parseInt(nfDialog.width, 10) / $(window).width() * 100) + '%';
                        }
                        dialog.css('width', nfDialog.width);
                    }
                }

                dialog.center();

                //persist data attribute
                dialog.data('nfDialog', nfDialog);
            }

            //apply scrollable style if applicable
            if (dialogContent[0].offsetHeight < dialogContent[0].scrollHeight) {
                // your element has overflow
                if (isDefinedAndNotNull(nfDialog.scrollableContentStyle)) {
                    dialogContent.addClass(nfDialog.scrollableContentStyle);
                }
            } else {
                // your element doesn't have overflow
                if (isDefinedAndNotNull(nfDialog.scrollableContentStyle)) {
                    dialogContent.removeClass(nfDialog.scrollableContentStyle);
                }
            }

            if (isDefinedAndNotNull(nfDialog.handler)) {
                var handler = nfDialog.handler.resize;
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

            var nfDialog = {};
            if (isDefinedAndNotNull(dialog.data('nf-dialog'))) {
                nfDialog = dialog.data('nf-dialog');
            }

            var glasspane;
            if (isDefinedAndNotNull(nfDialog.glasspane)) {
                glasspane = nfDialog.glasspane;
            } else {
                nfDialog.glasspane = glasspane = dialog.find('.dialog-header').css('background-color'); //default to header color
            }

            if(top !== window || !isDefinedAndNotNull(nfDialog.glasspane)) {
                nfDialog.glasspane = glasspane = 'transparent';
            }

            //create glass pane overlay
            var modalGlassMarkup = '<div data-nf-dialog-parent="' +
                dialog.attr('id') + '" class="modal-glass" style="background-color: ' + glasspane + ';"></div>';

            var modalGlass = $(modalGlassMarkup);

            modalGlass.css('z-index', zIndex - 1).appendTo($('body'));

            //persist data attribute
            dialog.data('nfDialog', nfDialog);

            return this.each(function () {
                // show the dialog
                if (!dialog.is(':visible')) {
                    dialog.show();
                    dialog.modal('resize');
                    dialog.center();

                    if (isDefinedAndNotNull(nfDialog.handler)) {
                        var handler = nfDialog.handler.open;
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

                var nfDialog = {};
                if (isDefinedAndNotNull(dialog.data('nf-dialog'))) {
                    nfDialog = dialog.data('nf-dialog');
                }

                if (isDefinedAndNotNull(nfDialog.handler)) {
                    var handler = nfDialog.handler.close;
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