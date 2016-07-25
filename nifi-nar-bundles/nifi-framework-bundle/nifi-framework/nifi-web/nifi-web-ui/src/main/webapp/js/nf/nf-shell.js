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

/* global nf */

$(document).ready(function () {
    // configure the dialog
    $('#shell-dialog').modal({
        scrollableContentStyle: 'scrollable',
        header: false,
        footer: false
    });

    // register a listener when the frame is closed
    $('#shell-close-button').click(function () {
        // close the shell
        $('#shell-dialog').modal('hide');
    });

    // register a listener when the frame is undocked
    $('#shell-undock-button').click(function () {
        var uri = $('#shell-iframe').attr('src');
        if (!nf.Common.isBlank(uri)) {
            // open the page and close the shell
            window.open(uri);

            // close the shell
            $('#shell-dialog').modal('hide');
        }
    });
});

nf.Shell = (function () {

    var showPageResize = null;
    var showContentResize = null;

    return {
        
        resizeContent: function (shell) {
            var contentContainer = shell.find('.shell-content-container');
            contentContainer.css({
                width: shell.width(),
                height: shell.height() - 28 - 40 //subtract shell-close-container and padding
            });
            shell.trigger("shell:content:resize");
        },

        // handle resize
        resizeIframe: function (shell) {
            var shellIframe = shell.find('#shell-iframe');
            shellIframe.css({
                width: shell.width(),
                height: shell.height() - 28 - 40 //subtract shell-close-container and padding
            });
            shell.trigger("shell:iframe:resize");
        },
        
        /**
         * Shows a page in the shell.
         * 
         * @argument {string} uri               The URI to show
         * @argument {boolean} canUndock        Whether or not the shell is undockable
         */
        showPage: function (uri, canUndock) {
            // if the context menu is on this page, attempt to close
            if (nf.Common.isDefinedAndNotNull(nf.ContextMenu)) {
                nf.ContextMenu.hide();
            }
            
            return $.Deferred(function (deferred) {
                var shell = $('#shell');

                // default undockable to true
                if (nf.Common.isNull(canUndock) || nf.Common.isUndefined(canUndock)) {
                    canUndock = true;
                }

                // remove the previous contents of the shell
                shell.empty();

                // register a new close handler
                $('#shell-dialog').modal('setCloseHandler', function () {
                    deferred.resolve();
                });

                // register a new open handler
                $('#shell-dialog').modal('setOpenHandler', function () {
                    nf.Common.toggleScrollable($('#' + this.find('.tab-container').attr('id') + '-content').get(0));
                });

                // show the custom processor ui
                $('#shell-dialog').modal('show');

                // conditionally show the undock button
                if (canUndock) {
                    $('#shell-undock-button').show();
                } else {
                    $('#shell-undock-button').hide();
                }

                // create an iframe to hold the custom ui
                var shellIframe = $('<iframe/>', {
                    id: 'shell-iframe',
                    frameBorder: '0',
                    src: uri
                }).css({
                    width: shell.width(),
                    height: shell.height() - 28 //subtract shell-close-container
                }).appendTo(shell);
            }).promise();
        },
        
        /**
         * Shows the specified content in the shell. When the shell is closed, the content
         * will be hidden and returned to its previous location in the dom.
         * 
         * @argument {string} domId             The id of the element to show in the shell
         */
        showContent: function (domId) {
            // if the context menu is on this page, attempt to close
            if (nf.Common.isDefinedAndNotNull(nf.ContextMenu)) {
                nf.ContextMenu.hide();
            }
            
            return $.Deferred(function (deferred) {
                var content = $(domId);
                if (content.length) {
                    var shell = $('#shell');

                    // remove the previous contents of the shell
                    shell.empty();

                    // get the parent of the content and detach it
                    var parent = content.parent();
                    content.detach();

                    // register a new close handler
                    $('#shell-dialog').modal('setCloseHandler', function () {
                        // close any open combos
                        var combos = $('.combo');
                        for (var i = 0, len = combos.length; i < len; i++) {
                            if ($(combos[i]).is(':visible')){
                                $(combos[i]).combo('close');
                            }
                        }

                        deferred.resolve();

                        // detach the content and add it back to the parent
                        content.hide().detach().appendTo(parent);
                    });

                    // hide the undock button
                    $('#shell-undock-button').hide();

                    // open the shell dialog
                    $('#shell-dialog').modal('show');

                    // create the content container
                    var contentContainer = $('<div>').addClass('shell-content-container').css({
                        width: shell.width(),
                        height: shell.height()
                    }).append(content).appendTo(shell);
                    
                    // show the content
                    content.show();
                }
            }).promise();
        }
    };
}());
