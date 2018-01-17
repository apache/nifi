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

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery'], function ($) {
            return (nf.Dialog = factory($));
        });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Dialog = factory(require('jquery')));
    } else {
        nf.Dialog = factory(root.$);
    }
}(this, function ($) {
    'use strict';

    $(document).ready(function () {
        // configure the ok dialog
        $('#nf-ok-dialog').modal({
            scrollableContentStyle: 'scrollable',
            handler: {
                close: function () {
                    // clear the content
                    $('#nf-ok-dialog-content').empty();
                }
            }
        });

        // configure the yes/no dialog
        $('#nf-yes-no-dialog').modal({
            scrollableContentStyle: 'scrollable',
            handler: {
                close: function () {
                    // clear the content and reset the button model
                    $('#nf-yes-no-dialog-content').empty();
                    $('#nf-yes-no-dialog').modal('setButtonModel', []);
                }
            }
        });
    });

    var nfDialog = {
        /**
         * Shows an general dialog with an Okay button populated with the
         * specified dialog content.
         *
         * @argument {object} options       Dialog options
         */
        showOkDialog: function (options) {
            options = $.extend({
                headerText: '',
                dialogContent: ''
            }, options);

            // regardless of whether the dialog is already visible, the new content will be appended
            var content = $('<p></p>').append(options.dialogContent);
            $('#nf-ok-dialog-content').append(content).append('</br>');

            // update the button model
            $('#nf-ok-dialog').modal('setButtonModel', [{
                buttonText: nf._.msg('nf-dialog.Ok'),
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        // close the dialog
                        $('#nf-ok-dialog').modal('hide');
                        if (typeof options.okHandler === 'function') {
                            options.okHandler.call(this);
                        }
                    }
                }
            }]);

            // show the dialog
            $('#nf-ok-dialog').modal('setHeaderText', options.headerText).modal('show');
        },

        /**
         * Shows an general dialog with Yes and No buttons populated with the
         * specified dialog content.
         *
         * @argument {object} options       Dialog options
         */
        showYesNoDialog: function (options) {
            options = $.extend({
                headerText: '',
                dialogContent: '',
                yesText: 'Yes',
                noText: 'No'
            }, options);

            // add the content to the prompt
            var content = $('<p></p>').append(options.dialogContent);
            $('#nf-yes-no-dialog-content').append(content);

            // update the button model
            $('#nf-yes-no-dialog').modal('setButtonModel', [{
                buttonText: options.yesText,
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        // close the dialog
                        $('#nf-yes-no-dialog').modal('hide');
                        if (typeof options.yesHandler === 'function') {
                            options.yesHandler.call(this);
                        }
                    }
                }
            },
                {
                    buttonText: options.noText,
                    color: {
                        base: '#E3E8EB',
                        hover: '#C7D2D7',
                        text: '#004849'
                    },
                    handler: {
                        click: function () {
                            // close the dialog
                            $('#nf-yes-no-dialog').modal('hide');
                            if (typeof options.noHandler === 'function') {
                                options.noHandler.call(this);
                            }
                        }
                    }
                }]);

            // show the dialog
            $('#nf-yes-no-dialog').modal('setHeaderText', options.headerText).modal('show');
        },

        /**
         * Shows a message when disconnected from the cluster.
         */
        showDisconnectedFromClusterMessage: function () {
            nfDialog.showOkDialog({
                headerText: 'Cluster Connection',
                dialogContent: 'This node is currently not connected to the cluster. Any modifications to the data flow made here will not replicate across the cluster.'
            });
        },

        /**
         * Shows a message when connected to the cluster.
         */
        showConnectedToClusterMessage: function () {
            nfDialog.showOkDialog({
                headerText: 'Cluster Connection',
                dialogContent: 'This node just joined the cluster. Any modifications to the data flow made here will replicate across the cluster.'
            });
        }
    };

    return nfDialog;
}));