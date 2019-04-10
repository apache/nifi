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
        define(['jquery',
                'nf.Dialog',
                'nf.Common'],
            function ($, nfDialog, nfCommon) {
                return (nf.ErrorHandler = factory($, nfDialog, nfCommon));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ErrorHandler = factory(require('jquery'),
            require('nf.Dialog'),
            require('nf.Common')));
    } else {
        nf.ErrorHandler = factory(root.$,
            root.nf.Dialog,
            root.nf.Common);
    }
}(this, function ($, nfDialog, nfCommon) {
    'use strict';

    var self = {
        /**
         * Method for handling ajax errors.
         *
         * @argument {object} xhr       The XmlHttpRequest
         * @argument {string} status    The status of the request
         * @argument {string} error     The error
         */
        handleAjaxError: function (xhr, status, error) {
            if (status === 'canceled') {
                if ($('#splash').is(':visible')) {
                    $('#message-title').text('Session Expired');
                    $('#message-content').text('Your session has expired. Please reload to log in again.');

                    // show the error pane
                    $('#message-pane').show();
                } else {
                    nfDialog.showOkDialog({
                        headerText: 'Session Expired',
                        dialogContent: 'Your session has expired. Please press Ok to log in again.',
                        okHandler: function () {
                            window.location = '/nifi';
                        }
                    });
                }
                return;
            }

            // if an error occurs while the splash screen is visible close the canvas show the error message
            if ($('#splash').is(':visible')) {
                if (xhr.status === 401) {
                    $('#message-title').text('Unauthorized');
                } else if (xhr.status === 403) {
                    $('#message-title').text('Insufficient Permissions');
                } else if (xhr.status === 409) {
                    $('#message-title').text('Invalid State');
                } else {
                    $('#message-title').text('An unexpected error has occurred');
                }

                if ($.trim(xhr.responseText) === '') {
                    $('#message-content').text('Please check the logs.');
                } else {
                    $('#message-content').text(xhr.responseText);
                }

                // show the error pane
                $('#message-pane').show();
                return;
            }

            // status code 400, 404, and 409 are expected response codes for nfCommon errors.
            if (xhr.status === 400 || xhr.status === 404 || xhr.status === 409 || xhr.status === 503) {
                nfDialog.showOkDialog({
                    headerText: 'Error',
                    dialogContent: nfCommon.escapeHtml(xhr.responseText)
                });
            } else if (xhr.status === 403) {
                nfDialog.showOkDialog({
                    headerText: 'Insufficient Permissions',
                    dialogContent: nfCommon.escapeHtml(xhr.responseText)
                });
            } else {
                if (xhr.status < 99 || xhr.status === 12007 || xhr.status === 12029) {
                    var content = 'Please ensure the application is running and check the logs for any errors.';
                    if (nfCommon.isDefinedAndNotNull(status)) {
                        if (status === 'timeout') {
                            content = 'Request has timed out. Please ensure the application is running and check the logs for any errors.';
                        } else if (status === 'abort') {
                            content = 'Request has been aborted.';
                        } else if (status === 'No Transport') {
                            content = 'Request transport mechanism failed. Please ensure the host where the application is running is accessible.';
                        }
                    }
                    $('#message-title').text('Unable to communicate with NiFi');
                    $('#message-content').text(content);
                } else if (xhr.status === 401) {
                    $('#message-title').text('Unauthorized');
                    if ($.trim(xhr.responseText) === '') {
                        $('#message-content').text('Authentication is required to use this NiFi.');
                    } else {
                        $('#message-content').text(xhr.responseText);
                    }
                } else if (xhr.status === 500) {
                    $('#message-title').text('An unexpected error has occurred');
                    if ($.trim(xhr.responseText) === '') {
                        $('#message-content').text('An error occurred communicating with the application core. Please check the logs and fix any configuration issues before restarting.');
                    } else {
                        $('#message-content').text(xhr.responseText);
                    }
                } else if (xhr.status === 200 || xhr.status === 201) {
                    $('#message-title').text('Parse Error');
                    if ($.trim(xhr.responseText) === '') {
                        $('#message-content').text('Unable to interpret response from NiFi.');
                    } else {
                        $('#message-content').text(xhr.responseText);
                    }
                } else {
                    $('#message-title').text(xhr.status + ': Unexpected Response');
                    $('#message-content').text('An unexpected error has occurred. Please check the logs.');
                }

                // show the error pane
                $('#message-pane').show();
            }
        },

        /**
         * Method for handling ajax errors when submitting configuration update (PUT/POST) requests.
         * In addition to what handleAjaxError does, this function splits
         * the error message text to display them as an unordered list.
         *
         * @argument {object} xhr       The XmlHttpRequest
         * @argument {string} status    The status of the request
         * @argument {string} error     The error
         */
        handleConfigurationUpdateAjaxError: function (xhr, status, error) {
            if (xhr.status === 400) {
                var errors = xhr.responseText.split('\n');

                var content;
                if (errors.length === 1) {
                    content = $('<span></span>').text(errors[0]);
                } else {
                    content = nfCommon.formatUnorderedList(errors);
                }

                nfDialog.showOkDialog({
                    dialogContent: content,
                    headerText: 'Configuration Error'
                });
            } else {
                self.handleAjaxError(xhr, status, error);
            }
        }
    };
    return self;
}));