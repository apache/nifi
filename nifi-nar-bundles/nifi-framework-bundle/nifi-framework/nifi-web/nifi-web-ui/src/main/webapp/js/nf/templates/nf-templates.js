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

/* global nf, top */

$(document).ready(function () {
    // initialize the templates page
    nf.Templates.init();
});

nf.Templates = (function () {

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        urls: {
            banners: '../nifi-api/flow/banners',
            about: '../nifi-api/flow/about',
            authorities: '../nifi-api/flow/authorities'
        }
    };

    /**
     * the current group id
     */
    var groupId;

    /**
     * Loads the current users authorities.
     */
    var loadAuthorities = function () {
        return $.Deferred(function (deferred) {
            $.ajax({
                type: 'GET',
                url: config.urls.authorities,
                dataType: 'json'
            }).done(function (response) {
                if (nf.Common.isDefinedAndNotNull(response.authorities)) {
                    // record the users authorities
                    nf.Common.setAuthorities(response.authorities);
                    deferred.resolve();
                } else {
                    deferred.reject();
                }
            }).fail(function (xhr, status, error) {
                nf.Common.handleAjaxError(xhr, status, error);
                deferred.reject();
            });
        }).promise();
    };

    /**
     * Initializes the templates table.
     */
    var initializeTemplatesPage = function () {
        var selectStatusMessage = 'Select template to import';

        // define mouse over event for the refresh button
        nf.Common.addHoverEffect('#refresh-button', 'button-refresh', 'button-refresh-hover').click(function () {
            nf.TemplatesTable.loadTemplatesTable();
        });

        // initialize the upload template status
        $('#upload-template-status').text(selectStatusMessage);

        // add a hover effect to the browse button
        nf.Common.addHoverEffect('#select-template-button', 'button-normal', 'button-over');

        // add a handler for the change file input chain event
        $('#template-file-field').on('change', function (e) {
            var filename = $(this).val();
            if (!nf.Common.isBlank(filename)) {
                filename = filename.replace(/^.*[\\\/]/, '');
            }

            // set the filename
            $('#selected-template-name').text(filename);

            // update the container visibility
            $('#select-template-container').hide();
            $('#submit-template-container').show();
        });

        // handles any uploading error - could be an error response or a successful response with an encoded error
        var handleError = function (error) {
            // show any errors
            $('#upload-template-status').removeClass('import-status').addClass('import-status-error').text(error);

            // clear the form
            $('#cancel-upload-template-button').click();
        };

        // initialize the form
        var templateForm = $('#template-upload-form').ajaxForm({
            url: '../nifi-api/process-groups/' + encodeURIComponent(groupId) + '/templates/upload',
            dataType: 'xml',
            success: function (response, statusText, xhr, form) {
                // see if the import was successful
                if (response.documentElement.tagName === 'templateEntity') {
                    // reset the status message
                    $('#upload-template-status').removeClass('import-status-error').addClass('import-status').text(selectStatusMessage);

                    // clear the form
                    $('#cancel-upload-template-button').click();

                    // reload the templates table
                    nf.TemplatesTable.loadTemplatesTable();
                } else {
                    // import failed
                    var status = 'Unable to import template. Please check the log for errors.';
                    if (response.documentElement.tagName === 'errorResponse') {
                        // if a more specific error was given, use it
                        var errorMessage = response.documentElement.getAttribute('statusText');
                        if (!nf.Common.isBlank(errorMessage)) {
                            status = errorMessage;
                        }
                    }
                    handleError(status);
                }
            },
            error: function (xhr, statusText, error) {
                handleError(error);
            }
        });

        // add a handler for the upload button
        nf.Common.addHoverEffect('#upload-template-button', 'button-normal', 'button-over').click(function () {
            templateForm.submit();
        });

        // add a handler for the cancel upload button
        nf.Common.addHoverEffect('#cancel-upload-template-button', 'button-normal', 'button-over').click(function () {
            // set the filename
            $('#selected-template-name').text('');

            // reset the form to ensure that the change fire will fire
            templateForm.resetForm();

            // update the container visibility
            $('#select-template-container').show();
            $('#submit-template-container').hide();
        });

        // get the banners if we're not in the shell
        return $.Deferred(function (deferred) {
            if (top === window) {
                $.ajax({
                    type: 'GET',
                    url: config.urls.banners,
                    dataType: 'json'
                }).done(function (response) {
                    // ensure the banners response is specified
                    if (nf.Common.isDefinedAndNotNull(response.banners)) {
                        if (nf.Common.isDefinedAndNotNull(response.banners.headerText) && response.banners.headerText !== '') {
                            // update the header text
                            var bannerHeader = $('#banner-header').text(response.banners.headerText).show();

                            // show the banner
                            var updateTop = function (elementId) {
                                var element = $('#' + elementId);
                                element.css('top', (parseInt(bannerHeader.css('height'), 10) + parseInt(element.css('top'), 10)) + 'px');
                            };

                            // update the position of elements affected by top banners
                            updateTop('templates');
                        }

                        if (nf.Common.isDefinedAndNotNull(response.banners.footerText) && response.banners.footerText !== '') {
                            // update the footer text and show it
                            var bannerFooter = $('#banner-footer').text(response.banners.footerText).show();

                            var updateBottom = function (elementId) {
                                var element = $('#' + elementId);
                                element.css('bottom', parseInt(bannerFooter.css('height'), 10) + 'px');
                            };

                            // update the position of elements affected by bottom banners
                            updateBottom('templates');
                        }
                    }

                    deferred.resolve();
                }).fail(function (xhr, status, error) {
                    nf.Common.handleAjaxError(xhr, status, error);
                    deferred.reject();
                });
            } else {
                deferred.resolve();
            }
        }).promise();
    };

    return {
        /**
         * Initializes the templates page.
         */
        init: function () {
            nf.Storage.init();

            // ensure the group id is specified
            groupId = $('#template-group-id').text();
            if (nf.Common.isUndefined(groupId) || nf.Common.isNull(groupId)) {
                nf.Dialog.showOkDialog({
                    overlayBackground: false,
                    content: 'Group id not specified.'
                });
            }
            
            // load the users authorities
            loadAuthorities().done(function () {

                // create the templates table
                nf.TemplatesTable.init();

                // load the table
                nf.TemplatesTable.loadTemplatesTable().done(function () {
                    // once the table is initialized, finish initializing the page
                    initializeTemplatesPage().done(function () {
                        // configure the initial grid height
                        nf.TemplatesTable.resetTableSize();

                        // get the about details
                        $.ajax({
                            type: 'GET',
                            url: config.urls.about,
                            dataType: 'json'
                        }).done(function (response) {
                            var aboutDetails = response.about;
                            var templatesTitle = aboutDetails.title + ' Templates';

                            // set the document title and the about title
                            document.title = templatesTitle;
                            $('#templates-header-text').text(templatesTitle);
                        }).fail(nf.Common.handleAjaxError);
                    });
                });
            });
        }
    };
}());