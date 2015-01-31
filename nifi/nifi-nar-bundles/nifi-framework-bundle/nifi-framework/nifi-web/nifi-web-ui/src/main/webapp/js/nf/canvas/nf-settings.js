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
nf.Settings = (function () {

    var config = {
        urls: {
            controllerConfig: '../nifi-api/controller/config',
            controllerArchive: '../nifi-api/controller/archive'
        }
    };

    /**
     * Marshals the details to include in the configuration request.
     */
    var marshalConfiguration = function () {
        // create the configuration
        var configuration = {};
        configuration['name'] = $('#data-flow-title-field').val();
        configuration['comments'] = $('#data-flow-comments-field').val();
        configuration['maxTimerDrivenThreadCount'] = $('#maximum-timer-driven-thread-count-field').val();
        configuration['maxEventDrivenThreadCount'] = $('#maximum-event-driven-thread-count-field').val();
        return configuration;
    };

    return {
        /**
         * Initializes the status page.
         */
        init: function () {

            // register the click listener for the archive link
            $('#archive-flow-link').click(function () {
                var revision = nf.Client.getRevision();

                $.ajax({
                    type: 'POST',
                    url: config.urls.controllerArchive,
                    data: {
                        version: revision.version,
                        clientId: revision.clientId
                    },
                    dataType: 'json'
                }).done(function (response) {
                    // update the revision
                    nf.Client.setRevision(response.revision);

                    // show the result dialog
                    nf.Dialog.showOkDialog({
                        dialogContent: 'A new flow archive was successfully created.',
                        overlayBackground: false
                    });
                }).fail(function (xhr, status, error) {
                    // close the details panel
                    $('#settings-cancel').click();

                    // handle the error
                    nf.Common.handleAjaxError(xhr, status, error);
                });
            });

            // register the click listener for the save button
            $('#settings-save').click(function () {
                var revision = nf.Client.getRevision();

                // marshal the configuration details
                var configuration = marshalConfiguration();
                configuration['version'] = revision.version;
                configuration['clientId'] = revision.clientId;

                // save the new configuration details
                $.ajax({
                    type: 'PUT',
                    url: config.urls.controllerConfig,
                    data: configuration,
                    dataType: 'json'
                }).done(function (response) {
                    // update the revision
                    nf.Client.setRevision(response.revision);

                    // update the displayed name
                    document.title = response.config.name;

                    // set the data flow title and close the shell
                    $('#data-flow-title-container').children('span.link:first-child').text(response.config.name);

                    // close the settings dialog
                    $('#shell-close-button').click();
                }).fail(function (xhr, status, error) {
                    // close the details panel
                    $('#settings-cancel').click();

                    // handle the error
                    nf.Common.handleAjaxError(xhr, status, error);
                });
            });

            // install a cancel button listener to close the shell
            $('#settings-cancel').click(function () {
                $('#shell-close-button').click();
            });
        },
        
        /**
         * Shows the settings dialog.
         */
        showSettings: function () {
            $.ajax({
                type: 'GET',
                url: config.urls.controllerConfig,
                dataType: 'json'
            }).done(function (response) {
                // ensure the config is present
                if (nf.Common.isDefinedAndNotNull(response.config)) {
                    // set the header
                    $('#settings-header-text').text(response.config.name + ' Settings');

                    // populate the controller settings
                    $('#data-flow-title-field').val(response.config.name);
                    $('#data-flow-comments-field').val(response.config.comments);
                    $('#maximum-timer-driven-thread-count-field').val(response.config.maxTimerDrivenThreadCount);
                    $('#maximum-event-driven-thread-count-field').val(response.config.maxEventDrivenThreadCount);
                }

                // show the settings dialog
                nf.Shell.showContent('#settings').done(function () {
                    // reset button state
                    $('#settings-save, #settings-cancel').mouseout();
                });
            }).fail(nf.Common.handleAjaxError);
        }
    };
}());