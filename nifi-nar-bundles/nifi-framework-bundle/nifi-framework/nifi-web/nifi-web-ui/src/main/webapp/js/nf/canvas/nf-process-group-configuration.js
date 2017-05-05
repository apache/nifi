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
                'd3',
                'nf.ErrorHandler',
                'nf.Common',
                'nf.Dialog',
                'nf.Client',
                'nf.ProcessGroup',
                'nf.Shell',
                'nf.CanvasUtils'],
            function ($, d3, nfErrorHandler, nfCommon, nfDialog, nfClient, nfProcessGroup, nfShell, nfCanvasUtils) {
                return (nf.ProcessGroupConfiguration = factory($, d3, nfErrorHandler, nfCommon, nfDialog, nfClient, nfProcessGroup, nfShell, nfCanvasUtils));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ProcessGroupConfiguration =
            factory(require('jquery'),
                require('d3'),
                require('nf.ErrorHandler'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.Client'),
                require('nf.ProcessGroup'),
                require('nf.Shell'),
                require('nf.CanvasUtils')));
    } else {
        nf.ProcessGroupConfiguration = factory(root.$,
            root.d3,
            root.nf.ErrorHandler,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.Client,
            root.nf.ProcessGroup,
            root.nf.Shell,
            root.nf.CanvasUtils);
    }
}(this, function ($, d3, nfErrorHandler, nfCommon, nfDialog, nfClient, nfProcessGroup, nfShell, nfCanvasUtils) {
    'use strict';

    var nfControllerServices;

    var config = {
        urls: {
            api: '../nifi-api'
        }
    };

    /**
     * Initializes the general tab.
     */
    var initGeneral = function () {
    };

    /**
     * Gets the controller services table.
     *
     * @returns {*|jQuery|HTMLElement}
     */
    var getControllerServicesTable = function () {
        return $('#process-group-controller-services-table');
    };

    /**
     * Saves the configuration for the specified group.
     *
     * @param version
     * @param groupId
     */
    var saveConfiguration = function (version, groupId) {
        // build the entity
        var entity = {
            'revision': nfClient.getRevision({
                'revision': {
                    'version': version
                }
            }),
            'component': {
                'id': groupId,
                'name': $('#process-group-name').val(),
                'comments': $('#process-group-comments').val()
            }
        };

        // update the selected component
        $.ajax({
            type: 'PUT',
            data: JSON.stringify(entity),
            url: config.urls.api + '/process-groups/' + encodeURIComponent(groupId),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (response) {
            // refresh the process group if necessary
            if (response.permissions.canRead && response.component.parentGroupId === nfCanvasUtils.getGroupId()) {
                nfProcessGroup.set(response);
            }

            // show the result dialog
            nfDialog.showOkDialog({
                headerText: 'Process Group Configuration',
                dialogContent: 'Process group configuration successfully saved.'
            });

            // update the click listener for the updated revision
            $('#process-group-configuration-save').off('click').on('click', function () {
                saveConfiguration(response.revision.version, groupId);
            });

            nfCanvasUtils.reload();
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Loads the configuration for the specified process group.
     *
     * @param {string} groupId
     */
    var loadConfiguration = function (groupId) {
        var setUnauthorizedText = function () {
            $('#read-only-process-group-name').addClass('unset').text('Unauthorized');
            $('#read-only-process-group-comments').addClass('unset').text('Unauthorized');
        };

        var setEditable = function (editable) {
            if (editable) {
                $('#process-group-configuration div.editable').show();
                $('#process-group-configuration div.read-only').hide();
                $('#process-group-configuration-save').show();
            } else {
                $('#process-group-configuration div.editable').hide();
                $('#process-group-configuration div.read-only').show();
                $('#process-group-configuration-save').hide();
            }
        };

        // record the group id
        $('#process-group-id').text(groupId);

        // update the click listener
        $('#process-group-configuration-refresh-button').off('click').on('click', function () {
            loadConfiguration(groupId);
        });

        // update the new controller service click listener
        $('#add-process-group-configuration-controller-service').off('click').on('click', function () {
            var selectedTab = $('#process-group-configuration-tabs li.selected-tab').text();
            if (selectedTab === 'Controller Services') {
                var controllerServicesUri = config.urls.api + '/process-groups/' + encodeURIComponent(groupId) + '/controller-services';
                nfControllerServices.promptNewControllerService(controllerServicesUri, getControllerServicesTable());
            }
        });

        var processGroup = $.Deferred(function (deferred) {
            $.ajax({
                type: 'GET',
                url: config.urls.api + '/process-groups/' + encodeURIComponent(groupId),
                dataType: 'json'
            }).done(function (response) {
                // store the process group
                $('#process-group-configuration').data('process-group', response);

                var processGroup = response.component;

                if (response.permissions.canWrite) {

                    // populate the process group settings
                    $('#process-group-name').removeClass('unset').val(processGroup.name);
                    $('#process-group-comments').removeClass('unset').val(processGroup.comments);

                    // populate the header
                    $('#process-group-configuration-header-text').text(processGroup.name + ' Configuration');

                    setEditable(true);

                    // register the click listener for the save button
                    $('#process-group-configuration-save').off('click').on('click', function () {
                        saveConfiguration(response.revision.version, response.id);
                    });
                } else {
                    if (response.permissions.canRead) {
                        // populate the process group settings
                        $('#read-only-process-group-name').removeClass('unset').text(processGroup.name);
                        $('#read-only-process-group-comments').removeClass('unset').text(processGroup.comments);

                        // populate the header
                        $('#process-group-configuration-header-text').text(processGroup.name + ' Configuration');
                    } else {
                        setUnauthorizedText();
                    }

                    setEditable(false);
                }
                deferred.resolve();
            }).fail(function (xhr, status, error) {
                if (xhr.status === 403) {
                    if (groupId === nfCanvasUtils.getGroupId()) {
                        $('#process-group-configuration').data('process-group', {
                            'permissions': {
                                canRead: false,
                                canWrite: nfCanvasUtils.canWrite()
                            }
                        });
                    } else {
                        $('#process-group-configuration').data('process-group', nfProcessGroup.get(groupId));
                    }

                    setUnauthorizedText();
                    setEditable(false);
                    deferred.resolve();
                } else {
                    deferred.reject(xhr, status, error);
                }
            });
        }).promise();

        // load the controller services
        var controllerServicesUri = config.urls.api + '/flow/process-groups/' + encodeURIComponent(groupId) + '/controller-services';
        var controllerServices = nfControllerServices.loadControllerServices(controllerServicesUri, getControllerServicesTable());

        // wait for everything to complete
        return $.when(processGroup, controllerServices).done(function (processGroupResult, controllerServicesResult) {
            var controllerServicesResponse = controllerServicesResult[0];

            // update the current time
            $('#process-group-configuration-last-refreshed').text(controllerServicesResponse.currentTime);
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Shows the process group configuration.
     */
    var showConfiguration = function () {
        // show the configuration dialog
        nfShell.showContent('#process-group-configuration').done(function () {
            reset();
        });

        //reset content to account for possible policy changes
        $('#process-group-configuration-tabs').find('.selected-tab').click();

        // adjust the table size
        nfProcessGroupConfiguration.resetTableSize();
    };

    /**
     * Resets the process group configuration dialog.
     */
    var reset = function () {
        $('#process-group-configuration').removeData('process-group');

        // reset button state
        $('#process-group-configuration-save').mouseout();

        // reset the fields
        $('#process-group-id').text('');
        $('#process-group-name').val('');
        $('#process-group-comments').val('');

        // reset the header
        $('#process-group-configuration-header-text').text('Process Group Configuration');
    };

    var nfProcessGroupConfiguration = {

        /**
         * Initialize the process group configuration.
         *
         * @param nfControllerServicesRef   The nfControllerServices module.
         */
        init: function (nfControllerServicesRef) {
            nfControllerServices = nfControllerServicesRef;

            // initialize the process group configuration tabs
            $('#process-group-configuration-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                scrollableTabContentStyle: 'scrollable',
                tabs: [{
                    name: 'General',
                    tabContentId: 'general-process-group-configuration-tab-content'
                }, {
                    name: 'Controller Services',
                    tabContentId: 'process-group-controller-services-tab-content'
                }],
                select: function () {
                    var processGroup = $('#process-group-configuration').data('process-group');
                    var canWrite = nfCommon.isDefinedAndNotNull(processGroup) ? processGroup.permissions.canWrite : false;

                    var tab = $(this).text();
                    if (tab === 'General') {
                        $('#flow-cs-availability').hide();
                        $('#add-process-group-configuration-controller-service').hide();

                        if (canWrite) {
                            $('#process-group-configuration-save').show();
                        } else {
                            $('#process-group-configuration-save').hide();
                        }
                    } else {
                        $('#flow-cs-availability').show();
                        $('#process-group-configuration-save').hide();

                        if (canWrite) {
                            $('#add-process-group-configuration-controller-service').show();
                            $('#process-group-controller-services-tab-content').css('top', '32px');
                        } else {
                            $('#add-process-group-configuration-controller-service').hide();
                            $('#process-group-controller-services-tab-content').css('top', '0');
                        }

                        // resize the table
                        nfProcessGroupConfiguration.resetTableSize();
                    }
                }
            });

            // initialize each tab
            initGeneral();
            nfControllerServices.init(getControllerServicesTable());
        },

        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            nfControllerServices.resetTableSize(getControllerServicesTable());
        },

        /**
         * Shows the settings dialog.
         */
        showConfiguration: function (groupId) {
            return loadConfiguration(groupId).done(showConfiguration);
        },

        /**
         * Loads the configuration for the specified process group.
         *
         * @param groupId
         */
        loadConfiguration: function (groupId) {
            return loadConfiguration(groupId);
        },

        /**
         * Selects the specified controller service.
         *
         * @param {string} controllerServiceId
         */
        selectControllerService: function (controllerServiceId) {
            var controllerServiceGrid = getControllerServicesTable().data('gridInstance');
            var controllerServiceData = controllerServiceGrid.getData();

            // select the desired service
            var row = controllerServiceData.getRowById(controllerServiceId);
            controllerServiceGrid.setSelectedRows([row]);
            controllerServiceGrid.scrollRowIntoView(row);

            // select the controller services tab
            $('#process-group-configuration-tabs').find('li:eq(1)').click();
        }
    };

    return nfProcessGroupConfiguration;
}));