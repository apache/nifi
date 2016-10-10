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

/* global nf, d3 */

nf.ProcessGroupConfiguration = (function () {

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
            'revision': nf.Client.getRevision({
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
            if (response.permissions.canRead && response.component.parentGroupId === nf.Canvas.getGroupId()) {
                nf.ProcessGroup.set(response);
            }

            // show the result dialog
            nf.Dialog.showOkDialog({
                headerText: 'Process Group Configuration',
                dialogContent: 'Process group configuration successfully saved.'
            });

            // update the click listener for the updated revision
            $('#process-group-configuration-save').off('click').on('click', function () {
                saveConfiguration(response.revision.version, groupId);
            });

            // inform Angular app values have changed
            nf.ng.Bridge.digest();
        }).fail(nf.Common.handleAjaxError);
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

        var processGroup = $.Deferred(function (deferred) {
            $.ajax({
                type: 'GET',
                url: config.urls.api + '/process-groups/' + encodeURIComponent(groupId),
                dataType: 'json'
            }).done(function (response) {
                // store the process group
                $('#process-group-configuration').data('process-group', response);

                if (response.permissions.canWrite) {
                    var processGroup = response.component;

                    // populate the process group settings
                    $('#process-group-id').text(processGroup.id);
                    $('#process-group-name').removeClass('unset').val(processGroup.name);
                    $('#process-group-comments').removeClass('unset').val(processGroup.comments);

                    setEditable(true);

                    // register the click listener for the save button
                    $('#process-group-configuration-save').off('click').on('click', function () {
                        saveConfiguration(response.revision.version, response.id);
                    });
                } else {
                    if (response.permissions.canRead) {
                        // populate the process group settings
                        $('#read-only-process-group-name').removeClass('unset').text(response.component.name);
                        $('#read-only-process-group-comments').removeClass('unset').text(response.component.comments);
                    } else {
                        setUnauthorizedText();
                    }

                    setEditable(false);
                }
                deferred.resolve();
            }).fail(function (xhr, status, error) {
                if (xhr.status === 403) {
                    if (groupId === nf.Canvas.getGroupId()) {
                        $('#process-group-configuration').data('process-group', {
                            'permissions': {
                                canRead: false,
                                canWrite: nf.Canvas.canWrite()
                            }
                        });
                    } else {
                        $('#process-group-configuration').data('process-group', nf.ProcessGroup.get(groupId));
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
        var controllerServices = nf.ControllerServices.loadControllerServices(controllerServicesUri, getControllerServicesTable());
        
        // wait for everything to complete
        return $.when(processGroup, controllerServices).done(function (processGroupResult, controllerServicesResult) {
            var controllerServicesResponse = controllerServicesResult[0];

            // update the current time
            $('#process-group-configuration-last-refreshed').text(controllerServicesResponse.currentTime);
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Shows the process group configuration.
     */
    var showConfiguration = function () {
        // show the configuration dialog
        nf.Shell.showContent('#process-group-configuration').done(function () {
            reset();
        });

        //reset content to account for possible policy changes
        $('#process-group-configuration-tabs').find('.selected-tab').click();

        // adjust the table size
        nf.ProcessGroupConfiguration.resetTableSize();
    };

    /**
     * Resets the process group configuration dialog.
     */
    var reset = function () {
        $('#process-group-configuration').removeData('process-group');

        // reset button state
        $('#process-group-configuration-save').mouseout();

        // reset the fields
        $('#process-group-name').val('');
        $('#process-group-comments').val('');
    };
    
    return {
        /**
         * Initializes the settings page.
         */
        init: function () {
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
                    var canWrite = nf.Common.isDefinedAndNotNull(processGroup) ? processGroup.permissions.canWrite : false;

                    var tab = $(this).text();
                    if (tab === 'General') {
                        $('#add-process-group-configuration-controller-service').hide();

                        if (canWrite) {
                            $('#process-group-configuration-save').show();
                        } else {
                            $('#process-group-configuration-save').hide();
                        }
                    } else {
                        $('#process-group-configuration-save').hide();

                        if (canWrite) {
                            $('#add-process-group-configuration-controller-service').show();
                            $('#process-group-controller-services-tab-content').css('top', '32px');
                        } else {
                            $('#add-process-group-configuration-controller-service').hide();
                            $('#process-group-controller-services-tab-content').css('top', '0');
                        }

                        // resize the table
                        nf.ProcessGroupConfiguration.resetTableSize();
                    }
                }
            });

            // initialize each tab
            initGeneral();
            nf.ControllerServices.init(getControllerServicesTable());
        },

        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            nf.ControllerServices.resetTableSize(getControllerServicesTable());
        },

        /**
         * Shows the settings dialog.
         */
        showConfiguration: function (groupId) {
            // update the click listener
            $('#process-group-configuration-refresh-button').off('click').on('click', function () {
                loadConfiguration(groupId);
            });

            // update the new controller service click listener
            $('#add-process-group-configuration-controller-service').off('click').on('click', function () {
                var selectedTab = $('#process-group-configuration-tabs li.selected-tab').text();
                if (selectedTab === 'Controller Services') {
                    var controllerServicesUri = config.urls.api + '/process-groups/' + encodeURIComponent(groupId) + '/controller-services';
                    nf.ControllerServices.promptNewControllerService(controllerServicesUri, getControllerServicesTable());
                }
            });

            // load the configuration
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
}());