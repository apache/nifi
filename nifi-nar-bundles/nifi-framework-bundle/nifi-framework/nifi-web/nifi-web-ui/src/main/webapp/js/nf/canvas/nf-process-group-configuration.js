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
                'nf.Storage',
                'nf.Client',
                'nf.ProcessGroup',
                'nf.Shell',
                'nf.CanvasUtils'],
            function ($, d3, nfErrorHandler, nfCommon, nfDialog, nfStorage, nfClient, nfProcessGroup, nfShell, nfCanvasUtils) {
                return (nf.ProcessGroupConfiguration = factory($, d3, nfErrorHandler, nfCommon, nfDialog, nfStorage, nfClient, nfProcessGroup, nfShell, nfCanvasUtils));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ProcessGroupConfiguration =
            factory(require('jquery'),
                require('d3'),
                require('nf.ErrorHandler'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.Storage'),
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
            root.nf.Storage,
            root.nf.Client,
            root.nf.ProcessGroup,
            root.nf.Shell,
            root.nf.CanvasUtils);
    }
}(this, function ($, d3, nfErrorHandler, nfCommon, nfDialog, nfStorage, nfClient, nfProcessGroup, nfShell, nfCanvasUtils) {
    'use strict';

    var nfControllerServices;
    var nfParameterContexts;

    var config = {
        urls: {
            api: '../nifi-api',
            parameterContexts: '../nifi-api/flow/parameter-contexts'
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
            'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
            'component': {
                'id': groupId,
                'name': $('#process-group-name').val(),
                'comments': $('#process-group-comments').val(),
                'parameterContext': {
                    'id': $('#process-group-parameter-context-combo').combo('getSelectedOption').value
                },
                'flowfileConcurrency': $('#process-group-flowfile-concurrency-combo').combo('getSelectedOption').value,
                'flowfileOutboundPolicy': $('#process-group-outbound-policy-combo').combo('getSelectedOption').value
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

            var controllerServicesUri = config.urls.api + '/flow/process-groups/' + encodeURIComponent(groupId) + '/controller-services';

            $.ajax({
                type: 'GET',
                url: controllerServicesUri,
                dataType: 'json'
            }).done(function (response) {
                var serviceTable = getControllerServicesTable();

                nfCommon.cleanUpTooltips(serviceTable, 'div.has-errors');

                var controllerServicesGrid = serviceTable.data('gridInstance');
                var controllerServicesData = controllerServicesGrid.getData();

                $.each(response.controllerServices, function (_, controllerServiceEntity) {
                    controllerServicesData.updateItem(controllerServiceEntity.id, controllerServiceEntity);
                });
            });

            nfCanvasUtils.reload();
        }).fail(nfErrorHandler.handleConfigurationUpdateAjaxError);
    };

    /**
     * Loads the configuration for the specified process group.
     *
     * @param {string} groupId
     */
    var loadConfiguration = function (groupId) {
        var setUnauthorizedText = function () {
            $('#read-only-process-group-name').text('Unauthorized');
            $('#read-only-process-group-comments').text('Unauthorized');
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
                    $('#process-group-flowfile-concurrency-combo').removeClass('unset').combo({
                        options: [{
                                text: 'Single FlowFile Per Node',
                                value: 'SINGLE_FLOWFILE_PER_NODE',
                                description: 'Only a single FlowFile is to be allowed to enter the Process Group at a time on each node in the cluster. While that FlowFile may be split into many or '
                                    + 'spawn many children, no additional FlowFiles will be allowed to enter the Process Group through a Local Input Port until the previous FlowFile '
                                    + '- and all of its child/descendent FlowFiles - have been processed.'
                            }, {
                                text: 'Single Batch Per Node',
                                value: "SINGLE_BATCH_PER_NODE",
                                description: 'When an Input Port pulls a FlowFile into the Process Group, FlowFiles will continue to be ingested into the Process Group until all input queues '
                                    + 'have been emptied. At that point, no additional FlowFiles will be allowed to enter the Process Group through a Local Input Port until the entire batch '
                                    + 'of FlowFiles has been processed.'
                            },{
                                text: 'Unbounded',
                                value: 'UNBOUNDED',
                                description: 'The number of FlowFiles that can be processed concurrently is unbounded.'
                            }],
                        selectedOption: {
                            value: processGroup.flowfileConcurrency
                        }
                    });

                    $('#process-group-outbound-policy-combo').removeClass('unset').combo({
                        options: [{
                                text: 'Stream When Available',
                                value: 'STREAM_WHEN_AVAILABLE',
                                description: 'FlowFiles that are queued up to be transferred out of a Process Group by an Output Port will be transferred out '
                                        + 'of the Process Group as soon as they are available.'
                            }, {
                                text: 'Batch Output',
                                value: 'BATCH_OUTPUT',
                                description: 'FlowFiles that are queued up to be transferred out of a Process Group by an Output Port will remain queued until '
                                        + 'all FlowFiles in the Process Group are ready to be transferred out of the group. The FlowFiles will then be transferred '
                                        + 'out of the group. This setting will be ignored if the FlowFile Concurrency is Unbounded.'
                            }],
                        selectedOption: {
                            value: processGroup.flowfileOutboundPolicy
                        }
                    });


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
                        $('#read-only-process-group-name').text(processGroup.name);
                        $('#read-only-process-group-comments').text(processGroup.comments);

                        // Determine the user-friendly name for the selected FlowFile Concurrency
                        var concurrencyName;
                        if (processGroup.flowfileConcurrency == "UNBOUNDED") {
                            concurrencyName = "Unbounded";
                        } else if (processGroup.flowfileConcurrency == "SINGLE_FLOWFILE_PER_NODE") {
                            concurrencyName = "Single FlowFile Per Node";
                        } else if (processGroup.flowfileConcurrency == "SINGLE_BATCH_PER_NODE") {
                            concurrencyName = "Single Batch Per Node";
                        } else {
                            concurrencyName = "Unknown";
                        }

                        $('#read-only-process-group-flowfile-concurrency').text(concurrencyName);

                        var outboundPolicyName = processGroup.flowfileOutboundPolicy == "BATCH_OUTPUT" ? "Batch Output" : "Stream When Available";
                        $('#read-only-process-group-outbound-policy').text(outboundPolicyName);

                        // populate the header
                        $('#process-group-configuration-header-text').text(processGroup.name + ' Configuration');
                    } else {
                        setUnauthorizedText();
                    }

                    setEditable(false);
                }
                deferred.resolve(response);
            }).fail(function (xhr, status, error) {
                if (xhr.status === 403) {
                    var unauthorizedGroup;
                    if (groupId === nfCanvasUtils.getGroupId()) {
                        unauthorizedGroup = {
                            'permissions': {
                                canRead: false,
                                canWrite: nfCanvasUtils.canWriteCurrentGroup()
                            }
                        };
                    } else {
                        unauthorizedGroup = nfProcessGroup.get(groupId);
                    }
                    $('#process-group-configuration').data('process-group', unauthorizedGroup);

                    setUnauthorizedText();
                    setEditable(false);
                    deferred.resolve(unauthorizedGroup);
                } else {
                    deferred.reject(xhr, status, error);
                }
            });
        }).promise();

        // load the controller services
        var controllerServicesUri = config.urls.api + '/flow/process-groups/' + encodeURIComponent(groupId) + '/controller-services';
        var controllerServices = nfControllerServices.loadControllerServices(controllerServicesUri, getControllerServicesTable());

        var parameterContexts = $.ajax({
                type: 'GET',
                url: config.urls.parameterContexts,
                dataType: 'json'
            });

        // wait for everything to complete
        return $.when(processGroup, controllerServices, parameterContexts).done(function (processGroupResult, controllerServicesResult, parameterContextsResult) {
            var controllerServicesResponse = controllerServicesResult[0];
            var parameterContextsResponse = parameterContextsResult[0];

            // update the current time
            $('#process-group-configuration-last-refreshed').text(controllerServicesResponse.currentTime);

            var parameterContexts = parameterContextsResponse.parameterContexts;
            var options = [{
                text: 'No parameter context',
                value: null
            }];

            var authorizedParameterContexts = parameterContexts.filter(function (parameterContext) {
                return parameterContext.permissions.canRead;
            });

            var unauthorizedParameterContexts = parameterContexts.filter(function (parameterContext) {
                return !parameterContext.permissions.canRead;
            });

            //sort alphabetically
            var sortedAuthorizedParameterContexts = authorizedParameterContexts.sort(function (a, b) {
                if (a.component.name < b.component.name) {
                    return -1;
                }
                if (a.component.name > b.component.name) {
                    return 1;
                }
                return 0;
            });

            //sort alphabetically
            var sortedUnauthorizedParameterContexts = unauthorizedParameterContexts.sort(function (a, b) {
                if (a.id < b.id) {
                    return -1;
                }
                if (a.id > b.id) {
                    return 1;
                }
                return 0;
            });

            var sortedParameterContexts = sortedAuthorizedParameterContexts.concat(sortedUnauthorizedParameterContexts);

            sortedParameterContexts.forEach(function (parameterContext) {
                var option;
                if (parameterContext.permissions.canRead) {
                    option = {
                        'text': parameterContext.component.name,
                        'value': parameterContext.id,
                        'description': parameterContext.component.description
                    };
                } else {
                    option = {
                        'disabled': true,
                        'text': parameterContext.id,
                        'value': parameterContext.id
                    }
                }

                options.push(option);
            });

            var createNewParameterContextOption = {
                text: 'Create new parameter context...',
                value: undefined,
                optionClass: 'unset'
            };

            if (nfCommon.canModifyParameterContexts()) {
                options.push(createNewParameterContextOption);
            }

            var comboOptions = {
                options: options,
                select: function (option) {
                    var combo = this;
                    if (typeof option.value === 'undefined') {
                        $('#parameter-context-dialog').modal('setHeaderText', 'Add Parameter Context').modal('setButtonModel', [{
                            buttonText: 'Apply',
                            color: {
                                base: '#728E9B',
                                hover: '#004849',
                                text: '#ffffff'
                            },
                            disabled: function () {
                                if ($('#parameter-context-name').val() !== '') {
                                    return false;
                                }
                                return true;
                            },
                            handler: {
                                click: function () {
                                    nfParameterContexts.addParameterContext(function (parameterContextEntity) {
                                        options.pop();
                                        var option = {
                                            'text': parameterContextEntity.component.name,
                                            'value': parameterContextEntity.component.id,
                                            'description': parameterContextEntity.component.description
                                        };
                                        options.push(option);

                                        if (nfCommon.canModifyParameterContexts()) {
                                            options.push(createNewParameterContextOption);
                                        }

                                        comboOptions.selectedOption = {
                                            value: parameterContextEntity.component.id
                                        };

                                        combo.combo('destroy').combo(comboOptions);
                                    });
                                }
                            }
                        }, {
                            buttonText: 'Cancel',
                            color: {
                                base: '#E3E8EB',
                                hover: '#C7D2D7',
                                text: '#004849'
                            },
                            handler: {
                                click: function () {
                                    $(this).modal('hide');
                                }
                            }
                        }]).modal('show');

                        // make sure the edit mode is properly set
                        if ($('#parameter-context-dialog').hasClass('read-only')) {
                            $('#parameter-context-dialog').removeClass('read-only');
                            $('#parameter-context-dialog').addClass('edit-mode');
                        }

                        // there is no id yet, make sure the id field isn't shown
                        if (!$('#parameter-context-id-setting').hasClass('hidden')) {
                            $('#parameter-context-id-setting').addClass('hidden');
                        }

                        // set the initial focus
                        $('#parameter-context-name').focus();
                    }
                }
            };

            // populate the parameter context
            if (nfCommon.isDefinedAndNotNull(processGroupResult.parameterContext)) {
                comboOptions.selectedOption = {
                    value: processGroupResult.parameterContext.id
                };
            }

            // initialize the parameter context combo
            $('#process-group-parameter-context-combo').combo('destroy').combo(comboOptions);
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
         * @param nfParameterContextsRef    The nfParameterContexts module.
         */
        init: function (nfControllerServicesRef, nfParameterContextsRef) {
            nfControllerServices = nfControllerServicesRef;
            nfParameterContexts = nfParameterContextsRef;

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
