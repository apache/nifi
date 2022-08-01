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
                'Slick',
                'nf.ErrorHandler',
                'nf.Common',
                'nf.CanvasUtils',
                'nf.Dialog',
                'nf.Storage',
                'nf.Client',
                'nf.ControllerService',
                'nf.ControllerServices',
                'nf.UniversalCapture',
                'nf.CustomUi',
                'nf.Verify',
                'nf.Processor',
                'nf.ProcessGroup',
                'nf.ParameterContexts'],
            function ($, Slick, nfErrorHandler, nfCommon, nfCanvasUtils, nfDialog, nfStorage, nfClient, nfControllerService, nfControllerServices, nfUniversalCapture, nfCustomUi, nfVerify, nfProcessor, nfProcessGroup, nfParameterContexts) {
                return (nf.ParameterProvider = factory($, Slick, nfErrorHandler, nfCommon, nfCanvasUtils, nfDialog, nfStorage, nfClient, nfControllerService, nfControllerServices, nfUniversalCapture, nfCustomUi, nfVerify, nfProcessor, nfProcessGroup, nfParameterContexts));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ParameterProvider =
            factory(require('jquery'),
                require('Slick'),
                require('nf.ErrorHandler'),
                require('nf.Common'),
                require('nf.CanvasUtils'),
                require('nf.Dialog'),
                require('nf.Storage'),
                require('nf.Client'),
                require('nf.ControllerService'),
                require('nf.ControllerServices'),
                require('nf.UniversalCapture'),
                require('nf.CustomUi'),
                require('nf.Verify'),
                require('nf.Processor'),
                require('nf.ProcessGroup'),
                require('nf.ParameterContexts')));
    } else {
        nf.ParameterProvider = factory(root.$,
            root.Slick,
            root.nf.ErrorHandler,
            root.nf.Common,
            root.nf.CanvasUtils,
            root.nf.Dialog,
            root.nf.Storage,
            root.nf.Client,
            root.nf.ControllerService,
            root.nf.ControllerServices,
            root.nf.UniversalCapture,
            root.nf.CustomUi,
            root.nf.Verify,
            root.nf.Processor,
            root.nf.ProcessGroup,
            root.nf.ParameterContexts);
    }
}(this, function ($, Slick, nfErrorHandler, nfCommon, nfCanvasUtils, nfDialog, nfStorage, nfClient, nfControllerService, nfControllerServices, nfUniversalCapture, nfCustomUi, nfVerify, nfProcessor, nfProcessGroup, nfParameterContexts) {
    'use strict';

    var nfSettings;

    var config = {
        urls: {
            parameterProviders: '../nifi-api/parameter-providers'
        }
    };

    var groupCount = 0;
    var groupIndex = 0;

    var parameterCount = 0;
    var sensitiveParametersArray = [];

    var SENSITIVE = 'SENSITIVE';
    var NON_SENSITIVE = 'NON_SENSITIVE';

    var parameterGroupsGridOptions = {
        autosizeColsMode: Slick.GridAutosizeColsMode.LegacyForceFit,
        enableTextSelectionOnCells: true,
        enableCellNavigation: true,
        enableColumnReorder: false,
        editable: false,
        enableAddRow: false,
        autoEdit: false,
        multiSelect: false,
        rowHeight: 24
    };

    var selectableParametersGridOptions = {
        autosizeColsMode: Slick.GridAutosizeColsMode.LegacyForceFit,
        enableTextSelectionOnCells: true,
        enableCellNavigation: true,
        enableColumnReorder: false,
        editable: false,
        enableAddRow: false,
        autoEdit: false,
        multiSelect: false,
        rowHeight: 24,
        asyncEditorLoading: false
    };

    // the last submitted referenced attributes
    var referencedAttributes = null;

    /**
     * Gets the controller services table.
     *
     * @returns {*|jQuery|HTMLElement}
     */
    var getControllerServicesTable = function () {
        return $('#controller-services-table');
    };

    /**
     * Determines whether the user has made any changes to the parameter provider configuration
     * that needs to be saved.
     */
    var isSaveRequired = function () {
        var entity = $('#parameter-provider-configuration').data('parameterProviderDetails');

        // determine if any parameter provider settings have changed
        if ($('#parameter-provider-name').val() !== entity.component['name']) {
            return true;
        }
        if ($('#parameter-provider-comments').val() !== entity.component['comments']) {
            return true;
        }

        // defer to the properties
        return $('#parameter-provider-properties').propertytable('isSaveRequired');
    };


    /**
     * Marshals the data that will be used to update the parameter provider's configuration.
     */
    var marshalDetails = function () {
        // properties
        var properties = $('#parameter-provider-properties').propertytable('marshalProperties');

        // create the parameter provider dto
        var parameterProviderDto = {};
        parameterProviderDto['id'] = $('#parameter-provider-id').text();
        parameterProviderDto['name'] = $('#parameter-provider-name').val();
        parameterProviderDto['comments'] = $('#parameter-provider-comments').val();

        // set the properties
        if ($.isEmptyObject(properties) === false) {
            parameterProviderDto['properties'] = properties;
        }

        // create the parameter provider entity
        var parameterProviderEntity = {};
        parameterProviderEntity['component'] = parameterProviderDto;

        // return the marshaled details
        return parameterProviderEntity;
    };

    /**
     * Marshals the parameter groups in the table.
     */
    var marshalParameterGroups = function () {
        var groups = [];
        var table = $('#parameter-groups-table');
        var groupsGrid = table.data('gridInstance');
        var groupsData = groupsGrid.getData();
        $.each(groupsData.getItems(), function (_, g) {
            if (g.isParameterContext) {
                var group = {
                    'groupName': g.name,
                    'parameterContextName': g.parameterContextName,
                    'synchronized': true,
                    'parameterSensitivities': g.parameterSensitivities
                }

                groups.push(group);
            }
        })

        return groups;
    };

    /**
     * Creates a listing of parameter names.
     *
     * @param parameterGroup the parameter group
     * @param groupName the parameter group name
     * @returns {*|HTMLElement} listItem
     */
    function createParametersListing(parameterGroup, groupName) {
        var listItem = $('<ol class="fetched-parameters">' + groupName + '</ol>');

        if (parameterGroup.length !== 0) {
            $.each(parameterGroup, function (i, parameterName) {
                var li = $('<li></li>').text(parameterName);
                $('<div class="clear"></div>').appendTo(li);
                li.appendTo(listItem);
            });
        }

        return listItem;
    }

    /**
     * Validates the specified details.
     *
     * @param providerDetails the parameter provider details to validate
     * @param originalProviderDetails the original parameter provider details to compare changes
     * @param existingParametersProviders existing parameter providers to verify there are no duplicates
     * @return {boolean}
     */
    var validateDetails = function (providerDetails, existingParametersProviders, originalProviderDetails) {
        var parameterProvider = providerDetails['component'];

        if (parameterProvider.name === '') {
            nfDialog.showOkDialog({
                headerText: 'Configuration Error',
                dialogContent: 'The name of the Parameter Provider must be specified.'
            });
            return false;
        }

        // make sure the parameter provider name does not use any unsupported characters
        var parameterProviderNameRegex = /^[a-zA-Z0-9-_. ]+$/;
        if (!parameterProviderNameRegex.test(parameterProvider.name)) {
            nfDialog.showOkDialog({
                headerText: 'Configuration Error',
                dialogContent: 'The name of the Parameter Provider appears to have an invalid character or characters. Only alpha-numeric characters (a-z, A-Z, 0-9), hyphens (-), underscores (_), periods (.), and spaces ( ) are accepted.'
            });
            return false;
        }

        // validate the parameter provider is not a duplicate
        var matchingParameterProvider;
        var match;

        if (nfCommon.isUndefinedOrNull(matchingParameterProvider) && originalProviderDetails.component.name !== providerDetails.component.name){
            $.each(existingParametersProviders, function (i, provider) {
                if (nfCommon.isUndefinedOrNull(match)) {
                    match = _.find(provider, {name: parameterProvider.name});
                    if (match) {
                        matchingParameterProvider = match;
                    }
                }

            });
        }

        if (_.isNil(matchingParameterProvider)) {
            return true;
        } else {
            nfDialog.showOkDialog({
                    headerText: 'Parameter Provider Exists',
                    dialogContent: 'A Parameter Provider with this name already exists.'
                });
            }
            return false;
    };

    /**
     * Renders the specified parameter provider.
     *
     * @param {object} parameterProviderEntity parameter provider entity
     */
    var renderParameterProvider = function (parameterProviderEntity) {
        // get the table and update the row accordingly
        var parameterProviderGrid = $('#parameter-providers-table').data('gridInstance');
        var parameterProviderData = parameterProviderGrid.getData();
        var currentParameterProvider = parameterProviderData.getItemById(parameterProviderEntity.id);
        parameterProviderData.updateItem(parameterProviderEntity.id, $.extend({
            type: 'ParameterProvider',
            bulletins: currentParameterProvider.bulletins
        }, parameterProviderEntity));
    };

    /**
     * Goes to a service configuration from the property table.
     */
    var goToServiceFromProperty = function () {
        return $.Deferred(function (deferred) {
            // close all fields currently being edited
            $('#parameter-provider-properties').propertytable('saveRow');

            // determine if changes have been made
            if (isSaveRequired()) {
                // see if those changes should be saved
                nfDialog.showYesNoDialog({
                    headerText: 'Save',
                    dialogContent: 'Save changes before going to this Controller Service?',
                    noHandler: function () {
                        deferred.resolve();
                    },
                    yesHandler: function () {
                        var parameterProvider = $('#parameter-provider-configuration').data('parameterProviderDetails');
                        saveParameterProvider(parameterProvider).done(function () {
                            deferred.resolve();
                        }).fail(function () {
                            deferred.reject();
                        });
                    }
                });
            } else {
                deferred.resolve();
            }
        }).promise();
    };

    /**
     * Saves the specified parameter provider.
     *
     * @param {type} parameterProviderEntity parameter provider entity
     */
    var saveParameterProvider = function (parameterProviderEntity) {
        // save the original provider to detect a name change
        var originalParameterProvider = parameterProviderEntity;

        // marshal the settings and properties and update the parameter provider
        var updatedParameterProvider = marshalDetails();

        // ensure details are valid as far as we can tell
        var parameterProvidersGrid = $('#parameter-providers-table').data('gridInstance');
        var parameterProvidersData = parameterProvidersGrid.getData();

        if (validateDetails(updatedParameterProvider, parameterProvidersData.getItems(), originalParameterProvider)) {
            updatedParameterProvider['revision'] = nfClient.getRevision(parameterProviderEntity);
            updatedParameterProvider['disconnectedNodeAcknowledged'] = nfStorage.isDisconnectionAcknowledged();

            // update the selected component
            return $.ajax({
                type: 'PUT',
                data: JSON.stringify(updatedParameterProvider),
                url: parameterProviderEntity.uri,
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // update the parameter provider
                renderParameterProvider(response);
            }).fail(nfErrorHandler.handleConfigurationUpdateAjaxError);
        } else {
            return $.Deferred(function (deferred) {
                deferred.reject();
            }).promise();
        }
    };

    /**
     * Gets a property descriptor for the parameter provider currently being configured.
     *
     * @param {type} propertyName property descriptor name
     */
    var getParameterProviderPropertyDescriptor = function (propertyName) {
        var details = $('#parameter-provider-configuration').data('parameterProviderDetails');
        return $.ajax({
            type: 'GET',
            url: details.uri + '/descriptors',
            data: {
                propertyName: propertyName
            },
            dataType: 'json'
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Handles verification results.
     */
    var handleVerificationResults = function (verificationResults, referencedAttributeMap) {
        // record the most recently submitted referenced attributes
        referencedAttributes = referencedAttributeMap;

        var verificationResultsContainer = $('#parameter-provider-properties-verification-results');

        // expand the dialog to make room for the verification result
        if (verificationResultsContainer.is(':visible') === false) {
            // show the verification results
            $('#parameter-provider-properties').css('bottom', '40%').propertytable('resetTableSize')
            verificationResultsContainer.show();
        }

        // show borders if appropriate
        var verificationResultsListing = $('#parameter-provider-properties-verification-results-listing');
        if (verificationResultsListing.get(0).scrollHeight > Math.round(verificationResultsListing.innerHeight())) {
            verificationResultsListing.css('border-width', '1px');
        }
    };

    /**
     * Applies the fetched parameters of a specified Parameter Provider.
     *
     * @param parameterProviderEntity
     * @returns {*}
     */
    var applyParametersHandler = function (parameterProviderEntity) {
        var currentParameterProviderEntity = parameterProviderEntity;
        var fetchParametersDialog = $('#fetch-parameters-dialog');

        // clean up any tooltips that may have been generated
        nfCommon.cleanUpTooltips($('#parameter-table'), 'div.fa-question-circle, div.fa-info');

        var groups = marshalParameterGroups();
        currentParameterProviderEntity.parameterGroupConfigurations = [...groups];

        return $.Deferred(function (deferred) {
            // updates the button model to show the close button
            var updateToCloseButtonModel = function () {
                fetchParametersDialog.modal('setButtonModel', [{
                    buttonText: 'Close',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            deferred.resolve();
                            closeModal('#fetch-parameters-dialog');
                        }
                    }
                }]);
            };

            var updateToApplyOrCancelButtonModel = function () {
                fetchParametersDialog.modal('setButtonModel', [{
                    buttonText: 'Apply',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            if ($('#fetch-parameters-referencing-components').is(':visible')) {
                                updateReferencingComponentsBorder($('#fetch-parameters-referencing-components'));
                            }

                            applyParametersHandler(currentParameterProviderEntity);
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
                            deferred.resolve();

                            if ($('#parameter-referencing-components-container').is(':visible')) {
                                updateReferencingComponentsBorder($('#parameter-referencing-components-container'));
                            }

                            closeModal('#fetch-parameters-dialog');
                        }
                    }
                }]);
            };

            var cancelled = false;

            // update the button model to show the cancel button
            fetchParametersDialog.modal('setButtonModel', [{
                buttonText: 'Cancel',
                color: {
                    base: '#E3E8EB',
                    hover: '#C7D2D7',
                    text: '#004849'
                },
                handler: {
                    click: function () {
                        cancelled = true;

                        if ($('#fetch-parameters-referencing-components').is(':visible')) {
                            updateReferencingComponentsBorder($('#fetch-parameters-referencing-components'));
                        }

                        updateToCloseButtonModel();
                    }
                }
            }]);

            var requestId;
            var handleAjaxFailure = function (xhr, status, error) {
                // delete the request if possible
                if (nfCommon.isDefinedAndNotNull(requestId)) {
                    deleteUpdateRequest(currentParameterProviderEntity.id, requestId);
                }

                // update the step status
                $('#fetch-parameters-update-steps').find('div.fetch-parameters-step.ajax-loading').removeClass('ajax-loading').addClass('ajax-error');

                // update the button model
                updateToApplyOrCancelButtonModel();
            };

            submitUpdateRequest(currentParameterProviderEntity).done(function (response) {
                var pollUpdateRequest = function (updateRequestEntity) {
                    var updateRequest = updateRequestEntity.request;
                    var errored = nfCommon.isDefinedAndNotNull(updateRequest.failureReason);

                    // get the request id
                    requestId = updateRequest.requestId;

                    // update the progress/steps
                    populateFetchParametersUpdateStep(updateRequest.updateSteps, cancelled, errored);

                    // show update steps
                    $('#fetch-parameters-update-status-container').show();

                    // if this request was cancelled, remove the update request
                    if (cancelled) {
                        deleteUpdateRequest(currentParameterProviderEntity.id, requestId);
                    } else {
                        if (updateRequest.complete === true) {
                            if (errored) {
                                nfDialog.showOkDialog({
                                    headerText: 'Apply Parameter Provider Error',
                                    dialogContent: 'Unable to complete parameter provider update request: ' + nfCommon.escapeHtml(updateRequest.failureReason)
                                });
                            }

                            // reload referencing processors
                            $.each(updateRequest.referencingComponents, function (_, referencingComponentEntity) {
                                if (referencingComponentEntity.permissions.canRead === true) {
                                    var referencingComponent = referencingComponentEntity.component;

                                    // reload the processor if it's in the current group
                                    if (referencingComponent.referenceType === 'PROCESSOR' && nfCanvasUtils.getGroupId() === referencingComponent.processGroupId) {
                                        nfProcessor.reload(referencingComponent.id);
                                    }
                                }
                            });

                            // update the fetch parameter table if displayed
                            if ($('#fetch-parameters-table').is(':visible')) {
                                var parameterProviderGrid = $('#fetch-parameters-table').data('gridInstance');
                                var parameterProviderData = parameterProviderGrid.getData();

                                $.extend(currentParameterProviderEntity, {
                                    revision: updateRequestEntity.parameterContextRevision,
                                    component: updateRequestEntity.request.parameterProvider
                                });

                                var item = parameterProviderData.getItemById(currentParameterProviderEntity.id);
                                if (nfCommon.isDefinedAndNotNull(item)) {
                                    parameterProviderData.updateItem(currentParameterProviderEntity.id, currentParameterProviderEntity);
                                }
                            }

                            // delete the update request
                            deleteUpdateRequest(currentParameterProviderEntity.id, requestId);

                            // update the button model
                            updateToCloseButtonModel();

                            // check if border is necessary
                            updateReferencingComponentsBorder($('#parameter-referencing-components-container'));
                        } else {
                            // wait to get an updated status
                            setTimeout(function () {
                                getUpdateRequest(currentParameterProviderEntity.id, requestId).done(function (getResponse) {
                                    pollUpdateRequest(getResponse);
                                }).fail(handleAjaxFailure);
                            }, 2000);
                        }
                    }
                };

                // update the visibility
                $('#fetch-parameters-usage-container').hide();
                $('#fetch-parameters-update-status').show();

                // // hide the pending apply message for parameter context
                // $('#inherited-parameter-contexts-message').addClass('hidden')

                pollUpdateRequest(response);
            }).fail(handleAjaxFailure);
        }).promise();
    };

    /**
     * Confirms a cancel dialog.
     */
    var confirmCancelDialog = function (dialog) {
        nfDialog.showYesNoDialog({
            headerText: 'Fetch Parameters',
            dialogContent: 'Are you sure you want to cancel?',
            noText: 'Cancel',
            yesText: 'Yes',
            yesHandler: function () {
                closeModal(dialog);
            }
        });
    };

    /**
     * Shows the dialog to fetch parameters.
     *
     * @param {object} parameterProviderEntity parameterProviderEntity
     */
    var showFetchParametersDialog = function (parameterProviderEntity) {
        updateFetchParametersRequest(parameterProviderEntity).done(function (response) {
            var updatedParameterProviderEntity = response;

            groupCount = 0;
            parameterCount = 0;

            // populate the fetch parameters dialog
            $('#fetch-parameters-id').text(updatedParameterProviderEntity.id);
            $('#fetch-parameters-name').text(nfCommon.getComponentName(updatedParameterProviderEntity));

            // get the reference container
            var referencingComponentsContainer = $('#fetch-parameter-referencing-components');

            // load the controller referencing components list
            // loadReferencingProcessGroups(referencingComponentsContainer, parameterProviderEntity);

            // load the referencing parameter contexts list
            loadReferencingParameterContexts(referencingComponentsContainer, updatedParameterProviderEntity);

            loadParameterGroups(updatedParameterProviderEntity, false);

            // show the parameters listing
            $('#fetch-parameters-usage-container').show();

            // build the button model
            var buttons = [{
                buttonText: 'Apply',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        applyParametersHandler(updatedParameterProviderEntity)
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
                        confirmCancelDialog('#fetch-parameters-dialog');
                    }
                }
            }];

            // show the dialog
            $('#fetch-parameters-dialog').modal('setButtonModel', buttons).modal('show');

            // load the bulletins
            nfCanvasUtils.queryBulletins([updatedParameterProviderEntity.id]).done(function (response) {
                updateBulletins(response.bulletinBoard.bulletins, $('#fetch-parameters-bulletins'));
            });

            // update the border if necessary
            updateReferencingComponentsBorder(referencingComponentsContainer);
        })
    };

    /**
     * Loads the specified fetched groups.
     *
     * @param {object} parameterProviderGroupEntity
     // * @param {string} groupToSelect to select
     * @param {boolean} if the parameters should be displayed in a read-only state regardless of permissions
     */
    var loadParameterGroups = function (parameterProviderGroupEntity, readOnly) {
        var groupToSelect = null;
        // providedGroups will be an array of groups
        if (nfCommon.isDefinedAndNotNull(parameterProviderGroupEntity)) {

            var groupsGrid = $('#parameter-groups-table').data('gridInstance');
            var groupsData = groupsGrid.getData();

            // begin the update
            groupsData.beginUpdate();

            var parameterGroups = [];
            $.each(parameterProviderGroupEntity.component.parameterGroupConfigurations, function (i, parameterGroupEntity) {

                var group = {
                    id: groupCount++,
                    hidden: false,
                    type: 'Provided Parameter Group',
                    isNew: false,
                    isModified: false,
                    hasValueChanged: false,
                    name: parameterGroupEntity.groupName,
                    parameterContextName: parameterGroupEntity.parameterContextName,
                    parameterSensitivities: parameterGroupEntity.parameterSensitivities,
                    affectedComponents: parameterGroupEntity.affectedComponents ? parameterGroupEntity.affectedComponents : null,
                    referencingParameterContexts: parameterGroupEntity.referencingParameterContexts ? parameterGroupEntity.referencingParameterContexts : null,
                    isParameterContext: parameterGroupEntity.referencingParameterContexts ? true : false,
                    provided: true
                };

                parameterGroups.push({
                    group: group
                });

                groupsData.addItem(group);
            });

            // complete the update
            groupsData.endUpdate();
            groupsData.reSort();

            // if we are pre-selecting a specific parameter, get its parameterIndex
            if (nfCommon.isDefinedAndNotNull(groupToSelect)) {
                $.each(parameterGroups, function (i, parameterGroupEntity) {
                    if (parameterGroupEntity.parameter.name === groupToSelect) {
                        groupIndex = groupsData.getRowById(parameterGroupEntity.group.id);
                        return false;
                    }
                });
            } else {
                groupIndex = 0;
            }

            if (parameterGroups.length === 0) {
                // resetUsage();
            } else {
                // select the desired row
                groupsGrid.setSelectedRows([groupIndex]);
            }
        }
    };

    /**
     * Loads the selectable parameters for a specified parameter group.
     *
     * @param {object} parametersEntity
     */
    var loadSelectableParameters = function (parametersEntity) {
        sensitiveParametersArray = [];
        console.log('parametersEntity: ', parametersEntity);
        if (nfCommon.isDefinedAndNotNull(parametersEntity)) {

            var selectableParametersGrid = $('#selectable-parameters-table').data('gridInstance');
            var parametersData = selectableParametersGrid.getData();

            //TODO: FIX! need to clear the grid before populating with new data,
            // but this triggers the onSelectedRowsChanged, which I don't want
            selectableParametersGrid.setSelectedRows([]);
            parametersData.setItems([]);
            console.log('parametersData.getItems(), before update: ', parametersData.getItems());

            // if this is the first time the Fetch Parameters dialog is opened, then load the selectable parameters
                // begin the update
                parametersData.beginUpdate();

                var idx = 0;
                for (var param in parametersEntity) {

                    var parameter = {
                        id: idx++,
                        name: param,
                        sensitivity: parametersEntity[param] ? parametersEntity[param] : SENSITIVE,
                        isNew: parametersEntity[param] === null ? true: false,
                    }
                    // if the parameter is sensitive, then show the parameter checkbox as check-marked
                    if (parameter.sensitivity === SENSITIVE) {
                        sensitiveParametersArray.push(parameter.id);
                    }

                    parametersData.addItem(parameter);
                }

                // complete the update
                parametersData.endUpdate();
                parametersData.reSort();
            // } else {
                // the parameters are still saved in the dialog,
                // and we just need to update the parameter (if there are changes)
                // or
                // update the parameter sensitivity based on selecting/un-selecting the checkbox (onClick)
            // }

            console.log('parametersData.getItems(), after update: ', parametersData.getItems());

            // select the desired row
            if (sensitiveParametersArray.length !== 0) {
                selectableParametersGrid.setSelectedRows(sensitiveParametersArray);
            }
        }
    };

    /**
     * Loads the reference for this parameter provider.
     *
     * @param {jQuery} referencingProcessGroupsContainer
     * @param {object} parameterProvider
     */
    var loadReferencingProcessGroups = function (referencingProcessGroupsContainer, parameterProvider) {
        if (parameterProvider.permissions.canRead === false) {
            referencingProcessGroupsContainer.append('<div class="unset">Unauthorized</div>');
            return;
        }
        var referencingProcessGroups = parameterProvider.component.boundProcessGroups;
        if (nfCommon.isEmpty(referencingProcessGroups)) {
            referencingProcessGroupsContainer.append('<div class="unset">No referencing components.</div>');
            return;
        }

        // toggles the visibility of a container
        var toggle = function (twist, container) {
            if (twist.hasClass('expanded')) {
                twist.removeClass('expanded').addClass('collapsed');
                container.hide();
            } else {
                twist.removeClass('collapsed').addClass('expanded');
                container.show();
            }
        };

        var processGroups = $('<ul class="referencing-component-listing clear"></ul>');
        var unauthorized = $('<ul class="referencing-component-listing clear"></ul>');
        $.each(referencingProcessGroups, function (_, referencingProcessGroupsEntity) {
            // check the access policy for this referencing component
            if (referencingProcessGroupsEntity.permissions.canRead === false) {
                var unauthorizedReferencingComponent = $('<div class="unset"></div>').text(referencingProcessGroupsEntity.id);
                unauthorized.append(unauthorizedReferencingComponent);
            } else {
                var referencingComponent = referencingProcessGroupsEntity.component;

                var processGroupLink = $('<span class="referencing-component-name link"></span>').text(referencingComponent.name).on('click', function () {
                    // show the component
                    if (nfCommon.isDefinedAndNotNull(referencingComponent.parentGroupId)) {
                        nfCanvasUtils.showComponent(referencingComponent.parentGroupId, referencingComponent.id);
                    } else {
                        nfProcessGroup.enterGroup(referencingComponent.id);
                    }

                    // close the dialog and shell
                    referencingProcessGroupsContainer.closest('.dialog').modal('hide');
                    $('#shell-close-button').click();
                });
                var processGroupItem = $('<li></li>').append(processGroupLink);
                processGroups.append(processGroupItem);
            }
        });

        // create the collapsable listing for each type
        var createReferenceBlock = function (titleText, list) {
            if (list.is(':empty')) {
                list.remove();
                return;
            }

            var twist = $('<div class="expansion-button expanded"></div>');
            var title = $('<span class="referencing-component-title"></span>').text(titleText);
            var count = $('<span class="referencing-component-count"></span>').text('(' + list.children().length + ')');

            // create the reference block
            $('<div class="referencing-component-block pointer unselectable"></div>').on('click', function () {
                // toggle this block
                toggle(twist, list);

                // update the border if necessary
                updateReferencingComponentsBorder(referencingProcessGroupsContainer);
            }).append(twist).append(title).append(count).appendTo(referencingProcessGroupsContainer);

            // add the listing
            list.appendTo(referencingProcessGroupsContainer);
        };

        // create blocks for each type of component
        createReferenceBlock('Process Groups', processGroups);
        createReferenceBlock('Unauthorized', unauthorized);
    };

    /**
     * Loads the reference parameter contexts for this parameter provider.
     *
     * @param {jQuery} referencingParameterContextsContainer
     * @param {object} referencingComponents
     */

        // TODO: not yet implemented
    var loadReferencingParameterContexts = function (referencingParameterContextsContainer, referencingComponents) {
        if (nfCommon.isEmpty(referencingComponents)) {
            referencingParameterContextsContainer.append('<div class="unset">No referencing components.</div>');
            return;
        }

        // toggles the visibility of a container
        var toggle = function (twist, container) {
            if (twist.hasClass('expanded')) {
                twist.removeClass('expanded').addClass('collapsed');
                container.hide();
            } else {
                twist.removeClass('collapsed').addClass('expanded');
                container.show();
            }
        };

        var parameterContexts = $('<ul class="referencing-component-listing clear"></ul>');
        var unauthorized = $('<ul class="referencing-component-listing clear"></ul>');
        $.each(referencingComponents, function (_, referencingComponent) {
            // // check the access policy for this referencing component
            // if (referencingParameterContextsEntity.permissions.canRead === false) {
            //     var unauthorizedReferencingComponent = $('<div class="unset"></div>').text(referencingParameterContextsEntity.id);
            //     unauthorized.append(unauthorizedReferencingComponent);
            // } else {
            //     var referencingComponent = referencingParameterContextsEntity.component;
            //
            //     var parameterContextLink = $('<span class="referencing-component-name link"></span>').text(referencingComponent.name).on('click', function () {
            //         // show the component
            //         if (nfCommon.isDefinedAndNotNull(referencingComponent.parentGroupId)) {
            //             nfCanvasUtils.showComponent(referencingComponent.parentGroupId, referencingComponent.id);
            //         } else {
            //             nfProcessGroup.enterGroup(referencingComponent.id);
            //         }
            //
            //         // close the dialog and shell
            //         referencingParameterContextsContainer.closest('.dialog').modal('hide');
            //         $('#shell-close-button').click();
            //     });
            //     var parameterContextItem = $('<li></li>').append(parameterContextLink);
            //     parameterContexts.append(parameterContextItem);
            // }

            // var referencingComponent = referencingParameterContextsEntity.component;

            var parameterContextLink = $('<span class="referencing-component-name link"></span>').text(referencingComponent.name).on('click', function () {
                // // show the component
                // if (nfCommon.isDefinedAndNotNull(referencingComponent.parentGroupId)) {
                //     nfCanvasUtils.showComponent(referencingComponent.parentGroupId, referencingComponent.id);
                // } else {
                //     nfProcessGroup.enterGroup(referencingComponent.id);
                // }

                // show the component
                nfParameterContexts.showParameterContext(referencingComponent.id);

                // close the dialog and shell
                referencingParameterContextsContainer.closest('.dialog').modal('hide');
                $('#shell-close-button').click();
            });
            var parameterContextItem = $('<li></li>').append(parameterContextLink);
            parameterContexts.append(parameterContextItem);
        });

        // create the collapsable listing for each type
        var createReferenceBlock = function (titleText, list) {
            if (list.is(':empty')) {
                list.remove();
                return;
            }

            var twist = $('<div class="expansion-button expanded"></div>');
            var title = $('<span class="referencing-component-title"></span>').text(titleText);
            var count = $('<span class="referencing-component-count"></span>').text('(' + list.children().length + ')');

            // create the reference block
            $('<div class="referencing-component-block pointer unselectable"></div>').on('click', function () {
                // toggle this block
                toggle(twist, list);

                // update the border if necessary
                updateReferencingComponentsBorder(referencingParameterContextsContainer);
            }).append(twist).append(title).append(count).appendTo(referencingParameterContextsContainer);

            // add the listing
            list.appendTo(referencingParameterContextsContainer);
        };

        // create blocks for each type of component
        createReferenceBlock('Parameter Contexts', parameterContexts);
        createReferenceBlock('Unauthorized', unauthorized);
    };

    /**
     * Updates the specified bulletinIcon with the specified bulletins if necessary.
     *
     * @param {array} bulletins
     * @param {jQuery} bulletinIcon
     */
    var updateBulletins = function (bulletins, bulletinIcon) {
        var currentBulletins = bulletinIcon.data('bulletins');

        // update the bulletins if necessary
        if (nfCommon.doBulletinsDiffer(currentBulletins, bulletins)) {
            bulletinIcon.data('bulletins', bulletins);

            // format the new bulletins
            var formattedBulletins = nfCommon.getFormattedBulletins(bulletins);

            // if there are bulletins update them
            if (bulletins.length > 0) {
                var list = nfCommon.formatUnorderedList(formattedBulletins);

                // update existing tooltip or initialize a new one if appropriate
                if (bulletinIcon.data('qtip')) {
                    bulletinIcon.qtip('option', 'content.text', list);
                } else {
                    bulletinIcon.addClass('has-bulletins').show().qtip($.extend({},
                        nfCanvasUtils.config.systemTooltipConfig,
                        {
                            content: list
                        }));
                }
            } else if (bulletinIcon.data('qtip')) {
                bulletinIcon.removeClass('has-bulletins').removeData('bulletins').hide().qtip('api').destroy(true);
            }
        }
    };

    /**
     * Adds a border to the fetch parameters referencing components if necessary.
     *
     * @argument {jQuery} referenceContainer
     */
    var updateReferencingComponentsBorder = function (referenceContainer) {
        // determine if it is too big
        var tooBig = referenceContainer.get(0).scrollHeight > Math.round(referenceContainer.innerHeight()) ||
            referenceContainer.get(0).scrollWidth > Math.round(referenceContainer.innerWidth());

        // draw the border if necessary
        if (referenceContainer.is(':visible') && tooBig) {
            referenceContainer.css('border-width', '1px');
        } else {
            referenceContainer.css('border-width', '0px');
        }
    };

    /**
     * Determines if any of the specified referencing components are not authorized to apply parameters.
     *
     * @param {object} parameterProviderEntity having referencingComponents referencing components
     * @returns {boolean}
     */
    var hasUnauthorizedReferencingComponent = function (parameterProviderEntity) {

        if (parameterProviderEntity.permissions.canRead === false
            || (parameterProviderEntity.permissions.canWrite === false && parameterProviderEntity.operatePermissions.canWrite === false)) {
            return true;
        }

        var hasUnauthorized = false;
        var referencingComponents = parameterProviderEntity.component.referencingComponents;
        $.each(referencingComponents, function (_, referencingComponentEntity) {
            if (hasUnauthorizedReferencingComponent(referencingComponentEntity)) {
                hasUnauthorized = true;
                return false;
            }
        });

        return hasUnauthorized;
    };

    /**
     * Used to handle closing a modal dialog
     *
     * * @param {string} dialog the dialog to close
     */
    var closeModal = function (dialog) {
        $(dialog).modal('hide');
    };

    /**
     * Obtains the current state of the updateRequest using the specified update request id.
     *
     * @param {string} updateRequestId update request id
     * @returns {deferred} update request xhr
     */
    var getUpdateRequest = function (parameterProviderId, updateRequestId) {
        return $.ajax({
            type: 'GET',
            url: config.urls.parameterProviders + '/' + encodeURIComponent(parameterProviderId) + '/apply-parameters-requests/' + encodeURIComponent(updateRequestId),
            dataType: 'json'
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Deletes an updateRequest using the specified update request id.
     *
     * @param {string} parameterProviderId parameter provider id
     * @param {string} updateRequestId update request id
     * @returns {deferred} update request xhr
     */
    var deleteUpdateRequest = function (parameterProviderId, updateRequestId) {
        return $.ajax({
            type: 'DELETE',
            url: config.urls.parameterProviders + '/' + encodeURIComponent(parameterProviderId) + '/apply-parameters-requests/' + encodeURIComponent(updateRequestId) + '?' + $.param({
                'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
            }),
            dataType: 'json'
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Submits a parameter provider update request.
     *
     * @param {object} parameterProviderEntity parameter provider id
     * @returns {deferred} update request xhr
     */
    var submitUpdateRequest = function (parameterProviderEntity) {
        var requestEntity = {
            'id': parameterProviderEntity.id,
            'revision': nfClient.getRevision(parameterProviderEntity),
            'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
            'parameterGroupConfigurations': parameterProviderEntity.parameterGroupConfigurations
        }

        return $.ajax({
            type: 'POST',
            data: JSON.stringify(requestEntity),
            url: config.urls.parameterProviders + '/' + encodeURIComponent(requestEntity.id) + '/apply-parameters-requests',
            dataType: 'json',
            contentType: 'application/json'
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Submits a parameter provider update request.
     *
     * @param {object} parameterProviderEntity parameter provider id
     * @returns {deferred} update request xhr
     */
    var updateFetchParametersRequest = function (parameterProviderEntity) {
        return $.ajax({
            type: 'POST',
            data: JSON.stringify(parameterProviderEntity),
            url: config.urls.parameterProviders + '/' + encodeURIComponent(parameterProviderEntity.id) + '/parameters/fetch-requests'+ '?' + $.param({
                'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
            }),
            dataType: 'json',
            contentType: 'application/json'
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Populates the fetch parameters update steps.
     *
     * @param {array} updateSteps steps to update
     * @param {boolean} cancelled whether this request has been cancelled
     * @param {boolean} errored whether this request has errored
     */
    var populateFetchParametersUpdateStep = function (updateSteps, cancelled, errored) {
        var updateStatusContainer = $('#fetch-parameters-update-steps').empty();

        // go through each step
        $.each(updateSteps, function (_, updateStep) {
            var stepItem = $('<li></li>').text(updateStep.description).appendTo(updateStatusContainer);

            $('<div class="fetch-parameters-step"></div>').addClass(function () {
                if (nfCommon.isDefinedAndNotNull(updateStep.failureReason)) {
                    return 'ajax-error';
                } else {
                    if (updateStep.complete === true) {
                        return 'ajax-complete';
                    } else {
                        return cancelled === true || errored === true ? 'ajax-error' : 'ajax-loading';
                    }
                }
            }).appendTo(stepItem);

            $('<div class="clear"></div>').appendTo(stepItem);
        });
    };

    /**
     * Populates the parameters column for the specified parameter group.
     *
     * @param {object} fetchedGroup the parameter group
     */

    var populateFetchParametersColumn = function (fetchedGroup) {
        var updatedGroup = fetchedGroup;
        var isParameterContext = updatedGroup.isParameterContext;

        // get an instance of the grid
        var groupsGrid = $('#parameter-groups-table').data('gridInstance');
        var groupsData = groupsGrid.getData();

        // show the appropriate parameters when dialog first opens
        if (isParameterContext) {
            // get the active Group's parameters to populate the selectable parameters container
            loadSelectableParameters(updatedGroup.parameterSensitivities);

            $('#selectable-parameters-container').show();
            $('#fetched-parameters-container').hide();
        } else {
            $('#selectable-parameters-container').hide();
            $('#fetched-parameters-container').show();
        }

        /* Form the create parameter context checkbox */
        var parametersCheckboxContainer = $('#create-parameter-context-checkbox-container').empty();

        // checkbox for create parameter context of the group
        var checkboxMarkup = $('<div class="setting"></div>');

        if (isParameterContext) {
            // check the checkbox
            $('<div id="create-parameter-context-field" class="nf-checkbox checkbox-checked"></div>').appendTo(checkboxMarkup);
        } else {
            // uncheck the checkbox
            $('<div id="create-parameter-context-field" class="nf-checkbox checkbox-unchecked"></div>').appendTo(checkboxMarkup);
        }

        $('<div id="create-parameter-context-label" class="nf-checkbox-label ellipsis" title="create-parameter-context" style="text-overflow: ellipsis;">' +
            'Create Parameter Context</div></div>').appendTo(checkboxMarkup);

        $(checkboxMarkup).appendTo(parametersCheckboxContainer);

        /* Form the parameter context name input */
        var createParamContextContainer;

        if (isParameterContext) {
            // show the name input
            createParamContextContainer = $('<div id="create-parameter-context-container" class="string-check-container setting" style="display:block;"></div>');
        } else {
            // hide the name input
            createParamContextContainer = $('<div id="create-parameter-context-container" class="string-check-container setting" style="display:none;"></div>');
        }
        $('<div class="setting">' +
            '<div class="setting-name">Parameter Context Name</div>' +
            '<div class="setting-field required">' +
            '<input id="create-parameter-context-input" type="text" name="parameter-context-name"/></div>' +
            '</div>').appendTo(createParamContextContainer);

        createParamContextContainer.appendTo(parametersCheckboxContainer);

        // populate the name input
        var contextName = updatedGroup.parameterContextName;
        $('#create-parameter-context-input').val(contextName);

        /* Form the parameters listing */
        var fetchedParametersContainer = $('#fetch-parameters-listing').empty();
        var fetchedParameters = updatedGroup.parameterSensitivities;

        // put all parameter names in array
        var parametersArray = Object.keys(fetchedParameters);

        // create parameters listing
        if (parametersArray.length > 0) {
            $.each(parametersArray, function (i, parameter) {
                var li = $('<li></li>').text(parameter);
                $('<div class="clear"></div>').appendTo(li);
                li.appendTo(fetchedParametersContainer);
            })
        } else {
            // set to none
            $('<div><span class="unset">None</span></div>').appendTo(fetchedParametersContainer);
        }

        // update the border if necessary
        updateReferencingComponentsBorder(fetchedParametersContainer);

        //TODO
        /* Form the selectable parameters table */

        /* Temporarily save any changes */

        // begin updating the group table
        groupsData.beginUpdate();

        // save parameter context name change
        $('#create-parameter-context-input').on('blur', function () {
            // get the input value
            updatedGroup.parameterContextName = $('#create-parameter-context-input').val();

            // update the group item
            groupsData.updateItem(updatedGroup.id, updatedGroup);
        })

        // create parameter checkbox behaviors
        $('#create-parameter-context-field').off().on('change', function (event, args) {
            // if checked then show the name input, hide parameters listing, show selectable parameters table
            if (args.isChecked) {
                updatedGroup.isParameterContext = true;

                loadSelectableParameters(updatedGroup.parameterSensitivities);

                $('#fetched-parameters-container').hide();
                $('#create-parameter-context-container').show();
                $('#selectable-parameters-container').show();
                $('#fetch-parameters-dialog').modal('refreshButtons');
            } else {
                // if unchecked, then hide the input and only show the parameters listing
                updatedGroup.isParameterContext = false;

                $('#create-parameter-context-container').hide();
                $('#selectable-parameters-container').hide();
                $('#fetched-parameters-container').show();
                $('#fetch-parameters-dialog').modal('refreshButtons');
            }

            // update the group item
            groupsData.updateItem(updatedGroup.id, updatedGroup);
        });

        // finish updating the group table
        groupsData.endUpdate();
        groupsData.reSort();
    };

    /**
     * Reset the dialog.
     */
    var resetFetchParametersDialog = function () {
        $('#create-parameter-context-input').val('');

        // clear the groups table
        var groupsGrid = $('#parameter-groups-table').data('gridInstance');
        var groupsData = groupsGrid.getData();
        groupsGrid.setSelectedRows([]);
        groupsData.setItems([]);

        // clear the selectable parameters table
        var selectableParametersGrid = $('#selectable-parameters-table').data('gridInstance');
        var selectableParametersData = selectableParametersGrid.getData();
        selectableParametersGrid.setSelectedRows([]);
        selectableParametersData.setItems([]);

        //TODO: add these back in when they're ready
        // resetUsage();
        // resetInheritance();

        // reset the last selected parameter
        lastSelectedId = null;
        groupIndex = 0;
        sensitiveParametersArray = [];
    };

    var lastSelectedId = null;

    /**
     * Sorts the specified data using the specified sort details.
     *
     * @param {object} sortDetails
     * @param {object} data
     */
    var sortParameters = function (sortDetails, data) {
        // defines a function for sorting
        var comparer = function (a, b) {
            // direct parameters always come above inherited ones
            if (a.isInherited === false && b.isInherited === true) {
                return -1;
            }
            if (a.isInherited === true && b.isInherited === false) {
                return 1;
            }
            if (sortDetails.columnId === 'name') {
                var aString = _.get(a, '[' + sortDetails.columnId + ']', '');
                var bString = _.get(b, '[' + sortDetails.columnId + ']', '');
                return aString === bString ? 0 : aString > bString ? 1 : -1;
            }
        };

        // perform the sort
        data.sort(comparer, sortDetails.sortAsc);
    };

    /**
     * Performs the filtering.
     *
     * @param {object} item     The item subject to filtering
     * @param {object} args     Filter arguments
     * @returns {Boolean}       Whether or not to include the item
     */
    var filter = function (item, args) {
        return item.hidden === false;
    };

    /**
     * Populates the referencing components and parameters for the specified parameter group.
     *
     * @param {object} group    the specified parameter group
     */
    var populateReferencingComponents = function (group) {
        var referencingComponents = group.referencingComponents;

        // toggles the visibility of a container
        var toggle = function (twist, container) {
            if (twist.hasClass('expanded')) {
                twist.removeClass('expanded').addClass('collapsed');
                container.hide();
            } else {
                twist.removeClass('collapsed').addClass('expanded');
                container.show();
            }
        };

        var referencingProcessors = [];
        var referencingControllerServices = [];
        var unauthorizedReferencingComponents = [];

        var spinner = $('#fetch-parameters-usage .referencing-components-loading');

        var loadingDeferred = $.Deferred(function (deferred) {
            spinner.addClass('ajax-loading');
            deferred.resolve();
        });
        loadingDeferred.then(function () {
            resetUsage();
        }).then(function() {
            populateFetchParametersColumn(group);

            var parameterReferencingComponentsContainer = $('#fetch-parameters-referencing-components-container').empty();

            // referencing component will be undefined when a new parameter is added
            if (nfCommon.isUndefined(referencingComponents)) {
                // set to pending
                $('<div class="referencing-component-container"><span class="unset">Pending Apply</span></div>').appendTo(parameterReferencingComponentsContainer);
            } else {
                // bin the referencing components according to their type
                $.each(referencingComponents, function (_, referencingComponentEntity) {
                    if (referencingComponentEntity.permissions.canRead === true && referencingComponentEntity.permissions.canWrite === true) {
                        if (referencingComponentEntity.component.referenceType === 'PROCESSOR') {
                            referencingProcessors.push(referencingComponentEntity);
                        } else {
                            referencingControllerServices.push(referencingComponentEntity);
                        }
                    } else {
                        unauthorizedReferencingComponents.push(referencingComponentEntity);
                    }
                });

                var referencingProcessGroups = {};

                // bin the referencing processors according to their PG
                $.each(referencingProcessors, function (_, referencingProcessorEntity) {
                    if (referencingProcessGroups[referencingProcessorEntity.processGroup.id]) {
                        referencingProcessGroups[referencingProcessorEntity.processGroup.id].referencingProcessors.push(referencingProcessorEntity);
                        referencingProcessGroups[referencingProcessorEntity.processGroup.id].id = referencingProcessorEntity.processGroup.id;
                        referencingProcessGroups[referencingProcessorEntity.processGroup.id].name = referencingProcessorEntity.processGroup.name;
                    } else {
                        referencingProcessGroups[referencingProcessorEntity.processGroup.id] = {
                            referencingProcessors: [],
                            referencingControllerServices: [],
                            unauthorizedReferencingComponents: [],
                            name: referencingProcessorEntity.processGroup.name,
                            id: referencingProcessorEntity.processGroup.id
                        };

                        referencingProcessGroups[referencingProcessorEntity.processGroup.id].referencingProcessors.push(referencingProcessorEntity);
                    }
                });

                // bin the referencing CS according to their PG
                $.each(referencingControllerServices, function (_, referencingControllerServiceEntity) {
                    if (referencingProcessGroups[referencingControllerServiceEntity.processGroup.id]) {
                        referencingProcessGroups[referencingControllerServiceEntity.processGroup.id].referencingControllerServices.push(referencingControllerServiceEntity);
                        referencingProcessGroups[referencingControllerServiceEntity.processGroup.id].id = referencingControllerServiceEntity.processGroup.id;
                        referencingProcessGroups[referencingControllerServiceEntity.processGroup.id].name = referencingControllerServiceEntity.processGroup.name;
                    } else {
                        referencingProcessGroups[referencingControllerServiceEntity.processGroup.id] = {
                            referencingProcessors: [],
                            referencingControllerServices: [],
                            unauthorizedReferencingComponents: [],
                            name: referencingControllerServiceEntity.processGroup.name,
                            id: referencingControllerServiceEntity.processGroup.id
                        };

                        referencingProcessGroups[referencingControllerServiceEntity.processGroup.id].referencingControllerServices.push(referencingControllerServiceEntity);
                    }
                });

                // bin the referencing unauthorized components according to their PG
                $.each(unauthorizedReferencingComponents, function (_, unauthorizedReferencingComponentEntity) {
                    if (referencingProcessGroups[unauthorizedReferencingComponentEntity.processGroup.id]) {
                        referencingProcessGroups[unauthorizedReferencingComponentEntity.processGroup.id].unauthorizedReferencingComponents.push(unauthorizedReferencingComponentEntity);
                        referencingProcessGroups[unauthorizedReferencingComponentEntity.processGroup.id].id = unauthorizedReferencingComponentEntity.processGroup.id;
                        referencingProcessGroups[unauthorizedReferencingComponentEntity.processGroup.id].name = unauthorizedReferencingComponentEntity.processGroup.name;
                    } else {
                        referencingProcessGroups[unauthorizedReferencingComponentEntity.processGroup.id] = {
                            referencingProcessors: [],
                            referencingControllerServices: [],
                            unauthorizedReferencingComponents: [],
                            name: unauthorizedReferencingComponentEntity.processGroup.name,
                            id: unauthorizedReferencingComponentEntity.processGroup.id
                        };

                        referencingProcessGroups[unauthorizedReferencingComponentEntity.processGroup.id].unauthorizedReferencingComponents.push(unauthorizedReferencingComponentEntity);
                    }
                });

                var parameterReferencingComponentsContainer = $('#fetch-parameters-referencing-components-container');
                var groups = $('<ul class="referencing-component-listing clear"></ul>');

                var referencingProcessGroupsArray = [];
                for (var key in referencingProcessGroups) {
                    if (referencingProcessGroups.hasOwnProperty(key)) {
                        referencingProcessGroupsArray.push(referencingProcessGroups[key]);
                    }
                }

                if (nfCommon.isEmpty(referencingProcessGroupsArray)) {
                    // set to none
                    $('<div class="referencing-component-container"><span class="unset">None</span></div>')
                        .appendTo(parameterReferencingComponentsContainer);
                } else {
                    //sort alphabetically
                    var sortedReferencingProcessGroups = referencingProcessGroupsArray.sort(function (a, b) {
                        if (a.name < b.name) {
                            return -1;
                        }
                        if (a.name > b.name) {
                            return 1;
                        }
                        return 0;
                    });
                }
            }
        })
            .always(function () {
                spinner.removeClass('ajax-loading');
            });
        return loadingDeferred.promise();
    };

    /**
     * Initializes the selectable parameters table
     */
    var initSelectableParametersTable = function () {
        var selectableParametersTable = $('#selectable-parameters-table');

        var nameFormatter = function (row, cell, value, columnDef, dataContext) {
            var nameWidthOffset = 30;
            var cellContent = $('<div></div>');

            // format the contents
            var formattedValue = $('<span/>').addClass('table-cell').text(value).appendTo(cellContent);
            if (dataContext.type === 'required') {
                formattedValue.addClass('required');
            }

            // adjust the width accordingly
            formattedValue.width(columnDef.width - nameWidthOffset).ellipsis();

            // return the cell content
            return cellContent.html();
        };

        var options = {
            editable: false,
            enableCellNavigation: true,
            asyncEditorLoading: false,
            autoEdit: false
        };

        var parametersColumn = [];

        // define the column model for the table
        var checkboxSelector = new Slick.CheckboxSelectColumn({
            cssClass: 'slick-cell-checkboxsel'
        });
        parametersColumn.push(checkboxSelector.getColumnDefinition());

        var nameColumnDefinition = {
            id: 'name',
            name: 'Name',
            field: 'name',
            formatter: nameFormatter,
            sortable: true,
            resizable: false,
            rerenderOnResize: true,
            width: 302,
            minWidth: 302,
            maxWidth: 302,
            editor: Slick.Editors.Text
        };

        parametersColumn.push(nameColumnDefinition);

        // initialize the dataview
        var selectableParametersData = new Slick.Data.DataView({
            inlineFilters: false
        });

        // initialize the sort
        sortParameters({
            columnId: 'name',
            sortAsc: true
        }, selectableParametersData);


        // initialize the grid
        var selectableParametersGrid = new Slick.Grid(selectableParametersTable, selectableParametersData, parametersColumn, selectableParametersGridOptions);
        selectableParametersGrid.setSelectionModel(new Slick.RowSelectionModel({ selectActiveRow: false }));
        selectableParametersGrid.registerPlugin(new Slick.AutoTooltips());
        selectableParametersGrid.registerPlugin(checkboxSelector);

        selectableParametersGrid.setSortColumn('name', true);
        selectableParametersGrid.onSort.subscribe(function (e, args) {
            sortParameters({
                columnId: args.sortCol.id,
                sortAsc: args.sortAsc
            }, selectableParametersData);
        });

        selectableParametersGrid.onSelectedRowsChanged.subscribe(function (e, args) {
            var selectableParametersData = $('#selectable-parameters-table')
                .data('gridInstance')
                .getData();

            var selectableParameters = selectableParametersData.getItems();

            var sensitiveParams = args.rows; // row #'s are parameters that are marked as sensitive

            var parametersToUpdate = {};

            // update the sensitivities of selectableParameters with sensitiveParams
            // var sensitiveParamsIdx = 0;
            var i = 0;
            $.each(selectableParameters, function (_, parameter) { // 2
                // if sensitiveParams has idx, then it's sensitive
                // if idx not in sensitiveParams, then it's non-sensitive
                if (i <= sensitiveParams.length) {
                    var id = sensitiveParams[i];
                    if (parameter.id === id) {
                        // mark as sensitive
                        parameter.sensitivity = SENSITIVE;
                        parametersToUpdate[parameter.name] = SENSITIVE;

                        i++;
                    } else {
                        // mark as non-sensitive
                        parameter.sensitivity = NON_SENSITIVE;
                        parametersToUpdate[parameter.name] = NON_SENSITIVE;
                    }
                } else {
                    // mark the remaining as non-sensitive
                    parameter.sensitivity = NON_SENSITIVE;
                    parametersToUpdate[parameter.name] = NON_SENSITIVE;
                }
            })

            // update the group data in the grid
            var groupsGrid = $('#parameter-groups-table').data('gridInstance');
            var groupsData = groupsGrid.getData();

            var groupIndex = groupsGrid.getSelectedRows();
            var groupToUpdate = groupsData.getItem(groupIndex);

            if (nfCommon.isDefinedAndNotNull(groupToUpdate)) {
                groupToUpdate.parameterSensitivities = parametersToUpdate;
            }
        })

        // wire up the dataview to the grid
        selectableParametersData.onRowCountChanged.subscribe(function (e, args) {
            selectableParametersGrid.updateRowCount();
            selectableParametersGrid.render();
        });
        selectableParametersData.onRowsChanged.subscribe(function (e, args) {
            selectableParametersGrid.invalidateRows(args.rows);
            selectableParametersGrid.render();
        });
        selectableParametersData.syncGridSelection(selectableParametersGrid, true);

        // hold onto an instance of the grid
        selectableParametersTable.data('gridInstance', selectableParametersGrid);

        // end
    }

    /**
     * Initializes the parameter group table
     */
    var initParameterGroupTable = function () {
        var parameterGroupsTable = $('#parameter-groups-table');

        var nameFormatter = function (row, cell, value, columnDef, dataContext) {
            var nameWidthOffset = 30;
            var cellContent = $('<div></div>');

            // format the contents
            var formattedValue = $('<span/>').addClass('table-cell').text(value).appendTo(cellContent);
            if (dataContext.type === 'required') {
                formattedValue.addClass('required');
            }

            // if not first time, check if previously saved as a parameter context
            if (dataContext.isParameterContext) {
                $('<div class="fa fa-star" alt="Info" style="float: right;"></div>').appendTo(cellContent);
            }

            // adjust the width accordingly
            formattedValue.width(columnDef.width - nameWidthOffset).ellipsis();

            // return the cell content
            return cellContent.html();
        };

        // define the column model for the table
        var groupsColumn = [
            {
                id: 'name',
                name: 'Name',
                field: 'parameterContextName',
                formatter: nameFormatter,
                sortable: true,
                resizable: true,
                rerenderOnResize: true,
                width: 327,
                minWidth: 327,
                maxWidth: 327
            }
        ];

        // initialize the dataview
        var groupsData = new Slick.Data.DataView({
            inlineFilters: false
        });
        groupsData.setFilterArgs({
            searchString: '',
            property: 'hidden'
        });
        groupsData.setFilter(filter);

        // initialize the sort
        sortParameters({
            columnId: 'name',
            sortAsc: true
        }, groupsData);

        // initialize the grid
        var groupsGrid = new Slick.Grid(parameterGroupsTable, groupsData, groupsColumn, parameterGroupsGridOptions);
        groupsGrid.setSelectionModel(new Slick.RowSelectionModel());
        groupsGrid.registerPlugin(new Slick.AutoTooltips());
        groupsGrid.setSortColumn('name', true);
        groupsGrid.onSort.subscribe(function (e, args) {
            sortParameters({
                columnId: args.sortCol.id,
                sortAsc: args.sortAsc
            }, groupsData);
        });
        groupsGrid.onSelectedRowsChanged.subscribe(function (e, args) {
            // first, save the previously selected group (row)
            // get the grid instance data to save it
            var previousGroupToSave = groupsGrid.getDataItem(args.previousSelectedRows);

            if ($.isArray(args.rows) && args.rows.length === 1) {
                // show the affected referencing components for the selected group
                if (groupsGrid.getDataLength() > 0) {
                    var groupIndex = args.rows[0];
                    var group = groupsGrid.getDataItem(groupIndex);
                    console.log(`GROUP [ ${group.name} ] to populate the parameter listing: `, group);

                    // only populate affected referencing components if this group is different than the last selected
                    if (lastSelectedId === null || lastSelectedId !== group.id) {
                        populateReferencingComponents(group)
                            .then(function () {
                                // update the details for this parameter
                                $('#fetch-parameter-referencing-components').removeClass('unset').attr('title', group.name).text(group.name);

                                updateReferencingComponentsBorder($('#parameter-referencing-components-container'));

                                // update the last selected id
                                lastSelectedId = group.id;
                            });
                    }
                }
            }
        });
        groupsGrid.onBeforeCellEditorDestroy.subscribe(function (e, args) {
            setTimeout(function () {
                groupsGrid.resizeCanvas();
            }, 50);
        });

        // wire up the dataview to the grid
        groupsData.onRowCountChanged.subscribe(function (e, args) {
            groupsGrid.updateRowCount();
            groupsGrid.render();
        });
        groupsData.onRowsChanged.subscribe(function (e, args) {
            groupsGrid.invalidateRows(args.rows);
            groupsGrid.render();
        });
        groupsData.syncGridSelection(groupsGrid, true);

        // hold onto an instance of the grid and create parameter description tooltip
        parameterGroupsTable.data('gridInstance', groupsGrid);

    };
    // end initParameterGroupTable

    var resetUsage = function () {
        // empty the containers
        var processorContainer = $('.parameter-context-referencing-processors');
        nfCommon.cleanUpTooltips(processorContainer, 'div.referencing-component-state');
        nfCommon.cleanUpTooltips(processorContainer, 'div.referencing-component-bulletins');
        processorContainer.empty();

        var controllerServiceContainer = $('.parameter-context-referencing-controller-services');
        nfCommon.cleanUpTooltips(controllerServiceContainer, 'div.referencing-component-state');
        nfCommon.cleanUpTooltips(controllerServiceContainer, 'div.referencing-component-bulletins');
        controllerServiceContainer.empty();

        var unauthorizedComponentsContainer = $('.parameter-context-referencing-unauthorized-components').empty();

        $('#parameter-referencing-components-container').empty();

        // reset the last selected parameter
        lastSelectedId = null;

        // indicate no referencing components
        $('<li class="referencing-component-container"><span class="unset">None</span></li>').appendTo(processorContainer);
        $('<li class="referencing-component-container"><span class="unset">None</span></li>').appendTo(controllerServiceContainer);
        $('<li class="referencing-component-container"><span class="unset">None</span></li>').appendTo(unauthorizedComponentsContainer);

        // update the selection context
        $('#parameter-referencing-components-context').addClass('unset').attr('title', 'None').text('None');

        // check if border is necessary
        updateReferencingComponentsBorder($('#parameter-referencing-components-container'));
    };

    var currentParameterProviderEntity = null;

    var nfParameterProvider = {
        /**
         * Initializes the parameter provider configuration dialog.
         *
         * @param nfSettingsRef   The nfSettings module.
         */
        init: function (nfSettingsRef) {
            nfSettings = nfSettingsRef;

            // initialize the configuration dialog tabs
            $('#parameter-provider-configuration-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                scrollableTabContentStyle: 'scrollable',
                tabs: [{
                    name: 'Settings',
                    tabContentId: 'parameter-provider-standard-settings-tab-content'
                }, {
                    name: 'Properties',
                    tabContentId: 'parameter-provider-properties-tab-content'
                }, {
                    name: 'Comments',
                    tabContentId: 'parameter-provider-comments-tab-content'
                }],
                select: function () {
                    // remove all property detail dialogs
                    nfUniversalCapture.removeAllPropertyDetailDialogs();

                    // update the property table size in case this is the first time its rendered
                    if ($(this).text() === 'Properties') {
                        $('#parameter-provider-properties').propertytable('resetTableSize');
                    }

                    // close all fields currently being edited
                    $('#parameter-provider-properties').propertytable('saveRow');
                }
            });

            // initialize the parameter provider configuration dialog
            $('#parameter-provider-configuration').data('mode', config.edit).modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'Configure Parameter Provider',
                handler: {
                    close: function () {
                        // cancel any active edits
                        $('#parameter-provider-properties').propertytable('cancelEdit');

                        // clear the tables
                        $('#parameter-provider-properties').propertytable('clear');

                        // clear the comments
                        nfCommon.clearField('read-only-parameter-provider-comments');

                        // removed the cached parameter provider details
                        $('#parameter-provider-configuration').removeData('parameterProviderDetails');

                        // clean up an shown verification errors
                        $('#parameter-provider-properties-verification-results').hide();
                        $('#parameter-provider-properties-verification-results-listing').css('border-width', '0').empty();
                        $('#parameter-provider-properties').css('bottom', '0');


                        // clean up an shown verification errors
                        $('#parameter-provider-properties-verification-results').hide();
                        $('#parameter-provider-properties-verification-results-listing').css('border-width', '0').empty();
                        $('#parameter-provider-properties').css('bottom', '0');

                        // clear most recently submitted referenced attributes
                        referencedAttributes = null;
                    },
                    open: function () {
                        nfCommon.toggleScrollable($('#' + this.find('.tab-container').attr('id') + '-content').get(0));
                    }
                }
            });

            // initialize the property table
            $('#parameter-provider-properties').propertytable({
                readOnly: false,
                supportsGoTo: true,
                dialogContainer: '#new-parameter-provider-property-container',
                descriptorDeferred: getParameterProviderPropertyDescriptor,
                controllerServiceCreatedDeferred: function (response) {
                    return nfControllerServices.loadControllerServices(controllerServicesUri, $('#controller-services-table'));
                },
                goToServiceDeferred: goToServiceFromProperty
            });

            // initialize the fetch parameters dialog
            $('#fetch-parameters-dialog').modal({
                headerText: 'Fetch Parameters',
                scrollableContentStyle: 'scrollable',
                handler: {
                    close: function () {
                        // reset visibility
                        $('#fetch-parameters-progress-container').hide();
                        $('#fetch-parameters-progress li.referencing-component').show();
                        $('#fetch-parameters-update-status-container').hide();

                        // clear the dialog
                        $('#fetch-parameters-id').text('');
                        $('#fetch-parameters-name').text('');
                        $('#fetch-parameters-listing').empty();
                        $('#fetch-parameters-update-steps').empty();

                        // bulletins
                        $('#fetch-parameters-bulletins').removeClass('has-bulletins').removeData('bulletins').hide();
                        nfCommon.cleanUpTooltips($('#fetch-parameters-container'), '#fetch-parameters-bulletins');

                        // reset progress
                        $('div.fetch-parameters-referencing-components').removeClass('ajax-loading ajax-complete ajax-error');

                        // referencing components
                        var referencingComponents = $('#fetch-parameters-referencing-components');
                        nfCommon.cleanUpTooltips(referencingComponents, 'div.referencing-component-state');
                        nfCommon.cleanUpTooltips(referencingComponents, 'div.referencing-component-bulletins');
                        referencingComponents.css('border-width', '0').empty();

                        // reset dialog
                        resetFetchParametersDialog();
                    }
                }
            });

            initParameterGroupTable();
            initSelectableParametersTable();
        },

        /**
         * Shows the configuration dialog for the specified parameter provider.
         *
         * @argument {parameterProvider} parameterProviderEntity the parameter provider
         */
        showConfiguration: function (parameterProviderEntity) {
            var parameterProviderDialog = $('#parameter-provider-configuration');

            parameterProviderDialog.find('.dialog-header .dialog-header-text').text('Configure Parameter Provider');
            if (parameterProviderDialog.data('mode') === config.readOnly) {
                // update the visibility
                $('#parameter-provider-configuration .parameter-provider-read-only').hide();
                $('#parameter-provider-configuration .parameter-provider-editable').show();

                // initialize the property table
                $('#parameter-provider-properties').propertytable('destroy').propertytable({
                    readOnly: false,
                    supportsGoTo: true,
                    dialogContainer: '#new-parameter-provider-property-container',
                    descriptorDeferred: getParameterProviderPropertyDescriptor,
                    controllerServiceCreatedDeferred: function (response) {
                        return nfControllerServices.loadControllerServices(controllerServicesUri, $('#controller-services-table'));
                    },
                    goToServiceDeferred: goToServiceFromProperty
                });

                // update the mode
                parameterProviderDialog.data('mode', config.edit);
            }

            // reload the provider in case the property descriptors have changed
            var reloadProvider = $.ajax({
                type: 'GET',
                url: parameterProviderEntity.uri,
                dataType: 'json'
            });

            // get the parameter provider history
            var loadHistory = $.ajax({
                type: 'GET',
                url: '../nifi-api/flow/history/components/' + encodeURIComponent(parameterProviderEntity.id),
                dataType: 'json'
            });

            // once everything is loaded, show the dialog
            $.when(reloadProvider, loadHistory).done(function (providerResponse, historyResponse) {
                // get the updated parameter provider
                parameterProviderEntity = providerResponse[0];
                currentParameterProviderEntity = parameterProviderEntity;
                var parameterProvider = parameterProviderEntity.component;

                // get the parameter provider history
                var parameterProviderHistory = historyResponse[0].componentHistory;

                // record the parameter provider details
                $('#parameter-provider-configuration').data('parameterProviderDetails', parameterProviderEntity);

                // populate the parameter provider settings
                nfCommon.populateField('parameter-provider-id', parameterProvider['id']);

                nfCommon.populateField('parameter-provider-type', nfCommon.formatType(parameterProvider));
                $('#parameter-provider-configuration').modal('setSubtitle', nfCommon.formatType(parameterProvider));

                nfCommon.populateField('parameter-provider-bundle', nfCommon.formatBundle(parameterProvider['bundle']));
                $('#parameter-provider-name').val(parameterProvider['name']);
                $('#parameter-provider-comments').val(parameterProvider['comments']);

                var buttons = [{
                    buttonText: 'Apply',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            // close all fields currently being edited
                            $('#parameter-provider-properties').propertytable('saveRow');

                            // save the parameter provider
                            saveParameterProvider(parameterProviderEntity).done(function (response) {
                                // reload the parameter provider
                                nfControllerService.reloadReferencedServices(getControllerServicesTable(), response.component);

                                // close the details panel
                                closeModal('#parameter-provider-configuration');
                            });
                        }
                    }
                },
                    {
                        buttonText: 'Cancel',
                        color: {
                            base: '#E3E8EB',
                            hover: '#C7D2D7',
                            text: '#004849'
                        },
                        handler: {
                            click: function () {
                                closeModal('#parameter-provider-configuration');
                            }
                        }
                    }];

                // determine if we should show the advanced button
                if (nfCommon.isDefinedAndNotNull(parameterProvider.customUiUrl) && parameterProvider.customUiUrl !== '') {
                    buttons.push({
                        buttonText: 'Advanced',
                        clazz: 'fa fa-cog button-icon',
                        color: {
                            base: '#E3E8EB',
                            hover: '#C7D2D7',
                            text: '#004849'
                        },
                        handler: {
                            click: function () {
                                var openCustomUi = function () {
                                    // reset state and close the dialog manually to avoid hiding the faded background
                                    closeModal('#parameter-provider-configuration');

                                    // close the settings dialog since the custom ui is also opened in the shell
                                    $('#shell-close-button').click();

                                    // show the custom ui
                                    nfCustomUi.showCustomUi(parameterProviderEntity, parameterProvider.customUiUrl, true).done(function () {
                                        // once the custom ui is closed, reload the parameter provider
                                        nfParameterProvider.reload(parameterProviderEntity.id).done(function (response) {
                                            nfControllerService.reloadReferencedServices(getControllerServicesTable(), response.parameterProvider);
                                        });

                                        // show the settings
                                        nfSettings.showSettings();
                                    });
                                };

                                // close all fields currently being edited
                                $('#parameter-provider-properties').propertytable('saveRow');

                                // determine if changes have been made
                                if (isSaveRequired()) {
                                    // see if those changes should be saved
                                    nfDialog.showYesNoDialog({
                                        headerText: 'Save',
                                        dialogContent: 'Save changes before opening the advanced configuration?',
                                        noHandler: openCustomUi,
                                        yesHandler: function () {
                                            saveParameterProvider(parameterProviderEntity).done(function () {
                                                // open the custom ui
                                                openCustomUi();
                                            });
                                        }
                                    });
                                } else {
                                    // if there were no changes, simply open the custom ui
                                    openCustomUi();
                                }
                            }
                        }
                    });
                }

                // set the button model
                $('#parameter-provider-configuration').modal('setButtonModel', buttons);

                // load the property table
                $('#parameter-provider-properties')
                    .propertytable('setGroupId', null)
                    .propertytable('loadProperties', parameterProvider.properties, parameterProvider.descriptors, parameterProviderHistory.propertyHistory)
                    .propertytable('setPropertyVerificationCallback', function (proposedProperties) {
                        nfVerify.verify(parameterProvider['id'], parameterProviderEntity['uri'], proposedProperties, referencedAttributes, handleVerificationResults, $('#parameter-provider-properties-verification-results-listing'));
                    });

                // show the details
                $('#parameter-provider-configuration').modal('show');

                $('#parameter-provider-properties').propertytable('resetTableSize');
            }).fail(nfErrorHandler.handleAjaxError);
        },

        /**
         * Shows the parameter provider details in a read only dialog.
         *
         * @param {object} parameterProviderEntity parameter provider id
         */
        showDetails: function (parameterProviderEntity) {
            var parameterProviderDialog = $('#parameter-provider-configuration');

            parameterProviderDialog.find('.dialog-header .dialog-header-text').text('Parameter Provider Details');
            if (parameterProviderDialog.data('mode') === config.edit) {
                // update the visibility
                $('#parameter-provider-configuration .parameter-provider-read-only').show();
                $('#parameter-provider-configuration .parameter-provider-editable').hide();

                // initialize the property table
                $('#parameter-provider-properties').propertytable('destroy').propertytable({
                    supportsGoTo: true,
                    readOnly: true
                });

                // update the mode
                parameterProviderDialog.data('mode', config.readOnly);
            }

            // TODO: is this necessary for parameter providers?
            // reload the provider in case the property descriptors have changed
            var reloadProvider = $.ajax({
                type: 'GET',
                url: parameterProviderEntity.uri,
                dataType: 'json'
            });

            // TODO: is there a parameter provider history?
            // get the parameter provider history
            var loadHistory = $.ajax({
                type: 'GET',
                url: '../nifi-api/flow/history/components/' + encodeURIComponent(parameterProviderEntity.id),
                dataType: 'json'
            });

            // once everything is loaded, show the dialog
            $.when(reloadProvider, loadHistory).done(function (providerResponse, historyResponse) {
                // get the updated parameter provider
                parameterProviderEntity = providerResponse[0];
                var parameterProvider = parameterProviderEntity.component;

                // get the parameter provider history
                var parameterProviderHistory = historyResponse[0].componentHistory;

                // populate the parameter provider settings
                nfCommon.populateField('parameter-provider-id', parameterProvider['id']);
                nfCommon.populateField('parameter-provider-type', nfCommon.substringAfterLast(parameterProvider['type'], '.'));
                nfCommon.populateField('parameter-provider-bundle', nfCommon.formatBundle(parameterProvider['bundle']));
                nfCommon.populateField('read-only-parameter-provider-name', parameterProvider['name']);
                nfCommon.populateField('read-only-parameter-provider-comments', parameterProvider['comments']);

                var buttons = [{
                    buttonText: 'Ok',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            // hide the dialog
                            closeModal('#parameter-provider-configuration');
                        }
                    }
                }];

                // determine if we should show the advanced button
                if (nfCommon.isDefinedAndNotNull(nfCustomUi) && nfCommon.isDefinedAndNotNull(parameterProvider.customUiUrl) && parameterProvider.customUiUrl !== '') {
                    buttons.push({
                        buttonText: 'Advanced',
                        clazz: 'fa fa-cog button-icon',
                        color: {
                            base: '#E3E8EB',
                            hover: '#C7D2D7',
                            text: '#004849'
                        },
                        handler: {
                            click: function () {
                                // reset state and close the dialog manually to avoid hiding the faded background
                                closeModal('#parameter-provider-configuration');

                                // close the settings dialog since the custom ui is also opened in the shell
                                $('#shell-close-button').click();

                                // show the custom ui
                                nfCustomUi.showCustomUi(parameterProviderEntity, parameterProvider.customUiUrl, false).done(function () {
                                    nfSettings.showSettings();
                                });
                            }
                        }
                    });
                }

                // show the dialog
                parameterProviderDialog.modal('setButtonModel', buttons).modal('show');

                // load the property table
                $('#parameter-provider-properties')
                    .propertytable('setGroupId', null)
                    .propertytable('loadProperties', parameterProvider.properties, parameterProvider.descriptors, parameterProviderHistory.propertyHistory);

                // show the details
                parameterProviderDialog.modal('show');

                $('#parameter-provider-properties').propertytable('resetTableSize');
            });
        },

        /**
         * Show the fetch parameters dialog.
         *
         * @param {object} parameterProviderEntity parameter provider id
         */
        showFetchDialog: function (parameterProviderEntity) {
            showFetchParametersDialog(parameterProviderEntity);
        },

        /**
         * Reloads the specified parameter provider.
         *
         * @param {string} id
         */
        reload: function (id) {
            var parameterProviderGrid = $('#parameter-providers-table').data('gridInstance');
            var parameterProviderData = parameterProviderGrid.getData();
            var parameterProviderEntity = parameterProviderData.getItemById(id);

            return $.ajax({
                type: 'GET',
                url: parameterProviderEntity.uri,
                dataType: 'json'
            }).done(function (response) {
                renderParameterProvider(response);
            }).fail(nfErrorHandler.handleAjaxError);
        },

        /**
         * Prompts the user before attempting to delete the specified parameter provider.
         *
         * @param {object} parameterProviderEntity parameter provider id
         */
        promptToDeleteParameterProvider: function (parameterProviderEntity) {
            // prompt for deletion
            nfDialog.showYesNoDialog({
                headerText: 'Delete Parameter Provider',
                dialogContent: 'Delete parameter provider \'' + nfCommon.escapeHtml(parameterProviderEntity.component.name) + '\'?',
                yesHandler: function () {
                    nfParameterProvider.remove(parameterProviderEntity);
                }
            });
        },

        /**
         * Deletes the specified parameter provider.
         *
         * @param {object} parameterProviderEntity parameter provider id
         */
        remove: function (parameterProviderEntity) {
            var revision = nfClient.getRevision(parameterProviderEntity);
            $.ajax({
                type: 'DELETE',
                url: parameterProviderEntity.uri + '?' + $.param({
                    'version': revision.version,
                    'clientId': revision.clientId,
                    'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
                }),
                dataType: 'json'
            }).done(function (response) {
                // remove the provider
                var parameterProviderGrid = $('#parameter-providers-table').data('gridInstance');
                var parameterProviderData = parameterProviderGrid.getData();
                parameterProviderData.deleteItem(parameterProviderEntity.id);
            }).fail(nfErrorHandler.handleAjaxError);
        }
    };

    return nfParameterProvider;
}));
