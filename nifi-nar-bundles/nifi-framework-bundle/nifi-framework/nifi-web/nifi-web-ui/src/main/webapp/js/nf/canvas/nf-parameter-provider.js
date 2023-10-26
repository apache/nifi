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
                'nf.ParameterContexts',
                'nf.ProcessGroupConfiguration',
                'lodash'],
            function ($, Slick, nfErrorHandler, nfCommon, nfCanvasUtils, nfDialog, nfStorage, nfClient, nfControllerService, nfControllerServices, nfUniversalCapture, nfCustomUi, nfVerify, nfProcessor, nfProcessGroup, nfParameterContexts, nfProcessGroupConfiguration, _) {
                return (nf.ParameterProvider = factory($, Slick, nfErrorHandler, nfCommon, nfCanvasUtils, nfDialog, nfStorage, nfClient, nfControllerService, nfControllerServices, nfUniversalCapture, nfCustomUi, nfVerify, nfProcessor, nfProcessGroup, nfParameterContexts, nfProcessGroupConfiguration, _));
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
                require('nf.ParameterContexts'),
                require('nf.ProcessGroupConfiguration'),
                require('lodash')));
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
            root.nf.ParameterContexts,
            root.nf.ProcessGroupConfiguration,
            root._);
    }
}(this, function ($, Slick, nfErrorHandler, nfCommon, nfCanvasUtils, nfDialog, nfStorage, nfClient, nfControllerService, nfControllerServices, nfUniversalCapture, nfCustomUi, nfVerify, nfProcessor, nfProcessGroup, nfParameterContexts, nfProcessGroupConfiguration, _) {
    'use strict';

    var nfSettings;
    var fetchParameterProviderOptions;

    var config = {
        edit: 'edit',
        readOnly: 'read-only',
        urls: {
            parameterProviders: '../nifi-api/parameter-providers',
            api: '../nifi-api'
        }
    };

    // load the controller services
    var controllerServicesUri = config.urls.api + '/flow/controller/controller-services';

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
            if (g.isParameterContext || g.createNewParameterContext) {
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
     * @param {type} sensitive requested sensitive status
     */
    var getParameterProviderPropertyDescriptor = function (propertyName, sensitive) {
        var details = $('#parameter-provider-configuration').data('parameterProviderDetails');
        return $.ajax({
            type: 'GET',
            url: details.uri + '/descriptors',
            data: {
                propertyName: propertyName,
                sensitive: sensitive
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
        currentParameterProviderEntity.component.parameterGroupConfigurations = groups;

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
                            applyParametersHandler(currentParameterProviderEntity).done(function () {
                                // reload the parameter provider
                                nfParameterProvider.reload(parameterProviderEntity.id);
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
                            deferred.resolve();
                            confirmCancelDialog('#fetch-parameters-dialog');
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
                $('#fetch-parameters-update-steps')
                    .find('div.fetch-parameters-step.ajax-loading')
                    .removeClass('ajax-loading')
                    .addClass('ajax-error');

                if ($('#affected-referencing-components-container').is(':visible')) {
                    updateReferencingComponentsBorder($('#affected-referencing-components-container'));
                }

                // update the button model
                updateToApplyOrCancelButtonModel();
            };

            submitUpdateRequest(currentParameterProviderEntity).done(function (response) {
                var pollUpdateRequest = function (updateRequestEntity) {
                    var updateRequest = updateRequestEntity.request;
                    var errored = nfCommon.isDefinedAndNotNull(updateRequest.failureReason);

                    // get the request id
                    requestId = updateRequest.requestId;

                    // update the affected referencing components
                    populateAffectedReferencingComponents(updateRequest.referencingComponents);

                    // update the progress/steps
                    populateFetchParametersUpdateStep(updateRequest.updateSteps, cancelled, errored);

                    // if this request was cancelled, remove the update request
                    if (cancelled) {
                        deleteUpdateRequest(currentParameterProviderEntity.id, requestId);
                    } else {
                        // show update steps
                        $('#fetch-parameters-update-status-container').show();

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
                            if ($('#affected-referencing-components-container').is(':visible')) {
                                updateReferencingComponentsBorder($('#affected-referencing-components-container'));
                            }
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

                // get the parameter provider groups names
                var parameterProviderGroupNames = [];
                $.each(response.request.parameterProvider.parameterGroupConfigurations, function (_, parameterProviderGroup) {
                    parameterProviderGroupNames.push(parameterProviderGroup.groupName);
                });
                $('#apply-groups-list')
                    .removeClass('unset')
                    .attr('title', parameterProviderGroupNames.join(', '))
                    .text(parameterProviderGroupNames.join(', '));

                // update the visibility
                // left column
                $('#fetch-parameters-usage-container').hide();
                $('#apply-groups-container').show();

                // middle column
                $('#parameters-container').hide();
                $('#fetch-parameters-update-status').show();

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
     * @param {object} fetchParameterProviderOptions fetchParameterProviderOptions
     */
    var showFetchParametersDialog = function (parameterProviderEntity, fetchParameterProviderOptions) {
        updateFetchParametersRequest(parameterProviderEntity).done(function (response) {
            var updatedParameterProviderEntity = _.cloneDeep(response);
            currentParameterProviderEntity = response;

            // populate the fetch parameters dialog
            $('#fetch-parameters-id').text(updatedParameterProviderEntity.id);
            $('#fetch-parameters-name').text(nfCommon.getComponentName(updatedParameterProviderEntity));

            // set parameters contexts to be updated to none
            $('<div class="parameter-contexts-to-create"><span class="unset">None</span></div>')
                .appendTo($('#parameter-contexts-to-create-container'));

            // list parameter contexts to update
            var parameterContextsToUpdate = $('#parameter-contexts-to-update-container').empty();

            if (!updatedParameterProviderEntity.component.referencingParameterContexts) {
                $('<div class="parameter-contexts-to-update"><span class="unset">None</span></div>')
                    .appendTo(parameterContextsToUpdate);
            } else {
                // populate contexts to be updated
                var parameterContextNames = [];
                $.each(updatedParameterProviderEntity.component.referencingParameterContexts, function (_, paramContext) {
                    parameterContextNames.push(paramContext.component.name);
                });
                parameterContextNames.sort();
                parameterContextsToUpdate
                    .removeClass('unset')
                    .attr('title', parameterContextNames.join(', '))
                    .text(parameterContextNames.join(', '));
            }

            loadParameterGroups(updatedParameterProviderEntity);

            // keep original group data
            var initialFetchedGroups = getFetchedParameterGroups(response);

            // update visibility
            $('#fetch-parameters-permissions-parameter-contexts-message').addClass('hidden');
            $('#fetch-parameters-permissions-affected-components-message').addClass('hidden');
            $('#fetch-parameters-missing-context-name-message').addClass('hidden');

            $('#fetch-parameters-usage-container').show();
            $('#parameters-container').show();

            // build the button model
            var buttons = [{
                buttonText: 'Apply',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                disabled: function () {
                    return disableApplyButton(updatedParameterProviderEntity, initialFetchedGroups);
                },
                handler: {
                    click: function () {
                        applyParametersHandler(updatedParameterProviderEntity).done(function () {
                            // reload the parameter provider
                            nfParameterProvider.reload(parameterProviderEntity.id);
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
                        if (!disableApplyButton(updatedParameterProviderEntity, initialFetchedGroups)) {
                            return confirmCancelDialog('#fetch-parameters-dialog');
                        }
                        closeModal('#fetch-parameters-dialog');
                    }
                }
            }];

            // synchronize the current component canvas attributes in the status bar
            if (fetchParameterProviderOptions.supportsStatusBar) {
                var formattedBulletins = nfCommon.getFormattedBulletins(updatedParameterProviderEntity.bulletins);
                var unorderedBulletins = nfCommon.formatUnorderedList(formattedBulletins);

                // initialize the canvas synchronization
                if (updatedParameterProviderEntity.bulletins.length !== 0) {
                    $('#fetch-parameters-status-bar').statusbar(
                        'observe',
                        { provider: unorderedBulletins }
                    );
                }
            }

            $('#fetch-parameters-affected-referencing-components-container').hide();
            $('#fetch-parameters-referencing-components-container').show();

            // show the dialog
            $('#fetch-parameters-dialog')
                .modal('setButtonModel', buttons)
                .modal('show');

            if ($('#fetched-parameters-listing-container').is(':visible')) {
                updateReferencingComponentsBorder($('#fetched-parameters-listing-container'));
            }
        });
    };

    /**
     * Determines if the fetch parameters dialog has changes and whether the user has permissions to apply the changes.
     *
     * @param {object} updatedParameterProviderEntity updatedParameterProviderEntity
     * @param {object} initialFetchedGroups initialFetchedGroups
     */
    var disableApplyButton = function (updatedParameterProviderEntity, initialFetchedGroups) {
        var canReadWrite = function (component) {
            return component.permissions.canRead && component.permissions.canWrite;
        }

        // referencing parameter contexts
        if (updatedParameterProviderEntity.component.referencingParameterContexts) {
            var hasReadWriteParameterContextsPermissions = updatedParameterProviderEntity.component.referencingParameterContexts.every(canReadWrite);

            if (hasReadWriteParameterContextsPermissions) {
                // user has read and write permissions on a referencing parameter context
                $('#fetch-parameters-permissions-parameter-contexts-message').addClass('hidden');
            } else {
                // user does not have read and write permissions on a referencing parameter context
                // no need to continue checking
                $('#fetch-parameters-permissions-parameter-contexts-message').removeClass('hidden');
                return true;
            }
        }

        // affected referencing components
        if (updatedParameterProviderEntity.component.affectedComponents) {
            var hasReadWriteAffectedComponentsPermissions = updatedParameterProviderEntity.component.affectedComponents.every(canReadWrite);

            if (hasReadWriteAffectedComponentsPermissions) {
                // user has read and write permissions on an affected component
                $('#fetch-parameters-permissions-affected-components-message').addClass('hidden');

                // user has permissions to all affected component... enable Apply button
            } else {
                // user does not have read and write permissions on an affected component
                // no need to continue checking
                $('#fetch-parameters-permissions-affected-components-message').removeClass('hidden');
                return true;
            }
        }

        var groupsData = $('#parameter-groups-table').data('gridInstance').getData();
        var groups = groupsData.getItems();

        // new parameter contexts
        var parameterContextNames = [];
        $.each(groups, function (i, g) {
            if (g.createNewParameterContext) {
                parameterContextNames.push(g.parameterContextName);
            }
        })

        // if any createNewParameterContext, then the name input cannot be empty
        var isNameBlank = function (name) {
            return nfCommon.isBlank(name);
        }

        if (!_.isEmpty(parameterContextNames)) {
            var isAnyParameterContextNameBlank = parameterContextNames.some(isNameBlank);

            if (isAnyParameterContextNameBlank) {
                // missing a parameter context name
                // no need to continue checking
                $('#fetch-parameters-missing-context-name-message').removeClass('hidden');
                return true;
            } else {
                $('#fetch-parameters-missing-context-name-message').addClass('hidden');
            }
        } else {
            // hide the message if previously shown
            $('#fetch-parameters-missing-context-name-message').addClass('hidden');
        }

        // parameter sensitivities
        for (var k = 0; k < initialFetchedGroups.length; k++) {
            var groupFromDataGrid = groups[k];

            if (nfCommon.isDefinedAndNotNull(groupFromDataGrid)) {
                if (groupFromDataGrid.isParameterContext && nfCommon.isUndefinedOrNull(groupFromDataGrid.createNewParameterContext)) {
                    // form the initially fetched group sensitivities
                    var groupInitiallyFetched = initialFetchedGroups[k];
                    var initialGroupParamSensitivity = {};

                    for (var param in groupInitiallyFetched.parameterSensitivities) {
                        initialGroupParamSensitivity[param] = groupInitiallyFetched.parameterSensitivities[param] ? groupInitiallyFetched.parameterSensitivities[param] : SENSITIVE;
                    }

                    // compare
                    if (!_.isEqual(initialGroupParamSensitivity, groupFromDataGrid.parameterSensitivities)) {
                        // parameter sensitive has changed... do not disable the Apply button
                        return false;
                    }
                }
            }
        }

        // check if a parameter is new, removed, missing but referenced, or has a changed value
        if (updatedParameterProviderEntity.component.parameterStatus) {
            var isStatusChanged = function (parameterStatus) {
                return parameterStatus.status !== 'UNCHANGED';
            }

            var isAnyParameterChanged = updatedParameterProviderEntity.component.parameterStatus.some(isStatusChanged);

            if (isAnyParameterChanged) {
                // a fetched parameter is new, removed, missing but referenced, or has a changed value... do not disable the Apply button
                return false;
            }
        }

        return _.isEmpty(parameterContextNames);
    };

    /**
     * Loads the specified fetched groups.
     *
     * @param {object} parameterProviderGroupEntity
     * @param {boolean} if the parameters should be displayed in a read-only state regardless of permissions
     */
    var loadParameterGroups = function (parameterProviderGroupEntity) {
        // providedGroups will be an array of groups
        if (nfCommon.isDefinedAndNotNull(parameterProviderGroupEntity)) {
            var groupCount = 0;
            var groupsGrid = $('#parameter-groups-table').data('gridInstance');
            var groupsData = groupsGrid.getData();

            // begin the update
            groupsData.beginUpdate();

            var parameterGroups = [];
            $.each(parameterProviderGroupEntity.component.parameterGroupConfigurations, function (i, groupConfig) {
                var referencingParameterContext = parameterProviderGroupEntity.component.referencingParameterContexts
                    ? isReferencingParamContext(parameterProviderGroupEntity.component.referencingParameterContexts, groupConfig.parameterContextName)
                    : false;

                var canWriteParameterContexts;
                if (referencingParameterContext) {
                    canWriteParameterContexts = referencingParameterContext.permissions.canWrite;
                } else {
                    canWriteParameterContexts = true;
                }

                var canWriteAffectedComponents;
                if (parameterProviderGroupEntity.component.affectedComponents) {
                    canWriteAffectedComponents = (parameterProviderGroupEntity.component.affectedComponents).every(function (c) {
                        return c.permissions.canRead && c.permissions.canWrite;
                    });
                } else {
                    canWriteAffectedComponents = true;
                }

                var group = {
                    id: groupCount++,
                    hidden: false,
                    isParameterContext: referencingParameterContext ? true : false,
                    name: groupConfig.groupName,
                    parameterContextName: groupConfig.parameterContextName,
                    parameterSensitivities: groupConfig.parameterSensitivities,
                    referencingParameterContexts: groupConfig.referencingParameterContexts ? groupConfig.referencingParameterContexts : null,
                    enableParametersCheckboxes: canWriteParameterContexts && canWriteAffectedComponents,
                    parameterStatus: parameterProviderGroupEntity.component.parameterStatus
                };

                parameterGroups.push({
                    group: group
                });

                groupsData.addItem(group);
            });

            // complete the update
            groupsData.endUpdate();
            groupsData.reSort();

            // if there is a new parameter, update its sensitivity
            if (!_.isEmpty(parameterProviderGroupEntity.component.parameterStatus)) {
                $.each(parameterProviderGroupEntity.component.parameterStatus, function (i, status) {
                    if (status.status !== 'UNCHANGED') {
                        var group = groupsData.getItems().find(function (group) { return group.parameterContextName === status.parameter.parameter.parameterContext.component.name });

                        if (nfCommon.isDefinedAndNotNull(group)) {
                            loadSelectableParameters(group.parameterSensitivities, group, true);
                        }
                        $('#fetch-parameters-dialog').modal('refreshButtons');
                    }
                })
            }

            // select the first row
            groupsGrid.setSelectedRows([0]);
        }
    };

    /**
     * Determines if the provided group is synced to a parameter context.
     *
     * @param {object} referencingParameterContexts
     * @param {string} parameterContextName
     * @returns {boolean}
     */
    var isReferencingParamContext = function (referencingParameterContexts, parameterContextName) {
        var referencingParamContext = null;
        $.each(referencingParameterContexts, function (i, paramContext) {
            if (paramContext.component.name.includes(parameterContextName)) {
                referencingParamContext = paramContext;
            }
        })

        return referencingParamContext;
    }

    /**
     * Loads the selectable parameters for a specified parameter group.
     *
     * @param {object} parameterSensitivitiesEntity
     * @param {object} updatedGroup
     * @param {boolean} saveToGroup
     */
    var loadSelectableParameters = function (parameterSensitivitiesEntity, updatedGroup, saveToGroup) {
        if (nfCommon.isDefinedAndNotNull(parameterSensitivitiesEntity)) {
            var selectableParametersGrid = $('#selectable-parameters-table').data('gridInstance');
            var parametersData = selectableParametersGrid.getData();

            var groupsData = $('#parameter-groups-table').data('gridInstance').getData();
            var currentGroup = groupsData.getItem([updatedGroup.id]);

            // clear the rows
            selectableParametersGrid.setSelectedRows([]);
            parametersData.setItems([]);

            // begin the update
            parametersData.beginUpdate();

            var isAffectedParameter = function (providerEntity, param) {
                var isAffectedParameter = false;
                // check for affected components
                $.each(providerEntity.component.parameterStatus, function (i, status) {
                    if (status.parameter.parameter.name === param) {
                        if (status.status === 'CHANGED') {
                            isAffectedParameter = true;
                        }
                    }
                });
                return isAffectedParameter;
            }

            var getParameterStatusEntity = function (parameterStatus, param) {
                return nfCommon.isDefinedAndNotNull(parameterStatus.find(function (status) { return status.parameter.parameter.name === param }))
                    ? parameterStatus.find(function (status) { return status.parameter.parameter.name === param })
                    : [];
            }

            var getStatus = function (paramStatus, param) {
                var status = getParameterStatusEntity(paramStatus, param);
                return !_.isEmpty(status) && status.status;
            }

            var isReferencedParameter = function (paramStatus, param) {
                var status = getParameterStatusEntity(paramStatus, param);
                return !_.isEmpty(status) && !_.isEmpty(status.parameter.parameter.referencingComponents);
            }

            var idx = 0;
            var referencingParametersCount = 0;
            var parameterCount = 0;
            for (var param in parameterSensitivitiesEntity) {

                var parameter = {
                    id: idx++,
                    groupId: updatedGroup.id,
                    name: param,
                    sensitivity: parameterSensitivitiesEntity[param] ? parameterSensitivitiesEntity[param] : SENSITIVE,
                    isAffectedParameter: currentParameterProviderEntity.component.affectedComponents ? isAffectedParameter(currentParameterProviderEntity, param) : false,
                    isReferencingParameter: !_.isEmpty(updatedGroup.parameterStatus) ? isReferencedParameter(updatedGroup.parameterStatus, param) : false,
                    parameterStatus: !_.isEmpty(updatedGroup.parameterStatus) ? getParameterStatusEntity(updatedGroup.parameterStatus, param) : [],
                    status: !_.isEmpty(updatedGroup.parameterStatus) ? getStatus(updatedGroup.parameterStatus, param) : null
                }

                parameterCount++;
                if (parameter.isReferencingParameter === true) {
                    referencingParametersCount++;
                }

                parametersData.addItem(parameter);

                // save to its group
                if (saveToGroup) {
                    currentGroup.parameterSensitivities[param] = parameterSensitivitiesEntity[param] ? parameterSensitivitiesEntity[param] : SENSITIVE;
                    groupsData.updateItem(updatedGroup.id, currentGroup);
                }
            }

            // add a parameter if the status has been REMOVED or MISSING_BUT_REFERENCED
            if (!_.isEmpty(updatedGroup.parameterStatus)) {

                $.each(updatedGroup.parameterStatus, function (i, status) {
                    if (currentGroup.name === status.parameter.parameter.parameterContext.component.name &&
                        (status.status === 'REMOVED' || status.status ===  'MISSING_BUT_REFERENCED')) {

                        // add the parameter
                        var parameter = {
                            id: idx++,
                            groupId: updatedGroup.id,
                            name: status.parameter.parameter.name,
                            sensitivity: NON_SENSITIVE,
                            parameterStatus: status,
                            isReferencingParameter: status.status ===  'MISSING_BUT_REFERENCED',
                            status: status.status
                        }

                        parametersData.addItem(parameter);
                        parameterCount++;
                    }
                })
            }

            // complete the update
            parametersData.endUpdate();
            parametersData.reSort();

            // select the the first row
            selectableParametersGrid.setSelectedRows([0]);

            // list the parameters to be created
            loadParameterContextsToCreate();
        }
    };

    /**
     * Populates the affected referencing components for the specified parameter provider.
     *
     * @param {object} referencingComponents
     */
    var populateAffectedReferencingComponents = function (referencingComponents) {
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

        // update visibility
        $('#fetch-parameters-referencing-components-container').hide();
        $('#affected-referencing-components-container').show();
        updateReferencingComponentsBorder($('#affected-referencing-components-container'));

        $('#fetch-parameters-affected-referencing-components-container').show();

        var referencingProcessors = [];
        var referencingControllerServices = [];
        var unauthorizedReferencingComponents = [];

        var spinner = $('#fetch-parameters-affected-referencing-components-container .referencing-components-loading');

        var loadingDeferred = $.Deferred(function (deferred) {
            spinner.addClass('ajax-loading');
            deferred.resolve();
        });
        loadingDeferred.then(function () {
            resetUsage();
        }).then(function() {
            var parameterReferencingComponentsContainer = $('#affected-referencing-components-container').empty();

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

                var parameterReferencingComponentsContainer = $('#affected-referencing-components-container');
                var groups = $('<ul class="referencing-component-listing clear"></ul>');

                var referencingProcessGroupsArray = [];
                for (var key in referencingProcessGroups) {
                    if (referencingProcessGroups.hasOwnProperty(key)) {
                        referencingProcessGroupsArray.push(referencingProcessGroups[key]);
                    }
                }

                if (nfCommon.isEmpty(referencingProcessGroupsArray)) {
                    // set to none
                    $('<div class="referencing-component-container"><span class="unset">None</span></div>').appendTo(parameterReferencingComponentsContainer);
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

                    sortedReferencingProcessGroups.forEach(function (referencingProcessGroup) {
                        // container for this pg's references
                        var referencingPgReferencesContainer = $('<div class="referencing-component-references"></div>');
                        parameterReferencingComponentsContainer.append(referencingPgReferencesContainer);

                        // create the collapsable listing for each PG
                        var createReferenceBlock = function (referencingProcessGroup, list) {
                            var twist = $('<div class="expansion-button collapsed"></div>');
                            var title = $('<span class="referencing-component-title"></span>').text(referencingProcessGroup.name);
                            var count = $('<span class="referencing-component-count"></span>').text('(' + (referencingProcessGroup.referencingProcessors.length + referencingProcessGroup.referencingControllerServices.length + referencingProcessGroup.unauthorizedReferencingComponents.length) + ')');
                            var referencingComponents = $('#affected-referencing-components-template').clone();
                            referencingComponents.removeAttr('id');
                            referencingComponents.removeClass('hidden');

                            // create the reference block
                            var groupTwist = $('<div class="referencing-component-block pointer unselectable"></div>').data('processGroupId', referencingProcessGroup.id).on('click', function () {
                                if (twist.hasClass('collapsed')) {
                                    groupTwist.append(referencingComponents);

                                    var processorContainer = groupTwist.find('.fetch-parameters-referencing-processors');
                                    nfCommon.cleanUpTooltips(processorContainer, 'div.referencing-component-state');
                                    nfCommon.cleanUpTooltips(processorContainer, 'div.referencing-component-bulletins');
                                    processorContainer.empty();

                                    var controllerServiceContainer = groupTwist.find('.fetch-parameters-referencing-controller-services');
                                    nfCommon.cleanUpTooltips(controllerServiceContainer, 'div.referencing-component-state');
                                    nfCommon.cleanUpTooltips(controllerServiceContainer, 'div.referencing-component-bulletins');
                                    controllerServiceContainer.empty();

                                    var unauthorizedComponentsContainer = groupTwist.find('.fetch-parameters-referencing-unauthorized-components').empty();

                                    if (referencingProcessGroups[$(this).data('processGroupId')].referencingProcessors.length === 0) {
                                        $('<li class="referencing-component-container"><span class="unset">None</span></li>').appendTo(processorContainer);
                                    } else {
                                        // sort the referencing processors
                                        referencingProcessGroups[$(this).data('processGroupId')].referencingProcessors.sort(nameComparator);

                                        // render each and register a click handler
                                        $.each(referencingProcessGroups[$(this).data('processGroupId')].referencingProcessors, function (_, referencingProcessorEntity) {
                                            renderReferencingProcessor(referencingProcessorEntity, processorContainer);
                                        });
                                    }

                                    if (referencingProcessGroups[$(this).data('processGroupId')].referencingControllerServices.length === 0) {
                                        $('<li class="referencing-component-container"><span class="unset">None</span></li>').appendTo(controllerServiceContainer);
                                    } else {
                                        // sort the referencing controller services
                                        referencingProcessGroups[$(this).data('processGroupId')].referencingControllerServices.sort(nameComparator);

                                        // render each and register a click handler
                                        $.each(referencingProcessGroups[$(this).data('processGroupId')].referencingControllerServices, function (_, referencingControllerServiceEntity) {
                                            renderReferencingControllerService(referencingControllerServiceEntity, controllerServiceContainer);
                                        });
                                    }

                                    if (referencingProcessGroups[$(this).data('processGroupId')].unauthorizedReferencingComponents.length === 0) {
                                        $('<li class="referencing-component-container"><span class="unset">None</span></li>').appendTo(unauthorizedComponentsContainer);
                                    } else {
                                        // sort the unauthorized referencing components
                                        referencingProcessGroups[$(this).data('processGroupId')].unauthorizedReferencingComponents.sort(function (a, b) {
                                            if (a.permissions.canRead === true && b.permissions.canRead === true) {
                                                // processors before controller services
                                                var sortVal = a.component.referenceType === b.component.referenceType ? 0 : a.component.referenceType > b.component.referenceType ? -1 : 1;

                                                // if a and b are the same type, then sort by name
                                                if (sortVal === 0) {
                                                    sortVal = a.component.name === b.component.name ? 0 : a.component.name > b.component.name ? 1 : -1;
                                                }

                                                return sortVal;
                                            } else {

                                                // if lacking read and write perms on both, sort by id
                                                if (a.permissions.canRead === false && b.permissions.canRead === false) {
                                                    return a.id > b.id ? 1 : -1;
                                                } else {
                                                    // if only one has read perms, then let it come first
                                                    if (a.permissions.canRead === true) {
                                                        return -1;
                                                    } else {
                                                        return 1;
                                                    }
                                                }
                                            }
                                        });

                                        $.each(referencingProcessGroups[$(this).data('processGroupId')].unauthorizedReferencingComponents, function (_, unauthorizedReferencingComponentEntity) {
                                            if (unauthorizedReferencingComponentEntity.permissions.canRead === true) {
                                                if (unauthorizedReferencingComponentEntity.component.referenceType === 'PROCESSOR') {
                                                    renderReferencingProcessor(unauthorizedReferencingComponentEntity, unauthorizedComponentsContainer);
                                                } else {
                                                    renderReferencingControllerService(unauthorizedReferencingComponentEntity, unauthorizedComponentsContainer);
                                                }
                                            } else {
                                                var referencingUnauthorizedComponentContainer = $('<li class="referencing-component-container"></li>').appendTo(unauthorizedComponentsContainer);
                                                $('<span class="fetch-parameters-referencing-component-name link ellipsis"></span>')
                                                    .prop('title', unauthorizedReferencingComponentEntity.id)
                                                    .text(unauthorizedReferencingComponentEntity.id)
                                                    .on('click', function () {
                                                        // close the shell
                                                        $('#shell-dialog').modal('hide');

                                                        // show the component in question
                                                        if (unauthorizedReferencingComponentEntity.referenceType === 'PROCESSOR') {
                                                            nfCanvasUtils.showComponent(unauthorizedReferencingComponentEntity.processGroup.id, unauthorizedReferencingComponentEntity.id);
                                                        } else if (unauthorizedReferencingComponentEntity.referenceType === 'CONTROLLER_SERVICE') {
                                                            nfProcessGroupConfiguration.showConfiguration(unauthorizedReferencingComponentEntity.processGroup.id).done(function () {
                                                                nfProcessGroup.enterGroup(unauthorizedReferencingComponentEntity.processGroup.id);
                                                                nfProcessGroupConfiguration.selectControllerService(unauthorizedReferencingComponentEntity.id);
                                                            });
                                                        }
                                                    })
                                                    .appendTo(referencingUnauthorizedComponentContainer);
                                            }
                                        });
                                    }
                                } else {
                                    groupTwist.find('.affected-referencing-components-template').remove();
                                }

                                // toggle this block
                                toggle(twist, list);

                                // update the border if necessary
                                updateReferencingComponentsBorder($('#affected-referencing-components-container'));
                            }).append(twist).append(title).append(count).appendTo(referencingPgReferencesContainer);

                            // add the listing
                            list.appendTo(referencingPgReferencesContainer);

                            // expand the group twist
                            groupTwist.click();
                        };

                        // create block for this process group
                        createReferenceBlock(referencingProcessGroup, groups);
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
     * Loads the referencing components for this parameter provider.
     *
     * @param {jQuery} parameterProviderReferencingComponentsContainer
     * @param {object} parameterProviderEntity
     */
    var loadParameterProviderReferencingComponents = function (parameterProviderReferencingComponentsContainer, parameterProviderEntity) {
        parameterProviderReferencingComponentsContainer.empty();
        var parameterProviderComponent = parameterProviderEntity;

        if (nfCommon.isEmpty(parameterProviderComponent.referencingParameterContexts)) {
            parameterProviderReferencingComponentsContainer.append('<div class="unset">No referencing components.</div>');
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

        $.each(parameterProviderComponent.referencingParameterContexts, function (_, refParameterContextComponent) {
            // check the access policy for this referencing component
            if (refParameterContextComponent.permissions.canRead === false) {
                var unauthorizedReferencingComponent = $('<div class="unset"></div>').text(refParameterContextComponent.id);
                unauthorized.append(unauthorizedReferencingComponent);
            } else {
                var referencingComponent = refParameterContextComponent.component;

                var parameterContextLink = $('<span class="referencing-component-name link"></span>')
                    .text(referencingComponent.name)
                    .on('click', function () {
                        // show the component
                        nfParameterContexts.showParameterContexts(referencingComponent.id);

                        // close the dialog and shell
                        parameterProviderReferencingComponentsContainer.closest('.dialog').modal('hide');
                        $('#shell-close-button').click();
                    });
                var parameterContextItem = $('<li></li>').append(parameterContextLink);
                parameterContexts.append(parameterContextItem);
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
            $('<div class="referencing-component-block pointer unselectable"></div>')
                .on('click', function () {
                    // toggle this block
                    toggle(twist, list);

                    // update the border if necessary
                    updateReferencingComponentsBorder(parameterProviderReferencingComponentsContainer);
                })
                .append(twist)
                .append(title)
                .append(count)
                .appendTo(parameterProviderReferencingComponentsContainer);

            // add the listing
            list.appendTo(parameterProviderReferencingComponentsContainer);
        };

        // create blocks for each type of component
        createReferenceBlock('Parameter Contexts', parameterContexts);
        createReferenceBlock('Unauthorized', unauthorized);
    };

    /**
     * Loads the groups that will be created as a parameter context.
     *
     */
    var loadParameterContextsToCreate = function () {
        var parameterContextsToCreate = $('#parameter-contexts-to-create-container').empty();
        var parameterContextNames = [];

        var groupsGrid = $('#parameter-groups-table').data('gridInstance');
        var groupsData = groupsGrid.getData();

        $.each(groupsData.getItems(), function (_, group) {
            if (group.createNewParameterContext) {
                parameterContextNames.push(group.parameterContextName);
            }
        });

        if (parameterContextNames.length === 0) {
            $('<div class="parameter-contexts-to-create"><span class="unset">None</span></div>')
                .appendTo(parameterContextsToCreate);
        } else {
            parameterContextNames.sort();
            parameterContextsToCreate
                .removeClass('unset')
                .attr('title', parameterContextNames.join(', '))
                .text(parameterContextNames.join(', '));
        }
    }

    var getFetchedParameterGroups = function (groupEntity) {
        var parameterGroups = [];
        $.each(groupEntity.component.parameterGroupConfigurations, function (i, groupConfig) {
            var referencingParameterContext = groupEntity.component.referencingParameterContexts
                ? isReferencingParamContext(groupEntity.component.referencingParameterContexts, groupConfig.parameterContextName)
                : false;

            var canWriteParameterContexts;
            if (referencingParameterContext) {
                canWriteParameterContexts = referencingParameterContext.permissions.canWrite;
            } else {
                canWriteParameterContexts = true;
            }

            var canWriteAffectedComponents;
            if (groupEntity.component.affectedComponents) {
                canWriteAffectedComponents = (groupEntity.component.affectedComponents).every(function (c) {
                    return c.permissions.canRead && c.permissions.canWrite;
                });
            } else {
                canWriteAffectedComponents = true;
            }

            var index = 0;
            var group = {
                id: index++,
                hidden: false,
                isParameterContext: referencingParameterContext ? true : false,
                name: groupConfig.groupName,
                parameterContextName: groupConfig.parameterContextName,
                parameterSensitivities: groupConfig.parameterSensitivities,
                referencingParameterContexts: groupConfig.referencingParameterContexts ? groupConfig.referencingParameterContexts : null,
                enableParametersCheckboxes: canWriteParameterContexts && canWriteAffectedComponents,
                parameterStatus: groupEntity.component.parameterStatus
            };

            parameterGroups.push(group);
        });
        return parameterGroups;
    }

    /**
     * Handles outstanding changes.
     *
     * @returns {deferred}
     */
    var handleOutstandingChanges = function () {
        return $.Deferred(function (deferred) {
            if ($('#fetch-parameters-update-status').is(':visible')) {
                close();
                deferred.resolve();
            } else {
                var parameterGroups = marshalParameterGroups();

                // if there are no parameters there is nothing to save
                if ($.isEmptyObject(parameterGroups)) {
                    close();
                    deferred.resolve();
                } else {
                    // see if those changes should be saved
                    nfDialog.showYesNoDialog({
                        headerText: 'Fetch Parameters',
                        dialogContent: 'Save changes before leaving parameters configuration?',
                        noHandler: function () {
                            close();
                            deferred.resolve();
                        },
                        yesHandler: function () {
                            applyParametersHandler(currentParameterProviderEntity).done(function () {
                                deferred.resolve();
                            }).fail(function () {
                                deferred.reject();
                            });
                        }
                    });
                }
            }

        }).promise();
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
     * @param {string} parameterProviderId parameter provider id
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
            'parameterGroupConfigurations': parameterProviderEntity.component.parameterGroupConfigurations
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
     * Loads the fetched parameters for the specified parameter group.
     *
     * @param {object} fetchedGroup the parameter group
     */
    var loadFetchedParameters = function (fetchedGroup) {
        var updatedGroup = fetchedGroup;
        var isParameterContext = updatedGroup.isParameterContext;
        var isCreateNewParameterContext = updatedGroup.createNewParameterContext;

        $('#fetched-parameters-listing').empty();

        // get an instance of the grid
        var groupsGrid = $('#parameter-groups-table').data('gridInstance');
        var groupsData = groupsGrid.getData();

        // show the appropriate parameters table when dialog first opens
        if (isParameterContext) {
            // get the active group's parameters to populate the selectable parameters container
            loadSelectableParameters(updatedGroup.parameterSensitivities, updatedGroup, false);

            $('#parameters-container').show();
            $('#selectable-parameters-container').show();
            $('#fetched-parameters-container').hide();
        } else {
            $('#selectable-parameters-container').hide();
            $('#fetched-parameters-container').show();
        }

        if (Object.keys(updatedGroup.parameterSensitivities).length !== 0) {
            $('#create-parameter-context-checkbox-container').show();

            /* Form the create parameter context checkbox */
            var parametersCheckboxContainer = $('#create-parameter-context-checkbox-container').empty();
            var createParamContextContainer;

            /* Form the parameter context name input */
            if (isParameterContext) {
                // only show the name
                createParamContextContainer = $('<div id="create-parameter-context-container" class="string-check-container setting" style="display:block;"></div>');

                $('<div class="setting">' +
                    '<div class="setting-name">Parameter Context Name</div>' +
                    '<div class="setting-field">' +
                    '<div id="referencing-parameter-context-name">' + nfCommon.escapeHtml(fetchedGroup.parameterContextName) + '</div>' +
                    '</div>').appendTo(createParamContextContainer);
            } else {

                var checkboxMarkup = $('<div id="checkbox-container"></div>');

                if (isCreateNewParameterContext) {
                    // select checkbox
                    $('<div id="create-parameter-context-field" class="nf-checkbox checkbox-checked"></div>').appendTo(checkboxMarkup);

                    loadSelectableParameters(updatedGroup.parameterSensitivities, updatedGroup, false);

                    $('#parameters-container').show();
                    $('#selectable-parameters-container').show();
                    $('#fetched-parameters-container').hide();
                } else {
                    // deselect checkbox
                    $('<div id="create-parameter-context-field" class="nf-checkbox checkbox-unchecked"></div>').appendTo(checkboxMarkup);

                    $('#selectable-parameters-container').hide();
                    $('#fetched-parameters-container').show();
                }

                $('<div id="create-parameter-context-label" class="nf-checkbox-label ellipsis" title="create-parameter-context" style="text-overflow: ellipsis;">' +
                    'Create Parameter Context' +
                    '</div></div>').appendTo(checkboxMarkup);

                var settingMarkup = $('<div class="setting"></div>');
                $(checkboxMarkup).appendTo(settingMarkup);
                $(settingMarkup).appendTo(parametersCheckboxContainer);

                // create the input container and set visibility
                if (isCreateNewParameterContext) {
                    createParamContextContainer = $('<div id="create-parameter-context-container" class="string-check-container setting" style="display:block;"></div>');
                } else {
                    createParamContextContainer = $('<div id="create-parameter-context-container" class="string-check-container setting" style="display:none;"></div>');
                }

                // create the input
                $('<div class="setting">' +
                    '<div class="setting-name">Parameter Context Name</div>' +
                    '<div class="setting-field required">' +
                    '<input id="create-parameter-context-input" type="text" name="parameter-context-name"/></div>' +
                    '</div>').appendTo(createParamContextContainer);
            }

            createParamContextContainer.appendTo(parametersCheckboxContainer);

            // populate the name input
            var contextName = updatedGroup.parameterContextName;
            $('#create-parameter-context-input').val(contextName);
        } else {
            $('#create-parameter-context-checkbox-container').hide();
        }

        /* Form the parameters listing */
        var fetchedParametersContainer = $('#fetched-parameters-listing').empty();
        var fetchedParameters = updatedGroup.parameterSensitivities;

        // get parameter names only
        var parametersArray = Object.keys(fetchedParameters);

        // create parameter names listing
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
        updateReferencingComponentsBorder($('#fetched-parameters-listing-container'));

        /* Temporarily save any changes */
        // begin updating the group table
        groupsData.beginUpdate();

        // save parameter context name change
        $('#create-parameter-context-input').on('blur', function () {
            // get the input value
            updatedGroup.parameterContextName = $('#create-parameter-context-input').val();

            // update the group item
            groupsData.updateItem(updatedGroup.id, updatedGroup);

            // update the list of parameters to be created
            loadParameterContextsToCreate();

            $('#fetch-parameters-dialog').modal('refreshButtons');
        })

        // create parameter checkbox behaviors
        $('#create-parameter-context-field').off().on('change', function (event, args) {
            // if checked then show the name input, hide parameters listing, show selectable parameters table
            if (args.isChecked) {
                updatedGroup.createNewParameterContext = true;

                loadSelectableParameters(updatedGroup.parameterSensitivities, updatedGroup, false);

                $('#fetched-parameters-container').hide();
                $('#create-parameter-context-container').show();
                $('#selectable-parameters-container').show();
                $('#fetch-parameters-dialog').modal('refreshButtons');
            } else {
                // if unchecked, then hide the input and only show the parameters listing
                updatedGroup.createNewParameterContext = false;

                loadSelectableParameters(updatedGroup.parameterSensitivities, updatedGroup, false);

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
        $('#fetch-parameters-affected-referencing-components-container').hide();
        $('#fetch-parameters-referencing-components-container').show();
        $('#fetch-parameters-permissions-parameter-contexts-message').addClass('hidden');
        $('#fetch-parameters-permissions-affected-components-message').addClass('hidden');
        $('#create-parameter-context-input').val('');

        var headerCheckbox = $('#selectable-parameters-table .slick-column-name input');
        headerCheckbox.removeAttr('checked');

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

        resetUsage();

        // reset the last selected
        lastSelectedGroupId = null;
        lastSelectedParameterId = null;
    };

    var lastSelectedGroupId = null;
    var lastSelectedParameterId = null;

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
     * Sorts the specified entities based on the name.
     *
     * @param {object} a
     * @param {object} b
     * @returns {number}
     */
    var nameComparator = function (a, b) {
        return a.component.name.localeCompare(b.component.name);
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
     * Renders the bulletins as a tooltip of the bulletinIconElement, shows the icon
     *
     * @param bulletins            bulletins to be rendered
     * @param bulletinIconElement  jQuery element to display as the bulletin icon and source for the tooltip
     */
    var renderBulletins = function (bulletins, bulletinIconElement) {
        // format the new bulletins
        var formattedBulletins = nfCommon.getFormattedBulletins(bulletins);

        var list = nfCommon.formatUnorderedList(formattedBulletins);

        // update existing tooltip or initialize a new one if appropriate
        bulletinIconElement.addClass('has-bulletins').show().qtip($.extend({},
            nfCanvasUtils.config.systemTooltipConfig,
            {
                content: list
            }));
    }

    /**
     * Populates the referencing components for the specified parameter context.
     *
     * @param {object} referencingComponents
     */
    var populateReferencingComponents = function (referencingComponents) {
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

        var spinner = $('#fetch-parameters-referencing-components-container .referencing-components-loading');

        var loadingDeferred = $.Deferred(function (deferred) {
            spinner.addClass('ajax-loading');
            deferred.resolve();
        });
        loadingDeferred.then(function () {
            resetUsage();
        }).then(function() {
            var fetchParameterReferencingComponentsContainer = $('#fetch-parameter-referencing-components-container').empty();

            // referencing component will be undefined when a new parameter is added
            if (_.isEmpty(referencingComponents)) {
                // set to pending
                $('<div class="referencing-component-container"><span class="unset">None</span></div>').appendTo(fetchParameterReferencingComponentsContainer);
            } else {
                // bin the referencing components according to their type
                $.each(referencingComponents.parameter.parameter.referencingComponents, function (_, referencingComponentEntity) {
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

                var fetchParameterReferencingComponentsContainer = $('#fetch-parameter-referencing-components-container');
                var groups = $('<ul class="referencing-component-listing clear"></ul>');

                var referencingProcessGroupsArray = [];
                for (var key in referencingProcessGroups) {
                    if (referencingProcessGroups.hasOwnProperty(key)) {
                        referencingProcessGroupsArray.push(referencingProcessGroups[key]);
                    }
                }

                if (nfCommon.isEmpty(referencingProcessGroupsArray)) {
                    // set to none
                    $('<div class="referencing-component-container"><span class="unset">None</span></div>').appendTo(fetchParameterReferencingComponentsContainer);
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

                    sortedReferencingProcessGroups.forEach(function (referencingProcessGroup) {
                        // container for this pg's references
                        var referencingPgReferencesContainer = $('<div class="referencing-component-references"></div>');
                        fetchParameterReferencingComponentsContainer.append(referencingPgReferencesContainer);

                        // create the collapsable listing for each PG
                        var createReferenceBlock = function (referencingProcessGroup, list) {
                            var twist = $('<div class="expansion-button collapsed"></div>');
                            var title = $('<span class="referencing-component-title"></span>').text(referencingProcessGroup.name);
                            var count = $('<span class="referencing-component-count"></span>').text('(' + (referencingProcessGroup.referencingProcessors.length + referencingProcessGroup.referencingControllerServices.length + referencingProcessGroup.unauthorizedReferencingComponents.length) + ')');
                            var referencingComponents = $('#fetch-parameters-referencing-components-template').clone();
                            referencingComponents.removeAttr('id');
                            referencingComponents.removeClass('hidden');

                            // create the reference block
                            var groupTwist = $('<div class="referencing-component-block pointer unselectable"></div>').data('processGroupId', referencingProcessGroup.id).on('click', function () {
                                if (twist.hasClass('collapsed')) {
                                    groupTwist.append(referencingComponents);

                                    var processorContainer = groupTwist.find('.fetch-parameters-referencing-processors');
                                    nfCommon.cleanUpTooltips(processorContainer, 'div.referencing-component-state');
                                    nfCommon.cleanUpTooltips(processorContainer, 'div.referencing-component-bulletins');
                                    processorContainer.empty();

                                    var controllerServiceContainer = groupTwist.find('.fetch-parameters-referencing-controller-services');
                                    nfCommon.cleanUpTooltips(controllerServiceContainer, 'div.referencing-component-state');
                                    nfCommon.cleanUpTooltips(controllerServiceContainer, 'div.referencing-component-bulletins');
                                    controllerServiceContainer.empty();

                                    var unauthorizedComponentsContainer = groupTwist.find('.fetch-parameters-referencing-unauthorized-components').empty();

                                    if (referencingProcessGroups[$(this).data('processGroupId')].referencingProcessors.length === 0) {
                                        $('<li class="referencing-component-container"><span class="unset">None</span></li>').appendTo(processorContainer);
                                    } else {
                                        // sort the referencing processors
                                        referencingProcessGroups[$(this).data('processGroupId')].referencingProcessors.sort(nameComparator);

                                        // render each and register a click handler
                                        $.each(referencingProcessGroups[$(this).data('processGroupId')].referencingProcessors, function (_, referencingProcessorEntity) {
                                            renderReferencingProcessor(referencingProcessorEntity, processorContainer);
                                        });
                                    }

                                    if (referencingProcessGroups[$(this).data('processGroupId')].referencingControllerServices.length === 0) {
                                        $('<li class="referencing-component-container"><span class="unset">None</span></li>').appendTo(controllerServiceContainer);
                                    } else {
                                        // sort the referencing controller services
                                        referencingProcessGroups[$(this).data('processGroupId')].referencingControllerServices.sort(nameComparator);

                                        // render each and register a click handler
                                        $.each(referencingProcessGroups[$(this).data('processGroupId')].referencingControllerServices, function (_, referencingControllerServiceEntity) {
                                            renderReferencingControllerService(referencingControllerServiceEntity, controllerServiceContainer);
                                        });
                                    }

                                    if (referencingProcessGroups[$(this).data('processGroupId')].unauthorizedReferencingComponents.length === 0) {
                                        $('<li class="referencing-component-container"><span class="unset">None</span></li>').appendTo(unauthorizedComponentsContainer);
                                    } else {
                                        // sort the unauthorized referencing components
                                        referencingProcessGroups[$(this).data('processGroupId')].unauthorizedReferencingComponents.sort(function (a, b) {
                                            if (a.permissions.canRead === true && b.permissions.canRead === true) {
                                                // processors before controller services
                                                var sortVal = a.component.referenceType === b.component.referenceType ? 0 : a.component.referenceType > b.component.referenceType ? -1 : 1;

                                                // if a and b are the same type, then sort by name
                                                if (sortVal === 0) {
                                                    sortVal = a.component.name === b.component.name ? 0 : a.component.name > b.component.name ? 1 : -1;
                                                }

                                                return sortVal;
                                            } else {

                                                // if lacking read and write perms on both, sort by id
                                                if (a.permissions.canRead === false && b.permissions.canRead === false) {
                                                    return a.id > b.id ? 1 : -1;
                                                } else {
                                                    // if only one has read perms, then let it come first
                                                    if (a.permissions.canRead === true) {
                                                        return -1;
                                                    } else {
                                                        return 1;
                                                    }
                                                }
                                            }
                                        });

                                        $.each(referencingProcessGroups[$(this).data('processGroupId')].unauthorizedReferencingComponents, function (_, unauthorizedReferencingComponentEntity) {
                                            if (unauthorizedReferencingComponentEntity.permissions.canRead === true) {
                                                if (unauthorizedReferencingComponentEntity.component.referenceType === 'PROCESSOR') {
                                                    renderReferencingProcessor(unauthorizedReferencingComponentEntity, unauthorizedComponentsContainer);
                                                } else {
                                                    renderReferencingControllerService(unauthorizedReferencingComponentEntity, unauthorizedComponentsContainer);
                                                }
                                            } else {
                                                var referencingUnauthorizedComponentContainer = $('<li class="referencing-component-container"></li>').appendTo(unauthorizedComponentsContainer);
                                                $('<span class="fetch-parameter-referencing-component-name link ellipsis"></span>')
                                                    .prop('title', unauthorizedReferencingComponentEntity.id)
                                                    .text(unauthorizedReferencingComponentEntity.id)
                                                    .on('click', function () {
                                                        // check if there are outstanding changes
                                                        handleOutstandingChanges().done(function () {
                                                            // close the shell
                                                            $('#shell-dialog').modal('hide');

                                                            // show the component in question
                                                            if (unauthorizedReferencingComponentEntity.referenceType === 'PROCESSOR') {
                                                                nfCanvasUtils.showComponent(unauthorizedReferencingComponentEntity.processGroup.id, unauthorizedReferencingComponentEntity.id);
                                                            } else if (unauthorizedReferencingComponentEntity.referenceType === 'CONTROLLER_SERVICE') {
                                                                nfProcessGroupConfiguration.showConfiguration(unauthorizedReferencingComponentEntity.processGroup.id).done(function () {
                                                                    nfProcessGroup.enterGroup(unauthorizedReferencingComponentEntity.processGroup.id);
                                                                    nfProcessGroupConfiguration.selectControllerService(unauthorizedReferencingComponentEntity.id);
                                                                });
                                                            }
                                                        });
                                                    })
                                                    .appendTo(referencingUnauthorizedComponentContainer);
                                            }
                                        });
                                    }
                                } else {
                                    groupTwist.find('.fetch-parameters-referencing-components-template').remove();
                                }

                                // toggle this block
                                toggle(twist, list);

                                // update the border if necessary
                                updateReferencingComponentsBorder($('#fetch-parameter-referencing-components-container'));
                            }).append(twist).append(title).append(count).appendTo(referencingPgReferencesContainer);

                            // add the listing
                            list.appendTo(referencingPgReferencesContainer);

                            // expand the group twist
                            groupTwist.click();
                        };

                        // create block for this process group
                        createReferenceBlock(referencingProcessGroup, groups);
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
     * Renders the specified referencing component.
     *
     * @param {object} referencingProcessorEntity
     * @param {jQuery} container
     */
    var renderReferencingProcessor = function (referencingProcessorEntity, container) {
        var referencingProcessorContainer = $('<li class="referencing-component-container"></li>').appendTo(container);
        var referencingProcessor = referencingProcessorEntity.component;

        // processor state
        $('<div class="referencing-component-state"></div>').addClass(function () {
            if (nfCommon.isDefinedAndNotNull(referencingProcessor.state)) {
                var icon = $(this);

                var state = referencingProcessor.state.toLowerCase();
                if (state === 'stopped' && !nfCommon.isEmpty(referencingProcessor.validationErrors)) {
                    state = 'invalid';

                    // build the validation error listing
                    var list = nfCommon.formatUnorderedList(referencingProcessor.validationErrors);

                    // add tooltip for the warnings
                    icon.qtip($.extend({},
                        nfCanvasUtils.config.systemTooltipConfig,
                        {
                            content: list
                        }));
                }

                return state;
            } else {
                return '';
            }
        }).appendTo(referencingProcessorContainer);


        // processor name
        $('<span class="parameter-provider-referencing-component-name link ellipsis"></span>')
            .prop('title', referencingProcessor.name)
            .text(referencingProcessor.name)
            .on('click', function () {
                // close the shell
                $('#shell-dialog').modal('hide');

                // show the component in question
                nfCanvasUtils.showComponent(referencingProcessorEntity.processGroup.id, referencingProcessor.id);
                $('#fetch-parameters-dialog').modal('hide');
            }).appendTo(referencingProcessorContainer);

        // bulletin
        var bulletinIcon = $('<div class="referencing-component-bulletins"></div>').addClass(referencingProcessor.id + '-referencing-bulletins');
        bulletinIcon.appendTo(referencingProcessorContainer);
        if (!nfCommon.isEmpty(referencingProcessorEntity.bulletins)) {
            renderBulletins(referencingProcessorEntity.bulletins, bulletinIcon);
        }

        // processor active threads
        $('<span class="referencing-component-active-thread-count"></span>').text(function () {
            if (nfCommon.isDefinedAndNotNull(referencingProcessor.activeThreadCount) && referencingProcessor.activeThreadCount > 0) {
                return '(' + referencingProcessor.activeThreadCount + ')';
            } else {
                return '';
            }
        }).appendTo(referencingProcessorContainer);
    };

    /**
     * Renders the specified affect controller service.
     *
     * @param {object} referencingControllerServiceEntity
     * @param {jQuery} container
     */
    var renderReferencingControllerService = function (referencingControllerServiceEntity, container) {
        var referencingControllerServiceContainer = $('<li class="referencing-component-container"></li>').appendTo(container);
        var referencingControllerService = referencingControllerServiceEntity.component;

        // controller service state
        $('<div class="referencing-component-state"></div>').addClass(function () {
            if (nfCommon.isDefinedAndNotNull(referencingControllerService.state)) {
                var icon = $(this);

                var state = referencingControllerService.state === 'ENABLED' ? 'enabled' : 'disabled';
                if (state === 'disabled' && !nfCommon.isEmpty(referencingControllerService.validationErrors)) {
                    state = 'invalid';

                    // build the error listing
                    var list = nfCommon.formatUnorderedList(referencingControllerService.validationErrors);

                    // add tooltip for the warnings
                    icon.qtip($.extend({},
                        nfCanvasUtils.config.systemTooltipConfig,
                        {
                            content: list
                        }));
                }
                return state;
            } else {
                return '';
            }
        }).appendTo(referencingControllerServiceContainer);

        // bulletin
        var bulletinIcon = $('<div class="referencing-component-bulletins"></div>')
            .addClass(referencingControllerService.id + '-referencing-bulletins')
            .appendTo(referencingControllerServiceContainer);
        if (!nfCommon.isEmpty(referencingControllerServiceEntity.bulletins)) {
            renderBulletins(referencingControllerServiceEntity.bulletins, bulletinIcon);
        }

        // controller service name
        $('<span class="parameter-provider-referencing-component-name link ellipsis"></span>')
            .prop('title', referencingControllerService.name)
            .text(referencingControllerService.name)
            .on('click', function () {
                // close the shell
                $('#shell-dialog').modal('hide');

                nfProcessGroup.enterGroup(referencingControllerService.processGroupId);

                // show the component in question
                nfProcessGroupConfiguration.showConfiguration(referencingControllerService.processGroupId)
                    .done(function () {
                        nfProcessGroupConfiguration.selectControllerService(referencingControllerService.id);
                        $('#fetch-parameters-dialog').modal('hide');
                    });
            }).appendTo(referencingControllerServiceContainer);
    };

    /**
     * Initializes the selectable parameters table
     */
    var initSelectableParametersTable = function () {
        var selectableParametersTable = $('#selectable-parameters-table');

        var nameFormatter = function (row, cell, value, columnDef, dataContext) {
            var valueWidthOffset = 10;
            var cellContent = $('<div></div>');

            // format the contents
            var formattedValue = $('<span/>').addClass('table-cell').text(value).appendTo(cellContent);
            if (dataContext.type === 'required') {
                formattedValue.addClass('required');
            }

            if (dataContext.isAffectedParameter || (nfCommon.isDefinedAndNotNull(dataContext.parameterStatus.status) && dataContext.parameterStatus.status !== 'UNCHANGED')) {
                valueWidthOffset += 30;
                var status = nfCommon.escapeHtml(dataContext.parameterStatus.status.toLowerCase());
                $('<div class="fa fa-asterisk ' + status + '" alt="Info" style="float: right;"></div>').appendTo(cellContent);
            }

            if (dataContext.isReferencingParameter) {
                valueWidthOffset += 30;
                $('<div class="fa fa-hashtag" alt="Info" style="float: right;"></div>').appendTo(cellContent);
            }

            // adjust the width accordingly
            formattedValue.width(columnDef.width - valueWidthOffset).ellipsis();

            // return the cell content
            return cellContent.html();
        };

        var checkboxSelectionFormatter = function (row, cell, value, columnDef, dataContext) {
            if (dataContext) {
                if (dataContext.status === 'REMOVED' || dataContext.status === 'MISSING_BUT_REFERENCED') {
                    // disable checkboxes
                    return '<div class="disabled nf-checkbox checkbox-unchecked"></div>';
                }

                if (_.isEmpty(dataContext.parameterStatus) || _.isEmpty(dataContext.parameterStatus.parameter.parameter.referencingComponents)) {
                    if (dataContext.sensitivity === SENSITIVE) {
                        return '<div class="checked-input-enabled nf-checkbox checkbox-checked"></div>';
                    } else {
                        return '<div class="unchecked-input-enabled nf-checkbox checkbox-unchecked"></div>';
                    }
                }

                if (!_.isEmpty(dataContext.parameterStatus.parameter.parameter.referencingComponents)) {
                    // disable checkboxes
                    if (dataContext.sensitivity === SENSITIVE) {
                        return '<div class="disabled nf-checkbox checkbox-checked"></div>';
                    } else {
                        return '<div class="disabled nf-checkbox checkbox-unchecked"></div>';
                    }
                }

                $('#fetch-parameters-dialog').modal('refreshButtons');
            }
            return null;
        };

        var parametersColumn = [];

        // define the column models for the table
        var nameColumnDefinition = {
            id: 'name',
            name: 'Parameter Name',
            field: 'name',
            formatter: nameFormatter,
            sortable: true,
            resizable: false,
            rerenderOnResize: true,
            width: 280,
            minWidth: 280,
            editor: Slick.Editors.Text
        };

        var checkboxColumnDefinition = {
            id: 'selector',
            field: 'sel',
            width: 30,
            resizable: false,
            sortable: false,
            cssClass: 'slick-cell-checkbox slick-cell-checkboxsel',
            hideSelectAllCheckbox: false,
            formatter: checkboxSelectionFormatter
        }

        parametersColumn.push(checkboxColumnDefinition);
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
        selectableParametersGrid.setSelectionModel(new Slick.RowSelectionModel());
        selectableParametersGrid.registerPlugin(new Slick.AutoTooltips());

        selectableParametersGrid.setSortColumn('name', true);
        selectableParametersGrid.onSort.subscribe(function (e, args) {
            // clean up tooltips
            nfCommon.cleanUpTooltips($('#selectable-parameters-table'), 'div.fa-asterisk');
            nfCommon.cleanUpTooltips($('#selectable-parameters-table'), 'div.fa-hashtag');

            sortParameters({
                columnId: args.sortCol.id,
                sortAsc: args.sortAsc
            }, selectableParametersData);
        });

        selectableParametersGrid.onClick.subscribe(function (e, args) {
            var target = $(e.target);
            // get the node at this row
            var item = selectableParametersData.getItem(args.row);
            var saveToGroup = false;

            if (selectableParametersGrid.getColumns()[args.cell].id === 'selector') {
                if (target.hasClass('unchecked-input-enabled')) {
                    saveToGroup = true;
                    // check the box
                    item.sensitivity = SENSITIVE;
                    selectableParametersData.updateItem(item.id, item)
                } else if (target.hasClass('checked-input-enabled')) {
                    saveToGroup = true;
                    // uncheck the box
                    item.sensitivity = NON_SENSITIVE;
                    selectableParametersData.updateItem(item.id, item)
                }

                // save to its group
                if (saveToGroup) {
                    var groupsGrid = $('#parameter-groups-table').data('gridInstance');
                    var groupsData = groupsGrid.getData();
                    var currentGroup = groupsData.getItem([item.groupId]);

                    currentGroup.parameterSensitivities[item.name] = item.sensitivity;
                    groupsData.updateItem(item.groupId, currentGroup);

                    $('#fetch-parameters-dialog').modal('refreshButtons');
                }
            }
        })

        selectableParametersGrid.onSelectedRowsChanged.subscribe(function (e, args) {
            if ($.isArray(args.rows) && args.rows.length === 1) {
                // show the referencing components for the selected parameter
                if (selectableParametersGrid.getDataLength() > 0) {
                    var parameterIndex = args.rows[0];
                    var parameter = selectableParametersGrid.getDataItem(parameterIndex);

                    // only populate referencing components if this parameter is different than the last selected
                    if (lastSelectedParameterId === null || lastSelectedParameterId !== parameter.id) {
                        if ($('#selectable-parameters-table').is(':visible')) {
                            populateReferencingComponents(parameter.parameterStatus)
                                .then(function () {
                                    updateReferencingComponentsBorder($('#fetch-parameter-referencing-components-container'));

                                    // update the last selected id
                                    lastSelectedParameterId = parameter.id;
                                });
                        }
                    }
                }
            }

            $('#fetch-parameters-dialog').modal('refreshButtons');
        })

        // wire up the dataview to the grid
        selectableParametersData.onRowCountChanged.subscribe(function (e, args) {
            var selectableParameters = args.dataView.getItems();

            if (!_.isEmpty(selectableParameters)) {
                var groupsGrid = $('#parameter-groups-table').data('gridInstance');
                var groupsData = groupsGrid.getData();
                var currentGroupId = selectableParameters[0].groupId;
                var currentGroup = groupsData.getItem([currentGroupId]);

                if (!currentGroup.isParameterContext) {
                    // update parameter sensitivities
                    var paramSensitivities = {};
                    $.each(selectableParameters, function (i, param) {
                        paramSensitivities[param.name] = param.sensitivity;
                    })

                    // save to its group
                    currentGroup.parameterSensitivities = paramSensitivities;
                    groupsData.updateItems(currentGroupId, currentGroup);
                }
            }

            selectableParametersGrid.updateRowCount();
            selectableParametersGrid.render();
        });
        selectableParametersData.onRowsChanged.subscribe(function (e, args) {
            selectableParametersGrid.invalidateRows(args.rows);
            selectableParametersGrid.render();
        });
        selectableParametersData.syncGridSelection(selectableParametersGrid, true);

        // hold onto an instance of the grid and create an affected component tooltip
        selectableParametersTable.data('gridInstance', selectableParametersGrid).on('mouseenter', 'div.slick-cell', function (e) {
            var asteriskIconElement =  $(this).find('div.fa-asterisk');

            if (asteriskIconElement.length && !asteriskIconElement.data('qtip')) {
                var asteriskTooltipContent = '';

                if (asteriskIconElement.hasClass('new')) {
                    asteriskTooltipContent = nfCommon.escapeHtml('Newly discovered parameter.');
                } else if (asteriskIconElement.hasClass('changed')) {
                    asteriskTooltipContent = nfCommon.escapeHtml('Value has changed.');
                } else if (asteriskIconElement.hasClass('removed')) {
                    asteriskTooltipContent = nfCommon.escapeHtml('Parameter has been removed from its source. Apply to remove from the synced parameter context.');
                } else if (asteriskIconElement.hasClass('missing_but_referenced')) {
                    asteriskTooltipContent = nfCommon.escapeHtml('Parameter has been removed from its source and is still being referenced in a component. To remove the parameter from the parameter context, first un-reference the parameter, then re-fetch and apply.');
                }

                // initialize tooltip
                asteriskIconElement.qtip($.extend({},
                    nfCommon.config.tooltipConfig,
                    {
                        content: asteriskTooltipContent
                    }));
            }

            var hashtagIconElement =  $(this).find('div.fa-hashtag');
            if (hashtagIconElement.length && !hashtagIconElement.data('qtip')) {
                var hashtagTooltipContent = nfCommon.escapeHtml('This parameter is currently referenced by a property. The sensitivity cannot be changed.');

                // initialize tooltip
                hashtagIconElement.qtip($.extend({},
                    nfCommon.config.tooltipConfig,
                    {
                        content: hashtagTooltipContent
                    }));
            }
        });

        /**
         * Select or deselect parameters that are not disabled.
         *
         * @param selectionType   The type of selection.
         */
        var selectOrDeselectAll = function (selectionType) {
            var sensitivity = selectionType === 'select' ? SENSITIVE : NON_SENSITIVE;

            // get all selectable parameters
            var selectableParams = [];
            $.each(selectableParametersData.getItems(), function (i, param) {
                if (!param.isReferencingParameter) {
                    selectableParams.push(param);
                }
            });

            if (!_.isEmpty(selectableParams)) {
                var groupsGrid = $('#parameter-groups-table').data('gridInstance');
                var groupsData = groupsGrid.getData();
                var currentGroupId = groupsGrid.getSelectedRows();
                var currentGroup = groupsData.getItem(currentGroupId);

                // begin updating
                groupsData.beginUpdate();

                // set sensitivities
                $.each(selectableParams, function (i, param) {
                    var updateParamItem = $.extend(param, { sensitivity: sensitivity });
                    selectableParametersData.updateItem(param.id, updateParamItem);

                    // save to its group
                    currentGroup.parameterSensitivities[param.name] = sensitivity;
                })

                // complete the update
                groupsData.endUpdate();

                $('#fetch-parameters-dialog').modal('refreshButtons');
            }
        }

        $('#select-all-fetched-parameters').on('click', function() {
            selectOrDeselectAll('select');
        });

        $('#deselect-all-fetched-parameters').on('click', function() {
            selectOrDeselectAll('deselect');
        });

        // end
    }

    /**
     * Initializes the parameter groups table
     */
    var initParameterGroupsTable = function () {
        var parameterGroupsTable = $('#parameter-groups-table');

        var nameFormatter = function (row, cell, value, columnDef, dataContext) {
            var nameWidthOffset = 30;
            var cellContent = $('<div></div>');

            // format the contents
            var formattedValue = $('<span/>').addClass('table-cell').text(value).appendTo(cellContent);
            if (dataContext.type === 'required') {
                formattedValue.addClass('required');
            }

            // add icon if group is or will be created as a parameter context
            if (dataContext.isParameterContext || dataContext.createNewParameterContext) {
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
                name: 'Parameter Group Name',
                field: 'name',
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
            // clean up tooltips
            nfCommon.cleanUpTooltips($('#parameter-groups-table'), 'div.fa-star');

            sortParameters({
                columnId: args.sortCol.id,
                sortAsc: args.sortAsc
            }, groupsData);
        });
        groupsGrid.onSelectedRowsChanged.subscribe(function (e, args) {
            // clean up tooltips
            nfCommon.cleanUpTooltips($('#selectable-parameters-table'), 'div.fa-asterisk');
            nfCommon.cleanUpTooltips($('#selectable-parameters-table'), 'div.fa-hashtag');

            if ($.isArray(args.rows) && args.rows.length === 1) {
                if (groupsGrid.getDataLength() > 0) {
                    var groupIndex = args.rows[0];
                    var group = groupsGrid.getDataItem(groupIndex);

                    // only populate fetch parameters column if this group is different than the last selected
                    if (lastSelectedGroupId === null || lastSelectedGroupId !== group.id) {
                        loadFetchedParameters(group);

                        // update the last selected id
                        lastSelectedGroupId = group.id;

                        // populate parameter referencing components
                        var selectableParametersGrid = $('#selectable-parameters-table').data('gridInstance');
                        var selectedRow = selectableParametersGrid.getSelectedRows()[0];
                        var parameter = selectableParametersGrid.getData().getItem(selectedRow);

                        if (parameter && group.isParameterContext) {
                            populateReferencingComponents(parameter.parameterStatus).then(function () {
                                updateReferencingComponentsBorder($('#fetch-parameter-referencing-components-container'));
                            });
                        } else {
                            // show 'None' for referencing components
                            populateReferencingComponents({}).then(function () {
                                updateReferencingComponentsBorder($('#fetch-parameter-referencing-components-container'));
                            });
                        }
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

        // hold onto an instance of the grid and create parameter context tooltip
        parameterGroupsTable.data('gridInstance', groupsGrid).on('mouseenter', 'div.slick-cell', function (e) {
            var starIconElement =  $(this).find('div.fa-star');
            if (starIconElement.length && !starIconElement.data('qtip')) {
                var tooltipContent = nfCommon.escapeHtml('Synced to a parameter context.');

                // initialize tooltip
                starIconElement.qtip($.extend({},
                    nfCommon.config.tooltipConfig,
                    {
                        content: tooltipContent
                    }));
            }
        });
    };
    // end initParameterGroupTable

    var resetUsage = function () {
        // empty the containers
        var fetchParameterRefComponents = $('#fetch-parameter-referencing-components-container .referencing-component-references');
        fetchParameterRefComponents.empty();
        var fetchParameterRefComponentsContainer = $('#fetch-parameter-referencing-components-container');

        var processorContainer = $('.fetch-parameters-referencing-processors');
        nfCommon.cleanUpTooltips(processorContainer, 'div.referencing-component-state');
        nfCommon.cleanUpTooltips(processorContainer, 'div.referencing-component-bulletins');
        processorContainer.empty();

        var controllerServiceContainer = $('.fetch-parameters-referencing-controller-services');
        nfCommon.cleanUpTooltips(controllerServiceContainer, 'div.referencing-component-state');
        nfCommon.cleanUpTooltips(controllerServiceContainer, 'div.referencing-component-bulletins');
        controllerServiceContainer.empty();

        var unauthorizedComponentsContainer = $('.fetch-parameters-referencing-unauthorized-components').empty();

        $('#affected-referencing-components-container').empty();

        // reset the last selected
        lastSelectedGroupId = null;
        lastSelectedParameterId = null;

        // indicate no referencing components
        $('<div class="referencing-component-container"><span class="unset">None</span></div>').appendTo(fetchParameterRefComponentsContainer);
        $('<li class="referencing-component-container"><span class="unset">None</span></li>').appendTo(processorContainer);
        $('<li class="referencing-component-container"><span class="unset">None</span></li>').appendTo(controllerServiceContainer);
        $('<li class="referencing-component-container"><span class="unset">None</span></li>').appendTo(unauthorizedComponentsContainer);
    };

    var currentParameterProviderEntity = null;

    var nfParameterProvider = {
        /**
         * Initializes the parameter provider configuration dialog.
         *
         * @param options   Option settings for the parameter provider.
         */
        init: function (options) {
            nfSettings = options.nfSettings;
            fetchParameterProviderOptions = options.statusBarOptions;


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

            // if the status bar is supported, initialize it.
            if (fetchParameterProviderOptions) {
                $('#fetch-parameters-status-bar').statusbar('provider');
            }

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
                        $('#fetch-parameters-update-status-container').hide();
                        $('#apply-groups-container').hide();

                        // clear the dialog
                        $('#fetch-parameters-id').text('');
                        $('#fetch-parameters-name').text('');
                        $('#fetched-parameters-listing').empty();
                        $('#fetch-parameters-update-steps').empty();
                        $('#parameter-contexts-to-create-container').empty();

                        // clean up tooltips
                        nfCommon.cleanUpTooltips($('#parameter-groups-table'), 'div.fa-star');
                        nfCommon.cleanUpTooltips($('#selectable-parameters-table'), 'div.fa-asterisk');
                        nfCommon.cleanUpTooltips($('#selectable-parameters-table'), 'div.fa-hashtag');

                        // reset progress
                        $('div.parameter-contexts-to-update').removeClass('ajax-loading ajax-complete ajax-error');

                        //stop any synchronization
                        if (fetchParameterProviderOptions) {
                            $('#fetch-parameters-status-bar').statusbar('disconnect');
                        }

                        // reset dialog
                        resetFetchParametersDialog();
                    }
                }
            });

            initParameterGroupsTable();
            initSelectableParametersTable();

            $(window).on('resize', function (e) {
                if ($('#affected-referencing-components-container').is(':visible')) {
                    updateReferencingComponentsBorder($('#affected-referencing-components-container'));
                }

                if ($('#fetch-parameter-referencing-components-container').is(':visible')) {
                    updateReferencingComponentsBorder($('#fetch-parameter-referencing-components-container'));
                }
            });
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

                // load referencing components for the parameter provider
                loadParameterProviderReferencingComponents($('#parameter-provider-referencing-components'), parameterProvider);

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
                var parameterProvider = parameterProviderEntity.component;

                // get the parameter provider history
                var parameterProviderHistory = historyResponse[0].componentHistory;

                // populate the parameter provider settings
                nfCommon.populateField('parameter-provider-id', parameterProvider['id']);
                nfCommon.populateField('parameter-provider-type', nfCommon.substringAfterLast(parameterProvider['type'], '.'));
                nfCommon.populateField('parameter-provider-bundle', nfCommon.formatBundle(parameterProvider['bundle']));
                nfCommon.populateField('read-only-parameter-provider-name', parameterProvider['name']);
                nfCommon.populateField('read-only-parameter-provider-comments', parameterProvider['comments']);

                // load referencing components for the parameter provider
                loadParameterProviderReferencingComponents($('#parameter-provider-referencing-components'), parameterProvider);

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
            showFetchParametersDialog(parameterProviderEntity, fetchParameterProviderOptions);
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
        },

        /**
         * Shows the specified parameter provider.
         */
        showParameterProvider: function (parameterProviderId) {
            // show the settings dialog
            nfSettings.showSettings().done(function () {
                var parameterProviderGrid = $('#parameter-providers-table').data('gridInstance');
                var parameterProviderData = parameterProviderGrid.getData();

                // select the desired provider
                var row = parameterProviderData.getRowById(parameterProviderId);
                parameterProviderGrid.setSelectedRows([row]);
                parameterProviderGrid.scrollRowIntoView(row);

                $('#settings-tabs').find('li:eq(5)').click();

                // adjust the table size
                nfSettings.resetTableSize();
            });
        }
    };

    return nfParameterProvider;
}));
