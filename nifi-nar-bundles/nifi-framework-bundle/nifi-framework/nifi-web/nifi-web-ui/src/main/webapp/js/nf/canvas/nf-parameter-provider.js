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
                'nf.ProcessGroup'],
            function ($, nfErrorHandler, nfCommon, nfCanvasUtils, nfDialog, nfStorage, nfClient, nfControllerService, nfControllerServices, nfUniversalCapture, nfCustomUi, nfVerify, nfProcessor, nfProcessGroup) {
                return (nf.ParameterProvider = factory($, nfErrorHandler, nfCommon, nfCanvasUtils, nfDialog, nfStorage, nfClient, nfControllerService, nfControllerServices, nfUniversalCapture, nfCustomUi, nfVerify, nfProcessor, nfProcessGroup));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ParameterProvider =
            factory(require('jquery'),
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
                require('nf.ProcessGroup')));
    } else {
        nf.ParameterProvider = factory(root.$,
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
            root.nf.ProcessGroup);
    }
}(this, function ($, nfErrorHandler, nfCommon, nfCanvasUtils, nfDialog, nfStorage, nfClient, nfControllerService, nfControllerServices, nfUniversalCapture, nfCustomUi, nfVerify, nfProcessor, nfProcessGroup) {
    'use strict';

    var nfSettings;

    var config = {
        urls: {
            parameterProviders: '../nifi-api/parameter-providers'
        }
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
        var fetchParametersDialog = $('#fetch-parameters-dialog');

        // clean up any tooltips that may have been generated
        nfCommon.cleanUpTooltips($('#parameter-table'), 'div.fa-question-circle, div.fa-info');

        // TODO: can I directly pass the parameters from the parameterProviderEntity, instead of from the dialog?
        // var parameters = marshalParameters();

        // var parameters = parameterProviderEntity.fetchedParameterNameGroups.getParameterNames;
        // var inheritedParameterContexts = marshalInheritedParameterContexts();

        // if (parameters.length === 0) {
        //     // nothing to update
        //     // parameterProviderEntity.component.parameters = [];
        //

        // TODO: is there anything else I need to check here besides name?

        //     // if ($('#fetch-parameters-name').val() === _.get(parameterProviderEntity, 'component.name')) {
        //     //     // && $('#fetch-parameters-description-field').val() === _.get(parameterProviderEntity, 'component.description')
        //     //     // && isInheritedParameterContextEquals(parameterContextEntity, inheritedParameterContexts)) {
        //     //     close();
        //     //
        //     //     return;
        //     // }
        // } else {
        //     parameterProviderEntity.component.parameters = parameters;
        // }

        // TODO: are there inherited parameter contexts for parameter providers?
        // // include the inherited parameter contexts
        // parameterContextEntity.component.inheritedParameterContexts = inheritedParameterContexts;
        //
        // parameterContextEntity.component.name = $('#parameter-context-name').val();
        // parameterContextEntity.component.description = $('#parameter-context-description-field').val();

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

                            applyParametersHandler(parameterProviderEntity);
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
                    deleteUpdateRequest(parameterProviderEntity.id, requestId);
                }

                // update the step status
                $('#fetch-parameters-update-steps').find('div.fetch-parameters-step.ajax-loading').removeClass('ajax-loading').addClass('ajax-error');

                // update the button model
                updateToApplyOrCancelButtonModel();
            };

            submitUpdateRequest(parameterProviderEntity).done(function (response) {
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
                        deleteUpdateRequest(parameterProviderEntity.id, requestId);
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

                                $.extend(parameterProviderEntity, {
                                    revision: updateRequestEntity.parameterContextRevision,
                                    component: updateRequestEntity.request.parameterProvider
                                });

                                var item = parameterProviderData.getItemById(parameterProviderEntity.id);
                                if (nfCommon.isDefinedAndNotNull(item)) {
                                    parameterProviderData.updateItem(parameterProviderEntity.id, parameterProviderEntity);
                                }
                            }

                            // delete the update request
                            deleteUpdateRequest(parameterProviderEntity.id, requestId);

                            // update the button model
                            updateToCloseButtonModel();

                            // check if border is necessary
                            updateReferencingComponentsBorder($('#parameter-referencing-components-container'));
                        } else {
                            // wait to get an updated status
                            setTimeout(function () {
                                getUpdateRequest(parameterProviderEntity.id, requestId).done(function (getResponse) {
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
     * Shows the dialog to fetch parameters.
     *
     * @param {object} parameterProviderEntity parameterProviderEntity
     */
    var showFetchParametersDialog = function (parameterProviderEntity) {
        var updatedParameterProviderEntity = null;

        // populate the fetch parameters dialog
        $('#fetch-parameters-id').text(parameterProviderEntity.id);
        $('#fetch-parameters-name').text(nfCommon.getComponentName(parameterProviderEntity));

        // get the reference container
        var referencingComponentsContainer = $('#fetch-parameters-referencing-components');

        // load the controller referencing components list
        loadReferencingProcessGroups(referencingComponentsContainer, parameterProviderEntity);

        // TODO: confirm do not need
        // loadParameters(parameterContextEntity, parameterToSelect, readOnly || !canWrite);

        updateFetchParametersRequest(parameterProviderEntity).done(function (response) {
            updatedParameterProviderEntity = response;

            populateFetchParametersListing(updatedParameterProviderEntity.component.fetchedParameterNameGroups);

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
                        closeModal('#fetch-parameters-dialog');
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
        return $.ajax({
            type: 'POST',
            data: JSON.stringify(parameterProviderEntity),
            url: config.urls.parameterProviders + '/' + encodeURIComponent(parameterProviderEntity.id) + '/apply-parameters-requests',
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
            url: config.urls.parameterProviders + '/' + encodeURIComponent(parameterProviderEntity.id) + '/parameters/fetch-requests',
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
     * Populates the fetched parameters for the specified parameter provider.
     *
     * @param {array} fetchedParameterNameGroups the parameter name groups
     * @param {boolean} cancelled whether this request has been cancelled
     * @param {boolean} errored whether this request has errored
     */
    var populateFetchParametersListing = function (fetchedParameterNameGroups, cancelled, errored) {
        var fetchedParametersContainer = $('#fetch-parameters-listing').empty();

        var parameterNames = [];

        if (fetchedParameterNameGroups.length !== 0) {
            $.each(fetchedParameterNameGroups, function (_, group) {
                if (group.parameterNames.length !== 0) {
                    $.each(group.parameterNames, function (_, parameterName) {
                        parameterNames.push(parameterName);
                    });
                }
            });

            if (parameterNames.length !== 0) {
                // go through each parameter
                $.each(parameterNames, function (_, parameter) {
                    var listItem = $('<li></li>').text(parameter).appendTo(fetchedParametersContainer);
                    $('<div class="clear"></div>').appendTo(listItem);
                });
            } else {
                // set to none
                $('<div class="referencing-component-container"><span class="unset">None</span></div>').appendTo(fetchedParametersContainer);
            }
        } else {
            // set to none
            $('<div class="referencing-component-container"><span class="unset">None</span></div>').appendTo(fetchedParametersContainer);
        }
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
                    }
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
