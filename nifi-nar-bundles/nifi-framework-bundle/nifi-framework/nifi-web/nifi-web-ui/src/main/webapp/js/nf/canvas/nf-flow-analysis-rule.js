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
                'nf.Dialog',
                'nf.Storage',
                'nf.Client',
                'nf.ControllerService',
                'nf.ControllerServices',
                'nf.UniversalCapture',
                'nf.CustomUi',
                'nf.Verify'],
            function ($, nfErrorHandler, nfCommon, nfDialog, nfStorage, nfClient, nfControllerService, nfControllerServices, nfUniversalCapture, nfCustomUi, nfVerify) {
                return (nf.FlowAnalysisRule = factory($, nfErrorHandler, nfCommon, nfDialog, nfStorage, nfClient, nfControllerService, nfControllerServices, nfUniversalCapture, nfCustomUi, nfVerify));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.FlowAnalysisRule =
            factory(require('jquery'),
                require('nf.ErrorHandler'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.Storage'),
                require('nf.Client'),
                require('nf.ControllerService'),
                require('nf.ControllerServices'),
                require('nf.UniversalCapture'),
                require('nf.CustomUi'),
                require('nf.Verify')));
    } else {
        nf.FlowAnalysisRule = factory(root.$,
            root.nf.ErrorHandler,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.Storage,
            root.nf.Client,
            root.nf.ControllerService,
            root.nf.ControllerServices,
            root.nf.UniversalCapture,
            root.nf.CustomUi,
            root.nf.Verify);
    }
}(this, function ($, nfErrorHandler, nfCommon, nfDialog, nfStorage, nfClient, nfControllerService, nfControllerServices, nfUniversalCapture, nfCustomUi, nfVerify) {
    'use strict';

    var nfSettings;

    var config = {
        edit: 'edit',
        readOnly: 'read-only',
        urls: {
            api: '../nifi-api'
        }
    };

    // the last submitted referenced attributes
    var referencedAttributes = null;

    // load the controller services
    var controllerServicesUri = config.urls.api + '/flow/controller/controller-services';

    /**
     * Gets the controller services table.
     *
     * @returns {*|jQuery|HTMLElement}
     */
    var getControllerServicesTable = function () {
        return $('#controller-services-table');
    };

    /**
     * Determines whether the user has made any changes to the flow analysis rule configuration
     * that needs to be saved.
     */
    var isSaveRequired = function () {
        var entity = $('#flow-analysis-rule-configuration').data('flowAnalysisRuleDetails');

        // determine if any flow analysis rule settings have changed

        if ($('#flow-analysis-rule-name').val() !== entity.component['name']) {
            return true;
        }
        if ($('#flow-analysis-rule-comments').val() !== entity.component['comments']) {
            return true;
        }

        var enforcementPolicy = $('#flow-analysis-rule-enforcement-policy-combo').combo('getSelectedOption').value;
        if (enforcementPolicy !== (entity.component['enforcementPolicy'] + '')) {
            return true;
        }

        // defer to the properties
        return $('#flow-analysis-rule-properties').propertytable('isSaveRequired');
    };

    /**
     * Marshals the data that will be used to update the flow analysis rule's configuration.
     */
    var marshalDetails = function () {
        // properties
        var properties = $('#flow-analysis-rule-properties').propertytable('marshalProperties');

        var enforcementPolicy = $('#flow-analysis-rule-enforcement-policy-combo').combo('getSelectedOption').value;

        // create the flow analysis rule dto
        var flowAnalysisRuleDto = {};
        flowAnalysisRuleDto['id'] = $('#flow-analysis-rule-id').text();
        flowAnalysisRuleDto['name'] = $('#flow-analysis-rule-name').val();
        flowAnalysisRuleDto['comments'] = $('#flow-analysis-rule-comments').val();
        flowAnalysisRuleDto['enforcementPolicy'] = enforcementPolicy;

        // set the properties
        if ($.isEmptyObject(properties) === false) {
            flowAnalysisRuleDto['properties'] = properties;
        }
        flowAnalysisRuleDto['sensitiveDynamicPropertyNames'] = $('#flow-analysis-rule-properties').propertytable('getSensitiveDynamicPropertyNames');

        // create the flow analysis rule entity
        var flowAnalysisRuleEntity = {};
        flowAnalysisRuleEntity['component'] = flowAnalysisRuleDto;

        // return the marshaled details
        return flowAnalysisRuleEntity;
    };

    /**
     * Validates the specified details.
     *
     * @argument {object} details       The details to validate
     */
    var validateDetails = function (details) {
        var errors = [];
        var flowAnalysisRule = details['component'];

        if (errors.length > 0) {
            nfDialog.showOkDialog({
                dialogContent: nfCommon.formatUnorderedList(errors),
                headerText: 'Flow Analysis Rule'
            });
            return false;
        } else {
            return true;
        }
    };

    /**
     * Renders the specified flow analysis rule.
     *
     * @param {object} flowAnalysisRule
     */
    var renderFlowAnalysisRule = function (flowAnalysisRuleEntity) {
        // get the table and update the row accordingly
        var flowAnalysisRuleGrid = $('#flow-analysis-rules-table').data('gridInstance');
        var flowAnalysisRuleData = flowAnalysisRuleGrid.getData();
        var currentFlowAnalysisRule = flowAnalysisRuleData.getItemById(flowAnalysisRuleEntity.id);
        flowAnalysisRuleData.updateItem(flowAnalysisRuleEntity.id, $.extend({
            type: 'FlowAnalysisRule',
            bulletins: currentFlowAnalysisRule.bulletins
        }, flowAnalysisRuleEntity));
    };

    /**
     *
     * @param {object} flowAnalysisRuleEntity
     * @param {boolean} enabled
     */
    var setEnabled = function (flowAnalysisRuleEntity, enabled) {
        var entity = {
            'revision': nfClient.getRevision(flowAnalysisRuleEntity),
            'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
            'state': enabled === true ? 'ENABLED' : 'DISABLED'
        };

        return $.ajax({
            type: 'PUT',
            url: flowAnalysisRuleEntity.uri + '/run-status',
            data: JSON.stringify(entity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (response) {
            // update the task
            renderFlowAnalysisRule(response);
            // component can be null if the user only has 'operate' permission without 'read'.
            if (nfCommon.isDefinedAndNotNull(response.component)) {
                nfControllerService.reloadReferencedServices(getControllerServicesTable(), response.component);
            }
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Goes to a service configuration from the property table.
     */
    var goToServiceFromProperty = function () {
        return $.Deferred(function (deferred) {
            // close all fields currently being edited
            $('#flow-analysis-rule-properties').propertytable('saveRow');

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
                        var flowAnalysisRule = $('#flow-analysis-rule-configuration').data('flowAnalysisRuleDetails');
                        saveFlowAnalysisRule(flowAnalysisRule).done(function () {
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
     * Saves the specified flow analysis rule.
     *
     * @param {type} flowAnalysisRule
     */
    var saveFlowAnalysisRule = function (flowAnalysisRuleEntity) {
        // marshal the settings and properties and update the flow analysis rule
        var updatedFlowAnalysisRule = marshalDetails();

        // ensure details are valid as far as we can tell
        if (validateDetails(updatedFlowAnalysisRule)) {
            updatedFlowAnalysisRule['revision'] = nfClient.getRevision(flowAnalysisRuleEntity);
            updatedFlowAnalysisRule['disconnectedNodeAcknowledged'] = nfStorage.isDisconnectionAcknowledged();

            // update the selected component
            return $.ajax({
                type: 'PUT',
                data: JSON.stringify(updatedFlowAnalysisRule),
                url: flowAnalysisRuleEntity.uri,
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // update the flow analysis rule
                renderFlowAnalysisRule(response);
            }).fail(nfErrorHandler.handleConfigurationUpdateAjaxError);
        } else {
            return $.Deferred(function (deferred) {
                deferred.reject();
            }).promise();
        }
    };

    /**
     * Gets a property descriptor for the flow analysis rule currently being configured.
     *
     * @param {type} propertyName
     * @param {type} sensitive Requested sensitive status
     */
    var getFlowAnalysisRulePropertyDescriptor = function (propertyName, sensitive) {
        var details = $('#flow-analysis-rule-configuration').data('flowAnalysisRuleDetails');
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

        var verificationResultsContainer = $('#flow-analysis-rule-properties-verification-results');

        // expand the dialog to make room for the verification result
        if (verificationResultsContainer.is(':visible') === false) {
            // show the verification results
            $('#flow-analysis-rule-properties').css('bottom', '40%').propertytable('resetTableSize')
            verificationResultsContainer.show();
        }

        // show borders if appropriate
        var verificationResultsListing = $('#flow-analysis-rule-properties-verification-results-listing');
        if (verificationResultsListing.get(0).scrollHeight > Math.round(verificationResultsListing.innerHeight())) {
            verificationResultsListing.css('border-width', '1px');
        }
    };

    var nfFlowAnalysisRule = {
        /**
         * Initializes the flow analysis rule configuration dialog.
         *
         * @param nfSettingsRef   The nfSettings module.
         */
        init: function (nfSettingsRef) {
            nfSettings = nfSettingsRef;

            // initialize the configuration dialog tabs
            $('#flow-analysis-rule-configuration-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                scrollableTabContentStyle: 'scrollable',
                tabs: [{
                    name: 'Settings',
                    tabContentId: 'flow-analysis-rule-standard-settings-tab-content'
                }, {
                    name: 'Properties',
                    tabContentId: 'flow-analysis-rule-properties-tab-content'
                }, {
                    name: 'Comments',
                    tabContentId: 'flow-analysis-rule-comments-tab-content'
                }],
                select: function () {
                    // remove all property detail dialogs
                    nfUniversalCapture.removeAllPropertyDetailDialogs();

                    // update the property table size in case this is the first time its rendered
                    if ($(this).text() === 'Properties') {
                        $('#flow-analysis-rule-properties').propertytable('resetTableSize');
                    }

                    // close all fields currently being edited
                    $('#flow-analysis-rule-properties').propertytable('saveRow');
                }
            });

            // initialize the flow analysis rule configuration dialog
            $('#flow-analysis-rule-configuration').data('mode', config.edit).modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'Configure Flow Analysis Rule',
                handler: {
                    close: function () {
                        // cancel any active edits
                        $('#flow-analysis-rule-properties').propertytable('cancelEdit');

                        // clear the tables
                        $('#flow-analysis-rule-properties').propertytable('clear');

                        // clear the comments
                        nfCommon.clearField('read-only-flow-analysis-rule-comments');

                        // removed the cached flow analysis rule details
                        $('#flow-analysis-rule-configuration').removeData('flowAnalysisRuleDetails');

                        // clean up an shown verification errors
                        $('#flow-analysis-rule-properties-verification-results').hide();
                        $('#flow-analysis-rule-properties-verification-results-listing').css('border-width', '0').empty();
                        $('#flow-analysis-rule-properties').css('bottom', '0');

                        // clear most recently submitted referenced attributes
                        referencedAttributes = null;
                    },
                    open: function () {
                        nfCommon.toggleScrollable($('#' + this.find('.tab-container').attr('id') + '-content').get(0));
                    }
                }
            });

            // initialize the property table
            $('#flow-analysis-rule-properties').propertytable({
                readOnly: false,
                supportsGoTo: true,
                dialogContainer: '#new-flow-analysis-rule-property-container',
                descriptorDeferred: getFlowAnalysisRulePropertyDescriptor,
                controllerServiceCreatedDeferred: function (response) {
                    return nfControllerServices.loadControllerServices(controllerServicesUri, $('#controller-services-table'));
                },
                goToServiceDeferred: goToServiceFromProperty
            });
        },

        /**
         * Shows the configuration dialog for the specified flow analysis rule.
         *
         * @argument {flowAnalysisRule} flowAnalysisRuleEntity      The flow analysis rule
         */
        showConfiguration: function (flowAnalysisRuleEntity) {
            var flowAnalysisRuleDialog = $('#flow-analysis-rule-configuration');

            flowAnalysisRuleDialog.find('.dialog-header .dialog-header-text').text('Configure Flow Analysis Rule');
            if (flowAnalysisRuleDialog.data('mode') === config.readOnly) {
                // update the visibility
                $('#flow-analysis-rule-configuration .flow-analysis-rule-read-only').hide();
                $('#flow-analysis-rule-configuration .flow-analysis-rule-editable').show();

                // initialize the property table
                $('#flow-analysis-rule-properties').propertytable('destroy').propertytable({
                    readOnly: false,
                    supportsGoTo: true,
                    dialogContainer: '#new-flow-analysis-rule-property-container',
                    descriptorDeferred: getFlowAnalysisRulePropertyDescriptor,
                    controllerServiceCreatedDeferred: function (response) {
                        return nfControllerServices.loadControllerServices(controllerServicesUri, $('#controller-services-table'));
                    },
                    goToServiceDeferred: goToServiceFromProperty
                });

                // update the mode
                flowAnalysisRuleDialog.data('mode', config.edit);
            }

            // reload the task in case the property descriptors have changed
            var reloadTask = $.ajax({
                type: 'GET',
                url: flowAnalysisRuleEntity.uri,
                dataType: 'json'
            });

            // get the flow analysis rule history
            var loadHistory = $.ajax({
                type: 'GET',
                url: '../nifi-api/flow/history/components/' + encodeURIComponent(flowAnalysisRuleEntity.id),
                dataType: 'json'
            });

            // once everything is loaded, show the dialog
            $.when(reloadTask, loadHistory).done(function (taskResponse, historyResponse) {
                // get the updated flow analysis rule
                flowAnalysisRuleEntity = taskResponse[0];
                var flowAnalysisRule = flowAnalysisRuleEntity.component;

                // get the flow analysis rule history
                var flowAnalysisRuleHistory = historyResponse[0].componentHistory;

                // record the flow analysis rule details
                $('#flow-analysis-rule-configuration').data('flowAnalysisRuleDetails', flowAnalysisRuleEntity);

                // populate the flow analysis rule settings
                nfCommon.populateField('flow-analysis-rule-id', flowAnalysisRule['id']);
                nfCommon.populateField('flow-analysis-rule-type', nfCommon.formatType(flowAnalysisRule));
                nfCommon.populateField('flow-analysis-rule-bundle', nfCommon.formatBundle(flowAnalysisRule['bundle']));
                $('#flow-analysis-rule-name').val(flowAnalysisRule['name']);
                $('#flow-analysis-rule-comments').val(flowAnalysisRule['comments']);

                $('#flow-analysis-rule-enforcement-policy-combo').combo({
                    options: [{
                        text: 'Enforce',
                        value: 'ENFORCE',
                        description: 'Treat violations of this rule as errors the correction of which is mandatory.'
                    }
                   , {
                       text: 'Warn',
                       value: 'WARN',
                       description: 'Treat violations of by this rule as warnings the correction of which is recommended but not mandatory.'
                   }
                    ],
                    selectedOption: {
                        value: flowAnalysisRule['enforcementPolicy']
                    }
                });

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
                            $('#flow-analysis-rule-properties').propertytable('saveRow');

                            // save the flow analysis rule
                            saveFlowAnalysisRule(flowAnalysisRuleEntity).done(function (response) {
                                // reload the flow analysis rule
                                nfControllerService.reloadReferencedServices(getControllerServicesTable(), response.component);

                                // close the details panel
                                $('#flow-analysis-rule-configuration').modal('hide');
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
                                $('#flow-analysis-rule-configuration').modal('hide');
                            }
                        }
                    }];

                // determine if we should show the advanced button
                if (nfCommon.isDefinedAndNotNull(flowAnalysisRule.customUiUrl) && flowAnalysisRule.customUiUrl !== '') {
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
                                    $('#flow-analysis-rule-configuration').modal('hide');

                                    // close the settings dialog since the custom ui is also opened in the shell
                                    $('#shell-close-button').click();

                                    // show the custom ui
                                    nfCustomUi.showCustomUi(flowAnalysisRuleEntity, flowAnalysisRule.customUiUrl, true).done(function () {
                                        // once the custom ui is closed, reload the flow analysis rule
                                        nfFlowAnalysisRule.reload(flowAnalysisRuleEntity.id).done(function (response) {
                                            nfControllerService.reloadReferencedServices(getControllerServicesTable(), response.flowAnalysisRule);
                                        });

                                        // show the settings
                                        nfSettings.showSettings();
                                    });
                                };

                                // close all fields currently being edited
                                $('#flow-analysis-rule-properties').propertytable('saveRow');

                                // determine if changes have been made
                                if (isSaveRequired()) {
                                    // see if those changes should be saved
                                    nfDialog.showYesNoDialog({
                                        headerText: 'Save',
                                        dialogContent: 'Save changes before opening the advanced configuration?',
                                        noHandler: openCustomUi,
                                        yesHandler: function () {
                                            saveFlowAnalysisRule(flowAnalysisRuleEntity).done(function () {
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
                $('#flow-analysis-rule-configuration').modal('setButtonModel', buttons);

                // load the property table
                $('#flow-analysis-rule-properties')
                    .propertytable('setGroupId', null)
                    .propertytable('setSupportsSensitiveDynamicProperties', flowAnalysisRule.supportsSensitiveDynamicProperties)
                    .propertytable('loadProperties', flowAnalysisRule.properties, flowAnalysisRule.descriptors, flowAnalysisRuleHistory.propertyHistory)
                    .propertytable('setPropertyVerificationCallback', function (proposedProperties) {
                        nfVerify.verify(flowAnalysisRule['id'], flowAnalysisRuleEntity['uri'], proposedProperties, referencedAttributes, handleVerificationResults, $('#flow-analysis-rule-properties-verification-results-listing'));
                    });

                // show the details
                $('#flow-analysis-rule-configuration').modal('show');

                $('#flow-analysis-rule-properties').propertytable('resetTableSize');
            }).fail(nfErrorHandler.handleAjaxError);
        },

        /**
         * Shows the flow analysis rule details in a read only dialog.
         *
         * @param {object} flowAnalysisRuleEntity
         */
        showDetails: function (flowAnalysisRuleEntity) {
            var flowAnalysisRuleDialog = $('#flow-analysis-rule-configuration');

            flowAnalysisRuleDialog.find('.dialog-header .dialog-header-text').text('Flow Analysis Rule Details');
            if (flowAnalysisRuleDialog.data('mode') === config.edit) {
                // update the visibility
                $('#flow-analysis-rule-configuration .flow-analysis-rule-read-only').show();
                $('#flow-analysis-rule-configuration .flow-analysis-rule-editable').hide();

                // initialize the property table
                $('#flow-analysis-rule-properties').propertytable('destroy').propertytable({
                    supportsGoTo: true,
                    readOnly: true
                });

                // update the mode
                flowAnalysisRuleDialog.data('mode', config.readOnly);
            }

            // reload the task in case the property descriptors have changed
            var reloadTask = $.ajax({
                type: 'GET',
                url: flowAnalysisRuleEntity.uri,
                dataType: 'json'
            });

            // get the flow analysis rule history
            var loadHistory = $.ajax({
                type: 'GET',
                url: '../nifi-api/flow/history/components/' + encodeURIComponent(flowAnalysisRuleEntity.id),
                dataType: 'json'
            });

            // once everything is loaded, show the dialog
            $.when(reloadTask, loadHistory).done(function (taskResponse, historyResponse) {
                // get the updated flow analysis rule
                flowAnalysisRuleEntity = taskResponse[0];
                var flowAnalysisRule = flowAnalysisRuleEntity.component;

                // get the flow analysis rule history
                var flowAnalysisRuleHistory = historyResponse[0].componentHistory;

                // populate the flow analysis rule settings
                nfCommon.populateField('flow-analysis-rule-id', flowAnalysisRule['id']);
                nfCommon.populateField('flow-analysis-rule-type', nfCommon.substringAfterLast(flowAnalysisRule['type'], '.'));
                nfCommon.populateField('flow-analysis-rule-bundle', nfCommon.formatBundle(flowAnalysisRule['bundle']));
                nfCommon.populateField('read-only-flow-analysis-rule-name', flowAnalysisRule['name']);
                nfCommon.populateField('read-only-flow-analysis-rule-comments', flowAnalysisRule['comments']);

                var enforcementPolicy = flowAnalysisRule['enforcementPolicy'];
                if (enforcementPolicy === 'ENFORCE') {
                    enforcementPolicy = 'Enforce';
                } else {
                    enforcementPolicy = 'Warn';
                }
                nfCommon.populateField('read-only-flow-analysis-rule-enforcement-policy', enforcementPolicy);

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
                            flowAnalysisRuleDialog.modal('hide');
                        }
                    }
                }];

                // determine if we should show the advanced button
                if (nfCommon.isDefinedAndNotNull(nfCustomUi) && nfCommon.isDefinedAndNotNull(flowAnalysisRule.customUiUrl) && flowAnalysisRule.customUiUrl !== '') {
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
                                flowAnalysisRuleDialog.modal('hide');

                                // close the settings dialog since the custom ui is also opened in the shell
                                $('#shell-close-button').click();

                                // show the custom ui
                                nfCustomUi.showCustomUi(flowAnalysisRuleEntity, flowAnalysisRule.customUiUrl, false).done(function () {
                                    nfSettings.showSettings();
                                });
                            }
                        }
                    });
                }

                // show the dialog
                flowAnalysisRuleDialog.modal('setButtonModel', buttons).modal('show');

                // load the property table
                $('#flow-analysis-rule-properties')
                    .propertytable('setGroupId', null)
                    .propertytable('loadProperties', flowAnalysisRule.properties, flowAnalysisRule.descriptors, flowAnalysisRuleHistory.propertyHistory);

                // show the details
                flowAnalysisRuleDialog.modal('show');

                $('#flow-analysis-rule-properties').propertytable('resetTableSize');
            });
        },

        /**
         * Enables the specified flow analysis rule.
         *
         * @param {object} flowAnalysisRuleEntity
         */
        enable: function (flowAnalysisRuleEntity) {
            setEnabled(flowAnalysisRuleEntity, true);
        },

        /**
         * Disables the specified flow analysis rule.
         *
         * @param {object} flowAnalysisRuleEntity
         */
        disable: function (flowAnalysisRuleEntity) {
            setEnabled(flowAnalysisRuleEntity, false);
        },

        /**
         * Reloads the specified flow analysis rule.
         *
         * @param {string} id
         */
        reload: function (id) {
            var flowAnalysisRuleGrid = $('#flow-analysis-rules-table').data('gridInstance');
            var flowAnalysisRuleData = flowAnalysisRuleGrid.getData();
            var flowAnalysisRuleEntity = flowAnalysisRuleData.getItemById(id);

            return $.ajax({
                type: 'GET',
                url: flowAnalysisRuleEntity.uri,
                dataType: 'json'
            }).done(function (response) {
                renderFlowAnalysisRule(response);
            }).fail(nfErrorHandler.handleAjaxError);
        },

        /**
         * Prompts the user before attempting to delete the specified flow analysis rule.
         *
         * @param {object} flowAnalysisRuleEntity
         */
        promptToDeleteFlowAnalysisRule: function (flowAnalysisRuleEntity) {
            // prompt for deletion
            nfDialog.showYesNoDialog({
                headerText: 'Delete Flow Analysis Rule',
                dialogContent: 'Delete flow analysis rule \'' + nfCommon.escapeHtml(flowAnalysisRuleEntity.component.name) + '\'?',
                yesHandler: function () {
                    nfFlowAnalysisRule.remove(flowAnalysisRuleEntity);
                }
            });
        },

        /**
         * Deletes the specified flow analysis rule.
         *
         * @param {object} flowAnalysisRuleEntity
         */
        remove: function (flowAnalysisRuleEntity) {
            // prompt for removal?

            var revision = nfClient.getRevision(flowAnalysisRuleEntity);
            $.ajax({
                type: 'DELETE',
                url: flowAnalysisRuleEntity.uri + '?' + $.param({
                    'version': revision.version,
                    'clientId': revision.clientId,
                    'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
                }),
                dataType: 'json'
            }).done(function (response) {
                // remove the task
                var flowAnalysisRuleGrid = $('#flow-analysis-rules-table').data('gridInstance');
                var flowAnalysisRuleData = flowAnalysisRuleGrid.getData();
                flowAnalysisRuleData.deleteItem(flowAnalysisRuleEntity.id);
            }).fail(nfErrorHandler.handleAjaxError);
        }
    };

    return nfFlowAnalysisRule;
}));
