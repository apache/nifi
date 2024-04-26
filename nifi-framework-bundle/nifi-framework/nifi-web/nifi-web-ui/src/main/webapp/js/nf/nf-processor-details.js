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
                'nf.Common',
                'nf.UniversalCapture',
                'nf.Dialog',
                'nf.ErrorHandler',
                'nf.CustomUi',
                'nf.ClusterSummary',
                'nf.CanvasUtils'],
            function ($, nfCommon, nfUniversalCapture, nfDialog, nfErrorHandler, nfCustomUi, nfClusterSummary, nfCanvasUtils) {
                return (nf.ProcessorDetails = factory($, nfCommon, nfUniversalCapture, nfDialog, nfErrorHandler, nfCustomUi, nfClusterSummary, nfCanvasUtils));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ProcessorDetails =
            factory(require('jquery'),
                require('nf.Common'),
                require('nf.UniversalCapture'),
                require('nf.Dialog'),
                require('nf.ErrorHandler'),
                require('nf.CustomUi'),
                require('nf.ClusterSummary'),
                require('nf.CanvasUtils')));
    } else {
        nf.ProcessorDetails = factory(root.$,
            root.nf.Common,
            root.nf.UniversalCapture,
            root.nf.Dialog,
            root.nf.ErrorHandler,
            root.nf.CustomUi,
            root.nf.ClusterSummary,
            root.nf.CanvasUtils);
    }
}(this, function ($, nfCommon, nfUniversalCapture, nfDialog, nfErrorHandler, nfCustomUi, nfClusterSummary, nfCanvasUtils) {
    'use strict';

    /**
     * Configuration option variable for the nfProcessorDetails dialog
     */
    var config;

    /**
     * Creates an option for the specified relationship name.
     *
     * @argument {object} relationship      The relationship
     */
    var createRelationshipOption = function (relationship) {
        var relationshipValue = $('<span class="relationship-name-value hidden"></span>').text(relationship.name);

        // build terminate checkbox element
        var terminateCheckbox = $('<div class="processor-terminate-relationship nf-checkbox disabled"></div>');
        var terminateLabel = $('<div class="relationship-name nf-checkbox-label ellipsis"></div>').text('terminate');
        if (relationship.autoTerminate === true) {
            terminateCheckbox.addClass('checkbox-checked');
        } else {
            terminateCheckbox.addClass('checkbox-unchecked');
        }
        var terminateCheckboxBundle = $('<div class="processor-terminate-relationship-container"></div>').append(terminateCheckbox).append(terminateLabel);

        // build the retry checkbox element
        var retryCheckbox = $('<div class="processor-retry-relationship nf-checkbox disabled"></div>');
        var retryLabel = $('<div class="relationship-name nf-checkbox-label ellipsis"></div>').text('retry');
        if (relationship.retry === true) {
            retryCheckbox.addClass('checkbox-checked');
        } else {
            retryCheckbox.addClass('checkbox-unchecked');
        }
        var retryCheckboxBundle = $('<div class="processor-retry-relationship-container"></div>').append(retryCheckbox).append(retryLabel);

        // build the relationship container element
        var relationshipContainerHeading = $('<div></div>').text(relationship.name);
        var relationshipContainerElement = $('<div class="processor-relationship-container"></div>').append(relationshipContainerHeading).append(terminateCheckboxBundle).append(retryCheckboxBundle).append(relationshipValue).appendTo('#read-only-auto-action-relationship-names');
        if (!nfCommon.isBlank(relationship.description)) {
            var relationshipDescription = $('<div class="relationship-description"></div>').text(relationship.description);
            relationshipContainerElement.append(relationshipDescription);
        }

        return relationshipContainerElement;
    };

    return {
        /**
         * Initializes the processor details dialog.
         *
         * @param {options}   The configuration options object for the dialog
         */
        init: function (options) {

            //set the dialog window configuration options.
            config = options;

            // initialize the properties tabs
            $('#processor-details-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                scrollableTabContentStyle: 'scrollable',
                tabs: [{
                    name: 'Settings',
                    tabContentId: 'details-standard-settings-tab-content'
                }, {
                    name: 'Scheduling',
                    tabContentId: 'details-scheduling-tab-content'
                }, {
                    name: 'Properties',
                    tabContentId: 'details-processor-properties-tab-content'
                }, {
                    name: 'Relationships',
                    tabContentId: 'details-processor-relationships-tab-content'
                }, {
                    name: 'Comments',
                    tabContentId: 'details-processor-comments-tab-content'
                }],
                select: function () {
                    // remove all property detail dialogs
                    nfUniversalCapture.removeAllPropertyDetailDialogs();

                    // resize the property grid in case this is the first time its rendered
                    if ($(this).text() === 'Properties') {
                        $('#read-only-processor-properties').propertytable('resetTableSize');
                    }
                }
            });

            // configure the processor details dialog
            $('#processor-details').modal({
                headerText: 'Processor Details',
                scrollableContentStyle: 'scrollable',
                handler: {
                    close: function () {
                        // empty the relationship list
                        $('#read-only-auto-action-relationship-names').empty();

                        // clear the property grid
                        $('#read-only-processor-properties').propertytable('clear');

                        // clear the processor details
                        nfCommon.clearField('read-only-processor-id');
                        nfCommon.clearField('read-only-processor-type');
                        nfCommon.clearField('read-only-processor-name');
                        nfCommon.clearField('read-only-concurrently-schedulable-tasks');
                        nfCommon.clearField('read-only-scheduling-period');
                        nfCommon.clearField('read-only-penalty-duration');
                        nfCommon.clearField('read-only-yield-duration');
                        nfCommon.clearField('read-only-run-duration');
                        nfCommon.clearField('read-only-bulletin-level');
                        nfCommon.clearField('read-only-execution-node');
                        nfCommon.clearField('read-only-execution-status');
                        nfCommon.clearField('read-only-processor-comments');

                        // removed the cached processor details
                        $('#processor-details').removeData('processorDetails');
                        $('#processor-details').removeData('processorHistory');

                        //stop any synchronization on the status bar
                        if(config.supportsStatusBar){
                            $("#processor-details-status-bar").statusbar('disconnect');
                        }
                    },
                    open: function () {
                        nfCommon.toggleScrollable($('#' + this.find('.tab-container').attr('id') + '-content').get(0));
                    }
                }
            });

            //apply the status bar if indicated
            if(config.supportsStatusBar){
                $("#processor-details-status-bar").statusbar('processor');
            }

            // initialize the properties

            $('#read-only-processor-properties').propertytable(Object.assign({
                    supportsGoTo: config.supportsGoTo,
                    readOnly: true
                },
                //incase of summary window, nfCanvasUtils module wont be loaded
                nfCanvasUtils && {
                    getParameterContext: function (groupId) {
                        // processors being configured must be in the current group
                        return nfCanvasUtils.getParameterContext();
                    }
                }));
        },

        /**
         * Shows the details for the specified processor.
         *
         * @argument {string} groupId       The group id
         * @argument {string} processorId   The processor id
         */
        showDetails: function (groupId, processorId) {
            // load the properties for the specified processor
            var getProcessor = $.ajax({
                type: 'GET',
                url: '../nifi-api/processors/' + encodeURIComponent(processorId),
                dataType: 'json'
            }).done(function (response) {
                if (nfCommon.isDefinedAndNotNull(response.component)) {
                    // get the processor details
                    var details = response.component;

                    // record the processor details
                    $('#processor-details').data('processorDetails', details);

                    // add subtitle with component type details
                    $('#processor-details').modal('setSubtitle', nfCommon.formatType(details));

                    // populate the processor settings
                    nfCommon.populateField('read-only-processor-id', details['id']);
                    nfCommon.populateField('read-only-processor-type', nfCommon.formatType(details));
                    nfCommon.populateField('read-only-processor-bundle', nfCommon.formatBundle(details['bundle']));
                    nfCommon.populateField('read-only-processor-name', details['name']);
                    nfCommon.populateField('read-only-concurrently-schedulable-tasks', details.config['concurrentlySchedulableTaskCount']);
                    nfCommon.populateField('read-only-scheduling-period', details.config['schedulingPeriod']);
                    nfCommon.populateField('read-only-penalty-duration', details.config['penaltyDuration']);
                    nfCommon.populateField('read-only-yield-duration', details.config['yieldDuration']);
                    nfCommon.populateField('read-only-run-duration', nfCommon.formatDuration(details.config['runDurationMillis']));
                    nfCommon.populateField('read-only-bulletin-level', details.config['bulletinLevel']);
                    nfCommon.populateField('read-only-processor-comments', details.config['comments']);

                    var showRunSchedule = true;

                    var schedulingStrategy = details.config['schedulingStrategy'];

                    // make the scheduling strategy human readable
                    if (schedulingStrategy === 'CRON_DRIVEN') {
                        schedulingStrategy = 'CRON driven';
                    } else {
                      // the only other option schedulingStrategy === 'TIMER_DRIVEN'
                        schedulingStrategy = "Timer driven";
                    }
                    nfCommon.populateField('read-only-scheduling-strategy', schedulingStrategy);

                    // only show the run schedule when applicable
                    if (showRunSchedule === true) {
                        $('#read-only-run-schedule').show();
                    } else {
                        $('#read-only-run-schedule').hide();
                    }

                    var executionNode = details.config['executionNode'];

                    if (executionNode === 'ALL') {
                        executionNode = "All nodes";
                    } else if (executionNode === 'PRIMARY') {
                        executionNode = "Primary node only";
                    }

                    nfCommon.populateField('read-only-execution-node', executionNode);

                    $('#read-only-execution-node-options').show();

                    // load the relationship list
                    if (!nfCommon.isEmpty(details.relationships)) {
                        $.each(details.relationships, function (i, relationship) {
                            createRelationshipOption(relationship);
                        });

                        // set initial disabled value for retry controls
                        var setRetryControlsDisabledState = (function() {
                            var isEnabled = $('#read-only-auto-action-relationship-names').find('div.nf-checkbox.processor-retry-relationship.checkbox-checked').length ? true : false;
                            if (isEnabled) {
                                $('#details-processor-relationships-tab-content .settings-right').show();
                            } else {
                                $('#details-processor-relationships-tab-content .settings-right').hide();
                            }
                        });
                        setRetryControlsDisabledState();
                    } else {
                        $('#read-only-auto-action-relationship-names').append('<div class="unset">This processor has no relationships.</div>');
                    }

                    if (nfCommon.isDefinedAndNotNull(details.config.backoffMechanism)) {
                        if (details.config.backoffMechanism === 'PENALIZE_FLOWFILE') {
                            $('.details-backoff-policy-setting #penalizeFlowFile').prop("checked", true);
                        } else if (details.config.backoffMechanism === 'YIELD_PROCESSOR') {
                            $('.details-backoff-policy-setting #yieldEntireProcessor').prop("checked", true);
                        }
                    }

                    if (nfCommon.isDefinedAndNotNull(details.config.maxBackoffPeriod)) {
                        $('.details-max-backoff-setting #details-max-backoff-period').text(details.config.maxBackoffPeriod);
                    }

                    if (nfCommon.isDefinedAndNotNull(details.config.retryCount)) {
                        $('.details-retry-count-setting #details-retry-attempt-count').text(details.config.retryCount);
                    }
                }
            });

            // get the processor history
            var getProcessorHistory = $.ajax({
                type: 'GET',
                url: '../nifi-api/flow/history/components/' + encodeURIComponent(processorId),
                dataType: 'json'
            }).done(function (response) {
                var processorHistory = response.componentHistory;

                // record the processor history
                $('#processor-details').data('processorHistory', processorHistory);
            });

            // show the dialog once we have the processor and its history
            $.when(getProcessor, getProcessorHistory).done(function (processorResult, historyResult) {
                var processorResponse = processorResult[0];
                var processor = processorResponse.component;
                var historyResponse = historyResult[0];
                var history = historyResponse.componentHistory;
                var selection;

                // load the properties
                $('#read-only-processor-properties')
                    .propertytable('setGroupId', processor.parentGroupId)
                    .propertytable('loadProperties', processor.config.properties, processor.config.descriptors, history.propertyHistory);

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
                            $('#processor-details').modal('hide');
                        }
                    }
                }];

                // determine if we should show the advanced button
                if (top === window && nfCommon.isDefinedAndNotNull(nfCustomUi) && nfCommon.isDefinedAndNotNull(processor.config.customUiUrl) && processor.config.customUiUrl !== '') {
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
                                $('#processor-details').modal('hide');

                                // show the custom ui
                                nfCustomUi.showCustomUi(processorResponse, processor.config.customUiUrl, false);
                            }
                        }
                    });
                }

                //Populate the status bar if the feature is enabled
                if (config.supportsStatusBar && nfCommon.isDefinedAndNotNull(config.nfCanvasUtils)){
                    var formattedBulletins = nfCommon.getFormattedBulletins(processorResponse.bulletins);
                    var unorderedBulletins = nfCommon.formatUnorderedList(formattedBulletins);

                    //initialize the canvas synchronization
                    $("#processor-details-status-bar").statusbar(
                        'observe',
                        {
                            processor: processor.id,
                            bulletins: unorderedBulletins
                        }
                    );

                    //Fetch the component as a selection from the canvas
                    selection = config.nfCanvasUtils.getSelectionById(processor.id);

                    //Add the stop & configure button if appropriate
                    if(nfCommon.isDefinedAndNotNull(config.nfActions) &&
                        config.nfCanvasUtils.isProcessor(selection) &&
                        config.nfCanvasUtils.canModify(selection)){

                        //Declare a callback handler to perform should ProcessorConfiguration be invoked
                        var cb = function(){
                            var selectedTab = $('#processor-details-tabs').find('.selected-tab').text();
                            $('#processor-configuration-tabs').find('.tab:contains("'+selectedTab+'")').trigger('click');
                            $('#processor-details').modal('hide');
                            $("#processor-details-status-bar").statusbar('showButtons');
                        };

                        $("#processor-details-status-bar").statusbar('buttons',[{
                            buttonHtml: '<i class="fa fa-stop stop-configure-icon" aria-hidden="true"></i><span>Stop & Configure</span>',
                            clazz: 'button button-icon auto-width',
                            color: {
                                hover: '#C7D2D7',
                                base: 'transparent',
                                text: '#004849'
                            },
                            disabled : function() {
                                return !config.nfCanvasUtils.isStoppable(selection);
                            },
                            handler: {
                                click: function() {
                                    //execute the stop and open the configuration modal
                                    $("#processor-details-status-bar").statusbar('hideButtons');
                                    config.nfActions.stopAndConfigure(selection,cb);
                                }
                            }
                        },
                        {
                            buttonText: 'Configure',
                            clazz: 'fa fa-cog button-icon',
                            color: {
                                hover: '#C7D2D7',
                                base: 'transparent',
                                text: '#004849'
                            },
                            disabled : function() {
                                return config.nfCanvasUtils.isStoppable(selection);
                            },
                            handler: {
                                click: function() {
                                    //execute the stop and open the configuration modal
                                    $("#processor-details-status-bar").statusbar('hideButtons');
                                    config.nfActions.showConfiguration(selection,cb);
                                }
                            }
                        }]);
                    }

                }

                // show the dialog
                $('#processor-details').modal('setButtonModel', buttons).modal('show');

                // add ellipsis if necessary
                $('#processor-details div.relationship-name').ellipsis();
            }).fail(nfErrorHandler.handleAjaxError);
        }
    };
}));
