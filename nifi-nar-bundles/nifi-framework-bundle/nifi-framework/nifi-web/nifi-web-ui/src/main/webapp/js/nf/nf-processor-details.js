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

/* global nf */

nf.ProcessorDetails = (function () {

    /**
     * Creates an option for the specified relationship name.
     *
     * @argument {object} relationship      The relationship
     */
    var createRelationshipOption = function (relationship) {
        var relationshipLabel = $('<div class="relationship-name ellipsis"></div>').text(relationship.name);

        // build the relationship checkbox element
        if (relationship.autoTerminate === true) {
            relationshipLabel.css('font-weight', 'bold');
        }

        // build the relationship container element
        var relationshipContainerElement = $('<div class="processor-relationship-container"></div>').append(relationshipLabel).appendTo('#read-only-auto-terminate-relationship-names');
        if (!nf.Common.isBlank(relationship.description)) {
            var relationshipDescription = $('<div class="relationship-description"></div>').text(relationship.description);
            relationshipContainerElement.append(relationshipDescription);
        }

        return relationshipContainerElement;
    };

    return {
        /**
         * Initializes the processor details dialog.
         */
        init: function () {
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
                    name: 'Comments',
                    tabContentId: 'details-processor-comments-tab-content'
                }],
                select: function () {
                    // remove all property detail dialogs
                    nf.UniversalCapture.removeAllPropertyDetailDialogs();

                    // resize the property grid in case this is the first time its rendered
                    if ($(this).text() === 'Properties') {
                        $('#read-only-processor-properties').propertytable('resetTableSize');
                    }

                    // show the border if processor relationship names if necessary
                    var processorRelationships = $('#read-only-auto-terminate-relationship-names');
                    if (processorRelationships.is(':visible') && processorRelationships.get(0).scrollHeight > processorRelationships.innerHeight()) {
                        processorRelationships.css('border-width', '1px');
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
                        $('#read-only-auto-terminate-relationship-names').css('border-width', '0').empty();

                        // clear the property grid
                        $('#read-only-processor-properties').propertytable('clear');

                        // clear the processor details
                        nf.Common.clearField('read-only-processor-id');
                        nf.Common.clearField('read-only-processor-type');
                        nf.Common.clearField('read-only-processor-name');
                        nf.Common.clearField('read-only-concurrently-schedulable-tasks');
                        nf.Common.clearField('read-only-scheduling-period');
                        nf.Common.clearField('read-only-penalty-duration');
                        nf.Common.clearField('read-only-yield-duration');
                        nf.Common.clearField('read-only-run-duration');
                        nf.Common.clearField('read-only-bulletin-level');
                        nf.Common.clearField('read-only-execution-status');
                        nf.Common.clearField('read-only-processor-comments');

                        // removed the cached processor details
                        $('#processor-details').removeData('processorDetails');
                        $('#processor-details').removeData('processorHistory');
                    },
                    open: function () {
                        nf.Common.toggleScrollable($('#' + this.find('.tab-container').attr('id') + '-content').get(0));
                    }
                }
            });

            // initialize the properties
            $('#read-only-processor-properties').propertytable({
                readOnly: true
            });
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
                if (nf.Common.isDefinedAndNotNull(response.component)) {
                    // get the processor details
                    var details = response.component;

                    // record the processor details
                    $('#processor-details').data('processorDetails', details);

                    // populate the processor settings
                    nf.Common.populateField('read-only-processor-id', details['id']);
                    nf.Common.populateField('read-only-processor-type', nf.Common.substringAfterLast(details['type'], '.'));
                    nf.Common.populateField('read-only-processor-name', details['name']);
                    nf.Common.populateField('read-only-concurrently-schedulable-tasks', details.config['concurrentlySchedulableTaskCount']);
                    nf.Common.populateField('read-only-scheduling-period', details.config['schedulingPeriod']);
                    nf.Common.populateField('read-only-penalty-duration', details.config['penaltyDuration']);
                    nf.Common.populateField('read-only-yield-duration', details.config['yieldDuration']);
                    nf.Common.populateField('read-only-run-duration', nf.Common.formatDuration(details.config['runDurationMillis']));
                    nf.Common.populateField('read-only-bulletin-level', details.config['bulletinLevel']);
                    nf.Common.populateField('read-only-processor-comments', details.config['comments']);

                    var showRunSchedule = true;

                    // make the scheduling strategy human readable
                    var schedulingStrategy = details.config['schedulingStrategy'];
                    if (schedulingStrategy === 'EVENT_DRIVEN') {
                        showRunSchedule = false;
                        schedulingStrategy = 'Event driven';
                    } else if (schedulingStrategy === 'CRON_DRIVEN') {
                        schedulingStrategy = 'CRON driven';
                    } else if (schedulingStrategy === 'TIMER_DRIVEN') {
                        schedulingStrategy = "Timer driven";
                    } else {
                        schedulingStrategy = "On primary node";
                    }
                    nf.Common.populateField('read-only-scheduling-strategy', schedulingStrategy);

                    // only show the run schedule when applicable
                    if (showRunSchedule === true) {
                        $('#read-only-run-schedule').show();
                    } else {
                        $('#read-only-run-schedule').hide();
                    }

                    // load the relationship list
                    if (!nf.Common.isEmpty(details.relationships)) {
                        $.each(details.relationships, function (i, relationship) {
                            createRelationshipOption(relationship);
                        });
                    } else {
                        $('#read-only-auto-terminate-relationship-names').append('<div class="unset">This processor has no relationships.</div>');
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

                // load the properties
                $('#read-only-processor-properties').propertytable('loadProperties', processor.config.properties, processor.config.descriptors, history.propertyHistory);

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
                if (nf.Common.isDefinedAndNotNull(nf.CustomUi) && nf.Common.isDefinedAndNotNull(processor.config.customUiUrl) && processor.config.customUiUrl !== '') {
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
                                nf.CustomUi.showCustomUi(processorResponse, processor.config.customUiUrl, false);
                            }
                        }
                    });
                }

                // show the dialog
                $('#processor-details').modal('setButtonModel', buttons).modal('show');

                // add ellipsis if necessary
                $('#processor-details div.relationship-name').ellipsis();

                // show the border if necessary
                var processorRelationships = $('#read-only-auto-terminate-relationship-names');
                if (processorRelationships.is(':visible') && processorRelationships.get(0).scrollHeight > processorRelationships.innerHeight()) {
                    processorRelationships.css('border-width', '1px');
                }
            }).fail(function (xhr, status, error) {
                if (xhr.status === 400 || xhr.status === 404 || xhr.status === 409) {
                    nf.Dialog.showOkDialog({
                        headerText: 'Error',
                        dialogContent: nf.Common.escapeHtml(xhr.responseText)
                    });
                } else {
                    nf.Common.handleAjaxError(xhr, status, error);
                }
            });
        }
    };
}());