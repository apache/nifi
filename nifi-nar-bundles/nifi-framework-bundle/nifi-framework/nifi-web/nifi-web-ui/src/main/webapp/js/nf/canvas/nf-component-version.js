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

/**
 * Views state for a given component.
 */
nf.ComponentVersion = (function () {

    var versionMap;

    /**
     * Gets the URI for retrieving available types/bundles
     *
     * @param {object} componentEntity
     * @returns {string} uri
     */
    var getTypeUri = function (componentEntity) {
        if (componentEntity.type === 'ReportingTask') {
            return '../nifi-api/flow/reporting-task-types';
        } else if (componentEntity.type === 'ControllerService') {
            return '../nifi-api/flow/controller-service-types';
        } else {
            return '../nifi-api/flow/processor-types';
        }
    };

    /**
     * Gets the field to use to access the returned types/bundles.
     *
     * @param {object} componentEntity
     * @returns {string} field
     */
    var getTypeField = function (componentEntity) {
        if (componentEntity.type === 'ReportingTask') {
            return 'reportingTaskTypes';
        } else if (componentEntity.type === 'ControllerService') {
            return 'controllerServiceTypes';
        } else {
            return 'processorTypes';
        }
    };

    /**
     * Reset the dialog.
     */
    var resetDialog = function () {
        // clear the versions
        var versions = versionMap.keys();
        $.each(versions, function (_, version) {
            versionMap.remove(version);
        });

        // clear the fields
        $('#component-version-name').text('');
        $('#component-version-bundle').text('');
        $('#component-version-tags').text('');
        $('#component-version-restriction').removeClass('unset').text('');
        $('#component-version-description').text('');

        // destroy the version combo
        $('#component-version-selector').combo('destroy');

        // removed the stored data
        $('#component-version-dialog').removeData('component');
    };

    /**
     * Sets the specified option.
     *
     * @param {object} selectedOption
     */
    var select = function (selectedOption) {
        var documentedType = versionMap.get(selectedOption.value);

        // set any restriction
        if (nf.Common.isDefinedAndNotNull(documentedType.usageRestriction)) {
            $('#component-version-restriction').text(documentedType.usageRestriction);
        } else {
            $('#component-version-restriction').addClass('unset').text('No restriction');
        }

        // show the tags and description
        $('#component-version-tags').text(documentedType.tags.join(', '));
        $('#component-version-description').text(documentedType.description);
    };

    return {
        init: function () {
            versionMap = d3.map();

            // initialize the component version dialog
            $('#component-version-dialog').modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'Component Version',
                buttons: [{
                    buttonText: 'Apply',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            // get the selected version
                            var selectedOption = $('#component-version-selector').combo('getSelectedOption');
                            var documentedType = versionMap.get(selectedOption.value);

                            // get the current component
                            var componentEntity = $('#component-version-dialog').data('component');

                            // build the request entity
                            var requestEntity = {
                                'revision': nf.Client.getRevision(componentEntity),
                                'component': {
                                    'id': componentEntity.id,
                                    'bundle': {
                                        'group': documentedType.bundle.group,
                                        'artifact': documentedType.bundle.artifact,
                                        'version': documentedType.bundle.version
                                    }
                                }
                            };

                            // save the bundle
                            $.ajax({
                                type: 'PUT',
                                url: componentEntity.uri,
                                data: JSON.stringify(requestEntity),
                                dataType: 'json',
                                contentType: 'application/json'
                            }).done(function (response) {
                                // set the response
                                if (componentEntity.type === 'Processor') {
                                    // update the processor
                                    nf.Processor.set(response);

                                    // inform Angular app values have changed
                                    nf.ng.Bridge.digest();
                                } else if (componentEntity.type === 'ControllerService') {
                                    var parentGroupId = componentEntity.component.parentGroupId;

                                    $.Deferred(function (deferred) {
                                        if (nf.Common.isDefinedAndNotNull(parentGroupId)) {
                                            if ($('#process-group-configuration').is(':visible')) {
                                                nf.ProcessGroupConfiguration.loadConfiguration(parentGroupId).done(function () {
                                                    deferred.resolve();
                                                });
                                            } else {
                                                nf.ProcessGroupConfiguration.showConfiguration(parentGroupId).done(function () {
                                                    deferred.resolve();
                                                });
                                            }
                                        } else {
                                            if ($('#settings').is(':visible')) {
                                                // reload the settings
                                                nf.Settings.loadSettings().done(function () {
                                                    deferred.resolve();
                                                });
                                            } else {
                                                // reload the settings and show
                                                nf.Settings.showSettings().done(function () {
                                                    deferred.resolve();
                                                });
                                            }
                                        }
                                    }).done(function () {
                                        if (nf.Common.isDefinedAndNotNull(parentGroupId)) {
                                            nf.ProcessGroupConfiguration.selectControllerService(componentEntity.id);
                                        } else {
                                            nf.Settings.selectControllerService(componentEntity.id);
                                        }
                                    });
                                } else if (componentEntity.type === 'ReportingTask') {
                                    $.Deferred(function (deferred) {
                                        if ($('#settings').is(':visible')) {
                                            // reload the settings
                                            nf.Settings.loadSettings().done(function () {
                                                deferred.resolve();
                                            });
                                        } else {
                                            // reload the settings and show
                                            nf.Settings.showSettings().done(function () {
                                                deferred.resolve();
                                            });
                                        }
                                    }).done(function () {
                                        nf.Settings.selectReportingTask(componentEntity.id);
                                    });
                                }
                            }).fail(nf.ErrorHandler.handleAjaxError);

                            // reset and hide the dialog
                            this.modal('hide');
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
                                this.modal('hide');
                            }
                        }
                    }],
                handler: {
                    close: function () {
                        resetDialog();
                    }
                }
            });
        },

        /**
         * Prompts to change the version of a component.
         *
         * @param {object} componentEntity
         */
        promptForVersionChange: function (componentEntity) {
            return $.ajax({
                type: 'GET',
                url: getTypeUri(componentEntity) + '?' + $.param({
                    'bundleGroup': componentEntity.component.bundle.group,
                    'bundleArtifact': componentEntity.component.bundle.artifact,
                    'type': componentEntity.component.type
                }),
                dataType: 'json'
            }).done(function (response) {
                var options = [];
                var selectedOption;

                // go through each type
                $.each(response[getTypeField(componentEntity)], function (i, documentedType) {
                    var type = documentedType.type;

                    // store the documented type
                    versionMap.set(documentedType.bundle.version, documentedType);

                    // create the option
                    var option = {
                        text: documentedType.bundle.version,
                        value: documentedType.bundle.version,
                        description: nf.Common.escapeHtml(documentedType.description),
                    };

                    // record the currently selected option
                    if (documentedType.bundle.version === componentEntity.component.bundle.version) {
                        selectedOption = option;
                    }

                    // store this option
                    options.push(option);
                });

                // populate the name/description
                $('#component-version-name').text(componentEntity.component.name);
                $('#component-version-bundle').text(nf.Common.formatBundle(componentEntity.component.bundle));

                // build the combo
                $('#component-version-selector').combo({
                    options: options,
                    selectedOption: selectedOption,
                    select: select
                });

                // show the dialog
                $('#component-version-dialog').data('component', componentEntity).modal('show');
            }).fail(nf.ErrorHandler.handleAjaxError);
        }
    };
}());