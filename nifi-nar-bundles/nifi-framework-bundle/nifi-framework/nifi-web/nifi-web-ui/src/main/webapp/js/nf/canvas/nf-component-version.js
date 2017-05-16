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
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'nf.ErrorHandler',
                'nf.Common',
                'nf.Client',
                'nf.CanvasUtils',
                'nf.ProcessGroupConfiguration',
                'nf.ng.Bridge'],
            function ($, Slick, nfErrorHandler, nfCommon, nfClient, nfCanvasUtils, nfProcessGroupConfiguration, nfNgBridge) {
                return (nf.ComponentState = factory($, nfErrorHandler, nfCommon, nfClient, nfCanvasUtils, nfProcessGroupConfiguration, nfNgBridge));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ComponentState =
            factory(require('jquery'),
                require('nf.ErrorHandler'),
                require('nf.Common',
                require('nf.Client'),
                require('nf.CanvasUtils'),
                require('nf.ProcessGroupConfiguration'),
                require('nf.ng.Bridge'))));
    } else {
        nf.ComponentVersion = factory(root.$,
            root.nf.ErrorHandler,
            root.nf.Common,
            root.nf.Client,
            root.nf.CanvasUtils,
            root.nf.ProcessGroupConfiguration,
            root.nf.ng.Bridge);
    }
}(this, function ($, nfErrorHandler, nfCommon, nfClient, nfCanvasUtils, nfProcessGroupConfiguration, nfNgBridge) {
    'use strict';

    var versionMap;
    var nfSettings;

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

        // clear the service apis
        $('#component-version-controller-service-apis').empty();
        $('#component-version-controller-service-apis-container').hide();

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
        if (nfCommon.isDefinedAndNotNull(documentedType.usageRestriction)) {
            $('#component-version-restriction').text(documentedType.usageRestriction);
        } else {
            $('#component-version-restriction').addClass('unset').text('No restriction');
        }

        // update the service apis if necessary
        if (!nfCommon.isEmpty(documentedType.controllerServiceApis)) {
            var formattedControllerServiceApis = nfCommon.getFormattedServiceApis(documentedType.controllerServiceApis);
            var serviceTips = nfCommon.formatUnorderedList(formattedControllerServiceApis);
            $('#component-version-controller-service-apis').empty().append(serviceTips);
            $('#component-version-controller-service-apis-container').show();
        }

        // update the tags and description
        $('#component-version-tags').text(documentedType.tags.join(', '));
        $('#component-version-description').text(documentedType.description);
    };

    return {
        init: function (settings) {
            versionMap = d3.map();
            nfSettings = settings;

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
                                'revision': nfClient.getRevision(componentEntity),
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
                                    nfCanvasUtils.getComponentByType(componentEntity.type).set(response);

                                    // inform Angular app values have changed
                                    nfNgBridge.digest();
                                } else if (componentEntity.type === 'ControllerService') {
                                    var parentGroupId = componentEntity.component.parentGroupId;

                                    $.Deferred(function (deferred) {
                                        if (nfCommon.isDefinedAndNotNull(parentGroupId)) {
                                            if ($('#process-group-configuration').is(':visible')) {
                                                nfProcessGroupConfiguration.loadConfiguration(parentGroupId).done(function () {
                                                    deferred.resolve();
                                                });
                                            } else {
                                                nfProcessGroupConfiguration.showConfiguration(parentGroupId).done(function () {
                                                    deferred.resolve();
                                                });
                                            }
                                        } else {
                                            if ($('#settings').is(':visible')) {
                                                // reload the settings
                                                nfSettings.loadSettings().done(function () {
                                                    deferred.resolve();
                                                });
                                            } else {
                                                // reload the settings and show
                                                nfSettings.showSettings().done(function () {
                                                    deferred.resolve();
                                                });
                                            }
                                        }
                                    }).done(function () {
                                        if (nfCommon.isDefinedAndNotNull(parentGroupId)) {
                                            nfProcessGroupConfiguration.selectControllerService(componentEntity.id);
                                        } else {
                                            nfSettings.selectControllerService(componentEntity.id);
                                        }
                                    });
                                } else if (componentEntity.type === 'ReportingTask') {
                                    $.Deferred(function (deferred) {
                                        if ($('#settings').is(':visible')) {
                                            // reload the settings
                                            nfSettings.loadSettings().done(function () {
                                                deferred.resolve();
                                            });
                                        } else {
                                            // reload the settings and show
                                            nfSettings.showSettings().done(function () {
                                                deferred.resolve();
                                            });
                                        }
                                    }).done(function () {
                                        nfSettings.selectReportingTask(componentEntity.id);
                                    });
                                }
                            }).fail(nfErrorHandler.handleAjaxError);

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
            var params = {
                 'bundleGroupFilter': componentEntity.component.bundle.group,
                 'bundleArtifactFilter': componentEntity.component.bundle.artifact
            };

            // special handling for incorrect query param
            if (getTypeField(componentEntity) === 'controllerServiceTypes') {
                params['typeFilter'] = componentEntity.component.type;
            } else {
                params['type'] = componentEntity.component.type;
            }

            return $.ajax({
                type: 'GET',
                url: getTypeUri(componentEntity) + '?' + $.param(params),
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
                        description: nfCommon.escapeHtml(documentedType.description)
                    };

                    // record the currently selected option
                    if (documentedType.bundle.version === componentEntity.component.bundle.version) {
                        selectedOption = option;
                    }

                    // store this option
                    options.push(option);
                });

                // sort the text version visible to the user
                options.sort(function (a, b) {
                    return -nfCommon.sortVersion(a.text, b.text);
                });

                // populate the name/description
                $('#component-version-name').text(componentEntity.component.name);
                $('#component-version-bundle').text(nfCommon.formatBundle(componentEntity.component.bundle));

                // build the combo
                $('#component-version-selector').combo({
                    options: options,
                    selectedOption: selectedOption,
                    select: select
                });

                // show the dialog
                $('#component-version-dialog').data('component', componentEntity).modal('show');
            }).fail(nfErrorHandler.handleAjaxError);
        }
    };
}));