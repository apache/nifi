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

/**
 * Handles versioning.
 */
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'nf.ng.Bridge',
                'nf.ErrorHandler',
                'nf.Dialog',
                'nf.Common',
                'nf.Client',
                'nf.CanvasUtils',
                'nf.ProcessGroup',
                'nf.Graph'],
            function ($, nfNgBridge, nfErrorHandler, nfDialog, nfCommon, nfClient, nfCanvasUtils, nfProcessGroup, nfGraph) {
                return (nf.FlowVersion = factory($, nfNgBridge, nfErrorHandler, nfDialog, nfCommon, nfClient, nfCanvasUtils, nfProcessGroup, nfGraph));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.FlowVerison =
            factory(require('jquery'),
                require('nf.ng.Bridge'),
                require('nf.ErrorHandler'),
                require('nf.Dialog'),
                require('nf.Common'),
                require('nf.Client'),
                require('nf.CanvasUtils'),
                require('nf.ProcessGroup'),
                require('nf.Graph')));
    } else {
        nf.FlowVersion = factory(root.$,
            root.nf.ng.Bridge,
            root.nf.ErrorHandler,
            root.nf.Dialog,
            root.nf.Common,
            root.nf.Client,
            root.nf.CanvasUtils,
            root.nf.ProcessGroup,
            root.nf.Graph);
    }
}(this, function ($, nfNgBridge, nfErrorHandler, nfDialog, nfCommon, nfClient, nfCanvasUtils, nfProcessGroup, nfGraph) {
    'use strict';

    /**
     * Reset the dialog.
     */
    var resetDialog = function () {
        $('#flow-version-registry-combo').combo('destroy').hide();
        $('#flow-version-bucket-combo').combo('destroy').hide();

        $('#flow-version-registry').text('').hide();
        $('#flow-version-bucket').text('').hide();

        $('#flow-version-name').val('');
        $('#flow-version-description').val('');
        $('#flow-version-change-comments').val('');

        $('#flow-version-process-group-id').removeData('versionControlInformation').removeData('revision').text('');
    };

    /**
     * Loads the buckets for the specified registryIdentifier for the current user.
     *
     * @param registryIdentifier
     * @returns {*}
     */
    var loadBuckets = function (registryIdentifier) {
        return $.ajax({
            type: 'GET',
            url: '../nifi-api/flow/registries/' + encodeURIComponent(registryIdentifier) + '/buckets',
            dataType: 'json'
        }).done(function (response) {
            var buckets = [];

            if (nfCommon.isDefinedAndNotNull(response.buckets) && response.buckets.length > 0) {
                response.buckets.sort(function (a, b) {
                    return a.bucket.name > b.bucket.name;
                });

                $.each(response.buckets, function (_, bucketEntity) {
                    var bucket = bucketEntity.bucket;
                    buckets.push({
                        text: bucket.name,
                        value: bucket.id,
                        description: nfCommon.escapeHtml(bucket.description)
                    });
                });
            } else {
                buckets.push({
                    text: 'No available buckets',
                    value: null,
                    optionClass: 'unset',
                    disabled: true
                });
            }

            // load the buckets
            $('#flow-version-bucket-combo').combo('destroy').combo({
                options: buckets,
                select: selectBucket
            });
        }).fail(function () {
            $('#save-flow-version-dialog').modal('refreshButtons');
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Select handler for the registries combo.
     *
     * @param selectedOption
     */
    var selectRegistry = function (selectedOption) {
        if (selectedOption.disabled === true) {
            $('#flow-version-bucket-combo').combo('destroy').combo({
                options: [{
                    text: 'No available buckets',
                    value: null,
                    optionClass: 'unset',
                    disabled: true
                }]
            });

            $('#save-flow-version-dialog').modal('refreshButtons');
        } else {
            loadBuckets(selectedOption.value);
        }
    };

    /**
     * Select handler for the buckets combo.
     *
     * @param selectedOption
     */
    var selectBucket = function (selectedOption) {
        $('#save-flow-version-dialog').modal('refreshButtons');
    };

    /**
     * Saves a flow version.
     *
     * @returns {*}
     */
    var saveFlowVersion = function () {
        var processGroupId = $('#flow-version-process-group-id').text();
        var processGroupRevision = $('#flow-version-process-group-id').data('revision');

        var saveFlowVersionRequest = {
            processGroupRevision: nfClient.getRevision({
                revision: {
                    version: processGroupRevision.version
                }
            })
        };

        var versionControlInformation = $('#flow-version-process-group-id').data('versionControlInformation');
        if (nfCommon.isDefinedAndNotNull(versionControlInformation)) {
            saveFlowVersionRequest['versionedFlow'] = {
                registryId: versionControlInformation.registryId,
                bucketId: versionControlInformation.bucketId,
                flowId: versionControlInformation.flowId,
                flowName: $('#flow-version-name').val(),
                description: $('#flow-version-description').val(),
                comments: $('#flow-version-change-comments').val()
            }
        } else {
            var selectedRegistry =  $('#flow-version-registry-combo').combo('getSelectedOption');
            var selectedBucket =  $('#flow-version-bucket-combo').combo('getSelectedOption');

            saveFlowVersionRequest['versionedFlow'] = {
                registryId: selectedRegistry.value,
                bucketId: selectedBucket.value,
                flowName: $('#flow-version-name').val(),
                description: $('#flow-version-description').val(),
                comments: $('#flow-version-change-comments').val()
            }
        }

        return $.ajax({
            type: 'POST',
            data: JSON.stringify(saveFlowVersionRequest),
            url: '../nifi-api/versions/process-groups/' + encodeURIComponent(processGroupId),
            dataType: 'json',
            contentType: 'application/json'
        }).fail(nfErrorHandler.handleAjaxError);
    };

    return {
        init: function () {
            // initialize the flow version dialog
            $('#save-flow-version-dialog').modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'Save Flow Version',
                buttons: [{
                    buttonText: 'Save',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    disabled: function () {
                        if ($('#flow-version-registry-combo').is(':visible')) {
                            var selectedRegistry =  $('#flow-version-registry-combo').combo('getSelectedOption');
                            var selectedBucket =  $('#flow-version-bucket-combo').combo('getSelectedOption');

                            if (nfCommon.isDefinedAndNotNull(selectedRegistry) && nfCommon.isDefinedAndNotNull(selectedBucket)) {
                                return selectedRegistry.disabled === true || selectedBucket.disabled === true;
                            } else {
                                return true;
                            }
                        } else {
                            return false;
                        }
                    },
                    handler: {
                        click: function () {
                            var processGroupId = $('#flow-version-process-group-id').text();
                            
                            saveFlowVersion().done(function (response) {
                                // refresh either selected PG or bread crumb to reflect connected/tracking status
                                if (nfCanvasUtils.getGroupId() === processGroupId) {
                                    nfNgBridge.injector.get('breadcrumbsCtrl').updateVersionControlInformation(processGroupId, response.versionControlInformation);
                                    nfNgBridge.digest();
                                } else {
                                    nfProcessGroup.reload(processGroupId);
                                }

                                // close the dialog
                                $('#save-flow-version-dialog').modal('hide');
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
                }],
                handler: {
                    close: function () {
                        resetDialog();
                    }
                }
            });
        },

        /**
         * Shows the flow version dialog.
         *
         * @param processGroupId
         */
        showFlowVersionDialog: function (processGroupId) {
            return $.Deferred(function (deferred) {
                $.ajax({
                    type: 'GET',
                    url: '../nifi-api/versions/process-groups/' + encodeURIComponent(processGroupId),
                    dataType: 'json'
                }).done(function (response) {
                    // record the revision
                    $('#flow-version-process-group-id').data('revision', response.processGroupRevision).text(processGroupId);

                    if (nfCommon.isDefinedAndNotNull(response.versionControlInformation)) {
                        var versionControlInformation = response.versionControlInformation;

                        // update the registry and bucket visibility
                        $('#flow-version-registry').text(versionControlInformation.registryId).show();
                        $('#flow-version-bucket').text(versionControlInformation.bucketId).show();

                        $('#flow-version-name').val('');
                        $('#flow-version-description').val('');

                        // record the versionControlInformation
                        $('#flow-version-process-group-id').data('versionControlInformation', versionControlInformation)

                        deferred.resolve();
                    } else {
                        // update the registry and bucket visibility
                        $('#flow-version-registry-combo').show();
                        $('#flow-version-bucket-combo').show();

                        $.ajax({
                            type: 'GET',
                            url: '../nifi-api/flow/registries',
                            dataType: 'json'
                        }).done(function (registriesResponse) {
                            var registries = [];

                            if (nfCommon.isDefinedAndNotNull(registriesResponse.registries) && registriesResponse.registries.length > 0) {
                                registriesResponse.registries.sort(function (a, b) {
                                    return a.component.name > b.component.name;
                                });

                                $.each(registriesResponse.registries, function (_, registryEntity) {
                                    var registry = registryEntity.component;
                                    registries.push({
                                        text: registry.name,
                                        value: registry.id,
                                        description: nfCommon.escapeHtml(registry.description)
                                    });
                                });
                            } else {
                                registries.push({
                                    text: 'No available registries',
                                    value: null,
                                    optionClass: 'unset',
                                    disabled: true
                                });
                            }

                            // load the registries
                            $('#flow-version-registry-combo').combo({
                                options: registries,
                                select: selectRegistry
                            });

                            deferred.resolve();
                        }).fail(function () {
                            deferred.reject();
                        }).fail(nfErrorHandler.handleAjaxError);
                    }
                }).fail(nfErrorHandler.handleAjaxError);
            }).done(function () {
                $('#save-flow-version-dialog').modal('show');
            }).fail(function () {
                $('#save-flow-version-dialog').modal('refreshButtons');
            }).promise();
        },

        /**
         * Reverts changes for the specified Process Group.
         *
         * @param processGroupId
         */
        revertFlowChanges: function (processGroupId) {
            // prompt the user before reverting
            nfDialog.showYesNoDialog({
                headerText: 'Revert Changes',
                dialogContent: 'Are you sure you want to revert changes? All flow configuration changes will be reverted to the last version.',
                noText: 'Cancel',
                yesText: 'Revert',
                yesHandler: function () {
                    $.ajax({
                        type: 'GET',
                        url: '../nifi-api/versions/process-groups/' + encodeURIComponent(processGroupId),
                        dataType: 'json'
                    }).done(function (response) {
                        if (nfCommon.isDefinedAndNotNull(response.versionControlInformation)) {
                            var revertFlowVersionRequest = {
                                processGroupRevision: nfClient.getRevision({
                                    revision: {
                                        version: response.processGroupRevision.version
                                    }
                                }),
                                versionControlInformation: response.versionControlInformation
                            };

                            $.ajax({
                                type: 'POST',
                                data: JSON.stringify(revertFlowVersionRequest),
                                url: '../nifi-api/versions/revert-requests/process-groups/' + encodeURIComponent(processGroupId),
                                dataType: 'json',
                                contentType: 'application/json'
                            }).done(function (response) {
                                // TODO update multi step to show user the ramifications of reverting for confirmation

                                if (nfCanvasUtils.getGroupId() === processGroupId) {
                                    // if reverting current PG... reload/refresh this group/canvas
                                    // TODO consider implementing this differently
                                    $.ajax({
                                        type: 'GET',
                                        url: '../nifi-api/flow/process-groups/' + encodeURIComponent(processGroupId),
                                        dataType: 'json'
                                    }).done(function (response) {
                                        nfGraph.set(response.processGroupFlow.flow);
                                    }).fail(nfErrorHandler.handleAjaxError);
                                } else {
                                    // if reverting selected PG... reload selected PG to update counts, etc
                                    nfProcessGroup.reload(processGroupId);
                                }

                                nfDialog.showOkDialog({
                                    headerText: 'Revert Changes',
                                    dialogContent: 'This Process Group has been reverted.'
                                });
                            }).fail(nfErrorHandler.handleAjaxError);
                        } else {
                            nfDialog.showOkDialog({
                                headerText: 'Revert Changes',
                                dialogContent: 'This Process Group is not currently under version control.'
                            });
                        }
                    }).fail(nfErrorHandler.handleAjaxError);
                }
            });
        },

        /**
         * Disconnects the specified Process Group from flow versioning.
         *
         * @param processGroupId
         */
        disconnectFlowVersioning: function (processGroupId) {
            // prompt the user before disconnecting
            nfDialog.showYesNoDialog({
                headerText: 'Disconnect',
                dialogContent: 'Are you sure you want to disconnect?',
                noText: 'Cancel',
                yesText: 'Disconnect',
                yesHandler: function () {
                    $.ajax({
                        type: 'GET',
                        url: '../nifi-api/versions/process-groups/' + encodeURIComponent(processGroupId),
                        dataType: 'json'
                    }).done(function (response) {
                        if (nfCommon.isDefinedAndNotNull(response.versionControlInformation)) {
                            var revision = nfClient.getRevision({
                                revision: {
                                    version: response.processGroupRevision.version
                                }
                            });

                            $.ajax({
                                type: 'DELETE',
                                url: '../nifi-api/versions/process-groups/' + encodeURIComponent(processGroupId) + '?' + $.param(revision),
                                dataType: 'json',
                                contentType: 'application/json'
                            }).done(function (response) {
                                // refresh either selected PG or bread crumb to reflect disconnected status
                                if (nfCanvasUtils.getGroupId() === processGroupId) {
                                    nfNgBridge.injector.get('breadcrumbsCtrl').updateVersionControlInformation(processGroupId, undefined);
                                    nfNgBridge.digest();
                                } else {
                                    nfProcessGroup.reload(processGroupId);
                                }

                                nfDialog.showOkDialog({
                                    headerText: 'Disconnect',
                                    dialogContent: 'This Process Group has been disconnected.'
                                });
                            }).fail(nfErrorHandler.handleAjaxError);
                        } else {
                            nfDialog.showOkDialog({
                                headerText: 'Disconnect',
                                dialogContent: 'This Process Group is not currently under version control.'
                            })
                        }
                    }).fail(nfErrorHandler.handleAjaxError);
                }
            });
        }
    };
}));