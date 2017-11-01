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
                'nf.Graph',
                'nf.Birdseye'],
            function ($, nfNgBridge, nfErrorHandler, nfDialog, nfCommon, nfClient, nfCanvasUtils, nfProcessGroup, nfGraph, nfBirdseye) {
                return (nf.FlowVersion = factory($, nfNgBridge, nfErrorHandler, nfDialog, nfCommon, nfClient, nfCanvasUtils, nfProcessGroup, nfGraph, nfBirdseye));
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
                require('nf.Graph'),
                require('nf.Birdseye')));
    } else {
        nf.FlowVersion = factory(root.$,
            root.nf.ng.Bridge,
            root.nf.ErrorHandler,
            root.nf.Dialog,
            root.nf.Common,
            root.nf.Client,
            root.nf.CanvasUtils,
            root.nf.ProcessGroup,
            root.nf.Graph,
            root.nf.Birdseye);
    }
}(this, function ($, nfNgBridge, nfErrorHandler, nfDialog, nfCommon, nfClient, nfCanvasUtils, nfProcessGroup, nfGraph, nfBirdseye) {
    'use strict';

    var serverTimeOffset = null;

    var gridOptions = {
        forceFitColumns: true,
        enableTextSelectionOnCells: true,
        enableCellNavigation: true,
        enableColumnReorder: false,
        autoEdit: false,
        multiSelect: false,
        rowHeight: 24
    };

    /**
     * Reset the save flow version dialog.
     */
    var resetSaveFlowVersionDialog = function () {
        $('#save-flow-version-registry-combo').combo('destroy').hide();
        $('#save-flow-version-bucket-combo').combo('destroy').hide();

        $('#save-flow-version-registry').text('').hide();
        $('#save-flow-version-bucket').text('').hide();

        $('#save-flow-version-name').text('').hide();
        $('#save-flow-version-description').text('').hide();

        $('#save-flow-version-name-field').val('').hide();
        $('#save-flow-version-description-field').val('').hide();
        $('#save-flow-version-change-comments').val('');

        $('#save-flow-version-process-group-id').removeData('versionControlInformation').removeData('revision').text('');
    };

    /**
     * Reset the import flow version dialog.
     */
    var resetImportFlowVersionDialog = function () {
        $('#import-flow-version-registry-combo').combo('destroy').hide();
        $('#import-flow-version-bucket-combo').combo('destroy').hide();
        $('#import-flow-version-name-combo').combo('destroy').hide();

        $('#import-flow-version-registry').text('').hide();
        $('#import-flow-version-bucket').text('').hide();
        $('#import-flow-version-name').text('').hide();

        var importFlowVersionGrid = $('#import-flow-version-table').data('gridInstance');
        if (nfCommon.isDefinedAndNotNull(importFlowVersionGrid)) {
            var importFlowVersionData = importFlowVersionGrid.getData();
            importFlowVersionData.setItems([]);
        }

        $('#import-flow-version-process-group-id').removeData('versionControlInformation').removeData('revision').text('');
    };

    /**
     * Loads the registries into the specified registry combo.
     *
     * @param dialog
     * @param registryCombo
     * @param bucketCombo
     * @returns {deferred}
     */
    var loadRegistries = function (dialog, registryCombo, bucketCombo, selectBucket) {
        return $.ajax({
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
            registryCombo.combo({
                options: registries,
                select: function (selectedOption) {
                    selectRegistry(dialog, selectedOption, bucketCombo, selectBucket)
                }
            });
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Loads the buckets for the specified registryIdentifier for the current user.
     *
     * @param registryIdentifier
     * @param bucketCombo
     * @returns {*}
     */
    var loadBuckets = function (registryIdentifier, bucketCombo, selectBucket) {
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
            bucketCombo.combo('destroy').combo({
                options: buckets,
                select: selectBucket
            });
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Select handler for the registries combo.
     *
     * @param dialog
     * @param selectedOption
     * @param bucketCombo
     * @param selectBucket
     */
    var selectRegistry = function (dialog, selectedOption, bucketCombo, selectBucket) {
        var showNoBucketsAvailable = function () {
            bucketCombo.combo('destroy').combo({
                options: [{
                    text: 'No available buckets',
                    value: null,
                    optionClass: 'unset',
                    disabled: true
                }]
            });

            dialog.modal('refreshButtons');
        };

        if (selectedOption.disabled === true) {
            showNoBucketsAvailable();
        } else {
            loadBuckets(selectedOption.value, bucketCombo, selectBucket).fail(function () {
                showNoBucketsAvailable();
            });
        }
    };

    /**
     * Select handler for the buckets combo.
     *
     * @param selectedOption
     */
    var selectBucketSaveFlowVersion = function (selectedOption) {
        $('#save-flow-version-dialog').modal('refreshButtons');
    };

    /**
     * Saves a flow version.
     *
     * @returns {*}
     */
    var saveFlowVersion = function () {
        var processGroupId = $('#save-flow-version-process-group-id').text();
        var processGroupRevision = $('#save-flow-version-process-group-id').data('revision');

        var saveFlowVersionRequest = {
            processGroupRevision: nfClient.getRevision({
                revision: {
                    version: processGroupRevision.version
                }
            })
        };

        var versionControlInformation = $('#save-flow-version-process-group-id').data('versionControlInformation');
        if (nfCommon.isDefinedAndNotNull(versionControlInformation)) {
            saveFlowVersionRequest['versionedFlow'] = {
                registryId: versionControlInformation.registryId,
                bucketId: versionControlInformation.bucketId,
                flowId: versionControlInformation.flowId,
                flowName: $('#save-flow-version-name').text(),
                description: $('#save-flow-version-description').text(),
                comments: $('#save-flow-version-change-comments').val()
            }
        } else {
            var selectedRegistry =  $('#save-flow-version-registry-combo').combo('getSelectedOption');
            var selectedBucket =  $('#save-flow-version-bucket-combo').combo('getSelectedOption');

            saveFlowVersionRequest['versionedFlow'] = {
                registryId: selectedRegistry.value,
                bucketId: selectedBucket.value,
                flowName: $('#save-flow-version-name-field').val(),
                description: $('#save-flow-version-description-field').val(),
                comments: $('#save-flow-version-change-comments').val()
            };
        }

        return $.ajax({
            type: 'POST',
            data: JSON.stringify(saveFlowVersionRequest),
            url: '../nifi-api/versions/process-groups/' + encodeURIComponent(processGroupId),
            dataType: 'json',
            contentType: 'application/json'
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Sorts the specified data using the specified sort details.
     *
     * @param {object} sortDetails
     * @param {object} data
     */
    var sort = function (sortDetails, data) {
        // defines a function for sorting
        var comparer = function (a, b) {
            var aString = nfCommon.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
            var bString = nfCommon.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
            return aString === bString ? 0 : aString > bString ? 1 : -1;
        };

        // perform the sort
        data.sort(comparer, sortDetails.sortAsc);
    };

    var initImportFlowVersionTable = function () {
        var importFlowVersionTable = $('#import-flow-version-table');

        var valueFormatter = function (row, cell, value, columnDef, dataContext) {
            return nfCommon.escapeHtml(value);
        };

        var timestampFormatter = function (row, cell, value, columnDef, dataContext) {
            // get the current user time to properly convert the server time
            var now = new Date();

            // convert the user offset to millis
            var userTimeOffset = now.getTimezoneOffset() * 60 * 1000;

            // create the proper date by adjusting by the offsets
            var date = new Date(dataContext.timestamp + userTimeOffset + serverTimeOffset);
            return nfCommon.formatDateTime(date);
        };

        // define the column model for the controller services table
        var importFlowVersionColumns = [
            {
                id: 'version',
                name: 'Version',
                field: 'version',
                formatter: valueFormatter,
                sortable: true,
                resizable: true,
                width: 75,
                maxWidth: 75
            },
            {
                id: 'timestamp',
                name: 'Created',
                field: 'timestamp',
                formatter: timestampFormatter,
                sortable: true,
                resizable: true,
                width: 175,
                maxWidth: 175
            },
            {
                id: 'changeComments',
                name: 'Comments',
                field: 'comments',
                sortable: true,
                resizable: true,
                formatter: valueFormatter
            }
        ];

        // initialize the dataview
        var importFlowVersionData = new Slick.Data.DataView({
            inlineFilters: false
        });

        // initialize the sort
        sort({
            columnId: 'version',
            sortAsc: false
        }, importFlowVersionData);

        // initialize the grid
        var importFlowVersionGrid = new Slick.Grid(importFlowVersionTable, importFlowVersionData, importFlowVersionColumns, gridOptions);
        importFlowVersionGrid.setSelectionModel(new Slick.RowSelectionModel());
        importFlowVersionGrid.registerPlugin(new Slick.AutoTooltips());
        importFlowVersionGrid.setSortColumn('version', false);
        importFlowVersionGrid.onSort.subscribe(function (e, args) {
            sort({
                columnId: args.sortCol.id,
                sortAsc: args.sortAsc
            }, importFlowVersionData);
        });
        importFlowVersionGrid.onSelectedRowsChanged.subscribe(function (e, args) {
            $('#import-flow-version-dialog').modal('refreshButtons');
        });
        importFlowVersionGrid.onDblClick.subscribe(function (e, args) {
            changeFlowVersion().done(function () {
                $('#import-flow-version-dialog').modal('hide');
            });
        });

        // wire up the dataview to the grid
        importFlowVersionData.onRowCountChanged.subscribe(function (e, args) {
            importFlowVersionGrid.updateRowCount();
            importFlowVersionGrid.render();
        });
        importFlowVersionData.onRowsChanged.subscribe(function (e, args) {
            importFlowVersionGrid.invalidateRows(args.rows);
            importFlowVersionGrid.render();
        });
        importFlowVersionData.syncGridSelection(importFlowVersionGrid, true);

        // hold onto an instance of the grid
        importFlowVersionTable.data('gridInstance', importFlowVersionGrid);
    };

    /**
     * Shows the import flow version dialog.
     */
    var showImportFlowVersionDialog = function () {
        var pt = $('#new-process-group-dialog').data('pt');

        // update the registry and bucket visibility
        var registryCombo = $('#import-flow-version-registry-combo').show();
        var bucketCombo = $('#import-flow-version-bucket-combo').show();
        $('#import-flow-version-name-combo').show();

        loadRegistries($('#import-flow-version-dialog'), registryCombo, bucketCombo, selectBucketImportVersion).done(function () {
            // reposition the version table
            $('#import-flow-version-table').css({
                'top': '202px',
                'height': '225px'
            });

            // show the import dialog
            $('#import-flow-version-dialog').modal('setHeaderText', 'Import Version').modal('setButtonModel', [{
                buttonText: 'Import',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                disabled: disableImportOrChangeButton,
                handler: {
                    click: function () {
                        importFlowVersion(pt).always(function (response) {
                            // close the dialog
                            $('#import-flow-version-dialog').modal('hide');
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

            // hide the new process group dialog
            $('#new-process-group-dialog').modal('hide');
        });
    };

    /**
     * Loads the flow versions for the specified registry, bucket, and flow.
     *
     * @param registryIdentifier
     * @param bucketIdentifier
     * @param flowIdentifier
     * @returns deferred
     */
    var loadFlowVersions = function (registryIdentifier, bucketIdentifier, flowIdentifier) {
        var importFlowVersionGrid = $('#import-flow-version-table').data('gridInstance');
        var importFlowVersionData = importFlowVersionGrid.getData();

        // begin the update
        importFlowVersionData.beginUpdate();

        // remove the current versions
        importFlowVersionGrid.setSelectedRows([]);
        importFlowVersionGrid.resetActiveCell();
        importFlowVersionData.setItems([]);

        return $.ajax({
            type: 'GET',
            url: '../nifi-api/flow/registries/' + encodeURIComponent(registryIdentifier) + '/buckets/' + encodeURIComponent(bucketIdentifier) + '/flows/' + encodeURIComponent(flowIdentifier) + '/versions',
            dataType: 'json'
        }).done(function (response) {
            if (nfCommon.isDefinedAndNotNull(response.versionedFlowSnapshotMetadataSet) && response.versionedFlowSnapshotMetadataSet.length > 0) {
                $.each(response.versionedFlowSnapshotMetadataSet, function (_, entity) {
                    importFlowVersionData.addItem($.extend({
                        id: entity.versionedFlowSnapshotMetadata.version
                    }, entity.versionedFlowSnapshotMetadata));
                });
            } else {
                nfDialog.showOkDialog({
                    headerText: 'Flow Versions',
                    dialogContent: 'This flow does not have any versions available.'
                });
            }
        }).fail(nfErrorHandler.handleAjaxError).always(function () {
            // end the update
            importFlowVersionData.endUpdate();

            // resort
            importFlowVersionData.reSort();
            importFlowVersionGrid.invalidate();
        });
    };

    /**
     * Loads the versioned flows from the specified registry and bucket.
     *
     * @param registryIdentifier
     * @param bucketIdentifier
     * @param selectFlow
     * @returns deferred
     */
    var loadFlows = function (registryIdentifier, bucketIdentifier, selectFlow) {
        return $.ajax({
            type: 'GET',
            url: '../nifi-api/flow/registries/' + encodeURIComponent(registryIdentifier) + '/buckets/' + encodeURIComponent(bucketIdentifier) + '/flows',
            dataType: 'json'
        }).done(function (response) {
            var versionedFlows = [];

            if (nfCommon.isDefinedAndNotNull(response.versionedFlows) && response.versionedFlows.length > 0) {
                response.versionedFlows.sort(function (a, b) {
                    return a.versionedFlow.flowName > b.versionedFlow.flowName;
                });

                $.each(response.versionedFlows, function (_, versionedFlowEntity) {
                    var versionedFlow = versionedFlowEntity.versionedFlow;
                    versionedFlows.push({
                        text: versionedFlow.flowName,
                        value: versionedFlow.flowId,
                        description: nfCommon.escapeHtml(versionedFlow.description)
                    });
                });
            } else {
                versionedFlows.push({
                    text: 'No available flows',
                    value: null,
                    optionClass: 'unset',
                    disabled: true
                });
            }

            // load the buckets
            $('#import-flow-version-name-combo').combo('destroy').combo({
                options: versionedFlows,
                select: function (selectedFlow) {
                    if (nfCommon.isDefinedAndNotNull(selectedFlow.value)) {
                        selectFlow(registryIdentifier, bucketIdentifier, selectedFlow.value)
                    } else {
                        var importFlowVersionGrid = $('#import-flow-version-table').data('gridInstance');
                        var importFlowVersionData = importFlowVersionGrid.getData();

                        // clear the current values
                        importFlowVersionData.beginUpdate();
                        importFlowVersionData.setItems([]);
                        importFlowVersionData.endUpdate();
                    }
                }
            });
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Handler when a versioned flow is selected.
     *
     * @param registryIdentifier
     * @param bucketIdentifier
     * @param flowIdentifier
     */
    var selectVersionedFlow = function (registryIdentifier, bucketIdentifier, flowIdentifier) {
        loadFlowVersions(registryIdentifier, bucketIdentifier, flowIdentifier).done(function () {
            $('#import-flow-version-dialog').modal('refreshButtons');
        });
    };

    /**
     * Handler when a bucket is selected.
     *
     * @param selectedBucket
     */
    var selectBucketImportVersion = function (selectedBucket) {
        var selectedRegistry = $('#import-flow-version-registry-combo').combo('getSelectedOption');

        // load the flows for the currently selected registry and bucket
        loadFlows(selectedRegistry.value, selectedBucket.value, selectVersionedFlow);
    };

    /**
     * Imports the selected flow version.
     */
    var importFlowVersion = function (pt) {
        var selectedRegistry =  $('#import-flow-version-registry-combo').combo('getSelectedOption');
        var selectedBucket =  $('#import-flow-version-bucket-combo').combo('getSelectedOption');
        var selectedFlow =  $('#import-flow-version-name-combo').combo('getSelectedOption');

        var importFlowVersionGrid = $('#import-flow-version-table').data('gridInstance');
        var selectedVersionIndex = importFlowVersionGrid.getSelectedRows();
        var selectedVersion = importFlowVersionGrid.getDataItem(selectedVersionIndex[0]);

        var processGroupEntity = {
            'revision': nfClient.getRevision({
                'revision': {
                    'version': 0
                }
            }),
            'component': {
                'name': selectedFlow.text, // TODO - name from versioned PG?
                'position': {
                    'x': pt.x,
                    'y': pt.y
                },
                'versionControlInformation': {
                    'registryId': selectedRegistry.value,
                    'bucketId': selectedBucket.value,
                    'flowId': selectedFlow.value,
                    'version': selectedVersion.version
                }
            }
        };

        return $.ajax({
            type: 'POST',
            data: JSON.stringify(processGroupEntity),
            url: '../nifi-api/process-groups/' + encodeURIComponent(nfCanvasUtils.getGroupId()) + '/process-groups',
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (response) {
            // add the process group to the graph
            nfGraph.add({
                'processGroups': [response]
            }, {
                'selectAll': true
            });

            // update component visibility
            nfGraph.updateVisibility();

            // update the birdseye
            nfBirdseye.refresh();
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Determines whether the import/change button is disabled.
     *
     * @returns {boolean}
     */
    var disableImportOrChangeButton = function () {
        var importFlowVersionGrid = $('#import-flow-version-table').data('gridInstance');
        if (nfCommon.isDefinedAndNotNull(importFlowVersionGrid)) {
            var selected = importFlowVersionGrid.getSelectedRows();
            return selected.length !== 1;
        } else {
            return true;
        }
    };

    /**
     * Changes the flow version for the currently selected Process Group.
     *
     * @returns {deferred}
     */
    var changeFlowVersion = function () {
        var changeTimer = null;

        var processGroupId = $('#import-flow-version-process-group-id').text();
        var processGroupRevision = $('#import-flow-version-process-group-id').data('revision');
        var versionControlInformation = $('#import-flow-version-process-group-id').data('versionControlInformation');

        var importFlowVersionGrid = $('#import-flow-version-table').data('gridInstance');
        var selectedVersionIndex = importFlowVersionGrid.getSelectedRows();
        var selectedVersion = importFlowVersionGrid.getDataItem(selectedVersionIndex[0]);

        // TODO - introduce dialog to show current state with option to cancel once available

        var submitChangeRequest = function () {
            var changeVersionRequest = {
                'processGroupRevision': nfClient.getRevision({
                    'revision': {
                        'version': processGroupRevision.version
                    }
                }),
                'versionControlInformation': {
                    'groupId': processGroupId,
                    'registryId': versionControlInformation.registryId,
                    'bucketId': versionControlInformation.bucketId,
                    'flowId': versionControlInformation.flowId,
                    'version': selectedVersion.version
                }
            };

            return $.ajax({
                type: 'POST',
                data: JSON.stringify(changeVersionRequest),
                url: '../nifi-api/versions/update-requests/process-groups/' + encodeURIComponent(processGroupId),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                console.log(response);
            }).fail(nfErrorHandler.handleAjaxError);
        };

        var pollChangeRequest = function (changeRequest) {
            getChangeRequest(changeRequest).done(function (response) {
                processChangeResponse(response);
            })
        };

        var getChangeRequest = function (changeRequest) {
            return $.ajax({
                type: 'GET',
                url: changeRequest.uri,
                dataType: 'json'
            }).fail(nfErrorHandler.handleAjaxError);
        };

        var deleteChangeRequest = function (changeRequest) {
            var deleteXhr = $.ajax({
                type: 'DELETE',
                url: changeRequest.uri,
                dataType: 'json'
            }).fail(nfErrorHandler.handleAjaxError);

            updateProcessGroup(processGroupId);

            nfDialog.showOkDialog({
                headerText: 'Change Version',
                dialogContent: 'This Process Group version has changed.'
            });

            return deleteXhr;
        };

        var processChangeResponse = function (response) {
            var changeRequest = response.request;

            if (nfCommon.isDefinedAndNotNull(changeRequest.failureReason)) {
                nfDialog.showOkDialog({
                    headerText: 'Change Version',
                    dialogContent: nfCommon.escapeHtml(changeRequest.failureReason)
                });
            }

            if (changeRequest.complete === true) {
                deleteChangeRequest(changeRequest);
            } else {
                changeTimer = setTimeout(function () {
                    // clear the timer since we've been invoked
                    changeTimer = null;

                    // poll revert request
                    pollChangeRequest(changeRequest);
                }, 2000);
            }
        };

        submitChangeRequest().done(function (response) {
            processChangeResponse(response);
        });

    };

    /**
     * Gets the version control information for the specified process group id.
     *
     * @param processGroupId
     * @return {deferred}
     */
    var getVersionControlInformation = function (processGroupId) {
        return $.Deferred(function (deferred) {
            if (processGroupId === nfCanvasUtils.getGroupId()) {
                $.ajax({
                    type: 'GET',
                    url: '../nifi-api/versions/process-groups/' + encodeURIComponent(processGroupId),
                    dataType: 'json'
                }).done(function (response) {
                    deferred.resolve(response);
                }).fail(function () {
                    deferred.reject();
                });
            } else {
                var processGroup = nfProcessGroup.get(processGroupId);
                if (processGroup.permissions.canRead === true && processGroup.permissions.canWrite === true) {
                    deferred.resolve({
                        'processGroupRevision': processGroup.revision,
                        'versionControlInformation': processGroup.component.versionControlInformation
                    });
                } else {
                    deferred.reject();
                }
            }
        }).promise();
    };

    /**
     * Updates the specified process group with the specified version control information.
     *
     * @param processGroupId
     * @param versionControlInformation
     */
    var updateVersionControlInformation = function (processGroupId, versionControlInformation) {
        // refresh either selected PG or bread crumb to reflect connected/tracking status
        if (nfCanvasUtils.getGroupId() === processGroupId) {
            nfNgBridge.injector.get('breadcrumbsCtrl').updateVersionControlInformation(processGroupId, versionControlInformation);
            nfNgBridge.digest();
        } else {
            nfProcessGroup.reload(processGroupId);
        }
    };

    /**
     * Updates the specified process group following an operation that may change it's contents.
     *
     * @param processGroupId
     */
    var updateProcessGroup = function (processGroupId) {
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
    };

    return {
        init: function (timeOffset) {
            serverTimeOffset = timeOffset;

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
                        if ($('#save-flow-version-registry-combo').is(':visible')) {
                            var selectedRegistry =  $('#save-flow-version-registry-combo').combo('getSelectedOption');
                            var selectedBucket =  $('#save-flow-version-bucket-combo').combo('getSelectedOption');

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
                            var processGroupId = $('#save-flow-version-process-group-id').text();
                            
                            saveFlowVersion().done(function (response) {
                                updateVersionControlInformation(processGroupId, response.versionControlInformation);

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
                        resetSaveFlowVersionDialog();
                    }
                }
            });

            // initialize the import flow version dialog
            $('#import-flow-version-dialog').modal({
                scrollableContentStyle: 'scrollable',
                handler: {
                    close: function () {
                        resetImportFlowVersionDialog();
                    }
                }
            });

            // handle the click for the process group import
            $('#import-process-group-link').on('click', function() {
                showImportFlowVersionDialog();
            });

            // initialize the import flow version table
            initImportFlowVersionTable();
        },

        /**
         * Shows the flow version dialog.
         *
         * @param processGroupId
         */
        showFlowVersionDialog: function (processGroupId) {
            var focusName = true;

            return $.Deferred(function (deferred) {
                getVersionControlInformation(processGroupId).done(function (groupVersionControlInformation) {
                    // record the revision
                    $('#save-flow-version-process-group-id').data('revision', groupVersionControlInformation.processGroupRevision).text(processGroupId);

                    if (nfCommon.isDefinedAndNotNull(groupVersionControlInformation.versionControlInformation)) {
                        var versionControlInformation = groupVersionControlInformation.versionControlInformation;

                        // update the registry and bucket visibility
                        $('#save-flow-version-registry').text(versionControlInformation.registryId).show();
                        $('#save-flow-version-bucket').text(versionControlInformation.bucketId).show();

                        $('#save-flow-version-name').text(versionControlInformation.flowName).show();
                        $('#save-flow-version-description').text('Flow description goes here').show();

                        // record the versionControlInformation
                        $('#save-flow-version-process-group-id').data('versionControlInformation', versionControlInformation);

                        focusName = false;
                        deferred.resolve();
                    } else {
                        // update the registry and bucket visibility
                        $('#save-flow-version-registry-combo').show();
                        $('#save-flow-version-bucket-combo').show();

                        $('#save-flow-version-name-field').show();
                        $('#save-flow-version-description-field').show();

                        loadRegistries($('#save-flow-version-dialog'), $('#save-flow-version-registry-combo'), $('#save-flow-version-bucket-combo'), selectBucketSaveFlowVersion).done(function () {
                            deferred.resolve();
                        }).fail(function () {
                            deferred.reject();
                        });
                    }
                }).fail(nfErrorHandler.handleAjaxError);
            }).done(function () {
                $('#save-flow-version-dialog').modal('show');

                if (focusName) {
                    $('#save-flow-version-name-field').focus();
                } else {
                    $('#save-flow-version-change-comments').focus();
                }
            }).fail(function () {
                $('#save-flow-version-dialog').modal('refreshButtons');
            }).promise();
        },

        /**
         * Reverts local changes for the specified Process Group.
         *
         * @param processGroupId
         */
        revertLocalChanges: function (processGroupId) {
            // TODO update to show user the ramifications of reverting for confirmation

            // prompt the user before reverting
            nfDialog.showYesNoDialog({
                headerText: 'Revert Changes',
                dialogContent: 'Are you sure you want to revert changes? All flow configuration changes will be reverted to the last version.',
                noText: 'Cancel',
                yesText: 'Revert',
                yesHandler: function () {
                    getVersionControlInformation(processGroupId).done(function (response) {
                        if (nfCommon.isDefinedAndNotNull(response.versionControlInformation)) {
                            var revertTimer = null;

                            // TODO - introduce dialog to show current state once available

                            var submitRevertRequest = function () {
                                var revertFlowVersionRequest = {
                                    'processGroupRevision': nfClient.getRevision({
                                        'revision': {
                                            'version': response.processGroupRevision.version
                                        }
                                    }),
                                    'versionControlInformation': response.versionControlInformation
                                };

                                return $.ajax({
                                    type: 'POST',
                                    data: JSON.stringify(revertFlowVersionRequest),
                                    url: '../nifi-api/versions/revert-requests/process-groups/' + encodeURIComponent(processGroupId),
                                    dataType: 'json',
                                    contentType: 'application/json'
                                }).fail(nfErrorHandler.handleAjaxError);
                            };

                            var pollRevertRequest = function (revertRequest) {
                                getRevertRequest(revertRequest).done(function (response) {
                                    processRevertResponse(response);
                                })
                            };

                            var getRevertRequest = function (revertRequest) {
                                return $.ajax({
                                    type: 'GET',
                                    url: revertRequest.uri,
                                    dataType: 'json'
                                }).fail(nfErrorHandler.handleAjaxError);
                            };

                            var deleteRevertRequest = function (revertRequest) {
                                var deleteXhr = $.ajax({
                                    type: 'DELETE',
                                    url: revertRequest.uri,
                                    dataType: 'json'
                                }).fail(nfErrorHandler.handleAjaxError);

                                updateProcessGroup(processGroupId);

                                nfDialog.showOkDialog({
                                    headerText: 'Revert Changes',
                                    dialogContent: 'This Process Group has been reverted.'
                                });

                                return deleteXhr;
                            };

                            var processRevertResponse = function (response) {
                                var revertRequest = response.request;

                                if (nfCommon.isDefinedAndNotNull(revertRequest.failureReason)) {
                                    nfDialog.showOkDialog({
                                        headerText: 'Revert Changes',
                                        dialogContent: nfCommon.escapeHtml(revertRequest.failureReason)
                                    });
                                }

                                if (revertRequest.complete === true) {
                                    deleteRevertRequest(revertRequest);
                                } else {
                                    revertTimer = setTimeout(function () {
                                        // clear the timer since we've been invoked
                                        revertTimer = null;

                                        // poll revert request
                                        pollRevertRequest(revertRequest);
                                    }, 2000);
                                }
                            };

                            submitRevertRequest().done(function (response) {
                                processRevertResponse(response);
                            });
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
         * Shows the change flow version dialog.
         *
         * @param processGroupId
         */
        showChangeFlowVersionDialog: function (processGroupId) {
            return $.Deferred(function (deferred) {
                getVersionControlInformation(processGroupId).done(function (groupVersionControlInformation) {
                    if (nfCommon.isDefinedAndNotNull(groupVersionControlInformation.versionControlInformation)) {
                        var versionControlInformation = groupVersionControlInformation.versionControlInformation;

                        // update the registry and bucket visibility
                        $('#import-flow-version-registry').text(versionControlInformation.registryId).show();
                        $('#import-flow-version-bucket').text(versionControlInformation.bucketId).show();
                        $('#import-flow-version-name').text(versionControlInformation.flowId).show();

                        // record the versionControlInformation
                        $('#import-flow-version-process-group-id').data('versionControlInformation', versionControlInformation).data('revision', groupVersionControlInformation.processGroupRevision).text(processGroupId);

                        // load the flow versions
                        loadFlowVersions(versionControlInformation.registryId, versionControlInformation.bucketId, versionControlInformation.flowId).done(function () {
                            deferred.resolve();
                        }).fail(function () {
                            nfDialog.showOkDialog({
                                headerText: 'Change Version',
                                dialogContent: 'Unable to load available versions for this Process Group.'
                            });

                            deferred.reject();
                        });
                    } else {
                        nfDialog.showOkDialog({
                            headerText: 'Change Version',
                            dialogContent: 'This Process Group is not currently under version control.'
                        });

                        deferred.reject();
                    }
                }).fail(nfErrorHandler.handleAjaxError);
            }).done(function () {
                // reposition the version table
                $('#import-flow-version-table').css({
                    'top': '150px',
                    'height': '277px'
                });

                // show the dialog
                $('#import-flow-version-dialog').modal('setHeaderText', 'Change Version').modal('setButtonModel', [{
                    buttonText: 'Change',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    disabled: disableImportOrChangeButton,
                    handler: {
                        click: function () {
                            changeFlowVersion().done(function () {
                                $('#import-flow-version-dialog').modal('hide');
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
            }).promise();
        },

        /**
         * Stops version control for the specified Process Group.
         *
         * @param processGroupId
         */
        stopVersionControl: function (processGroupId) {
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
                                updateVersionControlInformation(processGroupId, undefined);

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