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
                'nf.Storage',
                'nf.Common',
                'nf.Client',
                'nf.CanvasUtils',
                'nf.ProcessGroup',
                'nf.ProcessGroupConfiguration',
                'nf.Graph',
                'nf.Birdseye'],
            function ($, nfNgBridge, nfErrorHandler, nfDialog, nfStorage, nfCommon, nfClient, nfCanvasUtils, nfProcessGroup, nfProcessGroupConfiguration, nfGraph, nfBirdseye) {
                return (nf.FlowVersion = factory($, nfNgBridge, nfErrorHandler, nfDialog, nfStorage, nfCommon, nfClient, nfCanvasUtils, nfProcessGroup, nfProcessGroupConfiguration, nfGraph, nfBirdseye));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.FlowVerison =
            factory(require('jquery'),
                require('nf.ng.Bridge'),
                require('nf.ErrorHandler'),
                require('nf.Dialog'),
                require('nf.Storage'),
                require('nf.Common'),
                require('nf.Client'),
                require('nf.CanvasUtils'),
                require('nf.ProcessGroup'),
                require('nf.ProcessGroupConfiguration'),
                require('nf.Graph'),
                require('nf.Birdseye')));
    } else {
        nf.FlowVersion = factory(root.$,
            root.nf.ng.Bridge,
            root.nf.ErrorHandler,
            root.nf.Dialog,
            root.nf.Storage,
            root.nf.Common,
            root.nf.Client,
            root.nf.CanvasUtils,
            root.nf.ProcessGroup,
            root.nf.ProcessGroupConfiguration,
            root.nf.Graph,
            root.nf.Birdseye);
    }
}(this, function ($, nfNgBridge, nfErrorHandler, nfDialog, nfStorage, nfCommon, nfClient, nfCanvasUtils, nfProcessGroup, nfProcessGroupConfiguration, nfGraph, nfBirdseye) {
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

        $('#save-flow-version-label').text('');

        $('#save-flow-version-registry').text('').hide();
        $('#save-flow-version-bucket').text('').hide();

        $('#save-flow-version-name').text('').hide();
        $('#save-flow-version-description').removeClass('unset blank').text('').hide();

        $('#save-flow-version-name-field').val('').hide();
        $('#save-flow-version-description-field').val('').hide();
        $('#save-flow-version-change-comments').val('');

        $('#save-flow-version-process-group-id').removeData('versionControlInformation').removeData('revision').text('');
    };

    /**
     * Reset the revert local changes dialog.
     */
    var resetRevertLocalChangesDialog = function () {
        $('#revert-local-changes-process-group-id').text('');

        clearLocalChangesGrid($('#revert-local-changes-table'), $('#revert-local-changes-filter'), $('#displayed-revert-local-changes-entries'), $('#total-revert-local-changes-entries'));
    };

    /**
     * Reset the show local changes dialog.
     */
    var resetShowLocalChangesDialog = function () {
        clearLocalChangesGrid($('#show-local-changes-table'), $('#show-local-changes-filter'), $('#displayed-show-local-changes-entries'), $('#total-show-local-changes-entries'));
    };

    /**
     * Clears the local changes grid.
     */
    var clearLocalChangesGrid = function (localChangesTable, filterInput, displayedLabel, totalLabel) {
        var localChangesGrid = localChangesTable.data('gridInstance');
        if (nfCommon.isDefinedAndNotNull(localChangesGrid)) {
            localChangesGrid.setSelectedRows([]);
            localChangesGrid.resetActiveCell();

            var localChangesData = localChangesGrid.getData();
            localChangesData.setItems([]);
            localChangesData.setFilterArgs({
                searchString: ''
            });
        }

        filterInput.val('');

        displayedLabel.text('0');
        totalLabel.text('0');
    };

    /**
     * Clears the version grid
     */
    var clearFlowVersionsGrid = function () {
        var importFlowVersionGrid = $('#import-flow-version-table').data('gridInstance');
        if (nfCommon.isDefinedAndNotNull(importFlowVersionGrid)) {
            importFlowVersionGrid.setSelectedRows([]);
            importFlowVersionGrid.resetActiveCell();

            var importFlowVersionData = importFlowVersionGrid.getData();
            importFlowVersionData.setItems([]);
        }
    };

    /**
     * Reset the import flow version dialog.
     */
    var resetImportFlowVersionDialog = function () {
        $('#import-flow-version-dialog').removeData('pt');

        $('#import-flow-version-registry-combo').combo('destroy').hide();
        $('#import-flow-version-bucket-combo').combo('destroy').hide();
        $('#import-flow-version-name-combo').combo('destroy').hide();

        $('#import-flow-version-registry').text('').hide();
        $('#import-flow-version-bucket').text('').hide();
        $('#import-flow-version-name').text('').hide();

        clearFlowVersionsGrid();

        $('#import-flow-version-process-group-id').removeData('versionControlInformation').removeData('revision').text('');

        $('#import-flow-version-container').hide();
        $('#import-flow-version-label').text('');
    };

    /**
     * Loads the registries into the specified registry combo.
     *
     * @param dialog
     * @param registryCombo
     * @param bucketCombo
     * @param flowCombo
     * @param selectBucket
     * @param bucketCheck
     * @returns {deferred}
     */
    var loadRegistries = function (dialog, registryCombo, bucketCombo, flowCombo, selectBucket, bucketCheck) {
        return $.ajax({
            type: 'GET',
            url: '../nifi-api/flow/registries',
            dataType: 'json'
        }).done(function (registriesResponse) {
            var registries = [];

            if (nfCommon.isDefinedAndNotNull(registriesResponse.registries) && registriesResponse.registries.length > 0) {
                registriesResponse.registries.sort(function (a, b) {
                    return a.registry.name > b.registry.name;
                });

                $.each(registriesResponse.registries, function (_, registryEntity) {
                    var registry = registryEntity.registry;
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
                    selectRegistry(dialog, selectedOption, bucketCombo, flowCombo, selectBucket, bucketCheck)
                }
            });
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Loads the buckets for the specified registryIdentifier for the current user.
     *
     * @param registryIdentifier
     * @param bucketCombo
     * @param flowCombo
     * @param selectBucket
     * @param bucketCheck
     * @returns {*}
     */
    var loadBuckets = function (registryIdentifier, bucketCombo, flowCombo, selectBucket, bucketCheck) {
        return $.ajax({
            type: 'GET',
            url: '../nifi-api/flow/registries/' + encodeURIComponent(registryIdentifier) + '/buckets',
            dataType: 'json'
        }).done(function (response) {
            var buckets = [];

            if (nfCommon.isDefinedAndNotNull(response.buckets) && response.buckets.length > 0) {
                response.buckets.sort(function (a, b) {
                    if (a.permissions.canRead === false && b.permissions.canRead === false) {
                        return 0;
                    } else if (a.permissions.canRead === false) {
                        return -1;
                    } else if (b.permissions.canRead === false) {
                        return 1;
                    }

                    return a.bucket.name > b.bucket.name;
                });

                $.each(response.buckets, function (_, bucketEntity) {
                    if (bucketEntity.permissions.canRead === true) {
                        var bucket = bucketEntity.bucket;

                        if (bucketCheck(bucketEntity)) {
                            buckets.push({
                                text: bucket.name,
                                value: bucket.id,
                                description: nfCommon.escapeHtml(bucket.description)
                            });
                        }
                    }
                });
            }

            if (buckets.length === 0) {
                buckets.push({
                    text: 'No available buckets',
                    value: null,
                    optionClass: 'unset',
                    disabled: true
                });

                if (nfCommon.isDefinedAndNotNull(flowCombo)) {
                    flowCombo.combo('destroy').combo({
                        options: [{
                            text: 'No available flows',
                            value: null,
                            optionClass: 'unset',
                            disabled: true
                        }]
                    });
                }
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
     * @param flowCombo
     * @param selectBucket
     * @param bucketCheck
     */
    var selectRegistry = function (dialog, selectedOption, bucketCombo, flowCombo, selectBucket, bucketCheck) {
        var showNoBucketsAvailable = function () {
            bucketCombo.combo('destroy').combo({
                options: [{
                    text: 'No available buckets',
                    value: null,
                    optionClass: 'unset',
                    disabled: true
                }]
            });

            if (nfCommon.isDefinedAndNotNull(flowCombo)) {
                flowCombo.combo('destroy').combo({
                    options: [{
                        text: 'No available flows',
                        value: null,
                        optionClass: 'unset',
                        disabled: true
                    }]
                });
            }

            dialog.modal('refreshButtons');
        };

        if (selectedOption.disabled === true) {
            showNoBucketsAvailable();
        } else {
            bucketCombo.combo('destroy').combo({
                options: [{
                    text: 'Loading buckets...',
                    value: null,
                    optionClass: 'unset',
                    disabled: true
                }]
            });

            if (nfCommon.isDefinedAndNotNull(flowCombo)) {
                flowCombo.combo('destroy').combo({
                    options: [{
                        text: 'Loading flows...',
                        value: null,
                        optionClass: 'unset',
                        disabled: true
                    }]
                });

                clearFlowVersionsGrid();
            }

            loadBuckets(selectedOption.value, bucketCombo, flowCombo, selectBucket, bucketCheck).fail(function () {
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
            }),
            'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
        };

        var versionControlInformation = $('#save-flow-version-process-group-id').data('versionControlInformation');
        if (nfCommon.isDefinedAndNotNull(versionControlInformation)) {
            saveFlowVersionRequest['versionedFlow'] = {
                registryId: versionControlInformation.registryId,
                bucketId: versionControlInformation.bucketId,
                flowId: versionControlInformation.flowId,
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
        }).done(function (response) {
            if ('SYNC_FAILURE' === response.versionControlInformation.state) {
                nfDialog.showOkDialog({
                    headerText: 'Error',
                    dialogContent: response.versionControlInformation.stateExplanation
                });
            }
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
            var aIsBlank = nfCommon.isBlank(a[sortDetails.columnId]);
            var bIsBlank = nfCommon.isBlank(b[sortDetails.columnId]);

            if (aIsBlank && bIsBlank) {
                return 0;
            } else if (aIsBlank) {
                return 1;
            } else if (bIsBlank) {
                return -1;
            }

            return a[sortDetails.columnId] === b[sortDetails.columnId] ? 0 : a[sortDetails.columnId] > b[sortDetails.columnId] ? 1 : -1;
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

        // define the column model for flow versions
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
            if ($('#import-flow-version-label').is(':visible')) {
                changeFlowVersion();
            } else {
                importFlowVersion().always(function () {
                    $('#import-flow-version-dialog').modal('hide');
                });
            }
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
     * Initializes the specified local changes table.
     *
     * @param localChangesTable
     * @param filterInput
     * @param displayedLabel
     * @param totalLabel
     */
    var initLocalChangesTable = function (localChangesTable, filterInput, displayedLabel, totalLabel) {

        var getFilterText = function () {
            return filterInput.val();
        };

        var applyFilter = function () {
            // get the dataview
            var localChangesGrid = localChangesTable.data('gridInstance');

            // ensure the grid has been initialized
            if (nfCommon.isDefinedAndNotNull(localChangesGrid)) {
                var localChangesData = localChangesGrid.getData();

                // update the search criteria
                localChangesData.setFilterArgs({
                    searchString: getFilterText()
                });
                localChangesData.refresh();
            }
        };

        var filter = function (item, args) {
            if (args.searchString === '') {
                return true;
            }

            try {
                // perform the row filtering
                var filterExp = new RegExp(args.searchString, 'i');
            } catch (e) {
                // invalid regex
                return false;
            }

            // determine if the item matches the filter
            var matchesId = item['componentId'].search(filterExp) >= 0;
            var matchesDifferenceType = item['differenceType'].search(filterExp) >= 0;
            var matchesDifference = item['difference'].search(filterExp) >= 0;

            // conditionally consider the component name
            var matchesComponentName = false;
            if (nfCommon.isDefinedAndNotNull(item['componentName'])) {
                matchesComponentName = item['componentName'].search(filterExp) >= 0;
            }

            return matchesId || matchesComponentName || matchesDifferenceType || matchesDifference;
        };

        // initialize the component state filter
        filterInput.on('keyup', function () {
            applyFilter();
        });

        var valueFormatter = function (row, cell, value, columnDef, dataContext) {
            return nfCommon.escapeHtml(value);
        };

        var actionsFormatter = function (row, cell, value, columnDef, dataContext) {
            var markup = '';

            if (dataContext.differenceType !== 'Component Removed' && nfCommon.isDefinedAndNotNull(dataContext.processGroupId)) {
                markup += '<div class="pointer go-to-component fa fa-long-arrow-right" title="Go To"></div>';
            }

            return markup;
        };

        // define the column model for local changes
        var localChangesColumns = [
            {
                id: 'componentName',
                name: 'Component Name',
                field: 'componentName',
                formatter: valueFormatter,
                sortable: true,
                resizable: true
            },
            {
                id: 'differenceType',
                name: 'Change Type',
                field: 'differenceType',
                formatter: valueFormatter,
                sortable: true,
                resizable: true
            },
            {
                id: 'difference',
                name: 'Difference',
                field: 'difference',
                formatter: valueFormatter,
                sortable: true,
                resizable: true
            },
            {
                id: 'actions',
                name: '&nbsp;',
                formatter: actionsFormatter,
                sortable: false,
                resizable: false,
                width: 25
            }
        ];

        // initialize the dataview
        var localChangesData = new Slick.Data.DataView({
            inlineFilters: false
        });
        localChangesData.setFilterArgs({
            searchString: getFilterText()
        });
        localChangesData.setFilter(filter);

        // initialize the sort
        sort({
            columnId: 'componentName',
            sortAsc: true
        }, localChangesData);

        // initialize the grid
        var localChangesGrid = new Slick.Grid(localChangesTable, localChangesData, localChangesColumns, gridOptions);
        localChangesGrid.setSelectionModel(new Slick.RowSelectionModel());
        localChangesGrid.registerPlugin(new Slick.AutoTooltips());
        localChangesGrid.setSortColumn('componentName', true);
        localChangesGrid.onSort.subscribe(function (e, args) {
            sort({
                columnId: args.sortCol.id,
                sortAsc: args.sortAsc
            }, localChangesData);
        });

        // configure a click listener
        localChangesGrid.onClick.subscribe(function (e, args) {
            var target = $(e.target);

            // get the node at this row
            var componentDifference = localChangesData.getItem(args.row);

            // determine the desired action
            if (localChangesGrid.getColumns()[args.cell].id === 'actions') {
                if (target.hasClass('go-to-component')) {
                    if (componentDifference.componentType === 'Controller Service') {
                        nfProcessGroupConfiguration.showConfiguration(componentDifference.processGroupId).done(function () {
                            nfProcessGroupConfiguration.selectControllerService(componentDifference.componentId);

                            localChangesTable.closest('.large-dialog').modal('hide');
                        });
                    } else {
                        nfCanvasUtils.showComponent(componentDifference.processGroupId, componentDifference.componentId).done(function () {
                            localChangesTable.closest('.large-dialog').modal('hide');
                        });
                    }
                }
            }
        });

        // wire up the dataview to the grid
        localChangesData.onRowCountChanged.subscribe(function (e, args) {
            localChangesGrid.updateRowCount();
            localChangesGrid.render();

            // update the total number of displayed items
            displayedLabel.text(nfCommon.formatInteger(args.current));
        });
        localChangesData.onRowsChanged.subscribe(function (e, args) {
            localChangesGrid.invalidateRows(args.rows);
            localChangesGrid.render();
        });
        localChangesData.syncGridSelection(localChangesGrid, true);

        // hold onto an instance of the grid
        localChangesTable.data('gridInstance', localChangesGrid);

        // initialize the number of display items
        displayedLabel.text('0');
        totalLabel.text('0');
    };

    /**
     * Shows the import flow version dialog.
     */
    var showImportFlowVersionDialog = function () {
        var pt = $('#new-process-group-dialog').data('pt');
        $('#import-flow-version-dialog').data('pt', pt);

        // update the registry and bucket visibility
        var registryCombo = $('#import-flow-version-registry-combo').combo('destroy').combo({
            options: [{
                text: 'Loading registries...',
                value: null,
                optionClass: 'unset',
                disabled: true
            }]
        }).show();
        var bucketCombo = $('#import-flow-version-bucket-combo').combo('destroy').combo({
            options: [{
                text: 'Loading buckets...',
                value: null,
                optionClass: 'unset',
                disabled: true
            }]
        }).show();
        var flowCombo = $('#import-flow-version-name-combo').combo('destroy').combo({
            options: [{
                text: 'Loading flows...',
                value: null,
                optionClass: 'unset',
                disabled: true
            }]
        }).show();

        loadRegistries($('#import-flow-version-dialog'), registryCombo, bucketCombo, flowCombo, selectBucketImportVersion, function (bucketEntity) {
            return true;
        }).done(function () {
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
                        importFlowVersion().always(function () {
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
        // clear the flow versions grid
        clearFlowVersionsGrid();

        if (nfCommon.isDefinedAndNotNull(selectedBucket.value)) {
            // mark the flows as loading
            $('#import-flow-version-name-combo').combo('destroy').combo({
                options: [{
                    text: 'Loading flows...',
                    value: null,
                    optionClass: 'unset',
                    disabled: true
                }]
            });

            var selectedRegistry = $('#import-flow-version-registry-combo').combo('getSelectedOption');

            // load the flows for the currently selected registry and bucket
            loadFlows(selectedRegistry.value, selectedBucket.value, selectVersionedFlow);
        } else {
            // mark no flows available
            $('#import-flow-version-name-combo').combo('destroy').combo({
                options: [{
                    text: 'No available flows',
                    value: null,
                    optionClass: 'unset',
                    disabled: true
                }]
            });
        }
    };

    /**
     * Imports the selected flow version.
     */
    var importFlowVersion = function () {
        var pt = $('#import-flow-version-dialog').data('pt');

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
            'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
            'component': {
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

            // if the version label is visible, this is a change version request so disable when
            // the version that represents the current version is selected
            if ($('#import-flow-version-label').is(':visible')) {
                if (selected.length === 1) {
                    var selectedFlow = importFlowVersionGrid.getDataItem(selected[0]);

                    var currentVersion = parseInt($('#import-flow-version-label').text(), 10);
                    return currentVersion === selectedFlow.version;
                } else {
                    return true;
                }
            } else {
                // if importing, enable when a single row is selecting
                return selected.length !== 1;
            }
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
        var changeRequest = null;
        var cancelled = false;

        var processGroupId = $('#import-flow-version-process-group-id').text();
        var processGroupRevision = $('#import-flow-version-process-group-id').data('revision');
        var versionControlInformation = $('#import-flow-version-process-group-id').data('versionControlInformation');

        var importFlowVersionGrid = $('#import-flow-version-table').data('gridInstance');
        var selectedVersionIndex = importFlowVersionGrid.getSelectedRows();
        var selectedVersion = importFlowVersionGrid.getDataItem(selectedVersionIndex[0]);

        // update the button model of the change version status dialog
        $('#change-version-status-dialog').modal('setButtonModel', [{
            buttonText: 'Stop',
            color: {
                base: '#728E9B',
                hover: '#004849',
                text: '#ffffff'
            },
            handler: {
                click: function () {
                    cancelled = true;

                    $('#change-version-status-dialog').modal('setButtonModel', []);

                    // we are waiting for the next poll attempt
                    if (changeTimer !== null) {
                        // cancel it
                        clearTimeout(changeTimer);

                        // cancel the change request
                        completeChangeRequest();
                    }
                }
            }
        }]);

        // hide the import dialog immediately
        $('#import-flow-version-dialog').modal('hide');

        var submitChangeRequest = function () {
            var changeVersionRequest = {
                'processGroupRevision': nfClient.getRevision({
                    'revision': {
                        'version': processGroupRevision.version
                    }
                }),
                'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
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
            }).done(function () {
                // initialize the progress bar value
                updateProgress(0);

                // show the progress dialog
                $('#change-version-status-dialog').modal('show');
            }).fail(nfErrorHandler.handleAjaxError);
        };

        var pollChangeRequest = function () {
            getChangeRequest().done(processChangeResponse);
        };

        var getChangeRequest = function () {
            return $.ajax({
                type: 'GET',
                url: changeRequest.uri,
                dataType: 'json'
            }).fail(completeChangeRequest).fail(nfErrorHandler.handleAjaxError);
        };

        var completeChangeRequest = function () {
            if (cancelled === true) {
                // update the message to indicate successful completion
                $('#change-version-status-message').text('The change version request has been cancelled.');

                // update the button model
                $('#change-version-status-dialog').modal('setButtonModel', [{
                    buttonText: 'Close',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            $(this).modal('hide');
                        }
                    }
                }]);
            }

            if (nfCommon.isDefinedAndNotNull(changeRequest)) {
                $.ajax({
                    type: 'DELETE',
                    url: changeRequest.uri + '?' + $.param({
                        'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
                    }),
                    dataType: 'json'
                }).done(function (response) {
                    changeRequest = response.request;

                    // update the component that was changing
                    updateProcessGroup(processGroupId);

                    if (nfCommon.isDefinedAndNotNull(changeRequest.failureReason)) {
                        // hide the progress dialog
                        $('#change-version-status-dialog').modal('hide');

                        nfDialog.showOkDialog({
                            headerText: 'Change Version',
                            dialogContent: nfCommon.escapeHtml(changeRequest.failureReason)
                        });
                    } else {
                        // update the percent complete
                        updateProgress(changeRequest.percentCompleted);

                        // update the message to indicate successful completion
                        $('#change-version-status-message').text('This Process Group version has changed.');

                        // update the button model
                        $('#change-version-status-dialog').modal('setButtonModel', [{
                            buttonText: 'Close',
                            color: {
                                base: '#728E9B',
                                hover: '#004849',
                                text: '#ffffff'
                            },
                            handler: {
                                click: function () {
                                    $(this).modal('hide');
                                }
                            }
                        }]);
                    }
                });
            }
        };

        var processChangeResponse = function (response) {
            changeRequest = response.request;

            if (changeRequest.complete === true || cancelled === true) {
                completeChangeRequest();
            } else {
                // update the percent complete
                updateProgress(changeRequest.percentCompleted);

                // update the status of the listing request
                $('#change-version-status-message').text(changeRequest.state);

                changeTimer = setTimeout(function () {
                    // clear the timer since we've been invoked
                    changeTimer = null;

                    // poll revert request
                    pollChangeRequest();
                }, 2000);
            }
        };

        submitChangeRequest().done(processChangeResponse);
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
            // if reverting/changing current PG... reload/refresh this group/canvas

            $.ajax({
                type: 'GET',
                url: '../nifi-api/flow/process-groups/' + encodeURIComponent(processGroupId),
                dataType: 'json'
            }).done(function (response) {
                // update the graph components
                nfGraph.set(response.processGroupFlow.flow);

                // update the component visibility
                nfGraph.updateVisibility();

                // update the breadcrumbs
                var breadcrumbsCtrl = nfNgBridge.injector.get('breadcrumbsCtrl');
                breadcrumbsCtrl.resetBreadcrumbs();
                breadcrumbsCtrl.generateBreadcrumbs(response.processGroupFlow.breadcrumb);

                // inform Angular app values have changed
                nfNgBridge.digest();
            }).fail(nfErrorHandler.handleAjaxError);
        } else {
            // if reverting selected PG... reload selected PG to update counts, etc
            nfProcessGroup.reload(processGroupId);
        }
    };

    /**
     * Updates the progress bar to the specified percent complete.
     *
     * @param percentComplete
     */
    var updateProgress = function (percentComplete) {
        // remove existing labels
        var progressBar = $('#change-version-percent-complete');
        progressBar.find('div.progress-label').remove();
        progressBar.find('md-progress-linear').remove();

        // update the progress
        var label = $('<div class="progress-label"></div>').text(percentComplete + '%');
        (nfNgBridge.injector.get('$compile')($('<md-progress-linear ng-cloak ng-value="' + percentComplete + '" class="md-hue-2" md-mode="determinate" aria-label="Searching Queue"></md-progress-linear>'))(nfNgBridge.rootScope)).appendTo(progressBar);
        progressBar.append(label);
    };

    /**
     * Shows local changes for the specified process group.
     *
     * @param processGroupId
     * @param localChangesMessage
     * @param localChangesTable
     * @param totalLabel
     */
    var loadLocalChanges = function (processGroupId, localChangesMessage, localChangesTable, totalLabel) {
        var localChangesGrid = localChangesTable.data('gridInstance');
        var localChangesData = localChangesGrid.getData();

        // begin the update
        localChangesData.beginUpdate();

        // remove the current versions
        localChangesGrid.setSelectedRows([]);
        localChangesGrid.resetActiveCell();
        localChangesData.setItems([]);

        // load the necessary details
        var loadMessage = getVersionControlInformation(processGroupId).done(function (response) {
            if (nfCommon.isDefinedAndNotNull(response.versionControlInformation)) {
                var vci = response.versionControlInformation;
                localChangesMessage.text('The following changes have been made to ' + vci.flowName + ' (Version ' + vci.version + ').');
            } else {
                nfDialog.showOkDialog({
                    headerText: 'Change Version',
                    dialogContent: 'This Process Group is not currently under version control.'
                });
            }
        });
        var loadChanges = $.ajax({
            type: 'GET',
            url: '../nifi-api/process-groups/' + encodeURIComponent(processGroupId) + '/local-modifications',
            dataType: 'json'
        }).done(function (response) {
            if (nfCommon.isDefinedAndNotNull(response.componentDifferences) && response.componentDifferences.length > 0) {
                var totalDifferences = 0;
                $.each(response.componentDifferences, function (_, componentDifference) {
                    $.each(componentDifference.differences, function (_, difference) {
                        localChangesData.addItem({
                            id: totalDifferences++,
                            componentId: componentDifference.componentId,
                            componentName: componentDifference.componentName,
                            componentType: componentDifference.componentType,
                            processGroupId: componentDifference.processGroupId,
                            differenceType: difference.differenceType,
                            difference: difference.difference
                        });
                    });
                });

                // end the update
                localChangesData.endUpdate();

                // resort
                localChangesData.reSort();
                localChangesGrid.invalidate();

                // update the total displayed
                totalLabel.text(nfCommon.formatInteger(totalDifferences));
            } else {
                nfDialog.showOkDialog({
                    headerText: 'Local Changes',
                    dialogContent: 'This Process Group does not have any local changes.'
                });
            }
        }).fail(nfErrorHandler.handleAjaxError);

        return $.when(loadMessage, loadChanges);
    };

    /**
     * Revert local changes for the specified process group.
     *
     * @param processGroupId
     */
    var revertLocalChanges = function (processGroupId) {
        getVersionControlInformation(processGroupId).done(function (response) {
            if (nfCommon.isDefinedAndNotNull(response.versionControlInformation)) {
                var revertTimer = null;
                var revertRequest = null;
                var cancelled = false;

                // update the button model of the revert status dialog
                $('#change-version-status-dialog').modal('setButtonModel', [{
                    buttonText: 'Stop',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            cancelled = true;

                            $('#change-version-status-dialog').modal('setButtonModel', []);

                            // we are waiting for the next poll attempt
                            if (revertTimer !== null) {
                                // cancel it
                                clearTimeout(revertTimer);

                                // cancel the revert request
                                completeRevertRequest();
                            }
                        }
                    }
                }]);

                // hide the import dialog immediately
                $('#import-flow-version-dialog').modal('hide');

                var submitRevertRequest = function () {
                    var revertFlowVersionRequest = {
                        'processGroupRevision': nfClient.getRevision({
                            'revision': {
                                'version': response.processGroupRevision.version
                            }
                        }),
                        'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
                        'versionControlInformation': response.versionControlInformation
                    };

                    return $.ajax({
                        type: 'POST',
                        data: JSON.stringify(revertFlowVersionRequest),
                        url: '../nifi-api/versions/revert-requests/process-groups/' + encodeURIComponent(processGroupId),
                        dataType: 'json',
                        contentType: 'application/json'
                    }).done(function () {
                        // initialize the progress bar value
                        updateProgress(0);

                        // show the progress dialog
                        $('#change-version-status-dialog').modal('show');
                    }).fail(nfErrorHandler.handleAjaxError);
                };

                var pollRevertRequest = function () {
                    getRevertRequest().done(processRevertResponse);
                };

                var getRevertRequest = function () {
                    return $.ajax({
                        type: 'GET',
                        url: revertRequest.uri,
                        dataType: 'json'
                    }).fail(completeRevertRequest).fail(nfErrorHandler.handleAjaxError);
                };

                var completeRevertRequest = function () {
                    if (cancelled === true) {
                        // update the message to indicate successful completion
                        $('#change-version-status-message').text('The revert request has been cancelled.');

                        // update the button model
                        $('#change-version-status-dialog').modal('setButtonModel', [{
                            buttonText: 'Close',
                            color: {
                                base: '#728E9B',
                                hover: '#004849',
                                text: '#ffffff'
                            },
                            handler: {
                                click: function () {
                                    $(this).modal('hide');
                                }
                            }
                        }]);
                    }

                    if (nfCommon.isDefinedAndNotNull(revertRequest)) {
                        $.ajax({
                            type: 'DELETE',
                            url: revertRequest.uri + '?' + $.param({
                                'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
                            }),
                            dataType: 'json'
                        }).done(function (response) {
                            revertRequest = response.request;

                            // update the component that was changing
                            updateProcessGroup(processGroupId);

                            if (nfCommon.isDefinedAndNotNull(revertRequest.failureReason)) {
                                // hide the progress dialog
                                $('#change-version-status-dialog').modal('hide');

                                nfDialog.showOkDialog({
                                    headerText: 'Revert Local Changes',
                                    dialogContent: nfCommon.escapeHtml(revertRequest.failureReason)
                                });
                            } else {
                                // update the percent complete
                                updateProgress(revertRequest.percentCompleted);

                                // update the message to indicate successful completion
                                $('#change-version-status-message').text('This Process Group version has changed.');

                                // update the button model
                                $('#change-version-status-dialog').modal('setButtonModel', [{
                                    buttonText: 'Close',
                                    color: {
                                        base: '#728E9B',
                                        hover: '#004849',
                                        text: '#ffffff'
                                    },
                                    handler: {
                                        click: function () {
                                            $(this).modal('hide');
                                        }
                                    }
                                }]);
                            }
                        });
                    }
                };

                var processRevertResponse = function (response) {
                    revertRequest = response.request;

                    if (revertRequest.complete === true || cancelled === true) {
                        completeRevertRequest();
                    } else {
                        // update the percent complete
                        updateProgress(revertRequest.percentCompleted);

                        // update the status of the revert request
                        $('#change-version-status-message').text(revertRequest.state);

                        revertTimer = setTimeout(function () {
                            // clear the timer since we've been invoked
                            revertTimer = null;

                            // poll revert request
                            pollRevertRequest();
                        }, 2000);
                    }
                };

                submitRevertRequest().done(processRevertResponse);
            } else {
                nfDialog.showOkDialog({
                    headerText: 'Revert Changes',
                    dialogContent: 'This Process Group is not currently under version control.'
                });
            }
        }).fail(nfErrorHandler.handleAjaxError);
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
                            });

                            $(this).modal('hide');
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

            // configure the drop request status dialog
            $('#change-version-status-dialog').modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'Change Flow Version',
                handler: {
                    close: function () {
                        // clear the current button model
                        $('#change-version-status-dialog').modal('setButtonModel', []);
                    }
                }
            });

            // init the revert local changes dialog
            $('#revert-local-changes-dialog').modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'Revert Local Changes',
                buttons: [{
                    buttonText: 'Revert',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            var processGroupId = $('#revert-local-changes-process-group-id').text();
                            revertLocalChanges(processGroupId);

                            $(this).modal('hide');
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
                        resetRevertLocalChangesDialog();
                    }
                }
            });

            // init the show local changes dialog
            $('#show-local-changes-dialog').modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'Show Local Changes',
                buttons: [{
                    buttonText: 'Close',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            $(this).modal('hide');
                        }
                    }
                }],
                handler: {
                    close: function () {
                        resetShowLocalChangesDialog();
                    }
                }
            });

            // handle the click for the process group import
            $('#import-process-group-link').on('click', function() {
                showImportFlowVersionDialog();
            });

            // initialize the import flow version table
            initImportFlowVersionTable();
            initLocalChangesTable($('#revert-local-changes-table'), $('#revert-local-changes-filter'), $('#displayed-revert-local-changes-entries'), $('#total-revert-local-changes-entries'));
            initLocalChangesTable($('#show-local-changes-table'), $('#show-local-changes-filter'), $('#displayed-show-local-changes-entries'), $('#total-show-local-changes-entries'));
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
                    if (nfCommon.isDefinedAndNotNull(groupVersionControlInformation.versionControlInformation)) {
                        var versionControlInformation = groupVersionControlInformation.versionControlInformation;

                        // update the registry and bucket visibility
                        $('#save-flow-version-registry').text(versionControlInformation.registryName).show();
                        $('#save-flow-version-bucket').text(versionControlInformation.bucketName).show();
                        $('#save-flow-version-label').text(versionControlInformation.version + 1);

                        $('#save-flow-version-name').text(versionControlInformation.flowName).show();
                        nfCommon.populateField('save-flow-version-description', versionControlInformation.flowDescription);
                        $('#save-flow-version-description').show();

                        // record the versionControlInformation
                        $('#save-flow-version-process-group-id').data('versionControlInformation', versionControlInformation);

                        // reposition the version label
                        $('#save-flow-version-label').css('margin-top', '-15px');

                        focusName = false;
                        deferred.resolve();
                    } else {
                        // update the registry and bucket visibility
                        var registryCombo = $('#save-flow-version-registry-combo').combo('destroy').combo({
                            options: [{
                                text: 'Loading registries...',
                                value: null,
                                optionClass: 'unset',
                                disabled: true
                            }]
                        }).show();
                        var bucketCombo = $('#save-flow-version-bucket-combo').combo('destroy').combo({
                            options: [{
                                text: 'Loading buckets...',
                                value: null,
                                optionClass: 'unset',
                                disabled: true
                            }]
                        }).show();

                        // set the initial version
                        $('#save-flow-version-label').text(1);

                        $('#save-flow-version-name-field').show();
                        $('#save-flow-version-description-field').show();

                        // reposition the version label
                        $('#save-flow-version-label').css('margin-top', '0');

                        loadRegistries($('#save-flow-version-dialog'), registryCombo, bucketCombo, null, selectBucketSaveFlowVersion, function (bucketEntity) {
                            return bucketEntity.permissions.canWrite === true;
                        }).done(function () {
                            deferred.resolve();
                        }).fail(function () {
                            deferred.reject();
                        });
                    }

                    // record the revision
                    $('#save-flow-version-process-group-id').data('revision', groupVersionControlInformation.processGroupRevision).text(processGroupId);
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
            loadLocalChanges(processGroupId, $('#revert-local-changes-message'), $('#revert-local-changes-table'), $('#total-revert-local-changes-entries')).done(function () {
                $('#revert-local-changes-process-group-id').text(processGroupId);
                $('#revert-local-changes-dialog').modal('show');
            });
        },

        /**
         * Shows local changes for the specified process group.
         *
         * @param processGroupId
         */
        showLocalChanges: function (processGroupId) {
            loadLocalChanges(processGroupId, $('#show-local-changes-message'), $('#show-local-changes-table'), $('#total-show-local-changes-entries')).done(function () {
                $('#show-local-changes-dialog').modal('show');
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
                        $('#import-flow-version-registry').text(versionControlInformation.registryName).show();
                        $('#import-flow-version-bucket').text(versionControlInformation.bucketName).show();
                        $('#import-flow-version-name').text(versionControlInformation.flowName).show();

                        // show the current version information
                        $('#import-flow-version-container').show();
                        $('#import-flow-version-label').text(versionControlInformation.version);

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
                            changeFlowVersion();
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
                headerText: 'Stop Version Control',
                dialogContent: 'Are you sure you want to stop version control?',
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
                                url: '../nifi-api/versions/process-groups/' + encodeURIComponent(processGroupId) + '?' + $.param($.extend({
                                    'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
                                }, revision)),
                                dataType: 'json',
                                contentType: 'application/json'
                            }).done(function (response) {
                                updateVersionControlInformation(processGroupId, undefined);

                                nfDialog.showOkDialog({
                                    headerText: 'Disconnect',
                                    dialogContent: 'This Process Group is no longer under version control.'
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