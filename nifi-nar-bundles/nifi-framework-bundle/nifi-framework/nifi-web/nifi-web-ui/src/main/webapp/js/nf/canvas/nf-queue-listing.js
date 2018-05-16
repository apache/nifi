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
 * Lists FlowFiles from a given connection.
 */
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'Slick',
                'nf.Common',
                'nf.Dialog',
                'nf.Shell',
                'nf.ng.Bridge',
                'nf.ClusterSummary',
                'nf.ErrorHandler',
                'nf.Storage',
                'nf.CanvasUtils'],
            function ($, Slick, nfCommon, nfDialog, nfShell, nfNgBridge, nfClusterSummary, nfErrorHandler, nfStorage, nfCanvasUtils) {
                return (nf.QueueListing = factory($, Slick, nfCommon, nfDialog, nfShell, nfNgBridge, nfClusterSummary, nfErrorHandler, nfStorage, nfCanvasUtils));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.QueueListing =
            factory(require('jquery'),
                require('Slick'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.Shell'),
                require('nf.ng.Bridge'),
                require('nf.ClusterSummary'),
                require('nf.ErrorHandler'),
                require('nf.Storage'),
                require('nf.CanvasUtils')));
    } else {
        nf.QueueListing = factory(root.$,
            root.Slick,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.Shell,
            root.nf.ng.Bridge,
            root.nf.ClusterSummary,
            root.nf.ErrorHandler,
            root.nf.Storage,
            root.nf.CanvasUtils);
    }
}(this, function ($, Slick, nfCommon, nfDialog, nfShell, nfNgBridge, nfClusterSummary, nfErrorHandler, nfStorage, nfCanvasUtils) {
    'use strict';

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        urls: {
            uiExtensionToken: '../nifi-api/access/ui-extension-token',
            downloadToken: '../nifi-api/access/download-token'
        }
    };

    /**
     * Initializes the listing request status dialog.
     */
    var initializeListingRequestStatusDialog = function () {
        // configure the drop request status dialog
        $('#listing-request-status-dialog').modal({
            scrollableContentStyle: 'scrollable',
            headerText: 'Queue Listing',
            handler: {
                close: function () {
                    // clear the current button model
                    $('#listing-request-status-dialog').modal('setButtonModel', []);
                }
            }
        });
    };

    /**
     * Downloads the content for the flowfile currently being viewed.
     */
    var downloadContent = function () {
        var dataUri = $('#flowfile-uri').text() + '/content';

        // perform the request once we've received a token
        nfCommon.getAccessToken(config.urls.downloadToken).done(function (downloadToken) {
            var parameters = {};

            // conditionally include the ui extension token
            if (!nfCommon.isBlank(downloadToken)) {
                parameters['access_token'] = downloadToken;
            }

            // conditionally include the cluster node id
            var clusterNodeId = $('#flowfile-cluster-node-id').text();
            if (!nfCommon.isBlank(clusterNodeId)) {
                parameters['clusterNodeId'] = clusterNodeId;
            }

            // open the url
            if ($.isEmptyObject(parameters)) {
                window.open(dataUri);
            } else {
                window.open(dataUri + '?' + $.param(parameters));
            }
        }).fail(function () {
            nfDialog.showOkDialog({
                headerText: 'Queue Listing',
                dialogContent: 'Unable to generate access token for downloading content.'
            });
        });
    };

    /**
     * Views the content for the flowfile currently being viewed.
     */
    var viewContent = function () {
        var dataUri = $('#flowfile-uri').text() + '/content';

        // generate tokens as necessary
        var getAccessTokens = $.Deferred(function (deferred) {
            if (nfStorage.hasItem('jwt')) {
                // generate a token for the ui extension and another for the callback
                var uiExtensionToken = $.ajax({
                    type: 'POST',
                    url: config.urls.uiExtensionToken
                });
                var downloadToken = $.ajax({
                    type: 'POST',
                    url: config.urls.downloadToken
                });

                // wait for each token
                $.when(uiExtensionToken, downloadToken).done(function (uiExtensionTokenResult, downloadTokenResult) {
                    var uiExtensionToken = uiExtensionTokenResult[0];
                    var downloadToken = downloadTokenResult[0];
                    deferred.resolve(uiExtensionToken, downloadToken);
                }).fail(function () {
                    nfDialog.showOkDialog({
                        headerText: 'Queue Listing',
                        dialogContent: 'Unable to generate access token for viewing content.'
                    });
                    deferred.reject();
                });
            } else {
                deferred.resolve('', '');
            }
        }).promise();

        // perform the request after we've received the tokens
        getAccessTokens.done(function (uiExtensionToken, downloadToken) {
            var dataUriParameters = {};

            // conditionally include the cluster node id
            var clusterNodeId = $('#flowfile-cluster-node-id').text();
            if (!nfCommon.isBlank(clusterNodeId)) {
                dataUriParameters['clusterNodeId'] = clusterNodeId;
            }

            // include the download token if applicable
            if (!nfCommon.isBlank(downloadToken)) {
                dataUriParameters['access_token'] = downloadToken;
            }

            // include parameters if necessary
            if ($.isEmptyObject(dataUriParameters) === false) {
                dataUri = dataUri + '?' + $.param(dataUriParameters);
            }

            // open the content viewer
            var contentViewerUrl = $('#nifi-content-viewer-url').text();

            // if there's already a query string don't add another ?... this assumes valid
            // input meaning that if the url has already included a ? it also contains at
            // least one query parameter
            if (contentViewerUrl.indexOf('?') === -1) {
                contentViewerUrl += '?';
            } else {
                contentViewerUrl += '&';
            }

            var contentViewerParameters = {
                'ref': dataUri
            };

            // include the download token if applicable
            if (!nfCommon.isBlank(uiExtensionToken)) {
                contentViewerParameters['access_token'] = uiExtensionToken;
            }

            // open the content viewer
            window.open(contentViewerUrl + $.param(contentViewerParameters));
        });
    };

    /**
     * Performs a listing on the specified connection.
     *
     * @param connection the connection
     */
    var performListing = function (connection) {
        var MAX_DELAY = 4;
        var cancelled = false;
        var listingRequest = null;
        var listingRequestTimer = null;

        return $.Deferred(function (deferred) {
            // updates the progress bar
            var updateProgress = function (percentComplete) {
                // remove existing labels
                var progressBar = $('#listing-request-percent-complete');
                progressBar.find('div.progress-label').remove();
                progressBar.find('md-progress-linear').remove();

                // update the progress
                var label = $('<div class="progress-label"></div>').text(percentComplete + '%');
                (nfNgBridge.injector.get('$compile')($('<md-progress-linear ng-cloak ng-value="' + percentComplete + '" class="md-hue-2" md-mode="determinate" aria-label="Searching Queue"></md-progress-linear>'))(nfNgBridge.rootScope)).appendTo(progressBar);
                progressBar.append(label);
            };

            // update the button model of the drop request status dialog
            $('#listing-request-status-dialog').modal('setButtonModel', [{
                headerText: 'Queue Listing',
                buttonText: 'Stop',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        cancelled = true;

                        // we are waiting for the next poll attempt
                        if (listingRequestTimer !== null) {
                            // cancel it
                            clearTimeout(listingRequestTimer);

                            // cancel the listing request
                            completeListingRequest();
                        }
                    }
                }
            }]);

            // completes the listing request by removing it
            var completeListingRequest = function () {
                $('#listing-request-status-dialog').modal('hide');

                var reject = cancelled;

                // ensure the listing requests are present
                if (nfCommon.isDefinedAndNotNull(listingRequest)) {
                    $.ajax({
                        type: 'DELETE',
                        url: listingRequest.uri,
                        dataType: 'json'
                    });

                    // use the listing request from when the listing completed
                    if (nfCommon.isEmpty(listingRequest.flowFileSummaries)) {
                        if (cancelled === false) {
                            reject = true;

                            // show the dialog
                            nfDialog.showOkDialog({
                                headerText: 'Queue Listing',
                                dialogContent: 'The queue has no FlowFiles.'
                            });
                        }
                    } else {
                        // update the queue size
                        $('#total-flowfiles-count').text(nfCommon.formatInteger(listingRequest.queueSize.objectCount));
                        $('#total-flowfiles-size').text(nfCommon.formatDataSize(listingRequest.queueSize.byteCount));

                        // update the last updated time
                        $('#queue-listing-last-refreshed').text(listingRequest.lastUpdated);

                        // show a message for the queue listing if necessary
                        var queueListingTable = $('#queue-listing-table');
                        var queueListingMessage = $('#queue-listing-message');
                        if (listingRequest.sourceRunning === true || listingRequest.destinationRunning === true) {
                            if (listingRequest.souceRunning === true && listingRequest.destinationRunning === true) {
                                queueListingMessage.text('The source and destination of this queue are currently running. This listing may no longer be accurate.').show();
                            } else if (listingRequest.sourceRunning === true) {
                                queueListingMessage.text('The source of this queue is currently running. This listing may no longer be accurate.').show();
                            } else if (listingRequest.destinationRunning === true) {
                                queueListingMessage.text('The destination of this queue is currently running. This listing may no longer be accurate.').show();
                            }
                        } else {
                            queueListingMessage.text('').hide();
                        }

                        // get the grid to load the data
                        var queueListingGrid = $('#queue-listing-table').data('gridInstance');
                        var queueListingData = queueListingGrid.getData();

                        // load the flowfiles
                        queueListingData.beginUpdate();
                        queueListingData.setItems(listingRequest.flowFileSummaries, 'uuid');
                        queueListingData.endUpdate();
                    }
                } else {
                    reject = true;
                }

                if (reject) {
                    deferred.reject();
                } else {
                    deferred.resolve();
                }
            };

            // process the listing request
            var processListingRequest = function (delay) {
                // update the percent complete
                updateProgress(listingRequest.percentCompleted);

                // update the status of the listing request
                $('#listing-request-status-message').text(listingRequest.state);

                // close the dialog if the
                if (listingRequest.finished === true || cancelled === true) {
                    completeListingRequest();
                } else {
                    // wait delay to poll again
                    listingRequestTimer = setTimeout(function () {
                        // clear the listing request timer
                        listingRequestTimer = null;

                        // schedule to poll the status again in nextDelay
                        pollListingRequest(Math.min(MAX_DELAY, delay * 2));
                    }, delay * 1000);
                }
            };

            // schedule for the next poll iteration
            var pollListingRequest = function (nextDelay) {
                $.ajax({
                    type: 'GET',
                    url: listingRequest.uri,
                    dataType: 'json'
                }).done(function (response) {
                    listingRequest = response.listingRequest;
                    processListingRequest(nextDelay);
                }).fail(completeListingRequest).fail(nfErrorHandler.handleAjaxError);
            };

            // issue the request to list the flow files
            $.ajax({
                type: 'POST',
                url: '../nifi-api/flowfile-queues/' + connection.id + '/listing-requests',
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // initialize the progress bar value
                updateProgress(0);

                // show the progress dialog
                $('#listing-request-status-dialog').modal('show');

                // process the drop request
                listingRequest = response.listingRequest;
                processListingRequest(1);
            }).fail(completeListingRequest).fail(nfErrorHandler.handleAjaxError);
        }).promise();
    };

    /**
     * Shows the details for the specified flowfile.
     *
     * @param flowFileSummary the flowfile summary
     */
    var showFlowFileDetails = function (flowFileSummary) {
        // formats an flowfile detail
        var formatFlowFileDetail = function (label, value) {
            $('<div class="flowfile-detail"></div>').append(
                $('<div class="detail-name"></div>').text(label)).append(
                $('<div class="detail-value">' + nfCommon.formatValue(value) + '</div>').ellipsis()).append(
                $('<div class="clear"></div>')).appendTo('#additional-flowfile-details');
        };

        // formats the content value
        var formatContentValue = function (element, value) {
            if (nfCommon.isDefinedAndNotNull(value)) {
                element.removeClass('unset').text(value);
            } else {
                element.addClass('unset').text('No value set');
            }
        };

        var params = {};
        if (nfCommon.isDefinedAndNotNull(flowFileSummary.clusterNodeId)) {
            params['clusterNodeId'] = flowFileSummary.clusterNodeId;
        }

        $.ajax({
            type: 'GET',
            url: flowFileSummary.uri,
            data: params,
            dataType: 'json'
        }).done(function (response) {
            var flowFile = response.flowFile;

            // set a default for flowfiles with no content claim
            var fileSize = nfCommon.isDefinedAndNotNull(flowFile.contentClaimFileSize) ? flowFile.contentClaimFileSize : nfCommon.formatDataSize(0);

            // show the URI to this flowfile
            $('#flowfile-uri').text(flowFile.uri);

            // show the flowfile details dialog
            $('#flowfile-uuid').html(nfCommon.formatValue(flowFile.uuid));
            $('#flowfile-filename').html(nfCommon.formatValue(flowFile.filename));
            $('#flowfile-queue-position').html(nfCommon.formatValue(flowFile.position));
            $('#flowfile-file-size').html(nfCommon.formatValue(fileSize));
            $('#flowfile-queued-duration').text(nfCommon.formatDuration(flowFile.queuedDuration));
            $('#flowfile-lineage-duration').text(nfCommon.formatDuration(flowFile.lineageDuration));
            $('#flowfile-penalized').text(flowFile.penalized === true ? 'Yes' : 'No');

            // conditionally show the cluster node identifier
            if (nfCommon.isDefinedAndNotNull(flowFileSummary.clusterNodeId)) {
                // save the cluster node id
                $('#flowfile-cluster-node-id').text(flowFileSummary.clusterNodeId);

                // render the cluster node address
                formatFlowFileDetail('Node Address', flowFileSummary.clusterNodeAddress);
            }

            if (nfCommon.isDefinedAndNotNull(flowFile.contentClaimContainer)) {
                // content claim
                formatContentValue($('#content-container'), flowFile.contentClaimContainer);
                formatContentValue($('#content-section'), flowFile.contentClaimSection);
                formatContentValue($('#content-identifier'), flowFile.contentClaimIdentifier);
                formatContentValue($('#content-offset'), flowFile.contentClaimOffset);
                formatContentValue($('#content-bytes'), flowFile.contentClaimFileSizeBytes);

                // input content file size
                var contentSize = $('#content-size');
                formatContentValue(contentSize, flowFile.contentClaimFileSize);
                if (nfCommon.isDefinedAndNotNull(flowFile.contentClaimFileSize)) {
                    // over the default tooltip with the actual byte count
                    contentSize.attr('title', nfCommon.formatInteger(flowFile.contentClaimFileSizeBytes) + ' bytes');
                }

                // show the content details
                $('#flowfile-content-details').show();
                $('#flowfile-with-no-content').hide();
            } else {
                $('#flowfile-content-details').hide();
                $('#flowfile-with-no-content').show();
            }

            // attributes
            var attributesContainer = $('#flowfile-attributes-container');

            // get any action details
            var sortedAttributeNames = Object.keys(flowFile.attributes).sort();
            sortedAttributeNames.forEach(function (attributeName) {
                var attributeValue = flowFile.attributes[attributeName];

                // create the attribute record
                var attributeRecord = $('<div class="attribute-detail"></div>')
                    .append($('<div class="attribute-name">' + nfCommon.formatValue(attributeName) + '</div>').ellipsis())
                    .appendTo(attributesContainer);

                // add the current value
                attributeRecord
                    .append($('<div class="attribute-value">' + nfCommon.formatValue(attributeValue) + '</div>').ellipsis())
                    .append('<div class="clear"></div>');
            });

            // show the dialog
            $('#flowfile-details-dialog').modal('show');
        }).fail(nfErrorHandler.handleAjaxError);
    };

    var nfQueueListing = {
        init: function () {
            initializeListingRequestStatusDialog();

            // define mouse over event for the refresh button
            $('#queue-listing-refresh-button').click(function () {
                var connection = $('#queue-listing-table').data('connection');
                performListing(connection);
            });

            // define a custom formatter for showing more processor details
            var moreDetailsFormatter = function (row, cell, value, columnDef, dataContext) {
                return '<div class="pointer show-flowfile-details fa fa-info-circle" title="View Details" style="margin-top: 5px; float: left;"></div>';
            };

            // function for formatting data sizes
            var dataSizeFormatter = function (row, cell, value, columnDef, dataContext) {
                return nfCommon.formatDataSize(value);
            };

            // function for formatting durations
            var durationFormatter = function (row, cell, value, columnDef, dataContext) {
                return nfCommon.formatDuration(value);
            };

            // function for formatting penalization
            var penalizedFormatter = function (row, cell, value, columnDef, dataContext) {
                var markup = '';

                if (value === true) {
                    markup += 'Yes';
                }

                return markup;
            };

            // initialize the queue listing table
            var queueListingColumns = [
                {
                    id: 'moreDetails',
                    field: 'moreDetails',
                    name: '&nbsp;',
                    sortable: false,
                    resizable: false,
                    formatter: moreDetailsFormatter,
                    width: 50,
                    maxWidth: 50
                },
                {
                    id: 'position',
                    name: 'Position',
                    field: 'position',
                    sortable: false,
                    resizable: false,
                    width: 75,
                    maxWidth: 75,
                    formatter: nfCommon.genericValueFormatter
                },
                {
                    id: 'uuid',
                    name: 'UUID',
                    field: 'uuid',
                    sortable: false,
                    resizable: true,
                    formatter: nfCommon.genericValueFormatter
                },
                {
                    id: 'filename',
                    name: 'Filename',
                    field: 'filename',
                    sortable: false,
                    resizable: true,
                    formatter: nfCommon.genericValueFormatter
                },
                {
                    id: 'size',
                    name: 'File Size',
                    field: 'size',
                    sortable: false,
                    resizable: true,
                    defaultSortAsc: false,
                    formatter: dataSizeFormatter
                },
                {
                    id: 'queuedDuration',
                    name: 'Queued Duration',
                    field: 'queuedDuration',
                    sortable: false,
                    resizable: true,
                    formatter: durationFormatter
                },
                {
                    id: 'lineageDuration',
                    name: 'Lineage Duration',
                    field: 'lineageDuration',
                    sortable: false,
                    resizable: true,
                    formatter: durationFormatter
                },
                {
                    id: 'penalized',
                    name: 'Penalized',
                    field: 'penalized',
                    sortable: false,
                    resizable: false,
                    width: 100,
                    maxWidth: 100,
                    formatter: penalizedFormatter
                }
            ];

            // conditionally show the cluster node identifier
            if (nfClusterSummary.isClustered()) {
                queueListingColumns.push({
                    id: 'clusterNodeAddress',
                    name: 'Node',
                    field: 'clusterNodeAddress',
                    sortable: false,
                    resizable: true,
                    formatter: nfCommon.genericValueFormatter
                });
            }

            // add an actions column when the user can access provenance
            if (nfCommon.canAccessProvenance()) {
                // function for formatting actions
                var actionsFormatter = function () {
                    return '<div title="Provenance" class="pointer icon icon-provenance view-provenance"></div>';
                };

                queueListingColumns.push({
                    id: 'actions',
                    name: '&nbsp;',
                    resizable: false,
                    formatter: actionsFormatter,
                    sortable: false,
                    width: 50,
                    maxWidth: 50
                });
            }

            var queueListingOptions = {
                forceFitColumns: true,
                enableTextSelectionOnCells: true,
                enableCellNavigation: false,
                enableColumnReorder: false,
                autoEdit: false,
                rowHeight: 24
            };

            // initialize the dataview
            var queueListingData = new Slick.Data.DataView({
                inlineFilters: false
            });
            queueListingData.setItems([]);

            // initialize the grid
            var queueListingGrid = new Slick.Grid('#queue-listing-table', queueListingData, queueListingColumns, queueListingOptions);
            queueListingGrid.setSelectionModel(new Slick.RowSelectionModel());
            queueListingGrid.registerPlugin(new Slick.AutoTooltips());

            // configure a click listener
            queueListingGrid.onClick.subscribe(function (e, args) {
                var target = $(e.target);

                // get the node at this row
                var item = queueListingData.getItem(args.row);

                // determine the desired action
                if (queueListingGrid.getColumns()[args.cell].id === 'moreDetails') {
                    if (target.hasClass('show-flowfile-details')) {
                        showFlowFileDetails(item);
                    }
                } else if (queueListingGrid.getColumns()[args.cell].id === 'actions') {
                    if (target.hasClass('view-provenance')) {
                        // close the settings dialog
                        $('#shell-close-button').click();

                        // open the provenance page with the specified component
                        nfShell.showPage('provenance?' + $.param({
                                flowFileUuid: item.uuid
                            }));
                    }
                }
            });

            // wire up the dataview to the grid
            queueListingData.onRowCountChanged.subscribe(function (e, args) {
                queueListingGrid.updateRowCount();
                queueListingGrid.render();

                // update the total number of displayed flowfiles
                $('#displayed-flowfiles').text(args.current);
            });
            queueListingData.onRowsChanged.subscribe(function (e, args) {
                queueListingGrid.invalidateRows(args.rows);
                queueListingGrid.render();
            });

            // hold onto an instance of the grid
            $('#queue-listing-table').data('gridInstance', queueListingGrid);

            // initialize the number of display items
            $('#displayed-flowfiles').text('0');
        },

        /**
         * Initializes the flowfile details dialog.
         */
        initFlowFileDetailsDialog: function () {
            $('#content-download').on('click', downloadContent);

            // only show if content viewer is configured
            if (nfCommon.isContentViewConfigured()) {
                $('#content-view').show();
                $('#content-view').on('click', viewContent);
            }

            $('#flowfile-details-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                scrollableTabContentStyle: 'scrollable',
                tabs: [{
                    name: 'Details',
                    tabContentId: 'flowfile-details-tab-content'
                }, {
                    name: 'Attributes',
                    tabContentId: 'flowfile-attributes-tab-content'
                }]
            });

            $('#flowfile-details-dialog').modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'FlowFile',
                buttons: [{
                    buttonText: 'Ok',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            $('#flowfile-details-dialog').modal('hide');
                        }
                    }
                }],
                handler: {
                    close: function () {
                        // clear the details
                        $('#flowfile-attributes-container').empty();
                        $('#flowfile-cluster-node-id').text('');
                        $('#additional-flowfile-details').empty();
                    },
                    open: function () {
                        nfCommon.toggleScrollable($('#' + this.find('.tab-container').attr('id') + '-content').get(0));
                    }
                }
            });
        },

        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            var queueListingGrid = $('#queue-listing-table').data('gridInstance');
            if (nfCommon.isDefinedAndNotNull(queueListingGrid)) {
                queueListingGrid.resizeCanvas();
            }
        },

        /**
         * Shows the listing of the FlowFiles from a given connection.
         *
         * @param   {object}    The connection
         */
        listQueue: function (connection) {
            // perform the initial listing
            performListing(connection).done(function () {
                // update the connection name
                var connectionName = '';
                if (connection.permissions.canRead) {
                    connectionName = nfCanvasUtils.formatConnectionName(connection.component);
                }
                if (connectionName === '') {
                    connectionName = 'Connection';
                }
                $('#queue-listing-header-text').text(connectionName);

                // show the listing container
                nfShell.showContent('#queue-listing-container').done(function () {
                    $('#queue-listing-table').removeData('connection');

                    // clear the table
                    var queueListingGrid = $('#queue-listing-table').data('gridInstance');
                    var queueListingData = queueListingGrid.getData();

                    // clear the flowfiles
                    queueListingData.beginUpdate();
                    queueListingData.setItems([], 'uuid');
                    queueListingData.endUpdate();

                    // reset stats
                    $('#displayed-flowfiles, #total-flowfiles-count').text('0');
                    $('#total-flowfiles-size').text(nfCommon.formatDataSize(0));
                });

                // adjust the table size
                nfQueueListing.resetTableSize();

                // store the connection for access later
                $('#queue-listing-table').data('connection', connection);
            });
        }
    };

    return nfQueueListing;
}));