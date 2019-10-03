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
                'd3',
                'nf.CanvasUtils',
                'nf.Common',
                'nf.Dialog',
                'nf.Storage',
                'nf.Client',
                'nf.ErrorHandler',
                'nf.Clipboard',
                'nf.ParameterContexts',
                'nf.Snippet',
                'nf.GoTo',
                'nf.ng.Bridge',
                'nf.Shell',
                'nf.VariableRegistry',
                'nf.ComponentState',
                'nf.FlowVersion',
                'nf.Draggable',
                'nf.Birdseye',
                'nf.Connection',
                'nf.Graph',
                'nf.ProcessGroupConfiguration',
                'nf.ProcessorConfiguration',
                'nf.ProcessorDetails',
                'nf.LabelConfiguration',
                'nf.RemoteProcessGroupConfiguration',
                'nf.RemoteProcessGroupDetails',
                'nf.PortConfiguration',
                'nf.PortDetails',
                'nf.ConnectionConfiguration',
                'nf.ConnectionDetails',
                'nf.PolicyManagement',
                'nf.RemoteProcessGroup',
                'nf.Label',
                'nf.Processor',
                'nf.RemoteProcessGroupPorts',
                'nf.ComponentVersion',
                'nf.QueueListing',
                'nf.StatusHistory'],
            function ($, d3, nfCanvasUtils, nfCommon, nfDialog, nfStorage, nfClient, nfErrorHandler, nfClipboard, nfParameterContexts, nfSnippet, nfGoto, nfNgBridge, nfShell, nfVariableRegistry, nfComponentState, nfFlowVersion, nfDraggable, nfBirdseye, nfConnection, nfGraph, nfProcessGroupConfiguration, nfProcessorConfiguration, nfProcessorDetails, nfLabelConfiguration, nfRemoteProcessGroupConfiguration, nfRemoteProcessGroupDetails, nfPortConfiguration, nfPortDetails, nfConnectionConfiguration, nfConnectionDetails, nfPolicyManagement, nfRemoteProcessGroup, nfLabel, nfProcessor, nfRemoteProcessGroupPorts, nfComponentVersion, nfQueueListing, nfStatusHistory) {
                return (nf.Actions = factory($, d3, nfCanvasUtils, nfCommon, nfDialog, nfStorage, nfClient, nfErrorHandler, nfClipboard, nfParameterContexts, nfSnippet, nfGoto, nfNgBridge, nfShell, nfVariableRegistry, nfComponentState, nfFlowVersion, nfDraggable, nfBirdseye, nfConnection, nfGraph, nfProcessGroupConfiguration, nfProcessorConfiguration, nfProcessorDetails, nfLabelConfiguration, nfRemoteProcessGroupConfiguration, nfRemoteProcessGroupDetails, nfPortConfiguration, nfPortDetails, nfConnectionConfiguration, nfConnectionDetails, nfPolicyManagement, nfRemoteProcessGroup, nfLabel, nfProcessor, nfRemoteProcessGroupPorts, nfComponentVersion, nfQueueListing, nfStatusHistory));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Actions =
            factory(require('jquery'),
                require('d3'),
                require('nf.CanvasUtils'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.Storage'),
                require('nf.Client'),
                require('nf.ErrorHandler'),
                require('nf.Clipboard'),
                require('nf.ParameterContexts'),
                require('nf.Snippet'),
                require('nf.GoTo'),
                require('nf.ng.Bridge'),
                require('nf.Shell'),
                require('nf.VariableRegistry'),
                require('nf.ComponentState'),
                require('nf.FlowVersion'),
                require('nf.Draggable'),
                require('nf.Birdseye'),
                require('nf.Connection'),
                require('nf.Graph'),
                require('nf.ProcessGroupConfiguration'),
                require('nf.ProcessorConfiguration'),
                require('nf.ProcessorDetails'),
                require('nf.LabelConfiguration'),
                require('nf.RemoteProcessGroupConfiguration'),
                require('nf.RemoteProcessGroupDetails'),
                require('nf.PortConfiguration'),
                require('nf.PortDetails'),
                require('nf.ConnectionConfiguration'),
                require('nf.ConnectionDetails'),
                require('nf.PolicyManagement'),
                require('nf.RemoteProcessGroup'),
                require('nf.Label'),
                require('nf.Processor'),
                require('nf.RemoteProcessGroupPorts'),
                require('nf.ComponentVersion'),
                require('nf.QueueListing'),
                require('nf.StatusHistory')));
    } else {
        nf.Actions = factory(root.$,
            root.d3,
            root.nf.CanvasUtils,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.Storage,
            root.nf.Client,
            root.nf.ErrorHandler,
            root.nf.Clipboard,
            root.nf.ParameterContexts,
            root.nf.Snippet,
            root.nf.GoTo,
            root.nf.ng.Bridge,
            root.nf.Shell,
            root.nf.VariableRegistry,
            root.nf.ComponentState,
            root.nf.FlowVersion,
            root.nf.Draggable,
            root.nf.Birdseye,
            root.nf.Connection,
            root.nf.Graph,
            root.nf.ProcessGroupConfiguration,
            root.nf.ProcessorConfiguration,
            root.nf.ProcessorDetails,
            root.nf.LabelConfiguration,
            root.nf.RemoteProcessGroupConfiguration,
            root.nf.RemoteProcessGroupDetails,
            root.nf.PortConfiguration,
            root.nf.PortDetails,
            root.nf.ConnectionConfiguration,
            root.nf.ConnectionDetails,
            root.nf.PolicyManagement,
            root.nf.RemoteProcessGroup,
            root.nf.Label,
            root.nf.Processor,
            root.nf.RemoteProcessGroupPorts,
            root.nf.ComponentVersion,
            root.nf.QueueListing,
            root.nf.StatusHistory);
    }
}(this, function ($, d3, nfCanvasUtils, nfCommon, nfDialog, nfStorage, nfClient, nfErrorHandler, nfClipboard, nfParameterContexts, nfSnippet, nfGoto, nfNgBridge, nfShell, nfVariableRegistry, nfComponentState, nfFlowVersion, nfDraggable, nfBirdseye, nfConnection, nfGraph, nfProcessGroupConfiguration, nfProcessorConfiguration, nfProcessorDetails, nfLabelConfiguration, nfRemoteProcessGroupConfiguration, nfRemoteProcessGroupDetails, nfPortConfiguration, nfPortDetails, nfConnectionConfiguration, nfConnectionDetails, nfPolicyManagement, nfRemoteProcessGroup, nfLabel, nfProcessor, nfRemoteProcessGroupPorts, nfComponentVersion, nfQueueListing, nfStatusHistory) {
    'use strict';

    var config = {
        urls: {
            api: '../nifi-api',
            controller: '../nifi-api/controller',
            parameterContexts: '../nifi-api/parameter-contexts'
        }
    };

    /**
     * Initializes the drop request status dialog.
     */
    var initializeDropRequestStatusDialog = function () {
        // configure the drop request status dialog
        $('#drop-request-status-dialog').modal({
            scrollableContentStyle: 'scrollable',
            handler: {
                close: function () {
                    // clear the current button model
                    $('#drop-request-status-dialog').modal('setButtonModel', []);
                }
            }
        });
    };


    /**
     * Updates the resource with the specified entity.
     *
     * @param {string} uri
     * @param {object} entity
     */
    var updateResource = function (uri, entity) {
        entity['disconnectedNodeAcknowledged'] = nfStorage.isDisconnectionAcknowledged();

        return $.ajax({
            type: 'PUT',
            url: uri,
            data: JSON.stringify(entity),
            dataType: 'json',
            contentType: 'application/json'
        }).fail(function (xhr, status, error) {
            nfDialog.showOkDialog({
                headerText: 'Update Resource',
                dialogContent: nfCommon.escapeHtml(xhr.responseText)
            });
        });
    };

    // create a method for updating process groups and processors
    var updateProcessGroup = function (response) {
        $.ajax({
            type: 'GET',
            url: config.urls.api + '/flow/process-groups/' + encodeURIComponent(response.id),
            dataType: 'json'
        }).done(function (response) {
            nfGraph.set(response.processGroupFlow.flow);
        });
    };

    // determine if the source of this connection is part of the selection
    var isSourceSelected = function (connection, selection) {
        return selection.filter(function (d) {
                return nfCanvasUtils.getConnectionSourceComponentId(connection) === d.id;
            }).size() > 0;
    };

    /**
     * Empty all the queues inside the array of connections.
     *
     * @Param {string} actionName
     * @param {Array} componentsToEmpty array of component Ids along with a boolean indicating if a id refers to a processGroup or a connection
     * @Param {boolean} isRecursive if true indicates that component IDs refer to process groups, otherwise it is assumed they refer to connections
     */
    var emptyQueues = function (actionName, componentsToEmpty, isRecursive) {
        var cancelled = false;
        var finished = false;
        var progressBarRefreshingDelay = 100; //millis

        var dropRequest = null;
        var dropRequestTimer = null;
        var progressBarRefreshTimer = null;
        var emptyRequestPromise = null;
        var errors = null;

        var createFailedResponse = function (xhr, status, error) {
            return {
                success: false,
                xhr: xhr,
                status: status,
                error: error
            };
        };

        var createSuccessResponse = function () {
            return {
                success: true
            };
        };

        // set the progress bar to a certain percentage
        var setProgressBar = function (percentComplete) {
            if($("#drop-request-percent-complete .progress-label").length) {
                //set the request status text
                $('#drop-request-status-message').text('');

                //set progress bar
                $('#drop-request-percent-complete .md-hue-2 .md-container.md-mode-determinate .md-bar.md-bar2:first')
                    .attr('style','transform: translateX(' + ((percentComplete - 100) / 2) + '%) scale(' + (percentComplete * 0.01) + ', 1);');

                //set percentage
                $("#drop-request-percent-complete .progress-label:first").text(percentComplete.toFixed(2) + '%');
            }
            else {
                var progressBar = $('#drop-request-percent-complete');

                (nfNgBridge.injector.get('$compile')($('<md-progress-linear ng-cloak ng-value="' + percentComplete + '" class="md-hue-2" md-mode="determinate" aria-label="Drop request percent complete"></md-progress-linear>'))(nfNgBridge.rootScope)).appendTo(progressBar);

                progressBar.append($('<div class="progress-label"></div>').text(percentComplete + '%'));
            }
        };

        // process the drop request
        var refreshProgressBar = function () {
            // update the completed percentage
            var percentCompleted = 0;
            var droppedFlowfiles = 0;
            var totalFlowfilesToDrop = 0;

            if( nfCommon.isDefinedAndNotNull(dropRequest) &&
                nfCommon.isDefinedAndNotNull(dropRequest.droppedCount) &&
                nfCommon.isDefinedAndNotNull(dropRequest.originalCount)
            ) {
                droppedFlowfiles += dropRequest.droppedCount;
                totalFlowfilesToDrop += dropRequest.originalCount;
            }

            if(totalFlowfilesToDrop !== 0) {
                percentCompleted = (droppedFlowfiles / totalFlowfilesToDrop) * 100;
            }

            setProgressBar(percentCompleted);

            // check if can keep on refreshing the progress bar
            if (finished !== true && cancelled !== true) {
                // wait delay to refresh again
                progressBarRefreshTimer = setTimeout(function () {
                    // clear the progressBarRefreshTimer timer
                    progressBarRefreshTimer = null;

                    // schedule to poll the status again in nextDelay
                    refreshProgressBar(progressBarRefreshingDelay);
                }, progressBarRefreshingDelay);
            }
        };

        var finalizeDropRequest = function () {
            // tell the server to delete the drop request
            if (nfCommon.isDefinedAndNotNull(dropRequest)) {
                $.ajax({
                    type: 'DELETE',
                    url: dropRequest.uri,
                    dataType: 'json'
                }).done(function (response) {
                    // report the results of this drop request
                    dropRequest = response.dropRequest;
                }).always(function () {
                    // resolve this request
                    emptyRequestPromise.resolve(createSuccessResponse());
                });
            }
        };

        // schedule for the next poll iteration
        var getDropRequestStatus = function () {
            $.ajax({
                type: 'GET',
                url: dropRequest.uri,
                dataType: 'json'
            }).done(function (response) {
                dropRequest = response.dropRequest;
                processDropRequestResponse();
            }).fail(function (xhr, status, error) {
                emptyRequestPromise.resolve(createFailedResponse(xhr,status,error));
            });
        };

        // process the drop request
        var processDropRequestResponse = function () {
            // close the dialog if
            if (dropRequest.finished === true || cancelled === true) {
                finalizeDropRequest();
            } else {
                // wait delay to poll again
                dropRequestTimer = setTimeout(function () {
                    // clear the drop request timer
                    dropRequestTimer = null;
                    // schedule to poll the status again in nextDelay
                    getDropRequestStatus();
                }, progressBarRefreshingDelay);
            }
        };

        // empty a single queue and return a deferred representing the emptying process status
        var emptyRequestAsync = function () {
            var deferred = $.Deferred();

            // issue the request to delete the flow files
            $.ajax({
                type: 'POST',
                url: '../nifi-api/flowfile-queues/drop-requests' + (isRecursive ? '/true' : '/false'),
                dataType: 'json',
                contentType: 'application/json',
                data: JSON.stringify({
                    componentsToEmpty: componentsToEmpty.reduce(function(map, obj) {
                            map[obj.componentId] = obj.isProcessGroup;
                            return map; }, {})
                })
            }).done(function (response) {
                // process the drop request
                dropRequest = response.dropRequest;
                if(nfCommon.isDefinedAndNotNull(dropRequest.componentErrors)) {
                    errors = dropRequest.componentErrors;
                }
                processDropRequestResponse();
            }).fail(function (xhr, status, error) {
                deferred.resolve(createFailedResponse(xhr,status,error));
            });

            return deferred;
        };

        //start the drop request
        emptyRequestPromise = emptyRequestAsync();

        // initialize the progress bar value and auto refresh it each progressBarRefreshingDelay milliseconds
        refreshProgressBar();

        // update the button model and header of the drop request status dialog and show it
        $('#drop-request-status-dialog')
            .modal('setButtonModel', [{
                buttonText: 'Stop',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        //tell the emptyRequestPromise async jobs that the user cancelled the operation and thus it needs to terminate
                        cancelled = true;

                        // progressBarRefreshTimer !== null means there is a timeout in progress on the refreshProgressBar method
                        if (progressBarRefreshTimer !== null) {
                            // cancel it
                            clearTimeout(progressBarRefreshTimer);
                        }
                    }
                }}]
            )
            .modal('setHeaderText', actionName)
            .modal('show');

        $.when(emptyRequestPromise).then(function (response) {
            finished = true;

            if (progressBarRefreshTimer !== null) {
                // remove the timeout from the refreshProgressBar method if present
                clearTimeout(progressBarRefreshTimer);
            }

            //hide the status dialog
            $('#drop-request-status-dialog').modal('hide');

            var droppedFlowfiles = 0;
            var droppedFlowfilesSize = 0;
            var errorMessages = "";

            //check for response error
            if(nfCommon.isDefinedAndNotNull(errors)) {
                errors.forEach(function (componentError) {
                    errorMessages += '<p> ' + componentError.componentType + 'ID ' + componentError.componentId +
                        ': <span style="color: red">' + nfCommon.escapeHtml(componentError.errorMessage) + '</span></p>';
                })
            }
            // build the results
            var results = $('<div></div>');

            if(response.success === false) {
                results.html('<p><span style="color: red">' + response.xhr.status + ' - ' + nfCommon.escapeHtml(response.xhr.responseText) + '</span></p>');
            }
            else if(nfCommon.isDefinedAndNotNull(dropRequest)) {
                //check for single empty queue errors
                if(nfCommon.isDefinedAndNotNull(dropRequest.failureReasons)) {
                    dropRequest.failureReasons.forEach(function (message) {
                        errorMessages += '<p>QueueID ' + message.key + ': <span style="color: red">' + nfCommon.escapeHtml(message.value) + '</span></p>';
                    });
                }

                // count the dropped flowfiles
                droppedFlowfiles += dropRequest.droppedCount;
                droppedFlowfilesSize += dropRequest.droppedSize;

                if(droppedFlowfiles !== 0) {
                    $('<span class="label"></span>').text(droppedFlowfiles).appendTo(results);
                    $('<span></span>').text(' FlowFiles (' + nfCommon.toReadableBytes(droppedFlowfilesSize) + ')').appendTo(results);
                    $('<span></span>').text(' were removed.').appendTo(results);
                }
                else {
                    results.text('No FlowFiles were removed.');
                }

                if(errorMessages !== "") {
                    $('<br/><br/><p style="color: darkred">Failed Drop Requests:</p><br/>').appendTo(results);
                    $('<div style="color: darkred"></div>').html(errorMessages).appendTo(results);
                }
            }

            // display the results
            nfDialog.showOkDialog({
                headerText: actionName,
                dialogContent: results,
                okHandler: function () {
                    nfCanvasUtils.reload();
                }
            });
        });
    };

    // /**
    //  * Empty all the queues inside the array of connections.
    //  *
    //  * @Param {string} actionName
    //  * @param {Array} connections
    //  * @Param {Array} errors
    //  */
    // var emptyQueues = function (actionName, connections, errors) {
    //     var cancelled = false;
    //     var finished = false;
    //     var progressBarRefreshingDelay = 100; //millis
    //
    //     var dropRequests = [];
    //     var dropRequestTimers = [];
    //     var progressBarRefreshTimer = null;
    //     var singleEmptyQueuePromises = [];
    //
    //     var createFailedResponse = function (xhr, status, error) {
    //         return {
    //             success: false,
    //             xhr: xhr,
    //             status: status,
    //             error: error
    //         };
    //     };
    //
    //     var createSuccessResponse = function () {
    //         return {
    //             success: true
    //         };
    //     };
    //
    //     // set the progress bar to a certain percentage
    //     var setProgressBar = function (percentComplete) {
    //         if($("#drop-request-percent-complete .progress-label").length) {
    //             //set the request status text
    //             $('#drop-request-status-message').text('');
    //
    //             //set progress bar
    //             $('#drop-request-percent-complete .md-hue-2 .md-container.md-mode-determinate .md-bar.md-bar2:first')
    //                 .attr('style','transform: translateX(' + ((percentComplete - 100) / 2) + '%) scale(' + (percentComplete * 0.01) + ', 1);');
    //
    //             //set percentage
    //             $("#drop-request-percent-complete .progress-label:first").text(percentComplete.toFixed(2) + '%');
    //         }
    //         else {
    //             var progressBar = $('#drop-request-percent-complete');
    //
    //             (nfNgBridge.injector.get('$compile')($('<md-progress-linear ng-cloak ng-value="' + percentComplete + '" class="md-hue-2" md-mode="determinate" aria-label="Drop request percent complete"></md-progress-linear>'))(nfNgBridge.rootScope)).appendTo(progressBar);
    //
    //             progressBar.append($('<div class="progress-label"></div>').text(percentComplete + '%'));
    //         }
    //     };
    //
    //     // process the drop request
    //     var refreshProgressBar = function () {
    //         // update the completed percentage
    //         var percentCompleted = 0;
    //         var droppedFlowfiles = 0;
    //         var totalFlowfilesToDrop = 0;
    //
    //         dropRequests.forEach(function (dropRequest) {
    //             if( nfCommon.isDefinedAndNotNull(dropRequest) &&
    //                 nfCommon.isDefinedAndNotNull(dropRequest.droppedCount) &&
    //                 nfCommon.isDefinedAndNotNull(dropRequest.originalCount) ) {
    //                 droppedFlowfiles += dropRequest.droppedCount;
    //                 totalFlowfilesToDrop += dropRequest.originalCount;
    //             }
    //         });
    //
    //         if(totalFlowfilesToDrop !== 0) {
    //             percentCompleted = (droppedFlowfiles / totalFlowfilesToDrop) * 100;
    //         }
    //
    //         setProgressBar(percentCompleted);
    //
    //         // check if can keep on refreshing the progress bar
    //         if (finished !== true && cancelled !== true) {
    //             // wait delay to refresh again
    //             progressBarRefreshTimer = setTimeout(function () {
    //                 // clear the progressBarRefreshTimer timer
    //                 progressBarRefreshTimer = null;
    //
    //                 // schedule to poll the status again in nextDelay
    //                 refreshProgressBar(progressBarRefreshingDelay);
    //             }, progressBarRefreshingDelay);
    //         }
    //     };
    //
    //     var finalizeDropRequest = function (i) {
    //         // tell the server to delete the drop request
    //         if (nfCommon.isDefinedAndNotNull(dropRequests[i])) {
    //             $.ajax({
    //                 type: 'DELETE',
    //                 url: dropRequests[i].uri,
    //                 dataType: 'json'
    //             }).done(function (response) {
    //                 // report the results of this drop request
    //                 dropRequests[i] = response.dropRequest;
    //             }).always(function () {
    //                 // reload the connection status from the server and refreshes the connection UI
    //                 nfConnection.reloadStatus(connections[i].id);
    //                 // resolve this request
    //                 singleEmptyQueuePromises[i].resolve(createSuccessResponse());
    //             });
    //         }
    //     };
    //
    //     // schedule for the next poll iteration
    //     var getDropRequestStatus = function (i) {
    //         $.ajax({
    //             type: 'GET',
    //             url: dropRequests[i].uri,
    //             dataType: 'json'
    //         }).done(function (response) {
    //             dropRequests[i] = response.dropRequest;
    //             processDropRequestResponse(i);
    //         }).fail(function (xhr, status, error) {
    //             singleEmptyQueuePromises[i].resolve(createFailedResponse(xhr,status,error));
    //         });
    //     };
    //
    //     // process the drop request
    //     var processDropRequestResponse = function (i) {
    //         // close the dialog if
    //         if (dropRequests[i].finished === true || cancelled === true) {
    //             finalizeDropRequest(i);
    //         } else {
    //             // wait delay to poll again
    //             dropRequestTimers[i] = setTimeout(function () {
    //                 // clear the drop request timer
    //                 dropRequestTimers[i] = null;
    //                 // schedule to poll the status again in nextDelay
    //                 getDropRequestStatus(i);
    //             }, progressBarRefreshingDelay);
    //         }
    //     };
    //
    //     // empty a single queue and return a deferred representing the emptying process status
    //     var emptyQueueAsync = function (i) {
    //         var deferred = $.Deferred();
    //
    //         // issue the request to delete the flow files
    //         $.ajax({
    //             type: 'POST',
    //             url: '../nifi-api/flowfile-queues/' + encodeURIComponent(connections[i].id) + '/drop-requests',
    //             dataType: 'json',
    //             contentType: 'application/json'
    //         }).done(function (response) {
    //             // process the drop request
    //             dropRequests[i] = response.dropRequest;
    //             processDropRequestResponse(i);
    //         }).fail(function (xhr, status, error) {
    //             deferred.resolve(createFailedResponse(xhr,status,error));
    //         });
    //
    //         return deferred;
    //     };
    //
    //     //start the drop requests
    //     connections.forEach(function (connection,i) {
    //         dropRequests[i] = null;
    //         dropRequestTimers[i] = null;
    //         singleEmptyQueuePromises.push(emptyQueueAsync(i));
    //     });
    //
    //     // initialize the progress bar value and auto refresh it each progressBarRefreshingDelay milliseconds
    //     refreshProgressBar();
    //
    //     // update the button model and header of the drop request status dialog and show it
    //     $('#drop-request-status-dialog')
    //         .modal('setButtonModel', [{
    //             buttonText: 'Stop',
    //             color: {
    //                 base: '#728E9B',
    //                 hover: '#004849',
    //                 text: '#ffffff'
    //             },
    //             handler: {
    //                 click: function () {
    //                     //tell the singleEmptyQueue async jobs that the user cancelled the operation and thus they need to terminate
    //                     cancelled = true;
    //
    //                     // progressBarRefreshTimer !== null means there is a timeout in progress on the refreshProgressBar method
    //                     if (progressBarRefreshTimer !== null) {
    //                         // cancel it
    //                         clearTimeout(progressBarRefreshTimer);
    //                     }
    //                 }
    //             }}]
    //         )
    //         .modal('setHeaderText', actionName)
    //         .modal('show');
    //
    //     $.when.apply($,singleEmptyQueuePromises).then(function () {
    //         var responses = arguments;
    //         finished = true;
    //
    //         if (progressBarRefreshTimer !== null) {
    //             // remove the timeout from the refreshProgressBar method if present
    //             clearTimeout(progressBarRefreshTimer);
    //         }
    //
    //         //hide the status dialog
    //         $('#drop-request-status-dialog').modal('hide');
    //
    //         var droppedFlowfiles = 0;
    //         var droppedFlowfilesSize = 0;
    //         var errorMessages = "";
    //
    //         if(nfCommon.isDefinedAndNotNull(errors)) {
    //             errors.forEach(function (message) {
    //                 errorMessages += '<p>ProcessGroupID ' + message.processGroupId + ': <span style="color: red">' +  nfCommon.escapeHtml(message.errorMessage) + '</span></p>';
    //             });
    //         }
    //
    //         //check for 403 error
    //         for(var i = 0; i < responses.length; i++) {
    //             var response = responses[i];
    //             if(response.success === false && response.xhr.status === 403) {
    //                 errorMessages += '<p>QueueID ' + connections[i].id + ': <span style="color: red">' + response.xhr.status + ' - ' + nfCommon.escapeHtml(response.xhr.responseText) + '</span></p>';
    //             }
    //         }
    //
    //         dropRequests.forEach(function (dropRequest) {
    //             if(nfCommon.isDefinedAndNotNull(dropRequest)) {
    //                 // build the results
    //                 droppedFlowfiles += dropRequest.droppedCount;
    //                 droppedFlowfilesSize += dropRequest.droppedSize;
    //
    //                 // if this request failed show the error
    //                 if (nfCommon.isDefinedAndNotNull(dropRequest.failureReason)) {
    //                     errorMessages += '<p>QueueID ' + dropRequest.id + ': <span style="color: red">' + dropRequest.failureReason + '</span></p>';
    //                 }
    //             }
    //         });
    //
    //         var results = $('<div></div>');
    //
    //         if(droppedFlowfiles !== 0) {
    //             $('<span class="label"></span>').text(droppedFlowfiles).appendTo(results);
    //             $('<span></span>').text(' FlowFiles (' + nfCommon.toReadableBytes(droppedFlowfilesSize) + ')').appendTo(results);
    //             $('<span></span>').text(' were removed from the ' + (connections.length > 1 ? 'queues.' : 'queue.' )).appendTo(results);
    //         }
    //         else {
    //             results.text('No FlowFiles were removed.');
    //         }
    //
    //         if(errorMessages !== "") {
    //             $('<br/><br/><p style="color: darkred">Failed Drop Requests:</p><br/>').appendTo(results);
    //             $('<div style="color: darkred"></div>').html(errorMessages).appendTo(results);
    //         }
    //
    //         // display the results
    //         nfDialog.showOkDialog({
    //             headerText: actionName,
    //             dialogContent: results,
    //             okHandler: function () {
    //                 nfCanvasUtils.reload();
    //             }
    //         });
    //     });
    // };
    //
    // /**
    //  * Return the connections belonging to the specified process group.
    //  *
    //  * @param {string} processGroupId
    //  * @param {boolean} recursive
    //  */
    // var getProcessGroupConnections = function (processGroupId, recursive) {
    //     var deferredResponse = $.Deferred();
    //     var deferredConnectionsResponse = $.Deferred();
    //     var deferredProcessGroupsResponse = $.Deferred();
    //
    //     // get connections
    //     $.ajax({
    //         type: 'GET',
    //         url: '../nifi-api/process-groups/' + encodeURIComponent(processGroupId) + '/connections',
    //         dataType: 'json'
    //     }).done(function (response) {
    //         deferredConnectionsResponse.resolve({
    //             success: true,
    //             response: response.connections
    //         });
    //     }).fail(function (xhr, status, error) {
    //         deferredConnectionsResponse.resolve({
    //             success: false,
    //             xhr: xhr,
    //             status: status,
    //             error: error
    //         });
    //     });
    //
    //     // get process groups if recursive
    //     if(recursive) {
    //         $.ajax({
    //             type: 'GET',
    //             url: '../nifi-api/process-groups/' + encodeURIComponent(processGroupId) + '/process-groups',
    //             dataType: 'json'
    //         }).done(function (response) {
    //             deferredProcessGroupsResponse.resolve({
    //                 success: true,
    //                 response: response.processGroups
    //             });
    //         }).fail(function (xhr, status, error) {
    //             deferredProcessGroupsResponse.resolve({
    //                 success: false,
    //                 xhr: xhr,
    //                 status: status,
    //                 error: error
    //             });
    //         });
    //     }
    //     else {
    //         deferredProcessGroupsResponse.resolve();
    //     }
    //
    //     $.when(deferredConnectionsResponse, deferredProcessGroupsResponse)
    //         .done(function (connectionsResponse, processGroupsResponse) {
    //             var response = {
    //                 connections: [],
    //                 errorMessages: []
    //             };
    //
    //             if(connectionsResponse.success) {
    //                 response.connections = connectionsResponse.response;
    //             }
    //             else {
    //                 response.errorMessages.push({
    //                     processGroupId: processGroupId,
    //                     errorMessage: connectionsResponse.xhr.status + ' - Unable to get queues. ' + connectionsResponse.xhr.responseText
    //                 });
    //             }
    //
    //             if(!recursive) {
    //                 deferredResponse.resolve(response);
    //             }
    //             else {
    //                 if(!processGroupsResponse.success) {
    //                     response.errorMessages.push({
    //                         processGroupId: processGroupId,
    //                         errorMessage: processGroupsResponse.xhr.status + ' - Unable to get process groups. ' + processGroupsResponse.xhr.responseText
    //                     });
    //                     deferredResponse.resolve(response);
    //                 }
    //                 else {
    //                     var requests = processGroupsResponse.response.map(function (processGroup) {
    //                         return getProcessGroupConnections(processGroup.id,true);
    //                     });
    //
    //                     $.when.apply($,requests).then(function () {
    //                         var responses = arguments;
    //
    //                         for(var i = 0; i < responses.length; i++) {
    //                             responses[i].connections.forEach(function (connection) {
    //                                 response.connections.push(connection);
    //                             });
    //
    //                             responses[i].errorMessages.forEach(function (errorMessage) {
    //                                 response.errorMessages.push(errorMessage);
    //                             });
    //                         }
    //
    //                         deferredResponse.resolve(response);
    //                     });
    //                 }
    //             }
    //         });
    //
    //     return deferredResponse;
    // };
    //
    // /**
    //  * Return the connections belonging to the specified process groups.
    //  *
    //  * @param {Array} processGroupIDs
    //  * @param {boolean} recursive
    //  */
    // var getProcessGroupsConnections = function (processGroupIDs, recursive) {
    //     var deferredResponse = $.Deferred();
    //
    //     var deferredResponses = processGroupIDs.map(function (processGroupId) {
    //         return getProcessGroupConnections(processGroupId,recursive);
    //     });
    //
    //     $.when.apply($,deferredResponses).then(function () {
    //         var responses = arguments;
    //
    //         var response = {
    //             connections: [],
    //             errorMessages: []
    //         };
    //
    //         for(var i = 0; i < responses.length; i++) {
    //             responses[i].connections.forEach(function (connection) {
    //                 response.connections.push(connection);
    //             });
    //
    //             responses[i].errorMessages.forEach(function (errorMessage) {
    //                 response.errorMessages.push(errorMessage);
    //             });
    //         }
    //
    //         deferredResponse.resolve(response);
    //     });
    //
    //     return deferredResponse;
    // };

    var nfActions = {
        /**
         * Initializes the actions.
         */
        init: function () {
            initializeDropRequestStatusDialog();
        },

        /**
         * Enters the specified process group.
         *
         * @param {selection} selection     The the currently selected component
         */
        enterGroup: function (selection) {
            if (selection.size() === 1 && nfCanvasUtils.isProcessGroup(selection)) {
                var selectionData = selection.datum();
                nfCanvasUtils.getComponentByType('ProcessGroup').enterGroup(selectionData.id);
            }
        },

        /**
         * Exits the current process group but entering the parent group.
         */
        leaveGroup: function () {
            nfCanvasUtils.getComponentByType('ProcessGroup').enterGroup(nfCanvasUtils.getParentGroupId());
        },

        /**
         * Refresh the flow of the remote process group in the specified selection.
         *
         * @param {selection} selection
         */
        refreshRemoteFlow: function (selection) {
            if (selection.size() === 1 && nfCanvasUtils.isRemoteProcessGroup(selection)) {
                var d = selection.datum();
                var refreshTimestamp = d.component.flowRefreshed;

                var setLastRefreshed = function (lastRefreshed) {
                    // set the new value in case the component is redrawn during the refresh
                    d.component.flowRefreshed = lastRefreshed;

                    // update the UI to show last refreshed if appropriate
                    if (selection.classed('visible')) {
                        selection.select('text.remote-process-group-last-refresh')
                            .text(function () {
                                return lastRefreshed;
                            });
                    }
                };

                var poll = function (nextDelay) {
                    $.ajax({
                        type: 'GET',
                        url: d.uri,
                        dataType: 'json'
                    }).done(function (response) {
                        var remoteProcessGroup = response.component;

                        // the timestamp has not updated yet, poll again
                        if (refreshTimestamp === remoteProcessGroup.flowRefreshed) {
                            schedule(nextDelay);
                        } else {
                            nfRemoteProcessGroup.set(response);

                            // reload the group's connections
                            var connections = nfConnection.getComponentConnections(remoteProcessGroup.id);
                            $.each(connections, function (_, connection) {
                                if (connection.permissions.canRead) {
                                    nfConnection.reload(connection.id);
                                }
                            });
                        }
                    });
                };

                var schedule = function (delay) {
                    if (delay <= 32) {
                        setTimeout(function () {
                            poll(delay * 2);
                        }, delay * 1000);
                    } else {
                        // reset to the previous value since the contents could not be updated (set to null?)
                        setLastRefreshed(refreshTimestamp);
                    }
                };

                setLastRefreshed('Refreshing...');
                poll(1);
            }
        },

        /**
         * Opens the remote process group in the specified selection.
         *
         * @param {selection} selection         The selection
         */
        openUri: function (selection) {
            if (selection.size() === 1 && nfCanvasUtils.isRemoteProcessGroup(selection)) {
                var selectionData = selection.datum();
                var uri = selectionData.component.targetUri;

                if (!nfCommon.isBlank(uri)) {
                    window.open(encodeURI(uri));
                } else {
                    nfDialog.showOkDialog({
                        headerText: 'Remote Process Group',
                        dialogContent: 'No target URI defined.'
                    });
                }
            }
        },

        /**
         * Shows and selects the source of the connection in the specified selection.
         *
         * @param {selection} selection     The selection
         */
        showSource: function (selection) {
            if (selection.size() === 1 && nfCanvasUtils.isConnection(selection)) {
                var selectionData = selection.datum();

                // the source is in the current group
                if (selectionData.sourceGroupId === nfCanvasUtils.getGroupId()) {
                    var source = d3.select('#id-' + selectionData.sourceId);
                    nfActions.show(source);
                } else if (selectionData.sourceType === 'REMOTE_OUTPUT_PORT') {
                    // if the source is remote
                    var remoteSource = d3.select('#id-' + selectionData.sourceGroupId);
                    nfActions.show(remoteSource);
                } else {
                    // if the source is local but in a sub group
                    nfCanvasUtils.showComponent(selectionData.sourceGroupId, selectionData.sourceId);
                }
            }
        },

        /**
         * Shows and selects the destination of the connection in the specified selection.
         *
         * @param {selection} selection     The selection
         */
        showDestination: function (selection) {
            if (selection.size() === 1 && nfCanvasUtils.isConnection(selection)) {
                var selectionData = selection.datum();

                // the destination is in the current group or its remote
                if (selectionData.destinationGroupId === nfCanvasUtils.getGroupId()) {
                    var destination = d3.select('#id-' + selectionData.destinationId);
                    nfActions.show(destination);
                } else if (selectionData.destinationType === 'REMOTE_INPUT_PORT') {
                    // if the destination is remote
                    var remoteDestination = d3.select('#id-' + selectionData.destinationGroupId);
                    nfActions.show(remoteDestination);
                } else {
                    // if the destination is local but in a sub group
                    nfCanvasUtils.showComponent(selectionData.destinationGroupId, selectionData.destinationId);
                }
            }
        },

        /**
         * Shows the downstream components from the specified selection.
         *
         * @param {selection} selection     The selection
         */
        showDownstream: function (selection) {
            if (selection.size() === 1 && !nfCanvasUtils.isConnection(selection)) {

                // open the downstream dialog according to the selection
                if (nfCanvasUtils.isProcessor(selection)) {
                    nfGoto.showDownstreamFromProcessor(selection);
                } else if (nfCanvasUtils.isFunnel(selection)) {
                    nfGoto.showDownstreamFromFunnel(selection);
                } else if (nfCanvasUtils.isInputPort(selection)) {
                    nfGoto.showDownstreamFromInputPort(selection);
                } else if (nfCanvasUtils.isOutputPort(selection)) {
                    nfGoto.showDownstreamFromOutputPort(selection);
                } else if (nfCanvasUtils.isProcessGroup(selection) || nfCanvasUtils.isRemoteProcessGroup(selection)) {
                    nfGoto.showDownstreamFromGroup(selection);
                }
            }
        },

        /**
         * Shows the upstream components from the specified selection.
         *
         * @param {selection} selection     The selection
         */
        showUpstream: function (selection) {
            if (selection.size() === 1 && !nfCanvasUtils.isConnection(selection)) {

                // open the downstream dialog according to the selection
                if (nfCanvasUtils.isProcessor(selection)) {
                    nfGoto.showUpstreamFromProcessor(selection);
                } else if (nfCanvasUtils.isFunnel(selection)) {
                    nfGoto.showUpstreamFromFunnel(selection);
                } else if (nfCanvasUtils.isInputPort(selection)) {
                    nfGoto.showUpstreamFromInputPort(selection);
                } else if (nfCanvasUtils.isOutputPort(selection)) {
                    nfGoto.showUpstreamFromOutputPort(selection);
                } else if (nfCanvasUtils.isProcessGroup(selection) || nfCanvasUtils.isRemoteProcessGroup(selection)) {
                    nfGoto.showUpstreamFromGroup(selection);
                }
            }
        },

        /**
         * Shows and selects the component in the specified selection.
         *
         * @param {selection} selection     The selection
         */
        show: function (selection) {
            // deselect the current selection
            var currentlySelected = nfCanvasUtils.getSelection();
            currentlySelected.classed('selected', false);

            // select only the component/connection in question
            selection.classed('selected', true);

            if (selection.size() === 1) {
                nfActions.center(selection);
            } else {
                nfNgBridge.injector.get('navigateCtrl').zoomFit();
            }

            // update URL deep linking params
            nfCanvasUtils.setURLParameters(nfCanvasUtils.getGroupId(), selection);

            // inform Angular app that values have changed
            nfNgBridge.digest();
        },

        /**
         * Selects all components in the specified selection.
         *
         * @param {selection} selection     Selection of components to select
         */
        select: function (selection) {
            selection.classed('selected', true);
        },

        /**
         * Selects all components.
         */
        selectAll: function () {
            nfActions.select(d3.selectAll('g.component, g.connection'));
        },

        /**
         * Centers the component in the specified selection.
         *
         * @argument {selection} selection      The selection
         */
        center: function (selection) {
            if (selection.size() === 1) {
                var box;
                if (nfCanvasUtils.isConnection(selection)) {
                    var x, y;
                    var d = selection.datum();

                    // get the position of the connection label
                    if (d.bends.length > 0) {
                        var i = Math.min(Math.max(0, d.labelIndex), d.bends.length - 1);
                        x = d.bends[i].x;
                        y = d.bends[i].y;
                    } else {
                        x = (d.start.x + d.end.x) / 2;
                        y = (d.start.y + d.end.y) / 2;
                    }

                    box = {
                        x: x,
                        y: y,
                        width: 1,
                        height: 1
                    };
                } else {
                    var selectionData = selection.datum();
                    var selectionPosition = selectionData.position;

                    box = {
                        x: selectionPosition.x,
                        y: selectionPosition.y,
                        width: selectionData.dimensions.width,
                        height: selectionData.dimensions.height
                    };
                }

                // center on the component
                nfCanvasUtils.centerBoundingBox(box);
            }
        },

        /**
         * Enables all eligible selected components.
         *
         * @argument {selection} selection      The selection
         */
        enable: function (selection) {
            if (selection.empty()) {
                // build the entity
                var entity = {
                    'id': nfCanvasUtils.getGroupId(),
                    'state': 'ENABLED'
                };

                updateResource(config.urls.api + '/flow/process-groups/' + encodeURIComponent(nfCanvasUtils.getGroupId()), entity).done(updateProcessGroup);
            } else {
                var componentsToEnable = nfCanvasUtils.filterEnable(selection);

                if (!componentsToEnable.empty()) {
                    var enableRequests = [];

                    // enable the selected processors
                    componentsToEnable.each(function (d) {
                        var selected = d3.select(this);

                        // prepare the request
                        var uri, entity;
                        if (nfCanvasUtils.isProcessGroup(selected)) {
                            uri = config.urls.api + '/flow/process-groups/' + encodeURIComponent(d.id);
                            entity = {
                                'id': d.id,
                                'state': 'ENABLED'
                            }
                        } else {
                            uri = d.uri + '/run-status';
                            entity = {
                                'revision': nfClient.getRevision(d),
                                'state': 'STOPPED'
                            };
                        }

                        enableRequests.push(updateResource(uri, entity).done(function (response) {
                            if (nfCanvasUtils.isProcessGroup(selected)) {
                                nfCanvasUtils.getComponentByType('ProcessGroup').reload(d.id);
                            } else {
                                nfCanvasUtils.getComponentByType(d.type).set(response);
                            }
                        }));
                    });

                    // inform Angular app once the updates have completed
                    if (enableRequests.length > 0) {
                        $.when.apply(window, enableRequests).always(function () {
                            nfNgBridge.digest();
                        });
                    }
                }
            }
        },

        /**
         * Disables all eligible selected components.
         *
         * @argument {selection} selection      The selection
         */
        disable: function (selection) {
            if (selection.empty()) {
                // build the entity
                var entity = {
                    'id': nfCanvasUtils.getGroupId(),
                    'state': 'DISABLED'
                };

                updateResource(config.urls.api + '/flow/process-groups/' + encodeURIComponent(nfCanvasUtils.getGroupId()), entity).done(updateProcessGroup);
            } else {
                var componentsToDisable = nfCanvasUtils.filterDisable(selection);

                if (!componentsToDisable.empty()) {
                    var disableRequests = [];

                    // disable the selected components
                    componentsToDisable.each(function (d) {
                        var selected = d3.select(this);

                        // prepare the request
                        var uri, entity;
                        if (nfCanvasUtils.isProcessGroup(selected)) {
                            uri = config.urls.api + '/flow/process-groups/' + encodeURIComponent(d.id);
                            entity = {
                                'id': d.id,
                                'state': 'DISABLED'
                            }
                        } else {
                            uri = d.uri + "/run-status";
                            entity = {
                                'revision': nfClient.getRevision(d),
                                'state': 'DISABLED'
                            };
                        }

                        disableRequests.push(updateResource(uri, entity).done(function (response) {
                            if (nfCanvasUtils.isProcessGroup(selected)) {
                                nfCanvasUtils.getComponentByType('ProcessGroup').reload(d.id);
                            } else {
                                nfCanvasUtils.getComponentByType(d.type).set(response);
                            }
                        }));
                    });

                    // inform Angular app once the updates have completed
                    if (disableRequests.length > 0) {
                        $.when.apply(window, disableRequests).always(function () {
                            nfNgBridge.digest();
                        });
                    }
                }
            }
        },

        /**
         * Opens provenance with the component in the specified selection.
         *
         * @argument {selection} selection The selection
         */
        openProvenance: function (selection) {
            if (selection.size() === 1) {
                var selectionData = selection.datum();

                // open the provenance page with the specified component
                nfShell.showPage('provenance?' + $.param({
                        componentId: selectionData.id
                    }));
            }
        },

        /**
         * Starts the components in the specified selection.
         *
         * @argument {selection} selection      The selection
         */
        start: function (selection) {
            if (selection.empty()) {
                // build the entity
                var entity = {
                    'id': nfCanvasUtils.getGroupId(),
                    'state': 'RUNNING'
                };

                updateResource(config.urls.api + '/flow/process-groups/' + encodeURIComponent(nfCanvasUtils.getGroupId()), entity).done(updateProcessGroup);
            } else {
                var componentsToStart = selection.filter(function (d) {
                    return nfCanvasUtils.isRunnable(d3.select(this));
                });

                // ensure there are startable components selected
                if (!componentsToStart.empty()) {
                    var startRequests = [];

                    // start each selected component
                    componentsToStart.each(function (d) {
                        var selected = d3.select(this);

                        // prepare the request
                        var uri, entity;
                        if (nfCanvasUtils.isProcessGroup(selected)) {
                            uri = config.urls.api + '/flow/process-groups/' + encodeURIComponent(d.id);
                            entity = {
                                'id': d.id,
                                'state': 'RUNNING'
                            }
                        } else {
                            uri = d.uri + '/run-status';
                            entity = {
                                'revision': nfClient.getRevision(d),
                                'state': 'RUNNING'
                            };
                        }

                        startRequests.push(updateResource(uri, entity).done(function (response) {
                            if (nfCanvasUtils.isProcessGroup(selected)) {
                                nfCanvasUtils.getComponentByType('ProcessGroup').reload(d.id);
                            } else {
                                nfCanvasUtils.getComponentByType(d.type).set(response);
                            }
                        }));
                    });

                    // inform Angular app once the updates have completed
                    if (startRequests.length > 0) {
                        $.when.apply(window, startRequests).always(function () {
                            nfNgBridge.digest();
                        });
                    }
                }
            }
        },

        /**
         * Stops the components in the specified selection.
         *
         * @argument {selection} selection      The selection
         * @argument {cb} callback              The function to call when request is processed
         */
        stop: function (selection,cb) {
            if (selection.empty()) {
                // build the entity
                var entity = {
                    'id': nfCanvasUtils.getGroupId(),
                    'state': 'STOPPED'
                };

                updateResource(config.urls.api + '/flow/process-groups/' + encodeURIComponent(nfCanvasUtils.getGroupId()), entity).done(updateProcessGroup);
            } else {
                var componentsToStop = selection.filter(function (d) {
                    return nfCanvasUtils.isStoppable(d3.select(this));
                });

                // ensure there are some component to stop
                if (!componentsToStop.empty()) {
                    var stopRequests = [];

                    // stop each selected component
                    componentsToStop.each(function (d) {
                        var selected = d3.select(this);

                        // prepare the request
                        var uri, entity;
                        if (nfCanvasUtils.isProcessGroup(selected)) {
                            uri = config.urls.api + '/flow/process-groups/' + encodeURIComponent(d.id);
                            entity = {
                                'id': d.id,
                                'state': 'STOPPED'
                            };
                        } else {
                            uri = d.uri + '/run-status';
                            entity = {
                                'revision': nfClient.getRevision(d),
                                'state': 'STOPPED'
                            };
                        }

                        stopRequests.push(updateResource(uri, entity).done(function (response) {
                            if (nfCanvasUtils.isProcessGroup(selected)) {
                                nfCanvasUtils.getComponentByType('ProcessGroup').reload(d.id);
                            } else {
                                nfCanvasUtils.getComponentByType(d.type).set(response);
                            }
                        }));
                    });

                    // inform Angular app once the updates have completed
                    if (stopRequests.length > 0) {
                        $.when.apply(window, stopRequests).always(function () {
                            nfNgBridge.digest();
                            if(typeof cb == 'function'){
                                cb();
                            }
                        });
                    } else if(typeof cb == 'function'){
                         cb();
                    }
                }
            }
        },

        /**
         * Stops the component and displays the processor configuration dialog
         *
         * @param {selection} selection      The selection
         * @param {cb} callback              The function to call when complete
         */
        stopAndConfigure: function (selection,cb) {
            if(selection.size() === 1 &&
                nfCanvasUtils.isProcessor(selection) &&
                nfCanvasUtils.canModify(selection)){

                nfActions.stop(selection,function(){
                    nfProcessorConfiguration.showConfiguration(selection,cb);
                });
            }
        },

        /**
         * Terminates active threads for the selected component.
         *
         * @param {selection}       selection
         * @param {cb} callback     The function to call when complete
         */
        terminate: function (selection,cb) {
            if (selection.size() === 1 && nfCanvasUtils.isProcessor(selection)) {
                var selectionData = selection.datum();

                $.ajax({
                    type: 'DELETE',
                    url: selectionData.uri + '/threads',
                    dataType: 'json'
                }).done(function (response) {
                    nfProcessor.set(response);
                    if(typeof cb == 'function'){
                        cb();
                    }
                }).fail(nfErrorHandler.handleAjaxError);
            }
        },

        /**
         * Enables transmission for the components in the specified selection.
         *
         * @argument {selection} selection      The selection
         */
        enableTransmission: function (selection) {
            var componentsToEnable = selection.filter(function (d) {
                return nfCanvasUtils.canStartTransmitting(d3.select(this));
            });

            // start each selected component
            componentsToEnable.each(function (d) {
                // build the entity
                var entity = {
                    'revision': nfClient.getRevision(d),
                    'state': 'TRANSMITTING'
                };

                // start transmitting
                updateResource(d.uri + '/run-status', entity).done(function (response) {
                    nfRemoteProcessGroup.set(response);
                });
            });
        },

        /**
         * Disables transmission for the components in the specified selection.
         *
         * @argument {selection} selection      The selection
         */
        disableTransmission: function (selection) {
            var componentsToDisable = selection.filter(function (d) {
                return nfCanvasUtils.canStopTransmitting(d3.select(this));
            });

            // stop each selected component
            componentsToDisable.each(function (d) {
                // build the entity
                var entity = {
                    'revision': nfClient.getRevision(d),
                    'state': 'STOPPED'
                };

                updateResource(d.uri + '/run-status', entity).done(function (response) {
                    nfRemoteProcessGroup.set(response);
                });
            });
        },

        /**
         * Shows the configuration dialog for the specified selection.
         *
         * @param {selection} selection     Selection of the component to be configured
         * @param {fn} callback             Callback
         */
        showConfiguration: function (selection,cb) {
            if (selection.empty()) {
                nfProcessGroupConfiguration.showConfiguration(nfCanvasUtils.getGroupId());
            } else if (selection.size() === 1) {
                var selectionData = selection.datum();
                if (nfCanvasUtils.isProcessor(selection)) {
                    nfProcessorConfiguration.showConfiguration(selection,cb);
                } else if (nfCanvasUtils.isLabel(selection)) {
                    nfLabelConfiguration.showConfiguration(selection);
                } else if (nfCanvasUtils.isProcessGroup(selection)) {
                    nfProcessGroupConfiguration.showConfiguration(selectionData.id);
                } else if (nfCanvasUtils.isRemoteProcessGroup(selection)) {
                    nfRemoteProcessGroupConfiguration.showConfiguration(selection);
                } else if (nfCanvasUtils.isInputPort(selection) || nfCanvasUtils.isOutputPort(selection)) {
                    nfPortConfiguration.showConfiguration(selection);
                } else if (nfCanvasUtils.isConnection(selection)) {
                    nfConnectionConfiguration.showConfiguration(selection);
                }
            }
        },

        /**
         * Opens the policy management page for the selected component.
         *
         * @param selection
         */
        managePolicies: function(selection) {
            if (selection.size() <= 1) {
                nfPolicyManagement.showComponentPolicy(selection);
            }
        },

        // Defines an action for showing component details (like configuration but read only).
        showDetails: function (selection) {
            if (selection.empty()) {
                nfProcessGroupConfiguration.showConfiguration(nfCanvasUtils.getGroupId());
            } else if (selection.size() === 1) {
                var selectionData = selection.datum();
                if (nfCanvasUtils.isProcessor(selection)) {
                    if(!nfCanvasUtils.isStoppable(selection) && nfCanvasUtils.canModify(selection)){
                        nfProcessorConfiguration.showConfiguration(selection);
                    } else {
                        nfProcessorDetails.showDetails(nfCanvasUtils.getGroupId(), selectionData.id);
                    }
                } else if (nfCanvasUtils.isProcessGroup(selection)) {
                    nfProcessGroupConfiguration.showConfiguration(selectionData.id);
                } else if (nfCanvasUtils.isRemoteProcessGroup(selection)) {
                    nfRemoteProcessGroupDetails.showDetails(selection);
                } else if (nfCanvasUtils.isInputPort(selection) || nfCanvasUtils.isOutputPort(selection)) {
                    nfPortDetails.showDetails(selection);
                } else if (nfCanvasUtils.isConnection(selection)) {
                    nfConnectionDetails.showDetails(nfCanvasUtils.getGroupId(), selectionData.id);
                }
            }
        },

        /**
         * Shows the usage documentation for the component in the specified selection.
         *
         * @param {selection} selection     The selection
         */
        showUsage: function (selection) {
            if (selection.size() === 1 && nfCanvasUtils.isProcessor(selection)) {
                var selectionData = selection.datum();
                nfShell.showPage('../nifi-docs/documentation?' + $.param({
                        select: selectionData.component.type,
                        group: selectionData.component.bundle.group,
                        artifact: selectionData.component.bundle.artifact,
                        version: selectionData.component.bundle.version
                    }));
            }
        },

        /**
         * Shows the stats for the specified selection.
         *
         * @argument {selection} selection      The selection
         */
        showStats: function (selection) {
            if (selection.size() === 1) {
                var selectionData = selection.datum();
                if (nfCanvasUtils.isProcessor(selection)) {
                    nfStatusHistory.showProcessorChart(nfCanvasUtils.getGroupId(), selectionData.id);
                } else if (nfCanvasUtils.isProcessGroup(selection)) {
                    nfStatusHistory.showProcessGroupChart(nfCanvasUtils.getGroupId(), selectionData.id);
                } else if (nfCanvasUtils.isRemoteProcessGroup(selection)) {
                    nfStatusHistory.showRemoteProcessGroupChart(nfCanvasUtils.getGroupId(), selectionData.id);
                } else if (nfCanvasUtils.isConnection(selection)) {
                    nfStatusHistory.showConnectionChart(nfCanvasUtils.getGroupId(), selectionData.id);
                }
            }
        },

        /**
         * Opens the remote ports dialog for the remote process group in the specified selection.
         *
         * @param {selection} selection         The selection
         */
        remotePorts: function (selection) {
            if (selection.size() === 1 && nfCanvasUtils.isRemoteProcessGroup(selection)) {
                nfRemoteProcessGroupPorts.showPorts(selection);
            }
        },

        /**
         * Reloads the status for the entire canvas (components and flow.)
         */
        reload: function () {
            nfCanvasUtils.reload();
        },

        /**
         * Deletes the component in the specified selection.
         *
         * @param {selection} selection     The selection containing the component to be removed
         */
        'delete': function (selection) {
            if (nfCommon.isUndefined(selection) || selection.empty()) {
                nfDialog.showOkDialog({
                    headerText: 'Delete Components',
                    dialogContent: 'No eligible components are selected. Please select the components to be deleted.'
                });
            } else {
                if (selection.size() === 1) {
                    var selectionData = selection.datum();
                    var revision = nfClient.getRevision(selectionData);

                    $.ajax({
                        type: 'DELETE',
                        url: selectionData.uri + '?' + $.param({
                            'version': revision.version,
                            'clientId': revision.clientId,
                            'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
                        }),
                        dataType: 'json'
                    }).done(function (response) {
                        // remove the component/connection in question
                        nfCanvasUtils.getComponentByType(selectionData.type).remove(selectionData.id);

                        // if the selection is a connection, reload the source and destination accordingly
                        if (nfCanvasUtils.isConnection(selection) === false) {
                            var connections = nfConnection.getComponentConnections(selectionData.id);
                            if (connections.length > 0) {
                                var ids = [];
                                $.each(connections, function (_, connection) {
                                    ids.push(connection.id);
                                });

                                // remove the corresponding connections
                                nfConnection.remove(ids);
                            }
                        }

                        // update URL deep linking params
                        nfCanvasUtils.setURLParameters();

                        // refresh the birdseye
                        nfBirdseye.refresh();
                        // inform Angular app values have changed
                        nfNgBridge.digest();
                    }).fail(nfErrorHandler.handleAjaxError);
                } else {
                    var parentGroupId = nfCanvasUtils.getGroupId();

                    // create a snippet for the specified component and link to the data flow
                    var snippet = nfSnippet.marshal(selection, parentGroupId);
                    nfSnippet.create(snippet).done(function (response) {
                        // remove the snippet, effectively removing the components
                        nfSnippet.remove(response.snippet.id).done(function () {
                            var components = d3.map();

                            // add the id to the type's array
                            var addComponent = function (type, id) {
                                if (!components.has(type)) {
                                    components.set(type, []);
                                }
                                components.get(type).push(id);
                            };

                            // go through each component being removed
                            selection.each(function (d) {
                                // remove the corresponding entry
                                addComponent(d.type, d.id);

                                // if this is not a connection, see if it has any connections that need to be removed
                                if (d.type !== 'Connection') {
                                    var connections = nfConnection.getComponentConnections(d.id);
                                    if (connections.length > 0) {
                                        $.each(connections, function (_, connection) {
                                            addComponent('Connection', connection.id);
                                        });
                                    }
                                }
                            });

                            // remove all the non connections in the snippet first
                            components.each(function (ids, type) {
                                if (type !== 'Connection') {
                                    nfCanvasUtils.getComponentByType(type).remove(ids);
                                }
                            });

                            // then remove all the connections
                            if (components.has('Connection')) {
                                nfConnection.remove(components.get('Connection'));
                            }

                            // update URL deep linking params
                            nfCanvasUtils.setURLParameters();

                            // refresh the birdseye
                            nfBirdseye.refresh();

                            // inform Angular app values have changed
                            nfNgBridge.digest();
                        }).fail(nfErrorHandler.handleAjaxError);
                    }).fail(nfErrorHandler.handleAjaxError);
                }
            }
        },

        /**
         * Deletes the flow files inside the selected connections.
         *
         * @param {type} selection
         */
        emptySelectedQueues: function (selection) {
            var connectionsToEmpty = selection.filter(function (d) {
                var selectionItem = d3.select(this);
                return nfCanvasUtils.isConnection(selectionItem);
            }).data().map(function (selectedConnection) {
                return {
                    componentId: selectedConnection.id,
                    isProcessGroup: false
                };
            });

            var actionName = selection.size() > 1 ? 'Empty Selected Queues' : 'Empty Selected Queue';

            var dialogContent = selection.size() > 1
                ? 'Are you sure you want to empty the selected queues? All FlowFiles waiting at the time of the request will be removed.'
                : 'Are you sure you want to empty the selected queue? All FlowFiles waiting at the time of the request will be removed.';

            // prompt the user before emptying the queue
            nfDialog.showYesNoDialog({
                headerText: actionName,
                dialogContent: dialogContent,
                noText: 'Cancel',
                yesText: 'Empty',
                yesHandler: function () {
                    emptyQueues(actionName,connectionsToEmpty,false);
                }
            });
        },

        /**
         * Empty all the queues inside the selected process groups.
         *
         * @param {type} selection
         */
        emptyProcessGroupsQueues: function (selection) {
            var selectionSize = selection.size();
            var componentsToEmpty = [];
            var actionName = '';
            var dialogContent = '';

            if(selectionSize === 0) {
                actionName = 'Empty Current Process Group Queues';
                dialogContent = 'Are you sure you want to empty all queues inside the current process group? All FlowFiles waiting at the time of the request will be removed.';
                componentsToEmpty = d3.selectAll('g.connection')
                    .data().map(function (connection) {
                        return {
                            componentId: connection.id,
                            isProcessGroup: false
                        };
                    });

                if(componentsToEmpty.length === 0) {
                    // display the "no queues to empty" dialog
                    nfDialog.showOkDialog({
                        headerText: actionName,
                        dialogContent: 'No queues to empty.'
                    });
                    return;
                }
            }
            else if(selectionSize === 1) {
                actionName = 'Empty Selected Process Group Queues';
                dialogContent = 'Are you sure you want to empty all queues inside the selected process group? All FlowFiles waiting at the time of the request will be removed.';
            }
            else {
                actionName = 'Empty Selected Process Groups Queues';
                dialogContent = 'Are you sure you want to empty all queues inside the selected process groups? All FlowFiles waiting at the time of the request will be removed.';
            }

            // prompt the user before emptying the queue
            nfDialog.showYesNoDialog({
                headerText: actionName,
                dialogContent: dialogContent,
                noText: 'Cancel',
                yesText: 'Empty',
                yesHandler: function () {
                    if(selectionSize === 0) {
                        emptyQueues(actionName,componentsToEmpty,false);
                    }
                    else {
                        componentsToEmpty = selection.filter(function (d) {
                            var selectionItem = d3.select(this);
                            return nfCanvasUtils.isProcessGroup(selectionItem);
                        }).data().map(function (selectedProcessGroup) {
                            return {
                                componentId: selectedProcessGroup.id,
                                isProcessGroup: true
                            };
                        });

                        emptyQueues(actionName,componentsToEmpty,false);
                    }
                }
            });
        },

        emptyProcessGroupsQueuesRecursive: function (selection) {
            var selectionSize = selection.size();
            var componentsToEmpty = [];
            var actionName = '';
            var dialogContent = '';

            if(selectionSize === 0) {
                actionName = 'Empty Current Process Group Queues (Recursive)';
                dialogContent = 'Are you sure you want to empty all queues inside the current process group and all its sub process groups (recursive)? All FlowFiles waiting at the time of the request will be removed.';
                componentsToEmpty = d3.selectAll('g.connection')
                    .data().map(function (connection) {
                        return {
                            componentId: connection.id,
                            isProcessGroup: false
                        };
                    });
            }
            else if(selectionSize === 1) {
                actionName = 'Empty Selected Process Group Queues (Recursive)';
                dialogContent = 'Are you sure you want to empty all queues inside the selected process group and all its sub process groups (recursive)? All FlowFiles waiting at the time of the request will be removed.';
            }
            else {
                actionName = 'Empty Selected Process Groups Queues (Recursive)';
                dialogContent = 'Are you sure you want to empty all queues inside the selected process groups and all their sub process groups (recursive)? All FlowFiles waiting at the time of the request will be removed.';
            }

            // prompt the user before emptying the queue
            nfDialog.showYesNoDialog({
                headerText: actionName,
                dialogContent: dialogContent,
                noText: 'Cancel',
                yesText: 'Empty',
                yesHandler: function () {
                    if(selectionSize === 0) {
                        componentsToEmpty = componentsToEmpty.concat(d3.selectAll('g.process-group')
                            .data()
                            .map(function (processGroup) {
                                return {
                                    componentId: processGroup.id,
                                    isProcessGroup: true
                                };
                            })
                        );
                    }
                    else {
                        componentsToEmpty = selection.filter(function (d) {
                            var selectionItem = d3.select(this);
                            return nfCanvasUtils.isProcessGroup(selectionItem);
                        }).data().map(function (processGroup) {
                            return {
                                componentId: processGroup.id,
                                isProcessGroup: true
                            };
                        });
                    }

                    emptyQueues(actionName,componentsToEmpty,true);
                }
            });
        },

        /**
         * Lists the flow files in the specified connection.
         *
         * @param {selection} selection
         */
        listQueue: function (selection) {
            if (selection.size() !== 1 || !nfCanvasUtils.isConnection(selection)) {
                return;
            }

            // get the connection data
            var connection = selection.datum();

            // list the flow files in the specified connection
            nfQueueListing.listQueue(connection);
        },

        /**
         * Views the state for the specified processor.
         *
         * @param {selection} selection
         */
        viewState: function (selection) {
            if (selection.size() !== 1 || !nfCanvasUtils.isProcessor(selection)) {
                return;
            }

            // get the processor data
            var processor = selection.datum();

            // view the state for the selected processor
            nfComponentState.showState(processor, nfCanvasUtils.isConfigurable(selection));
        },

        /**
         * Shows the flow version dialog.
         */
        saveFlowVersion: function (selection) {
            if (selection.empty()) {
                nfFlowVersion.showFlowVersionDialog(nfCanvasUtils.getGroupId(), 'COMMIT');
            } else if (selection.size() === 1) {
                var selectionData = selection.datum();
                if (nfCanvasUtils.isProcessGroup(selection)) {
                    nfFlowVersion.showFlowVersionDialog(selectionData.id, 'COMMIT');
                }
            }
        },

        /**
         * Confirms force save and shows the flow version dialog.
         */
        forceSaveFlowVersion: function (selection) {
            nfDialog.showYesNoDialog({
                headerText: 'Commit',
                dialogContent: 'Committing will ignore available upgrades and commit local changes as the next version. Are you sure you want to proceed?',
                noText: 'Cancel',
                yesText: 'Yes',
                yesHandler: function () {
                    if (selection.empty()) {
                        nfFlowVersion.showFlowVersionDialog(nfCanvasUtils.getGroupId(),'FORCE_COMMIT');
                    } else if (selection.size() === 1) {
                        var selectionData = selection.datum();
                        if (nfCanvasUtils.isProcessGroup(selection)) {
                            nfFlowVersion.showFlowVersionDialog(selectionData.id, 'FORCE_COMMIT');
                        }
                    };
                }
            });
        },

        /**
         * Reverts local changes.
         */
        revertLocalChanges: function (selection) {
            if (selection.empty()) {
                nfFlowVersion.revertLocalChanges(nfCanvasUtils.getGroupId());
            } else if (selection.size() === 1) {
                var selectionData = selection.datum();
                nfFlowVersion.revertLocalChanges(selectionData.id);
            }
        },

        /**
         * Shows local changes.
         */
        showLocalChanges: function (selection) {
            if (selection.empty()) {
                nfFlowVersion.showLocalChanges(nfCanvasUtils.getGroupId());
            } else if (selection.size() === 1) {
                var selectionData = selection.datum();
                nfFlowVersion.showLocalChanges(selectionData.id)
            }
        },

        /**
         * Changes the flow version.
         */
        changeFlowVersion: function (selection) {
            if (selection.empty()) {
                nfFlowVersion.showChangeFlowVersionDialog(nfCanvasUtils.getGroupId());
            } else if (selection.size() === 1) {
                var selectionData = selection.datum();
                if (nfCanvasUtils.isProcessGroup(selection)) {
                    nfFlowVersion.showChangeFlowVersionDialog(selectionData.id);
                }
            }
        },

        /**
         * Disconnects a Process Group from flow versioning.
         */
        stopVersionControl: function (selection) {
            if (selection.empty()) {
                nfFlowVersion.stopVersionControl(nfCanvasUtils.getGroupId());
            } else if (selection.size() === 1) {
                var selectionData = selection.datum();
                nfFlowVersion.stopVersionControl(selectionData.id);
            }
        },

        /**
         * Opens the variable registry for the specified selection of the current group if the selection is emtpy.
         *
         * @param {selection} selection
         */
        openVariableRegistry: function (selection) {
            if (selection.empty()) {
                nfVariableRegistry.showVariables(nfCanvasUtils.getGroupId());
            } else if (selection.size() === 1) {
                var selectionData = selection.datum();
                if (nfCanvasUtils.isProcessGroup(selection)) {
                    nfVariableRegistry.showVariables(selectionData.id);
                }
            }
        },

        /**
         * Opens the parameter context for the specified selection of the current group if the selection is empty.
         *
         * @param {selection} selection
         */
        openParameterContext: function (selection) {
            var pcid;
            if (selection.empty()) {
                pcid = nfCanvasUtils.getParameterContextId();
            } else if (selection.size() === 1) {
                if (nfCanvasUtils.isProcessGroup(selection)) {
                    var pg = selection.datum();
                    pcid = pg.component.parameterContext.id;
                }
            }

            if (nfCommon.isDefinedAndNotNull(pcid)) {
                nfParameterContexts.showParameterContext(pcid);
            }
        },

        /**
         * Views the state for the specified processor.
         *
         * @param {selection} selection
         */
        changeVersion: function (selection) {
            if (selection.size() !== 1 || !nfCanvasUtils.isProcessor(selection)) {
                return;
            }

            // get the processor data
            var processor = selection.datum();

            // attempt to change the version of the specified component
            nfComponentVersion.promptForVersionChange(processor);
        },

        /**
         * Aligns the components in the specified selection vertically along the center of the components.
         *
         * @param {array} selection      The selection
         */
        alignVertical: function (selection) {
            var updates = d3.map();
            // ensure every component is writable
            if (nfCanvasUtils.canModify(selection) === false) {
                nfDialog.showOkDialog({
                    headerText: 'Component Position',
                    dialogContent: 'Must be authorized to modify every component selected.'
                });
                return;
            }
            // determine the extent
            var minX = null, maxX = null;
            selection.each(function (d) {
                if (d.type !== "Connection") {
                    if (minX === null || d.position.x < minX) {
                        minX = d.position.x;
                    }
                    var componentMaxX = d.position.x + d.dimensions.width;
                    if (maxX === null || componentMaxX > maxX) {
                        maxX = componentMaxX;
                    }
                }
            });
            var center = (minX + maxX) / 2;

            // align all components left
            selection.each(function(d) {
                if (d.type !== "Connection") {
                    var delta = {
                        x: center - (d.position.x + d.dimensions.width / 2),
                        y: 0
                    };
                    // if this component is already centered, no need to updated it
                    if (delta.x !== 0) {
                        // consider any connections
                        var connections = nfConnection.getComponentConnections(d.id);
                        $.each(connections, function(_, connection) {
                            var connectionSelection = d3.select('#id-' + connection.id);

                            if (!updates.has(connection.id) && nfCanvasUtils.getConnectionSourceComponentId(connection) === nfCanvasUtils.getConnectionDestinationComponentId(connection)) {
                                // this connection is self looping and hasn't been updated by the delta yet
                                var connectionUpdate = nfDraggable.updateConnectionPosition(nfConnection.get(connection.id), delta);
                                if (connectionUpdate !== null) {
                                    updates.set(connection.id, connectionUpdate);
                                }
                            } else if (!updates.has(connection.id) && connectionSelection.classed('selected') && nfCanvasUtils.canModify(connectionSelection)) {
                                // this is a selected connection that hasn't been updated by the delta yet
                                if (nfCanvasUtils.getConnectionSourceComponentId(connection) === d.id || !isSourceSelected(connection, selection)) {
                                    // the connection is either outgoing or incoming when the source of the connection is not part of the selection
                                    var connectionUpdate = nfDraggable.updateConnectionPosition(nfConnection.get(connection.id), delta);
                                    if (connectionUpdate !== null) {
                                        updates.set(connection.id, connectionUpdate);
                                    }
                                }
                            }
                        });
                        updates.set(d.id, nfDraggable.updateComponentPosition(d, delta));
                    }
                }
            });
            nfDraggable.refreshConnections(updates);
        },

        /**
         * Aligns the components in the specified selection horizontally along the center of the components.
         *
         * @param {array} selection      The selection
         */
        alignHorizontal: function (selection) {
            var updates = d3.map();
            // ensure every component is writable
            if (nfCanvasUtils.canModify(selection) === false) {
                nfDialog.showOkDialog({
                    headerText: 'Component Position',
                    dialogContent: 'Must be authorized to modify every component selected.'
                });
                return;
            }

            // determine the extent
            var minY = null, maxY = null;
            selection.each(function (d) {
                if (d.type !== "Connection") {
                    if (minY === null || d.position.y < minY) {
                        minY = d.position.y;
                    }
                    var componentMaxY = d.position.y + d.dimensions.height;
                    if (maxY === null || componentMaxY > maxY) {
                        maxY = componentMaxY;
                    }
                }
            });
            var center = (minY + maxY) / 2;

            // align all components with top most component
            selection.each(function(d) {
                if (d.type !== "Connection") {
                    var delta = {
                        x: 0,
                        y: center - (d.position.y + d.dimensions.height / 2)
                    };

                    // if this component is already centered, no need to updated it
                    if (delta.y !== 0) {
                        // consider any connections
                        var connections = nfConnection.getComponentConnections(d.id);
                        $.each(connections, function(_, connection) {
                            var connectionSelection = d3.select('#id-' + connection.id);

                            if (!updates.has(connection.id) && nfCanvasUtils.getConnectionSourceComponentId(connection) === nfCanvasUtils.getConnectionDestinationComponentId(connection)) {
                                // this connection is self looping and hasn't been updated by the delta yet
                                var connectionUpdate = nfDraggable.updateConnectionPosition(nfConnection.get(connection.id), delta);
                                if (connectionUpdate !== null) {
                                    updates.set(connection.id, connectionUpdate);
                                }
                            } else if (!updates.has(connection.id) && connectionSelection.classed('selected') && nfCanvasUtils.canModify(connectionSelection)) {
                                // this is a selected connection that hasn't been updated by the delta yet
                                if (nfCanvasUtils.getConnectionSourceComponentId(connection) === d.id || !isSourceSelected(connection, selection)) {
                                    // the connection is either outgoing or incoming when the source of the connection is not part of the selection
                                    var connectionUpdate = nfDraggable.updateConnectionPosition(nfConnection.get(connection.id), delta);
                                    if (connectionUpdate !== null) {
                                        updates.set(connection.id, connectionUpdate);
                                    }
                                }
                            }
                        });
                        updates.set(d.id, nfDraggable.updateComponentPosition(d, delta));
                    }
                }
            });

            nfDraggable.refreshConnections(updates);
        },

        /**
         * Opens the fill color dialog for the component in the specified selection.
         *
         * @param {type} selection      The selection
         */
        fillColor: function (selection) {
            if (nfCanvasUtils.isColorable(selection)) {
                // we know that the entire selection is processors or labels... this
                // checks if the first item is a processor... if true, all processors
                var allProcessors = nfCanvasUtils.isProcessor(selection);

                var color;
                if (allProcessors) {
                    color = nfProcessor.defaultFillColor();
                } else {
                    color = nfLabel.defaultColor();
                }

                // if there is only one component selected, get its color otherwise use default
                if (selection.size() === 1) {
                    var selectionData = selection.datum();

                    // use the specified color if appropriate
                    if (nfCommon.isDefinedAndNotNull(selectionData.component.style['background-color'])) {
                        color = selectionData.component.style['background-color'];
                    }
                }

                // set the color
                $('#fill-color').minicolors('value', color);

                // update the preview visibility
                if (allProcessors) {
                    $('#fill-color-processor-preview').show();
                    $('#fill-color-label-preview').hide();
                } else {
                    $('#fill-color-processor-preview').hide();
                    $('#fill-color-label-preview').show();
                }

                // show the dialog
                $('#fill-color-dialog').modal('show');
            }
        },

        /**
         * Groups the currently selected components into a new group.
         */
        group: function () {
            var selection = nfCanvasUtils.getSelection();

            // ensure that components have been specified
            if (selection.empty()) {
                return;
            }

            // determine the origin of the bounding box for the selected components
            var origin = nfCanvasUtils.getOrigin(selection);

            var pt = {'x': origin.x, 'y': origin.y};
            $.when(nfNgBridge.injector.get('groupComponent').promptForGroupName(pt, false)).done(function (processGroup) {
                var group = d3.select('#id-' + processGroup.id);
                nfCanvasUtils.moveComponents(selection, group);
            });
        },

        /**
         * Moves the currently selected component into the current parent group.
         */
        moveIntoParent: function () {
            var selection = nfCanvasUtils.getSelection();

            // ensure that components have been specified
            if (selection.empty()) {
                return;
            }

            // move the current selection into the parent group
            nfCanvasUtils.moveComponentsToParent(selection);
        },

        /**
         * Uploads a new template.
         */
        uploadTemplate: function () {
            $('#upload-template-dialog').modal('show');
        },

        /**
         * Creates a new template based off the currently selected components. If no components
         * are selected, a template of the entire canvas is made.
         */
        template: function () {
            var selection = nfCanvasUtils.getSelection();

            // if no components are selected, use the entire graph
            if (selection.empty()) {
                selection = d3.selectAll('g.component, g.connection');
            }

            // ensure that components have been specified
            if (selection.empty()) {
                nfDialog.showOkDialog({
                    headerText: 'Create Template',
                    dialogContent: "The current selection is not valid to create a template."
                });
                return;
            }

            // remove dangling edges (where only the source or destination is also selected)
            selection = nfCanvasUtils.trimDanglingEdges(selection);

            // ensure that components specified are valid
            if (selection.empty()) {
                nfDialog.showOkDialog({
                    headerText: 'Create Template',
                    dialogContent: "The current selection is not valid to create a template."
                });
                return;
            }

            // prompt for the template name
            $('#new-template-dialog').modal('setButtonModel', [{
                buttonText: 'Create',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        // get the template details
                        var templateName = $('#new-template-name').val();

                        // ensure the template name is not blank
                        if (nfCommon.isBlank(templateName)) {
                            nfDialog.showOkDialog({
                                headerText: 'Configuration Error',
                                dialogContent: "The name of the template must be specified."
                            });
                            return;
                        }

                        // hide the dialog
                        $('#new-template-dialog').modal('hide');

                        // get the description
                        var templateDescription = $('#new-template-description').val();

                        // create a snippet
                        var parentGroupId = nfCanvasUtils.getGroupId();
                        var snippet = nfSnippet.marshal(selection, parentGroupId);

                        // create the snippet
                        nfSnippet.create(snippet).done(function (response) {
                            var createSnippetEntity = {
                                'name': templateName,
                                'description': templateDescription,
                                'snippetId': response.snippet.id,
                                'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
                            };

                            // create the template
                            $.ajax({
                                type: 'POST',
                                url: config.urls.api + '/process-groups/' + encodeURIComponent(nfCanvasUtils.getGroupId()) + '/templates',
                                data: JSON.stringify(createSnippetEntity),
                                dataType: 'json',
                                contentType: 'application/json'
                            }).done(function () {
                                // show the confirmation dialog
                                nfDialog.showOkDialog({
                                    headerText: 'Create Template',
                                    dialogContent: "Template '" + nfCommon.escapeHtml(templateName) + "' was successfully created."
                                });
                            }).always(function () {
                                // clear the template dialog fields
                                $('#new-template-name').val('');
                                $('#new-template-description').val('');
                            }).fail(nfErrorHandler.handleAjaxError);
                        }).fail(nfErrorHandler.handleAjaxError);
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
                        // clear the template dialog fields
                        $('#new-template-name').val('');
                        $('#new-template-description').val('');

                        $('#new-template-dialog').modal('hide');
                    }
                }
            }]).modal('show');

            // auto focus on the template name
            $('#new-template-name').focus();
        },

        /**
         * Copies the component in the specified selection.
         *
         * @param {selection} selection     The selection containing the component to be copied
         */
        copy: function (selection) {
            if (selection.empty()) {
                return;
            }

            // determine the origin of the bounding box of the selection
            var origin = nfCanvasUtils.getOrigin(selection);
            var selectionDimensions = nfCanvasUtils.getSelectionBoundingClientRect(selection);

            // copy the snippet details
            var parentGroupId = nfCanvasUtils.getGroupId();
            nfClipboard.copy({
                snippet: nfSnippet.marshal(selection, parentGroupId),
                origin: origin,
                dimensions: selectionDimensions
            });
        },

        /**
         * Pastes the currently copied selection.
         *
         * @param {selection} selection     The selection containing the component to be copied
         * @param {obj} evt                 The mouse event
         */
        paste: function (selection, evt) {
            if (nfCommon.isDefinedAndNotNull(evt)) {
                // get the current scale and translation
                var scale = nfCanvasUtils.getCanvasScale();
                var translate = nfCanvasUtils.getCanvasTranslate();

                var mouseX = evt.pageX;
                var mouseY = evt.pageY - nfCanvasUtils.getCanvasOffset();

                // adjust the x and y coordinates accordingly
                var x = (mouseX / scale) - (translate[0] / scale);
                var y = (mouseY / scale) - (translate[1] / scale);

                // record the paste origin
                var pasteLocation = {
                    x: x,
                    y: y
                };
            }

            // perform the paste
            nfClipboard.paste().done(function (data) {
                var copySnippet = $.Deferred(function (deferred) {
                    var reject = function (xhr, status, error) {
                        deferred.reject(xhr.responseText);
                    };

                    var destinationProcessGroupId = nfCanvasUtils.getGroupId();

                    // create a snippet from the details
                    nfSnippet.create(data['snippet']).done(function (createResponse) {
                        // determine the origin of the bounding box of the copy
                        var origin = pasteLocation;
                        var snippetOrigin = data['origin'];
                        var dimensions = data['dimensions'];

                        // determine the appropriate origin
                        if (!nfCommon.isDefinedAndNotNull(origin)) {
                            // if the copied item(s) are from a different group or the origin item is not in the viewport, center the pasted item(s)
                            if (nfCanvasUtils.getGroupId() !== data['snippet'].parentGroupId || !nfCanvasUtils.isBoundingBoxInViewport(dimensions, false)) {
                                var scale = nfCanvasUtils.getCanvasScale();

                                // put it in the center of the screen
                                var center = nfCanvasUtils.getCenterForBoundingBox(dimensions);
                                var translate = nfCanvasUtils.getCanvasTranslate();
                                origin = {
                                    x: center[0] - (translate[0] / scale),
                                    y: center[1] - (translate[1] / scale)
                                };

                            } else {
                                // paste it just offset from the original
                                snippetOrigin.x += 25;
                                snippetOrigin.y += 25;
                                origin = snippetOrigin;
                            }
                        }

                        // copy the snippet to the new location
                        nfSnippet.copy(createResponse.snippet.id, origin, destinationProcessGroupId).done(function (copyResponse) {
                            var snippetFlow = copyResponse.flow;

                            // update the graph accordingly
                            nfGraph.add(snippetFlow, {
                                'selectAll': true
                            });

                            // update component visibility
                            nfGraph.updateVisibility();

                            // refresh the birdseye/toolbar
                            nfBirdseye.refresh();
                        }).fail(function () {
                            // an error occured while performing the copy operation, reload the
                            // graph in case it was a partial success
                            nfCanvasUtils.reload().done(function () {
                                // update component visibility
                                nfGraph.updateVisibility();

                                // refresh the birdseye/toolbar
                                nfBirdseye.refresh();
                            });
                        }).fail(reject);
                    }).fail(reject);
                }).promise();

                // show the appropriate message is the copy fails
                copySnippet.fail(function (responseText) {
                    // look for a message
                    var message = 'An error occurred while attempting to copy and paste.';
                    if ($.trim(responseText) !== '') {
                        message = responseText;
                    }

                    nfDialog.showOkDialog({
                        headerText: 'Paste Error',
                        dialogContent: nfCommon.escapeHtml(message)
                    });
                });
            });
        },

        /**
         * Moves the connection in the specified selection to the front.
         *
         * @param {selection} selection
         */
        toFront: function (selection) {
            if (selection.size() !== 1 || !nfCanvasUtils.isConnection(selection)) {
                return;
            }

            // get the connection data
            var connection = selection.datum();

            // determine the current max zIndex
            var maxZIndex = -1;
            $.each(nfConnection.get(), function (_, otherConnection) {
                if (connection.id !== otherConnection.id && otherConnection.zIndex > maxZIndex) {
                    maxZIndex = otherConnection.zIndex;
                }
            });

            // ensure the edge wasn't already in front
            if (maxZIndex >= 0) {
                // use one higher
                var zIndex = maxZIndex + 1;

                // build the connection entity
                var connectionEntity = {
                    'revision': nfClient.getRevision(connection),
                    'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
                    'component': {
                        'id': connection.id,
                        'zIndex': zIndex
                    }
                };

                // update the edge in question
                $.ajax({
                    type: 'PUT',
                    url: connection.uri,
                    data: JSON.stringify(connectionEntity),
                    dataType: 'json',
                    contentType: 'application/json'
                }).done(function (response) {
                    nfConnection.set(response);
                }).fail(nfErrorHandler.handleAjaxError);
            }
        }
    };

    return nfActions;
}));
