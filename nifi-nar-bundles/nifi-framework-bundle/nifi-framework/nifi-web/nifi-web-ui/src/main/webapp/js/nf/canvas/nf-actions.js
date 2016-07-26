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

/* global nf, d3 */

nf.Actions = (function () {

    var config = {
        urls: {
            api: '../nifi-api',
            controller: '../nifi-api/controller'
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
        return $.ajax({
            type: 'PUT',
            url: uri,
            data: JSON.stringify(entity),
            dataType: 'json',
            contentType: 'application/json'
        }).fail(function (xhr, status, error) {
            if (xhr.status === 400 || xhr.status === 404 || xhr.status === 409) {
                nf.Dialog.showOkDialog({
                    headerText: 'Update Resource',
                    dialogContent: nf.Common.escapeHtml(xhr.responseText)
                });
            }
        });
    };

    // create a method for updating process groups and processors
    var updateProcessGroup = function (response) {
        $.ajax({
            type: 'GET',
            url: config.urls.api + '/flow/process-groups/' + encodeURIComponent(response.id),
            dataType: 'json'
        }).done(function (response) {
            nf.Graph.set(response.processGroupFlow.flow);
        });
    };

    return {
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
            if (selection.size() === 1 && nf.CanvasUtils.isProcessGroup(selection)) {
                var selectionData = selection.datum();
                nf.CanvasUtils.enterGroup(selectionData.id);
            }
        },

        /**
         * Exits the current process group but entering the parent group.
         */
        leaveGroup: function () {
            nf.CanvasUtils.enterGroup(nf.Canvas.getParentGroupId());
        },

        /**
         * Refresh the flow of the remote process group in the specified selection.
         *
         * @param {selection} selection
         */
        refreshRemoteFlow: function (selection) {
            if (selection.size() === 1 && nf.CanvasUtils.isRemoteProcessGroup(selection)) {
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
                            nf.RemoteProcessGroup.set(response);

                            // reload the group's connections
                            var connections = nf.Connection.getComponentConnections(remoteProcessGroup.id);
                            $.each(connections, function (_, connection) {
                                if (connection.permissions.canRead) {
                                    nf.Connection.reload(connection.component);
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
            if (selection.size() === 1 && nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                var selectionData = selection.datum();
                var uri = selectionData.component.targetUri;

                if (!nf.Common.isBlank(uri)) {
                    window.open(encodeURI(uri));
                } else {
                    nf.Dialog.showOkDialog({
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
            if (selection.size() === 1 && nf.CanvasUtils.isConnection(selection)) {
                var selectionData = selection.datum();

                // the source is in the current group
                if (selectionData.sourceGroupId === nf.Canvas.getGroupId()) {
                    var source = d3.select('#id-' + selectionData.sourceId);
                    nf.Actions.show(source);
                } else if (selectionData.sourceType === 'REMOTE_OUTPUT_PORT') {
                    // if the source is remote
                    var remoteSource = d3.select('#id-' + selectionData.sourceGroupId);
                    nf.Actions.show(remoteSource);
                } else {
                    // if the source is local but in a sub group
                    nf.CanvasUtils.showComponent(selectionData.sourceGroupId, selectionData.sourceId);
                }
            }
        },

        /**
         * Shows and selects the destination of the connection in the specified selection.
         *
         * @param {selection} selection     The selection
         */
        showDestination: function (selection) {
            if (selection.size() === 1 && nf.CanvasUtils.isConnection(selection)) {
                var selectionData = selection.datum();

                // the destination is in the current group or its remote
                if (selectionData.destinationGroupId === nf.Canvas.getGroupId()) {
                    var destination = d3.select('#id-' + selectionData.destinationId);
                    nf.Actions.show(destination);
                } else if (selectionData.destinationType === 'REMOTE_INPUT_PORT') {
                    // if the destination is remote
                    var remoteDestination = d3.select('#id-' + selectionData.destinationGroupId);
                    nf.Actions.show(remoteDestination);
                } else {
                    // if the destination is local but in a sub group
                    nf.CanvasUtils.showComponent(selectionData.destinationGroupId, selectionData.destinationId);
                }
            }
        },

        /**
         * Shows the downstream components from the specified selection.
         *
         * @param {selection} selection     The selection
         */
        showDownstream: function (selection) {
            if (selection.size() === 1 && !nf.CanvasUtils.isConnection(selection)) {

                // open the downstream dialog according to the selection
                if (nf.CanvasUtils.isProcessor(selection)) {
                    nf.GoTo.showDownstreamFromProcessor(selection);
                } else if (nf.CanvasUtils.isFunnel(selection)) {
                    nf.GoTo.showDownstreamFromFunnel(selection);
                } else if (nf.CanvasUtils.isInputPort(selection)) {
                    nf.GoTo.showDownstreamFromInputPort(selection);
                } else if (nf.CanvasUtils.isOutputPort(selection)) {
                    nf.GoTo.showDownstreamFromOutputPort(selection);
                } else if (nf.CanvasUtils.isProcessGroup(selection) || nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                    nf.GoTo.showDownstreamFromGroup(selection);
                }
            }
        },

        /**
         * Shows the upstream components from the specified selection.
         *
         * @param {selection} selection     The selection
         */
        showUpstream: function (selection) {
            if (selection.size() === 1 && !nf.CanvasUtils.isConnection(selection)) {

                // open the downstream dialog according to the selection
                if (nf.CanvasUtils.isProcessor(selection)) {
                    nf.GoTo.showUpstreamFromProcessor(selection);
                } else if (nf.CanvasUtils.isFunnel(selection)) {
                    nf.GoTo.showUpstreamFromFunnel(selection);
                } else if (nf.CanvasUtils.isInputPort(selection)) {
                    nf.GoTo.showUpstreamFromInputPort(selection);
                } else if (nf.CanvasUtils.isOutputPort(selection)) {
                    nf.GoTo.showUpstreamFromOutputPort(selection);
                } else if (nf.CanvasUtils.isProcessGroup(selection) || nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                    nf.GoTo.showUpstreamFromGroup(selection);
                }
            }
        },

        /**
         * Shows and selects the component in the specified selection.
         *
         * @param {selection} selection     The selection
         */
        show: function (selection) {
            if (selection.size() === 1) {
                // deselect the current selection
                var currentlySelected = nf.CanvasUtils.getSelection();
                currentlySelected.classed('selected', false);

                // select only the component/connection in question
                selection.classed('selected', true);
                nf.Actions.center(selection);
            }
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
            nf.Actions.select(d3.selectAll('g.component, g.connection'));
        },

        /**
         * Centers the component in the specified selection.
         *
         * @argument {selection} selection      The selection
         */
        center: function (selection) {
            if (selection.size() === 1) {
                var box;
                if (nf.CanvasUtils.isConnection(selection)) {
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
                nf.CanvasUtils.centerBoundingBox(box);

                // refresh the canvas
                nf.Canvas.View.refresh({
                    transition: true
                });
            }
        },

        /**
         * Enables all eligible selected components.
         *
         * @argument {selection} selection      The selection
         */
        enable: function (selection) {
            var componentsToEnable = nf.CanvasUtils.filterEnable(selection);

            if (componentsToEnable.empty()) {
                nf.Dialog.showOkDialog({
                    headerText: 'Enable Components',
                    dialogContent: 'No eligible components are selected. Please select the components to be enabled and ensure they are no longer running.'
                });
            } else {
                var enableRequests = [];

                // enable the selected processors
                componentsToEnable.each(function (d) {
                    var selected = d3.select(this);

                    // build the entity
                    var entity = {
                        'revision': nf.Client.getRevision(d),
                        'component': {
                            'id': d.id,
                            'state': 'STOPPED'
                        }
                    };

                    enableRequests.push(updateResource(d.uri, entity).done(function (response) {
                        nf[d.type].set(response);
                    }));
                });

                // inform Angular app once the updates have completed
                if (enableRequests.length > 0) {
                    $.when.apply(window, enableRequests).always(function () {
                        nf.ng.Bridge.digest();
                    });
                }
            }
        },

        /**
         * Disables all eligible selected components.
         *
         * @argument {selection} selection      The selection
         */
        disable: function (selection) {
            var componentsToDisable = nf.CanvasUtils.filterDisable(selection);

            if (componentsToDisable.empty()) {
                nf.Dialog.showOkDialog({
                    headerText: 'Disable Components',
                    dialogContent: 'No eligible components are selected. Please select the components to be disabled and ensure they are no longer running.'
                });
            } else {
                var disableRequests = [];

                // disable the selected components
                componentsToDisable.each(function (d) {
                    var selected = d3.select(this);

                    // build the entity
                    var entity = {
                        'revision': nf.Client.getRevision(d),
                        'component': {
                            'id': d.id,
                            'state': 'DISABLED'
                        }
                    };

                    disableRequests.push(updateResource(d.uri, entity).done(function (response) {
                        nf[d.type].set(response);
                    }));
                });

                // inform Angular app once the updates have completed
                if (disableRequests.length > 0) {
                    $.when.apply(window, disableRequests).always(function () {
                        nf.ng.Bridge.digest();
                    });
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
                nf.Shell.showPage('provenance?' + $.param({
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
                    'id': nf.Canvas.getGroupId(),
                    'state': 'RUNNING'
                };

                updateResource(config.urls.api + '/flow/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()), entity).done(updateProcessGroup);
            } else {
                var componentsToStart = selection.filter(function (d) {
                    return nf.CanvasUtils.isRunnable(d3.select(this));
                });

                // ensure there are startable components selected
                if (componentsToStart.empty()) {
                    nf.Dialog.showOkDialog({
                        headerText: 'Start Components',
                        dialogContent: 'No eligible components are selected. Please select the components to be started and ensure they are no longer running.'
                    });
                } else {
                    var startRequests = [];

                    // start each selected component
                    componentsToStart.each(function (d) {
                        var selected = d3.select(this);

                        // prepare the request
                        var uri, entity;
                        if (nf.CanvasUtils.isProcessGroup(selected)) {
                            uri = config.urls.api + '/flow/process-groups/' + encodeURIComponent(d.id);
                            entity = {
                                'id': d.id,
                                'state': 'RUNNING'
                            }
                        } else {
                            uri = d.uri;
                            entity = {
                                'revision': nf.Client.getRevision(d),
                                'component': {
                                    'id': d.id,
                                    'state': 'RUNNING'
                                }
                            };
                        }

                        startRequests.push(updateResource(uri, entity).done(function (response) {
                            if (nf.CanvasUtils.isProcessGroup(selected)) {
                                nf.ProcessGroup.reload(d.component);
                            } else {
                                nf[d.type].set(response);
                            }
                        }));
                    });

                    // inform Angular app once the updates have completed
                    if (startRequests.length > 0) {
                        $.when.apply(window, startRequests).always(function () {
                            nf.ng.Bridge.digest();
                        });
                    }
                }
            }
        },

        /**
         * Stops the components in the specified selection.
         *
         * @argument {selection} selection      The selection
         */
        stop: function (selection) {
            if (selection.empty()) {
                // build the entity
                var entity = {
                    'id': nf.Canvas.getGroupId(),
                    'state': 'STOPPED'
                };

                updateResource(config.urls.api + '/flow/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()), entity).done(updateProcessGroup);
            } else {
                var componentsToStop = selection.filter(function (d) {
                    return nf.CanvasUtils.isStoppable(d3.select(this));
                });

                // ensure there are some component to stop
                if (componentsToStop.empty()) {
                    nf.Dialog.showOkDialog({
                        headerText: 'Stop Components',
                        dialogContent: 'No eligible components are selected. Please select the components to be stopped.'
                    });
                } else {
                    var stopRequests = [];

                    // stop each selected component
                    componentsToStop.each(function (d) {
                        var selected = d3.select(this);

                        // prepare the request
                        var uri, entity;
                        if (nf.CanvasUtils.isProcessGroup(selected)) {
                            uri = config.urls.api + '/flow/process-groups/' + encodeURIComponent(d.id);
                            entity = {
                                'id': d.id,
                                'state': 'STOPPED'
                            };
                        } else {
                            uri = d.uri;
                            entity = {
                                'revision': nf.Client.getRevision(d),
                                'component': {
                                    'id': d.id,
                                    'state': 'STOPPED'
                                }
                            };
                        }

                        stopRequests.push(updateResource(uri, entity).done(function (response) {
                            if (nf.CanvasUtils.isProcessGroup(selected)) {
                                nf.ProcessGroup.reload(d.component);
                            } else {
                                nf[d.type].set(response);
                            }
                        }));
                    });

                    // inform Angular app once the updates have completed
                    if (stopRequests.length > 0) {
                        $.when.apply(window, stopRequests).always(function () {
                            nf.ng.Bridge.digest();
                        });
                    }
                }
            }
        },

        /**
         * Enables transmission for the components in the specified selection.
         *
         * @argument {selection} selection      The selection
         */
        enableTransmission: function (selection) {
            var componentsToEnable = selection.filter(function (d) {
                return nf.CanvasUtils.canStartTransmitting(d3.select(this));
            });

            // start each selected component
            componentsToEnable.each(function (d) {
                // build the entity
                var entity = {
                    'revision': nf.Client.getRevision(d),
                    'component': {
                        'id': d.id,
                        'transmitting': true
                    }
                };

                // start transmitting
                updateResource(d.uri, entity).done(function (response) {
                    nf.RemoteProcessGroup.set(response);
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
                return nf.CanvasUtils.canStopTransmitting(d3.select(this));
            });

            // stop each selected component
            componentsToDisable.each(function (d) {
                // build the entity
                var entity = {
                    'revision': nf.Client.getRevision(d),
                    'component': {
                        'id': d.id,
                        'transmitting': false
                    }
                };

                updateResource(d.uri, entity).done(function (response) {
                    nf.RemoteProcessGroup.set(response);
                });
            });
        },

        /**
         * Shows the configuration dialog for the specified selection.
         *
         * @param {selection} selection     Selection of the component to be configured
         */
        showConfiguration: function (selection) {
            if (selection.empty()) {
                nf.ProcessGroupConfiguration.showConfiguration(nf.Canvas.getGroupId());
            } else if (selection.size() === 1) {
                var selectionData = selection.datum();
                if (nf.CanvasUtils.isProcessor(selection)) {
                    nf.ProcessorConfiguration.showConfiguration(selection);
                } else if (nf.CanvasUtils.isLabel(selection)) {
                    nf.LabelConfiguration.showConfiguration(selection);
                } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                    nf.ProcessGroupConfiguration.showConfiguration(selectionData.id);
                } else if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                    nf.RemoteProcessGroupConfiguration.showConfiguration(selection);
                } else if (nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection)) {
                    nf.PortConfiguration.showConfiguration(selection);
                } else if (nf.CanvasUtils.isConnection(selection)) {
                    nf.ConnectionConfiguration.showConfiguration(selection);
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
                nf.PolicyManagement.showComponentPolicy(selection);
            }
        },

        // Defines an action for showing component details (like configuration but read only).
        showDetails: function (selection) {
            if (selection.empty()) {
                nf.ProcessGroupConfiguration.showConfiguration(nf.Canvas.getGroupId());
            } else if (selection.size() === 1) {
                var selectionData = selection.datum();
                if (nf.CanvasUtils.isProcessor(selection)) {
                    nf.ProcessorDetails.showDetails(nf.Canvas.getGroupId(), selectionData.id);
                } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                    nf.ProcessGroupConfiguration.showConfiguration(selectionData.id);
                } else if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                    nf.RemoteProcessGroupDetails.showDetails(selection);
                } else if (nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection)) {
                    nf.PortDetails.showDetails(selection);
                } else if (nf.CanvasUtils.isConnection(selection)) {
                    nf.ConnectionDetails.showDetails(nf.Canvas.getGroupId(), selectionData.id);
                }
            }
        },

        /**
         * Shows the usage documentation for the component in the specified selection.
         *
         * @param {selection} selection     The selection
         */
        showUsage: function (selection) {
            if (selection.size() === 1 && nf.CanvasUtils.isProcessor(selection)) {
                var selectionData = selection.datum();
                nf.Shell.showPage('../nifi-docs/documentation?' + $.param({
                        select: nf.Common.substringAfterLast(selectionData.component.type, '.')
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
                if (nf.CanvasUtils.isProcessor(selection)) {
                    nf.StatusHistory.showProcessorChart(nf.Canvas.getGroupId(), selectionData.id);
                } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                    nf.StatusHistory.showProcessGroupChart(nf.Canvas.getGroupId(), selectionData.id);
                } else if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                    nf.StatusHistory.showRemoteProcessGroupChart(nf.Canvas.getGroupId(), selectionData.id);
                } else if (nf.CanvasUtils.isConnection(selection)) {
                    nf.StatusHistory.showConnectionChart(nf.Canvas.getGroupId(), selectionData.id);
                }
            }
        },

        /**
         * Opens the remote ports dialog for the remote process group in the specified selection.
         *
         * @param {selection} selection         The selection
         */
        remotePorts: function (selection) {
            if (selection.size() === 1 && nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                nf.RemoteProcessGroupPorts.showPorts(selection);
            }
        },

        /**
         * Reloads the status for the entire canvas (components and flow.)
         */
        reload: function () {
            nf.Canvas.reload({
                'transition': true
            });
        },

        /**
         * Deletes the component in the specified selection.
         *
         * @param {selection} selection     The selection containing the component to be removed
         */
        'delete': function (selection) {
            if (nf.Common.isUndefined(selection) || selection.empty()) {
                nf.Dialog.showOkDialog({
                    headerText: 'Reload',
                    dialogContent: 'No eligible components are selected. Please select the components to be deleted.'
                });
            } else {
                if (selection.size() === 1) {
                    var selectionData = selection.datum();
                    var revision = nf.Client.getRevision(selectionData);

                    $.ajax({
                        type: 'DELETE',
                        url: selectionData.uri + '?' + $.param({
                            version: revision.version,
                            clientId: revision.clientId
                        }),
                        dataType: 'json'
                    }).done(function (response) {
                        // remove the component/connection in question
                        nf[selectionData.type].remove(selectionData.id);

                        // if the selection is a connection, reload the source and destination accordingly
                        if (nf.CanvasUtils.isConnection(selection) === false) {
                            var connections = nf.Connection.getComponentConnections(selectionData.id);
                            if (connections.length > 0) {
                                var ids = [];
                                $.each(connections, function (_, connection) {
                                    ids.push(connection.id);
                                });

                                // remove the corresponding connections
                                nf.Connection.remove(ids);
                            }
                        }

                        // refresh the birdseye
                        nf.Birdseye.refresh();
                        // inform Angular app values have changed
                        nf.ng.Bridge.digest();
                    }).fail(nf.Common.handleAjaxError);
                } else {
                    // create a snippet for the specified component and link to the data flow
                    var snippet = nf.Snippet.marshal(selection);
                    nf.Snippet.create(snippet).done(function (response) {
                        // remove the snippet, effectively removing the components
                        nf.Snippet.remove(response.snippet.id).done(function () {
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
                                    var connections = nf.Connection.getComponentConnections(d.id);
                                    if (connections.length > 0) {
                                        $.each(connections, function (_, connection) {
                                            addComponent('Connection', connection.id);
                                        });
                                    }
                                }
                            });

                            // remove all the non connections in the snippet first
                            components.forEach(function (type, ids) {
                                if (type !== 'Connection') {
                                    nf[type].remove(ids);
                                }
                            });

                            // then remove all the connections
                            if (components.has('Connection')) {
                                nf.Connection.remove(components.get('Connection'));
                            }

                            // refresh the birdseye
                            nf.Birdseye.refresh();
                            
                            // inform Angular app values have changed
                            nf.ng.Bridge.digest();
                        }).fail(nf.Common.handleAjaxError);
                    }).fail(nf.Common.handleAjaxError);
                }
            }
        },

        /**
         * Deletes the flow files in the specified connection.
         *
         * @param {type} selection
         */
        emptyQueue: function (selection) {
            if (selection.size() !== 1 || !nf.CanvasUtils.isConnection(selection)) {
                return;
            }

            // prompt the user before emptying the queue
            nf.Dialog.showYesNoDialog({
                headerText: 'Empty Queue',
                dialogContent: 'Are you sure you want to empty this queue? All FlowFiles waiting at the time of the request will be removed.',
                noText: 'Cancel',
                yesText: 'Empty',
                yesHandler: function () {
                    // get the connection data
                    var connection = selection.datum();

                    var MAX_DELAY = 4;
                    var cancelled = false;
                    var dropRequest = null;
                    var dropRequestTimer = null;

                    // updates the progress bar
                    var updateProgress = function (percentComplete) {
                        // remove existing labels
                        var progressBar = $('#drop-request-percent-complete');
                        progressBar.find('div.progress-label').remove();
                        progressBar.find('md-progress-linear').remove();

                        // update the progress bar
                        var label = $('<div class="progress-label"></div>').text(percentComplete + '%');
                        (nf.ng.Bridge.injector.get('$compile')($('<md-progress-linear ng-cloak ng-value="' + percentComplete + '" class="md-hue-2" md-mode="determinate" aria-label="Drop request percent complete"></md-progress-linear>'))(nf.ng.Bridge.rootScope)).appendTo(progressBar);
                        progressBar.append(label);
                    };

                    // update the button model of the drop request status dialog
                    $('#drop-request-status-dialog').modal('setButtonModel', [{
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
                                if (dropRequestTimer !== null) {
                                    // cancel it
                                    clearTimeout(dropRequestTimer);

                                    // cancel the drop request
                                    completeDropRequest();
                                }
                            }
                        }
                    }]);

                    // completes the drop request by removing it and showing how many flowfiles were deleted
                    var completeDropRequest = function () {
                        if (nf.Common.isDefinedAndNotNull(dropRequest)) {
                            $.ajax({
                                type: 'DELETE',
                                url: dropRequest.uri,
                                dataType: 'json'
                            }).done(function (response) {
                                // report the results of this drop request
                                dropRequest = response.dropRequest;

                                // build the results
                                var droppedTokens = dropRequest.dropped.split(/ \/ /);
                                var results = $('<div></div>');
                                $('<span class="label"></span>').text(droppedTokens[0]).appendTo(results);
                                $('<span></span>').text(' FlowFiles (' + droppedTokens[1] + ')').appendTo(results);

                                // if the request did not complete, include the original
                                if (dropRequest.percentCompleted < 100) {
                                    var originalTokens = dropRequest.original.split(/ \/ /);
                                    $('<span class="label"></span>').text(' out of ' + originalTokens[0]).appendTo(results);
                                    $('<span></span>').text(' (' + originalTokens[1] + ')').appendTo(results);
                                }
                                $('<span></span>').text(' were removed from the queue.').appendTo(results);

                                // if this request failed so the error
                                if (nf.Common.isDefinedAndNotNull(dropRequest.failureReason)) {
                                    $('<br/><br/><span></span>').text(dropRequest.failureReason).appendTo(results);
                                }

                                // display the results
                                nf.Dialog.showOkDialog({
                                    headerText: 'Empty Queue',
                                    dialogContent: results
                                });
                            }).always(function () {
                                $('#drop-request-status-dialog').modal('hide');
                            });
                        } else {
                            // nothing was removed
                            nf.Dialog.showOkDialog({
                                headerText: 'Empty Queue',
                                dialogContent: 'No FlowFiles were removed.'
                            });

                            // close the dialog
                            $('#drop-request-status-dialog').modal('hide');
                        }
                    };

                    // process the drop request
                    var processDropRequest = function (delay) {
                        // update the percent complete
                        updateProgress(dropRequest.percentCompleted);

                        // update the status of the drop request
                        $('#drop-request-status-message').text(dropRequest.state);

                        // update the current number of enqueued flowfiles
                        if (nf.Common.isDefinedAndNotNull(dropRequest.currentCount)) {
                            connection.status.queued = dropRequest.current;
                            connection.status.aggregateSnapshot.queued = dropRequest.current;
                            nf.Connection.refresh(connection.id);
                        }

                        // close the dialog if the 
                        if (dropRequest.finished === true || cancelled === true) {
                            completeDropRequest();
                        } else {
                            // wait delay to poll again
                            dropRequestTimer = setTimeout(function () {
                                // clear the drop request timer
                                dropRequestTimer = null;

                                // schedule to poll the status again in nextDelay
                                pollDropRequest(Math.min(MAX_DELAY, delay * 2));
                            }, delay * 1000);
                        }
                    };

                    // schedule for the next poll iteration
                    var pollDropRequest = function (nextDelay) {
                        $.ajax({
                            type: 'GET',
                            url: dropRequest.uri,
                            dataType: 'json'
                        }).done(function (response) {
                            dropRequest = response.dropRequest;
                            processDropRequest(nextDelay);
                        }).fail(function (xhr, status, error) {
                            if (xhr.status === 403) {
                                nf.Common.handleAjaxError(xhr, status, error);
                            } else {
                                completeDropRequest()
                            }
                        });
                    };

                    // issue the request to delete the flow files
                    $.ajax({
                        type: 'POST',
                        url: '../nifi-api/flowfile-queues/' + encodeURIComponent(connection.id) + '/drop-requests',
                        dataType: 'json',
                        contentType: 'application/json'
                    }).done(function (response) {
                        // initialize the progress bar value
                        updateProgress(0);

                        // show the progress dialog
                        $('#drop-request-status-dialog').modal('show');

                        // process the drop request
                        dropRequest = response.dropRequest;
                        processDropRequest(1);
                    }).fail(function (xhr, status, error) {
                        if (xhr.status === 403) {
                            nf.Common.handleAjaxError(xhr, status, error);
                        } else {
                            completeDropRequest()
                        }
                    });
                }
            });
        },

        /**
         * Lists the flow files in the specified connection.
         *
         * @param {selection} selection
         */
        listQueue: function (selection) {
            if (selection.size() !== 1 || !nf.CanvasUtils.isConnection(selection)) {
                return;
            }

            // get the connection data
            var connection = selection.datum();

            // list the flow files in the specified connection
            nf.QueueListing.listQueue(connection);
        },

        /**
         * Views the state for the specified processor.
         *
         * @param {selection} selection
         */
        viewState: function (selection) {
            if (selection.size() !== 1 || !nf.CanvasUtils.isProcessor(selection)) {
                return;
            }

            // get the processor data
            var processor = selection.datum();

            // view the state for the selected processor
            nf.ComponentState.showState(processor, nf.CanvasUtils.isConfigurable(selection));
        },

        /**
         * Opens the fill color dialog for the component in the specified selection.
         *
         * @param {type} selection      The selection
         */
        fillColor: function (selection) {
            if (nf.CanvasUtils.isColorable(selection)) {
                // we know that the entire selection is processors or labels... this
                // checks if the first item is a processor... if true, all processors
                var allProcessors = nf.CanvasUtils.isProcessor(selection);

                var color;
                if (allProcessors) {
                    color = nf.Processor.defaultColor();
                } else {
                    color = nf.Label.defaultColor();
                }

                // if there is only one component selected, get its color otherwise use default
                if (selection.size() === 1) {
                    var selectionData = selection.datum();

                    // use the specified color if appropriate
                    if (nf.Common.isDefinedAndNotNull(selectionData.component.style['background-color'])) {
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
            var selection = nf.CanvasUtils.getSelection();

            // ensure that components have been specified
            if (selection.empty()) {
                return;
            }

            // ensure the selected components are eligible being moved into a new group
            $.when(nf.CanvasUtils.eligibleForMove(selection)).done(function () {
                // determine the origin of the bounding box for the selected components
                var origin = nf.CanvasUtils.getOrigin(selection);

                var pt = {'x': origin.x, 'y': origin.y};
                $.when(nf.ng.Bridge.injector.get('groupComponent').promptForGroupName(pt)).done(function (processGroup) {
                    var group = d3.select('#id-' + processGroup.id);
                    nf.CanvasUtils.moveComponents(selection, group);
                });
            });
        },

        /**
         * Moves the currently selected component into the current parent group.
         */
        moveIntoParent: function () {
            var selection = nf.CanvasUtils.getSelection();

            // ensure that components have been specified
            if (selection.empty()) {
                return;
            }

            // move the current selection into the parent group
            nf.CanvasUtils.moveComponentsToParent(selection);
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
            var selection = nf.CanvasUtils.getSelection();

            // if no components are selected, use the entire graph
            if (selection.empty()) {
                selection = d3.selectAll('g.component, g.connection');
            }

            // ensure that components have been specified
            if (selection.empty()) {
                nf.Dialog.showOkDialog({
                    headerText: 'Create Template',
                    dialogContent: "The current selection is not valid to create a template."
                });
                return;
            }

            // remove dangling edges (where only the source or destination is also selected)
            selection = nf.CanvasUtils.trimDanglingEdges(selection);

            // ensure that components specified are valid
            if (selection.empty()) {
                nf.Dialog.showOkDialog({
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
                        if (nf.Common.isBlank(templateName)) {
                            nf.Dialog.showOkDialog({
                                headerText: 'Create Template',
                                dialogContent: "The template name cannot be blank."
                            });
                            return;
                        }

                        // hide the dialog
                        $('#new-template-dialog').modal('hide');

                        // get the description
                        var templateDescription = $('#new-template-description').val();

                        // create a snippet
                        var snippet = nf.Snippet.marshal(selection);

                        // create the snippet
                        nf.Snippet.create(snippet).done(function (response) {
                            var createSnippetEntity = {
                                'name': templateName,
                                'description': templateDescription,
                                'snippetId': response.snippet.id
                            };

                            // create the template
                            $.ajax({
                                type: 'POST',
                                url: config.urls.api + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/templates',
                                data: JSON.stringify(createSnippetEntity),
                                dataType: 'json',
                                contentType: 'application/json'
                            }).done(function () {
                                // show the confirmation dialog
                                nf.Dialog.showOkDialog({
                                    headerText: 'Create Template',
                                    dialogContent: "Template '" + nf.Common.escapeHtml(templateName) + "' was successfully created."
                                });
                            }).always(function () {
                                // clear the template dialog fields
                                $('#new-template-name').val('');
                                $('#new-template-description').val('');
                            }).fail(nf.Common.handleAjaxError);
                        }).fail(nf.Common.handleAjaxError);
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
            var origin = nf.CanvasUtils.getOrigin(selection);

            // copy the snippet details
            nf.Clipboard.copy({
                snippet: nf.Snippet.marshal(selection),
                origin: origin
            });
        },

        /**
         * Pastes the currently copied selection.
         *
         * @param {selection} selection     The selection containing the component to be copied
         * @param {obj} evt                 The mouse event
         */
        paste: function (selection, evt) {
            if (nf.Common.isDefinedAndNotNull(evt)) {
                // get the current scale and translation
                var scale = nf.Canvas.View.scale();
                var translate = nf.Canvas.View.translate();

                var mouseX = evt.pageX;
                var mouseY = evt.pageY - nf.Canvas.CANVAS_OFFSET;

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
            nf.Clipboard.paste().done(function (data) {
                var copySnippet = $.Deferred(function (deferred) {
                    var reject = function (xhr, status, error) {
                        deferred.reject(xhr.responseText);
                    };

                    // create a snippet from the details
                    nf.Snippet.create(data['snippet']).done(function (createResponse) {
                        // determine the origin of the bounding box of the copy
                        var origin = pasteLocation;
                        var snippetOrigin = data['origin'];

                        // determine the appropriate origin
                        if (!nf.Common.isDefinedAndNotNull(origin)) {
                            snippetOrigin.x += 25;
                            snippetOrigin.y += 25;
                            origin = snippetOrigin;
                        }

                        // copy the snippet to the new location
                        nf.Snippet.copy(createResponse.snippet.id, origin).done(function (copyResponse) {
                            var snippetFlow = copyResponse.flow;

                            // update the graph accordingly
                            nf.Graph.add(snippetFlow, {
                                'selectAll': true
                            });

                            // update component visibility
                            nf.Canvas.View.updateVisibility();

                            // refresh the birdseye/toolbar
                            nf.Birdseye.refresh();
                        }).fail(function () {
                            // an error occured while performing the copy operation, reload the
                            // graph in case it was a partial success
                            nf.Canvas.reload().done(function () {
                                // update component visibility
                                nf.Canvas.View.updateVisibility();

                                // refresh the birdseye/toolbar
                                nf.Birdseye.refresh();
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

                    nf.Dialog.showOkDialog({
                        headerText: 'Paste Error',
                        dialogContent: nf.Common.escapeHtml(message)
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
            if (selection.size() !== 1 || !nf.CanvasUtils.isConnection(selection)) {
                return;
            }

            // get the connection data
            var connection = selection.datum();

            // determine the current max zIndex
            var maxZIndex = -1;
            $.each(nf.Connection.get(), function (_, otherConnection) {
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
                    'revision': nf.Client.getRevision(connection),
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
                    // update the edge's zIndex
                    nf.Connection.set(response);
                    nf.Connection.reorder();
                });
            }
        }
    };
}());