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
nf.Actions = (function () {

    var config = {
        urls: {
            controller: '../nifi-api/controller'
        }
    };

    /**
     * Updates the resource with the specified data.
     * 
     * @param {type} uri
     * @param {type} data
     */
    var updateResource = function (uri, data) {
        var revision = nf.Client.getRevision();

        // ensure the version and client ids are specified
        data.version = revision.version;
        data.clientId = revision.clientId;

        return $.ajax({
            type: 'PUT',
            url: uri,
            data: data,
            dataType: 'json'
        }).done(function (response) {
            // update the revision
            nf.Client.setRevision(response.revision);
        }).fail(function (xhr, status, error) {
            if (xhr.status === 400 || xhr.status === 404 || xhr.status === 409) {
                nf.Dialog.showOkDialog({
                    dialogContent: nf.Common.escapeHtml(xhr.responseText),
                    overlayBackground: true
                });
            }
        });
    };

    // create a method for updating process groups and processors
    var updateProcessGroup = function (response) {
        if (nf.Common.isDefinedAndNotNull(response.processGroup)) {
            if (nf.Common.isDefinedAndNotNull(response.processGroup.contents)) {
                var contents = response.processGroup.contents;

                // update all the components in the contents
                nf.Graph.set(contents);

                // update each process group
                $.each(contents.processGroups, function (_, processGroup) {
                    // reload the group's connections
                    var connections = nf.Connection.getComponentConnections(processGroup.id);
                    $.each(connections, function (_, connection) {
                        nf.Connection.reload(connection);
                    });
                });
            }
        }
    };

    return {
        /**
         * Enters the specified process group.
         * 
         * @param {selection} selection     The the currently selected component
         */
        enterGroup: function (selection) {
            if (selection.size() === 1 && nf.CanvasUtils.isProcessGroup(selection)) {
                var selectionData = selection.datum();
                nf.CanvasUtils.enterGroup(selectionData.component.id);
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
                        url: d.component.uri,
                        dataType: 'json'
                    }).done(function (response) {
                        var remoteProcessGroup = response.remoteProcessGroup;

                        // the timestamp has not updated yet, poll again
                        if (refreshTimestamp === remoteProcessGroup.flowRefreshed) {
                            schedule(nextDelay);
                        } else {
                            nf.RemoteProcessGroup.set(response.remoteProcessGroup);

                            // reload the group's connections
                            var connections = nf.Connection.getComponentConnections(remoteProcessGroup.id);
                            $.each(connections, function (_, connection) {
                                nf.Connection.reload(connection);
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
                    window.open(encodeURI(uri + '/nifi'));
                } else {
                    nf.Dialog.showOkDialog({
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

                // if the source is actually in another group
                if (selectionData.component.source.groupId !== nf.Canvas.getGroupId()) {
                    nf.CanvasUtils.showComponent(selectionData.component.source.groupId, selectionData.component.source.id);
                } else {
                    var source = d3.select('#id-' + selectionData.component.source.id);
                    nf.Actions.show(source);
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

                // if the destination is actually in another group
                if (selectionData.component.destination.groupId !== nf.Canvas.getGroupId()) {
                    nf.CanvasUtils.showComponent(selectionData.component.destination.groupId, selectionData.component.destination.id);
                } else {
                    var destination = d3.select('#id-' + selectionData.component.destination.id);
                    nf.Actions.show(destination);
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
                    var selectionPosition = selectionData.component.position;

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
         */
        enable: function () {
            var components = d3.selectAll('g.component.selected').filter(function (d) {
                var selected = d3.select(this);
                return (nf.CanvasUtils.isProcessor(selected) || nf.CanvasUtils.isInputPort(selected) || nf.CanvasUtils.isOutputPort(selected)) && nf.CanvasUtils.supportsModification(selected);
            });
            if (components.empty()) {
                nf.Dialog.showOkDialog({
                    dialogContent: 'No eligible components are selected. Please select the components to be enabled and ensure they are no longer running.',
                    overlayBackground: true
                });
            } else {
                // enable the selected processors
                components.each(function (d) {
                    var selected = d3.select(this);
                    updateResource(d.component.uri, {state: 'STOPPED'}).done(function (response) {
                        if (nf.CanvasUtils.isProcessor(selected)) {
                            nf.Processor.set(response.processor);
                        } else if (nf.CanvasUtils.isInputPort(selected)) {
                            nf.Port.set(response.inputPort);
                        } else if (nf.CanvasUtils.isOutputPort(selected)) {
                            nf.Port.set(response.outputPort);
                        }
                    });
                });
            }
        },
        
        /**
         * Disables all eligible selected components.
         */
        disable: function () {
            var components = d3.selectAll('g.component.selected').filter(function (d) {
                var selected = d3.select(this);
                return (nf.CanvasUtils.isProcessor(selected) || nf.CanvasUtils.isInputPort(selected) || nf.CanvasUtils.isOutputPort(selected)) && nf.CanvasUtils.supportsModification(selected);
            });
            if (components.empty()) {
                nf.Dialog.showOkDialog({
                    dialogContent: 'No eligible components are selected. Please select the components to be disabled and ensure they are no longer running.',
                    overlayBackground: true
                });
            } else {
                // disable the selected components
                components.each(function (d) {
                    var selected = d3.select(this);
                    updateResource(d.component.uri, {state: 'DISABLED'}).done(function (response) {
                        if (nf.CanvasUtils.isProcessor(selected)) {
                            nf.Processor.set(response.processor);
                        } else if (nf.CanvasUtils.isInputPort(selected)) {
                            nf.Port.set(response.inputPort);
                        } else if (nf.CanvasUtils.isOutputPort(selected)) {
                            nf.Port.set(response.outputPort);
                        }
                    });
                });
            }
        },
        
        /**
         * Starts the components in the specified selection.
         * 
         * @argument {selection} selection      The selection
         */
        start: function (selection) {
            if (selection.empty()) {
                updateResource(config.urls.controller + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()), {running: true}).done(updateProcessGroup);
            } else {
                var componentsToStart = selection.filter(function (d) {
                    return nf.CanvasUtils.isRunnable(d3.select(this));
                });

                // ensure there are startable components selected
                if (componentsToStart.empty()) {
                    nf.Dialog.showOkDialog({
                        dialogContent: 'No eligible components are selected. Please select the components to be started and ensure they are no longer running.',
                        overlayBackground: true
                    });
                } else {
                    // start each selected component
                    componentsToStart.each(function (d) {
                        var selected = d3.select(this);

                        // processor endpoint does not use running flag...
                        var data = {};
                        if (nf.CanvasUtils.isProcessor(selected) || nf.CanvasUtils.isInputPort(selected) || nf.CanvasUtils.isOutputPort(selected)) {
                            data['state'] = 'RUNNING';
                        } else {
                            data['running'] = true;
                        }

                        // update the resource
                        updateResource(d.component.uri, data).done(function (response) {
                            if (nf.CanvasUtils.isProcessor(selected)) {
                                nf.Processor.set(response.processor);
                            } else if (nf.CanvasUtils.isProcessGroup(selected)) {
                                nf.ProcessGroup.set(response.processGroup);

                                // reload the group's connections
                                var connections = nf.Connection.getComponentConnections(response.processGroup.id);
                                $.each(connections, function (_, connection) {
                                    nf.Connection.reload(connection);
                                });
                            } else if (nf.CanvasUtils.isInputPort(selected)) {
                                nf.Port.set(response.inputPort);
                            } else if (nf.CanvasUtils.isOutputPort(selected)) {
                                nf.Port.set(response.outputPort);
                            }
                        });
                    });
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
                updateResource(config.urls.controller + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()), {running: false}).done(updateProcessGroup);
            } else {
                var componentsToStop = selection.filter(function (d) {
                    return nf.CanvasUtils.isStoppable(d3.select(this));
                });

                // ensure there are some component to stop
                if (componentsToStop.empty()) {
                    nf.Dialog.showOkDialog({
                        dialogContent: 'No eligible components are selected. Please select the components to be stopped.',
                        overlayBackground: true
                    });
                } else {
                    // stop each selected component
                    componentsToStop.each(function (d) {
                        var selected = d3.select(this);

                        // processor endpoint does not use running flag...
                        var data = {};
                        if (nf.CanvasUtils.isProcessor(selected) || nf.CanvasUtils.isInputPort(selected) || nf.CanvasUtils.isOutputPort(selected)) {
                            data['state'] = 'STOPPED';
                        } else {
                            data['running'] = false;
                        }

                        updateResource(d.component.uri, data).done(function (response) {
                            if (nf.CanvasUtils.isProcessor(selected)) {
                                nf.Processor.set(response.processor);
                            } else if (nf.CanvasUtils.isProcessGroup(selected)) {
                                nf.ProcessGroup.set(response.processGroup);

                                // reload the group's connections
                                var connections = nf.Connection.getComponentConnections(response.processGroup.id);
                                $.each(connections, function (_, connection) {
                                    nf.Connection.reload(connection);
                                });
                            } else if (nf.CanvasUtils.isInputPort(selected)) {
                                nf.Port.set(response.inputPort);
                            } else if (nf.CanvasUtils.isOutputPort(selected)) {
                                nf.Port.set(response.outputPort);
                            }
                        });
                    });
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
                updateResource(d.component.uri, {transmitting: true}).done(function (response) {
                    nf.RemoteProcessGroup.set(response.remoteProcessGroup);
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
                updateResource(d.component.uri, {transmitting: false}).done(function (response) {
                    nf.RemoteProcessGroup.set(response.remoteProcessGroup);
                });
            });
        },
        
        /**
         * Shows the configuration dialog for the specified selection.
         * 
         * @param {selection} selection     Selection of the component to be configured
         */
        showConfiguration: function (selection) {
            if (selection.size() === 1) {
                if (nf.CanvasUtils.isProcessor(selection)) {
                    nf.ProcessorConfiguration.showConfiguration(selection);
                } else if (nf.CanvasUtils.isLabel(selection)) {
                    nf.LabelConfiguration.showConfiguration(selection);
                } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                    nf.ProcessGroupConfiguration.showConfiguration(selection);
                } else if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                    nf.RemoteProcessGroupConfiguration.showConfiguration(selection);
                } else if (nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection)) {
                    // ports in the root group can be configured for access control
                    if (nf.Canvas.getParentGroupId() === null && nf.Canvas.isSecureSiteToSite()) {
                        nf.SecurePortConfiguration.showConfiguration(selection);
                    } else {
                        nf.PortConfiguration.showConfiguration(selection);
                    }
                } else if (nf.CanvasUtils.isConnection(selection)) {
                    nf.ConnectionConfiguration.showConfiguration(selection);
                }
            }
        },
        
        // Defines an action for showing component details (like configuration but read only).
        showDetails: function (selection) {
            if (selection.size() === 1) {
                var selectionData = selection.datum();
                if (nf.CanvasUtils.isProcessor(selection)) {
                    nf.ProcessorDetails.showDetails(nf.Canvas.getGroupId(), selectionData.component.id);
                } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                    nf.ProcessGroupDetails.showDetails(selection);
                } else if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                    nf.RemoteProcessGroupDetails.showDetails(selection);
                } else if (nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection)) {
                    // ports in the root group can be configured for access control
                    if (nf.Canvas.getParentGroupId() === null && nf.Canvas.isSecureSiteToSite()) {
                        nf.SecurePortDetails.showDetails(selection);
                    } else {
                        nf.PortDetails.showDetails(selection);
                    }
                } else if (nf.CanvasUtils.isConnection(selection)) {
                    nf.ConnectionDetails.showDetails(nf.Canvas.getGroupId(), selectionData.component.id);
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
                if (nf.Canvas.isClustered()) {
                    if (nf.CanvasUtils.isProcessor(selection)) {
                        nf.StatusHistory.showClusterProcessorChart(nf.Canvas.getGroupId(), selectionData.component.id);
                    } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                        nf.StatusHistory.showClusterProcessGroupChart(nf.Canvas.getGroupId(), selectionData.component.id);
                    } else if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                        nf.StatusHistory.showClusterRemoteProcessGroupChart(nf.Canvas.getGroupId(), selectionData.component.id);
                    } else if (nf.CanvasUtils.isConnection(selection)) {
                        nf.StatusHistory.showClusterConnectionChart(nf.Canvas.getGroupId(), selectionData.component.id);
                    }
                } else {
                    if (nf.CanvasUtils.isProcessor(selection)) {
                        nf.StatusHistory.showStandaloneProcessorChart(nf.Canvas.getGroupId(), selectionData.component.id);
                    } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                        nf.StatusHistory.showStandaloneProcessGroupChart(nf.Canvas.getGroupId(), selectionData.component.id);
                    } else if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                        nf.StatusHistory.showStandaloneRemoteProcessGroupChart(nf.Canvas.getGroupId(), selectionData.component.id);
                    } else if (nf.CanvasUtils.isConnection(selection)) {
                        nf.StatusHistory.showStandaloneConnectionChart(nf.Canvas.getGroupId(), selectionData.component.id);
                    }
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
         * Hides and open cancellable dialogs.
         */
        hideDialogs: function () {
            // ensure all cancellable dialogs are closed
            var cancellable = $('.cancellable');
            $.each(cancellable, function () {
                // if this dialog is open, close it 
                if ($(this).is(':visible')) {
                    $(this).modal('hide');
                }
            });
        },
        
        /**
         * Reloads the status for the entire canvas (components and flow.)
         */
        reloadStatus: function () {
            nf.Canvas.reloadStatus();
        },
        
        /**
         * Deletes the component in the specified selection.
         * 
         * @param {selection} selection     The selection containing the component to be removed
         */
        'delete': function (selection) {
            if (nf.Common.isUndefined(selection) || selection.empty()) {
                nf.Dialog.showOkDialog({
                    dialogContent: 'No eligible components are selected. Please select the components to be deleted.',
                    overlayBackground: true
                });
            } else {
                if (selection.size() === 1) {
                    var selectionData = selection.datum();
                    var revision = nf.Client.getRevision();

                    $.ajax({
                        type: 'DELETE',
                        url: selectionData.component.uri + '?' + $.param({
                            version: revision.version,
                            clientId: revision.clientId
                        }),
                        dataType: 'json'
                    }).done(function (response) {
                        // update the revision
                        nf.Client.setRevision(response.revision);

                        // remove the component/connection in question
                        nf[selectionData.type].remove(selectionData.component.id);

                        // if the source processor is part of the response, we
                        // have just removed a relationship. must update the status
                        // of the source processor in case its validity has changed
                        if (nf.CanvasUtils.isConnection(selection)) {
                            var sourceComponentId = nf.CanvasUtils.getConnectionSourceComponentId(selectionData.component);
                            var source = d3.select('#id-' + sourceComponentId);
                            var sourceData = source.datum();

                            // update the source status if necessary
                            if (nf.CanvasUtils.isProcessor(source)) {
                                nf.Processor.reload(sourceData.component);
                            } else if (nf.CanvasUtils.isInputPort(source)) {
                                nf.Port.reload(sourceData.component);
                            } else if (nf.CanvasUtils.isRemoteProcessGroup(source)) {
                                nf.RemoteProcessGroup.reload(sourceData.component);
                            }

                            var destinationComponentId = nf.CanvasUtils.getConnectionDestinationComponentId(selectionData.component);
                            var destination = d3.select('#id-' + destinationComponentId);
                            var destinationData = destination.datum();

                            // update the destination component accordingly
                            if (nf.CanvasUtils.isRemoteProcessGroup(destination)) {
                                nf.RemoteProcessGroup.reload(destinationData.component);
                            }
                        } else {
                            var connections = nf.Connection.getComponentConnections(selectionData.component.id);
                            if (connections.length > 0) {
                                var ids = [];
                                $.each(connections, function (_, connection) {
                                    ids.push(connection.id);
                                });

                                // remove the corresponding connections
                                nf.Connection.remove(ids);
                            }
                        }

                        // refresh the birdseye/toolbar
                        nf.Birdseye.refresh();
                        nf.CanvasToolbar.refresh();
                    }).fail(nf.Common.handleAjaxError);
                } else {
                    // create a snippet for the specified component and link to the data flow
                    var snippetDetails = nf.Snippet.marshal(selection, true);
                    nf.Snippet.create(snippetDetails).done(function (response) {
                        var snippet = response.snippet;

                        // remove the snippet, effectively removing the components
                        nf.Snippet.remove(snippet.id).done(function () {
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
                                addComponent(d.type, d.component.id);

                                // if this is not a connection, see if it has any connections that need to be removed
                                if (d.type !== 'Connection') {
                                    var connections = nf.Connection.getComponentConnections(d.component.id);
                                    if (connections.length > 0) {
                                        $.each(connections, function (_, connection) {
                                            addComponent('Connection', connection.id);
                                        });
                                    }
                                }
                            });

                            // refresh all component types as necessary (handle components that have been removed)
                            components.forEach(function (type, ids) {
                                nf[type].remove(ids);
                            });

                            // if some connections were removed
                            if (snippet.connections > 0) {
                                selection.filter(function (d) {
                                    return d.type === 'Connection';
                                }).each(function (d) {
                                    // add the source to refresh if its not already going to be refreshed
                                    var sourceComponentId = nf.CanvasUtils.getConnectionSourceComponentId(d.component);
                                    var source = d3.select('#id-' + sourceComponentId);
                                    var sourceData = source.datum();

                                    // update the source status if necessary - if the source was already removed
                                    // as part of this operation the reloading has no affect
                                    if (nf.CanvasUtils.isProcessor(source)) {
                                        nf.Processor.reload(sourceData.component);
                                    } else if (nf.CanvasUtils.isInputPort(source)) {
                                        nf.Port.reload(sourceData.component);
                                    } else if (nf.CanvasUtils.isRemoteProcessGroup(source)) {
                                        nf.RemoteProcessGroup.reload(sourceData.component);
                                    }

                                    // add the destination to refresh if its not already going to be refreshed
                                    var destinationComponentId = nf.CanvasUtils.getConnectionDestinationComponentId(d.component);
                                    var destination = d3.select('#id-' + destinationComponentId);
                                    var destinationData = destination.datum();

                                    if (nf.CanvasUtils.isRemoteProcessGroup(destination)) {
                                        nf.RemoteProcessGroup.reload(destinationData.component);
                                    }
                                });
                            }

                            // refresh the birdseye/toolbar
                            nf.Birdseye.refresh();
                            nf.CanvasToolbar.refresh();
                        }).fail(function (xhr, status, error) {
                            // unable to acutally remove the components so attempt to
                            // unlink and remove just the snippet - if unlinking fails
                            // just ignore
                            nf.Snippet.unlink(snippet.id).done(function () {
                                nf.Snippet.remove(snippet.id);
                            });

                            nf.Common.handleAjaxError(xhr, status, error);
                        });
                    }).fail(nf.Common.handleAjaxError);
                }
            }
        },
        
        /**
         * Opens the fill color dialog for the component in the specified selection.
         * 
         * @param {type} selection      The selection
         */
        fillColor: function (selection) {
            if (selection.size() === 1 && (nf.CanvasUtils.isProcessor(selection) || nf.CanvasUtils.isLabel(selection))) {
                var selectionData = selection.datum();
                var color = nf[selectionData.type].defaultColor();

                // use the specified color if appropriate
                if (nf.Common.isDefinedAndNotNull(selectionData.component.style['background-color'])) {
                    color = selectionData.component.style['background-color'];
                }

                // set the color
                $('#fill-color-value').minicolors('value', color);

                // update the preview visibility
                if (nf.CanvasUtils.isProcessor(selection)) {
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
                $.when(nf.CanvasToolbox.promptForGroupName(pt)).done(function (processGroup) {
                    var group = d3.select('#id-' + processGroup.id);
                    nf.CanvasUtils.moveComponents(selection, group);
                });
            });
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
                    dialogContent: "The current selection is not valid to create a template.",
                    overlayBackground: false
                });
                return;
            }

            // remove dangling edges (where only the source or destination is also selected)
            selection = nf.CanvasUtils.trimDanglingEdges(selection);

            // ensure that components specified are valid
            if (selection.empty()) {
                nf.Dialog.showOkDialog({
                    dialogContent: "The current selection is not valid to create a template.",
                    overlayBackground: false
                });
                return;
            }

            // prompt for the template name
            $('#new-template-dialog').modal('setButtonModel', [{
                    buttonText: 'Create',
                    handler: {
                        click: function () {
                            // hide the dialog
                            $('#new-template-dialog').modal('hide');

                            // get the template details
                            var templateName = $('#new-template-name').val();
                            var templateDescription = $('#new-template-description').val();

                            // create a snippet
                            var snippetDetails = nf.Snippet.marshal(selection, false);

                            // create the snippet
                            nf.Snippet.create(snippetDetails).done(function (response) {
                                var snippet = response.snippet;

                                // create the template
                                $.ajax({
                                    type: 'POST',
                                    url: config.urls.controller + '/templates',
                                    data: {
                                        name: templateName,
                                        description: templateDescription,
                                        snippetId: snippet.id
                                    },
                                    dataType: 'json'
                                }).done(function () {
                                    // show the confirmation dialog
                                    nf.Dialog.showOkDialog({
                                        dialogContent: "Template '" + nf.Common.escapeHtml(templateName) + "' was successfully created.",
                                        overlayBackground: false
                                    });
                                }).always(function () {
                                    // remove the snippet
                                    nf.Snippet.remove(snippet.id);

                                    // clear the template dialog fields
                                    $('#new-template-name').val('');
                                    $('#new-template-description').val('');
                                }).fail(nf.Common.handleAjaxError);
                            }).fail(nf.Common.handleAjaxError);
                        }
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: function () {
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
                snippet: nf.Snippet.marshal(selection, false),
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
                    var reject = function () {
                        deferred.reject();
                    };

                    // create a snippet from the details
                    nf.Snippet.create(data['snippet']).done(function (createResponse) {
                        var snippet = createResponse.snippet;

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
                        nf.Snippet.copy(snippet.id, nf.Canvas.getGroupId(), origin).done(function (copyResponse) {
                            var snippetContents = copyResponse.contents;

                            // update the graph accordingly
                            nf.Graph.add(snippetContents, true);

                            // update component visibility
                            nf.Canvas.View.updateVisibility();

                            // refresh the birdseye/toolbar
                            nf.Birdseye.refresh();
                            nf.CanvasToolbar.refresh();

                            // remove the original snippet
                            nf.Snippet.remove(snippet.id).fail(reject);
                        }).fail(function () {
                            // an error occured while performing the copy operation, reload the
                            // graph in case it was a partial success
                            nf.Canvas.reload().done(function () {
                                // update component visibility
                                nf.Canvas.View.updateVisibility();

                                // refresh the birdseye/toolbar
                                nf.Birdseye.refresh();
                                nf.CanvasToolbar.refresh();
                            });

                            // reject the deferred
                            reject();
                        });
                    }).fail(reject);
                }).promise();

                // show the appropriate message is the copy fails
                copySnippet.fail(function () {
                    // unable to create the template
                    nf.Dialog.showOkDialog({
                        dialogContent: 'An error occurred while attempting to copy and paste.',
                        overlayBackground: true
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
                if (connection.component.id !== otherConnection.component.id && otherConnection.component.zIndex > maxZIndex) {
                    maxZIndex = otherConnection.component.zIndex;
                }
            });

            // ensure the edge wasn't already in front
            if (maxZIndex >= 0) {
                // use one higher
                var zIndex = maxZIndex + 1;

                var revision = nf.Client.getRevision();

                // update the edge in question
                $.ajax({
                    type: 'PUT',
                    url: connection.component.uri,
                    data: {
                        version: revision.version,
                        clientId: revision.clientId,
                        zIndex: zIndex
                    },
                    dataType: 'json'
                }).done(function (response) {
                    // update the edge's zIndex
                    nf.Connection.set(response.connection);
                    nf.Connection.reorder();

                    // update the revision
                    nf.Client.setRevision(response.revision);
                });
            }
        }
    };
}());