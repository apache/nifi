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
/**
 * Handles the upstream/downstream dialogs.
 */
nf.GoTo = (function () {

    var config = {
        urls: {
            controller: '../nifi-api/controller',
            processGroups: '../nifi-api/controller/process-groups/'
        }
    };

    /**
     * Returns a deferred for the process group.
     * 
     * @param {string} processGroupId
     */
    var getProcessGroup = function (processGroupId) {
        return $.ajax({
            type: 'GET',
            url: config.urls.processGroups + encodeURIComponent(processGroupId),
            dataType: 'json'
        });
    };

    /**
     * Returns a deferred for the remote process group.
     * 
     * @param {string} parentGroupId
     * @param {string} remoteProcessGroupId
     */
    var getRemoteProcessGroup = function (parentGroupId, remoteProcessGroupId) {
        return $.ajax({
            type: 'GET',
            url: config.urls.processGroups + encodeURIComponent(parentGroupId) + '/remote-process-groups/' + encodeURIComponent(remoteProcessGroupId),
            dataType: 'json'
        });
    };

    /**
     * Adds a connection to the connections dialog.
     * 
     * @param {object} connection
     */
    var addConnection = function (connection) {
        var container = $('<div class="source-destination-connection"></div>').appendTo('#connections-container');

        var source;
        if (connection.source.type === 'OUTPUT_PORT') {
            source = addSourceOutputPort(container, connection);
        } else if (connection.source.type === 'REMOTE_OUTPUT_PORT') {
            source = addSourceRemoteOutputPort(container, connection);
        } else {
            source = addSourceComponent(container, connection);
        }

        source.done(function () {
            var connectionEntry = $('<div class="connection-entry"></div>').appendTo(container);

            // format the connection name.. fall back to id
            var connectionStyle = '';
            var connectionName = nf.CanvasUtils.formatConnectionName(connection);
            if (connectionName === '') {
                connectionStyle = 'unset';
                connectionName = 'Connection';
            }

            // connection
            $('<div class="search-result-icon connection-small-icon"></div>').appendTo(connectionEntry);
            $('<div class="connection-entry-name go-to-link"></div>').attr('title', connectionName).addClass(connectionStyle).text(connectionName).on('click', function () {
                // go to the connection
                nf.CanvasUtils.showComponent(connection.parentGroupId, connection.id);

                // close the dialog
                $('#connections-dialog').modal('hide');
            }).appendTo(connectionEntry);

            // clear
            $('<div class="clear"></div>').appendTo(connectionEntry);

            var destination;
            if (connection.destination.type === 'INPUT_PORT') {
                destination = addDestinationInputPort(container, connection);
            } else if (connection.destination.type === 'REMOTE_INPUT_PORT') {
                destination = addDestinationRemoteInputPort(container, connection);
            } else {
                destination = addDestinationComponent(container, connection);
            }

            destination.done(function () {
                // clear
                $('<div class="clear"></div>').appendTo(container);
            });
        });
    };

    /**
     * Adds a destination component to the dialog.
     * 
     * @param {jQuery} container                    The container to add to
     * @param {object} connection                   The connection to the downstream component
     */
    var addDestinationComponent = function (container, connection) {
        return $.Deferred(function (deferred) {
            // get the appropriate component icon
            var smallIconClass = '';
            if (connection.destination.type === 'PROCESSOR') {
                smallIconClass = 'processor-small-icon';
            } else if (connection.destination.type === 'OUTPUT_PORT') {
                smallIconClass = 'output-port-small-icon';
            }

            // component
            var downstreamComponent = $('<div class="destination-component"></div>').appendTo(container);
            $('<div class="search-result-icon"></div>').addClass(smallIconClass).appendTo(downstreamComponent);
            $('<div class="destination-component-name go-to-link"></div>').attr('title', connection.destination.name).text(connection.destination.name).on('click', function () {
                // go to the component
                nf.CanvasUtils.showComponent(connection.destination.groupId, connection.destination.id);

                // close the dialog
                $('#connections-dialog').modal('hide');
            }).appendTo(downstreamComponent);

            // clear
            $('<div class="clear"></div>').appendTo(downstreamComponent);

            deferred.resolve();
        }).promise();
    };

    /**
     * Adds a destination input port to the dialog.
     * 
     * @param {jQuery} container                    The container to add to
     * @param {object} connection                   The connection to the downstream input port
     */
    var addDestinationInputPort = function (container, connection) {
        // get the remote process group
        return getProcessGroup(connection.destination.groupId).done(function (response) {
            var processGroup = response.processGroup;

            // process group
            var downstreamComponent = $('<div class="destination-component"></div>').appendTo(container);
            $('<div class="search-result-icon process-group-small-icon"></div>').appendTo(downstreamComponent);
            $('<div class="destination-component-name go-to-link"></div>').attr('title', processGroup.name).text(processGroup.name).on('click', function () {
                // go to the remote process group
                nf.CanvasUtils.showComponent(processGroup.parentGroupId, processGroup.id);

                // close the dialog
                $('#connections-dialog').modal('hide');
            }).appendTo(downstreamComponent);

            // clear
            $('<div class="clear"></div>').appendTo(downstreamComponent);

            // input port
            var downstreamInputPort = $('<div class="destination-input-port"></div>').appendTo(downstreamComponent);
            $('<div class="search-result-icon input-port-small-icon"></div>').appendTo(downstreamInputPort);
            $('<div class="destination-input-port-name go-to-link"></div>').attr('title', connection.destination.name).text(connection.destination.name).on('click', function () {
                // go to the remote process group
                nf.CanvasUtils.showComponent(connection.destination.groupId, connection.destination.id);

                // close the dialog
                $('#connections-dialog').modal('hide');
            }).appendTo(downstreamInputPort);

            // clear
            $('<div class="clear"></div>').appendTo(downstreamComponent);
        });
    };

    /**
     * Adds a destination remote input port to the dialog.
     * 
     * @param {jQuery} container                    The container to add to
     * @param {object} connection                   The connection to the downstream remote input port
     */
    var addDestinationRemoteInputPort = function (container, connection) {
        // get the remote process group
        return getRemoteProcessGroup(connection.parentGroupId, connection.destination.groupId).done(function (response) {
            var remoteProcessGroup = response.remoteProcessGroup;

            // remote process group
            var downstreamComponent = $('<div class="destination-component"></div>').appendTo(container);
            $('<div class="search-result-icon remote-process-group-small-icon"></div>').appendTo(downstreamComponent);
            $('<div class="destination-component-name go-to-link"></div>').attr('title', remoteProcessGroup.name).text(remoteProcessGroup.name).on('click', function () {
                // go to the remote process group
                nf.CanvasUtils.showComponent(remoteProcessGroup.parentGroupId, remoteProcessGroup.id);

                // close the dialog
                $('#connections-dialog').modal('hide');
            }).appendTo(downstreamComponent);

            // clear
            $('<div class="clear"></div>').appendTo(downstreamComponent);

            // remote input port
            var downstreamInputPort = $('<div class="destination-input-port"></div>').appendTo(downstreamComponent);
            $('<div class="search-result-icon input-port-small-icon"></div>').appendTo(downstreamInputPort);
            $('<div class="destination-input-port-name"></div>').attr('title', connection.destination.name).text(connection.destination.name).appendTo(downstreamInputPort);

            // clear
            $('<div class="clear"></div>').appendTo(downstreamComponent);
        });
    };

    /**
     * Adds a source component to the dialog.
     * 
     * @param {jQuery} container                    The container to add to
     * @param {object} connection                   The connection to the upstream component
     */
    var addSourceComponent = function (container, connection) {
        return $.Deferred(function (deferred) {
            // get the appropriate component icon
            var smallIconClass = '';
            if (connection.source.type === 'PROCESSOR') {
                smallIconClass = 'processor-small-icon';
            } else if (connection.source.type === 'INPUT_PORT') {
                smallIconClass = 'input-port-small-icon';
            }

            // component
            var sourceComponent = $('<div class="source-component"></div>').appendTo(container);
            $('<div class="search-result-icon"></div>').addClass(smallIconClass).appendTo(sourceComponent);
            $('<div class="source-component-name go-to-link"></div>').attr('title', connection.source.name).text(connection.source.name).on('click', function () {
                // go to the component
                nf.CanvasUtils.showComponent(connection.source.groupId, connection.source.id);

                // close the dialog
                $('#connections-dialog').modal('hide');
            }).appendTo(sourceComponent);

            deferred.resolve();
        }).promise();
    };

    /**
     * Adds a source output port to the dialog.
     * 
     * @param {jQuery} container                    The container to add to
     * @param {object} connection                   The connection 
     */
    var addSourceOutputPort = function (container, connection) {
        // get the remote process group
        return getProcessGroup(connection.source.groupId).done(function (response) {
            var processGroup = response.processGroup;

            // process group
            var sourceComponent = $('<div class="source-component"></div>').appendTo(container);
            $('<div class="search-result-icon process-group-small-icon"></div>').appendTo(sourceComponent);
            $('<div class="source-component-name go-to-link"></div>').attr('title', processGroup.name).text(processGroup.name).on('click', function () {
                // go to the process group
                nf.CanvasUtils.showComponent(processGroup.parentGroupId, processGroup.id);

                // close the dialog
                $('#connections-dialog').modal('hide');
            }).appendTo(sourceComponent);
            $('<div class="clear"></div>').appendTo(sourceComponent);
            var sourceOutputPort = $('<div class="source-output-port"></div>').appendTo(sourceComponent);
            $('<div class="search-result-icon output-port-small-icon"></div>').appendTo(sourceOutputPort);
            $('<div class="source-output-port-name go-to-link"></div>').attr('title', connection.source.name).text(connection.source.name).on('click', function () {
                // go to the output port
                nf.CanvasUtils.showComponent(connection.source.groupId, connection.source.id);

                // close the dialog
                $('#connections-dialog').modal('hide');
            }).appendTo(sourceOutputPort);
        });
    };

    /**
     * Adds a source remote output port to the dialog.
     * 
     * @param {jQuery} container                    The container to add to
     * @param {object} connection                   The connection to the downstream remote output port
     */
    var addSourceRemoteOutputPort = function (container, connection) {
        // get the remote process group
        return getRemoteProcessGroup(connection.parentGroupId, connection.source.groupId).done(function (response) {
            var remoteProcessGroup = response.remoteProcessGroup;

            // remote process group
            var sourceComponent = $('<div class="source-component"></div>').appendTo(container);
            $('<div class="search-result-icon remote-process-group-small-icon"></div>').appendTo(sourceComponent);
            $('<div class="source-component-name go-to-link"></div>').attr('title', remoteProcessGroup.name).text(remoteProcessGroup.name).on('click', function () {
                // go to the remote process group
                nf.CanvasUtils.showComponent(remoteProcessGroup.parentGroupId, remoteProcessGroup.id);

                // close the dialog
                $('#connections-dialog').modal('hide');
            }).appendTo(sourceComponent);
            $('<div class="clear"></div>').appendTo(sourceComponent);
            var sourceOutputPort = $('<div class="source-output-port"></div>').appendTo(sourceComponent);
            $('<div class="search-result-icon output-port-small-icon"></div>').appendTo(sourceOutputPort);
            $('<div class="source-output-port-name"></div>').attr('title', connection.source.name).text(connection.source.name).appendTo(sourceOutputPort);
        });
    };

    return {
        /**
         * Initializes the dialog.
         */
        init: function () {
            $('#connections-dialog').modal({
                overlayBackground: false,
                buttons: [{
                        buttonText: 'Close',
                        handler: {
                            click: function () {
                                $('#connections-dialog').modal('hide');
                            }
                        }
                    }],
                handler: {
                    close: function () {
                        $('#connections-context').empty();
                        $('#connections-container').empty();
                    }
                }
            });
        },
        
        /**
         * Shows components downstream from a processor.
         * 
         * @param {selection} selection The processor selection
         */
        showDownstreamFromProcessor: function (selection) {
            var selectionData = selection.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.controller + '/process-groups/' + encodeURIComponent(selectionData.component.parentGroupId) + '/connections',
                dataType: 'json'
            }).done(function (response) {
                var connections = response.connections;

                // populate the downstream dialog
                $('#connections-context')
                        .append('<div class="search-result-icon processor-small-icon"></div>')
                        .append($('<div class="connections-component-name"></div>').text(selectionData.component.name))
                        .append('<div class="clear"></div>');

                // add the destination for each connection
                $.each(connections, function (_, connection) {
                    // only show connections for which this selection is the source
                    if (connection.source.id === selectionData.component.id) {
                        addConnection(connection);
                    }
                });

                // ensure there are downstream components
                if ($('#connections-container').is(':empty')) {
                    $('#connections-container').html('<span class="unset">No downstream components</span>');
                }

                // show the downstream dialog
                $('#connections-dialog').modal('setHeaderText', 'Downstream Connections').modal('show');
            }).fail(nf.Common.handleAjaxError);
        },
        
        /**
         * Shows components upstream from a processor.
         * 
         * @param {selection} selection The processor selection
         */
        showUpstreamFromProcessor: function (selection) {
            var selectionData = selection.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.controller + '/process-groups/' + encodeURIComponent(selectionData.component.parentGroupId) + '/connections',
                dataType: 'json'
            }).done(function (response) {
                var connections = response.connections;

                // populate the upstream dialog
                $('#connections-context')
                        .append('<div class="search-result-icon processor-small-icon"></div>')
                        .append($('<div class="connections-component-name"></div>').text(selectionData.component.name))
                        .append('<div class="clear"></div>');

                // add the source for each connection
                $.each(connections, function (_, connection) {
                    // only show connections for which this selection is the destination
                    if (connection.destination.id === selectionData.component.id) {
                        addConnection(connection);
                    }
                });

                // ensure there are upstream components
                if ($('#connections-container').is(':empty')) {
                    $('#connections-container').html('<span class="unset">No upstream components</span>');
                }

                // show the upstream dialog
                $('#connections-dialog').modal('setHeaderText', 'Upstream Connections').modal('show');
            }).fail(nf.Common.handleAjaxError);
        },
        
        /**
         * Shows components downstream from a process group or a remote process group.
         * 
         * @param {selection} selection The process group or remote process group selection
         */
        showDownstreamFromGroup: function (selection) {
            var selectionData = selection.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.controller + '/process-groups/' + encodeURIComponent(selectionData.component.parentGroupId) + '/connections',
                dataType: 'json'
            }).done(function (response) {
                var connections = response.connections;

                // populate the downstream dialog
                $('#connections-context')
                        .append('<div class="search-result-icon process-group-small-icon"></div>')
                        .append($('<div class="connections-component-name"></div>').text(selectionData.component.name))
                        .append('<div class="clear"></div>');

                // add the destination for each connection
                $.each(connections, function (_, connection) {
                    // only show connections for which this selection is the source
                    if (connection.source.groupId === selectionData.component.id) {
                        addConnection(connection);
                    }
                });

                // ensure there are downstream components
                if ($('#connections-container').is(':empty')) {
                    $('#connections-container').html('<span class="unset">No downstream components</span>');
                }

                // show the downstream dialog
                $('#connections-dialog').modal('setHeaderText', 'Downstream Connections').modal('show');
            }).fail(nf.Common.handleAjaxError);
        },
        
        /**
         * Shows components upstream from a process group or a remote process group.
         * 
         * @param {selection} selection The process group or remote process group selection
         */
        showUpstreamFromGroup: function (selection) {
            var selectionData = selection.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.controller + '/process-groups/' + encodeURIComponent(selectionData.component.parentGroupId) + '/connections',
                dataType: 'json'
            }).done(function (response) {
                var connections = response.connections;

                // populate the upstream dialog
                $('#connections-context')
                        .append('<div class="search-result-icon process-group-small-icon"></div>')
                        .append($('<div class="connections-component-name"></div>').text(selectionData.component.name))
                        .append('<div class="clear"></div>');

                // add the source for each connection
                $.each(connections, function (_, connection) {
                    // only show connections for which this selection is the destination
                    if (connection.destination.groupId === selectionData.component.id) {
                        addConnection(connection);
                    }
                });

                // ensure there are upstream components
                if ($('#connections-container').is(':empty')) {
                    $('#connections-container').html('<span class="unset">No upstream components</span>');
                }

                // show the dialog
                $('#connections-dialog').modal('setHeaderText', 'Upstream Connections').modal('show');
            }).fail(nf.Common.handleAjaxError);
        },
        
        /**
         * Shows components downstream from an input port.
         * 
         * @param {selection} selection The input port selection
         */
        showDownstreamFromInputPort: function (selection) {
            var selectionData = selection.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.controller + '/process-groups/' + encodeURIComponent(selectionData.component.parentGroupId) + '/connections',
                dataType: 'json'
            }).done(function (response) {
                var connections = response.connections;

                // populate the downstream dialog
                $('#connections-context')
                        .append('<div class="search-result-icon input-port-small-icon"></div>')
                        .append($('<div class="connections-component-name"></div>').text(selectionData.component.name))
                        .append('<div class="clear"></div>');

                // add the destination for each connection
                $.each(connections, function (_, connection) {
                    // only show connections for which this selection is the source
                    if (connection.source.id === selectionData.component.id) {
                        addConnection(connection);
                    }
                });

                // ensure there are downstream components
                if ($('#connections-container').is(':empty')) {
                    $('#connections-container').html('<span class="unset">No downstream components</span>');
                }

                // show the downstream dialog
                $('#connections-dialog').modal('setHeaderText', 'Downstream Connections').modal('show');
            }).fail(nf.Common.handleAjaxError);
        },
        
        /**
         * Shows components upstream from an input port.
         * 
         * @param {selection} selection The input port selection
         */
        showUpstreamFromInputPort: function (selection) {
            var selectionData = selection.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.controller + '/process-groups/' + encodeURIComponent(nf.Canvas.getParentGroupId()) + '/connections',
                dataType: 'json'
            }).done(function (response) {
                var connections = response.connections;

                // populate the upstream dialog
                $('#connections-context')
                        .append('<div class="search-result-icon process-group-small-icon"></div>')
                        .append($('<div class="connections-component-name"></div>').text(nf.Canvas.getGroupName()))
                        .append('<div class="clear"></div>')
                        .append('<div class="search-result-icon input-port-small-icon" style="margin-left: 20px;"></div>')
                        .append($('<div class="connections-component-name"></div>').text(selectionData.component.name))
                        .append('<div class="clear"></div>');

                // add the source for each connection
                $.each(connections, function (_, connection) {
                    // only show connections for which this selection is the destination
                    if (connection.destination.id === selectionData.component.id) {
                        addConnection(connection);
                    }
                });

                // ensure there are upstream components
                if ($('#connections-container').is(':empty')) {
                    $('#connections-container').html('<span class="unset">No upstream components</span>');
                }

                // show the upstream dialog
                $('#connections-dialog').modal('setHeaderText', 'Upstream Connections').modal('show');
            }).fail(nf.Common.handleAjaxError);
        },
        
        /**
         * Shows components downstream from an output port.
         * 
         * @param {selection} selection The output port selection
         */
        showDownstreamFromOutputPort: function (selection) {
            var selectionData = selection.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.controller + '/process-groups/' + encodeURIComponent(nf.Canvas.getParentGroupId()) + '/connections',
                dataType: 'json'
            }).done(function (response) {
                var connections = response.connections;

                // populate the downstream dialog
                $('#connections-context')
                        .append('<div class="search-result-icon process-group-small-icon"></div>')
                        .append($('<div class="connections-component-name"></div>').text(nf.Canvas.getGroupName()))
                        .append('<div class="clear"></div>')
                        .append('<div class="search-result-icon output-port-small-icon" style="margin-left: 20px;"></div>')
                        .append($('<div class="connections-component-name"></div>').text(selectionData.component.name))
                        .append('<div class="clear"></div>');

                // add the destination for each connection
                $.each(connections, function (_, connection) {
                    // only show connections for which this selection is the source
                    if (connection.source.id === selectionData.component.id) {
                        addConnection(connection);
                    }
                });

                // ensure there are downstream components
                if ($('#connections-container').is(':empty')) {
                    $('#connections-container').html('<span class="unset">No downstream components</span>');
                }

                // show the downstream dialog
                $('#connections-dialog').modal('setHeaderText', 'Downstream Connections').modal('show');
            }).fail(nf.Common.handleAjaxError);
        },
        
        /**
         * Shows components upstream from an output port.
         * 
         * @param {selection} selection The output port selection
         */
        showUpstreamFromOutputPort: function (selection) {
            var selectionData = selection.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.controller + '/process-groups/' + encodeURIComponent(selectionData.component.parentGroupId) + '/connections',
                dataType: 'json'
            }).done(function (response) {
                var connections = response.connections;

                // populate the upstream dialog
                $('#connections-context')
                        .append('<div class="search-result-icon input-port-small-icon"></div>')
                        .append($('<div class="connections-component-name"></div>').text(selectionData.component.name))
                        .append('<div class="clear"></div>');

                // add the source for each connection
                $.each(connections, function (_, connection) {
                    // only show connections for which this selection is the destination
                    if (connection.destination.id === selectionData.component.id) {
                        addConnection(connection);
                    }
                });

                // ensure there are upstream components
                if ($('#connections-container').is(':empty')) {
                    $('#connections-container').html('<span class="unset">No upstream components</span>');
                }

                // show the upstream dialog
                $('#connections-dialog').modal('setHeaderText', 'Upstream Connections').modal('show');
            }).fail(nf.Common.handleAjaxError);
        },
        
        /**
         * Shows components downstream from a funnel.
         * 
         * @param {selection} selection The funnel selection
         */
        showDownstreamFromFunnel: function (selection) {
            var selectionData = selection.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.controller + '/process-groups/' + encodeURIComponent(selectionData.component.parentGroupId) + '/connections',
                dataType: 'json'
            }).done(function (response) {
                var connections = response.connections;

                // populate the downstream dialog
                $('#connections-context').append($('<div class="connections-component-name"></div>').text('Funnel'));

                // add the destination for each connection
                $.each(connections, function (_, connection) {
                    // only show connections for which this selection is the source
                    if (connection.source.id === selectionData.component.id) {
                        addConnection(connection);
                    }
                });

                // ensure there are downstream components
                if ($('#connections-container').is(':empty')) {
                    $('#connections-container').html('<span class="unset">No downstream components</span>');
                }

                // show the downstream dialog
                $('#connections-dialog').modal('setHeaderText', 'Downstream Connections').modal('show');
            }).fail(nf.Common.handleAjaxError);
        },
        
        /**
         * Shows components upstream from a funnel.
         * 
         * @param {selection} selection The funnel selection
         */
        showUpstreamFromFunnel: function (selection) {
            var selectionData = selection.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.controller + '/process-groups/' + encodeURIComponent(selectionData.component.parentGroupId) + '/connections',
                dataType: 'json'
            }).done(function (response) {
                var connections = response.connections;

                // populate the upstream dialog
                $('#connections-context').append($('<div class="upstream-destination-name"></div>').text('Funnel'));

                // add the source for each connection
                $.each(connections, function (_, connection) {
                    // only show connections for which this selection is the destination
                    if (connection.destination.id === selectionData.component.id) {
                        addConnection(connection);
                    }
                });

                // ensure there are upstream components
                if ($('#connections-container').is(':empty')) {
                    $('#connections-container').html('<span class="unset">No upstream components</span>');
                }

                // show the upstream dialog
                $('#connections-dialog').modal('setHeaderText', 'Upstream Connections').modal('show');
            }).fail(nf.Common.handleAjaxError);
        }
    };
}());