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
 * Handles the upstream/downstream dialogs.
 */
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'nf.ErrorHandler',
                'nf.CanvasUtils'],
            function ($, nfErrorHandler, nfCanvasUtils) {
                return (nf.GoTo = factory($, nfErrorHandler, nfCanvasUtils));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.GoTo =
            factory(require('jquery'),
                require('nf.ErrorHandler'),
                require('nf.CanvasUtils')));
    } else {
        nf.GoTo = factory(root.$,
            root.nf.ErrorHandler,
            root.nf.CanvasUtils);
    }
}(this, function ($, nfErrorHandler, nfCanvasUtils) {
    'use strict';

    var config = {
        urls: {
            api: '../nifi-api',
            processGroups: '../nifi-api/flow/process-groups/'
        }
    };
    
    var currentComponentId;
    var currentComponentLabel;

    /**
     * Adds a connection to the connections dialog.
     *
     * @param {string}  parentProcessGroupId
     * @param {object}  connectionEntity
     * @param {array}   processGroupEntities
     * @param {array}   remoteProcessGroupEntities
     */
    var addConnection = function (parentProcessGroupId, connectionEntity, processGroupEntities, remoteProcessGroupEntities) {
        var container = $('<div class="source-destination-connection"></div>').appendTo('#connections-container');

        if (connectionEntity.sourceType === 'OUTPUT_PORT') {
            addSourceOutputPort(parentProcessGroupId, container, connectionEntity, processGroupEntities);
        } else if (connectionEntity.sourceType === 'REMOTE_OUTPUT_PORT') {
            addSourceRemoteOutputPort(parentProcessGroupId, container, connectionEntity, remoteProcessGroupEntities);
        } else {
            addSourceComponent(container, connectionEntity);
        }

        var connectionEntry = $('<div class="connection-entry"></div>').appendTo(container);

        // format the connection name.. fall back to id
        var connectionStyle = 'unset';
        var connectionName = 'Connection';
        if (connectionEntity.permissions.canRead === true) {
            var formattedConnectionName = nfCanvasUtils.formatConnectionName(connectionEntity.component);
            if (formattedConnectionName !== '') {
                connectionStyle = '';
                connectionName = formattedConnectionName;
            }
        }

        // connection
        $('<div class="search-result-icon icon-connect"></div>').appendTo(connectionEntry);
        $('<div class="connection-entry-name go-to-link"></div>').attr('title', connectionName).addClass(connectionStyle).text(connectionName).on('click', function () {
            // go to the connection
            nfCanvasUtils.showComponent(parentProcessGroupId, connectionEntity.id);

            // close the dialog
            $('#connections-dialog').modal('hide');
        }).appendTo(connectionEntry);

        // clear
        $('<div class="clear"></div>').appendTo(connectionEntry);

        if (connectionEntity.destinationType === 'INPUT_PORT') {
            addDestinationInputPort(parentProcessGroupId, container, connectionEntity, processGroupEntities);
        } else if (connectionEntity.destinationType === 'REMOTE_INPUT_PORT') {
            addDestinationRemoteInputPort(parentProcessGroupId, container, connectionEntity, remoteProcessGroupEntities);
        } else {
            addDestinationComponent(container, connectionEntity);
        }

        // clear
        $('<div class="clear"></div>').appendTo(container);
    };

    /**
     * Adds a destination component to the dialog.
     *
     * @param {jQuery} container                    The container to add to
     * @param {object} connectionEntity             The connection to the downstream component
     */
    var addDestinationComponent = function (container, connectionEntity) {
        // get the appropriate component icon
        var smallIconClass = 'icon-funnel';
        if (connectionEntity.destinationType === 'PROCESSOR') {
            smallIconClass = 'icon-processor';
        } else if (connectionEntity.destinationType === 'OUTPUT_PORT') {
            smallIconClass = 'icon-port-out';
        }

        // get the source name
        var destinationName = connectionEntity.destinationId;
        if (connectionEntity.permissions.canRead === true) {
            destinationName = connectionEntity.component.destination.name;
        } else if (currentComponentId === connectionEntity.destinationId) {
            destinationName = currentComponentLabel;
        }

        // component
        var downstreamComponent = $('<div class="destination-component"></div>').appendTo(container);
        $('<div class="search-result-icon"></div>').addClass(smallIconClass).appendTo(downstreamComponent);
        $('<div class="destination-component-name go-to-link"></div>').attr('title', destinationName).text(destinationName).on('click', function () {
            // go to the component
            nfCanvasUtils.showComponent(connectionEntity.destinationGroupId, connectionEntity.destinationId);

            // close the dialog
            $('#connections-dialog').modal('hide');
        }).appendTo(downstreamComponent);

        // clear
        $('<div class="clear"></div>').appendTo(downstreamComponent);
    };

    /**
     * Adds a destination input port to the dialog.
     *
     * @param {string} parentProcessGroupId               The process group id
     * @param {jQuery} container                    The container to add to
     * @param {object} connectionEntity             The connection to the downstream input port
     * @param {array}  processGroupEntities         The process groups
     */
    var addDestinationInputPort = function (parentProcessGroupId, container, connectionEntity, processGroupEntities) {
        var processGroupEntity;
        $.each(processGroupEntities, function (_, pge) {
            if (connectionEntity.destinationGroupId === pge.id) {
                processGroupEntity = pge;
                return false;
            }
        });

        // get the group label
        var groupLabel = getDisplayName(processGroupEntity);

        // process group
        var downstreamComponent = $('<div class="destination-component"></div>').appendTo(container);
        $('<div class="search-result-icon icon-group"></div>').appendTo(downstreamComponent);
        $('<div class="destination-component-name go-to-link"></div>').attr('title', groupLabel).text(groupLabel).on('click', function () {
            // go to the remote process group
            nfCanvasUtils.showComponent(parentProcessGroupId, processGroupEntity.id);

            // close the dialog
            $('#connections-dialog').modal('hide');
        }).appendTo(downstreamComponent);

        // clear
        $('<div class="clear"></div>').appendTo(downstreamComponent);

        // get the port label
        var portLabel = connectionEntity.destinationId;
        if (connectionEntity.permissions.canRead === true) {
            portLabel = connectionEntity.component.destination.name;
        } else if (currentComponentId === connectionEntity.destinationId) {
            portLabel = currentComponentLabel;
        }

        // input port
        var downstreamInputPort = $('<div class="destination-input-port"></div>').appendTo(downstreamComponent);
        $('<div class="search-result-icon icon-port-in"></div>').appendTo(downstreamInputPort);
        $('<div class="destination-input-port-name go-to-link"></div>').attr('title', portLabel).text(portLabel).on('click', function () {
            // go to the remote process group
            nfCanvasUtils.showComponent(connectionEntity.destinationGroupId, connectionEntity.destinationId);

            // close the dialog
            $('#connections-dialog').modal('hide');
        }).appendTo(downstreamInputPort);

        // clear
        $('<div class="clear"></div>').appendTo(downstreamComponent);
    };

    /**
     * Adds a destination remote input port to the dialog.
     *
     * @param {string} parentProcessGroupId               The process group id
     * @param {jQuery} container                    The container to add to
     * @param {object} connectionEntity             The connection to the downstream remote input port
     * @param {array} remoteProcessGroupEntities    The remote process groups
     */
    var addDestinationRemoteInputPort = function (parentProcessGroupId, container, connectionEntity, remoteProcessGroupEntities) {
        var remoteProcessGroupEntity;
        $.each(remoteProcessGroupEntities, function (_, rpge) {
            if (connectionEntity.destinationGroupId === rpge.id) {
                remoteProcessGroupEntity = rpge;
                return false;
            }
        });

        // get the group label
        var remoteGroupLabel = getDisplayName(remoteProcessGroupEntity);

        // remote process group
        var downstreamComponent = $('<div class="destination-component"></div>').appendTo(container);
        $('<div class="search-result-icon icon-group-remote"></div>').appendTo(downstreamComponent);
        $('<div class="destination-component-name go-to-link"></div>').attr('title', remoteGroupLabel).text(remoteGroupLabel).on('click', function () {
            // go to the remote process group
            nfCanvasUtils.showComponent(parentProcessGroupId, remoteProcessGroupEntity.id);

            // close the dialog
            $('#connections-dialog').modal('hide');
        }).appendTo(downstreamComponent);

        // clear
        $('<div class="clear"></div>').appendTo(downstreamComponent);

        // get the port label
        var remotePortLabel = connectionEntity.destinationId;
        if (connectionEntity.permissions.canRead === true) {
            remotePortLabel = connectionEntity.component.destination.name;
        } else if (currentComponentId === connectionEntity.destinationId) {
            remotePortLabel = currentComponentLabel;
        }

        // remote input port
        var downstreamInputPort = $('<div class="destination-input-port"></div>').appendTo(downstreamComponent);
        $('<div class="search-result-icon icon-port-in"></div>').appendTo(downstreamInputPort);
        $('<div class="destination-input-port-name"></div>').attr('title', remotePortLabel).text(remotePortLabel).appendTo(downstreamInputPort);

        // clear
        $('<div class="clear"></div>').appendTo(downstreamComponent);
    };

    /**
     * Adds a source component to the dialog.
     *
     * @param {jQuery} container                    The container to add to
     * @param {object} connectionEntity             The connection to the upstream component
     */
    var addSourceComponent = function (container, connectionEntity) {
        // get the appropriate component icon
        var smallIconClass = 'icon-funnel';
        if (connectionEntity.sourceType === 'PROCESSOR') {
            smallIconClass = 'icon-processor';
        } else if (connectionEntity.sourceType === 'INPUT_PORT') {
            smallIconClass = 'icon-port-in';
        }

        // get the source name
        var sourceName = connectionEntity.sourceId;
        if (connectionEntity.permissions.canRead === true) {
            sourceName = connectionEntity.component.source.name;
        } else if (currentComponentId === connectionEntity.sourceId) {
            sourceName = currentComponentLabel;
        }

        // component
        var sourceComponent = $('<div class="source-component"></div>').appendTo(container);
        $('<div class="search-result-icon"></div>').addClass(smallIconClass).appendTo(sourceComponent);
        $('<div class="source-component-name go-to-link"></div>').attr('title', sourceName).text(sourceName).on('click', function () {
            // go to the component
            nfCanvasUtils.showComponent(connectionEntity.sourceGroupId, connectionEntity.sourceId);

            // close the dialog
            $('#connections-dialog').modal('hide');
        }).appendTo(sourceComponent);

        // clear
        $('<div class="clear"></div>').appendTo(sourceComponent);
    };

    /**
     * Adds a source output port to the dialog.
     *
     * @param {string} parentProcessGroupId               The process group id
     * @param {jQuery} container                    The container to add to
     * @param {object} connectionEntity             The connection
     * @param {array} processGroupEntities          The process groups
     */
    var addSourceOutputPort = function (parentProcessGroupId, container, connectionEntity, processGroupEntities) {
        var processGroupEntity;
        $.each(processGroupEntities, function (_, pge) {
            if (connectionEntity.sourceGroupId === pge.id) {
                processGroupEntity = pge;
                return false;
            }
        });

        // get the group label
        var groupLabel = getDisplayName(processGroupEntity);

        // process group
        var sourceComponent = $('<div class="source-component"></div>').appendTo(container);
        $('<div class="search-result-icon icon-group"></div>').appendTo(sourceComponent);
        $('<div class="source-component-name go-to-link"></div>').attr('title', groupLabel).text(groupLabel).on('click', function () {
            // go to the process group
            nfCanvasUtils.showComponent(parentProcessGroupId, processGroupEntity.id);

            // close the dialog
            $('#connections-dialog').modal('hide');
        }).appendTo(sourceComponent);

        // clear
        $('<div class="clear"></div>').appendTo(sourceComponent);

        // get the port label
        var portLabel = connectionEntity.sourceId;
        if (connectionEntity.permissions.canRead === true) {
            portLabel = connectionEntity.component.source.name;
        } else if (currentComponentId === connectionEntity.sourceId) {
            portLabel = currentComponentLabel;
        }

        var sourceOutputPort = $('<div class="source-output-port"></div>').appendTo(sourceComponent);
        $('<div class="search-result-icon icon-port-out"></div>').appendTo(sourceOutputPort);
        $('<div class="source-output-port-name go-to-link"></div>').attr('title', portLabel).text(portLabel).on('click', function () {
            // go to the output port
            nfCanvasUtils.showComponent(connectionEntity.sourceGroupId, connectionEntity.sourceId);

            // close the dialog
            $('#connections-dialog').modal('hide');
        }).appendTo(sourceOutputPort);

        // clear
        $('<div class="clear"></div>').appendTo(sourceComponent);
    };

    /**
     * Adds a source remote output port to the dialog.
     *
     * @param {string} parentProcessGroupId               The process group id
     * @param {jQuery} container                    The container to add to
     * @param {object} connectionEntity             The connection to the downstream remote output port
     * @param {array} remoteProcessGroupEntities    The remote process groups
     */
    var addSourceRemoteOutputPort = function (parentProcessGroupId, container, connectionEntity, remoteProcessGroupEntities) {
        var remoteProcessGroupEntity;
        $.each(remoteProcessGroupEntities, function (_, rpge) {
            if (connectionEntity.sourceGroupId === rpge.id) {
                remoteProcessGroupEntity = rpge;
                return false;
            }
        });

        // get the group label
        var remoteGroupLabel = getDisplayName(remoteProcessGroupEntity);

        // remote process group
        var sourceComponent = $('<div class="source-component"></div>').appendTo(container);
        $('<div class="search-result-icon icon-group-remote"></div>').appendTo(sourceComponent);
        $('<div class="source-component-name go-to-link"></div>').attr('title', remoteGroupLabel).text(remoteGroupLabel).on('click', function () {
            // go to the remote process group
            nfCanvasUtils.showComponent(parentProcessGroupId, remoteProcessGroupEntity.id);

            // close the dialog
            $('#connections-dialog').modal('hide');
        }).appendTo(sourceComponent);

        // clear
        $('<div class="clear"></div>').appendTo(sourceComponent);

        // get the port label
        var remotePortLabel = connectionEntity.sourceId;
        if (connectionEntity.permissions.canRead === true) {
            remotePortLabel = connectionEntity.component.source.name;
        } else if (connectionEntity.sourceId === currentComponentId) {
            remotePortLabel = currentComponentLabel;
        }

        // port markup
        var sourceOutputPort = $('<div class="source-output-port"></div>').appendTo(sourceComponent);
        $('<div class="search-result-icon icon-port-out"></div>').appendTo(sourceOutputPort);
        $('<div class="source-output-port-name"></div>').attr('title', remotePortLabel).text(remotePortLabel).appendTo(sourceOutputPort);

        // clear
        $('<div class="clear"></div>').appendTo(sourceComponent);
    };

    /**
     * Gets the display name for the specified entity.
     *
     * @param entity entity
     * @returns display name
     */
    var getDisplayName = function (entity) {
        if (entity.permissions.canRead === true) {
            return entity.component.name;
        } else if (currentComponentId === entity.id) {
            return currentComponentLabel;
        } else {
            return entity.id;
        }
    };

    return {
        /**
         * Initializes the dialog.
         */
        init: function () {
            $('#connections-dialog').modal({
                scrollableContentStyle: 'scrollable',
                buttons: [{
                    buttonText: 'Close',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
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
            var processorEntity = selection.datum();
            var connections = nfCanvasUtils.getComponentByType('Connection').get();
            var processGroups = nfCanvasUtils.getComponentByType('ProcessGroup').get();
            var remoteProcessGroups = nfCanvasUtils.getComponentByType('RemoteProcessGroup').get();

            var processorLabel = getDisplayName(processorEntity);

            // record details of the current component
            currentComponentId = processorEntity.id;
            currentComponentLabel = processorLabel;

            // populate the downstream dialog
            $('#connections-context')
                .append('<div class="search-result-icon icon-processor"></div>')
                .append($('<div class="connections-component-name"></div>').text(processorLabel))
                .append('<div class="clear"></div>');

            // add the destination for each connection
            $.each(connections, function (_, connectionEntity) {
                // only show connections for which this selection is the source
                if (connectionEntity.sourceId === processorEntity.id) {
                    addConnection(nfCanvasUtils.getGroupId(), connectionEntity, processGroups, remoteProcessGroups);
                }
            });

            // ensure there are downstream components
            if ($('#connections-container').is(':empty')) {
                $('#connections-container').html('<span class="unset">No downstream components</span>');
            }

            // show the downstream dialog
            $('#connections-dialog').modal('setHeaderText', 'Downstream Connections').modal('show');
        },

        /**
         * Shows components upstream from a processor.
         *
         * @param {selection} selection The processor selection
         */
        showUpstreamFromProcessor: function (selection) {
            var processorEntity = selection.datum();
            var connections = nfCanvasUtils.getComponentByType('Connection').get();
            var processGroups = nfCanvasUtils.getComponentByType('ProcessGroup').get();
            var remoteProcessGroups = nfCanvasUtils.getComponentByType('RemoteProcessGroup').get();

            var processorLabel = getDisplayName(processorEntity);

            // record details of the current component
            currentComponentId = processorEntity.id;
            currentComponentLabel = processorLabel;
            
            // populate the upstream dialog
            $('#connections-context')
                .append('<div class="search-result-icon icon-processor"></div>')
                .append($('<div class="connections-component-name"></div>').text(processorLabel))
                .append('<div class="clear"></div>');

            // add the source for each connection
            $.each(connections, function (_, connectionEntity) {
                // only show connections for which this selection is the destination
                if (connectionEntity.destinationId === processorEntity.id) {
                    addConnection(nfCanvasUtils.getGroupId(), connectionEntity, processGroups, remoteProcessGroups);
                }
            });

            // ensure there are upstream components
            if ($('#connections-container').is(':empty')) {
                $('#connections-container').html('<span class="unset">No upstream components</span>');
            }

            // show the upstream dialog
            $('#connections-dialog').modal('setHeaderText', 'Upstream Connections').modal('show');
        },

        /**
         * Shows components downstream from a process group or a remote process group.
         *
         * @param {selection} selection The process group or remote process group selection
         */
        showDownstreamFromGroup: function (selection) {
            var groupEntity = selection.datum();
            var connections = nfCanvasUtils.getComponentByType('Connection').get();
            var processGroups = nfCanvasUtils.getComponentByType('ProcessGroup').get();
            var remoteProcessGroups = nfCanvasUtils.getComponentByType('RemoteProcessGroup').get();

            var iconStyle = 'icon-group';
            if (nfCanvasUtils.isRemoteProcessGroup(selection)) {
                iconStyle = 'icon-group-remote';
            }

            var groupLabel = getDisplayName(groupEntity);

            // record details of the current component
            currentComponentId = groupEntity.id;
            currentComponentLabel = groupLabel;
            
            // populate the downstream dialog
            $('#connections-context')
                .append($('<div class="search-result-icon"></div>').addClass(iconStyle))
                .append($('<div class="connections-component-name"></div>').text(groupLabel))
                .append('<div class="clear"></div>');

            // add the destination for each connection
            $.each(connections, function (_, connectionEntity) {
                // only show connections for which this selection is the source
                if (connectionEntity.sourceGroupId === groupEntity.id) {
                    addConnection(nfCanvasUtils.getGroupId(), connectionEntity, processGroups, remoteProcessGroups);
                }
            });

            // ensure there are downstream components
            if ($('#connections-container').is(':empty')) {
                $('#connections-container').html('<span class="unset">No downstream components</span>');
            }

            // show the downstream dialog
            $('#connections-dialog').modal('setHeaderText', 'Downstream Connections').modal('show');
        },

        /**
         * Shows components upstream from a process group or a remote process group.
         *
         * @param {selection} selection The process group or remote process group selection
         */
        showUpstreamFromGroup: function (selection) {
            var groupEntity = selection.datum();
            var connections = nfCanvasUtils.getComponentByType('Connection').get();
            var processGroups = nfCanvasUtils.getComponentByType('ProcessGroup').get();
            var remoteProcessGroups = nfCanvasUtils.getComponentByType('RemoteProcessGroup').get();

            var iconStyle = 'icon-group';
            if (nfCanvasUtils.isRemoteProcessGroup(selection)) {
                iconStyle = 'icon-group-remote';
            }

            var groupLabel = getDisplayName(groupEntity);

            // record details of the current component
            currentComponentId = groupEntity.id;
            currentComponentLabel = groupLabel;
            
            // populate the upstream dialog
            $('#connections-context')
                .append($('<div class="search-result-icon"></div>').addClass(iconStyle))
                .append($('<div class="connections-component-name"></div>').text(groupLabel))
                .append('<div class="clear"></div>');

            // add the source for each connection
            $.each(connections, function (_, connectionEntity) {
                // only show connections for which this selection is the destination
                if (connectionEntity.destinationGroupId === groupEntity.id) {
                    addConnection(nfCanvasUtils.getGroupId(), connectionEntity, processGroups, remoteProcessGroups);
                }
            });

            // ensure there are upstream components
            if ($('#connections-container').is(':empty')) {
                $('#connections-container').html('<span class="unset">No upstream components</span>');
            }

            // show the dialog
            $('#connections-dialog').modal('setHeaderText', 'Upstream Connections').modal('show');
        },

        /**
         * Shows components downstream from an input port.
         *
         * @param {selection} selection The input port selection
         */
        showDownstreamFromInputPort: function (selection) {
            var portEntity = selection.datum();
            var connections = nfCanvasUtils.getComponentByType('Connection').get();
            var processGroups = nfCanvasUtils.getComponentByType('ProcessGroup').get();
            var remoteProcessGroups = nfCanvasUtils.getComponentByType('RemoteProcessGroup').get();

            var portLabel = getDisplayName(portEntity);

            // record details of the current component
            currentComponentId = portEntity.id;
            currentComponentLabel = portLabel;
            
            // populate the downstream dialog
            $('#connections-context')
                .append('<div class="search-result-icon icon-port-in"></div>')
                .append($('<div class="connections-component-name"></div>').text(portLabel))
                .append('<div class="clear"></div>');

            // add the destination for each connection
            $.each(connections, function (_, connectionEntity) {
                // only show connections for which this selection is the source
                if (connectionEntity.sourceId === portEntity.id) {
                    addConnection(nfCanvasUtils.getGroupId(), connectionEntity, processGroups, remoteProcessGroups);
                }
            });

            // ensure there are downstream components
            if ($('#connections-container').is(':empty')) {
                $('#connections-container').html('<span class="unset">No downstream components</span>');
            }

            // show the downstream dialog
            $('#connections-dialog').modal('setHeaderText', 'Downstream Connections').modal('show');
        },

        /**
         * Shows components upstream from an input port.
         *
         * @param {selection} selection The input port selection
         */
        showUpstreamFromInputPort: function (selection) {
            var portEntity = selection.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.processGroups + encodeURIComponent(nfCanvasUtils.getParentGroupId()),
                dataType: 'json'
            }).done(function (response) {
                var flow = response.processGroupFlow.flow;
                var connections = flow.connections;
                var processGroups = flow.processGroups;
                var remoteProcessGroups = flow.remoteProcessGroups;

                var portLabel = getDisplayName(portEntity);

                // record details of the current component
                currentComponentId = portEntity.id;
                currentComponentLabel = portLabel;
                
                // populate the upstream dialog
                $('#connections-context')
                    .append('<div class="search-result-icon icon-group"></div>')
                    .append($('<div class="connections-component-name"></div>').text(nfCanvasUtils.getGroupName()))
                    .append('<div class="clear"></div>')
                    .append('<div class="search-result-icon icon-port-in" style="margin-left: 20px;"></div>')
                    .append($('<div class="connections-component-name"></div>').text(portLabel))
                    .append('<div class="clear"></div>');

                // add the source for each connection
                $.each(connections, function (_, connectionEntity) {
                    // only show connections for which this selection is the destination
                    if (connectionEntity.destinationId === portEntity.id) {
                        addConnection(nfCanvasUtils.getParentGroupId(), connectionEntity, processGroups, remoteProcessGroups);
                    }
                });

                // ensure there are upstream components
                if ($('#connections-container').is(':empty')) {
                    $('#connections-container').html('<span class="unset">No upstream components</span>');
                }

                // show the upstream dialog
                $('#connections-dialog').modal('setHeaderText', 'Upstream Connections').modal('show');
            }).fail(nfErrorHandler.handleAjaxError);
        },

        /**
         * Shows components downstream from an output port.
         *
         * @param {selection} selection The output port selection
         */
        showDownstreamFromOutputPort: function (selection) {
            var portEntity = selection.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.processGroups + encodeURIComponent(nfCanvasUtils.getParentGroupId()),
                dataType: 'json'
            }).done(function (response) {
                var flow = response.processGroupFlow.flow;
                var connections = flow.connections;
                var processGroups = flow.processGroups;
                var remoteProcessGroups = flow.remoteProcessGroups;

                var portLabel = getDisplayName(portEntity);

                // record details of the current component
                currentComponentId = portEntity.id;
                currentComponentLabel = portLabel;
                
                // populate the downstream dialog
                $('#connections-context')
                    .append('<div class="search-result-icon icon-group"></div>')
                    .append($('<div class="connections-component-name"></div>').text(nfCanvasUtils.getGroupName()))
                    .append('<div class="clear"></div>')
                    .append('<div class="search-result-icon icon-port-out" style="margin-left: 20px;"></div>')
                    .append($('<div class="connections-component-name"></div>').text(portLabel))
                    .append('<div class="clear"></div>');

                // add the destination for each connection
                $.each(connections, function (_, connectionEntity) {
                    // only show connections for which this selection is the source
                    if (connectionEntity.sourceId === portEntity.id) {
                        addConnection(nfCanvasUtils.getParentGroupId(), connectionEntity, processGroups, remoteProcessGroups);
                    }
                });

                // ensure there are downstream components
                if ($('#connections-container').is(':empty')) {
                    $('#connections-container').html('<span class="unset">No downstream components</span>');
                }

                // show the downstream dialog
                $('#connections-dialog').modal('setHeaderText', 'Downstream Connections').modal('show');
            }).fail(nfErrorHandler.handleAjaxError);
        },

        /**
         * Shows components upstream from an output port.
         *
         * @param {selection} selection The output port selection
         */
        showUpstreamFromOutputPort: function (selection) {
            var portEntity = selection.datum();
            var connections = nfCanvasUtils.getComponentByType('Connection').get();
            var processGroups = nfCanvasUtils.getComponentByType('ProcessGroup').get();
            var remoteProcessGroups = nfCanvasUtils.getComponentByType('RemoteProcessGroup').get();

            var portLabel = getDisplayName(portEntity);
            
            // record details of the current component
            currentComponentId = portEntity.id;
            currentComponentLabel = portLabel;
            
            // populate the upstream dialog
            $('#connections-context')
                .append('<div class="search-result-icon icon-port-out"></div>')
                .append($('<div class="connections-component-name"></div>').text(portLabel))
                .append('<div class="clear"></div>');

            // add the source for each connection
            $.each(connections, function (_, connectionEntity) {
                // only show connections for which this selection is the destination
                if (connectionEntity.destinationId === portEntity.id) {
                    addConnection(nfCanvasUtils.getGroupId(), connectionEntity, processGroups, remoteProcessGroups);
                }
            });

            // ensure there are upstream components
            if ($('#connections-container').is(':empty')) {
                $('#connections-container').html('<span class="unset">No upstream components</span>');
            }

            // show the upstream dialog
            $('#connections-dialog').modal('setHeaderText', 'Upstream Connections').modal('show');
        },

        /**
         * Shows components downstream from a funnel.
         *
         * @param {selection} selection The funnel selection
         */
        showDownstreamFromFunnel: function (selection) {
            var funnelEntity = selection.datum();
            var connections = nfCanvasUtils.getComponentByType('Connection').get();
            var processGroups = nfCanvasUtils.getComponentByType('ProcessGroup').get();
            var remoteProcessGroups = nfCanvasUtils.getComponentByType('RemoteProcessGroup').get();

            // record details of the current component
            currentComponentId = funnelEntity.id;
            currentComponentLabel = 'Funnel';
            
            // populate the downstream dialog
            $('#connections-context')
                .append('<div class="search-result-icon icon-funnel"></div>')
                .append($('<div class="connections-component-name"></div>').text('Funnel'))
                .append('<div class="clear"></div>');

            // add the destination for each connection
            $.each(connections, function (_, connectionEntity) {
                // only show connections for which this selection is the source
                if (connectionEntity.sourceId === funnelEntity.id) {
                    addConnection(nfCanvasUtils.getGroupId(), connectionEntity, processGroups, remoteProcessGroups);
                }
            });

            // ensure there are downstream components
            if ($('#connections-container').is(':empty')) {
                $('#connections-container').html('<span class="unset">No downstream components</span>');
            }

            // show the downstream dialog
            $('#connections-dialog').modal('setHeaderText', 'Downstream Connections').modal('show');
        },

        /**
         * Shows components upstream from a funnel.
         *
         * @param {selection} selection The funnel selection
         */
        showUpstreamFromFunnel: function (selection) {
            var funnelEntity = selection.datum();
            var connections = nfCanvasUtils.getComponentByType('Connection').get();
            var processGroups = nfCanvasUtils.getComponentByType('ProcessGroup').get();
            var remoteProcessGroups = nfCanvasUtils.getComponentByType('RemoteProcessGroup').get();

            // record details of the current component
            currentComponentId = funnelEntity.id;
            currentComponentLabel = 'Funnel';
            
            // populate the upstream dialog
            $('#connections-context')
                .append('<div class="search-result-icon icon-funnel"></div>')
                .append($('<div class="upstream-destination-name"></div>').text('Funnel'))
                .append('<div class="clear"></div>');

            // add the source for each connection
            $.each(connections, function (_, connectionEntity) {
                // only show connections for which this selection is the destination
                if (connectionEntity.destinationId === funnelEntity.id) {
                    addConnection(nfCanvasUtils.getGroupId(), connectionEntity, processGroups, remoteProcessGroups);
                }
            });

            // ensure there are upstream components
            if ($('#connections-container').is(':empty')) {
                $('#connections-container').html('<span class="unset">No upstream components</span>');
            }

            // show the upstream dialog
            $('#connections-dialog').modal('setHeaderText', 'Upstream Connections').modal('show');
        }
    };
}));