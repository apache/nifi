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
                'nf.ErrorHandler',
                'nf.Common',
                'nf.Dialog',
                'nf.Client',
                'nf.CanvasUtils',
                'nf.Connection'],
            function ($, d3, nfErrorHandler, nfCommon, nfDialog, nfClient, nfCanvasUtils, nfConnection) {
                return (nf.ConnectionConfiguration = factory($, d3, nfErrorHandler, nfCommon, nfDialog, nfClient, nfCanvasUtils, nfConnection));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ConnectionConfiguration =
            factory(require('jquery'),
                require('d3'),
                require('nf.ErrorHandler'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.Client'),
                require('nf.CanvasUtils'),
                require('nf.Connection')));
    } else {
        nf.ConnectionConfiguration = factory(root.$,
            root.d3,
            root.nf.ErrorHandler,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.Client,
            root.nf.CanvasUtils,
            root.nf.Connection);
    }
}(this, function ($, d3, nfErrorHandler, nfCommon, nfDialog, nfClient, nfCanvasUtils, nfConnection) {
    'use strict';

    var nfBirdseye;
    var nfGraph;

    var CONNECTION_OFFSET_Y_INCREMENT = 75;
    var CONNECTION_OFFSET_X_INCREMENT = 200;

    var config = {
        urls: {
            api: '../nifi-api',
            prioritizers: '../nifi-api/flow/prioritizers'
        }
    };

    /**
     * Removes the temporary if necessary.
     */
    var removeTempEdge = function () {
        d3.select('path.connector').remove();
    };

    /**
     * Activates dialog's button model refresh on a connection relationships change.
     */
    var addDialogRelationshipsChangeListener = function() {
        // refresh button model when a relationship selection changes
        $('div.available-relationship').bind('change', function() {
            $('#connection-configuration').modal('refreshButtons');
        });
    }

    /**
     * Initializes the source in the new connection dialog.
     *
     * @argument {selection} source        The source
     */
    var initializeSourceNewConnectionDialog = function (source) {
        // handle the selected source
        if (nfCanvasUtils.isProcessor(source)) {
            return $.Deferred(function (deferred) {
                // initialize the source processor
                initializeSourceProcessor(source).done(function (processor) {
                    if (!nfCommon.isEmpty(processor.relationships)) {
                        // populate the available connections
                        $.each(processor.relationships, function (i, relationship) {
                            createRelationshipOption(relationship.name);
                        });
                        
                        addDialogRelationshipsChangeListener();

                        // if there is a single relationship auto select
                        var relationships = $('#relationship-names').children('div');
                        if (relationships.length === 1) {
                            relationships.children('div.available-relationship').removeClass('checkbox-unchecked').addClass('checkbox-checked');
                        }

                        // configure the button model
                        $('#connection-configuration').modal('setButtonModel', [{
                            buttonText: 'Add',
                            color: {
                                base: '#728E9B',
                                hover: '#004849',
                                text: '#ffffff'
                            },
                            disabled: function () {
                                // ensure some relationships were selected
                                return getSelectedRelationships().length === 0;
                            },
                            handler: {
                                click: function () {
                                    addConnection(getSelectedRelationships());

                                    // close the dialog
                                    $('#connection-configuration').modal('hide');
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
                                        $('#connection-configuration').modal('hide');
                                    }
                                }
                            }]);

                        // resolve the deferred
                        deferred.resolve();
                    } else {
                        // there are no relationships for this processor
                        nfDialog.showOkDialog({
                            headerText: 'Connection Configuration',
                            dialogContent: '\'' + nfCommon.escapeHtml(processor.name) + '\' does not support any relationships.'
                        });

                        // reset the dialog
                        resetDialog();

                        deferred.reject();
                    }
                }).fail(function () {
                    deferred.reject();
                });
            }).promise();
        } else {
            return $.Deferred(function (deferred) {
                // determine how to initialize the source
                var connectionSourceDeferred;
                if (nfCanvasUtils.isInputPort(source)) {
                    connectionSourceDeferred = initializeSourceInputPort(source);
                } else if (nfCanvasUtils.isRemoteProcessGroup(source)) {
                    connectionSourceDeferred = initializeSourceRemoteProcessGroup(source);
                } else if (nfCanvasUtils.isProcessGroup(source)) {
                    connectionSourceDeferred = initializeSourceProcessGroup(source);
                } else {
                    connectionSourceDeferred = initializeSourceFunnel(source);
                }

                // finish initialization when appropriate
                connectionSourceDeferred.done(function () {
                    // configure the button model
                    $('#connection-configuration').modal('setButtonModel', [{
                        buttonText: 'Add',
                        color: {
                            base: '#728E9B',
                            hover: '#004849',
                            text: '#ffffff'
                        },
                        handler: {
                            click: function () {
                                // add the connection
                                addConnection();

                                // close the dialog
                                $('#connection-configuration').modal('hide');
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
                                    $('#connection-configuration').modal('hide');
                                }
                            }
                        }]);

                    deferred.resolve();
                }).fail(function () {
                    deferred.reject();
                });
            }).promise();
        }
    };

    /**
     * Initializes the source when the source is an input port.
     *
     * @argument {selection} source        The source
     */
    var initializeSourceInputPort = function (source) {
        return $.Deferred(function (deferred) {
            // get the input port data
            var inputPortData = source.datum();
            var inputPortName = inputPortData.permissions.canRead ? inputPortData.component.name : inputPortData.id;

            // populate the port information
            $('#input-port-source').show();
            $('#input-port-source-name').text(inputPortName).attr('title', inputPortName);

            // populate the connection source details
            $('#connection-source-id').val(inputPortData.id);
            $('#connection-source-component-id').val(inputPortData.id);

            // populate the group details
            $('#connection-source-group-id').val(nfCanvasUtils.getGroupId());
            $('#connection-source-group-name').text(nfCanvasUtils.getGroupName());

            // resolve the deferred
            deferred.resolve();
        }).promise();
    };

    /**
     * Initializes the source when the source is an input port.
     *
     * @argument {selection} source        The source
     */
    var initializeSourceFunnel = function (source) {
        return $.Deferred(function (deferred) {
            // get the funnel data
            var funnelData = source.datum();

            // populate the port information
            $('#funnel-source').show();

            // populate the connection source details
            $('#connection-source-id').val(funnelData.id);
            $('#connection-source-component-id').val(funnelData.id);

            // populate the group details
            $('#connection-source-group-id').val(nfCanvasUtils.getGroupId());
            $('#connection-source-group-name').text(nfCanvasUtils.getGroupName());

            // resolve the deferred
            deferred.resolve();
        }).promise();
    };

    /**
     * Initializes the source when the source is a processor.
     *
     * @argument {selection} source        The source
     */
    var initializeSourceProcessor = function (source) {
        return $.Deferred(function (deferred) {
            // get the processor data
            var processorData = source.datum();
            var processorName = processorData.permissions.canRead ? processorData.component.name : processorData.id;
            var processorType = processorData.permissions.canRead ? nfCommon.substringAfterLast(processorData.component.type, '.') : 'Processor';

            // populate the source processor information
            $('#processor-source').show();
            $('#processor-source-name').text(processorName).attr('title', processorName);
            $('#processor-source-type').text(processorType).attr('title', processorType);

            // populate the connection source details
            $('#connection-source-id').val(processorData.id);
            $('#connection-source-component-id').val(processorData.id);

            // populate the group details
            $('#connection-source-group-id').val(nfCanvasUtils.getGroupId());
            $('#connection-source-group-name').text(nfCanvasUtils.getGroupName());

            // show the available relationships
            $('#relationship-names-container').show();

            deferred.resolve(processorData.component);
        });
    };

    /**
     * Initializes the source when the source is a process group.
     *
     * @argument {selection} source        The source
     */
    var initializeSourceProcessGroup = function (source) {
        return $.Deferred(function (deferred) {
            // get the process group data
            var processGroupData = source.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.api + '/flow/process-groups/' + encodeURIComponent(processGroupData.id),
                dataType: 'json'
            }).done(function (response) {
                var processGroup = response.processGroupFlow;
                var processGroupName = response.permissions.canRead ? processGroup.breadcrumb.breadcrumb.name : processGroup.id;
                var processGroupContents = processGroup.flow;

                // show the output port options
                var options = [];
                $.each(processGroupContents.outputPorts, function (i, outputPort) {
                    // require explicit access to the output port as it's the source of the connection
                    if (outputPort.permissions.canRead && outputPort.permissions.canWrite) {
                        var component = outputPort.component;
                        options.push({
                            text: component.name,
                            value: component.id,
                            description: nfCommon.escapeHtml(component.comments)
                        });
                    }
                });

                // only proceed if there are output ports
                if (!nfCommon.isEmpty(options)) {
                    $('#output-port-source').show();

                    // sort the options
                    options.sort(function (a, b) {
                        return a.text.localeCompare(b.text);
                    });

                    // create the combo
                    $('#output-port-options').combo({
                        options: options,
                        maxHeight: 300,
                        select: function (option) {
                            $('#connection-source-id').val(option.value);
                        }
                    });

                    // populate the connection details
                    $('#connection-source-component-id').val(processGroup.id);

                    // populate the group details
                    $('#connection-source-group-id').val(processGroup.id);
                    $('#connection-source-group-name').text(processGroupName);

                    deferred.resolve();
                } else {
                    var message = '\'' + nfCommon.escapeHtml(processGroupName) + '\' does not have any output ports.';
                    if (nfCommon.isEmpty(processGroupContents.outputPorts) === false) {
                        message = 'Not authorized for any output ports in \'' + nfCommon.escapeHtml(processGroupName) + '\'.';
                    }

                    // there are no output ports for this process group
                    nfDialog.showOkDialog({
                        headerText: 'Connection Configuration',
                        dialogContent: message
                    });

                    // reset the dialog
                    resetDialog();

                    deferred.reject();
                }
            }).fail(function (xhr, status, error) {
                // handle the error
                nfErrorHandler.handleAjaxError(xhr, status, error);

                deferred.reject();
            });
        }).promise();
    };

    /**
     * Initializes the source when the source is a remote process group.
     *
     * @argument {selection} source        The source
     */
    var initializeSourceRemoteProcessGroup = function (source) {
        return $.Deferred(function (deferred) {
            // get the remote process group data
            var remoteProcessGroupData = source.datum();

            $.ajax({
                type: 'GET',
                url: remoteProcessGroupData.uri,
                dataType: 'json'
            }).done(function (response) {
                var remoteProcessGroup = response.component;
                var remoteProcessGroupContents = remoteProcessGroup.contents;

                // only proceed if there are output ports
                if (!nfCommon.isEmpty(remoteProcessGroupContents.outputPorts)) {
                    $('#output-port-source').show();

                    // show the output port options
                    var options = [];
                    $.each(remoteProcessGroupContents.outputPorts, function (i, outputPort) {
                        options.push({
                            text: outputPort.name,
                            value: outputPort.id,
                            disabled: outputPort.exists === false,
                            description: nfCommon.escapeHtml(outputPort.comments)
                        });
                    });

                    // sort the options
                    options.sort(function (a, b) {
                        return a.text.localeCompare(b.text);
                    });

                    // create the combo
                    $('#output-port-options').combo({
                        options: options,
                        maxHeight: 300,
                        select: function (option) {
                            $('#connection-source-id').val(option.value);
                        }
                    });

                    // populate the connection details
                    $('#connection-source-component-id').val(remoteProcessGroup.id);

                    // populate the group details
                    $('#connection-source-group-id').val(remoteProcessGroup.id);
                    $('#connection-source-group-name').text(remoteProcessGroup.name);

                    deferred.resolve();
                } else {
                    // there are no relationships for this processor
                    nfDialog.showOkDialog({
                        headerText: 'Connection Configuration',
                        dialogContent: '\'' + nfCommon.escapeHtml(remoteProcessGroup.name) + '\' does not have any output ports.'
                    });

                    // reset the dialog
                    resetDialog();

                    deferred.reject();
                }
            }).fail(function (xhr, status, error) {
                // handle the error
                nfErrorHandler.handleAjaxError(xhr, status, error);

                deferred.reject();
            });
        }).promise();
    };

    var initializeDestinationNewConnectionDialog = function (destination) {
        if (nfCanvasUtils.isOutputPort(destination)) {
            return initializeDestinationOutputPort(destination);
        } else if (nfCanvasUtils.isProcessor(destination)) {
            return initializeDestinationProcessor(destination);
        } else if (nfCanvasUtils.isRemoteProcessGroup(destination)) {
            return initializeDestinationRemoteProcessGroup(destination);
        } else if (nfCanvasUtils.isFunnel(destination)) {
            return initializeDestinationFunnel(destination);
        } else {
            return initializeDestinationProcessGroup(destination);
        }
    };

    var initializeDestinationOutputPort = function (destination) {
        return $.Deferred(function (deferred) {
            var outputPortData = destination.datum();
            var outputPortName = outputPortData.permissions.canRead ? outputPortData.component.name : outputPortData.id;

            $('#output-port-destination').show();
            $('#output-port-destination-name').text(outputPortName).attr('title', outputPortName);

            // populate the connection destination details
            $('#connection-destination-id').val(outputPortData.id);
            $('#connection-destination-component-id').val(outputPortData.id);

            // populate the group details
            $('#connection-destination-group-id').val(nfCanvasUtils.getGroupId());
            $('#connection-destination-group-name').text(nfCanvasUtils.getGroupName());

            deferred.resolve();
        }).promise();
    };

    var initializeDestinationFunnel = function (destination) {
        return $.Deferred(function (deferred) {
            var funnelData = destination.datum();

            $('#funnel-destination').show();

            // populate the connection destination details
            $('#connection-destination-id').val(funnelData.id);
            $('#connection-destination-component-id').val(funnelData.id);

            // populate the group details
            $('#connection-destination-group-id').val(nfCanvasUtils.getGroupId());
            $('#connection-destination-group-name').text(nfCanvasUtils.getGroupName());

            deferred.resolve();
        }).promise();
    };

    var initializeDestinationProcessor = function (destination) {
        return $.Deferred(function (deferred) {
            var processorData = destination.datum();
            var processorName = processorData.permissions.canRead ? processorData.component.name : processorData.id;
            var processorType = processorData.permissions.canRead ? nfCommon.substringAfterLast(processorData.component.type, '.') : 'Processor';

            $('#processor-destination').show();
            $('#processor-destination-name').text(processorName).attr('title', processorName);
            $('#processor-destination-type').text(processorType).attr('title', processorType);

            // populate the connection destination details
            $('#connection-destination-id').val(processorData.id);
            $('#connection-destination-component-id').val(processorData.id);

            // populate the group details
            $('#connection-destination-group-id').val(nfCanvasUtils.getGroupId());
            $('#connection-destination-group-name').text(nfCanvasUtils.getGroupName());

            deferred.resolve();
        }).promise();
    };

    /**
     * Initializes the destination when the destination is a process group.
     *
     * @argument {selection} destination        The destination
     */
    var initializeDestinationProcessGroup = function (destination) {
        return $.Deferred(function (deferred) {
            var processGroupData = destination.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.api + '/flow/process-groups/' + encodeURIComponent(processGroupData.id),
                dataType: 'json'
            }).done(function (response) {
                var processGroup = response.processGroupFlow;
                var processGroupName = response.permissions.canRead ? processGroup.breadcrumb.breadcrumb.name : processGroup.id;
                var processGroupContents = processGroup.flow;

                // show the input port options
                var options = [];
                $.each(processGroupContents.inputPorts, function (i, inputPort) {
                    options.push({
                        text: inputPort.permissions.canRead ? inputPort.component.name : inputPort.id,
                        value: inputPort.id,
                        description: inputPort.permissions.canRead ? nfCommon.escapeHtml(inputPort.component.comments) : null
                    });
                });

                // only proceed if there are output ports
                if (!nfCommon.isEmpty(options)) {
                    $('#input-port-destination').show();

                    // sort the options
                    options.sort(function (a, b) {
                        return a.text.localeCompare(b.text);
                    });

                    // create the combo
                    $('#input-port-options').combo({
                        options: options,
                        maxHeight: 300,
                        select: function (option) {
                            $('#connection-destination-id').val(option.value);
                        }
                    });

                    // populate the connection details
                    $('#connection-destination-component-id').val(processGroup.id);

                    // populate the group details
                    $('#connection-destination-group-id').val(processGroup.id);
                    $('#connection-destination-group-name').text(processGroupName);

                    deferred.resolve();
                } else {
                    // there are no relationships for this processor
                    nfDialog.showOkDialog({
                        headerText: 'Connection Configuration',
                        dialogContent: '\'' + nfCommon.escapeHtml(processGroupName) + '\' does not have any input ports.'
                    });

                    // reset the dialog
                    resetDialog();

                    deferred.reject();
                }
            }).fail(function (xhr, status, error) {
                // handle the error
                nfErrorHandler.handleAjaxError(xhr, status, error);

                deferred.reject();
            });
        }).promise();
    };

    /**
     * Initializes the source when the source is a remote process group.
     *
     * @argument {selection} destination        The destination
     * @argument {object} connectionDestination The connection destination object
     */
    var initializeDestinationRemoteProcessGroup = function (destination, connectionDestination) {
        return $.Deferred(function (deferred) {
            var remoteProcessGroupData = destination.datum();

            $.ajax({
                type: 'GET',
                url: remoteProcessGroupData.uri,
                dataType: 'json'
            }).done(function (response) {
                var remoteProcessGroup = response.component;
                var remoteProcessGroupContents = remoteProcessGroup.contents;

                // only proceed if there are output ports
                if (!nfCommon.isEmpty(remoteProcessGroupContents.inputPorts)) {
                    $('#input-port-destination').show();

                    // show the input port options
                    var options = [];
                    $.each(remoteProcessGroupContents.inputPorts, function (i, inputPort) {
                        options.push({
                            text: inputPort.name,
                            value: inputPort.id,
                            disabled: inputPort.exists === false,
                            description: nfCommon.escapeHtml(inputPort.comments)
                        });
                    });

                    // sort the options
                    options.sort(function (a, b) {
                        return a.text.localeCompare(b.text);
                    });

                    // create the combo
                    $('#input-port-options').combo({
                        options: options,
                        maxHeight: 300,
                        select: function (option) {
                            $('#connection-destination-id').val(option.value);
                        }
                    });

                    // populate the connection details
                    $('#connection-destination-component-id').val(remoteProcessGroup.id);

                    // populate the group details
                    $('#connection-destination-group-id').val(remoteProcessGroup.id);
                    $('#connection-destination-group-name').text(remoteProcessGroup.name);

                    deferred.resolve();
                } else {
                    // there are no relationships for this processor
                    nfDialog.showOkDialog({
                        headerText: 'Connection Configuration',
                        dialogContent: '\'' + nfCommon.escapeHtml(remoteProcessGroup.name) + '\' does not have any input ports.'
                    });

                    // reset the dialog
                    resetDialog();

                    deferred.reject();
                }
            }).fail(function (xhr, status, error) {
                // handle the error
                nfErrorHandler.handleAjaxError(xhr, status, error);

                deferred.reject();
            });
        }).promise();
    };

    /**
     * Initializes the source panel for groups.
     *
     * @argument {selection} source    The source of the connection
     */
    var initializeSourceReadOnlyGroup = function (source) {
        return $.Deferred(function (deferred) {
            var sourceData = source.datum();
            var sourceName = sourceData.permissions.canRead ? sourceData.component.name : sourceData.id;

            // populate the port information
            $('#read-only-output-port-source').show();

            // populate the component information
            $('#connection-source-component-id').val(sourceData.id);

            // populate the group details
            $('#connection-source-group-id').val(sourceData.id);
            $('#connection-source-group-name').text(sourceName);

            // resolve the deferred
            deferred.resolve();
        }).promise();
    };

    /**
     * Initializes the source in the existing connection dialog.
     *
     * @argument {selection} source        The source
     */
    var initializeSourceEditConnectionDialog = function (source) {
        if (nfCanvasUtils.isProcessor(source)) {
            return initializeSourceProcessor(source);
        } else if (nfCanvasUtils.isInputPort(source)) {
            return initializeSourceInputPort(source);
        } else if (nfCanvasUtils.isFunnel(source)) {
            return initializeSourceFunnel(source);
        } else {
            return initializeSourceReadOnlyGroup(source);
        }
    };

    /**
     * Initializes the destination in the existing connection dialog.
     *
     * @argument {selection} destination        The destination
     * @argument {object} connectionDestination The connection destination object
     */
    var initializeDestinationEditConnectionDialog = function (destination, connectionDestination) {
        if (nfCanvasUtils.isProcessor(destination)) {
            return initializeDestinationProcessor(destination);
        } else if (nfCanvasUtils.isOutputPort(destination)) {
            return initializeDestinationOutputPort(destination);
        } else if (nfCanvasUtils.isRemoteProcessGroup(destination)) {
            return initializeDestinationRemoteProcessGroup(destination, connectionDestination);
        } else if (nfCanvasUtils.isFunnel(destination)) {
            return initializeDestinationFunnel(destination);
        } else {
            return initializeDestinationProcessGroup(destination);
        }
    };

    /**
     * Creates an option for the specified relationship name.
     *
     * @argument {string} name      The relationship name
     */
    var createRelationshipOption = function (name) {
        var relationshipLabel = $('<div class="relationship-name nf-checkbox-label ellipsis"></div>').text(name);
        var relationshipValue = $('<span class="relationship-name-value hidden"></span>').text(name);
        return $('<div class="available-relationship-container"><div class="available-relationship nf-checkbox checkbox-unchecked"></div>' +
            '</div>').append(relationshipLabel).append(relationshipValue).appendTo('#relationship-names');
    };

    /**
     * Adds a new connection.
     *
     * @argument {array} selectedRelationships      The selected relationships
     */
    var addConnection = function (selectedRelationships) {
        // get the connection details
        var sourceId = $('#connection-source-id').val();
        var destinationId = $('#connection-destination-id').val();

        // get the selection components
        var sourceComponentId = $('#connection-source-component-id').val();
        var source = d3.select('#id-' + sourceComponentId);
        var destinationComponentId = $('#connection-destination-component-id').val();
        var destination = d3.select('#id-' + destinationComponentId);

        // get the source/destination data
        var sourceData = source.datum();
        var destinationData = destination.datum();

        // add bend points if we're dealing with a self loop
        var bends = [];
        if (sourceComponentId === destinationComponentId) {
            var rightCenter = {
                x: sourceData.position.x + (sourceData.dimensions.width),
                y: sourceData.position.y + (sourceData.dimensions.height / 2)
            };

            var xOffset = nfConnection.config.selfLoopXOffset;
            var yOffset = nfConnection.config.selfLoopYOffset;
            bends.push({
                'x': (rightCenter.x + xOffset),
                'y': (rightCenter.y - yOffset)
            });
            bends.push({
                'x': (rightCenter.x + xOffset),
                'y': (rightCenter.y + yOffset)
            });
        } else {
            var existingConnections = [];

            // get all connections for the source component
            var connectionsForSourceComponent = nfConnection.getComponentConnections(sourceComponentId);
            $.each(connectionsForSourceComponent, function (_, connectionForSourceComponent) {
                // get the id for the source/destination component
                var connectionSourceComponentId = nfCanvasUtils.getConnectionSourceComponentId(connectionForSourceComponent);
                var connectionDestinationComponentId = nfCanvasUtils.getConnectionDestinationComponentId(connectionForSourceComponent);

                // if the connection is between these same components, consider it for collisions
                if ((connectionSourceComponentId === sourceComponentId && connectionDestinationComponentId === destinationComponentId) ||
                    (connectionDestinationComponentId === sourceComponentId && connectionSourceComponentId === destinationComponentId)) {

                    // record all connections between these two components in question
                    existingConnections.push(connectionForSourceComponent);
                }
            });

            // if there are existing connections between these components, ensure the new connection won't collide
            if (existingConnections.length > 0) {
                var avoidCollision = false;
                $.each(existingConnections, function (_, existingConnection) {
                    // only consider multiple connections with no bend points a collision, the existance of 
                    // bend points suggests that the user has placed the connection into a desired location
                    if (nfCommon.isEmpty(existingConnection.bends)) {
                        avoidCollision = true;
                        return false;
                    }
                });

                // if we need to avoid a collision
                if (avoidCollision === true) {
                    // determine the middle of the source/destination components
                    var sourceMiddle = [sourceData.position.x + (sourceData.dimensions.width / 2), sourceData.position.y + (sourceData.dimensions.height / 2)];
                    var destinationMiddle = [destinationData.position.x + (destinationData.dimensions.width / 2), destinationData.position.y + (destinationData.dimensions.height / 2)];

                    // detect if the line is more horizontal or vertical
                    var slope = ((sourceMiddle[1] - destinationMiddle[1]) / (sourceMiddle[0] - destinationMiddle[0]));
                    var isMoreHorizontal = slope <= 1 && slope >= -1;

                    // determines if the specified coordinate collides with another connection
                    var collides = function (x, y) {
                        var collides = false;
                        $.each(existingConnections, function (_, existingConnection) {
                            if (!nfCommon.isEmpty(existingConnection.bends)) {
                                if (isMoreHorizontal) {
                                    // horizontal lines are adjusted in the y space
                                    if (existingConnection.bends[0].y === y) {
                                        collides = true;
                                        return false;
                                    }
                                } else {
                                    // vertical lines are adjusted in the x space
                                    if (existingConnection.bends[0].x === x) {
                                        collides = true;
                                        return false;
                                    }
                                }
                            }
                        });
                        return collides;
                    };

                    // find the mid point on the connection
                    var xCandidate = (sourceMiddle[0] + destinationMiddle[0]) / 2;
                    var yCandidate = (sourceMiddle[1] + destinationMiddle[1]) / 2;

                    // attempt to position this connection so it doesn't collide
                    var xStep = isMoreHorizontal ? 0 : CONNECTION_OFFSET_X_INCREMENT;
                    var yStep = isMoreHorizontal ? CONNECTION_OFFSET_Y_INCREMENT : 0;
                    var positioned = false;
                    while (positioned === false) {
                        // consider above and below, then increment and try again (if necessary)
                        if (collides(xCandidate - xStep, yCandidate - yStep) === false) {
                            bends.push({
                                'x': (xCandidate - xStep),
                                'y': (yCandidate - yStep)
                            });
                            positioned = true;
                        } else if (collides(xCandidate + xStep, yCandidate + yStep) === false) {
                            bends.push({
                                'x': (xCandidate + xStep),
                                'y': (yCandidate + yStep)
                            });
                            positioned = true;
                        }

                        if (isMoreHorizontal) {
                            yStep += CONNECTION_OFFSET_Y_INCREMENT;
                        } else {
                            xStep += CONNECTION_OFFSET_X_INCREMENT;
                        }
                    }
                }
            }
        }

        // determine the source group id
        var sourceGroupId = $('#connection-source-group-id').val();
        var destinationGroupId = $('#connection-destination-group-id').val();

        // determine the source and destination types
        var sourceType = nfCanvasUtils.getConnectableTypeForSource(source);
        var destinationType = nfCanvasUtils.getConnectableTypeForDestination(destination);

        // get the settings
        var connectionName = $('#connection-name').val();
        var flowFileExpiration = $('#flow-file-expiration').val();
        var backPressureObjectThreshold = $('#back-pressure-object-threshold').val();
        var backPressureDataSizeThreshold = $('#back-pressure-data-size-threshold').val();
        var prioritizers = $('#prioritizer-selected').sortable('toArray');

        if (validateSettings()) {
            var connectionEntity = {
                'revision': nfClient.getRevision({
                    'revision': {
                        'version': 0
                    }
                }),
                'component': {
                    'name': connectionName,
                    'source': {
                        'id': sourceId,
                        'groupId': sourceGroupId,
                        'type': sourceType
                    },
                    'destination': {
                        'id': destinationId,
                        'groupId': destinationGroupId,
                        'type': destinationType
                    },
                    'selectedRelationships': selectedRelationships,
                    'flowFileExpiration': flowFileExpiration,
                    'backPressureDataSizeThreshold': backPressureDataSizeThreshold,
                    'backPressureObjectThreshold': backPressureObjectThreshold,
                    'bends': bends,
                    'prioritizers': prioritizers
                }
            };

            // create the new connection
            $.ajax({
                type: 'POST',
                url: config.urls.api + '/process-groups/' + encodeURIComponent(nfCanvasUtils.getGroupId()) + '/connections',
                data: JSON.stringify(connectionEntity),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // add the connection
                nfGraph.add({
                    'connections': [response]
                }, {
                    'selectAll': true
                });

                // reload the connections source/destination components
                nfCanvasUtils.reloadConnectionSourceAndDestination(sourceComponentId, destinationComponentId);

                // update component visibility
                nfGraph.updateVisibility();

                // update the birdseye
                nfBirdseye.refresh();
            }).fail(function (xhr, status, error) {
                // handle the error
                nfErrorHandler.handleAjaxError(xhr, status, error);
            });
        }
    };

    /**
     * Updates an existing connection.
     *
     * @argument {array} selectedRelationships          The selected relationships
     */
    var updateConnection = function (selectedRelationships) {
        // get the connection details
        var connectionId = $('#connection-id').text();
        var connectionUri = $('#connection-uri').val();

        // get the source details
        var sourceComponentId = $('#connection-source-component-id').val();

        // get the destination details
        var destinationComponentId = $('#connection-destination-component-id').val();
        var destination = d3.select('#id-' + destinationComponentId);
        var destinationType = nfCanvasUtils.getConnectableTypeForDestination(destination);

        // get the destination details
        var destinationId = $('#connection-destination-id').val();
        var destinationGroupId = $('#connection-destination-group-id').val();

        // get the settings
        var connectionName = $('#connection-name').val();
        var flowFileExpiration = $('#flow-file-expiration').val();
        var backPressureObjectThreshold = $('#back-pressure-object-threshold').val();
        var backPressureDataSizeThreshold = $('#back-pressure-data-size-threshold').val();
        var prioritizers = $('#prioritizer-selected').sortable('toArray');

        if (validateSettings()) {
            var d = nfConnection.get(connectionId);
            var connectionEntity = {
                'revision': nfClient.getRevision(d),
                'component': {
                    'id': connectionId,
                    'name': connectionName,
                    'destination': {
                        'id': destinationId,
                        'groupId': destinationGroupId,
                        'type': destinationType
                    },
                    'selectedRelationships': selectedRelationships,
                    'flowFileExpiration': flowFileExpiration,
                    'backPressureDataSizeThreshold': backPressureDataSizeThreshold,
                    'backPressureObjectThreshold': backPressureObjectThreshold,
                    'prioritizers': prioritizers
                }
            };

            // update the connection
            return $.ajax({
                type: 'PUT',
                url: connectionUri,
                data: JSON.stringify(connectionEntity),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // update this connection
                nfConnection.set(response);

                // reload the connections source/destination components
                nfCanvasUtils.reloadConnectionSourceAndDestination(sourceComponentId, destinationComponentId);
            }).fail(function (xhr, status, error) {
                if (xhr.status === 400 || xhr.status === 404 || xhr.status === 409) {
                    nfDialog.showOkDialog({
                        headerText: 'Connection Configuration',
                        dialogContent: nfCommon.escapeHtml(xhr.responseText),
                    });
                } else {
                    nfErrorHandler.handleAjaxError(xhr, status, error);
                }
            });
        } else {
            return $.Deferred(function (deferred) {
                deferred.reject();
            }).promise();
        }
    };

    /**
     * Returns an array of selected relationship names.
     */
    var getSelectedRelationships = function () {
        // get all available relationships
        var availableRelationships = $('#relationship-names');
        var selectedRelationships = [];

        // go through each relationship to determine which are selected
        $.each(availableRelationships.children(), function (i, relationshipElement) {
            var relationship = $(relationshipElement);

            // get each relationship and its corresponding checkbox
            var relationshipCheck = relationship.children('div.available-relationship');

            // see if this relationship has been selected
            if (relationshipCheck.hasClass('checkbox-checked')) {
                selectedRelationships.push(relationship.children('span.relationship-name-value').text());
            }
        });

        return selectedRelationships;
    };

    /**
     * Validates the specified settings.
     */
    var validateSettings = function () {
        var errors = [];

        // validate the settings
        if (nfCommon.isBlank($('#flow-file-expiration').val())) {
            errors.push('File expiration must be specified');
        }
        if (!$.isNumeric($('#back-pressure-object-threshold').val())) {
            errors.push('Back pressure object threshold must be an integer value');
        }
        if (nfCommon.isBlank($('#back-pressure-data-size-threshold').val())) {
            errors.push('Back pressure data size threshold must be specified');
        }

        if (errors.length > 0) {
            nfDialog.showOkDialog({
                headerText: 'Connection Configuration',
                dialogContent: nfCommon.formatUnorderedList(errors)
            });
            return false;
        } else {
            return true;
        }
    };

    /**
     * Resets the dialog.
     */
    var resetDialog = function () {
        // reset the prioritizers
        var selectedList = $('#prioritizer-selected');
        var availableList = $('#prioritizer-available');
        selectedList.children().detach().appendTo(availableList);

        // sort the available list
        var listItems = availableList.children('li').get();
        listItems.sort(function (a, b) {
            var compA = $(a).text().toUpperCase();
            var compB = $(b).text().toUpperCase();
            return (compA < compB) ? -1 : (compA > compB) ? 1 : 0;
        });

        // clear the available list and re-insert each list item
        $.each(listItems, function () {
            $(this).detach();
        });
        $.each(listItems, function () {
            $(this).appendTo(availableList);
        });

        // reset the fields
        $('#connection-name').val('');
        $('#relationship-names').css('border-width', '0').empty();
        $('#relationship-names-container').hide();

        // clear the id field
        nfCommon.clearField('connection-id');

        // hide all the connection source panels
        $('#processor-source').hide();
        $('#input-port-source').hide();
        $('#output-port-source').hide();
        $('#read-only-output-port-source').hide();
        $('#funnel-source').hide();

        // hide all the connection destination panels
        $('#processor-destination').hide();
        $('#input-port-destination').hide();
        $('#output-port-destination').hide();
        $('#funnel-destination').hide();

        // clear and destination details
        $('#connection-source-id').val('');
        $('#connection-source-component-id').val('');
        $('#connection-source-group-id').val('');

        // clear any destination details
        $('#connection-destination-id').val('');
        $('#connection-destination-component-id').val('');
        $('#connection-destination-group-id').val('');

        // clear any ports
        $('#output-port-options').empty();
        $('#input-port-options').empty();

        // see if the temp edge needs to be removed
        removeTempEdge();
    };

    var nfConnectionConfiguration = {

        /**
         * Initialize the connection configuration.
         *
         * @param nfBirdseyeRef   The nfBirdseye module.
         * @param nfGraphRef   The nfGraph module.
         */
        init: function (nfBirdseyeRef, nfGraphRef) {
            nfBirdseye = nfBirdseyeRef;
            nfGraph = nfGraphRef;

            // initially hide the relationship names container
            $('#relationship-names-container').hide();

            // initialize the configure connection dialog
            $('#connection-configuration').modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'Configure Connection',
                handler: {
                    close: function () {
                        // reset the dialog on close
                        resetDialog();
                    },
                    open: function () {
                        nfCommon.toggleScrollable($('#' + this.find('.tab-container').attr('id') + '-content').get(0));
                    }
                }
            });

            // initialize the properties tabs
            $('#connection-configuration-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                scrollableTabContentStyle: 'scrollable',
                tabs: [{
                    name: 'Details',
                    tabContentId: 'connection-details-tab-content'
                }, {
                    name: 'Settings',
                    tabContentId: 'connection-settings-tab-content'
                }]
            });

            // load the processor prioritizers
            $.ajax({
                type: 'GET',
                url: config.urls.prioritizers,
                dataType: 'json'
            }).done(function (response) {
                // create an element for each available prioritizer
                $.each(response.prioritizerTypes, function (i, documentedType) {
                    nfConnectionConfiguration.addAvailablePrioritizer('#prioritizer-available', documentedType);
                });

                // make the prioritizer containers sortable
                $('#prioritizer-available, #prioritizer-selected').sortable({
                    containment: $('#connection-settings-tab-content').find('.settings-right'),
                    connectWith: 'ul',
                    placeholder: 'ui-state-highlight',
                    scroll: true,
                    opacity: 0.6
                });
                $('#prioritizer-available, #prioritizer-selected').disableSelection();
            }).fail(nfErrorHandler.handleAjaxError);
        },

        /**
         * Adds the specified prioritizer to the specified container.
         *
         * @argument {string} prioritizerContainer      The dom Id of the prioritizer container
         * @argument {object} prioritizerType           The type of prioritizer
         */
        addAvailablePrioritizer: function (prioritizerContainer, prioritizerType) {
            var type = prioritizerType.type;
            var name = nfCommon.substringAfterLast(type, '.');

            // add the prioritizers to the available list
            var prioritizerList = $(prioritizerContainer);
            var prioritizer = $('<li></li>').append($('<span style="float: left;"></span>').text(name)).attr('id', type).addClass('ui-state-default').appendTo(prioritizerList);

            // add the description if applicable
            if (nfCommon.isDefinedAndNotNull(prioritizerType.description)) {
                $('<div class="fa fa-question-circle" style="float: right; margin-right: 5px;""></div>').appendTo(prioritizer).qtip($.extend({
                    content: nfCommon.escapeHtml(prioritizerType.description)
                }, nfCommon.config.tooltipConfig));
            }
        },

        /**
         * Shows the dialog for creating a new connection.
         *
         * @argument {string} sourceId      The source id
         * @argument {string} destinationId The destination id
         */
        createConnection: function (sourceId, destinationId) {
            // select the source and destination
            var source = d3.select('#id-' + sourceId);
            var destination = d3.select('#id-' + destinationId);

            if (source.empty() || destination.empty()) {
                return;
            }

            // initialize the connection dialog
            $.when(initializeSourceNewConnectionDialog(source), initializeDestinationNewConnectionDialog(destination)).done(function () {
                // set the default values
                $('#flow-file-expiration').val('0 sec');
                $('#back-pressure-object-threshold').val('10000');
                $('#back-pressure-data-size-threshold').val('1 GB');

                // select the first tab
                $('#connection-configuration-tabs').find('li:first').click();

                // configure the header and show the dialog
                $('#connection-configuration').modal('setHeaderText', 'Create Connection').modal('show');

                // add the ellipsis if necessary
                $('#connection-configuration div.relationship-name').ellipsis();

                // fill in the connection id
                nfCommon.populateField('connection-id', null);

                // show the border if necessary
                var relationshipNames = $('#relationship-names');
                if (relationshipNames.is(':visible') && relationshipNames.get(0).scrollHeight > Math.round(relationshipNames.innerHeight())) {
                    relationshipNames.css('border-width', '1px');
                }
            }).fail(function () {
                // see if the temp edge needs to be removed
                removeTempEdge();
            });
        },

        /**
         * Shows the configuration for the specified connection. If a destination is
         * specified it will be considered a new destination.
         *
         * @argument {selection} selection         The connection entry
         * @argument {selection} destination          Optional new destination
         */
        showConfiguration: function (selection, destination) {
            return $.Deferred(function (deferred) {
                var connectionEntry = selection.datum();
                var connection = connectionEntry.component;

                // identify the source component
                var sourceComponentId = nfCanvasUtils.getConnectionSourceComponentId(connectionEntry);
                var source = d3.select('#id-' + sourceComponentId);

                // identify the destination component
                if (nfCommon.isUndefinedOrNull(destination)) {
                    var destinationComponentId = nfCanvasUtils.getConnectionDestinationComponentId(connectionEntry);
                    destination = d3.select('#id-' + destinationComponentId);
                }

                // initialize the connection dialog
                $.when(initializeSourceEditConnectionDialog(source), initializeDestinationEditConnectionDialog(destination, connection.destination)).done(function () {
                    var availableRelationships = connection.availableRelationships;
                    var selectedRelationships = connection.selectedRelationships;

                    // show the available relationship if applicable
                    if (nfCommon.isDefinedAndNotNull(availableRelationships) || nfCommon.isDefinedAndNotNull(selectedRelationships)) {
                        // populate the available connections
                        $.each(availableRelationships, function (i, name) {
                            createRelationshipOption(name);
                        });

                        addDialogRelationshipsChangeListener();

                        // ensure all selected relationships are present
                        // (may be undefined) and selected
                        $.each(selectedRelationships, function (i, name) {
                            // mark undefined relationships accordingly
                            if ($.inArray(name, availableRelationships) === -1) {
                                var option = createRelationshipOption(name);
                                $(option).children('div.relationship-name').addClass('undefined');
                            }

                            // ensure all selected relationships are checked
                            var relationships = $('#relationship-names').children('div');
                            $.each(relationships, function (i, relationship) {
                                var relationshipName = $(relationship).children('span.relationship-name-value');
                                if (relationshipName.text() === name) {
                                    $(relationship).children('div.available-relationship').removeClass('checkbox-unchecked').addClass('checkbox-checked');
                                }
                            });
                        });
                    }

                    // if the source is a process group or remote process group, select the appropriate port if applicable
                    if (nfCanvasUtils.isProcessGroup(source) || nfCanvasUtils.isRemoteProcessGroup(source)) {
                        // populate the connection source details
                        $('#connection-source-id').val(connection.source.id);
                        $('#read-only-output-port-name').text(connection.source.name).attr('title', connection.source.name);
                    }

                    // if the destination is a process gorup or remote process group, select the appropriate port if applicable
                    if (nfCanvasUtils.isProcessGroup(destination) || nfCanvasUtils.isRemoteProcessGroup(destination)) {
                        var destinationData = destination.datum();

                        // when the group ids differ, its a new destination component so we don't want to preselect any port
                        if (connection.destination.groupId === destinationData.id) {
                            $('#input-port-options').combo('setSelectedOption', {
                                value: connection.destination.id
                            });
                        }
                    }

                    // set the connection settings
                    $('#connection-name').val(connection.name);
                    $('#flow-file-expiration').val(connection.flowFileExpiration);
                    $('#back-pressure-object-threshold').val(connection.backPressureObjectThreshold);
                    $('#back-pressure-data-size-threshold').val(connection.backPressureDataSizeThreshold);

                    // format the connection id
                    nfCommon.populateField('connection-id', connection.id);

                    // handle each prioritizer
                    $.each(connection.prioritizers, function (i, type) {
                        $('#prioritizer-available').children('li[id="' + type + '"]').detach().appendTo('#prioritizer-selected');
                    });

                    // store the connection details
                    $('#connection-uri').val(connectionEntry.uri);

                    // configure the button model
                    $('#connection-configuration').modal('setButtonModel', [{
                        buttonText: 'Apply',
                        color: {
                            base: '#728E9B',
                            hover: '#004849',
                            text: '#ffffff'
                        },
                        disabled: function () {
                            // ensure some relationships were selected with a processor as the source
                            if (nfCanvasUtils.isProcessor(source)) {
                                return getSelectedRelationships().length === 0;
                            }
                            return false;
                        },
                        handler: {
                            click: function () {
                                // see if we're working with a processor as the source
                                if (nfCanvasUtils.isProcessor(source)) {
                                    // update the selected relationships
                                    updateConnection(getSelectedRelationships()).done(function () {
                                        deferred.resolve();
                                    }).fail(function () {
                                        deferred.reject();
                                    });
                                } else {
                                    // there are no relationships, but the source wasn't a processor, so update anyway
                                    updateConnection(undefined).done(function () {
                                        deferred.resolve();
                                    }).fail(function () {
                                        deferred.reject();
                                    });
                                }

                                // close the dialog
                                $('#connection-configuration').modal('hide');
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
                                    // hide the dialog
                                    $('#connection-configuration').modal('hide');

                                    // reject the deferred
                                    deferred.reject();
                                }
                            }
                        }]);

                    // show the details dialog
                    $('#connection-configuration').modal('setHeaderText', 'Configure Connection').modal('show');

                    // add the ellipsis if necessary
                    $('#connection-configuration div.relationship-name').ellipsis();

                    // show the border if necessary
                    var relationshipNames = $('#relationship-names');
                    if (relationshipNames.is(':visible') && relationshipNames.get(0).scrollHeight > Math.round(relationshipNames.innerHeight())) {
                        relationshipNames.css('border-width', '1px');
                    }
                }).fail(function () {
                    deferred.reject();
                });
            }).promise();
        }
    };

    return nfConnectionConfiguration;
}));