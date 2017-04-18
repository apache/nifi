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
                'nf.ng.Bridge',
                'nf.RemoteProcessGroup'],
            function ($, d3, nfErrorHandler, nfCommon, nfDialog, nfClient, nfCanvasUtils, nfNgBridge, nfRemoteProcessGroup) {
                return (nf.RemoteProcessGroupPorts = factory($, d3, nfErrorHandler, nfCommon, nfDialog, nfClient, nfCanvasUtils, nfNgBridge, nfRemoteProcessGroup));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.RemoteProcessGroupPorts =
            factory(require('jquery'),
                require('d3'),
                require('nf.ErrorHandler'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.Client'),
                require('nf.CanvasUtils'),
                require('nf.ng.Bridge'),
                require('nf.RemoteProcessGroup')));
    } else {
        nf.RemoteProcessGroupPorts = factory(root.$,
            root.d3,
            root.nf.ErrorHandler,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.Client,
            root.nf.CanvasUtils,
            root.nf.ng.Bridge,
            root.nf.RemoteProcessGroup);
    }
}(this, function ($, d3, nfErrorHandler, nfCommon, nfDialog, nfClient, nfCanvasUtils, nfNgBridge, nfRemoteProcessGroup) {
    'use strict';

    /**
     * Initializes the remote port configuration dialog.
     */
    var initRemotePortConfigurationDialog = function () {
        $('#remote-port-configuration').modal({
            headerText: 'Configure Remote Port',
            scrollableContentStyle: 'scrollable',
            buttons: [{
                buttonText: 'Apply',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        var remotePortConcurrentTasks = $('#remote-port-concurrent-tasks').val();

                        // ensure the property name and value is specified
                        if ($.isNumeric(remotePortConcurrentTasks)) {
                            var remoteProcessGroupId = $('#remote-process-group-ports-id').text();
                            var remoteProcessGroupData = d3.select('#id-' + remoteProcessGroupId).datum();
                            var remotePortId = $('#remote-port-id').text();

                            // create the remote process group details
                            var remoteProcessGroupPortEntity = {
                                'revision': nfClient.getRevision(remoteProcessGroupData),
                                'remoteProcessGroupPort': {
                                    id: remotePortId,
                                    groupId: remoteProcessGroupId,
                                    useCompression: $('#remote-port-use-compression').hasClass('checkbox-checked'),
                                    concurrentlySchedulableTaskCount: remotePortConcurrentTasks
                                }
                            };

                            // determine the type of port this is
                            var portContextPath = '/output-ports/';
                            if ($('#remote-port-type').text() === 'input') {
                                portContextPath = '/input-ports/';
                            }

                            // update the selected component
                            $.ajax({
                                type: 'PUT',
                                data: JSON.stringify(remoteProcessGroupPortEntity),
                                url: remoteProcessGroupData.uri + portContextPath + encodeURIComponent(remotePortId),
                                dataType: 'json',
                                contentType: 'application/json'
                            }).done(function (response) {
                                // Update the RemoteProcessGroup revision.
                                // RemotePorts share revision with parent RemoteProcessGroup.
                                remoteProcessGroupData.revision = response.revision;

                                // get the response
                                var remotePort = response.remoteProcessGroupPort;

                                // determine the compression label
                                var compressionLabel = 'No';
                                if (remotePort.useCompression === true) {
                                    compressionLabel = 'Yes';
                                }

                                // set the new values
                                $('#' + remotePortId + '-concurrent-tasks').text(remotePort.concurrentlySchedulableTaskCount);
                                $('#' + remotePortId + '-compression').text(compressionLabel);
                            }).fail(function (xhr, status, error) {
                                if (xhr.status === 400) {
                                    var errors = xhr.responseText.split('\n');

                                    var content;
                                    if (errors.length === 1) {
                                        content = $('<span></span>').text(errors[0]);
                                    } else {
                                        content = nfCommon.formatUnorderedList(errors);
                                    }

                                    nfDialog.showOkDialog({
                                        dialogContent: content,
                                        headerText: 'Remote Process Group Ports'
                                    });
                                } else {
                                    nfErrorHandler.handleAjaxError(xhr, status, error);
                                }
                            }).always(function () {
                                // close the dialog
                                $('#remote-port-configuration').modal('hide');
                            });
                        } else {
                            nfDialog.showOkDialog({
                                headerText: 'Remote Process Group Ports',
                                dialogContent: 'Concurrent tasks must be an integer value.'
                            });

                            // close the dialog
                            $('#remote-port-configuration').modal('hide');
                        }
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
                            $('#remote-port-configuration').modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // clear the name/value textfields
                    $('#remote-port-id').text('');
                    $('#remote-port-name').text('');
                    $('#remote-port-concurrent-tasks').val('');
                    $('#remote-port-use-compression').removeClass('checkbox-checked checkbox-unchecked');
                }
            }
        });
    };

    /**
     * Initializes the remote process group configuration dialog.
     */
    var initRemoteProcessGroupConfigurationDialog = function () {
        $('#remote-process-group-ports').modal({
            scrollableContentStyle: 'scrollable',
            headerText: 'Remote Process Group Ports',
            buttons: [{
                buttonText: 'Close',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        // get the component in question
                        var remoteProcessGroupId = $('#remote-process-group-ports-id').text();
                        var remoteProcessGroup = d3.select('#id-' + remoteProcessGroupId);
                        var remoteProcessGroupData = remoteProcessGroup.datum();

                        // if can modify, the over status of this node may have changed
                        if (nfCanvasUtils.canModify(remoteProcessGroup)) {
                            // reload the remote process group
                            nfRemoteProcessGroup.reload(remoteProcessGroupData.id);
                        }

                        // hide the dialog
                        $('#remote-process-group-ports').modal('hide');
                    }
                }
            }],
            handler: {
                close: function () {
                    // clear the remote process group details
                    $('#remote-process-group-ports-id').text('');
                    $('#remote-process-group-ports-name').text('');
                    $('#remote-process-group-ports-urls').text('');

                    // clear any tooltips
                    var dialog = $('#remote-process-group-ports');
                    nfCommon.cleanUpTooltips(dialog, 'div.remote-port-removed');
                    nfCommon.cleanUpTooltips(dialog, 'div.concurrent-tasks-info');

                    // clear the input and output ports
                    $('#remote-process-group-input-ports-container').empty();
                    $('#remote-process-group-output-ports-container').empty();
                }
            }
        });
    };

    /**
     * Creates the markup for configuration concurrent tasks for a port.
     *
     * @argument {jQuery} container         The container
     * @argument {object} port              The port
     * @argument {string} portType          The type of port
     */
    var createPortOption = function (container, port, portType) {
        var portId = nfCommon.escapeHtml(port.id);
        var portContainer = $('<div class="remote-port-container"></div>').appendTo(container);
        var portContainerEditContainer = $('<div class="remote-port-edit-container"></div>').appendTo(portContainer);
        var portContainerDetailsContainer = $('<div class="remote-port-details-container"></div>').appendTo(portContainer);

        // get the component in question
        var remoteProcessGroupId = $('#remote-process-group-ports-id').text();
        var remoteProcessGroup = d3.select('#id-' + remoteProcessGroupId);

        // if can modify, support updating the remote group port
        if (nfCanvasUtils.canModify(remoteProcessGroup)) {

            var createTransmissionSwitch = function (port) {
                var transmissionSwitch;
                if (port.connected === true) {
                    if (port.transmitting === true) {
                        transmissionSwitch = (nfNgBridge.injector.get('$compile')($('<md-switch style="margin:0px" class="md-primary enabled-active-transmission" aria-label="Toggle port transmission"></md-switch>'))(nfNgBridge.rootScope));
                        transmissionSwitch.click();
                    } else {
                        if (port.exists === true) {
                            transmissionSwitch = (nfNgBridge.injector.get('$compile')($('<md-switch style="margin:0px" class="md-primary enabled-inactive-transmission" aria-label="Toggle port transmission"></md-switch>'))(nfNgBridge.rootScope));
                        } else {
                            transmissionSwitch = (nfNgBridge.injector.get('$compile')($('<md-switch ng-disabled="true" style="margin:0px" class="md-primary disabled-inactive-transmission" aria-label="Toggle port transmission"></md-switch>'))(nfNgBridge.rootScope));
                        }
                    }
                } else {
                    if (port.transmitting === true) {
                        transmissionSwitch = (nfNgBridge.injector.get('$compile')($('<md-switch style="margin:0px" class="md-primary disabled-active-transmission" aria-label="Toggle port transmission"></md-switch>'))(nfNgBridge.rootScope));
                    } else {
                        transmissionSwitch = (nfNgBridge.injector.get('$compile')($('<md-switch ng-disabled="true" style="margin:0px" class="md-primary disabled-inactive-transmission" aria-label="Toggle port transmission"></md-switch>'))(nfNgBridge.rootScope));
                    }
                }

                return transmissionSwitch;
            };

            // show the enabled transmission switch
            var transmissionSwitch = createTransmissionSwitch(port);
            transmissionSwitch.appendTo(portContainerEditContainer);

            // only support configuration when the remote port exists
            if (port.exists === true && port.connected === true) {
                // create the button for editing the ports configuration
                var editRemotePort = $('<button class="button edit-remote-port fa fa-pencil"></button>').click(function () {
                    var portName = $('#' + portId + '-name').text();
                    var portConcurrentTasks = $('#' + portId + '-concurrent-tasks').text();
                    var portCompression = $('#' + portId + '-compression').text() === 'Yes';

                    // show the configuration dialog
                    configureRemotePort(port.id, portName, portConcurrentTasks, portCompression, portType);
                }).appendTo(portContainerEditContainer);

                // show/hide the edit button as appropriate
                if (port.transmitting === true) {
                    editRemotePort.hide();
                } else {
                    editRemotePort.show();
                }
            } else if (port.exists === false) {
                $('<div class="remote-port-removed"/>').appendTo(portContainerEditContainer).qtip($.extend({},
                    nfCommon.config.tooltipConfig,
                    {
                        content: 'This port has been removed.'
                    }));
            }

            // only allow modifications to transmission when the swtich is defined
            if (nfCommon.isDefinedAndNotNull(transmissionSwitch)) {
                var transmissionSwitchClickFunction = function () {
                    // get the component being edited
                    var remoteProcessGroupId = $('#remote-process-group-ports-id').text();
                    var remoteProcessGroupData = d3.select('#id-' + remoteProcessGroupId).datum();

                    // determine the new transmission status
                    var isTransmitting = false;
                    if (transmissionSwitch.hasClass('enabled-inactive-transmission')) {
                        isTransmitting = true;
                    }

                    // create the remote process group details
                    var remoteProcessGroupPortEntity = {
                        'revision': nfClient.getRevision(remoteProcessGroupData),
                        'remoteProcessGroupPort': {
                            id: port.id,
                            groupId: remoteProcessGroupId,
                            transmitting: isTransmitting
                        }
                    };

                    // determine the type of port this is
                    var portContextPath = '/output-ports/';
                    if (portType === 'input') {
                        portContextPath = '/input-ports/';
                    }

                    // update the selected component
                    $.ajax({
                        type: 'PUT',
                        data: JSON.stringify(remoteProcessGroupPortEntity),
                        url: remoteProcessGroupData.uri + portContextPath + encodeURIComponent(port.id),
                        dataType: 'json',
                        contentType: 'application/json'
                    }).done(function (response) {
                        // Update the RemoteProcessGroup revision.
                        // RemotePorts share revision with parent RemoteProcessGroup.
                        remoteProcessGroupData.revision = response.revision;

                        // get the response
                        var remotePort = response.remoteProcessGroupPort;

                        // if the remote port no long exists, disable the switch
                        if (remotePort.exists === false) {
                            // make the transmission switch disabled
                            transmissionSwitch.removeClass('enabled-active-transmission enabled-inactive-transmission').addClass('disabled-inactive-transmission').off('click');

                            // hide the edit button
                            if (nfCommon.isDefinedAndNotNull(editRemotePort)) {
                                editRemotePort.hide();
                            }
                        } else {
                            // update the transmission status accordingly
                            if (remotePort.transmitting === true) {
                                // mark the status as transmitting
                                transmissionSwitch.removeClass('enabled-active-transmission enabled-inactive-transmission').addClass('enabled-active-transmission');

                                // hide the edit button
                                if (nfCommon.isDefinedAndNotNull(editRemotePort)) {
                                    editRemotePort.hide();
                                }
                            } else {
                                // mark the status as not transmitting
                                transmissionSwitch.removeClass('enabled-active-transmission enabled-inactive-transmission').addClass('enabled-inactive-transmission');

                                // show the edit button
                                if (nfCommon.isDefinedAndNotNull(editRemotePort)) {
                                    editRemotePort.show();
                                }
                            }
                        }
                    }).fail(function (xhr, status, error) {
                        // create replacement switch
                        var newTransmissionSwitch = createTransmissionSwitch(port);
                        // add click handler
                        newTransmissionSwitch.click(transmissionSwitchClickFunction);
                        //replace DOM element
                        transmissionSwitch.replaceWith(newTransmissionSwitch);
                        // update transmissionSwitch variable to reference the new switch
                        transmissionSwitch = newTransmissionSwitch;
                        if (xhr.status === 400) {
                            var errors = xhr.responseText.split('\n');

                            var content;
                            if (errors.length === 1) {
                                content = $('<span></span>').text(errors[0]);
                            } else {
                                content = nfCommon.formatUnorderedList(errors);
                            }

                            nfDialog.showOkDialog({
                                headerText: 'Remote Process Group Ports',
                                dialogContent: content
                            });
                        } else {
                            nfErrorHandler.handleAjaxError(xhr, status, error);
                        }
                    });
                };

                // create toggle for changing transmission state
                transmissionSwitch.click(transmissionSwitchClickFunction);
            }
        } else {
            // show the disabled transmission switch
            if (port.transmitting === true) {
                (nfNgBridge.injector.get('$compile')($('<md-switch style="margin:0px" class="md-primary disabled-active-transmission" aria-label="Toggle port transmission"></md-switch>'))(nfNgBridge.rootScope)).appendTo(portContainerEditContainer);
            } else {
                (nfNgBridge.injector.get('$compile')($('<md-switch ng-disabled="true" style="margin:0px" class="md-primary disabled-inactive-transmission" aria-label="Toggle port transmission"></md-switch>'))(nfNgBridge.rootScope)).appendTo(portContainerEditContainer);
            }
        }

        // add the port id and name
        $('<div id="' + portId + '-id" class="remote-port-id hidden"></div>').text(port.id).appendTo(portContainerDetailsContainer);
        $('<div id="' + portId + '-name" class="remote-port-name ellipsis"></div>').text(port.name).appendTo(portContainerDetailsContainer);

        // clear
        $('<div class="clear"></div>').appendTo(portContainerDetailsContainer);

        // add the comments for this port
        if (nfCommon.isBlank(port.comments)) {
            $('<div class="remote-port-description unset">No description specified.</div>').appendTo(portContainerDetailsContainer);
        } else {
            $('<div class="remote-port-description"></div>').text(port.comments).appendTo(portContainerDetailsContainer);
        }

        // clear
        $('<div class="clear"></div>').appendTo(portContainerDetailsContainer);

        var concurrentTasksContainer = $('<div class="concurrent-task-container"></div>').appendTo(portContainerDetailsContainer);

        // concurrent tasks
        var concurrentTasks = $('<div class="setting-field"></div>').append($('<div id="' + portId + '-concurrent-tasks"></div>').text(port.concurrentlySchedulableTaskCount));

        // add this ports concurrent tasks
        $('<div>' +
            '<div class="setting-name">' +
            'Concurrent tasks' +
            '<div class="processor-setting concurrent-tasks-info fa fa-question-circle"></div>' +
            '</div>' +
            '</div>').append(concurrentTasks).appendTo(concurrentTasksContainer).find('div.concurrent-tasks-info').qtip($.extend({},
            nfCommon.config.tooltipConfig,
            {
                content: 'The number of tasks that should be concurrently scheduled for this port.'
            }));

        var compressionContainer = $('<div class="compression-container"></div>').appendTo(portContainerDetailsContainer);

        // determine the compression label
        var compressionLabel = 'No';
        if (port.useCompression === true) {
            compressionLabel = 'Yes';
        }

        // add this ports compression config
        $('<div>' +
            '<div class="setting-name">' +
            'Compressed' +
            '</div>' +
            '<div class="setting-field">' +
            '<div id="' + portId + '-compression">' + compressionLabel + '</div>' +
            '</div>' +
            '</div>').appendTo(compressionContainer);

        // clear
        $('<div class="clear"></div>').appendTo(portContainer);

        // apply ellipsis where appropriate
        portContainer.find('.ellipsis').ellipsis();

        // inform Angular app values have changed
        nfNgBridge.digest();
    };

    /**
     * Configures the specified remote port.
     *
     * @argument {string} portId            The port id
     * @argument {string} portName          The port name
     * @argument {int} portConcurrentTasks  The number of concurrent tasks for the port
     * @argument {boolean} portCompression  The compression flag for the port
     * @argument {string} portType          The type of port this is
     */
    var configureRemotePort = function (portId, portName, portConcurrentTasks, portCompression, portType) {
        // set port identifiers
        $('#remote-port-id').text(portId);
        $('#remote-port-type').text(portType);

        // set port configuration
        var checkState = 'checkbox-unchecked';
        if (portCompression === true) {
            checkState = 'checkbox-checked';
        }
        $('#remote-port-use-compression').addClass(checkState);
        $('#remote-port-concurrent-tasks').val(portConcurrentTasks);

        // set the port name
        $('#remote-port-name').text(portName).ellipsis();

        // show the dialog
        $('#remote-port-configuration').modal('show');
    };

    return {
        init: function () {
            initRemotePortConfigurationDialog();
            initRemoteProcessGroupConfigurationDialog();
        },

        /**
         * Shows the details for the remote process group in the specified selection.
         *
         * @argument {selection} selection      The selection
         */
        showPorts: function (selection) {
            // if the specified component is a remote process group, load its properties
            if (nfCanvasUtils.isRemoteProcessGroup(selection)) {
                var selectionData = selection.datum();

                // load the properties for the specified component
                $.ajax({
                    type: 'GET',
                    url: selectionData.uri,
                    dataType: 'json'
                }).done(function (response) {
                    var remoteProcessGroup = response.component;

                    // set the model locally
                    nfRemoteProcessGroup.set(response);

                    // populate the port settings
                    $('#remote-process-group-ports-id').text(remoteProcessGroup.id);
                    $('#remote-process-group-ports-name').text(remoteProcessGroup.name);
                    $('#remote-process-group-ports-urls').text(remoteProcessGroup.targetUris);

                    // get the contents
                    var remoteProcessGroupContents = remoteProcessGroup.contents;
                    if (nfCommon.isDefinedAndNotNull(remoteProcessGroupContents)) {
                        var connectedInputPorts = [];
                        var disconnectedInputPorts = [];

                        // show connected ports first
                        var inputPortContainer = $('#remote-process-group-input-ports-container');
                        $.each(remoteProcessGroupContents.inputPorts, function (_, inputPort) {
                            if (inputPort.connected === true) {
                                connectedInputPorts.push(inputPort);
                            } else {
                                disconnectedInputPorts.push(inputPort);
                            }
                        });

                        // add all connected input ports
                        $.each(connectedInputPorts, function (_, inputPort) {
                            createPortOption(inputPortContainer, inputPort, 'input');
                        });

                        // add all disconnected input ports
                        $.each(disconnectedInputPorts, function (_, inputPort) {
                            createPortOption(inputPortContainer, inputPort, 'input');
                        });

                        if (nfCommon.isEmpty(connectedInputPorts) && nfCommon.isEmpty(disconnectedInputPorts)) {
                            $('<div class="unset"></div>').text("No ports to display").appendTo(inputPortContainer);
                        }

                        var connectedOutputPorts = [];
                        var disconnectedOutputPorts = [];

                        // process all the output ports
                        var outputPortContainer = $('#remote-process-group-output-ports-container');
                        $.each(remoteProcessGroupContents.outputPorts, function (i, outputPort) {
                            if (outputPort.connected === true) {
                                connectedOutputPorts.push(outputPort);
                            } else {
                                disconnectedOutputPorts.push(outputPort);
                            }
                        });

                        // add all connected output ports
                        $.each(connectedOutputPorts, function (_, outputPort) {
                            createPortOption(outputPortContainer, outputPort, 'output');
                        });

                        // add all disconnected output ports
                        $.each(disconnectedOutputPorts, function (_, outputPort) {
                            createPortOption(outputPortContainer, outputPort, 'output');
                        });

                        if (nfCommon.isEmpty(connectedOutputPorts) && nfCommon.isEmpty(disconnectedOutputPorts)) {
                            $('<div class="unset"></div>').text("No ports to display").appendTo(outputPortContainer);
                        }
                    }

                    // show the details
                    $('#remote-process-group-ports').modal('show');
                }).fail(nfErrorHandler.handleAjaxError);
            }
        }
    };
}));