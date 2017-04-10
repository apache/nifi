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
                return (nf.RemoteProcessGroupConfiguration = factory($, d3, nfErrorHandler, nfCommon, nfDialog, nfClient, nfCanvasUtils, nfNgBridge, nfRemoteProcessGroup));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.RemoteProcessGroupConfiguration =
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
        nf.RemoteProcessGroupConfiguration = factory(root.$,
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

    return {
        init: function () {
            $('#remote-process-group-configuration').modal({
                headerText: 'Configure Remote Process Group',
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
                            var remoteProcessGroupId = $('#remote-process-group-id').text();
                            var remoteProcessGroupData = d3.select('#id-' + remoteProcessGroupId).datum();

                            // create the remote process group details
                            var remoteProcessGroupEntity = {
                                'revision': nfClient.getRevision(remoteProcessGroupData),
                                'component': {
                                    id: remoteProcessGroupId,
                                    communicationsTimeout: $('#remote-process-group-timeout').val(),
                                    yieldDuration: $('#remote-process-group-yield-duration').val(),
                                    transportProtocol: $('#remote-process-group-transport-protocol-combo').combo('getSelectedOption').value,
                                    proxyHost: $('#remote-process-group-proxy-host').val(),
                                    proxyPort: $('#remote-process-group-proxy-port').val(),
                                    proxyUser: $('#remote-process-group-proxy-user').val(),
                                    proxyPassword: $('#remote-process-group-proxy-password').val(),
                                    localNetworkInterface: $('#remote-process-group-local-network-interface').val()
                                }
                            };

                            // update the selected component
                            $.ajax({
                                type: 'PUT',
                                data: JSON.stringify(remoteProcessGroupEntity),
                                url: remoteProcessGroupData.uri,
                                dataType: 'json',
                                contentType: 'application/json'
                            }).done(function (response) {
                                // refresh the remote process group component
                                nfRemoteProcessGroup.set(response);

                                // inform Angular app values have changed
                                nfNgBridge.digest();

                                // close the details panel
                                $('#remote-process-group-configuration').modal('hide');
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
                                        headerText: 'Remote Process Group Configuration'
                                    });
                                } else {
                                    nfErrorHandler.handleAjaxError(xhr, status, error);
                                }
                            });
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
                                $('#remote-process-group-configuration').modal('hide');
                            }
                        }
                    }],
                handler: {
                    close: function () {
                        // clear the remote process group details
                        $('#remote-process-group-id').text('');
                        $('#remote-process-group-name').text('');
                        $('#remote-process-group-urls').text('');
                        $('#remote-process-group-timeout').val('');
                        $('#remote-process-group-yield-duration').val('');
                        $('#remote-process-group-transport-protocol-combo').combo('setSelectedOption', {
                            value: 'RAW'
                        });
                        $('#remote-process-group-local-network-interface').val('');
                        $('#remote-process-group-proxy-host').val('');
                        $('#remote-process-group-proxy-port').val('');
                        $('#remote-process-group-proxy-user').val('');
                        $('#remote-process-group-proxy-password').val('');
                    }
                }
            });
            // initialize the transport protocol combo
            $('#remote-process-group-transport-protocol-combo').combo({
                options: [{
                    text: 'RAW',
                    value: 'RAW'
                }, {
                    text: 'HTTP',
                    value: 'HTTP'
                }]
            });
        },

        /**
         * Shows the details for the remote process group in the specified selection.
         *
         * @argument {selection} selection      The selection
         */
        showConfiguration: function (selection) {
            // if the specified component is a remote process group, load its properties
            if (nfCanvasUtils.isRemoteProcessGroup(selection)) {
                var selectionData = selection.datum();

                // populate the port settings
                $('#remote-process-group-id').text(selectionData.id);
                $('#remote-process-group-name').text(selectionData.component.name);
                $('#remote-process-group-urls').text(selectionData.component.targetUris);

                // populate the text fields
                $('#remote-process-group-timeout').val(selectionData.component.communicationsTimeout);
                $('#remote-process-group-yield-duration').val(selectionData.component.yieldDuration);
                $('#remote-process-group-proxy-host').val(selectionData.component.proxyHost);
                $('#remote-process-group-proxy-port').val(selectionData.component.proxyPort);
                $('#remote-process-group-proxy-user').val(selectionData.component.proxyUser);
                $('#remote-process-group-proxy-password').val(selectionData.component.proxyPassword);
                $('#remote-process-group-local-network-interface').val(selectionData.component.localNetworkInterface);

                // select the appropriate transport-protocol
                $('#remote-process-group-transport-protocol-combo').combo('setSelectedOption', {
                    value: selectionData.component.transportProtocol
                });

                // show the details
                $('#remote-process-group-configuration').modal('show');
            }
        }
    };
}));