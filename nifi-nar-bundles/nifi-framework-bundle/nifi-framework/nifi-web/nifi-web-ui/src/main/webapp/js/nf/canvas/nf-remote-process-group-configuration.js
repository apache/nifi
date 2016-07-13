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

nf.RemoteProcessGroupConfiguration = (function () {
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
                                    'revision': nf.Client.getRevision(remoteProcessGroupData),
                                    'component': {
                                        id: remoteProcessGroupId,
                                        communicationsTimeout: $('#remote-process-group-timeout').val(),
                                        yieldDuration: $('#remote-process-group-yield-duration').val(),
                                        transportProtocol: $('#remote-process-group-transport-protocol-combo').combo('getSelectedOption').value,
                                        proxyHost: $('#remote-process-group-proxy-host').val(),
                                        proxyPort: $('#remote-process-group-proxy-port').val(),
                                        proxyUser: $('#remote-process-group-proxy-user').val(),
                                        proxyPassword: $('#remote-process-group-proxy-password').val()
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
                                nf.RemoteProcessGroup.set(response);

                                // inform Angular app values have changed
                                nf.ng.Bridge.digest();
                                
                                // close the details panel
                                $('#remote-process-group-configuration').modal('hide');
                            }).fail(function (xhr, status, error) {
                                if (xhr.status === 400) {
                                    var errors = xhr.responseText.split('\n');

                                    var content;
                                    if (errors.length === 1) {
                                        content = $('<span></span>').text(errors[0]);
                                    } else {
                                        content = nf.Common.formatUnorderedList(errors);
                                    }

                                    nf.Dialog.showOkDialog({
                                        dialogContent: content,
                                        headerText: 'Remote Process Group Configuration'
                                    });
                                } else {
                                    nf.Common.handleAjaxError(xhr, status, error);
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
                        $('#remote-process-group-url').text('');
                        $('#remote-process-group-timeout').val('');
                        $('#remote-process-group-yield-duration').val('');
                        $('#remote-process-group-transport-protocol-combo').combo('setSelectedOption', {
                            value: 'RAW'
                        });
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
            if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                var selectionData = selection.datum();

                // populate the port settings
                $('#remote-process-group-id').text(selectionData.id);
                $('#remote-process-group-name').text(selectionData.component.name);
                $('#remote-process-group-url').text(selectionData.component.targetUri);

                // populate the text fields
                $('#remote-process-group-timeout').val(selectionData.component.communicationsTimeout);
                $('#remote-process-group-yield-duration').val(selectionData.component.yieldDuration);
                $('#remote-process-group-proxy-host').val(selectionData.component.proxyHost);
                $('#remote-process-group-proxy-port').val(selectionData.component.proxyPort);
                $('#remote-process-group-proxy-user').val(selectionData.component.proxyUser);
                $('#remote-process-group-proxy-password').val(selectionData.component.proxyPassword);

                // select the appropriate transport-protocol
                $('#remote-process-group-transport-protocol-combo').combo('setSelectedOption', {
                    value: selectionData.component.transportProtocol
                });

                // show the details
                $('#remote-process-group-configuration').modal('show');
            }
        }
    };
}());