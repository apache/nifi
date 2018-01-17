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
                'nf.Common',
                'nf.CanvasUtils'],
            function ($, nfCommon, nfCanvasUtils) {
                return (nf.RemoteProcessGroupDetails = factory($, nfCommon, nfCanvasUtils));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.RemoteProcessGroupDetails =
            factory(require('jquery'),
                require('nf.Common'),
                require('nf.CanvasUtils')));
    } else {
        nf.RemoteProcessGroupDetails = factory(root.$,
            root.nf.Common,
            root.nf.CanvasUtils);
    }
}(this, function ($, nfCommon, nfCanvasUtils) {
    'use strict';

    return {
        init: function () {
            $('#remote-process-group-details').modal({
                headerText: nf._.msg('nf-remote-process-group-details.RemoteProcessGroupDetails'),
                scrollableContentStyle: 'scrollable',
                buttons: [{
                    buttonText: nf._.msg('nf-remote-process-group-details.Ok'),
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            $('#remote-process-group-details').modal('hide');
                        }
                    }
                }],
                handler: {
                    close: function () {
                        // clear the remote process group details
                        nfCommon.clearField('read-only-remote-process-group-id');
                        nfCommon.clearField('read-only-remote-process-group-name');
                        nfCommon.clearField('read-only-remote-process-group-urls');
                        nfCommon.clearField('read-only-remote-process-group-timeout');
                        nfCommon.clearField('read-only-remote-process-group-yield-duration');
                        nfCommon.clearField('read-only-remote-process-group-transport-protocol');
                        nfCommon.clearField('read-only-remote-process-group-local-network-interface');
                        nfCommon.clearField('read-only-remote-process-group-proxy-host');
                        nfCommon.clearField('read-only-remote-process-group-proxy-port');
                        nfCommon.clearField('read-only-remote-process-group-proxy-user');
                        nfCommon.clearField('read-only-remote-process-group-proxy-password');
                    }
                }
            });
        },

        /**
         * Shows the details for the remote process group in the specified selection.
         *
         * @argument {selection} selection      The selection
         */
        showDetails: function (selection) {
            // if the specified component is a remote process group, load its properties
            if (nfCanvasUtils.isRemoteProcessGroup(selection)) {
                var selectionData = selection.datum();

                // populate the port settings
                nfCommon.populateField('read-only-remote-process-group-id', selectionData.id);
                nfCommon.populateField('read-only-remote-process-group-name', selectionData.component.name);
                nfCommon.populateField('read-only-remote-process-group-urls', selectionData.component.targetUris);
                nfCommon.populateField('read-only-remote-process-group-timeout', selectionData.component.communicationsTimeout);
                nfCommon.populateField('read-only-remote-process-group-yield-duration', selectionData.component.yieldDuration);
                nfCommon.populateField('read-only-remote-process-group-transport-protocol', selectionData.component.transportProtocol);
                nfCommon.populateField('read-only-remote-process-group-local-network-interface', selectionData.component.localNetworkInterface);
                nfCommon.populateField('read-only-remote-process-group-proxy-host', selectionData.component.proxyHost);
                nfCommon.populateField('read-only-remote-process-group-proxy-port', selectionData.component.proxyPort);
                nfCommon.populateField('read-only-remote-process-group-proxy-user', selectionData.component.proxyUser);
                nfCommon.populateField('read-only-remote-process-group-proxy-password', selectionData.component.proxyPassword);

                // show the details
                $('#remote-process-group-details').modal('show');
            }
        }
    };
}));