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
            function ($, common, canvasUtils) {
                return (nf.RemoteProcessGroupDetails = factory($, common, canvasUtils));
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
}(this, function ($, common, canvasUtils) {
    'use strict';

    return {
        init: function () {
            $('#remote-process-group-details').modal({
                headerText: 'Remote Process Group Details',
                scrollableContentStyle: 'scrollable',
                buttons: [{
                    buttonText: 'Ok',
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
                        common.clearField('read-only-remote-process-group-id');
                        common.clearField('read-only-remote-process-group-name');
                        common.clearField('read-only-remote-process-group-urls');
                        common.clearField('read-only-remote-process-group-timeout');
                        common.clearField('read-only-remote-process-group-yield-duration');
                        common.clearField('read-only-remote-process-group-transport-protocol');
                        common.clearField('read-only-remote-process-group-proxy-host');
                        common.clearField('read-only-remote-process-group-proxy-port');
                        common.clearField('read-only-remote-process-group-proxy-user');
                        common.clearField('read-only-remote-process-group-proxy-password');
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
            if (canvasUtils.isRemoteProcessGroup(selection)) {
                var selectionData = selection.datum();

                // populate the port settings
                common.populateField('read-only-remote-process-group-id', selectionData.id);
                common.populateField('read-only-remote-process-group-name', selectionData.component.name);
                common.populateField('read-only-remote-process-group-urls', selectionData.component.targetUris);
                common.populateField('read-only-remote-process-group-timeout', selectionData.component.communicationsTimeout);
                common.populateField('read-only-remote-process-group-yield-duration', selectionData.component.yieldDuration);
                common.populateField('read-only-remote-process-group-transport-protocol', selectionData.component.transportProtocol);
                common.populateField('read-only-remote-process-group-proxy-host', selectionData.component.proxyHost);
                common.populateField('read-only-remote-process-group-proxy-port', selectionData.component.proxyPort);
                common.populateField('read-only-remote-process-group-proxy-user', selectionData.component.proxyUser);
                common.populateField('read-only-remote-process-group-proxy-password', selectionData.component.proxyPassword);

                // show the details
                $('#remote-process-group-details').modal('show');
            }
        }
    };
}));