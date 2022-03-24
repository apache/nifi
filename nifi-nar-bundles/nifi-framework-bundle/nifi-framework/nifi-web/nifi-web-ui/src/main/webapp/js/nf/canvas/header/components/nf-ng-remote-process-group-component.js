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
                'nf.Client',
                'nf.Birdseye',
                'nf.Storage',
                'nf.Graph',
                'nf.CanvasUtils',
                'nf.ErrorHandler',
                'nf.Dialog',
                'nf.Common'],
            function ($, nfClient, nfBirdseye, nfStorage, nfGraph, nfCanvasUtils, nfErrorHandler, nfDialog, nfCommon) {
                return (nf.ng.RemoteProcessGroupComponent = factory($, nfClient, nfBirdseye, nfStorage, nfGraph, nfCanvasUtils, nfErrorHandler, nfDialog, nfCommon));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.RemoteProcessGroupComponent =
            factory(require('jquery'),
                require('nf.Client'),
                require('nf.Birdseye'),
                require('nf.Storage'),
                require('nf.Graph'),
                require('nf.CanvasUtils'),
                require('nf.ErrorHandler'),
                require('nf.Dialog'),
                require('nf.Common')));
    } else {
        nf.ng.RemoteProcessGroupComponent = factory(root.$,
            root.nf.Client,
            root.nf.Birdseye,
            root.nf.Storage,
            root.nf.Graph,
            root.nf.CanvasUtils,
            root.nf.ErrorHandler,
            root.nf.Dialog,
            root.nf.Common);
    }
}(this, function ($, nfClient, nfBirdseye, nfStorage, nfGraph, nfCanvasUtils, nfErrorHandler, nfDialog, nfCommon) {
    'use strict';

    return function (serviceProvider) {
        'use strict';

        /**
         * Create the controller and add to the graph.
         *
         * @argument {object} pt                            The point that the remote group was dropped.
         */
        var createRemoteProcessGroup = function (pt) {

            var remoteProcessGroupEntity = {
                'revision': nfClient.getRevision({
                    'revision': {
                        'version': 0
                    }
                }),
                'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
                'component': {
                    'targetUris': $('#new-remote-process-group-uris').val(),
                    'position': {
                        'x': pt.x,
                        'y': pt.y
                    },
                    'communicationsTimeout': $('#new-remote-process-group-timeout').val(),
                    'yieldDuration': $('#new-remote-process-group-yield-duration').val(),
                    'transportProtocol': $('#new-remote-process-group-transport-protocol-combo').combo('getSelectedOption').value,
                    'localNetworkInterface': $('#new-remote-process-group-local-network-interface').val(),
                    'proxyHost': $('#new-remote-process-group-proxy-host').val(),
                    'proxyPort': $('#new-remote-process-group-proxy-port').val(),
                    'proxyUser': $('#new-remote-process-group-proxy-user').val(),
                    'proxyPassword': $('#new-remote-process-group-proxy-password').val()
                }
            };

            // create a new processor of the defined type
            $.ajax({
                type: 'POST',
                url: serviceProvider.headerCtrl.toolboxCtrl.config.urls.api + '/process-groups/' + encodeURIComponent(nfCanvasUtils.getGroupId()) + '/remote-process-groups',
                data: JSON.stringify(remoteProcessGroupEntity),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // add the processor to the graph
                nfGraph.add({
                    'remoteProcessGroups': [response]
                }, {
                    'selectAll': true
                });

                // hide the dialog
                $('#new-remote-process-group-dialog').modal('hide');

                // update component visibility
                nfGraph.updateVisibility();

                // update the birdseye
                nfBirdseye.refresh();
            }).fail(nfErrorHandler.handleConfigurationUpdateAjaxError);
        };

        function RemoteProcessGroupComponent() {

            this.icon = 'icon icon-group-remote';

            this.hoverIcon = 'icon icon-group-remote-add';

            /**
             * The remote group component's modal.
             */
            this.modal = {

                /**
                 * Gets the modal element.
                 *
                 * @returns {*|jQuery|HTMLElement}
                 */
                getElement: function () {
                    return $('#new-remote-process-group-dialog');
                },

                /**
                 * Initialize the modal.
                 */
                init: function () {
                    var defaultTimeout = "30 sec";
                    var defaultYieldDuration = "10 sec";

                    // configure the new remote process group dialog
                    this.getElement().modal({
                        scrollableContentStyle: 'scrollable',
                        headerText: 'Add Remote Process Group',
                        handler: {
                            close: function () {
                                $('#new-remote-process-group-uris').val('');
                                $('#new-remote-process-group-timeout').val(defaultTimeout);
                                $('#new-remote-process-group-yield-duration').val(defaultYieldDuration);
                                $('#new-remote-process-group-transport-protocol-combo').combo('setSelectedOption', {
                                    value: 'RAW'
                                });
                                $('#new-remote-process-group-local-network-interface').val('');
                                $('#new-remote-process-group-proxy-host').val('');
                                $('#new-remote-process-group-proxy-port').val('');
                                $('#new-remote-process-group-proxy-user').val('');
                                $('#new-remote-process-group-proxy-password').val('');
                            }
                        }
                    });

                    // set default values
                    $('#new-remote-process-group-timeout').val(defaultTimeout);
                    $('#new-remote-process-group-yield-duration').val(defaultYieldDuration);

                    // initialize the transport protocol combo
                    $('#new-remote-process-group-transport-protocol-combo').combo({
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
                 * Updates the modal config.
                 *
                 * @param {string} name             The name of the property to update.
                 * @param {object|array} config     The config for the `name`.
                 */
                update: function (name, config) {
                    this.getElement().modal(name, config);
                },

                /**
                 * Show the modal.
                 */
                show: function () {
                    this.getElement().modal('show');
                },

                /**
                 * Hide the modal.
                 */
                hide: function () {
                    this.getElement().modal('hide');
                }
            };
        }

        RemoteProcessGroupComponent.prototype = {
            constructor: RemoteProcessGroupComponent,

            /**
             * Gets the component.
             *
             * @returns {*|jQuery|HTMLElement}
             */
            getElement: function () {
                return $('#group-remote-component');
            },

            /**
             * Enable the component.
             */
            enabled: function () {
                this.getElement().attr('disabled', false);
            },

            /**
             * Disable the component.
             */
            disabled: function () {
                this.getElement().attr('disabled', true);
            },

            /**
             * Handler function for when component is dropped on the canvas.
             *
             * @argument {object} pt        The point that the component was dropped.
             */
            dropHandler: function (pt) {
                this.promptForRemoteProcessGroupUri(pt);
            },

            /**
             * The drag icon for the toolbox component.
             *
             * @param event
             * @returns {*|jQuery|HTMLElement}
             */
            dragIcon: function (event) {
                return $('<div class="icon icon-group-remote-add"></div>');
            },

            /**
             * Prompts the user to enter the URI for the remote process group.
             *
             * @argument {object} pt        The point that the remote group was dropped.
             */
            promptForRemoteProcessGroupUri: function (pt) {
                var remoteProcessGroupComponent = this;
                var addRemoteProcessGroup = function () {
                    // create the remote process group
                    createRemoteProcessGroup(pt);
                };

                this.modal.update('setButtonModel', [{
                    buttonText: 'Add',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: addRemoteProcessGroup
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
                                remoteProcessGroupComponent.modal.hide();
                            }
                        }
                    }]);

                // show the dialog
                this.modal.show();

                // set the focus and key handlers
                $('#new-remote-process-group-uris').focus().off('keyup').on('keyup', function (e) {
                    var code = e.keyCode ? e.keyCode : e.which;
                    if (code === $.ui.keyCode.ENTER) {
                        addRemoteProcessGroup();
                    }
                });
            }
        }

        var remoteProcessGroupComponent = new RemoteProcessGroupComponent();
        return remoteProcessGroupComponent;
    };
}));