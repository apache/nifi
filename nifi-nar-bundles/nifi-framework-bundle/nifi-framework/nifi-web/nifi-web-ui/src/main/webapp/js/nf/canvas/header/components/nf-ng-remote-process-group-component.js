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

nf.ng.RemoteProcessGroupComponent = (function () {

    function RemoteProcessGroupComponent(serviceProvider) {

        /**
         * Create the controller and add to the graph.
         *
         * @argument {string} remoteProcessGroupUri         The remote group uri.
         * @argument {object} pt                            The point that the remote group was dropped.
         */
        var createRemoteProcessGroup = function (remoteProcessGroupUri, pt) {
            var remoteProcessGroupEntity = {
                'revision': nf.Client.getRevision(),
                'component': {
                    'targetUri': remoteProcessGroupUri,
                    'position': {
                        'x': pt.x,
                        'y': pt.y
                    }
                }
            };

            // create a new processor of the defined type
            $.ajax({
                type: 'POST',
                url: serviceProvider.headerCtrl.toolboxCtrl.config.urls.api + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/remote-process-groups',
                data: JSON.stringify(remoteProcessGroupEntity),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                if (nf.Common.isDefinedAndNotNull(response.component)) {
                    // update the revision
                    nf.Client.setRevision(response.revision);

                    // add the processor to the graph
                    nf.Graph.add({
                        'remoteProcessGroups': [response]
                    }, true);

                    // update component visibility
                    nf.Canvas.View.updateVisibility();

                    // update the birdseye
                    nf.Birdseye.refresh();
                }
            }).fail(nf.Common.handleAjaxError);
        };

        function RemoteProcessGroupComponent() {
        };
        RemoteProcessGroupComponent.prototype = {
            constructor: RemoteProcessGroupComponent,

            /**
             * The remote group component's modal.
             */
            modal: {

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
                    // configure the new remote process group dialog
                    this.getElement().modal({
                        headerText: 'Add Remote Process Group',
                        overlayBackground: false,
                        handler: {
                            close: function () {
                                $('#new-remote-process-group-uri').val('');
                            }
                        }
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
            },

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
             * Prompts the user to enter the URI for the remote process group.
             *
             * @argument {object} pt        The point that the remote group was dropped.
             */
            promptForRemoteProcessGroupUri: function (pt) {
                var self = this;
                var addRemoteProcessGroup = function () {
                    // get the uri of the controller and clear the textfield
                    var remoteProcessGroupUri = $('#new-remote-process-group-uri').val();

                    // hide the dialog
                    self.modal.hide();

                    // create the remote process group
                    createRemoteProcessGroup(remoteProcessGroupUri, pt);
                };

                this.modal.update('setButtonModel', [{
                    buttonText: 'Add',
                    handler: {
                        click: addRemoteProcessGroup
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: function () {
                            self.modal.hide();
                            ;
                        }
                    }
                }]);

                // show the dialog
                this.modal.show();

                // set the focus and key handlers
                $('#new-remote-process-group-uri').focus().off('keyup').on('keyup', function (e) {
                    var code = e.keyCode ? e.keyCode : e.which;
                    if (code === $.ui.keyCode.ENTER) {
                        addRemoteProcessGroup();
                    }
                });
            }
        };
        var remoteProcessGroupComponent = new RemoteProcessGroupComponent();
        return remoteProcessGroupComponent;
    }

    RemoteProcessGroupComponent.$inject = ['serviceProvider'];

    return RemoteProcessGroupComponent;
}());