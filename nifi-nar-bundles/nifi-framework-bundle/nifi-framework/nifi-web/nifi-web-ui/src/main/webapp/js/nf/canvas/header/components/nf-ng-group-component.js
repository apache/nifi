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
                'nf.Common',
                'nf.Dialog'],
            function ($, nfClient, nfBirdseye, nfStorage, nfGraph, nfCanvasUtils, nfErrorHandler, nfCommon, nfDialog) {
                return (nf.ng.GroupComponent = factory($, nfClient, nfBirdseye, nfStorage, nfGraph, nfCanvasUtils, nfErrorHandler, nfCommon, nfDialog));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.GroupComponent =
            factory(require('jquery'),
                require('nf.Client'),
                require('nf.Birdseye'),
                require('nf.Storage'),
                require('nf.Graph'),
                require('nf.CanvasUtils'),
                require('nf.ErrorHandler'),
                require('nf.Common'),
                require('nf.Dialog')));
    } else {
        nf.ng.GroupComponent = factory(root.$,
            root.nf.Client,
            root.nf.Birdseye,
            root.nf.Storage,
            root.nf.Graph,
            root.nf.CanvasUtils,
            root.nf.ErrorHandler,
            root.nf.Common,
            root.nf.Dialog);
    }
}(this, function ($, nfClient, nfBirdseye, nfStorage, nfGraph, nfCanvasUtils, nfErrorHandler, nfCommon, nfDialog) {
    'use strict';

    return function (serviceProvider) {
        'use strict';

        /**
         * Create the group and add to the graph.
         *
         * @argument {string} groupName The name of the group.
         * @argument {object} pt        The point that the group was dropped.
         */
        var createGroup = function (groupName, pt) {
            var processGroupEntity = {
                'revision': nfClient.getRevision({
                    'revision': {
                        'version': 0
                    }
                }),
                'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
                'component': {
                    'name': groupName,
                    'position': {
                        'x': pt.x,
                        'y': pt.y
                    }
                }
            };

            // create a new processor of the defined type
            return $.ajax({
                type: 'POST',
                url: serviceProvider.headerCtrl.toolboxCtrl.config.urls.api + '/process-groups/' + encodeURIComponent(nfCanvasUtils.getGroupId()) + '/process-groups',
                data: JSON.stringify(processGroupEntity),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // add the process group to the graph
                nfGraph.add({
                    'processGroups': [response]
                }, {
                    'selectAll': true
                });

                // update component visibility
                nfGraph.updateVisibility();

                // update the birdseye
                nfBirdseye.refresh();
            }).fail(nfErrorHandler.handleConfigurationUpdateAjaxError);
        };

        function GroupComponent() {

            this.icon = 'icon icon-group';

            this.hoverIcon = 'icon icon-group-add';

            /**
             * The group component's modal.
             */
            this.modal = {

                /**
                 * Gets the modal element.
                 *
                 * @returns {*|jQuery|HTMLElement}
                 */
                getElement: function () {
                    return $('#new-process-group-dialog');
                },

                /**
                 * Initialize the modal.
                 */
                init: function () {
                    // configure the new process group dialog
                    this.getElement().modal({
                        scrollableContentStyle: 'scrollable',
                        headerText: 'Add Process Group',
                        handler: {
                            close: function () {
                                $('#new-process-group-name').val('');
                                $('#new-process-group-dialog').removeData('pt');
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
                 * Stores the pt.
                 *
                 * @param pt
                 */
                storePt: function (pt) {
                    $('#new-process-group-dialog').data('pt', pt);
                },

                /**
                 * Hide the modal.
                 */
                hide: function () {
                    this.getElement().modal('hide');
                }
            };
        }

        GroupComponent.prototype = {
            constructor: GroupComponent,

            /**
             * Gets the component.
             *
             * @returns {*|jQuery|HTMLElement}
             */
            getElement: function () {
                return $('#group-component');
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
                this.promptForGroupName(pt, true);
            },

            /**
             * The drag icon for the toolbox component.
             *
             * @param event
             * @returns {*|jQuery|HTMLElement}
             */
            dragIcon: function (event) {
                return $('<div class="icon icon-group-add"></div>');
            },

            /**
             * Prompts the user to enter the name for the group.
             *
             * @argument {object} pt        The point that the group was dropped.
             * @argument {boolean} showImportLink Whether we should show the import link
             */
            promptForGroupName: function (pt, showImportLink) {
                var groupComponent = this;
                return $.Deferred(function (deferred) {
                    var addGroup = function () {
                        // get the name of the group and clear the textfield
                        var groupName = $('#new-process-group-name').val();

                        // ensure the group name is specified
                        if (nfCommon.isBlank(groupName)) {
                            nfDialog.showOkDialog({
                                headerText: 'Configuration Error',
                                dialogContent: 'The name of the process group must be specified.'
                            });

                            deferred.reject();
                        } else {
                            // hide the dialog
                            groupComponent.modal.hide();

                            // create the group and resolve the deferred accordingly
                            createGroup(groupName, pt).done(function (response) {
                                deferred.resolve(response.component);
                            }).fail(function () {
                                deferred.reject();
                            });
                        }
                    };

                    groupComponent.modal.update('setButtonModel', [{
                        buttonText: 'Add',
                        color: {
                            base: '#728E9B',
                            hover: '#004849',
                            text: '#ffffff'
                        },
                        handler: {
                            click: addGroup
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
                                    // reject the deferred
                                    deferred.reject();

                                    // close the dialog
                                    groupComponent.modal.hide();
                                }
                            }
                        }]);

                    if (showImportLink === true && nfCommon.canVersionFlows()) {
                        $('#import-process-group-link').show();
                    } else {
                        $('#import-process-group-link').hide();
                    }

                    // show the dialog
                    groupComponent.modal.storePt(pt);
                    groupComponent.modal.show();

                    // set up the focus and key handlers
                    $('#new-process-group-name').focus().off('keyup').on('keyup', function (e) {
                        var code = e.keyCode ? e.keyCode : e.which;
                        if (code === $.ui.keyCode.ENTER) {
                            addGroup();
                        }
                    });
                }).promise();
            }
        }

        var groupComponent = new GroupComponent();
        return groupComponent;
    };
}));