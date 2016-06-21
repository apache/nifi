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

nf.ng.GroupComponent = function (serviceProvider) {
    'use strict';

    /**
     * Create the group and add to the graph.
     *
     * @argument {string} groupName The name of the group.
     * @argument {object} pt        The point that the group was dropped.
     */
    var createGroup = function (groupName, pt) {
        var processGroupEntity = {
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
            url: serviceProvider.headerCtrl.toolboxCtrl.config.urls.api + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/process-groups',
            data: JSON.stringify(processGroupEntity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.component)) {
                // add the process group to the graph
                nf.Graph.add({
                    'processGroups': [response]
                }, {
                    'selectAll': true
                });

                // update component visibility
                nf.Canvas.View.updateVisibility();

                // update the birdseye
                nf.Birdseye.refresh();
            }
        }).fail(nf.Common.handleAjaxError);
    };

    function GroupComponent() {

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
                    headerText: 'Add Process Group',
                    handler: {
                        close: function () {
                            $('#new-process-group-name').val('');
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
            this.promptForGroupName(pt);
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
         */
        promptForGroupName: function (pt) {
            var self = this;
            return $.Deferred(function (deferred) {
                var addGroup = function () {
                    // get the name of the group and clear the textfield
                    var groupName = $('#new-process-group-name').val();

                    // hide the dialog
                    self.modal.hide();

                    // create the group and resolve the deferred accordingly
                    createGroup(groupName, pt).done(function (response) {
                        deferred.resolve(response.component);
                    }).fail(function () {
                        deferred.reject();
                    });
                };

                self.modal.update('setButtonModel', [{
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
                                self.modal.hide();
                            }
                        }
                    }]);

                // show the dialog
                self.modal.show();

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