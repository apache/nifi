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

nf.ng.Canvas.GlobalMenuCtrl = (function () {

    function GlobalMenuCtrl(serviceProvider) {

        var config = {
            urls: {
                helpDocument: '../nifi-docs/documentation',
                controllerAbout: '../nifi-api/flow/about'
            }
        };

        function GlobalMenuCtrl() {
        };
        GlobalMenuCtrl.prototype = {
            constructor: GlobalMenuCtrl,

            init: function () {
                this.about.modal.init();
            },

            /**
             * The summary menu item.
             */
            summary: {

                /**
                 * The summary menu item's shell.
                 */
                shell: {

                    /**
                     * Launch the summary shell.
                     */
                    launch: function () {
                        nf.Shell.showPage('summary');
                    }
                }
            },

            /**
             * The counters menu item.
             */
            counters: {

                /**
                 * The counters menu item's shell.
                 */
                shell: {

                    /**
                     * Launch the counters shell.
                     */
                    launch: function () {
                        nf.Shell.showPage('counters');
                    }
                }
            },

            /**
             * The bulletin board menu item.
             */
            bulletinBoard: {

                /**
                 * The bulletin board menu item's shell.
                 */
                shell: {

                    /**
                     * Launch the bulletin board shell.
                     */
                    launch: function () {
                        nf.Shell.showPage('bulletin-board');
                    }
                }
            },

            /**
             * The data provenance menu item.
             */
            dataProvenance: {

                /**
                 * Determines if the data provenance menu item is enabled.
                 *
                 * @returns {*|boolean}
                 */
                enabled: function () {
                    return nf.Common.canAccessProvenance();
                },

                /**
                 * The data provenance menu item's shell.
                 */
                shell: {

                    /**
                     * Launch the data provenance shell.
                     */
                    launch: function () {
                        nf.Shell.showPage('provenance');
                    }
                }
            },

            /**
             * The controller settings menu item.
             */
            controllerSettings: {

                /**
                 * The controller settings menu item's shell.
                 */
                shell: {

                    /**
                     * Launch the settings shell.
                     */
                    launch: function () {
                        nf.Settings.loadSettings().done(function () {
                            nf.Settings.showSettings();
                        });
                    }
                }
            },

            /**
             * The cluster menu item.
             */
            cluster: {

                /**
                 * Determines if the cluster menu item is enabled.
                 *
                 * @returns {*|boolean}
                 */
                enabled: function () {
                    return nf.Canvas.isClustered();
                },

                /**
                 * The cluster menu item's shell.
                 */
                shell: {

                    /**
                     * Launch the cluster shell.
                     */
                    launch: function () {
                        nf.Shell.showPage('cluster');
                    }
                }
            },

            /**
             * The flow config history menu item.
             */
            flowConfigHistory: {

                /**
                 * The flow config history menu item's shell.
                 */
                shell: {

                    /**
                     * Launch the history shell.
                     */
                    launch: function () {
                        nf.Shell.showPage('history');
                    }
                }
            },

            /**
             * The users menu item.
             */
            users: {

                /**
                 * Determines if the users menu item is enabled.
                 *
                 * @returns {*|boolean}
                 */
                enabled: function () {
                    return nf.Common.isAdmin();
                },

                /**
                 * The users menu item's shell.
                 */
                shell: {

                    /**
                     * Launch the users shell.
                     */
                    launch: function () {
                        nf.Shell.showPage('users');
                    }
                }
            },

            /**
             * The templates menu item.
             */
            templates: {

                /**
                 * The templates menu item's shell.
                 */
                shell: {

                    /**
                     * Launch the templates shell.
                     */
                    launch: function () {
                        nf.Shell.showPage('templates?' + $.param({
                                groupId: nf.Canvas.getGroupId()
                            }));
                    }
                }
            },

            /**
             * The help menu item.
             */
            help: {

                /**
                 * The help menu item's shell.
                 */
                shell: {

                    /**
                     * Launch the help documentation shell.
                     */
                    launch: function () {
                        nf.Shell.showPage(config.urls.helpDocument);
                    }
                }
            },

            /**
             * The about menu item.
             */
            about: {

                /**
                 * The about menu item's modal.
                 */
                modal: {
                    
                    /**
                     * Gets the modal element.
                     *
                     * @returns {*|jQuery|HTMLElement}
                     */
                    getElement: function () {
                        return $('#nf-about');
                    },

                    /**
                     * Initialize the modal.
                     */
                    init: function () {
                        var self = this;
                        // get the about details
                        $.ajax({
                            type: 'GET',
                            url: config.urls.controllerAbout,
                            dataType: 'json'
                        }).done(function (response) {
                            var aboutDetails = response.about;
                            // set the document title and the about title
                            document.title = aboutDetails.title;
                            $('#nf-version').text(aboutDetails.version);
                        }).fail(nf.Common.handleAjaxError);

                        // configure the about dialog
                        this.getElement().modal({
                            overlayBackground: true,
                            buttons: [{
                                buttonText: 'Ok',
                                handler: {
                                    click: function () {
                                        self.hide();
                                    }
                                }
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
                     * Show the modal
                     */
                    show: function () {
                        this.getElement().modal('show');
                    },

                    /**
                     * Hide the modal
                     */
                    hide: function () {
                        this.getElement().modal('hide');
                    }
                }
            }
        };
        var globalMenuCtrl = new GlobalMenuCtrl();
        return globalMenuCtrl;
    }

    GlobalMenuCtrl.$inject = ['serviceProvider'];

    return GlobalMenuCtrl;
}());