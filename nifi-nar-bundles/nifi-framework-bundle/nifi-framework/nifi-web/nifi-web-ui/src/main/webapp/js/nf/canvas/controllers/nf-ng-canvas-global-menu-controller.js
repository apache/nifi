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

nf.ng.Canvas.GlobalMenuCtrl = function (serviceProvider) {
    'use strict';

    var config = {
        urls: {
            helpDocument: '../nifi-docs/documentation',
            controllerAbout: '../nifi-api/flow/about'
        }
    };

    function GlobalMenuCtrl(serviceProvider) {

        /**
         * The summary menu item controller.
         */
        this.summary = {

            /**
             * The summary menu item's shell controller.
             */
            shell: {

                /**
                 * Launch the summary shell.
                 */
                launch: function () {
                    nf.Shell.showPage('summary');
                }
            }
        };

        /**
         * The counters menu item controller.
         */
        this.counters = {

            /**
             * The counters menu item's shell controller.
             */
            shell: {

                /**
                 * Launch the counters shell.
                 */
                launch: function () {
                    if (nf.Common.canAccessCounters()) {
                        nf.Shell.showPage('counters');
                    }
                }
            }
        };

        /**
         * The bulletin board menu item controller.
         */
        this.bulletinBoard = {

            /**
             * The bulletin board menu item's shell controller.
             */
            shell: {

                /**
                 * Launch the bulletin board shell.
                 */
                launch: function () {
                    nf.Shell.showPage('bulletin-board');
                }
            }
        };

        /**
         * The data provenance menu item controller.
         */
        this.dataProvenance = {

            /**
             * The data provenance menu item's shell controller.
             */
            shell: {

                /**
                 * Launch the data provenance shell.
                 */
                launch: function () {
                    if (nf.Common.canAccessProvenance()) {
                        nf.Shell.showPage('provenance');
                    }
                }
            }
        };

        /**
         * The controller settings menu item controller.
         */
        this.controllerSettings = {

            /**
             * The controller settings menu item's shell controller.
             */
            shell: {

                /**
                 * Launch the settings shell.
                 */
                launch: function () {
                    if (nf.Common.canAccessController()) {
                        nf.Settings.showSettings();
                    }
                }
            }
        };

        /**
         * The cluster menu item controller.
         */
        this.cluster = {

            /**
             * Determines if the cluster menu item is enabled.
             *
             * @returns {*|boolean}
             */
            visible: function () {
                return nf.Canvas.isClustered();
            },

            /**
             * The cluster menu item's shell controller.
             */
            shell: {

                /**
                 * Launch the cluster shell.
                 */
                launch: function () {
                    if (nf.Common.canAccessController()) {
                        nf.Shell.showPage('cluster');
                    }
                }
            }
        };

        /**
         * The flow config history menu item controller.
         */
        this.flowConfigHistory = {

            /**
             * The flow config history menu item's shell controller.
             */
            shell: {

                /**
                 * Launch the history shell.
                 */
                launch: function () {
                    nf.Shell.showPage('history');
                }
            }
        };

        /**
         * The users menu item controller.
         */
        this.users = {

            /**
             * The users menu item's shell controller.
             */
            shell: {

                /**
                 * Launch the users shell.
                 */
                launch: function () {
                    if (nf.Common.canModifyTenants()) {
                        nf.Shell.showPage('users');
                    }
                }
            }
        };

        /**
         * The policies menu item controller.
         */
        this.policies = {

            /**
             * The policies menu item's shell controller.
             */
            shell: {

                /**
                 * Launch the policies shell.
                 */
                launch: function () {
                    if (nf.Common.canModifyPolicies() && nf.Common.canAccessTenants()) {
                        nf.PolicyManagement.showGlobalPolicies();
                    }
                }
            }
        };

        /**
         * The templates menu item controller.
         */
        this.templates = {

            /**
             * The templates menu item's shell controller.
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
        };

        /**
         * The help menu item controller.
         */
        this.help = {

            /**
             * The help menu item's shell controller.
             */
            shell: {

                /**
                 * Launch the help documentation shell.
                 */
                launch: function () {
                    nf.Shell.showPage(config.urls.helpDocument);
                }
            }
        };

        /**
         * The about menu item controller.
         */
        this.about = {

            /**
             * Initialize the about details.
             */
            init: function () {
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

                    // store the content viewer url if available
                    if (!nf.Common.isBlank(aboutDetails.contentViewerUrl)) {
                        $('#nifi-content-viewer-url').text(aboutDetails.contentViewerUrl);
                    }
                }).fail(nf.Common.handleAjaxError);

                this.modal.init();
            },

            /**
             * The about menu item's modal controller.
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

                    var resizeAbout = function(){
                        var dialog = $(this);
                        var top = $('#nf-about-pic-container').height() + $('.dialog-header').height() + 10; //10 for padding-top
                        dialog.find('.dialog-content').css('top', top);
                    };

                    this.getElement().modal({
                        scrollableContentStyle: 'scrollable',
                        headerText: 'About Apache NiFi',
                        handler: {
                          resize: resizeAbout
                        },
                        buttons: [{
                            buttonText: 'Ok',
                            color: {
                                base: '#728E9B',
                                hover: '#004849',
                                text: '#ffffff'
                            },
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
    }

    GlobalMenuCtrl.prototype = {
        constructor: GlobalMenuCtrl,

        /**
         * Initialize the global menu controls.
         */
        init: function () {
            this.about.init();
        }
    }

    var globalMenuCtrl = new GlobalMenuCtrl();
    return globalMenuCtrl;
};