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
                'nf.QueueListing',
                'nf.Shell',
                'nf.PolicyManagement',
                'nf.ClusterSummary',
                'nf.ErrorHandler',
                'nf.Settings',
                'nf.CanvasUtils'],
            function ($, nfCommon, nfQueueListing, nfShell, nfPolicyManagement, nfClusterSummary, nfErrorHandler, nfSettings, nfCanvasUtils) {
                return (nf.ng.Canvas.GlobalMenuCtrl = factory($, nfCommon, nfQueueListing, nfShell, nfPolicyManagement, nfClusterSummary, nfErrorHandler, nfSettings, nfCanvasUtils));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.Canvas.GlobalMenuCtrl =
            factory(require('jquery'),
                require('nf.Common'),
                require('nf.QueueListing'),
                require('nf.Shell'),
                require('nf.PolicyManagement'),
                require('nf.ClusterSummary'),
                require('nf.ErrorHandler'),
                require('nf.Settings'),
                require('nf.CanvasUtils')));
    } else {
        nf.ng.Canvas.GlobalMenuCtrl = factory(root.$,
            root.nf.Common,
            root.nf.QueueListing,
            root.nf.Shell,
            root.nf.PolicyManagement,
            root.nf.ClusterSummary,
            root.nf.ErrorHandler,
            root.nf.Settings,
            root.nf.CanvasUtils);
    }
}(this, function ($, nfCommon, nfQueueListing, nfShell, nfPolicyManagement, nfClusterSummary, nfErrorHandler, nfSettings, nfCanvasUtils) {
    'use strict';

    return function (serviceProvider) {
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
                        nfShell.showPage('summary');
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
                        if (nfCommon.canAccessCounters()) {
                            nfShell.showPage('counters');
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
                        nfShell.showPage('bulletin-board');
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
                        if (nfCommon.canAccessProvenance()) {
                            nfShell.showPage('provenance');
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
                        nfSettings.showSettings();
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
                    return nfClusterSummary.isConnectedToCluster();
                },

                /**
                 * The cluster menu item's shell controller.
                 */
                shell: {

                    /**
                     * Launch the cluster shell.
                     */
                    launch: function () {
                        if (nfCommon.canAccessController()) {
                            nfShell.showPage('cluster');
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
                        nfShell.showPage('history');
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
                        if (nfCommon.canAccessTenants()) {
                            nfShell.showPage('users');
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
                        if (nfCommon.canModifyPolicies() && nfCommon.canAccessTenants()) {
                            nfPolicyManagement.showGlobalPolicies();
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
                        nfShell.showPage('templates?' + $.param({
                                groupId: nfCanvasUtils.getGroupId()
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
                        nfShell.showPage(config.urls.helpDocument);
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
                        var showVersionDetail = false;
                        if (aboutDetails.buildTag && aboutDetails.buildTag !== 'HEAD') {
                            $('#nf-about-build-tag').text(aboutDetails.buildTag);
                            $('#nf-version-detail-tag').show();
                            showVersionDetail = true;
                        }
                        if (aboutDetails.buildRevision) {
                            $('#nf-about-build-revision').text(aboutDetails.buildRevision);
                            $('#nf-about-build-branch').text(aboutDetails.buildBranch);
                            $('#nf-version-detail-commit').show();
                            showVersionDetail = true
                        }
                        if (aboutDetails.buildTimestamp) {
                            $('#nf-about-build-timestamp').text(aboutDetails.buildTimestamp);
                            $('#nf-version-detail-timestamp').show();
                            showVersionDetail = true;
                        }
                        if (showVersionDetail) {
                            $('#nf-version-detail').show();
                        }

                        // store the content viewer url if available
                        if (!nfCommon.isBlank(aboutDetails.contentViewerUrl)) {
                            $('#nifi-content-viewer-url').text(aboutDetails.contentViewerUrl);
                            nfQueueListing.initFlowFileDetailsDialog();
                        }
                    }).fail(nfErrorHandler.handleAjaxError);

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
                        var aboutModal = this;

                        var resizeAbout = function () {
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
                                        aboutModal.hide();
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
}));