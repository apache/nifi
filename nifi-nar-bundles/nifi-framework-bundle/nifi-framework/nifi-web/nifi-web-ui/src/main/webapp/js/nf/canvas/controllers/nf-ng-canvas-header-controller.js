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

nf.ng.Canvas.HeaderCtrl = function (serviceProvider, toolboxCtrl, globalMenuCtrl) {
    'use strict';

    var MIN_TOOLBAR_WIDTH = 640;

    function HeaderCtrl(toolboxCtrl, globalMenuCtrl) {
        this.toolboxCtrl = toolboxCtrl;
        this.globalMenuCtrl = globalMenuCtrl;

        /**
         * The login link.
         */
        this.loginLink = {
            shell: {

                /**
                 * Launch the login shell.
                 */
                launch: function () {
                    nf.Shell.showPage('login', false);
                }
            }
        };

        /**
         * Logout.
         */
        this.logoutLink = {
            logout: function () {
                nf.Storage.removeItem("jwt");
                window.location = '/nifi';
            }
        };
    }
    HeaderCtrl.prototype = {
        constructor: HeaderCtrl,

        /**
         *  Register the header controller.
         */
        register: function() {
            if (serviceProvider.headerCtrl === undefined) {
                serviceProvider.register('headerCtrl', headerCtrl);
            }
        },

        /**
         * Initialize the canvas header.
         */
        init: function() {
            this.toolboxCtrl.init();
            this.globalMenuCtrl.init();

            // if the user is not anonymous or accessing via http
            if ($('#current-user').text() !== nf.Common.ANONYMOUS_USER_TEXT || location.protocol === 'http:') {
                $('#login-link-container').css('display', 'none');
            }

            // if accessing via http, don't show the current user
            if (location.protocol === 'http:') {
                $('#current-user-container').css('display', 'none');
            }
        },

        /**
         * Reloads and clears any warnings.
         */
        reloadAndClearWarnings: function() {
            nf.Canvas.reload().done(function () {
                // update component visibility
                nf.Canvas.View.updateVisibility();

                // refresh the birdseye
                nf.Birdseye.refresh();

                // hide the refresh link on the canvas
                $('#stats-last-refreshed').removeClass('alert');
                $('#refresh-required-container').hide();

                // hide the refresh link on the settings
                $('#settings-last-refreshed').removeClass('alert');
                $('#settings-refresh-required-icon').hide();
            }).fail(function () {
                nf.Dialog.showOkDialog({
                    dialogContent: 'Unable to refresh the current group.',
                    overlayBackground: true
                });
            });
        }
    }

    var headerCtrl = new HeaderCtrl(toolboxCtrl, globalMenuCtrl);
    headerCtrl.register();
    return headerCtrl;
};