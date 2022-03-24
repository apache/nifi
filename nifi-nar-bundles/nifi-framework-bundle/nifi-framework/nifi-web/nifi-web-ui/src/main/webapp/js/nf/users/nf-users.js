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

/* global top, define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'nf.Common',
                'nf.Dialog',
                'nf.UsersTable',
                'nf.ErrorHandler',
                'nf.Storage',
                'nf.Client'],
            function ($, nfCommon, nfDialog, nfUsersTable, nfErrorHandler, nfStorage, nfClient) {
                return (nf.Users = factory($, nfCommon, nfDialog, nfUsersTable, nfErrorHandler, nfStorage, nfClient));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Users =
            factory(require('jquery'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.UsersTable'),
                require('nf.ErrorHandler'),
                require('nf.Storage'),
                require('nf.Client')));
    } else {
        nf.Users =
            factory(root.$,
                root.nf.Common,
                root.nf.Dialog,
                root.nf.UsersTable,
                root.nf.ErrorHandler,
                root.nf.Storage,
                root.nf.Client);
    }
}(this, function ($, nfCommon, nfDialog, nfUsersTable, nfErrorHandler, nfStorage, nfClient) {
    'use strict';

    $(document).ready(function () {
        // initialize the counters page
        nfUsers.init();

        //alter styles if we're not in the shell
        if (top === window) {
            $('#users').css('top', 20);
            $('#users-refresh-container').css('bottom', 20);
        }
    });

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        urls: {
            banners: '../nifi-api/flow/banners',
            controllerAbout: '../nifi-api/flow/about',
            currentUser: '../nifi-api/flow/current-user',
            flowConfig: '../nifi-api/flow/config'
        }
    };

    /**
     * Loads the current users authorities.
     */
    var ensureAccess = function () {
        return $.ajax({
            type: 'GET',
            url: config.urls.currentUser,
            dataType: 'json'
        }).done(function (currentUser) {
            nfCommon.setCurrentUser(currentUser);
        }).fail(nfErrorHandler.handleAjaxError);
    };

    var getFlowConfig = function () {
        return $.ajax({
            type: 'GET',
            url: config.urls.flowConfig,
            dataType: 'json'
        }).fail(nfErrorHandler.handleAjaxError);
    }

    /**
     * Verifies if the current node is disconnected from the cluster.
     */
    var verifyDisconnectedCluster = function () {
        return $.Deferred(function (deferred) {
            if (top !== window && nfCommon.isDefinedAndNotNull(parent.nf) && nfCommon.isDefinedAndNotNull(parent.nf.Storage)) {
                deferred.resolve(parent.nf.Storage.isDisconnectionAcknowledged());
            } else {
                $.ajax({
                    type: 'GET',
                    url: '../nifi-api/flow/cluster/summary',
                    dataType: 'json'
                }).done(function (clusterSummaryResult) {
                    var clusterSummaryResponse = clusterSummaryResult;
                    var clusterSummary = clusterSummaryResponse.clusterSummary;

                    if (clusterSummary.connectedToCluster) {
                        deferred.resolve(false);
                    } else {
                        if (clusterSummary.clustered) {
                            nfDialog.showDisconnectedFromClusterMessage(function () {
                                deferred.resolve(true);
                            });
                        } else {
                            deferred.resolve(false);
                        }
                    }
                }).fail(nfErrorHandler.handleAjaxError).fail(function () {
                    deferred.reject();
                });
            }
        }).promise();
    };

    var initializeUsersPage = function () {
        // define mouse over event for the refresh button
        nfCommon.addHoverEffect('#user-refresh-button', 'button-refresh', 'button-refresh-hover').click(function () {
            nfUsersTable.loadUsersTable();
        });

        // get the banners if we're not in the shell
        return $.Deferred(function (deferred) {
            if (top === window) {
                $.ajax({
                    type: 'GET',
                    url: config.urls.banners,
                    dataType: 'json'
                }).done(function (bannerResponse) {
                    // ensure the banners response is specified
                    if (nfCommon.isDefinedAndNotNull(bannerResponse.banners)) {
                        if (nfCommon.isDefinedAndNotNull(bannerResponse.banners.headerText) && bannerResponse.banners.headerText !== '') {
                            // update the header text
                            var bannerHeader = $('#banner-header').text(bannerResponse.banners.headerText).show();

                            // show the banner
                            var updateTop = function (elementId) {
                                var element = $('#' + elementId);
                                element.css('top', (parseInt(bannerHeader.css('height'), 10) + parseInt(element.css('top'), 10)) + 'px');
                            };

                            // update the position of elements affected by top banners
                            updateTop('users');
                        }

                        if (nfCommon.isDefinedAndNotNull(bannerResponse.banners.footerText) && bannerResponse.banners.footerText !== '') {
                            // update the footer text and show it
                            var bannerFooter = $('#banner-footer').text(bannerResponse.banners.footerText).show();

                            var updateBottom = function (elementId) {
                                var element = $('#' + elementId);
                                element.css('bottom', parseInt(bannerFooter.css('height'), 10) + 'px');
                            };

                            // update the position of elements affected by bottom banners
                            updateBottom('users');
                        }
                    }

                    deferred.resolve();
                }).fail(function (xhr, status, error) {
                    nfErrorHandler.handleAjaxError(xhr, status, error);
                    deferred.reject();
                });
            } else {
                deferred.resolve();
            }
        });
    };

    var nfUsers = {
        /**
         * Initializes the counters page.
         */
        init: function () {
            nfStorage.init();

            // initialize the client
            nfClient.init();

            // load the users authorities
            $.when(getFlowConfig(), verifyDisconnectedCluster(), ensureAccess()).done(function (configResult, verifyDisconnectedClusterResult) {
                var configResponse = configResult[0];
                var configDetails = configResponse.flowConfiguration;

                // create the counters table
                nfUsersTable.init(configDetails.supportsConfigurableUsersAndGroups, verifyDisconnectedClusterResult);

                // load the users table
                nfUsersTable.loadUsersTable().done(function () {
                    // finish initializing users page
                    initializeUsersPage().done(function () {
                        // listen for browser resize events to update the page size
                        $(window).resize(nfUsersTable.resetTableSize);

                        // configure the initial grid height
                        nfUsersTable.resetTableSize();

                        // get the about details
                        $.ajax({
                            type: 'GET',
                            url: config.urls.controllerAbout,
                            dataType: 'json'
                        }).done(function (response) {
                            var aboutDetails = response.about;
                            var countersTitle = aboutDetails.title + ' Users';

                            // set the document title and the about title
                            document.title = countersTitle;
                            $('#users-header-text').text(countersTitle);
                        }).fail(nfErrorHandler.handleAjaxError);
                    });

                    $(window).on('resize', function (e) {
                        // resize dialogs when appropriate
                        var dialogs = $('.dialog');
                        for (var i = 0, len = dialogs.length; i < len; i++) {
                            if ($(dialogs[i]).is(':visible')) {
                                setTimeout(function (dialog) {
                                    dialog.modal('resize');
                                }, 50, $(dialogs[i]));
                            }
                        }

                        // resize grids when appropriate
                        var gridElements = $('*[class*="slickgrid_"]');
                        for (var j = 0, len = gridElements.length; j < len; j++) {
                            if ($(gridElements[j]).is(':visible')) {
                                setTimeout(function (gridElement) {
                                    gridElement.data('gridInstance').resizeCanvas();
                                }, 50, $(gridElements[j]));
                            }
                        }
                    });
                });
            });
        }
    };

    return nfUsers;
}));