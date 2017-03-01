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
                'angular',
                'nf.Common',
                'nf.ClusterSummary',
                'nf.ClusterSearch',
                'nf.ng.AppConfig',
                'nf.ng.AppCtrl',
                'nf.ng.ServiceProvider',
                'nf.ng.Bridge',
                'nf.ErrorHandler',
                'nf.Storage',
                'nf.SummaryTable'],
            function ($,
                      angular,
                      nfCommon,
                      nfClusterSummary,
                      nfClusterSearch,
                      appConfig,
                      appCtrl,
                      serviceProvider,
                      provenanceTable,
                      nfNgBridge,
                      nfErrorHandler,
                      nfStorage,
                      nfSummaryTable) {
                return (nf.Summary =
                    factory($,
                        angular,
                        nfCommon,
                        nfClusterSummary,
                        nfClusterSearch,
                        appConfig,
                        appCtrl,
                        serviceProvider,
                        nfNgBridge,
                        nfErrorHandler,
                        nfStorage,
                        nfSummaryTable));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Summary =
            factory(require('jquery'),
                require('angular'),
                require('nf.Common'),
                require('nf.ClusterSummary'),
                require('nf.ClusterSearch'),
                require('nf.ng.AppConfig'),
                require('nf.ng.AppCtrl'),
                require('nf.ng.ServiceProvider'),
                require('nf.ng.Bridge'),
                require('nf.ErrorHandler'),
                require('nf.Storage'),
                require('nf.SummaryTable')));
    } else {
        nf.Summary = factory(root.$,
            root.angular,
            root.nf.Common,
            root.nf.ClusterSummary,
            root.nf.ClusterSearch,
            root.nf.ng.AppConfig,
            root.nf.ng.AppCtrl,
            root.nf.ng.ServiceProvider,
            root.nf.ng.Bridge,
            root.nf.ErrorHandler,
            root.nf.Storage,
            root.nf.SummaryTable);
    }
}(this, function ($, angular, nfCommon, nfClusterSummary, nfClusterSearch, appConfig, appCtrl, serviceProvider, nfNgBridge, nfErrorHandler, nfStorage, nfSummaryTable) {
    'use strict';

    $(document).ready(function () {
        //Create Angular App
        var app = angular.module('ngSummaryApp', ['ngResource', 'ngRoute', 'ngMaterial', 'ngMessages']);

        //Define Dependency Injection Annotations
        appConfig.$inject = ['$mdThemingProvider', '$compileProvider'];
        appCtrl.$inject = ['$scope', 'serviceProvider'];
        serviceProvider.$inject = [];

        //Configure Angular App
        app.config(appConfig);

        //Define Angular App Controllers
        app.controller('ngSummaryAppCtrl', appCtrl);

        //Define Angular App Services
        app.service('serviceProvider', serviceProvider);

        //Manually Boostrap Angular App
        nfNgBridge.injector = angular.bootstrap($('body'), ['ngSummaryApp'], {strictDi: true});

        // initialize the summary page
        nfClusterSummary.loadClusterSummary().done(function () {
            nfSummary.init();
        });
    });

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        urls: {
            banners: '../nifi-api/flow/banners',
            about: '../nifi-api/flow/about',
            clusterSummary: '../nifi-api/flow/cluster/summary'
        }
    };

    /**
     * Initializes the summary table. Must first determine if we are running clustered.
     */
    var initializeSummaryTable = function () {
        return $.Deferred(function (deferred) {
            $.ajax({
                type: 'GET',
                url: config.urls.clusterSummary
            }).done(function (response) {
                nfSummaryTable.init(response.clusterSummary.connectedToCluster).done(function () {
                    // initialize the search field if applicable
                    if (response.clusterSummary.connectedToCluster) {
                        nfClusterSearch.init();
                    }
                    deferred.resolve();
                }).fail(function () {
                    deferred.reject();
                });
            }).fail(nfErrorHandler.handleAjaxError);
        }).promise();
    };

    /**
     * Initializes the summary page.
     */
    var initializeSummaryPage = function () {
        // define mouse over event for the refresh buttons
        $('#refresh-button').click(function () {
            nfClusterSummary.loadClusterSummary().done(function () {
                nfSummaryTable.loadSummaryTable();
            });
        });

        // return a deferred for page initialization
        return $.Deferred(function (deferred) {
            // get the banners if we're not in the shell
            if (top === window) {
                $.ajax({
                    type: 'GET',
                    url: config.urls.banners,
                    dataType: 'json'
                }).done(function (response) {
                    // ensure the banners response is specified
                    if (nfCommon.isDefinedAndNotNull(response.banners)) {
                        if (nfCommon.isDefinedAndNotNull(response.banners.headerText) && response.banners.headerText !== '') {
                            // update the header text
                            var bannerHeader = $('#banner-header').text(response.banners.headerText).show();

                            // show the banner
                            var updateTop = function (elementId) {
                                var element = $('#' + elementId);
                                element.css('top', (parseInt(bannerHeader.css('height'), 10) + parseInt(element.css('top'), 10)) + 'px');
                            };

                            // update the position of elements affected by top banners
                            updateTop('summary');
                        }

                        if (nfCommon.isDefinedAndNotNull(response.banners.footerText) && response.banners.footerText !== '') {
                            // update the footer text and show it
                            var bannerFooter = $('#banner-footer').text(response.banners.footerText).show();

                            var updateBottom = function (elementId) {
                                var element = $('#' + elementId);
                                element.css('bottom', parseInt(bannerFooter.css('height'), 10) + 'px');
                            };

                            // update the position of elements affected by bottom banners
                            updateBottom('summary');
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
        }).promise();
    };

    var nfSummary = {
        /**
         * Initializes the status page.
         */
        init: function () {
            nfStorage.init();

            // intialize the summary table
            initializeSummaryTable().done(function () {
                // load the table
                nfSummaryTable.loadSummaryTable().done(function () {
                    // once the table is initialized, finish initializing the page
                    initializeSummaryPage().done(function () {

                        var setBodySize = function () {
                            //alter styles if we're not in the shell
                            if (top === window) {
                                $('body').css({
                                    'height': $(window).height() + 'px',
                                    'width': $(window).width() + 'px'
                                });

                                $('#summary').css('margin', 40);
                                $('div.summary-table').css('bottom', 127);
                                $('#flow-summary-refresh-container').css({
                                    "position": "absolute",
                                    "bottom": "0px",
                                    "margin": "40px"
                                });
                            }

                            nfSummaryTable.resetTableSize();
                        };

                        // get the about details
                        $.ajax({
                            type: 'GET',
                            url: config.urls.about,
                            dataType: 'json'
                        }).done(function (response) {
                            var aboutDetails = response.about;
                            var statusTitle = aboutDetails.title + ' Summary';

                            // set the document title and the about title
                            document.title = statusTitle;
                            $('#status-header-text').text(statusTitle);

                            // set the initial size
                            setBodySize();
                        }).fail(nfErrorHandler.handleAjaxError);

                        $(window).on('resize', function (e) {
                            setBodySize();
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

                            // toggle tabs .scrollable when appropriate
                            var tabsContainers = $('.tab-container');
                            var tabsContents = [];
                            for (var k = 0, len = tabsContainers.length; k < len; k++) {
                                if ($(tabsContainers[k]).is(':visible')) {
                                    tabsContents.push($('#' + $(tabsContainers[k]).attr('id') + '-content'));
                                }
                            }
                            $.each(tabsContents, function (index, tabsContent) {
                                nfCommon.toggleScrollable(tabsContent.get(0));
                            });
                        });
                    });
                });
            });
        }
    };

    return nfSummary;
}));