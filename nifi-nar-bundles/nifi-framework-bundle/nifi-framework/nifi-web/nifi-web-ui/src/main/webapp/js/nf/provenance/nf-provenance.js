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
                'angular',
                'nf.Common',
                'nf.ng.AppConfig',
                'nf.ng.AppCtrl',
                'nf.ng.ProvenanceLineage',
                'nf.ng.ProvenanceTable',
                'nf.ng.Bridge',
                'nf.ErrorHandler',
                'nf.Storage'],
            function ($,
                      angular,
                      nfCommon,
                      appConfig,
                      appCtrl,
                      provenanceLineage,
                      provenanceTable,
                      nfNgBridge,
                      nfErrorHandler,
                      nfStorage) {
                return (nf.ng.Provenance =
                    factory($,
                        angular,
                        nfCommon,
                        appConfig,
                        appCtrl,
                        provenanceLineage,
                        provenanceTable,
                        nfNgBridge,
                        nfErrorHandler,
                        nfStorage));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.Provenance =
            factory(require('jquery'),
                require('angular'),
                require('nf.Common'),
                require('nf.ng.AppConfig'),
                require('nf.ng.AppCtrl'),
                require('nf.ng.ProvenanceLineage'),
                require('nf.ng.ProvenanceTable'),
                require('nf.ng.Bridge'),
                require('nf.ErrorHandler'),
                require('nf.Storage')));
    } else {
        nf.ng.Provenance = factory(root.$,
            root.angular,
            root.nf.Common,
            root.nf.ng.AppConfig,
            root.nf.ng.AppCtrl,
            root.nf.ng.ProvenanceLineage,
            root.nf.ng.ProvenanceTable,
            root.nf.ng.Bridge,
            root.nf.ErrorHandler,
            root.nf.Storage);
    }
}(this, function ($, angular, nfCommon, appConfig, appCtrl, provenanceLineage, provenanceTable, nfNgBridge, nfErrorHandler, nfStorage) {
    'use strict';

    $(document).ready(function () {
        //Create Angular App
        var app = angular.module('ngProvenanceApp', ['ngResource', 'ngRoute', 'ngMaterial', 'ngMessages']);

        //Define Dependency Injection Annotations
        appConfig.$inject = ['$mdThemingProvider', '$compileProvider'];
        appCtrl.$inject = ['$scope'];
        nfProvenance.$inject = ['provenanceTableCtrl'];
        provenanceLineage.$inject = [];
        provenanceTable.$inject = ['provenanceLineageCtrl'];

        //Configure Angular App
        app.config(appConfig);

        //Define Angular App Controllers
        app.controller('ngProvenanceAppCtrl', appCtrl);

        //Define Angular App Services
        app.service('provenanceCtrl', nfProvenance);
        app.service('provenanceLineageCtrl', provenanceLineage);
        app.service('provenanceTableCtrl', provenanceTable);

        //Manually Boostrap Angular App
        nfNgBridge.injector = angular.bootstrap($('body'), ['ngProvenanceApp'], {strictDi: true});

        // initialize the status page
        nfNgBridge.injector.get('provenanceCtrl').init();
    });

    var nfProvenance = function (provenanceTableCtrl) {

        /**
         * Configuration object used to hold a number of configuration items.
         */
        var config = {
            urls: {
                clusterSummary: '../nifi-api/flow/cluster/summary',
                banners: '../nifi-api/flow/banners',
                about: '../nifi-api/flow/about',
                currentUser: '../nifi-api/flow/current-user'
            }
        };

        /**
         * Whether or not this NiFi is clustered.
         */
        var isClustered = null;

        /**
         * Determines if this NiFi is clustered.
         */
        var detectedCluster = function () {
            return $.ajax({
                type: 'GET',
                url: config.urls.clusterSummary
            }).done(function (response) {
                isClustered = response.clusterSummary.connectedToCluster;
            }).fail(nfErrorHandler.handleAjaxError);
        };

        /**
         * Loads the controller configuration.
         */
        var loadAbout = function () {
            // get the about details
            return $.ajax({
                type: 'GET',
                url: config.urls.about,
                dataType: 'json'
            }).done(function (response) {
                var aboutDetails = response.about;
                var provenanceTitle = aboutDetails.title + ' Data Provenance';

                // store the controller name
                $('#nifi-controller-uri').text(aboutDetails.uri);

                // set the timezone for the start and end time
                $('.timezone').text(aboutDetails.timezone);

                // store the content viewer url if available
                if (!nfCommon.isBlank(aboutDetails.contentViewerUrl)) {
                    $('#nifi-content-viewer-url').text(aboutDetails.contentViewerUrl);
                }

                // set the document title and the about title
                document.title = provenanceTitle;
                $('#provenance-header-text').text(provenanceTitle);
            }).fail(nfErrorHandler.handleAjaxError);
        };

        /**
         * Loads the current user.
         */
        var loadCurrentUser = function () {
            return $.ajax({
                type: 'GET',
                url: config.urls.currentUser,
                dataType: 'json'
            }).done(function (currentUser) {
                nfCommon.setCurrentUser(currentUser);
            }).fail(nfErrorHandler.handleAjaxError);
        };

        /**
         * Initializes the provenance page.
         */
        var initializeProvenancePage = function () {
            // define mouse over event for the refresh button
            $('#refresh-button').click(function () {
                provenanceTableCtrl.loadProvenanceTable();
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
                                updateTop('provenance');
                            }

                            if (nfCommon.isDefinedAndNotNull(response.banners.footerText) && response.banners.footerText !== '') {
                                // update the footer text and show it
                                var bannerFooter = $('#banner-footer').text(response.banners.footerText).show();

                                var updateBottom = function (elementId) {
                                    var element = $('#' + elementId);
                                    element.css('bottom', parseInt(bannerFooter.css('height'), 10) + 'px');
                                };

                                // update the position of elements affected by bottom banners
                                updateBottom('provenance');
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

        function ProvenanceCtrl() {
        }

        ProvenanceCtrl.prototype = {
            constructor: ProvenanceCtrl,

            /**
             * Initializes the status page.
             */
            init: function () {
                nfStorage.init();

                // load the user and detect if the NiFi is clustered
                $.when(loadAbout(), loadCurrentUser(), detectedCluster()).done(function () {
                    // create the provenance table
                    provenanceTableCtrl.init(isClustered).done(function () {
                        var searchTerms = {};

                        // look for a processor id in the query search
                        var initialComponentId = $('#initial-component-query').text();
                        if ($.trim(initialComponentId) !== '') {
                            // populate initial search component
                            $('input.searchable-component-id').val(initialComponentId);

                            // build the search criteria
                            searchTerms['ProcessorID'] = initialComponentId;
                        }

                        // look for a flowfile uuid in the query search
                        var initialFlowFileUuid = $('#initial-flowfile-query').text();
                        if ($.trim(initialFlowFileUuid) !== '') {
                            // populate initial search component
                            $('input.searchable-flowfile-uuid').val(initialFlowFileUuid);

                            // build the search criteria
                            searchTerms['FlowFileUUID'] = initialFlowFileUuid;
                        }

                        // load the provenance table
                        if ($.isEmptyObject(searchTerms)) {
                            // load the provenance table
                            provenanceTableCtrl.loadProvenanceTable();
                        } else {
                            // load the provenance table
                            provenanceTableCtrl.loadProvenanceTable({
                                'searchTerms': searchTerms
                            });
                        }

                        var setBodySize = function () {
                            //alter styles if we're not in the shell
                            if (top === window) {
                                $('body').css({
                                    'height': $(window).height() + 'px',
                                    'width': $(window).width() + 'px'
                                });

                                $('#provenance').css('margin', 40);
                                $('#provenance-refresh-container').css({
                                    'bottom': '0px',
                                    'left': '0px',
                                    'right': '0px'
                                });
                            }

                            // configure the initial grid height
                            provenanceTableCtrl.resetTableSize();
                        };

                        // once the table is initialized, finish initializing the page
                        initializeProvenancePage().done(function () {
                            // set the initial size
                            setBodySize();
                        });

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
            }
        }

        var provenanceCtrl = new ProvenanceCtrl();
        return provenanceCtrl;
    }

    return nfProvenance;
}));