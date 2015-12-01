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

/* global nf, top */

$(document).ready(function () {
    // initialize the status page
    nf.History.init();
});

nf.History = (function () {

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        urls: {
            banners: '../nifi-api/controller/banners',
            controllerAbout: '../nifi-api/controller/about',
            authorities: '../nifi-api/controller/authorities'
        }
    };

    /**
     * Loads the current users authorities.
     */
    var loadAuthorities = function () {
        // get the banners and update the page accordingly
        return $.Deferred(function (deferred) {
            $.ajax({
                type: 'GET',
                url: config.urls.authorities,
                dataType: 'json'
            }).done(function (response) {
                if (nf.Common.isDefinedAndNotNull(response.authorities)) {
                    // record the users authorities
                    nf.Common.setAuthorities(response.authorities);
                    deferred.resolve();
                } else {
                    deferred.reject();
                }
            }).fail(function (xhr, status, error) {
                nf.Common.handleAjaxError(xhr, status, error);
                deferred.reject();
            });
        }).promise();
    };

    /**
     * Initializes the history page.
     */
    var initializeHistoryPage = function () {
        // define mouse over event for the refresh button
        nf.Common.addHoverEffect('#refresh-button', 'button-refresh', 'button-refresh-hover').click(function () {
            nf.HistoryTable.loadHistoryTable();
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
                    if (nf.Common.isDefinedAndNotNull(response.banners)) {
                        if (nf.Common.isDefinedAndNotNull(response.banners.headerText) && response.banners.headerText !== '') {
                            // update the header text
                            var bannerHeader = $('#banner-header').text(response.banners.headerText).show();

                            // show the banner
                            var updateTop = function (elementId) {
                                var element = $('#' + elementId);
                                element.css('top', (parseInt(bannerHeader.css('height'), 10) + parseInt(element.css('top'), 10)) + 'px');
                            };

                            // update the position of elements affected by top banners
                            updateTop('history');
                        }

                        if (nf.Common.isDefinedAndNotNull(response.banners.footerText) && response.banners.footerText !== '') {
                            // update the footer text and show it
                            var bannerFooter = $('#banner-footer').text(response.banners.footerText).show();

                            var updateBottom = function (elementId) {
                                var element = $('#' + elementId);
                                element.css('bottom', parseInt(bannerFooter.css('height'), 10) + 'px');
                            };

                            // update the position of elements affected by bottom banners
                            updateBottom('history');
                        }
                    }

                    deferred.resolve();
                }).fail(function (xhr, status, error) {
                    nf.Common.handleAjaxError(xhr, status, error);
                    deferred.reject();
                });
            } else {
                deferred.resolve();
            }
        }).promise();
    };

    return {
        /**
         * Initializes the status page.
         */
        init: function () {
            nf.Storage.init();
            
            // load the users authorities
            loadAuthorities().done(function () {
                // create the history table
                nf.HistoryTable.init();

                // load the history table
                nf.HistoryTable.loadHistoryTable();

                // once the table is initialized, finish initializing the page
                initializeHistoryPage().done(function () {
                    // configure the initial grid height
                    nf.HistoryTable.resetTableSize();

                    // get the about details
                    $.ajax({
                        type: 'GET',
                        url: config.urls.controllerAbout,
                        dataType: 'json'
                    }).done(function (response) {
                        var aboutDetails = response.about;
                        var historyTitle = aboutDetails.title + ' History';

                        // set the document title and the about title
                        document.title = historyTitle;
                        $('#history-header-text').text(historyTitle);
                    }).fail(nf.Common.handleAjaxError);
                });
            });
        }
    };
}());