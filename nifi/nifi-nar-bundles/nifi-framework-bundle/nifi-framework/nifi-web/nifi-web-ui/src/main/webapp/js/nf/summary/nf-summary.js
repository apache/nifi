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
$(document).ready(function () {
    // initialize the summary page
    nf.Summary.init();
});

nf.Summary = (function () {

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        urls: {
            banners: '../nifi-api/controller/banners',
            controllerAbout: '../nifi-api/controller/about',
            cluster: '../nifi-api/cluster'
        }
    };

    /**
     * Initializes the summary table. Must first determine if we are running clustered.
     */
    var initializeSummaryTable = function () {
        return $.Deferred(function (deferred) {
            $.ajax({
                type: 'HEAD',
                url: config.urls.cluster
            }).done(function () {
                nf.SummaryTable.init(true).done(function () {
                    deferred.resolve();
                }).fail(function () {
                    deferred.reject();
                });
            }).fail(function (xhr, status, error) {
                if (xhr.status === 404) {
                    nf.SummaryTable.init(false).done(function () {
                        deferred.resolve();
                    }).fail(function () {
                        deferred.reject();
                    });
                } else {
                    nf.Common.handleAjaxError(xhr, status, error);
                    deferred.reject();
                }
            });
        }).promise();
    };

    /**
     * Initializes the summary page.
     */
    var initializeSummaryPage = function () {
        // define mouse over event for the refresh buttons
        nf.Common.addHoverEffect('#refresh-button', 'button-refresh', 'button-refresh-hover').click(function () {
            nf.SummaryTable.loadProcessorSummaryTable();
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
                            updateTop('summary');
                        }

                        if (nf.Common.isDefinedAndNotNull(response.banners.footerText) && response.banners.footerText !== '') {
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
            // intialize the summary table
            initializeSummaryTable().done(function () {
                // load the table
                nf.SummaryTable.loadProcessorSummaryTable().done(function () {
                    // once the table is initialized, finish initializing the page
                    initializeSummaryPage().done(function () {
                        // configure the initial grid height
                        nf.SummaryTable.resetTableSize();

                        // get the about details
                        $.ajax({
                            type: 'GET',
                            url: config.urls.controllerAbout,
                            dataType: 'json'
                        }).done(function (response) {
                            var aboutDetails = response.about;
                            var statusTitle = aboutDetails.title + ' Summary';

                            // set the document title and the about title
                            document.title = statusTitle;
                            $('#status-header-text').text(statusTitle);
                        }).fail(nf.Common.handleAjaxError);

                        var setBodySize = function () {
                            $('body').css({
                                'height': $(window).height() + 'px',
                                'width': $(window).width() + 'px'
                            });
                        };

                        // listen for browser resize events to reset the body size
                        $(window).resize(setBodySize);

                        // set the initial size
                        setBodySize();
                    });
                });
            });
        }
    };
}());