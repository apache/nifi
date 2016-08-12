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
    // initialize the counters page
    nf.Cluster.init();
});

nf.Cluster = (function () {

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        urls: {
            banners: '../nifi-api/flow/banners',
            about: '../nifi-api/flow/about',
            currentUser: '../nifi-api/flow/current-user'
        }
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
            nf.Common.setCurrentUser(currentUser);
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Initializes the cluster page.
     */
    var initializeClusterPage = function () {
        // define mouse over event for the refresh button
        $('#refresh-button').click(function () {
            nf.ClusterTable.loadClusterTable();
        });

        // return a deferred for page initialization
        return $.Deferred(function (deferred) {
            // get the banners if we're not in the shell
            if (top === window) {
                $.ajax({
                    type: 'GET',
                    url: config.urls.banners,
                    dataType: 'json'
                }).done(function (bannerResponse) {
                    // ensure the banners response is specified
                    if (nf.Common.isDefinedAndNotNull(bannerResponse.banners)) {
                        if (nf.Common.isDefinedAndNotNull(bannerResponse.banners.headerText) && bannerResponse.banners.headerText !== '') {
                            // update the header text
                            var bannerHeader = $('#banner-header').text(bannerResponse.banners.headerText).show();

                            // show the banner
                            var updateTop = function (elementId) {
                                var element = $('#' + elementId);
                                element.css('top', (parseInt(bannerHeader.css('height'), 10) + parseInt(element.css('top'), 10)) + 'px');
                            };

                            // update the position of elements affected by top banners
                            updateTop('counters');
                        }

                        if (nf.Common.isDefinedAndNotNull(bannerResponse.banners.footerText) && bannerResponse.banners.footerText !== '') {
                            // update the footer text and show it
                            var bannerFooter = $('#banner-footer').text(bannerResponse.banners.footerText).show();

                            var updateBottom = function (elementId) {
                                var element = $('#' + elementId);
                                element.css('bottom', parseInt(bannerFooter.css('height'), 10) + 'px');
                            };

                            // update the position of elements affected by bottom banners
                            updateBottom('counters');
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
         * Initializes the counters page.
         */
        init: function () {
            nf.Storage.init();
            
            // load the current user
            loadCurrentUser().done(function () {
                var setBodySize = function () {
                    //alter styles if we're not in the shell
                    if (top === window) {
                        $('body').css({
                            'height': $(window).height() + 'px',
                            'width': $(window).width() + 'px'
                        });

                        $('#cluster').css('margin', 40);
                        $('#cluster-table').css('bottom', 127);
                        $('#cluster-refresh-container').css('margin', 40);
                    }

                    // configure the initial grid height
                    nf.ClusterTable.resetTableSize();
                };

                // set the initial size
                setBodySize();

                // create the counters table
                nf.ClusterTable.init();

                // load the table
                nf.ClusterTable.loadClusterTable().done(function () {
                    // once the table is initialized, finish initializing the page
                    initializeClusterPage().done(function () {

                        // get the about details
                        $.ajax({
                            type: 'GET',
                            url: config.urls.about,
                            dataType: 'json'
                        }).done(function (response) {
                            var aboutDetails = response.about;
                            var countersTitle = aboutDetails.title + ' Cluster';

                            // set the document title and the about title
                            document.title = countersTitle;
                            $('#counters-header-text').text(countersTitle);
                        }).fail(nf.Common.handleAjaxError);

                        $(window).on('resize', function (e) {
                            setBodySize();
                            // resize dialogs when appropriate
                            var dialogs = $('.dialog');
                            for (var i = 0, len = dialogs.length; i < len; i++) {
                                if ($(dialogs[i]).is(':visible')){
                                    setTimeout(function(dialog){
                                        dialog.modal('resize');
                                    }, 50, $(dialogs[i]));
                                }
                            }

                            // resize grids when appropriate
                            var gridElements = $('*[class*="slickgrid_"]');
                            for (var j = 0, len = gridElements.length; j < len; j++) {
                                if ($(gridElements[j]).is(':visible')){
                                    setTimeout(function(gridElement){
                                        gridElement.data('gridInstance').resizeCanvas();
                                    }, 50, $(gridElements[j]));
                                }
                            }

                            // toggle tabs .scrollable when appropriate
                            var tabsContainers = $('.tab-container');
                            var tabsContents = [];
                            for (var k = 0, len = tabsContainers.length; k < len; k++) {
                                if ($(tabsContainers[k]).is(':visible')){
                                    tabsContents.push($('#' + $(tabsContainers[k]).attr('id') + '-content'));
                                }
                            }
                            $.each(tabsContents, function (index, tabsContent) {
                                nf.Common.toggleScrollable(tabsContent.get(0));
                            });
                        });
                    });
                });
            });
        }
    };
}());