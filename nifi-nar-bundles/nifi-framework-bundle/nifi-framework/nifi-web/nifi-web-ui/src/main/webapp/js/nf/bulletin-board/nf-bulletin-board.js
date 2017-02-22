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
                'nf.ng.AppConfig',
                'nf.ng.AppCtrl',
                'nf.ng.ServiceProvider',
                'nf.ng.Bridge',
                'nf.ErrorHandler',
                'nf.Storage'],
            function ($,
                      angular,
                      nfCommon,
                      appConfig,
                      appCtrl,
                      serviceProvider,
                      nfNgBridge,
                      nfErrorHandler,
                      nfStorage) {
                return (nf.ng.BulletinBoardCtrl =
                    factory($,
                        angular,
                        nfCommon,
                        appConfig,
                        appCtrl,
                        serviceProvider,
                        nfNgBridge,
                        nfErrorHandler,
                        nfStorage));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.BulletinBoardCtrl =
            factory(require('jquery'),
                require('angular'),
                require('nf.Common'),
                require('nf.ng.AppConfig'),
                require('nf.ng.AppCtrl'),
                require('nf.ng.ServiceProvider'),
                require('nf.ng.Bridge'),
                require('nf.ErrorHandler'),
                require('nf.Storage')));
    } else {
        nf.ng.BulletinBoardCtrl = factory(root.$,
            root.angular,
            root.nf.Common,
            root.nf.ng.AppConfig,
            root.nf.ng.AppCtrl,
            root.nf.ng.ServiceProvider,
            root.nf.ng.Bridge,
            root.nf.ErrorHandler,
            root.nf.Storage);
    }
}(this, function ($, angular, nfCommon, appConfig, appCtrl, serviceProvider, nfNgBridge, nfErrorHandler, nfStorage) {
    'use strict';

    $(document).ready(function () {

        //Create Angular App
        var app = angular.module('ngBulletinBoardApp', ['ngResource', 'ngRoute', 'ngMaterial', 'ngMessages']);

        //Define Dependency Injection Annotations
        appConfig.$inject = ['$mdThemingProvider', '$compileProvider'];
        appCtrl.$inject = ['$scope', 'serviceProvider', 'bulletinBoardCtrl'];
        nfBulletinBoard.$inject = ['serviceProvider'];
        serviceProvider.$inject = [];

        //Configure Angular App
        app.config(appConfig);

        //Define Angular App Controllers
        app.controller('ngBulletinBoardAppCtrl', appCtrl);

        //Define Angular App Services
        app.service('serviceProvider', serviceProvider);
        app.service('bulletinBoardCtrl', nfBulletinBoard);

        //Manually Boostrap Angular App
        nfNgBridge.injector = angular.bootstrap($('body'), ['ngBulletinBoardApp'], {strictDi: true});

        // initialize the bulletin board
        nfNgBridge.injector.get('bulletinBoardCtrl').init();
    });

    var nfBulletinBoard = function (serviceProvider) {

        /**
         * Configuration object used to hold a number of configuration items.
         */
        var config = {
            pollInterval: 3,
            maxBulletins: 1000,
            urls: {
                banners: '../nifi-api/flow/banners',
                about: '../nifi-api/flow/about',
                bulletinBoard: '../nifi-api/flow/bulletin-board'
            }
        };

        var lastBulletin = null;
        var filterText = null;
        var filterType = null;

        /**
         * Initializes the bulletin board.
         */
        var initializePage = function () {
            // add hover effect and click handler for clearing the bulletins
            $('#clear-bulletins-button').click(function () {
                $('#bulletin-board-container').empty();
            });

            // filter type
            $('#bulletin-board-filter-type').combo({
                options: [{
                    text: 'by message',
                    value: 'message'
                }, {
                    text: 'by name',
                    value: 'sourceName'
                }, {
                    text: 'by id',
                    value: 'sourceId'
                }, {
                    text: 'by group id',
                    value: 'groupId'
                }]
            });

            // get the about details
            var getTitle = $.ajax({
                type: 'GET',
                url: config.urls.about,
                dataType: 'json'
            }).done(function (response) {
                var aboutDetails = response.about;
                var bulletinBoardTitle = aboutDetails.title + ' Bulletin Board';

                // set the document title and the about title
                document.title = bulletinBoardTitle;
                $('#bulletin-board-header-text').text(bulletinBoardTitle);
            }).fail(nfErrorHandler.handleAjaxError);

            // get the banners if we're not in the shell
            var loadBanners = $.Deferred(function (deferred) {
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
                                updateTop('bulletin-board');
                            }

                            if (nfCommon.isDefinedAndNotNull(response.banners.footerText) && response.banners.footerText !== '') {
                                // update the footer text and show it
                                var bannerFooter = $('#banner-footer').text(response.banners.footerText).show();

                                var updateBottom = function (elementId) {
                                    var element = $('#' + elementId);
                                    element.css('bottom', parseInt(bannerFooter.css('height'), 10) + 'px');
                                };

                                // update the position of elements affected by bottom banners
                                updateBottom('bulletin-board');
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

            return $.Deferred(function (deferred) {
                $.when(getTitle, loadBanners).done(function () {
                    deferred.resolve();
                }).fail(function () {
                    deferred.reject();
                });
            }).promise();
        };

        /**
         * Starts polling for new bulletins.
         */
        var start = function () {
            var refreshButton = $('#refresh-button');
            var bulletinContainer = $('#bulletin-board-container');

            appendAndScroll(bulletinContainer, '<div class="bulletin-action">Auto refresh started</div>');

            // clear any error messages
            $('#bulletin-error-message').text('').hide();
            poll();
        };

        /**
         * Stops polling for new bulletins.
         */
        var stop = function () {
            var refreshButton = $('#refresh-button');
            var bulletinContainer = $('#bulletin-board-container');

            appendAndScroll(bulletinContainer, '<div class="bulletin-action">Auto refresh stopped</div>');

            // reset state
            lastBulletin = null;
            filterText = null;
            filterType = null;
        };

        /**
         * Polls for new bulletins
         */
        var poll = function () {
            // if the page is no longer open, stop polling
            var isOpen = $('#bulletin-board').is(':visible');
            if (!isOpen) {
                bulletinBoardCtrl.polling = false;
            }

            // if polling, reload the bulletins
            if (bulletinBoardCtrl.polling) {
                bulletinBoardCtrl.loadBulletins().done(function () {
                    if (bulletinBoardCtrl.polling) {
                        setTimeout(poll, config.pollInterval * 1000);
                    }
                });
            }
        };

        /**
         * Appends the specified string to the specified container and scrolls to the bottom.
         *
         * @argument {jQuery} bulletinContainer     The container for the bulletins
         * @argument {string} content               The content to added to the bulletin container
         */
        var appendAndScroll = function (bulletinContainer, content) {
            bulletinContainer.append(content).animate({scrollTop: bulletinContainer[0].scrollHeight}, 'slow');
        };

        /**
         * Goes to the specified source on the graph.
         *
         * @argument {string} groupId   The id of the group
         * @argument {string} sourceId  The id of the source component
         */
        var goToSource = function (groupId, sourceId) {
            // only attempt this if we're within a frame
            if (top !== window) {
                // and our parent has canvas utils and shell defined
                if (nfCommon.isDefinedAndNotNull(parent.nf) && nfCommon.isDefinedAndNotNull(parent.nf.CanvasUtils) && nfCommon.isDefinedAndNotNull(parent.nf.Shell)) {
                    parent.nf.CanvasUtils.showComponent(groupId, sourceId);
                    parent.$('#shell-close-button').click();
                }
            }
        };

        function BulletinBoardCtrl() {
            /**
             * Toggle state
             */
            this.polling = true;
        }

        BulletinBoardCtrl.prototype = {
            constructor: BulletinBoardCtrl,

            /**
             *  Register the bulletin board controller.
             */
            register: function () {
                if (serviceProvider.bulletinBoardCtrl === undefined) {
                    serviceProvider.register('bulletinBoardCtrl', bulletinBoardCtrl);
                }
            },

            /**
             * Initializes the bulletin board page.
             */
            init: function () {
                //alter styles if we're not in the shell
                if (top === window) {
                    $('#bulletin-board').css({
                        "position": "absolute",
                        "bottom": "40px",
                        "left": "40px",
                        "right": "40px",
                        "top": "40px"
                    });
                    $('#bulletin-board-refresh-container').css({
                        "position": "absolute",
                        "bottom": "40px",
                        "left": "40px",
                        "right": "40px"
                    });
                }

                nfStorage.init();

                initializePage().done(function () {
                    start();
                });
            },

            /**
             * Loads the bulletins since the last refresh.
             */
            loadBulletins: function () {
                var data = {};

                // include the timestamp if appropriate
                if (nfCommon.isDefinedAndNotNull(lastBulletin)) {
                    data['after'] = lastBulletin;
                } else {
                    data['limit'] = 10;
                }

                var bulletinContainer = $('#bulletin-board-container');

                // get the filter text
                var filter = $('#bulletin-board-filter').val();
                if (filter !== '') {
                    // determine which field to filter on
                    var filterOption = $('#bulletin-board-filter-type').combo('getSelectedOption');
                    data[filterOption.value] = filter;

                    // append filtering message if necessary
                    if (filterText !== filter || filterType !== filterOption.text) {
                        var filterContent = $('<div class="bulletin-action"></div>').text('Filter ' + filterOption.text + ' matching \'' + filter + '\'');
                        appendAndScroll(bulletinContainer, filterContent.get(0));
                        filterText = filter;
                        filterType = filterOption.text;
                    }
                } else if (filterText !== null) {
                    appendAndScroll(bulletinContainer, '<div class="bulletin-action">Filter removed</div>');
                    filterText = null;
                    filterType = null;
                }

                return $.ajax({
                    type: 'GET',
                    url: config.urls.bulletinBoard,
                    data: data,
                    dataType: 'json'
                }).done(function (response) {
                    // ensure the bulletin board was specified
                    if (nfCommon.isDefinedAndNotNull(response.bulletinBoard)) {
                        var bulletinBoard = response.bulletinBoard;

                        // update the stats last refreshed timestamp
                        $('#bulletin-board-last-refreshed').text(bulletinBoard.generated);

                        // process the bulletins
                        var bulletinEntities = response.bulletinBoard.bulletins;
                        var content = [];

                        // append each bulletin
                        $.each(bulletinEntities, function (i, bulletinEntity) {
                            if (bulletinEntity.canRead === true) {
                                var bulletin = bulletinEntity.bulletin;

                                // format the severity
                                var severityStyle = 'bulletin-normal';
                                if (bulletin.level === 'ERROR') {
                                    severityStyle = 'bulletin-error';
                                } else if (bulletin.level === 'WARN' || bulletin.level === 'WARNING') {
                                    severityStyle = 'bulletin-warn';
                                }

                                // format the source id
                                var source;
                                if (nfCommon.isDefinedAndNotNull(bulletin.sourceId) && nfCommon.isDefinedAndNotNull(bulletin.groupId) && top !== window) {
                                    source = $('<div class="bulletin-source bulletin-link"></div>').text(bulletin.sourceId).on('click', function () {
                                        goToSource(bulletin.groupId, bulletin.sourceId);
                                    });
                                } else {
                                    var sourceId = bulletin.sourceId;
                                    if (nfCommon.isUndefined(sourceId) || nfCommon.isNull(sourceId)) {
                                        sourceId = '';
                                    }
                                    source = $('<div class="bulletin-source"></div>').text(sourceId);
                                }

                                // build the markup for this bulletin
                                var bulletinMarkup = $('<div class="bulletin"></div>');

                                // build the markup for this bulletins info
                                var bulletinInfoMarkup = $('<div class="bulletin-info"></div>').appendTo(bulletinMarkup);
                                $('<div class="bulletin-timestamp"></div>').text(bulletin.timestamp).appendTo(bulletinInfoMarkup);
                                $('<div class="bulletin-severity"></div>').addClass(severityStyle).text(bulletin.level).appendTo(bulletinInfoMarkup);
                                source.appendTo(bulletinInfoMarkup);
                                $('<div class="clear"></div>').appendTo(bulletinInfoMarkup);

                                // format the node address if applicable
                                if (nfCommon.isDefinedAndNotNull(bulletin.nodeAddress)) {
                                    $('<div class="bulletin-node"></div>').text(bulletin.nodeAddress).appendTo(bulletinMarkup);
                                }

                                // add the bulletin message (treat as text)
                                $('<pre class="bulletin-message"></pre>').text(bulletin.message).appendTo(bulletinMarkup);
                                $('<div class="clear"></div>').appendTo(bulletinMarkup);

                                // append the content
                                content.push(bulletinMarkup.get(0));
                            }

                            // record the id of the last bulletin in this request
                            if (i + 1 === bulletinEntities.length) {
                                lastBulletin = bulletinEntity.id;
                            }
                        });

                        // add the content to the scroll pane
                        appendAndScroll(bulletinContainer, content);

                        // trim the container as necessary
                        var contents = bulletinContainer.contents();
                        var length = contents.length;
                        if (length > config.maxBulletins) {
                            contents.slice(0, length - config.maxBulletins).remove();
                            bulletinContainer.prepend('<div class="bulletin-action">&#8230;</div>');
                        }
                    }
                }).fail(function (xhr, status, error) {
                    // likely caused by a invalid regex
                    if (xhr.status === 404) {
                        $('#bulletin-error-message').text(xhr.responseText).show();

                        // stop future polling
                        bulletinBoardCtrl.togglePolling();
                    } else {
                        nfErrorHandler.handleAjaxError(xhr, status, error);
                    }
                });
            },

            /**
             * Toggles whether the page is actively polling for new bulletins.
             */
            togglePolling: function () {
                // conditionally start or stop
                if (bulletinBoardCtrl.polling === true) {
                    start();
                } else {
                    stop();
                }
            }
        }

        var bulletinBoardCtrl = new BulletinBoardCtrl();
        bulletinBoardCtrl.register();
        return bulletinBoardCtrl;
    };

    return nfBulletinBoard;
}));