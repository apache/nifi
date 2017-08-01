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
                'd3',
                'nf.Common',
                'nf.Graph',
                'nf.Shell',
                'nf.ng.Bridge',
                'nf.ClusterSummary',
                'nf.ErrorHandler',
                'nf.Storage',
                'nf.CanvasUtils',
                'nf.Birdseye',
                'nf.ContextMenu',
                'nf.Actions',
                'nf.ProcessGroup'],
            function ($, d3, nfCommon, nfGraph, nfShell, nfNgBridge, nfClusterSummary, nfErrorHandler, nfStorage, nfCanvasUtils, nfBirdseye, nfContextMenu, nfActions, nfProcessGroup) {
                return (nf.Canvas = factory($, d3, nfCommon, nfGraph, nfShell, nfNgBridge, nfClusterSummary, nfErrorHandler, nfStorage, nfCanvasUtils, nfBirdseye, nfContextMenu, nfActions, nfProcessGroup));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Canvas =
            factory(require('jquery'),
                require('d3'),
                require('nf.Common'),
                require('nf.Graph'),
                require('nf.Shell'),
                require('nf.ng.Bridge'),
                require('nf.ClusterSummary'),
                require('nf.ErrorHandler'),
                require('nf.Storage'),
                require('nf.CanvasUtils'),
                require('nf.Birdseye'),
                require('nf.ContextMenu'),
                require('nf.Actions'),
                require('nf.ProcessGroup')));
    } else {
        nf.Canvas = factory(root.$,
            root.d3,
            root.nf.Common,
            root.nf.Graph,
            root.nf.Shell,
            root.nf.ng.Bridge,
            root.nf.ClusterSummary,
            root.nf.ErrorHandler,
            root.nf.Storage,
            root.nf.CanvasUtils,
            root.nf.Birdseye,
            root.nf.ContextMenu,
            root.nf.Actions,
            root.nf.ProcessGroup);
    }
}(this, function ($, d3, nfCommon, nfGraph, nfShell, nfNgBridge, nfClusterSummary, nfErrorHandler, nfStorage, nfCanvasUtils, nfBirdseye, nfContextMenu, nfActions, nfProcessGroup) {
    'use strict';

    var SCALE = 1;
    var TRANSLATE = [0, 0];
    var INCREMENT = 1.2;
    var MAX_SCALE = 8;
    var MIN_SCALE = 0.2;
    var MIN_SCALE_TO_RENDER = 0.6;

    var polling = false;
    var allowPageRefresh = false;
    var groupId = 'root';
    var groupName = null;
    var permissions = null;
    var parentGroupId = null;
    var managedAuthorizer = false;
    var configurableAuthorizer = false;
    var configurableUsersAndGroups = false;
    var svg = null;
    var canvas = null;

    var canvasClicked = false;
    var panning = false;

    var config = {
        urls: {
            api: '../nifi-api',
            currentUser: '../nifi-api/flow/current-user',
            controllerBulletins: '../nifi-api/flow/controller/bulletins',
            kerberos: '../nifi-api/access/kerberos',
            oidc: '../nifi-api/access/oidc/exchange',
            revision: '../nifi-api/flow/revision',
            banners: '../nifi-api/flow/banners'
        }
    };

    /**
     * Register the poller.
     *
     * @argument {int} autoRefreshInterval      The auto refresh interval
     */
    var poll = function (autoRefreshInterval) {
        // ensure we're suppose to poll
        if (polling) {
            // reload the status
            nfCanvas.reload({
                'transition': true
            }).done(function () {
                // start the wait to poll again
                setTimeout(function () {
                    poll(autoRefreshInterval);
                }, autoRefreshInterval * 1000);
            });
        }
    };

    /**
     * Refreshes the graph.
     *
     * @argument {string} processGroupId        The process group id
     * @argument {object} options               Configuration options
     */
    var reloadProcessGroup = function (processGroupId, options) {
        var now = new Date().getTime();

        // load the controller
        return $.ajax({
            type: 'GET',
            url: config.urls.api + '/flow/process-groups/' + encodeURIComponent(processGroupId),
            dataType: 'json'
        }).done(function (flowResponse) {
            // get the controller and its contents
            var processGroupFlow = flowResponse.processGroupFlow;

            // set the group details
            nfCanvas.setGroupId(processGroupFlow.id);

            // get the current group name from the breadcrumb
            var breadcrumb = processGroupFlow.breadcrumb;
            if (breadcrumb.permissions.canRead) {
                nfCanvas.setGroupName(breadcrumb.breadcrumb.name);
            } else {
                nfCanvas.setGroupName(breadcrumb.id);
            }

            // update the access policies
            permissions = flowResponse.permissions;

            // update the breadcrumbs
            nfNgBridge.injector.get('breadcrumbsCtrl').resetBreadcrumbs();
            // inform Angular app values have changed
            nfNgBridge.digest();
            nfNgBridge.injector.get('breadcrumbsCtrl').generateBreadcrumbs(breadcrumb);
            nfNgBridge.injector.get('breadcrumbsCtrl').resetScrollPosition();

            // update the timestamp
            $('#stats-last-refreshed').text(processGroupFlow.lastRefreshed);

            // set the parent id if applicable
            if (nfCommon.isDefinedAndNotNull(processGroupFlow.parentGroupId)) {
                nfCanvas.setParentGroupId(processGroupFlow.parentGroupId);
            } else {
                nfCanvas.setParentGroupId(null);
            }

            // refresh the graph
            nfGraph.expireCaches(now);
            nfGraph.set(processGroupFlow.flow, $.extend({
                'selectAll': false,
                'overrideRevisionCheck': nfClusterSummary.didConnectedStateChange()
            }, options));

            // update component visibility
            nfCanvas.View.updateVisibility();

            // update the birdseye
            nfBirdseye.refresh();
        }).fail(nfErrorHandler.handleAjaxError);
    };

    /**
     * Loads the current user and updates the current user locally.
     *
     * @returns xhr
     */
    var loadCurrentUser = function () {
        // get the current user
        return $.ajax({
            type: 'GET',
            url: config.urls.currentUser,
            dataType: 'json'
        }).done(function (currentUser) {
            // set the current user
            nfCommon.setCurrentUser(currentUser);
        });
    };

    var nfCanvas = {
        CANVAS_OFFSET: 0,

        /**
         * Determines if the current broswer supports SVG.
         */
        SUPPORTS_SVG: !!document.createElementNS && !!document.createElementNS('http://www.w3.org/2000/svg', 'svg').createSVGRect,

        /**
         * Hides the splash that is displayed while the application is loading.
         */
        hideSplash: function () {
            $('#splash').fadeOut();
        },

        /**
         * Remove the status poller.
         */
        stopPolling: function () {
            // set polling flag
            polling = false;
        },

        /**
         * Disable the canvas refresh hot key.
         */
        disableRefreshHotKey: function () {
            allowPageRefresh = true;
        },

        /**
         * Reloads the flow from the server based on the currently specified group id.
         * To load another group, update nfCanvas.setGroupId, clear the canvas, and call nfCanvas.reload.
         */
        reload: function (options) {
            return $.Deferred(function (deferred) {
                // issue the requests
                var processGroupXhr = reloadProcessGroup(nfCanvas.getGroupId(), options);
                var statusXhr = nfNgBridge.injector.get('flowStatusCtrl').reloadFlowStatus();
                var currentUserXhr = loadCurrentUser();
                var controllerBulletins = $.ajax({
                    type: 'GET',
                    url: config.urls.controllerBulletins,
                    dataType: 'json'
                }).done(function (response) {
                    nfNgBridge.injector.get('flowStatusCtrl').updateBulletins(response);
                });
                var clusterSummary = nfClusterSummary.loadClusterSummary().done(function (response) {
                    var clusterSummary = response.clusterSummary;

                    // update the cluster summary
                    nfNgBridge.injector.get('flowStatusCtrl').updateClusterSummary(clusterSummary);
                });

                // wait for all requests to complete
                $.when(processGroupXhr, statusXhr, currentUserXhr, controllerBulletins, clusterSummary).done(function (processGroupResult) {
                    // inform Angular app values have changed
                    nfNgBridge.digest();

                    // resolve the deferred
                    deferred.resolve(processGroupResult);
                }).fail(function (xhr, status, error) {
                    deferred.reject(xhr, status, error);
                });
            }).promise();
        },

        /**
         * Initializes the canvas.
         */
        initCanvas: function () {
            var canvasContainer = $('#canvas-container');

            // create the canvas
            svg = d3.select('#canvas-container').append('svg')
                .on('contextmenu', function () {
                    // reset the canvas click flag
                    canvasClicked = false;

                    // since the context menu event propagated back to the canvas, clear the selection
                    nfCanvasUtils.getSelection().classed('selected', false);

                    // update URL deep linking params
                    nfCanvasUtils.setURLParameters();

                    // show the context menu on the canvas
                    nfContextMenu.show();

                    // prevent default browser behavior
                    d3.event.preventDefault();
                });

            // create the definitions element
            var defs = svg.append('defs');

            // create arrow definitions for the various line types
            defs.selectAll('marker')
                .data(['normal', 'ghost', 'unauthorized', 'full'])
                .enter().append('marker')
                .attr({
                    'id': function (d) {
                        return d;
                    },
                    'viewBox': '0 0 6 6',
                    'refX': 5,
                    'refY': 3,
                    'markerWidth': 6,
                    'markerHeight': 6,
                    'orient': 'auto',
                    'fill': function (d) {
                        if (d === 'ghost') {
                            return '#aaaaaa';
                        } else if (d === 'unauthorized') {
                            return '#ba554a';
                        } else if (d === 'full') {
                            return '#ba554a';
                        } else {
                            return '#000000';
                        }
                    }
                })
                .append('path')
                .attr('d', 'M2,3 L0,6 L6,3 L0,0 z');

            // filter for drop shadow
            var componentDropShadowFilter = defs.append('filter')
                .attr({
                    'id': 'component-drop-shadow',
                    'height': '140%',
                    'y': '-20%'
                });

            // blur
            componentDropShadowFilter.append('feGaussianBlur')
                .attr({
                    'in': 'SourceAlpha',
                    'stdDeviation': 3,
                    'result': 'blur'
                });

            // offset
            componentDropShadowFilter.append('feOffset')
                .attr({
                    'in': 'blur',
                    'dx': 0,
                    'dy': 1,
                    'result': 'offsetBlur'
                });

            // color/opacity
            componentDropShadowFilter.append('feFlood')
                .attr({
                    'flood-color': '#000000',
                    'flood-opacity': 0.4,
                    'result': 'offsetColor'
                });

            // combine
            componentDropShadowFilter.append('feComposite')
                .attr({
                    'in': 'offsetColor',
                    'in2': 'offsetBlur',
                    'operator': 'in',
                    'result': 'offsetColorBlur'
                });

            // stack the effect under the source graph
            var componentDropShadowFeMerge = componentDropShadowFilter.append('feMerge');
            componentDropShadowFeMerge.append('feMergeNode')
                .attr('in', 'offsetColorBlur');
            componentDropShadowFeMerge.append('feMergeNode')
                .attr('in', 'SourceGraphic');

            // filter for drop shadow
            var connectionFullDropShadowFilter = defs.append('filter')
                .attr({
                    'id': 'connection-full-drop-shadow',
                    'height': '140%',
                    'y': '-20%'
                });

            // blur
            connectionFullDropShadowFilter.append('feGaussianBlur')
                .attr({
                    'in': 'SourceAlpha',
                    'stdDeviation': 3,
                    'result': 'blur'
                });

            // offset
            connectionFullDropShadowFilter.append('feOffset')
                .attr({
                    'in': 'blur',
                    'dx': 0,
                    'dy': 1,
                    'result': 'offsetBlur'
                });

            // color/opacity
            connectionFullDropShadowFilter.append('feFlood')
                .attr({
                    'flood-color': '#ba554a',
                    'flood-opacity': 1,
                    'result': 'offsetColor'
                });

            // combine
            connectionFullDropShadowFilter.append('feComposite')
                .attr({
                    'in': 'offsetColor',
                    'in2': 'offsetBlur',
                    'operator': 'in',
                    'result': 'offsetColorBlur'
                });

            // stack the effect under the source graph
            var connectionFullFeMerge = connectionFullDropShadowFilter.append('feMerge');
            connectionFullFeMerge.append('feMergeNode')
                .attr('in', 'offsetColorBlur');
            connectionFullFeMerge.append('feMergeNode')
                .attr('in', 'SourceGraphic');

            // create the canvas element
            canvas = svg.append('g')
                .attr({
                    'transform': 'translate(' + TRANSLATE + ') scale(' + SCALE + ')',
                    'pointer-events': 'all',
                    'id': 'canvas'
                });

            // handle canvas events
            svg.on('mousedown.selection', function () {
                canvasClicked = true;

                if (d3.event.button !== 0) {
                    // prevent further propagation (to parents and others handlers
                    // on the same element to prevent zoom behavior)
                    d3.event.stopImmediatePropagation();
                    return;
                }

                // show selection box if shift is held down
                if (d3.event.shiftKey) {
                    var position = d3.mouse(canvas.node());
                    canvas.append('rect')
                        .attr('rx', 6)
                        .attr('ry', 6)
                        .attr('x', position[0])
                        .attr('y', position[1])
                        .attr('class', 'selection')
                        .attr('width', 0)
                        .attr('height', 0)
                        .attr('stroke-width', function () {
                            return 1 / nfCanvas.View.scale();
                        })
                        .attr('stroke-dasharray', function () {
                            return 4 / nfCanvas.View.scale();
                        })
                        .datum(position);

                    // prevent further propagation (to parents and others handlers
                    // on the same element to prevent zoom behavior)
                    d3.event.stopImmediatePropagation();

                    // prevents the browser from changing to a text selection cursor
                    d3.event.preventDefault();
                }
            })
                .on('mousemove.selection', function () {
                    // update selection box if shift is held down
                    if (d3.event.shiftKey) {
                        // get the selection box
                        var selectionBox = d3.select('rect.selection');
                        if (!selectionBox.empty()) {
                            // get the original position
                            var originalPosition = selectionBox.datum();
                            var position = d3.mouse(canvas.node());

                            var d = {};
                            if (originalPosition[0] < position[0]) {
                                d.x = originalPosition[0];
                                d.width = position[0] - originalPosition[0];
                            } else {
                                d.x = position[0];
                                d.width = originalPosition[0] - position[0];
                            }

                            if (originalPosition[1] < position[1]) {
                                d.y = originalPosition[1];
                                d.height = position[1] - originalPosition[1];
                            } else {
                                d.y = position[1];
                                d.height = originalPosition[1] - position[1];
                            }

                            // update the selection box
                            selectionBox.attr(d);

                            // prevent further propagation (to parents)
                            d3.event.stopPropagation();
                        }
                    }
                })
                .on('mouseup.selection', function () {
                    // ensure this originated from clicking the canvas, not a component.
                    // when clicking on a component, the event propagation is stopped so
                    // it never reaches the canvas. we cannot do this however on up events
                    // since the drag events break down
                    if (canvasClicked === false) {
                        return;
                    }

                    // reset the canvas click flag
                    canvasClicked = false;

                    // get the selection box
                    var selectionBox = d3.select('rect.selection');
                    if (!selectionBox.empty()) {
                        var selectionBoundingBox = {
                            x: parseInt(selectionBox.attr('x'), 10),
                            y: parseInt(selectionBox.attr('y'), 10),
                            width: parseInt(selectionBox.attr('width'), 10),
                            height: parseInt(selectionBox.attr('height'), 10)
                        };

                        // see if a component should be selected or not
                        d3.selectAll('g.component').classed('selected', function (d) {
                            // consider it selected if its already selected or enclosed in the bounding box
                            return d3.select(this).classed('selected') ||
                                d.position.x >= selectionBoundingBox.x && (d.position.x + d.dimensions.width) <= (selectionBoundingBox.x + selectionBoundingBox.width) &&
                                d.position.y >= selectionBoundingBox.y && (d.position.y + d.dimensions.height) <= (selectionBoundingBox.y + selectionBoundingBox.height);
                        });

                        // see if a connection should be selected or not
                        d3.selectAll('g.connection').classed('selected', function (d) {
                            // consider all points
                            var points = [d.start].concat(d.bends, [d.end]);

                            // determine the bounding box
                            var x = d3.extent(points, function (pt) {
                                return pt.x;
                            });
                            var y = d3.extent(points, function (pt) {
                                return pt.y;
                            });

                            // consider it selected if its already selected or enclosed in the bounding box
                            return d3.select(this).classed('selected') ||
                                x[0] >= selectionBoundingBox.x && x[1] <= (selectionBoundingBox.x + selectionBoundingBox.width) &&
                                y[0] >= selectionBoundingBox.y && y[1] <= (selectionBoundingBox.y + selectionBoundingBox.height);
                        });

                        // remove the selection box
                        selectionBox.remove();

                        // update URL deep linking params
                        nfCanvasUtils.setURLParameters();
                    } else if (panning === false) {
                        // deselect as necessary if we are not panning
                        nfCanvasUtils.getSelection().classed('selected', false);

                        // update URL deep linking params
                        nfCanvasUtils.setURLParameters();
                    }

                    // inform Angular app values have changed
                    nfNgBridge.digest();
                });

            // define a function for update the graph dimensions
            var updateGraphSize = function () {
                // get the location of the bottom of the graph
                var footer = $('#banner-footer');
                var bottom = 0;
                if (footer.is(':visible')) {
                    bottom = footer.height();
                }

                // calculate size
                var top = parseInt(canvasContainer.css('top'), 10);
                var windowHeight = $(window).height();
                var canvasHeight = (windowHeight - (bottom + top));

                // canvas/svg
                canvasContainer.css({
                    'height': canvasHeight + 'px',
                    'bottom': bottom + 'px'
                });
                svg.attr({
                    'height': canvasContainer.height(),
                    'width': $(window).width()
                });

                //breadcrumbs
                nfNgBridge.injector.get('breadcrumbsCtrl').updateBreadcrumbsCss({'bottom': bottom + 'px'});

                // body
                $('#canvas-body').css({
                    'height': windowHeight + 'px',
                    'width': $(window).width() + 'px'
                });
            };

            // define a function for update the flow status dimensions
            var updateFlowStatusContainerSize = function () {
                $('#flow-status-container').css({
                    'width': ((($('#nifi-logo').width() + $('#component-container').width())/$(window).width())*100)*2 + '%'
                });
            };
            updateFlowStatusContainerSize();

            // listen for events to go to components
            $('body').on('GoTo:Component', function (e, item) {
                nfCanvasUtils.showComponent(item.parentGroupId, item.id);
            });

            // listen for events to go to process groups
            $('body').on('GoTo:ProcessGroup', function (e, item) {
                nfProcessGroup.enterGroup(item.id).done(function () {
                    nfCanvasUtils.getSelection().classed('selected', false);

                    // inform Angular app that values have changed
                    nfNgBridge.digest();
                });
            });

            // listen for browser resize events to reset the graph size
            $(window).on('resize', function (e) {
                if (e.target === window) {
                    // close the hamburger menu if open
                    if($('.md-menu-backdrop').is(':visible') === true){
                        $('.md-menu-backdrop').click();
                    }

                    updateGraphSize();
                    updateFlowStatusContainerSize();

                    // resize shell when appropriate
                    var shell = $('#shell-dialog');
                    if (shell.is(':visible')){
                        setTimeout(function(shell){
                            nfShell.resizeContent(shell);
                            if(shell.find('#shell-iframe').is(':visible')) {
                                nfShell.resizeIframe(shell);
                            }
                        }, 50, shell);
                    }

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
                        nfCommon.toggleScrollable(tabsContent.get(0));
                    });
                }
            }).on('keydown', function (evt) {
                // if a dialog is open, disable canvas shortcuts
                if ($('.dialog').is(':visible') || $('#search-field').is(':focus')) {
                    return;
                }

                // get the current selection
                var selection = nfCanvasUtils.getSelection();

                // handle shortcuts
                var isCtrl = evt.ctrlKey || evt.metaKey;
                if (isCtrl) {
                    if (evt.keyCode === 82) {
                        if (allowPageRefresh === true) {
                            location.reload();
                            return;
                        }
                        // ctrl-r
                        nfActions.reload();

                        // default prevented in nf-universal-capture.js
                    } else if (evt.keyCode === 65) {
                        // ctrl-a
                        nfActions.selectAll();

                        // update URL deep linking params
                        nfCanvasUtils.setURLParameters();

                        nfNgBridge.digest();

                        // only want to prevent default if the action was performed, otherwise default select all would be overridden
                        evt.preventDefault();
                    } else if (evt.keyCode === 67) {
                        // ctrl-c
                        if (nfCanvas.canWrite() && nfCanvasUtils.isCopyable(selection)) {
                            nfActions.copy(selection);
                        }
                    } else if (evt.keyCode === 86) {
                        // ctrl-v
                        if (nfCanvas.canWrite() && nfCanvasUtils.isPastable()) {
                            nfActions.paste(selection);

                            // only want to prevent default if the action was performed, otherwise default paste would be overridden
                            evt.preventDefault();
                        }
                    }
                } else {
                    if (evt.keyCode === 8 || evt.keyCode === 46) {
                        // backspace or delete
                        if (nfCanvas.canWrite() && nfCanvasUtils.areDeletable(selection)) {
                            nfActions['delete'](selection);
                        }

                        // default prevented in nf-universal-capture.js
                    }
                }
            });

            // get the banners and update the page accordingly
            $.ajax({
                type: 'GET',
                url: config.urls.banners,
                dataType: 'json'
            }).done(function (response) {
                // ensure the banners response is specified
                if (nfCommon.isDefinedAndNotNull(response.banners)) {
                    if (nfCommon.isDefinedAndNotNull(response.banners.headerText) && response.banners.headerText !== '') {
                        // update the header text and show it
                        $('#banner-header').addClass('banner-header-background').text(response.banners.headerText).show();
                        $('#canvas-container').css('top', '98px');
                    }

                    if (nfCommon.isDefinedAndNotNull(response.banners.footerText) && response.banners.footerText !== '') {
                        // update the footer text and show it
                        var bannerFooter = $('#banner-footer').text(response.banners.footerText).show();

                        var updateBottom = function (elementId) {
                            var element = $('#' + elementId);
                            element.css('bottom', parseInt(bannerFooter.css('height'), 10) + 'px');
                        };

                        // update the position of elements affected by bottom banners
                        updateBottom('graph');
                    }
                }

                // update the graph dimensions
                updateGraphSize();
            }).fail(nfErrorHandler.handleAjaxError);
        },

        /**
         * Initialize NiFi.
         */
        init: function () {
            // attempt kerberos/oidc authentication
            var ticketExchange = $.Deferred(function (deferred) {
                var successfulAuthentication = function (jwt) {
                    // get the payload and store the token with the appropriate expiration
                    var token = nfCommon.getJwtPayload(jwt);
                    var expiration = parseInt(token['exp'], 10) * nfCommon.MILLIS_PER_SECOND;
                    nfStorage.setItem('jwt', jwt, expiration);
                    deferred.resolve();
                };

                if (nfStorage.hasItem('jwt')) {
                    deferred.resolve();
                } else {
                    $.ajax({
                        type: 'POST',
                        url: config.urls.kerberos,
                        dataType: 'text'
                    }).done(function (jwt) {
                        successfulAuthentication(jwt);
                    }).fail(function () {
                        $.ajax({
                            type: 'POST',
                            url: config.urls.oidc,
                            dataType: 'text'
                        }).done(function (jwt) {
                            successfulAuthentication(jwt)
                        }).fail(function () {
                            deferred.reject();
                        });
                    });
                }
            }).promise();

            // load the current user
            return $.Deferred(function (deferred) {
                ticketExchange.always(function () {
                    loadCurrentUser().done(function (currentUser) {
                        // if the user is logged, we want to determine if they were logged in using a certificate
                        if (currentUser.anonymous === false) {
                            // render the users name
                            $('#current-user').text(currentUser.identity).show();

                            // render the logout button if there is a token locally
                            if (nfStorage.getItem('jwt') !== null) {
                                $('#logout-link-container').show();
                            }
                        } else {
                            // set the anonymous user label
                            nfCommon.setAnonymousUserLabel();
                        }
                        deferred.resolve();
                    }).fail(function (xhr, status, error) {
                        // there is no anonymous access and we don't know this user - open the login page which handles login/registration/etc
                        if (xhr.status === 401) {
                            window.location = '../nifi/login';
                        } else {
                            deferred.reject(xhr, status, error);
                        }
                    });
                });
            }).promise();
        },

        /**
         * Set the group id.
         *
         * @argument {string} gi       The group id
         */
        setGroupId: function (gi) {
            groupId = gi;
        },

        /**
         * Get the group id.
         */
        getGroupId: function () {
            return groupId;
        },

        /**
         * Set the group name.
         *
         * @argument {string} gn     The group name
         */
        setGroupName: function (gn) {
            groupName = gn;
        },

        /**
         * Get the group name.
         */
        getGroupName: function () {
            return groupName;
        },

        /**
         * Set the parent group id.
         *
         * @argument {string} pgi     The id of the parent group
         */
        setParentGroupId: function (pgi) {
            parentGroupId = pgi;
        },

        /**
         * Get the parent group id.
         */
        getParentGroupId: function () {
            return parentGroupId;
        },

        /**
         * Set whether the authorizer is managed.
         *
         * @param bool The boolean value representing whether the authorizer is managed
         */
        setManagedAuthorizer: function (bool) {
            managedAuthorizer = bool;
        },

        /**
         * Returns whether the authorizer is managed.
         */
        isManagedAuthorizer: function () {
            return managedAuthorizer;
        },

        /**
         * Set whether the authorizer is configurable.
         *
         * @param bool The boolean value representing whether the authorizer is configurable.
         */
        setConfigurableAuthorizer: function(bool){
            configurableAuthorizer = bool;
        },

        /**
         * Returns whether the authorizer is configurable.
         */
        isConfigurableAuthorizer: function () {
            return configurableAuthorizer;
        },

        /**
         * Set whether the users and groups is configurable.
         *
         * @param bool The boolean value representing whether the users and groups is configurable.
         */
        setConfigurableUsersAndGroups: function(bool){
            configurableUsersAndGroups = bool;
        },

        /**
         * Returns whether the users and groups is configurable.
         */
        isConfigurableUsersAndGroups: function () {
            return configurableUsersAndGroups;
        },

        /**
         * Whether the current user can read from this group.
         *
         * @returns {boolean}   can write
         */
        canRead: function () {
            if (permissions === null) {
                return false;
            } else {
                return permissions.canRead === true;
            }
        },

        /**
         * Whether the current user can write in this group.
         *
         * @returns {boolean}   can write
         */
        canWrite: function () {
            if (permissions === null) {
                return false;
            } else {
                return permissions.canWrite === true;
            }
        },

        /**
         * Starts polling.
         *
         * @argument {int} autoRefreshInterval      The auto refresh interval
         */
        startPolling: function (autoRefreshInterval) {
            // set polling flag
            polling = true;
            poll(autoRefreshInterval);
        },

        View: (function () {

            /**
             * Updates component visibility based on their proximity to the screen's viewport.
             */
            var updateComponentVisibility = function () {
                var canvasContainer = $('#canvas-container');
                var translate = nfCanvas.View.translate();
                var scale = nfCanvas.View.scale();

                // scale the translation
                translate = [translate[0] / scale, translate[1] / scale];

                // get the normalized screen width and height
                var screenWidth = canvasContainer.width() / scale;
                var screenHeight = canvasContainer.height() / scale;

                // calculate the screen bounds one screens worth in each direction
                var screenLeft = -translate[0] - screenWidth;
                var screenTop = -translate[1] - screenHeight;
                var screenRight = screenLeft + (screenWidth * 3);
                var screenBottom = screenTop + (screenHeight * 3);

                // detects whether a component is visible and should be rendered
                var isComponentVisible = function (d) {
                    if (!nfCanvas.View.shouldRenderPerScale()) {
                        return false;
                    }

                    var left = d.position.x;
                    var top = d.position.y;
                    var right = left + d.dimensions.width;
                    var bottom = top + d.dimensions.height;

                    // determine if the component is now visible
                    return screenLeft < right && screenRight > left && screenTop < bottom && screenBottom > top;
                };

                // detects whether a connection is visible and should be rendered
                var isConnectionVisible = function (d) {
                    if (!nfCanvas.View.shouldRenderPerScale()) {
                        return false;
                    }

                    var x, y;
                    if (d.bends.length > 0) {
                        var i = Math.min(Math.max(0, d.labelIndex), d.bends.length - 1);
                        x = d.bends[i].x;
                        y = d.bends[i].y;
                    } else {
                        x = (d.start.x + d.end.x) / 2;
                        y = (d.start.y + d.end.y) / 2;
                    }

                    return screenLeft < x && screenRight > x && screenTop < y && screenBottom > y;
                };

                // marks the specific component as visible and determines if its entering or leaving visibility
                var updateVisibility = function (d, isVisible) {
                    var selection = d3.select('#id-' + d.id);
                    var visible = isVisible(d);
                    var wasVisible = selection.classed('visible');

                    // mark the selection as appropriate
                    selection.classed('visible', visible)
                        .classed('entering', function () {
                            return visible && !wasVisible;
                        }).classed('leaving', function () {
                        return !visible && wasVisible;
                    });
                };

                // get the all components
                var graph = nfGraph.get();

                // update the visibility for each component
                $.each(graph.processors, function (_, d) {
                    updateVisibility(d, isComponentVisible);
                });
                $.each(graph.ports, function (_, d) {
                    updateVisibility(d, isComponentVisible);
                });
                $.each(graph.processGroups, function (_, d) {
                    updateVisibility(d, isComponentVisible);
                });
                $.each(graph.remoteProcessGroups, function (_, d) {
                    updateVisibility(d, isComponentVisible);
                });
                $.each(graph.connections, function (_, d) {
                    updateVisibility(d, isConnectionVisible);
                });
            };

            // initialize the zoom behavior
            var behavior;

            return {
                init: function () {
                    var refreshed;
                    var zoomed = false;

                    // define the behavior
                    behavior = d3.behavior.zoom()
                        .scaleExtent([MIN_SCALE, MAX_SCALE])
                        .translate(TRANSLATE)
                        .scale(SCALE)
                        .on('zoomstart', function () {
                            // hide the context menu
                            nfContextMenu.hide();
                        })
                        .on('zoom', function () {
                            // if we have zoomed, indicate that we are panning
                            // to prevent deselection elsewhere
                            if (zoomed) {
                                panning = true;
                            } else {
                                zoomed = true;
                            }

                            // see if the scale has changed during this zoom event,
                            // we want to only transition when zooming in/out as running
                            // the transitions during pan events is
                            var transition = d3.event.sourceEvent.type === 'wheel' || d3.event.sourceEvent.type === 'mousewheel';

                            // refresh the canvas
                            refreshed = nfCanvas.View.refresh({
                                persist: false,
                                transition: transition,
                                refreshComponents: false,
                                refreshBirdseye: false
                            });
                        })
                        .on('zoomend', function () {
                            // ensure the canvas was actually refreshed
                            if (nfCommon.isDefinedAndNotNull(refreshed)) {
                                nfCanvas.View.updateVisibility();

                                // refresh the birdseye
                                refreshed.done(function () {
                                    nfBirdseye.refresh();
                                });

                                // persist the users view
                                nfCanvasUtils.persistUserView();

                                // reset the refreshed deferred
                                refreshed = null;
                            }

                            panning = false;
                            zoomed = false;
                        });

                    // add the behavior to the canvas and disable dbl click zoom
                    svg.call(behavior).on('dblclick.zoom', null);
                },

                /**
                 * Whether or not a component should be rendered based solely on the current scale.
                 *
                 * @returns {Boolean}
                 */
                shouldRenderPerScale: function () {
                    return nfCanvas.View.scale() >= MIN_SCALE_TO_RENDER;
                },

                /**
                 * Updates component visibility based on the current translation/scale.
                 */
                updateVisibility: function () {
                    updateComponentVisibility();
                    nfGraph.pan();
                },

                /**
                 * Sets/gets the current translation.
                 *
                 * @param {array} translate     [x, y]
                 */
                translate: function (translate) {
                    if (nfCommon.isUndefined(translate)) {
                        return behavior.translate();
                    } else {
                        behavior.translate(translate);
                    }
                },

                /**
                 * Sets/gets the current scale.
                 *
                 * @param {number} scale        The new scale
                 */
                scale: function (scale) {
                    if (nfCommon.isUndefined(scale)) {
                        return behavior.scale();
                    } else {
                        behavior.scale(scale);
                    }
                },

                /**
                 * Zooms in a single zoom increment.
                 */
                zoomIn: function () {
                    var translate = nfCanvas.View.translate();
                    var scale = nfCanvas.View.scale();
                    var newScale = Math.min(scale * INCREMENT, MAX_SCALE);

                    // get the canvas normalized width and height
                    var canvasContainer = $('#canvas-container');
                    var screenWidth = canvasContainer.width() / scale;
                    var screenHeight = canvasContainer.height() / scale;

                    // adjust the scale
                    nfCanvas.View.scale(newScale);

                    // center around the center of the screen accounting for the translation accordingly
                    nfCanvasUtils.centerBoundingBox({
                        x: (screenWidth / 2) - (translate[0] / scale),
                        y: (screenHeight / 2) - (translate[1] / scale),
                        width: 1,
                        height: 1
                    });
                },

                /**
                 * Zooms out a single zoom increment.
                 */
                zoomOut: function () {
                    var translate = nfCanvas.View.translate();
                    var scale = nfCanvas.View.scale();
                    var newScale = Math.max(scale / INCREMENT, MIN_SCALE);

                    // get the canvas normalized width and height
                    var canvasContainer = $('#canvas-container');
                    var screenWidth = canvasContainer.width() / scale;
                    var screenHeight = canvasContainer.height() / scale;

                    // adjust the scale
                    nfCanvas.View.scale(newScale);

                    // center around the center of the screen accounting for the translation accordingly
                    nfCanvasUtils.centerBoundingBox({
                        x: (screenWidth / 2) - (translate[0] / scale),
                        y: (screenHeight / 2) - (translate[1] / scale),
                        width: 1,
                        height: 1
                    });
                },

                /**
                 * Zooms to fit the entire graph on the canvas.
                 */
                fit: function () {
                    var translate = nfCanvas.View.translate();
                    var scale = nfCanvas.View.scale();
                    var newScale;

                    // get the canvas normalized width and height
                    var canvasContainer = $('#canvas-container');
                    var canvasWidth = canvasContainer.width();
                    var canvasHeight = canvasContainer.height();

                    // get the bounding box for the graph
                    var graphBox = d3.select('#canvas').node().getBoundingClientRect();
                    var graphWidth = graphBox.width / scale;
                    var graphHeight = graphBox.height / scale;
                    var graphLeft = graphBox.left / scale;
                    var graphTop = (graphBox.top - nfCanvas.CANVAS_OFFSET) / scale;


                    // adjust the scale to ensure the entire graph is visible
                    if (graphWidth > canvasWidth || graphHeight > canvasHeight) {
                        newScale = Math.min(canvasWidth / graphWidth, canvasHeight / graphHeight);

                        // ensure the scale is within bounds
                        newScale = Math.min(Math.max(newScale, MIN_SCALE), MAX_SCALE);
                    } else {
                        newScale = 1;

                        // since the entire graph will fit on the canvas, offset origin appropriately
                        graphLeft -= 100;
                        graphTop -= 50;
                    }

                    // set the new scale
                    nfCanvas.View.scale(newScale);

                    // center as appropriate
                    nfCanvasUtils.centerBoundingBox({
                        x: graphLeft - (translate[0] / scale),
                        y: graphTop - (translate[1] / scale),
                        width: canvasWidth / newScale,
                        height: canvasHeight / newScale
                    });
                },

                /**
                 * Zooms to the actual size (1 to 1).
                 */
                actualSize: function () {
                    var translate = nfCanvas.View.translate();
                    var scale = nfCanvas.View.scale();

                    // get the first selected component
                    var selection = nfCanvasUtils.getSelection();

                    // set the updated scale
                    nfCanvas.View.scale(1);

                    // box to zoom towards
                    var box;

                    // if components have been selected position the view accordingly
                    if (!selection.empty()) {
                        // gets the data for the first component
                        var selectionBox = selection.node().getBoundingClientRect();

                        // get the bounding box for the selected components
                        box = {
                            x: (selectionBox.left / scale) - (translate[0] / scale),
                            y: ((selectionBox.top - nfCanvas.CANVAS_OFFSET) / scale) - (translate[1] / scale),
                            width: selectionBox.width / scale,
                            height: selectionBox.height / scale
                        };
                    } else {
                        // get the offset
                        var canvasContainer = $('#canvas-container');

                        // get the canvas normalized width and height
                        var screenWidth = canvasContainer.width() / scale;
                        var screenHeight = canvasContainer.height() / scale;

                        // center around the center of the screen accounting for the translation accordingly
                        box = {
                            x: (screenWidth / 2) - (translate[0] / scale),
                            y: (screenHeight / 2) - (translate[1] / scale),
                            width: 1,
                            height: 1
                        };
                    }

                    // center as appropriate
                    nfCanvasUtils.centerBoundingBox(box);
                },

                /**
                 * Refreshes the view based on the configured translation and scale.
                 *
                 * @param {object} options Options for the refresh operation
                 */
                refresh: function (options) {
                    return $.Deferred(function (deferred) {

                        var persist = true;
                        var transition = false;
                        var refreshComponents = true;
                        var refreshBirdseye = true;

                        // extract the options if specified
                        if (nfCommon.isDefinedAndNotNull(options)) {
                            persist = nfCommon.isDefinedAndNotNull(options.persist) ? options.persist : persist;
                            transition = nfCommon.isDefinedAndNotNull(options.transition) ? options.transition : transition;
                            refreshComponents = nfCommon.isDefinedAndNotNull(options.refreshComponents) ? options.refreshComponents : refreshComponents;
                            refreshBirdseye = nfCommon.isDefinedAndNotNull(options.refreshBirdseye) ? options.refreshBirdseye : refreshBirdseye;
                        }

                        // update component visibility
                        if (refreshComponents) {
                            nfCanvas.View.updateVisibility();
                        }

                        // persist if appropriate
                        if (persist === true) {
                            nfCanvasUtils.persistUserView();
                        }

                        // update the canvas
                        if (transition === true) {
                            canvas.transition()
                                .duration(500)
                                .attr('transform', function () {
                                    return 'translate(' + behavior.translate() + ') scale(' + behavior.scale() + ')';
                                })
                                .each('end', function () {
                                    // refresh birdseye if appropriate
                                    if (refreshBirdseye === true) {
                                        nfBirdseye.refresh();
                                    }

                                    deferred.resolve();
                                });
                        } else {
                            canvas.attr('transform', function () {
                                return 'translate(' + behavior.translate() + ') scale(' + behavior.scale() + ')';
                            });

                            // refresh birdseye if appropriate
                            if (refreshBirdseye === true) {
                                nfBirdseye.refresh();
                            }

                            deferred.resolve();
                        }
                    }).promise();
                }
            };
        }())
    };

    return nfCanvas;
}));