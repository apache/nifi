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

$(document).ready(function () {
    if (nf.Canvas.SUPPORTS_SVG) {

        //Create Angular App
        var app = angular.module('ngCanvasApp', ['ngResource', 'ngRoute', 'ngMaterial', 'ngMessages']);

        //Define Dependency Injection Annotations
        nf.ng.AppConfig.$inject = ['$mdThemingProvider', '$compileProvider'];
        nf.ng.AppCtrl.$inject = ['$scope', 'serviceProvider', '$compile', 'headerCtrl', 'graphControlsCtrl'];
        nf.ng.ServiceProvider.$inject = [];
        nf.ng.BreadcrumbsCtrl.$inject = ['serviceProvider'];
        nf.ng.Canvas.HeaderCtrl.$inject = ['serviceProvider', 'toolboxCtrl', 'globalMenuCtrl', 'flowStatusCtrl'];
        nf.ng.Canvas.FlowStatusCtrl.$inject = ['serviceProvider'];
        nf.ng.Canvas.GlobalMenuCtrl.$inject = ['serviceProvider'];
        nf.ng.Canvas.ToolboxCtrl.$inject = ['processorComponent',
            'inputPortComponent',
            'outputPortComponent',
            'groupComponent',
            'remoteGroupComponent',
            'funnelComponent',
            'templateComponent',
            'labelComponent'];
        nf.ng.ProcessorComponent.$inject = ['serviceProvider'];
        nf.ng.InputPortComponent.$inject = ['serviceProvider'];
        nf.ng.OutputPortComponent.$inject = ['serviceProvider'];
        nf.ng.GroupComponent.$inject = ['serviceProvider'];
        nf.ng.RemoteProcessGroupComponent.$inject = ['serviceProvider'];
        nf.ng.FunnelComponent.$inject = ['serviceProvider'];
        nf.ng.TemplateComponent.$inject = ['serviceProvider'];
        nf.ng.LabelComponent.$inject = ['serviceProvider'];
        nf.ng.Canvas.GraphControlsCtrl.$inject = ['serviceProvider', 'navigateCtrl', 'operateCtrl'];
        nf.ng.Canvas.NavigateCtrl.$inject = [];
        nf.ng.Canvas.OperateCtrl.$inject = [];
        nf.ng.BreadcrumbsDirective.$inject = ['breadcrumbsCtrl'];
        nf.ng.DraggableDirective.$inject = [];

        //Configure Angular App
        app.config(nf.ng.AppConfig);

        //Define Angular App Controllers
        app.controller('ngCanvasAppCtrl', nf.ng.AppCtrl);

        //Define Angular App Services
        app.service('serviceProvider', nf.ng.ServiceProvider);
        app.service('breadcrumbsCtrl', nf.ng.BreadcrumbsCtrl);
        app.service('headerCtrl', nf.ng.Canvas.HeaderCtrl);
        app.service('globalMenuCtrl', nf.ng.Canvas.GlobalMenuCtrl);
        app.service('toolboxCtrl', nf.ng.Canvas.ToolboxCtrl);
        app.service('flowStatusCtrl', nf.ng.Canvas.FlowStatusCtrl);
        app.service('processorComponent', nf.ng.ProcessorComponent);
        app.service('inputPortComponent', nf.ng.InputPortComponent);
        app.service('outputPortComponent', nf.ng.OutputPortComponent);
        app.service('groupComponent', nf.ng.GroupComponent);
        app.service('remoteGroupComponent', nf.ng.RemoteProcessGroupComponent);
        app.service('funnelComponent', nf.ng.FunnelComponent);
        app.service('templateComponent', nf.ng.TemplateComponent);
        app.service('labelComponent', nf.ng.LabelComponent);
        app.service('graphControlsCtrl', nf.ng.Canvas.GraphControlsCtrl);
        app.service('navigateCtrl', nf.ng.Canvas.NavigateCtrl);
        app.service('operateCtrl', nf.ng.Canvas.OperateCtrl);

        //Define Angular App Directives
        app.directive('nfBreadcrumbs', nf.ng.BreadcrumbsDirective);
        app.directive('nfDraggable', nf.ng.DraggableDirective);

        //Manually Boostrap Angular App
        nf.ng.Bridge.injector = angular.bootstrap($('body'), ['ngCanvasApp'], { strictDi: true });

        // initialize the NiFi
        nf.Canvas.init();

        //initialize toolbox components tooltips
        $('.component-button').qtip($.extend({}, nf.Common.config.tooltipConfig));
    } else {
        $('#message-title').text('Unsupported Browser');
        $('#message-content').text('Flow graphs are shown using SVG. Please use a browser that supports rendering SVG.');

        // show the error pane
        $('#message-pane').show();

        // hide the splash screen
        nf.Canvas.hideSplash();
    }
});

nf.Canvas = (function () {

    var SCALE = 1;
    var TRANSLATE = [0, 0];
    var INCREMENT = 1.2;
    var MAX_SCALE = 8;
    var MIN_SCALE = 0.2;
    var MIN_SCALE_TO_RENDER = 0.6;

    var polling = false;
    var groupId = 'root';
    var groupName = null;
    var permissions = null;
    var parentGroupId = null;
    var clustered = false;
    var connectedToCluster = false;
    var configurableAuthorizer = false;
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
            revision: '../nifi-api/flow/revision',
            banners: '../nifi-api/flow/banners',
            flowConfig: '../nifi-api/flow/config',
            clusterSummary: '../nifi-api/flow/cluster/summary',
            cluster: '../nifi-api/controller/cluster'
        }
    };

    /**
     * Starts polling.
     *
     * @argument {int} autoRefreshInterval      The auto refresh interval
     */
    var startPolling = function (autoRefreshInterval) {
        // set polling flag
        polling = true;
        poll(autoRefreshInterval);
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
            nf.Canvas.reload({
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
     * Initializes the canvas.
     */
    var initCanvas = function () {
        var canvasContainer = $('#canvas-container');

        // create the canvas
        svg = d3.select('#canvas-container').append('svg')
            .on('contextmenu', function () {
                // reset the canvas click flag
                canvasClicked = false;

                // since the context menu event propagated back to the canvas, clear the selection
                nf.CanvasUtils.getSelection().classed('selected', false);

                // show the context menu on the canvas
                nf.ContextMenu.show();

                // prevent default browser behavior
                d3.event.preventDefault();
            });

        // create the definitions element
        var defs = svg.append('defs');

        // create arrow definitions for the various line types
        defs.selectAll('marker')
            .data(['normal', 'ghost', 'unauthorized'])
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
                    } else {
                        return '#000000';
                    }
                }
            })
            .append('path')
            .attr('d', 'M2,3 L0,6 L6,3 L0,0 z');

        // filter for drop shadow
        var filter = defs.append('filter')
            .attr({
                'id': 'component-drop-shadow',
                'height': '140%',
                'y': '-20%'
            });

        // blur
        filter.append('feGaussianBlur')
            .attr({
                'in': 'SourceAlpha',
                'stdDeviation': 3,
                'result': 'blur'
            });

        // offset
        filter.append('feOffset')
            .attr({
                'in': 'blur',
                'dx': 0,
                'dy': 1,
                'result': 'offsetBlur'
            });

        // color/opacity
        filter.append('feFlood')
            .attr({
                'flood-color': '#000000',
                'flood-opacity': 0.4,
                'result': 'offsetColor'
            });

        // combine
        filter.append('feComposite')
            .attr({
                'in': 'offsetColor',
                'in2': 'offsetBlur',
                'operator': 'in',
                'result': 'offsetColorBlur'
            });

        // stack the effect under the source graph
        var feMerge = filter.append('feMerge');
        feMerge.append('feMergeNode')
            .attr('in', 'offsetColorBlur');
        feMerge.append('feMergeNode')
            .attr('in', 'SourceGraphic');

        // define the gradient for the expiration icon
        var expirationBackground = defs.append('linearGradient')
            .attr({
                'id': 'expiration',
                'x1': '0%',
                'y1': '0%',
                'x2': '0%',
                'y2': '100%'
            });

        expirationBackground.append('stop')
            .attr({
                'offset': '0%',
                'stop-color': '#aeafb1'
            });

        expirationBackground.append('stop')
            .attr({
                'offset': '100%',
                'stop-color': '#87888a'
            });

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
                        return 1 / nf.Canvas.View.scale();
                    })
                    .attr('stroke-dasharray', function () {
                        return 4 / nf.Canvas.View.scale();
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
                } else if (panning === false) {
                    // deselect as necessary if we are not panning
                    nf.CanvasUtils.getSelection().classed('selected', false);
                }

                // inform Angular app values have changed
                nf.ng.Bridge.digest();
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
            nf.ng.Bridge.injector.get('breadcrumbsCtrl').updateBreadcrumbsCss({'bottom': bottom + 'px'});

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

        // listen for browser resize events to reset the graph size
        $(window).on('resize', function (e) {
            if (e.target === window) {
                updateGraphSize();
                updateFlowStatusContainerSize();

                // resize shell when appropriate
                var shell = $('#shell-dialog');
                if (shell.is(':visible')){
                    setTimeout(function(shell){
                        nf.Shell.resizeContent(shell);
                        if(shell.find('#shell-iframe').is(':visible')) {
                            nf.Shell.resizeIframe(shell);
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
                    nf.Common.toggleScrollable(tabsContent.get(0));
                });
            }
        }).on('keydown', function (evt) {
            // if a dialog is open, disable canvas shortcuts
            if ($('.dialog').is(':visible') || $('#search-field').is(':focus')) {
                return;
            }

            // get the current selection
            var selection = nf.CanvasUtils.getSelection();

            // handle shortcuts
            var isCtrl = evt.ctrlKey || evt.metaKey;
            if (isCtrl) {
                if (evt.keyCode === 82) {
                    // ctrl-r
                    nf.Actions.reload();

                    // default prevented in nf-universal-capture.js
                } else if (evt.keyCode === 65) {
                    // ctrl-a
                    nf.Actions.selectAll();
                    nf.ng.Bridge.digest();

                    // only want to prevent default if the action was performed, otherwise default select all would be overridden
                    evt.preventDefault();
                } else if (evt.keyCode === 67) {
                    // ctrl-c
                    if (nf.Canvas.canWrite() && nf.CanvasUtils.isCopyable(selection)) {
                        nf.Actions.copy(selection);
                    }
                } else if (evt.keyCode === 86) {
                    // ctrl-v
                    if (nf.Canvas.canWrite() && nf.CanvasUtils.isPastable()) {
                        nf.Actions.paste(selection);

                        // only want to prevent default if the action was performed, otherwise default paste would be overridden
                        evt.preventDefault();
                    }
                }
            } else {
                if (evt.keyCode === 8 || evt.keyCode === 46) {
                    // backspace or delete
                    if (nf.Canvas.canWrite() && nf.CanvasUtils.areDeletable(selection)) {
                        nf.Actions['delete'](selection);
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
            if (nf.Common.isDefinedAndNotNull(response.banners)) {
                if (nf.Common.isDefinedAndNotNull(response.banners.headerText) && response.banners.headerText !== '') {
                    // update the header text and show it
                    $('#banner-header').addClass('banner-header-background').text(response.banners.headerText).show();
                    $('#canvas-container').css('top', '98px');
                }

                if (nf.Common.isDefinedAndNotNull(response.banners.footerText) && response.banners.footerText !== '') {
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
        }).fail(nf.Common.handleAjaxError);
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
            nf.Canvas.setGroupId(processGroupFlow.id);

            // get the current group name from the breadcrumb
            var breadcrumb = processGroupFlow.breadcrumb;
            if (breadcrumb.permissions.canRead) {
                nf.Canvas.setGroupName(breadcrumb.breadcrumb.name);
            } else {
                nf.Canvas.setGroupName(breadcrumb.id);
            }

            // update the access policies
            permissions = flowResponse.permissions;
            
            // update the breadcrumbs
            nf.ng.Bridge.injector.get('breadcrumbsCtrl').resetBreadcrumbs();
            nf.ng.Bridge.injector.get('breadcrumbsCtrl').generateBreadcrumbs(breadcrumb);
            nf.ng.Bridge.injector.get('breadcrumbsCtrl').resetScrollPosition();

            // update the timestamp
            $('#stats-last-refreshed').text(processGroupFlow.lastRefreshed);

            // set the parent id if applicable
            if (nf.Common.isDefinedAndNotNull(processGroupFlow.parentGroupId)) {
                nf.Canvas.setParentGroupId(processGroupFlow.parentGroupId);
            } else {
                nf.Canvas.setParentGroupId(null);
            }

            // refresh the graph
            nf.Graph.expireCaches(now);
            nf.Graph.set(processGroupFlow.flow, $.extend({
                'selectAll': false
            }, options));

            // update component visibility
            nf.Canvas.View.updateVisibility();

            // update the birdseye
            nf.Birdseye.refresh();
        }).fail(nf.Common.handleAjaxError);
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
            nf.Common.setCurrentUser(currentUser);
        });
    };

    /**
     * Loads the flow configuration and updated the cluster state.
     *
     * @returns xhr
     */
    var loadClusterSummary = function () {
        return $.ajax({
            type: 'GET',
            url: config.urls.clusterSummary,
            dataType: 'json'
        });
    };

    return {
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
         * Reloads the flow from the server based on the currently specified group id.
         * To load another group, update nf.Canvas.setGroupId, clear the canvas, and call nf.Canvas.reload.
         */
        reload: function (options) {
            return $.Deferred(function (deferred) {
                // issue the requests
                var processGroupXhr = reloadProcessGroup(nf.Canvas.getGroupId(), options);
                var statusXhr = nf.ng.Bridge.injector.get('flowStatusCtrl').reloadFlowStatus();
                var currentUserXhr = loadCurrentUser();
                var controllerBulletins = $.ajax({
                    type: 'GET',
                    url: config.urls.controllerBulletins,
                    dataType: 'json'
                }).done(function (response) {
                    nf.ng.Bridge.injector.get('flowStatusCtrl').updateBulletins(response);
                });
                var clusterSummary = loadClusterSummary().done(function (response) {
                    var clusterSummary = response.clusterSummary;

                    // update the cluster summary
                    nf.ng.Bridge.injector.get('flowStatusCtrl').updateClusterSummary(clusterSummary);

                    // update the clustered flag
                    clustered = clusterSummary.clustered;
                    connectedToCluster = clusterSummary.connectedToCluster;
                });

                // wait for all requests to complete
                $.when(processGroupXhr, statusXhr, currentUserXhr, controllerBulletins, clusterSummary).done(function (processGroupResult) {
                    // inform Angular app values have changed
                    nf.ng.Bridge.digest();

                    // resolve the deferred
                    deferred.resolve(processGroupResult);
                }).fail(function (xhr, status, error) {
                    deferred.reject(xhr, status, error);
                });
            }).promise();
        },

        /**
         * Shows a message when disconnected from the cluster.
         */
        showDisconnectedFromClusterMessage: function () {
            nf.Dialog.showOkDialog({
                headerText: 'Cluster Connection',
                dialogContent: 'This node is currently not connected to the cluster. Any modifications to the data flow made here will not replicate across the cluster.'
            });
        },

        /**
         * Shows a message when connected to the cluster.
         */
        showConnectedToClusterMessage: function () {
            nf.Dialog.showOkDialog({
                headerText: 'Cluster Connection',
                dialogContent: 'This node just joined the cluster. Any modifications to the data flow made here will replicate across the cluster.'
            });
        },

        /**
         * Initialize NiFi.
         */
        init: function () {
            // attempt kerberos authentication
            var ticketExchange = $.Deferred(function (deferred) {
                if (nf.Storage.hasItem('jwt')) {
                    deferred.resolve();
                } else {
                    $.ajax({
                        type: 'POST',
                        url: config.urls.kerberos,
                        dataType: 'text'
                    }).done(function (jwt) {
                        // get the payload and store the token with the appropriate expiration
                        var token = nf.Common.getJwtPayload(jwt);
                        var expiration = parseInt(token['exp'], 10) * nf.Common.MILLIS_PER_SECOND;
                        nf.Storage.setItem('jwt', jwt, expiration);
                        deferred.resolve();
                    }).fail(function () {
                        deferred.reject();
                    });
                }
            }).promise();

            // load the current user
            var userXhr = $.Deferred(function (deferred) {
                ticketExchange.always(function () {
                    loadCurrentUser().done(function (currentUser) {
                        // if the user is logged, we want to determine if they were logged in using a certificate
                        if (currentUser.anonymous === false) {
                            // render the users name
                            $('#current-user').text(currentUser.identity).show();

                            // render the logout button if there is a token locally
                            if (nf.Storage.getItem('jwt') !== null) {
                                $('#logout-link-container').show();
                            }
                        } else {
                            // set the anonymous user label
                            nf.Common.setAnonymousUserLabel();
                        }
                        deferred.resolve();
                    }).fail(function (xhr, status, error) {
                        // there is no anonymous access and we don't know this user - open the login page which handles login/registration/etc
                        if (xhr.status === 401) {
                            window.location = '/nifi/login';
                        } else {
                            deferred.reject(xhr, status, error);
                        }
                    });
                });
            }).promise();
            
            userXhr.done(function () {
                // load the client id
                var clientXhr = nf.Client.init();

                // get the controller config to register the status poller
                var configXhr = $.ajax({
                    type: 'GET',
                    url: config.urls.flowConfig,
                    dataType: 'json'
                });

                // get the initial cluster summary
                var clusterSummary = loadClusterSummary();

                // ensure the config requests are loaded
                $.when(configXhr, clusterSummary, userXhr, clientXhr).done(function (configResult, clusterSummaryResult) {
                    var configResponse = configResult[0];
                    var clusterSummaryResponse = clusterSummaryResult[0];

                    // calculate the canvas offset
                    var canvasContainer = $('#canvas-container');
                    nf.Canvas.CANVAS_OFFSET = canvasContainer.offset().top;

                    // get the config details
                    var configDetails = configResponse.flowConfiguration;
                    var clusterSummary = clusterSummaryResponse.clusterSummary;

                    // show disconnected message on load if necessary
                    if (clusterSummary.clustered && !clusterSummary.connectedToCluster) {
                        nf.Canvas.showDisconnectedFromClusterMessage();
                    }

                    // establish the initial cluster state
                    clustered = clusterSummary.clustered;
                    connectedToCluster = clusterSummary.connectedToCluster;

                    // update the cluster summary
                    nf.ng.Bridge.injector.get('flowStatusCtrl').updateClusterSummary(clusterSummary);

                    // get the auto refresh interval
                    var autoRefreshIntervalSeconds = parseInt(configDetails.autoRefreshIntervalSeconds, 10);

                    // record whether we can configure the authorizer
                    configurableAuthorizer = configDetails.supportsConfigurableAuthorizer;

                    // init storage
                    nf.Storage.init();

                    // initialize the application
                    initCanvas();
                    nf.Canvas.View.init();
                    nf.ContextMenu.init();
                    nf.ng.Bridge.injector.get('headerCtrl').init();
                    nf.Settings.init();
                    nf.Actions.init();
                    nf.QueueListing.init();
                    nf.ComponentState.init();

                    // initialize the component behaviors
                    nf.Draggable.init();
                    nf.Selectable.init();
                    nf.Connectable.init();

                    // initialize the chart
                    nf.StatusHistory.init(configDetails.timeOffset);

                    // initialize the birdseye
                    nf.Birdseye.init();

                    // initialize components
                    nf.ConnectionConfiguration.init();
                    nf.ControllerService.init();
                    nf.ReportingTask.init();
                    nf.PolicyManagement.init();
                    nf.ProcessorConfiguration.init();
                    nf.ProcessGroupConfiguration.init();
                    nf.RemoteProcessGroupConfiguration.init();
                    nf.RemoteProcessGroupPorts.init();
                    nf.PortConfiguration.init();
                    nf.LabelConfiguration.init();
                    nf.ProcessorDetails.init();
                    nf.PortDetails.init();
                    nf.ConnectionDetails.init();
                    nf.RemoteProcessGroupDetails.init();
                    nf.GoTo.init();
                    nf.Graph.init().done(function () {
                        nf.ng.Bridge.injector.get('graphControlsCtrl').init();

                        // determine the split between the polling
                        var pollingSplit = autoRefreshIntervalSeconds / 2;

                        // register the polling
                        setTimeout(function () {
                            startPolling(autoRefreshIntervalSeconds);
                        }, pollingSplit * 1000);

                        // hide the splash screen
                        nf.Canvas.hideSplash();
                    }).fail(nf.Common.handleAjaxError);
                }).fail(nf.Common.handleAjaxError);
            }).fail(nf.Common.handleAjaxError);
        },

        /**
         * Return whether this instance of NiFi is clustered.
         *
         * @returns {Boolean}
         */
        isClustered: function () {
            return clustered === true;
        },

        /**
         * Return whether this instance is connected to a cluster.
         *
         * @returns {boolean}
         */
        isConnectedToCluster: function () {
            return connectedToCluster === true;
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
         * Returns whether the authorizer is configurable.
         */
        isConfigurableAuthorizer: function () {
            return configurableAuthorizer;
        },

        /**
         * Whether the current user can write in this group.
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

        View: (function () {

            /**
             * Updates component visibility based on their proximity to the screen's viewport.
             */
            var updateComponentVisibility = function () {
                var canvasContainer = $('#canvas-container');
                var translate = nf.Canvas.View.translate();
                var scale = nf.Canvas.View.scale();

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
                    if (!nf.Canvas.View.shouldRenderPerScale()) {
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
                    if (!nf.Canvas.View.shouldRenderPerScale()) {
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
                var graph = nf.Graph.get();

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
                            nf.ContextMenu.hide();
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
                            refreshed = nf.Canvas.View.refresh({
                                persist: false,
                                transition: transition,
                                refreshComponents: false,
                                refreshBirdseye: false
                            });
                        })
                        .on('zoomend', function () {
                            // ensure the canvas was actually refreshed
                            if (nf.Common.isDefinedAndNotNull(refreshed)) {
                                nf.Canvas.View.updateVisibility();

                                // refresh the birdseye
                                refreshed.done(function () {
                                    nf.Birdseye.refresh();
                                });

                                // persist the users view
                                nf.CanvasUtils.persistUserView();

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
                    return nf.Canvas.View.scale() >= MIN_SCALE_TO_RENDER;
                },

                /**
                 * Updates component visibility based on the current translation/scale.
                 */
                updateVisibility: function () {
                    updateComponentVisibility();
                    nf.Graph.pan();
                },

                /**
                 * Sets/gets the current translation.
                 *
                 * @param {array} translate     [x, y]
                 */
                translate: function (translate) {
                    if (nf.Common.isUndefined(translate)) {
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
                    if (nf.Common.isUndefined(scale)) {
                        return behavior.scale();
                    } else {
                        behavior.scale(scale);
                    }
                },

                /**
                 * Zooms in a single zoom increment.
                 */
                zoomIn: function () {
                    var translate = nf.Canvas.View.translate();
                    var scale = nf.Canvas.View.scale();
                    var newScale = Math.min(scale * INCREMENT, MAX_SCALE);

                    // get the canvas normalized width and height
                    var canvasContainer = $('#canvas-container');
                    var screenWidth = canvasContainer.width() / scale;
                    var screenHeight = canvasContainer.height() / scale;

                    // adjust the scale
                    nf.Canvas.View.scale(newScale);

                    // center around the center of the screen accounting for the translation accordingly
                    nf.CanvasUtils.centerBoundingBox({
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
                    var translate = nf.Canvas.View.translate();
                    var scale = nf.Canvas.View.scale();
                    var newScale = Math.max(scale / INCREMENT, MIN_SCALE);

                    // get the canvas normalized width and height
                    var canvasContainer = $('#canvas-container');
                    var screenWidth = canvasContainer.width() / scale;
                    var screenHeight = canvasContainer.height() / scale;

                    // adjust the scale
                    nf.Canvas.View.scale(newScale);

                    // center around the center of the screen accounting for the translation accordingly
                    nf.CanvasUtils.centerBoundingBox({
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
                    var translate = nf.Canvas.View.translate();
                    var scale = nf.Canvas.View.scale();
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
                    var graphTop = (graphBox.top - nf.Canvas.CANVAS_OFFSET) / scale;


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
                    nf.Canvas.View.scale(newScale);

                    // center as appropriate
                    nf.CanvasUtils.centerBoundingBox({
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
                    var translate = nf.Canvas.View.translate();
                    var scale = nf.Canvas.View.scale();

                    // get the first selected component
                    var selection = nf.CanvasUtils.getSelection();

                    // set the updated scale
                    nf.Canvas.View.scale(1);

                    // box to zoom towards
                    var box;

                    // if components have been selected position the view accordingly
                    if (!selection.empty()) {
                        // gets the data for the first component
                        var selectionBox = selection.node().getBoundingClientRect();

                        // get the bounding box for the selected components
                        box = {
                            x: (selectionBox.left / scale) - (translate[0] / scale),
                            y: ((selectionBox.top - nf.Canvas.CANVAS_OFFSET) / scale) - (translate[1] / scale),
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
                    nf.CanvasUtils.centerBoundingBox(box);
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
                        if (nf.Common.isDefinedAndNotNull(options)) {
                            persist = nf.Common.isDefinedAndNotNull(options.persist) ? options.persist : persist;
                            transition = nf.Common.isDefinedAndNotNull(options.transition) ? options.transition : transition;
                            refreshComponents = nf.Common.isDefinedAndNotNull(options.refreshComponents) ? options.refreshComponents : refreshComponents;
                            refreshBirdseye = nf.Common.isDefinedAndNotNull(options.refreshBirdseye) ? options.refreshBirdseye : refreshBirdseye;
                        }

                        // update component visibility
                        if (refreshComponents) {
                            nf.Canvas.View.updateVisibility();
                        }

                        // persist if appropriate
                        if (persist === true) {
                            nf.CanvasUtils.persistUserView();
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
                                        nf.Birdseye.refresh();
                                    }

                                    deferred.resolve();
                                });
                        } else {
                            canvas.attr('transform', function () {
                                return 'translate(' + behavior.translate() + ') scale(' + behavior.scale() + ')';
                            });

                            // refresh birdseye if appropriate
                            if (refreshBirdseye === true) {
                                nf.Birdseye.refresh();
                            }

                            deferred.resolve();
                        }
                    }).promise();
                }
            };
        }())
    };
}());