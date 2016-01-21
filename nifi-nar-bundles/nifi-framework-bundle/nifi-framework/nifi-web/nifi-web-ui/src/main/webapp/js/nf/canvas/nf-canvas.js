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
        // initialize the NiFi
        nf.Canvas.init();
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

    var revisionPolling = false;
    var statusPolling = false;
    var groupId = 'root';
    var groupName = null;
    var parentGroupId = null;
    var secureSiteToSite = false;
    var clustered = false;
    var svg = null;
    var canvas = null;

    var canvasClicked = false;
    var panning = false;

    var config = {
        urls: {
            identity: '../nifi-api/controller/identity',
            authorities: '../nifi-api/controller/authorities',
            revision: '../nifi-api/controller/revision',
            status: '../nifi-api/controller/status',
            bulletinBoard: '../nifi-api/controller/bulletin-board',
            banners: '../nifi-api/controller/banners',
            controller: '../nifi-api/controller',
            controllerConfig: '../nifi-api/controller/config',
            accessConfig: '../nifi-api/access/config',
            cluster: '../nifi-api/cluster',
            d3Script: 'js/d3/d3.min.js'
        }
    };

    /**
     * Generates the breadcrumbs.
     * 
     * @argument {object} processGroup      The process group
     */
    var generateBreadcrumbs = function (processGroup) {
        // create the link for loading the correct group
        var groupLink = $('<span class="link"></span>').text(processGroup.name).click(function () {
            nf.CanvasUtils.enterGroup(processGroup.id);
        });

        // make the current group bold
        if (nf.Canvas.getGroupId() === processGroup.id) {
            groupLink.css('font-weight', 'bold');
        }

        // if there is a parent, create the appropriate mark up
        if (nf.Common.isDefinedAndNotNull(processGroup.parent)) {
            var separator = $('<span>&raquo;</span>').css({
                'color': '#598599',
                'margin': '0 10px'
            });
            $('#data-flow-title-container').append(generateBreadcrumbs(processGroup.parent)).append(separator);
        }

        // append this link
        $('#data-flow-title-container').append(groupLink);
        return groupLink;
    };

    /**
     * Loads D3.
     */
    var loadD3 = function () {
        return nf.Common.cachedScript(config.urls.d3Script);
    };

    /**
     * Starts polling for the revision.
     * 
     * @argument {int} autoRefreshInterval      The auto refresh interval
     */
    var startRevisionPolling = function (autoRefreshInterval) {
        // set polling flag
        revisionPolling = true;
        pollForRevision(autoRefreshInterval);
    };

    /**
     * Polls for the revision.
     * 
     * @argument {int} autoRefreshInterval      The auto refresh interval
     */
    var pollForRevision = function (autoRefreshInterval) {
        // ensure we're suppose to poll
        if (revisionPolling) {
            // check the revision
            checkRevision().done(function () {
                // start the wait to poll again
                setTimeout(function () {
                    pollForRevision(autoRefreshInterval);
                }, autoRefreshInterval * 1000);
            });
        }
    };

    /**
     * Start polling for the status.
     * 
     * @argument {int} autoRefreshInterval      The auto refresh interval
     */
    var startStatusPolling = function (autoRefreshInterval) {
        // set polling flag
        statusPolling = true;
        pollForStatus(autoRefreshInterval);
    };

    /**
     * Register the status poller.
     * 
     * @argument {int} autoRefreshInterval      The auto refresh interval
     */
    var pollForStatus = function (autoRefreshInterval) {
        // ensure we're suppose to poll
        if (statusPolling) {
            // reload the status
            nf.Canvas.reloadStatus().done(function () {
                // start the wait to poll again
                setTimeout(function () {
                    pollForStatus(autoRefreshInterval);
                }, autoRefreshInterval * 1000);
            });
        }
    };

    /**
     * Checks the current revision against this version of the flow.
     */
    var checkRevision = function () {
        // get the revision
        return $.ajax({
            type: 'GET',
            url: config.urls.revision,
            dataType: 'json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.revision)) {
                var revision = response.revision;
                var currentRevision = nf.Client.getRevision();

                // if there is a newer revision, there are outstanding
                // changes that need to be updated
                if (revision.version > currentRevision.version && revision.clientId !== currentRevision.clientId) {
                    var refreshContainer = $('#refresh-required-container');
                    var settingsRefreshIcon = $('#settings-refresh-required-icon');

                    // insert the refresh needed text in the canvas - if necessary
                    if (!refreshContainer.is(':visible')) {
                        $('#stats-last-refreshed').addClass('alert');
                        var refreshMessage = "This flow has been modified by '" + revision.lastModifier + "'. Please refresh.";

                        // update the tooltip
                        var refreshRequiredIcon = $('#refresh-required-icon');
                        if (refreshRequiredIcon.data('qtip')) {
                            refreshRequiredIcon.qtip('option', 'content.text', refreshMessage);
                        } else {
                            refreshRequiredIcon.qtip($.extend({
                                content: refreshMessage
                            }, nf.CanvasUtils.config.systemTooltipConfig));
                        }

                        refreshContainer.show();
                    }

                    // insert the refresh needed text in the settings - if necessary
                    if (!settingsRefreshIcon.is(':visible')) {
                        $('#settings-last-refreshed').addClass('alert');
                        settingsRefreshIcon.show();
                    }
                }
            }
        }).fail(nf.Common.handleAjaxError);
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
                .data(['normal', 'ghost'])
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
                        } else {
                            return '#000000';
                        }
                    }
                })
                .append('path')
                .attr('d', 'M2,3 L0,6 L6,3 L0,0 z');

        // define the gradient for the processor stats background
        var processGroupStatsBackground = defs.append('linearGradient')
                .attr({
                    'id': 'process-group-stats-background',
                    'x1': '0%',
                    'y1': '100%',
                    'x2': '0%',
                    'y2': '0%'
                });

        processGroupStatsBackground.append('stop')
                .attr({
                    'offset': '0%',
                    'stop-color': '#dedede'
                });

        processGroupStatsBackground.append('stop')
                .attr({
                    'offset': '50%',
                    'stop-color': '#ffffff'
                });

        processGroupStatsBackground.append('stop')
                .attr({
                    'offset': '100%',
                    'stop-color': '#dedede'
                });

        // define the gradient for the processor stats background
        var processorStatsBackground = defs.append('linearGradient')
                .attr({
                    'id': 'processor-stats-background',
                    'x1': '0%',
                    'y1': '100%',
                    'x2': '0%',
                    'y2': '0%'
                });

        processorStatsBackground.append('stop')
                .attr({
                    'offset': '0%',
                    'stop-color': '#6f97ac'
                });

        processorStatsBackground.append('stop')
                .attr({
                    'offset': '100%',
                    'stop-color': '#30505c'
                });

        // define the gradient for the port background
        var portBackground = defs.append('linearGradient')
                .attr({
                    'id': 'port-background',
                    'x1': '0%',
                    'y1': '100%',
                    'x2': '0%',
                    'y2': '0%'
                });

        portBackground.append('stop')
                .attr({
                    'offset': '0%',
                    'stop-color': '#aaaaaa'
                });

        portBackground.append('stop')
                .attr({
                    'offset': '100%',
                    'stop-color': '#ffffff'
                });

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
                                    d.component.position.x >= selectionBoundingBox.x && (d.component.position.x + d.dimensions.width) <= (selectionBoundingBox.x + selectionBoundingBox.width) &&
                                    d.component.position.y >= selectionBoundingBox.y && (d.component.position.y + d.dimensions.height) <= (selectionBoundingBox.y + selectionBoundingBox.height);
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

                    // update the toolbar
                    nf.CanvasToolbar.refresh();
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
                'width': canvasContainer.width()
            });

            // body
            $('#canvas-body').css({
                'height': windowHeight + 'px',
                'width': $(window).width() + 'px'
            });
        };

        // listen for browser resize events to reset the graph size
        $(window).on('resize', function (e) {
            if (e.target === window) {
                updateGraphSize();
                nf.Settings.resetTableSize();
            }
        }).on('keydown', function (evt) {
            var isCtrl = evt.ctrlKey || evt.metaKey;

            // consider escape, before checking dialogs
            if (!isCtrl && evt.keyCode === 27) {
                // esc

                // prevent escape when a property value is being edited and it is unable to close itself 
                // (due to focus loss on field) - allowing this to continue would could cause other
                // unsaved changes to be lost as it would end up cancel the entire configuration dialog
                // not just the field itself
                if ($('div.value-combo').is(':visible') || $('div.slickgrid-nfel-editor').is(':visible') || $('div.slickgrid-editor').is(':visible')) {
                    return;
                }

                // first consider read only property detail dialog
                if ($('div.property-detail').is(':visible')) {
                    nf.Common.removeAllPropertyDetailDialogs();

                    // prevent further bubbling as we're already handled it
                    evt.stopPropagation();
                    evt.preventDefault();
                } else {
                    var target = $(evt.target);
                    if (target.length) {
                        var isBody = target.get(0) === $('#canvas-body').get(0);
                        var inShell = target.closest('#shell-dialog').length;

                        // special handling for body and shell
                        if (isBody || inShell) {
                            var cancellables = $('.cancellable');
                            if (cancellables.length) {
                                var zIndexMax = null;
                                var dialogMax = null;

                                // identify the top most cancellable
                                $.each(cancellables, function (_, cancellable) {
                                    var dialog = $(cancellable);
                                    var zIndex = dialog.css('zIndex');

                                    // if the dialog has a zIndex consider it
                                    if (dialog.is(':visible') && nf.Common.isDefinedAndNotNull(zIndex)) {
                                        zIndex = parseInt(zIndex, 10);
                                        if (zIndexMax === null || zIndex > zIndexMax) {
                                            zIndexMax = zIndex;
                                            dialogMax = dialog;
                                        }
                                    }
                                });

                                // if we've identified a dialog to close do so and stop propagation
                                if (dialogMax !== null) {
                                    // hide the cancellable
                                    if (dialogMax.hasClass('modal')) {
                                        dialogMax.modal('hide');
                                    } else {
                                        dialogMax.hide();
                                    }

                                    // prevent further bubbling as we're already handled it
                                    evt.stopPropagation();
                                    evt.preventDefault();
                                }
                            }
                        } else {
                            // otherwise close the closest visible cancellable
                            var parentDialog = target.closest('.cancellable:visible').first();
                            if (parentDialog.length) {
                                if (parentDialog.hasClass('modal')) {
                                    parentDialog.modal('hide');
                                } else {
                                    parentDialog.hide();
                                }

                                // prevent further bubbling as we're already handled it
                                evt.stopPropagation();
                                evt.preventDefault();
                            }
                        }
                    }
                }

                return;
            }

            // if a dialog is open, disable canvas shortcuts
            if ($('.dialog').is(':visible')) {
                return;
            }

            // get the current selection
            var selection = nf.CanvasUtils.getSelection();

            // handle shortcuts
            if (isCtrl) {
                if (evt.keyCode === 82) {
                    // ctrl-r
                    nf.Actions.reloadStatus();

                    evt.preventDefault();
                } else if (evt.keyCode === 65) {
                    // ctrl-a
                    nf.Actions.selectAll();
                    nf.CanvasToolbar.refresh();

                    evt.preventDefault();
                } else if (evt.keyCode === 67) {
                    if (nf.Common.isDFM() && nf.CanvasUtils.isCopyable(selection)) {
                        // ctrl-c
                        nf.Actions.copy(selection);

                        evt.preventDefault();
                    }
                } else if (evt.keyCode === 86) {
                    if (nf.Common.isDFM() && nf.CanvasUtils.isPastable()) {
                        // ctrl-p
                        nf.Actions.paste(selection);

                        evt.preventDefault();
                    }
                }
            } else {
                if (evt.keyCode === 46) {
                    if (nf.Common.isDFM() && nf.CanvasUtils.isDeletable(selection)) {
                        // delete
                        nf.Actions['delete'](selection);

                        evt.preventDefault();
                    }
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
                    // update the header text
                    $('#banner-header').addClass('banner-header-background').text(response.banners.headerText);
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
     * Sets the colors for the specified type.
     * 
     * @param {array} colors The possible colors
     * @param {string} type The component type for these colors
     */
    var setColors = function (colors, type) {
        var defs = d3.select('defs');

        // update processors
        var processorSelection = defs.selectAll('linearGradient.' + type + '-background').data(colors, function (d) {
            return d;
        });

        // define the gradient for the processor background
        var gradient = processorSelection.enter().append('linearGradient')
                .attr({
                    'id': function (d) {
                        return type + '-background-' + d;
                    },
                    'class': type + '-background',
                    'x1': '0%',
                    'y1': '100%',
                    'x2': '0%',
                    'y2': '0%'
                });

        gradient.append('stop')
                .attr({
                    'offset': '0%',
                    'stop-color': function (d) {
                        return '#' + d;
                    }
                });

        gradient.append('stop')
                .attr({
                    'offset': '100%',
                    'stop-color': '#ffffff'
                });

        // remove old processor colors
        processorSelection.exit().remove();
    };

    /**
     * Reloads the current status of this flow.
     */
    var reloadFlowStatus = function () {
        return $.ajax({
            type: 'GET',
            url: config.urls.status,
            dataType: 'json'
        }).done(function (response) {
            // report the updated status
            if (nf.Common.isDefinedAndNotNull(response.controllerStatus)) {
                var controllerStatus = response.controllerStatus;

                // update the report values
                $('#active-thread-count').text(controllerStatus.activeThreadCount);
                $('#total-queued').text(controllerStatus.queued);

                // update the connected nodes if applicable
                if (nf.Common.isDefinedAndNotNull(controllerStatus.connectedNodes)) {
                    var connectedNodes = controllerStatus.connectedNodes.split(' / ');
                    if (connectedNodes.length === 2 && connectedNodes[0] !== connectedNodes[1]) {
                        $('#connected-nodes-count').addClass('alert');
                    } else {
                        $('#connected-nodes-count').removeClass('alert');
                    }

                    // set the connected nodes
                    $('#connected-nodes-count').text(controllerStatus.connectedNodes);
                }

                // update the component counts
                if (nf.Common.isDefinedAndNotNull(controllerStatus.activeRemotePortCount)) {
                    $('#controller-transmitting-count').text(controllerStatus.activeRemotePortCount);
                } else {
                    $('#controller-transmitting-count').text('-');
                }
                if (nf.Common.isDefinedAndNotNull(controllerStatus.inactiveRemotePortCount)) {
                    $('#controller-not-transmitting-count').text(controllerStatus.inactiveRemotePortCount);
                } else {
                    $('#controller-not-transmitting-count').text('-');
                }
                if (nf.Common.isDefinedAndNotNull(controllerStatus.runningCount)) {
                    $('#controller-running-count').text(controllerStatus.runningCount);
                } else {
                    $('#controller-running-count').text('-');
                }
                if (nf.Common.isDefinedAndNotNull(controllerStatus.stoppedCount)) {
                    $('#controller-stopped-count').text(controllerStatus.stoppedCount);
                } else {
                    $('#controller-stopped-count').text('-');
                }
                if (nf.Common.isDefinedAndNotNull(controllerStatus.invalidCount)) {
                    $('#controller-invalid-count').text(controllerStatus.invalidCount);
                } else {
                    $('#controller-invalid-count').text('-');
                }
                if (nf.Common.isDefinedAndNotNull(controllerStatus.disabledCount)) {
                    $('#controller-disabled-count').text(controllerStatus.disabledCount);
                } else {
                    $('#controller-disabled-count').text('-');
                }

                // icon for system bulletins
                var bulletinIcon = $('#controller-bulletins');
                var currentBulletins = bulletinIcon.data('bulletins');

                // update the bulletins if necessary
                if (nf.Common.doBulletinsDiffer(currentBulletins, controllerStatus.bulletins)) {
                    bulletinIcon.data('bulletins', controllerStatus.bulletins);

                    // get the formatted the bulletins
                    var bulletins = nf.Common.getFormattedBulletins(controllerStatus.bulletins);

                    // bulletins for this processor are now gone
                    if (bulletins.length === 0) {
                        if (bulletinIcon.data('qtip')) {
                            bulletinIcon.removeClass('has-bulletins').qtip('api').destroy(true);
                        }

                        // hide the icon
                        bulletinIcon.hide();
                    } else {
                        var newBulletins = nf.Common.formatUnorderedList(bulletins);

                        // different bulletins, refresh
                        if (bulletinIcon.data('qtip')) {
                            bulletinIcon.qtip('option', 'content.text', newBulletins);
                        } else {
                            // no bulletins before, show icon and tips
                            bulletinIcon.addClass('has-bulletins').qtip($.extend({
                                content: newBulletins
                            }, nf.CanvasUtils.config.systemTooltipConfig));
                        }

                        // show the icon
                        bulletinIcon.show();
                    }
                }

                // update controller service and reporting task bulletins
                nf.Settings.setBulletins(controllerStatus.controllerServiceBulletins, controllerStatus.reportingTaskBulletins);

                // handle any pending user request
                if (controllerStatus.hasPendingAccounts === true) {
                    $('#has-pending-accounts').show();
                } else {
                    $('#has-pending-accounts').hide();
                }
            }
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Refreshes the graph.
     * 
     * @argument {string} processGroupId        The process group id
     */
    var reloadProcessGroup = function (processGroupId) {
        // load the controller
        return $.ajax({
            type: 'GET',
            url: config.urls.controller + '/process-groups/' + encodeURIComponent(processGroupId),
            data: {
                verbose: true
            },
            dataType: 'json'
        }).done(function (processGroupResponse) {
            // set the revision
            nf.Client.setRevision(processGroupResponse.revision);

            // get the controller and its contents
            var processGroup = processGroupResponse.processGroup;

            // set the group details
            nf.Canvas.setGroupId(processGroup.id);
            nf.Canvas.setGroupName(processGroup.name);

            // update the breadcrumbs
            $('#data-flow-title-container').empty();
            generateBreadcrumbs(processGroup);

            // set the parent id if applicable
            if (nf.Common.isDefinedAndNotNull(processGroup.parent)) {
                nf.Canvas.setParentGroupId(processGroup.parent.id);
            } else {
                nf.Canvas.setParentGroupId(null);
            }

            // since we're getting a new group, we want to clear it
            nf.Graph.removeAll();

            // refresh the graph
            nf.Graph.add(processGroup.contents, false);

            // update the toolbar
            nf.CanvasToolbar.refresh();
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Refreshes the status for the resources that exist in the specified process group.
     * 
     * @argument {string} processGroupId        The id of the process group
     */
    var reloadStatus = function (processGroupId) {
        // get the stats
        return  $.Deferred(function (deferred) {
            $.ajax({
                type: 'GET',
                url: config.urls.controller + '/process-groups/' + encodeURIComponent(processGroupId) + '/status',
                data: {
                    recursive: false
                },
                dataType: 'json'
            }).done(function (response) {
                // report the updated stats
                if (nf.Common.isDefinedAndNotNull(response.processGroupStatus)) {
                    var processGroupStatus = response.processGroupStatus;

                    // update all the stats
                    nf.Graph.setStatus(processGroupStatus);

                    // update the timestamp
                    $('#stats-last-refreshed').text(processGroupStatus.statsLastRefreshed);
                }
                deferred.resolve();
            }).fail(function (xhr, status, error) {
                // if clustered, a 404 likely means the flow status at the ncm is stale
                if (!nf.Canvas.isClustered() || xhr.status !== 404) {
                    nf.Common.handleAjaxError(xhr, status, error);
                    deferred.reject();
                } else {
                    deferred.resolve();
                }
            });
        }).promise();
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
         * Stop polling for revision.
         */
        stopRevisionPolling: function () {
            // set polling flag
            revisionPolling = false;
        },

        /**
         * Remove the status poller.
         */
        stopStatusPolling: function () {
            // set polling flag
            statusPolling = false;
        },

        /**
         * Reloads the flow from the server based on the currently specified group id.
         * To load another group, update nf.Canvas.setGroupId and call nf.Canvas.reload.
         */
        reload: function () {
            return $.Deferred(function (deferred) {
                // hide the context menu
                nf.ContextMenu.hide();

                // get the process group to refresh everything
                var processGroupXhr = reloadProcessGroup(nf.Canvas.getGroupId());
                var statusXhr = reloadFlowStatus();
                var settingsXhr = nf.Settings.loadSettings(false); // don't reload the status as we want to wait for deferreds to complete
                $.when(processGroupXhr, statusXhr, settingsXhr).done(function (processGroupResult) {
                    // adjust breadcrumbs if necessary
                    var title = $('#data-flow-title-container');
                    var titlePosition = title.position();
                    var titleWidth = title.outerWidth();
                    var titleRight = titlePosition.left + titleWidth;

                    var padding = $('#breadcrumbs-right-border').width();
                    var viewport = $('#data-flow-title-viewport');
                    var viewportWidth = viewport.width();
                    var viewportRight = viewportWidth - padding;

                    // if the title's right is past the viewport's right, shift accordingly
                    if (titleRight > viewportRight) {
                        // adjust the position
                        title.css('left', (titlePosition.left - (titleRight - viewportRight)) + 'px');
                    } else {
                        title.css('left', '10px');
                    }

                    // don't load the status until the graph is loaded
                    reloadStatus(nf.Canvas.getGroupId()).done(function () {
                        deferred.resolve(processGroupResult);
                    }).fail(function () {
                        deferred.reject();
                    });
                });
            }).promise();
        },

        /**
         * Reloads the status.
         */
        reloadStatus: function () {
            return $.Deferred(function (deferred) {
                // refresh the status and check any bulletins
                $.when(reloadStatus(nf.Canvas.getGroupId()), reloadFlowStatus(), checkRevision()).done(function () {
                    deferred.resolve();
                }).fail(function () {
                    deferred.reject();
                });
            }).promise();
        },

        /**
         * Initialize NiFi.
         */
        init: function () {
            // get the current user's identity
            var identityXhr = $.ajax({
                type: 'GET',
                url: config.urls.identity,
                dataType: 'json'
            });

            // get the current user's authorities
            var authoritiesXhr = $.ajax({
                type: 'GET',
                url: config.urls.authorities,
                dataType: 'json'
            });

            // load the identity and authorities for the current user
            var userXhr = $.Deferred(function (deferred) {
                $.when(authoritiesXhr, identityXhr).done(function (authoritiesResult, identityResult) {
                    var authoritiesResponse = authoritiesResult[0];
                    var identityResponse = identityResult[0];

                    // set the user's authorities
                    nf.Common.setAuthorities(authoritiesResponse.authorities);

                    // at this point the user may be themselves or anonymous

                    // if the user is logged, we want to determine if they were logged in using a certificate
                    if (identityResponse.identity !== 'anonymous') {
                        // rendner the users name
                        $('#current-user').text(identityResponse.identity).show();

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
                    if (xhr.status === 401 || xhr.status === 403) {
                        window.location = '/nifi/login';
                    } else {
                        deferred.reject(xhr, status, error);
                    }
                });
            }).promise();

            userXhr.done(function () {
                // get the controller config to register the status poller
                var configXhr = $.ajax({
                    type: 'GET',
                    url: config.urls.controllerConfig,
                    dataType: 'json'
                });

                // get the login config
                var loginXhr = $.ajax({
                    type: 'GET',
                    url: config.urls.accessConfig,
                    dataType: 'json'
                });

                // create the deferred cluster request
                var isClusteredRequest = $.Deferred(function (deferred) {
                    $.ajax({
                        type: 'HEAD',
                        url: config.urls.cluster
                    }).done(function (response, status, xhr) {
                        clustered = true;
                        deferred.resolve(response, status, xhr);
                    }).fail(function (xhr, status, error) {
                        if (xhr.status === 404) {
                            clustered = false;
                            deferred.resolve('', 'success', xhr);
                        } else {
                            deferred.reject(xhr, status, error);
                        }
                    });
                }).promise();

                // ensure the config requests are loaded
                $.when(configXhr, loginXhr, userXhr).done(function (configResult, loginResult) {
                    var configResponse = configResult[0];
                    var loginResponse = loginResult[0];

                    // calculate the canvas offset
                    var canvasContainer = $('#canvas-container');
                    nf.Canvas.CANVAS_OFFSET = canvasContainer.offset().top;

                    // get the config details
                    var configDetails = configResponse.config;
                    var loginDetails = loginResponse.config;

                    // store the content viewer url if available
                    if (!nf.Common.isBlank(configDetails.contentViewerUrl)) {
                        $('#nifi-content-viewer-url').text(configDetails.contentViewerUrl);
                    }

                    // when both request complete, load the application
                    isClusteredRequest.done(function () {
                        // get the auto refresh interval
                        var autoRefreshIntervalSeconds = parseInt(configDetails.autoRefreshIntervalSeconds, 10);

                        // initialize whether site to site is secure
                        secureSiteToSite = configDetails.siteToSiteSecure;

                        // load d3
                        loadD3().done(function () {
                            nf.Storage.init();

                            // initialize the application
                            initCanvas();
                            nf.Canvas.View.init();
                            nf.ContextMenu.init();
                            nf.CanvasToolbar.init();
                            nf.CanvasToolbox.init();
                            nf.CanvasHeader.init(loginDetails.supportsLogin);
                            nf.GraphControl.init();
                            nf.Search.init();
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
                            nf.ProcessorConfiguration.init();
                            nf.ProcessGroupConfiguration.init();
                            nf.RemoteProcessGroupConfiguration.init();
                            nf.RemoteProcessGroupPorts.init();
                            nf.PortConfiguration.init();
                            nf.SecurePortConfiguration.init();
                            nf.LabelConfiguration.init();
                            nf.ProcessorDetails.init();
                            nf.ProcessGroupDetails.init();
                            nf.PortDetails.init();
                            nf.SecurePortDetails.init();
                            nf.ConnectionDetails.init();
                            nf.RemoteProcessGroupDetails.init();
                            nf.GoTo.init();
                            nf.Graph.init().done(function () {
                                // determine the split between the polling
                                var pollingSplit = autoRefreshIntervalSeconds / 2;

                                // register the revision and status polling
                                startRevisionPolling(autoRefreshIntervalSeconds);
                                setTimeout(function () {
                                    startStatusPolling(autoRefreshIntervalSeconds);
                                }, pollingSplit * 1000);

                                // hide the splash screen
                                nf.Canvas.hideSplash();
                            }).fail(nf.Common.handleAjaxError);
                        }).fail(nf.Common.handleAjaxError);
                    }).fail(nf.Common.handleAjaxError);
                }).fail(nf.Common.handleAjaxError);
            }).fail(nf.Common.handleAjaxError);
        },

        /**
         * Defines the gradient colors used to render processors.
         * 
         * @param {array} colors The colors
         */
        defineProcessorColors: function (colors) {
            setColors(colors, 'processor');
        },

        /**
         * Defines the gradient colors used to render label.
         * 
         * @param {array} colors The colors
         */
        defineLabelColors: function (colors) {
            setColors(colors, 'label');
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
         * Returns whether site to site communications is secure.
         */
        isSecureSiteToSite: function () {
            return secureSiteToSite;
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

                    var left = d.component.position.x;
                    var top = d.component.position.y;
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
                    var selection = d3.select('#id-' + d.component.id);
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