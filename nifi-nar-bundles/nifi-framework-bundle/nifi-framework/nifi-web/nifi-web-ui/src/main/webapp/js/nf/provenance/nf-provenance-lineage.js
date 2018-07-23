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
                'd3',
                'nf.Common',
                'nf.Dialog',
                'nf.ErrorHandler'],
            function ($, d3, nfCommon, nfDialog, nfErrorHandler) {
                return (nf.ng.ProvenanceLineage = factory($, d3, nfCommon, nfDialog, nfErrorHandler));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.ProvenanceLineage =
            factory(require('jquery'),
                require('d3'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.ErrorHandler')));
    } else {
        nf.ng.ProvenanceLineage = factory(root.$,
            root.d3,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.ErrorHandler);
    }
}(this, function ($, d3, nfCommon, nfDialog, nfErrorHandler) {
    'use strict';

    var mySelf = function () {
        'use strict';

        /**
         * Configuration object used to hold a number of configuration items.
         */
        var config = {
            sliderTickCount: 75,
            urls: {
                lineage: '../nifi-api/provenance/lineage'

            }
        };

        /**
         * Initializes the lineage query dialog.
         */
        var initLineageQueryDialog = function () {
            // initialize the dialog
            $('#lineage-query-dialog').modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'Computing FlowFile lineage...'
            });
        };

        var downloadSvgFile = function (svgString) {
            var link = document.getElementById("image-download-link");
            var downloadSupported = typeof link.download != 'undefined';
            var fileName = 'lineage.svg';

            if (downloadSupported) {
                var DOMURL = self.URL || self.webkitURL || self;
                var svg = new Blob([svgString], {type: "image/svg+xml;charset=utf-8"});

                if (window.navigator.msSaveOrOpenBlob) {
                    window.navigator.msSaveOrOpenBlob(svg, fileName);
                } else {
                    var url = DOMURL.createObjectURL(svg);
                    link.href = url;
                    link.download = fileName;
                    link.click();
                }
            } else {
                window.open('data:image/svg+xml;charset=utf-8,' + encodeURI(svgString));
            }
        };

        /**
         * Appends the items to the context menu.
         *
         * items = [{class: ..., text: ..., click: function() {...}}, ...]
         *
         * @param {array} items
         */
        var addContextMenuItems = function (items) {
            var contextMenu = $('#provenance-lineage-context-menu');

            $.each(items, function (_, item) {
                if (typeof item.click === 'function') {
                    var menuItem = $('<div class="context-menu-item"></div>').on('click', item.click).on('mouseenter', function () {
                        $(this).addClass('hover');
                    }).on('mouseleave', function () {
                        $(this).removeClass('hover');
                    }).appendTo(contextMenu);

                    // add the img and the text
                    $('<div class="context-menu-item-img"></div>').addClass(item['class']).appendTo(menuItem);
                    $('<div class="context-menu-item-text"></div>').text(item['text']).appendTo(menuItem);
                    $('<div class="clear"></div>').appendTo(menuItem);
                }
            });
        };

        /**
         * Submits the specified lineage request.
         *
         * @param {type} lineageRequest
         * @returns {deferred}
         */
        var submitLineage = function (lineageRequest) {
            var lineageEntity = {
                'lineage': {
                    'request': lineageRequest
                }
            };

            return $.ajax({
                type: 'POST',
                url: config.urls.lineage,
                data: JSON.stringify(lineageEntity),
                dataType: 'json',
                contentType: 'application/json'
            }).fail(nfErrorHandler.handleAjaxError);
        };

        /**
         * Gets the specified lineage.
         *
         * @param {type} lineage
         * @returns {deferred}
         */
        var getLineage = function (lineage) {
            var url = lineage.uri;
            if (nfCommon.isDefinedAndNotNull(lineage.request.clusterNodeId)) {
                url += '?' + $.param({
                        clusterNodeId: lineage.request.clusterNodeId
                    });
            }

            return $.ajax({
                type: 'GET',
                url: url,
                dataType: 'json'
            }).fail(nfErrorHandler.handleAjaxError);
        };

        /**
         * Cancels the specified lineage.
         *
         * @param {type} lineage
         * @returns {deferred}
         */
        var cancelLineage = function (lineage) {
            var url = lineage.uri;
            if (nfCommon.isDefinedAndNotNull(lineage.request.clusterNodeId)) {
                url += '?' + $.param({
                        clusterNodeId: lineage.request.clusterNodeId
                    });
            }

            return $.ajax({
                type: 'DELETE',
                url: url,
                dataType: 'json'
            }).fail(nfErrorHandler.handleAjaxError);
        };

        var DEFAULT_NODE_SPACING = 100;
        var DEFAULT_LEVEL_DIFFERENCE = 120;

        /**
         * Renders the lineage in the specified results.
         *
         * @param {object} lineageResults
         * @param {integer} eventId
         * @param {string} clusterNodeId    The id of the node in the cluster where this event/flowfile originated
         */
        var renderLineage = function (lineageResults, eventId, clusterNodeId, provenanceTableCtrl) {
            // get the container
            var lineageContainer = $('#provenance-lineage');
            var width = lineageContainer.width();
            var height = lineageContainer.height();

            // record the min/max event time
            var minMillis;
            var minTimestamp;
            var maxMillis;

            // data lookups
            var nodeLookup = d3.map();
            var linkLookup = d3.map();

            var locateDescendants = function (nodeIds, descendants, depth) {
                $.each(nodeIds, function (_, nodeId) {
                    var node = nodeLookup.get(nodeId);

                    var children = [];
                    $.each(node.outgoing, function (_, link) {
                        children.push(link.target.id);
                        descendants.add(link.target.id);
                    });

                    if (nfCommon.isUndefined(depth)) {
                        locateDescendants(children, descendants);
                    } else if (depth > 1) {
                        locateDescendants(children, descendants, depth - 1);
                    }
                });
            };

            var positionNodes = function (nodeIds, depth, parents, levelDifference) {
                var immediateSet = d3.set(nodeIds);
                var childSet = d3.set();
                var descendantSet = d3.set();

                // locate children
                locateDescendants(nodeIds, childSet, 1);

                // locate all descendants (including children)
                locateDescendants(nodeIds, descendantSet);

                // push off processing a node until its deepest point
                // by removing any descendants from the immediate nodes.
                // in this case, a link is panning multiple levels
                descendantSet.each(function (d) {
                    immediateSet.remove(d);
                });

                // convert the children to an array to ensure consistent
                // order when performing index of checks below
                var children = childSet.values().sort(d3.descending);

                // convert the immediate to allow for sorting below
                var immediate = immediateSet.values();

                // attempt to identify fan in/out cases
                var nodesWithTwoParents = 0;
                $.each(immediate, function (_, nodeId) {
                    var node = nodeLookup.get(nodeId);

                    // identify fanning cases
                    if (node.incoming.length > 3) {
                        levelDifference = DEFAULT_LEVEL_DIFFERENCE;
                    } else if (node.incoming.length >= 2) {
                        nodesWithTwoParents++;
                    }
                });

                // increate the level difference if more than two nodes have two or more parents
                if (nodesWithTwoParents > 2) {
                    levelDifference = DEFAULT_LEVEL_DIFFERENCE;
                }

                // attempt to sort the nodes to provide an optimum layout
                if (parents.length === 1) {
                    immediate = immediate.sort(function (one, two) {
                        var oneNode = nodeLookup.get(one);
                        var twoNode = nodeLookup.get(two);

                        // try to order by children
                        if (oneNode.outgoing.length > 0 && twoNode.outgoing.length > 0) {
                            var oneIndex = children.indexOf(oneNode.outgoing[0].target.id);
                            var twoIndex = children.indexOf(twoNode.outgoing[0].target.id);
                            if (oneIndex !== twoIndex) {
                                return oneIndex - twoIndex;
                            }
                        }

                        // try to order by parents
                        if (oneNode.incoming.length > 0 && twoNode.incoming.length > 0) {
                            var oneIndex = oneNode.incoming[0].source.index;
                            var twoIndex = twoNode.incoming[0].source.index;
                            if (oneIndex !== twoIndex) {
                                return oneIndex - twoIndex;
                            }
                        }

                        // type of node
                        if (oneNode.type !== twoNode.type) {
                            return oneNode.type > twoNode.type ? 1 : -1;
                        }

                        // type of event
                        if (oneNode.eventType !== twoNode.eventType) {
                            return oneNode.eventType > twoNode.eventType ? 1 : -1;
                        }

                        // timestamp
                        return oneNode.millis - twoNode.millis;
                    });
                } else if (parents.length > 1) {
                    immediate = immediate.sort(function (one, two) {
                        var oneNode = nodeLookup.get(one);
                        var twoNode = nodeLookup.get(two);

                        // try to order by parents
                        if (oneNode.incoming.length > 0 && twoNode.incoming.length > 0) {
                            var oneIndex = oneNode.incoming[0].source.index;
                            var twoIndex = twoNode.incoming[0].source.index;
                            if (oneIndex !== twoIndex) {
                                return oneIndex - twoIndex;
                            }
                        }

                        // try to order by children
                        if (oneNode.outgoing.length > 0 && twoNode.outgoing.length > 0) {
                            var oneIndex = children.indexOf(oneNode.outgoing[0].target.id);
                            var twoIndex = children.indexOf(twoNode.outgoing[0].target.id);
                            if (oneIndex !== twoIndex) {
                                return oneIndex - twoIndex;
                            }
                        }

                        // node type
                        if (oneNode.type !== twoNode.type) {
                            return oneNode.type > twoNode.type ? 1 : -1;
                        }

                        // event type
                        if (oneNode.eventType !== twoNode.eventType) {
                            return oneNode.eventType > twoNode.eventType ? 1 : -1;
                        }

                        // timestamp
                        return oneNode.millis - twoNode.millis;
                    });
                }

                var originX = width / 2;
                if (parents.length > 0) {
                    originX = d3.mean(parents, function (parentId) {
                        var parent = nodeLookup.get(parentId);
                        return parent.x;
                    });
                }

                var depthWidth = (immediate.length - 1) * DEFAULT_NODE_SPACING;
                $.each(immediate, function (i, nodeId) {
                    var node = nodeLookup.get(nodeId);

                    // set the y position based on the depth
                    node.y = levelDifference + depth - 25;

                    // ensure the children won't position on top of one another
                    // based on the number of parent nodes
                    if (immediate.length <= parents.length) {
                        if (node.incoming.length === 1) {
                            var parent = node.incoming[0].source;
                            if (parent.outgoing.length === 1) {
                                node.x = parent.x;
                                return;
                            }
                        } else if (node.incoming.length > 1) {
                            var nodesOnPreviousLevel = $.grep(node.incoming, function (link) {
                                return (node.y - link.source.y) <= DEFAULT_LEVEL_DIFFERENCE;
                            });
                            node.x = d3.mean(nodesOnPreviousLevel, function (link) {
                                return link.source.x;
                            });
                            return;
                        }
                    }

                    // evenly space the nodes under the origin
                    node.x = (i * DEFAULT_NODE_SPACING) + originX - (depthWidth / 2);
                });

                // sort the immediate nodes after positioning by the x coordinate
                // so they can be shifted accordingly if necessary
                var sortedImmediate = immediate.slice().sort(function (one, two) {
                    var nodeOne = nodeLookup.get(one);
                    var nodeTwo = nodeLookup.get(two);
                    return nodeOne.x - nodeTwo.x;
                });

                // adjust the x positioning if necessary to avoid positioning on top
                // of one another, only need to consider the x coordinate since the
                // y coordinate will be the same for each node on this row
                for (var i = 0; i < sortedImmediate.length - 1; i++) {
                    var first = nodeLookup.get(sortedImmediate[i]);
                    var second = nodeLookup.get(sortedImmediate[i + 1]);
                    var difference = second.x - first.x;

                    if (difference < DEFAULT_NODE_SPACING) {
                        second.x += (DEFAULT_NODE_SPACING - difference);
                    }
                }

                // if there are children to position
                if (children.length > 0) {
                    var childLevelDifference = DEFAULT_LEVEL_DIFFERENCE / 3;

                    // resort the immediate values after each node has been positioned
                    immediate = immediate.sort(function (one, two) {
                        var oneNode = nodeLookup.get(one);
                        var twoNode = nodeLookup.get(two);
                        return oneNode.x - twoNode.x;
                    });

                    // mark each nodes index so subsequent recursive calls can position children accordingly
                    var nodesWithTwoChildren = 0;
                    $.each(immediate, function (i, nodeId) {
                        var node = nodeLookup.get(nodeId);
                        node.index = i;

                        // precompute the next level difference since we have easy access to going here
                        if (node.outgoing.length > 3) {
                            childLevelDifference = DEFAULT_LEVEL_DIFFERENCE;
                        } else if (node.outgoing.length >= 2) {
                            nodesWithTwoChildren++;
                        }
                    });

                    // if there are at least two immediate nodes with two or more children, increase the level difference
                    if (nodesWithTwoChildren > 2) {
                        childLevelDifference = DEFAULT_LEVEL_DIFFERENCE;
                    }

                    // position the children
                    positionNodes(children, levelDifference + depth, immediate, childLevelDifference);
                }
            };

            var addLineage = function (nodes, links, provenanceTableCtrl) {
                // add the new nodes
                $.each(nodes, function (_, node) {
                    if (nodeLookup.has(node.id)) {
                        return;
                    }

                    // add values to the node to support rendering
                    $.extend(node, {
                        x: 0,
                        y: 0,
                        visible: true
                    });

                    // store the node in a lookup
                    nodeLookup.set(node.id, node);
                });

                // add the new links
                $.each(links, function (_, link) {
                    // create the link object
                    var linkObj = {
                        id: link.sourceId + '-' + link.targetId,
                        source: nodeLookup.get(link.sourceId),
                        target: nodeLookup.get(link.targetId),
                        flowFileUuid: link.flowFileUuid,
                        millis: link.millis,
                        visible: true
                    };

                    linkLookup.set(linkObj.id, linkObj);
                });

                refresh(provenanceTableCtrl);
            };

            var refresh = function (provenanceTableCtrl) {
                // consider all nodes as starting points
                var startNodes = d3.set(nodeLookup.keys());

                // go through the nodes to reset their outgoing links
                nodeLookup.each(function (node, id) {
                    node.outgoing = [];
                    node.incoming = [];

                    // ensure this event has an event time
                    if (nfCommon.isUndefined(minMillis) || minMillis > node.millis) {
                        minMillis = node.millis;
                        minTimestamp = node.timestamp;
                    }
                    if (nfCommon.isUndefined(maxMillis) || maxMillis < node.millis) {
                        maxMillis = node.millis;
                    }
                });

                // go through the links in order to compute the new layout
                linkLookup.each(function (link, id) {
                    // updating the nodes connections
                    link.source.outgoing.push(link);
                    link.target.incoming.push(link);

                    // remove the target from being a potential starting node
                    startNodes.remove(link.target.id);
                });

                // position the nodes
                positionNodes(startNodes.values(), 1, [], 50);

                // update the slider min/max/step values
                var step = (maxMillis - minMillis) / config.sliderTickCount;
                slider.slider('option', 'min', minMillis).slider('option', 'max', maxMillis).slider('option', 'step', step > 0 ? step : 1).slider('value', maxMillis);

                // populate the event timeline
                $('#event-time').text(formatEventTime(maxMillis, provenanceTableCtrl));

                // update the layout
                update(provenanceTableCtrl);
            };

            // formats the specified millis
            var formatEventTime = function (millis, provenanceTableCtrl) {
                // get the current user time to properly convert the server time
                var now = new Date();

                // conver the user offset to millis
                var userTimeOffset = now.getTimezoneOffset() * 60 * 1000;

                // create the proper date by adjusting by the offsets
                var date = new Date(millis + userTimeOffset + provenanceTableCtrl.serverTimeOffset);
                return nfCommon.formatDateTime(date);
            };

            // handle context menu clicks...
            $('#provenance-lineage-context-menu').on('click', function () {
                $(this).hide().empty();
            });

            // handle zoom behavior
            var lineageZoom = d3.zoom()
                .scaleExtent([0.2, 8])
                .on('zoom', function () {
                    d3.select('g.lineage').attr('transform', function () {
                        return 'translate(' + d3.event.transform.x + ', ' + d3.event.transform.y + ') scale(' + d3.event.transform.k + ')';
                    });
                });

            // build the svg img
            var svg = d3.select('#provenance-lineage-container').append('svg:svg')
                .attr('width', '100%')
                .attr('height', '100%')
                .call(lineageZoom)
                .on('dblclick.zoom', null)
                .on('mousedown', function (d) {
                    // hide the context menu if necessary
                    d3.selectAll('circle.context').classed('context', false);
                    $('#provenance-lineage-context-menu').hide().empty();

                    // prevents browser from using text cursor
                    d3.event.preventDefault();
                })
                .on('contextmenu', function () {
                    var contextMenu = $('#provenance-lineage-context-menu');

                    // if there is something to show in the context menu
                    if (!contextMenu.is(':empty')) {
                        var position = d3.mouse(this);

                        // show the context menu
                        contextMenu.css({
                            'left': position[0] + 'px',
                            'top': position[1] + 'px'
                        }).show();
                    }

                    // prevent the native default context menu
                    d3.event.preventDefault();
                });

            svg.append('rect')
                .attrs({
                    'width': '100%',
                    'height': '100%',
                    'fill': '#f9fafb'
                });

            svg.append('defs').selectAll('marker')
                .data(['FLOWFILE', 'FLOWFILE-SELECTED', 'EVENT', 'EVENT-SELECTED'])
                .enter().append('marker')
                .attrs({
                    'id': function (d) {
                        return d;
                    },
                    'viewBox': '0 -3 6 6',
                    'refX': function (d) {
                        if (d.indexOf('FLOWFILE') >= 0) {
                            return 16;
                        } else {
                            return 11;
                        }
                    },
                    'refY': 0,
                    'markerWidth': 6,
                    'markerHeight': 6,
                    'orient': 'auto',
                    'fill': function (d) {
                        if (d.indexOf('SELECTED') >= 0) {
                            return '#ba554a';
                        } else {
                            return '#000000';
                        }
                    }
                })
                .append('path')
                .attr('d', 'M0,-3 L6,0 L0,3');

            // group everything together
            var lineageContainer = svg.append('g')
                .attrs({
                    'transform': 'translate(0, 0) scale(1)',
                    'pointer-events': 'all',
                    'class': 'lineage'
                });

            // select the nodes and links
            var nodes = lineageContainer.selectAll('g.node');
            var links = lineageContainer.selectAll('path.link');

            var previousMillis = maxMillis;
            var slide = function (event, ui) {
                if (previousMillis > ui.value) {
                    // the slider is descending

                    // determine the nodes to hide
                    var nodesToHide = nodes.filter(function (d) {
                        return d.millis > ui.value && d.millis <= previousMillis;
                    });
                    var linksToHide = links.filter(function (d) {
                        return d.millis > ui.value && d.millis <= previousMillis;
                    });

                    // hide applicable nodes and lines
                    nodesToHide.transition().delay(200).duration(400).style('opacity', 0);
                    linksToHide.transition().duration(400).style('opacity', 0);
                } else {
                    // the slider is ascending

                    // determine the nodes to show
                    var nodesToShow = nodes.filter(function (d) {
                        return d.millis <= ui.value && d.millis > previousMillis;
                    });
                    var linksToShow = links.filter(function (d) {
                        return d.millis <= ui.value && d.millis > previousMillis;
                    });

                    // show applicable nodes and lines
                    linksToShow.transition().delay(200).duration(400).style('opacity', 1);
                    nodesToShow.transition().duration(400).style('opacity', 1);
                }

                // update the event time
                $('#event-time').text(formatEventTime(ui.value, provenanceTableCtrl));

                // update the previous value
                previousMillis = ui.value;
            };

            // set up a slider for the showing the timeline of events
            var slider = $('#provenance-lineage-slider').slider({
                change: slide,
                slide: slide
            });

            // renders flowfile nodes
            var renderFlowFile = function (flowfiles) {
                flowfiles
                    .classed('flowfile', true)
                    .on('mousedown', function (d) {
                        d3.event.stopPropagation();
                    });

                // node
                flowfiles.append('circle')
                    .attrs({
                        'r': 16,
                        'fill': '#fff',
                        'stroke': '#000',
                        'stroke-width': 1.0
                    })
                    .on('mouseover', function (d) {
                        links.filter(function (linkDatum) {
                            return d.id === linkDatum.flowFileUuid;
                        })
                            .classed('selected', true)
                            .attr('marker-end', function (d) {
                                return 'url(#' + d.target.type + '-SELECTED)';
                            });
                    })
                    .on('mouseout', function (d) {
                        links.filter(function (linkDatum) {
                            return d.id === linkDatum.flowFileUuid;
                        }).classed('selected', false)
                            .attr('marker-end', function (d) {
                                return 'url(#' + d.target.type + ')';
                            });
                    });

                var icon = flowfiles.append('g')
                    .attrs({
                        'class': 'flowfile-icon',
                        'transform': function (d) {
                            return 'translate(-9,-9)';
                        }
                    }).append('text')
                    .attrs({
                        'font-family': 'flowfont',
                        'font-size': '18px',
                        'fill': '#ad9897',
                        'transform': function (d) {
                            return 'translate(0,15)';
                        }
                    })
                    .on('mouseover', function (d) {
                        links.filter(function (linkDatum) {
                            return d.id === linkDatum.flowFileUuid;
                        })
                            .classed('selected', true)
                            .attr('marker-end', function (d) {
                                return 'url(#' + d.target.type + '-SELECTED)';
                            });
                    })
                    .on('mouseout', function (d) {
                        links.filter(function (linkDatum) {
                            return d.id === linkDatum.flowFileUuid;
                        }).classed('selected', false)
                            .attr('marker-end', function (d) {
                                return 'url(#' + d.target.type + ')';
                            });
                    })
                    .text(function (d) {
                        return '\ue808';
                    });
            };

            var showContextMenu = function (d, provenanceTableCtrl) {
                // empty an previous contents - in case they right click on the
                // node twice without closing the previous context menu
                $('#provenance-lineage-context-menu').hide().empty();

                var menuItems = [{
                    'class': 'lineage-view-event',
                    'text': 'View details',
                    'click': function () {
                        provenanceTableCtrl.showEventDetails(d.id, clusterNodeId);
                    }
                }];

                // if this is a spawn event show appropriate actions
                if (d.eventType === 'SPAWN' || d.eventType === 'CLONE' || d.eventType === 'FORK' || d.eventType === 'JOIN' || d.eventType === 'REPLAY') {
                    // starts the lineage expansion process
                    var expandLineage = function (lineageRequest) {
                        var lineageProgress = $('#lineage-percent-complete');

                        // add support to cancel outstanding requests - when the button is pressed we
                        // could be in one of two stages, 1) waiting to GET the status or 2)
                        // in the process of GETting the status. Handle both cases by cancelling
                        // the setTimeout (1) and by setting a flag to indicate that a request has
                        // been request so we can ignore the results (2).

                        var cancelled = false;
                        var lineage = null;
                        var lineageTimer = null;

                        // update the progress bar value
                        provenanceTableCtrl.updateProgress(lineageProgress, 0);

                        // show the 'searching...' dialog
                        $('#lineage-query-dialog').modal('setButtonModel', [{
                            buttonText: 'Cancel',
                            color: {
                                base: '#E3E8EB',
                                hover: '#C7D2D7',
                                text: '#004849'
                            },
                            handler: {
                                click: function () {
                                    cancelled = true;

                                    // we are waiting for the next poll attempt
                                    if (lineageTimer !== null) {
                                        // cancel it
                                        clearTimeout(lineageTimer);

                                        // cancel the provenance
                                        closeDialog();
                                    }
                                }
                            }
                        }]).modal('show');


                        // closes the searching dialog and cancels the query on the server
                        var closeDialog = function () {
                            // cancel the provenance results since we've successfully processed the results
                            if (nfCommon.isDefinedAndNotNull(lineage)) {
                                cancelLineage(lineage);
                            }

                            // close the dialog
                            $('#lineage-query-dialog').modal('hide');
                        };

                        // polls for the event lineage
                        var pollLineage = function () {
                            getLineage(lineage).done(function (response) {
                                lineage = response.lineage;

                                // process the lineage
                                processLineage();
                            }).fail(closeDialog);
                        };

                        // processes the event lineage
                        var processLineage = function () {
                            // if the request was cancelled just ignore the current response
                            if (cancelled === true) {
                                closeDialog();
                                return;
                            }

                            // close the dialog if the results contain an error
                            if (!nfCommon.isEmpty(lineage.results.errors)) {
                                var errors = lineage.results.errors;
                                nfDialog.showOkDialog({
                                    headerText: 'Process Lineage',
                                    dialogContent: nfCommon.formatUnorderedList(errors)
                                });

                                closeDialog();
                                return;
                            }

                            // update the precent complete
                            provenanceTableCtrl.updateProgress(lineageProgress, lineage.percentCompleted);

                            // process the results if they are finished
                            if (lineage.finished === true) {
                                var results = lineage.results;

                                // ensure the events haven't aged off
                                if (results.nodes.length > 0) {
                                    // update the lineage graph
                                    renderEventLineage(results);
                                } else {
                                    // inform the user that no results were found
                                    nfDialog.showOkDialog({
                                        headerText: 'Lineage Results',
                                        dialogContent: 'The lineage search has completed successfully but there no results were found. The events may have aged off.'
                                    });
                                }

                                // close the searching.. dialog
                                closeDialog();
                            } else {
                                lineageTimer = setTimeout(function () {
                                    // clear the timer since we've been invoked
                                    lineageTimer = null;

                                    // for the lineage
                                    pollLineage();
                                }, 2000);
                            }
                        };

                        // once the query is submitted wait until its finished
                        submitLineage(lineageRequest).done(function (response) {
                            lineage = response.lineage;

                            // process the lineage, if its not done computing wait 1 second before checking again
                            processLineage(1);
                        }).fail(closeDialog);
                    };

                    // handles updating the lineage graph
                    var renderEventLineage = function (lineageResults) {
                        addLineage(lineageResults.nodes, lineageResults.links, provenanceTableCtrl);
                    };

                    // collapses the lineage for the specified event in the specified direction
                    var collapseLineage = function (eventId, provenanceTableCtrl) {
                        // get the event in question and collapse in the appropriate direction
                        provenanceTableCtrl.getEventDetails(eventId, clusterNodeId).done(function (response) {
                            var provenanceEvent = response.provenanceEvent;
                            var eventUuid = provenanceEvent.flowFileUuid;
                            var eventUuids = d3.set(provenanceEvent.childUuids);

                            // determines if the specified event should be removable based on if the collapsing is fanning in/out
                            var allowEventRemoval = function (fanIn, node) {
                                if (fanIn) {
                                    return node.id !== eventId;
                                } else {
                                    return node.flowFileUuid !== eventUuid && $.inArray(eventUuid, node.parentUuids) === -1;
                                }
                            };

                            // determines if the specified link should be removable based on if the collapsing is fanning in/out
                            var allowLinkRemoval = function (fanIn, link) {
                                if (fanIn) {
                                    return true;
                                } else {
                                    return link.flowFileUuid !== eventUuid;
                                }
                            };

                            // the event is fan in if the flowfile uuid is in the children
                            var fanIn = $.inArray(eventUuid, provenanceEvent.childUuids) >= 0;

                            // collapses the specified uuids
                            var collapse = function (uuids) {
                                var newUuids = false;

                                // consider each node for being collapsed
                                $.each(nodeLookup.values(), function (_, node) {
                                    // if this node is in the uuids remove it unless its the original event or is part of this and another lineage
                                    if (uuids.has(node.flowFileUuid) && allowEventRemoval(fanIn, node)) {
                                        // remove it from the look lookup
                                        nodeLookup.remove(node.id);

                                        // include all related outgoing flow file uuids
                                        $.each(node.outgoing, function (_, outgoing) {
                                            if (!uuids.has(outgoing.flowFileUuid)) {
                                                uuids.add(outgoing.flowFileUuid);
                                                newUuids = true;
                                            }
                                        });
                                    }
                                });

                                // update the link data
                                $.each(linkLookup.values(), function (_, link) {
                                    // if this link is in the uuids remove it
                                    if (uuids.has(link.flowFileUuid) && allowLinkRemoval(fanIn, link)) {
                                        // remove it from the link lookup
                                        linkLookup.remove(link.id);

                                        // add a related uuid that needs to be collapse
                                        var next = link.target;
                                        if (!uuids.has(next.flowFileUuid)) {
                                            uuids.add(next.flowFileUuid);
                                            newUuids = true;
                                        }
                                    }
                                });

                                // collapse any related uuids
                                if (newUuids) {
                                    collapse(uuids);
                                }
                            };

                            // collapse the specified uuids
                            collapse(eventUuids);

                            // update the layout
                            refresh(provenanceTableCtrl);
                        });
                    };

                    // add menu items
                    menuItems.push({
                        'class': 'lineage-view-parents',
                        'text': 'Find parents',
                        'click': function () {
                            expandLineage({
                                lineageRequestType: 'PARENTS',
                                eventId: d.id,
                                clusterNodeId: clusterNodeId
                            });
                        }
                    }, {
                        'class': 'lineage-view-children',
                        'text': 'Expand',
                        'click': function () {
                            expandLineage({
                                lineageRequestType: 'CHILDREN',
                                eventId: d.id,
                                clusterNodeId: clusterNodeId
                            });
                        }
                    }, {
                        'class': 'lineage-collapse-children',
                        'text': 'Collapse',
                        'click': function () {
                            // collapse the children lineage
                            collapseLineage(d.id, provenanceTableCtrl);
                        }
                    });
                }

                // show the context menu for an event
                addContextMenuItems(menuItems);
            };

            // renders event nodes
            var renderEvent = function (events, provenanceTableCtrl) {
                events
                    .on('contextmenu', function (d) {
                        // select the current node for a visible cue
                        d3.select('#event-node-' + d.id).classed('context', true);

                        // show the context menu
                        showContextMenu(d, provenanceTableCtrl);
                    })
                    .on('mousedown', function (d) {
                        d3.event.stopPropagation();
                    })
                    .on('dblclick', function (d) {
                        // show the event details
                        provenanceTableCtrl.showEventDetails(d.id, clusterNodeId);
                    });

                events
                    .classed('event', true)
                    // join node to its label
                    .append('rect')
                    .attrs({
                        'x': 0,
                        'y': -8,
                        'height': 16,
                        'width': 14,
                        'opacity': 0,
                        'id': function (d) {
                            return 'event-filler-' + d.id;
                        }
                    });

                events    
                    .append('circle')
                    .classed('selected', function (d) {
                        return d.id === eventId;
                    })
                    .attrs({
                        'r': 8,
                        'fill': '#aabbc3',
                        'stroke': '#000',
                        'stroke-width': 1.0,
                        'id': function (d) {
                            return 'event-node-' + d.id;
                        }
                    });

                events
                    .append('text')
                    .attrs({
                        'id': function (d) {
                            return 'event-text-' + d.id;
                        },
                        'class': 'event-type'
                    })
                    .classed('expand-parents', function (d) {
                        return d.eventType === 'SPAWN';
                    })
                    .classed('expand-children', function (d) {
                        return d.eventType === 'SPAWN';
                    })
                    .each(function (d) {
                        var label = d3.select(this);
                        if (d.eventType === 'CONTENT_MODIFIED' || d.eventType === 'ATTRIBUTES_MODIFIED') {
                            var lines = [];
                            if (d.eventType === 'CONTENT_MODIFIED') {
                                lines.push('CONTENT');
                            } else {
                                lines.push('ATTRIBUTES');
                            }
                            lines.push('MODIFIED');

                            // append each line
                            $.each(lines, function (i, line) {
                                label.append('tspan')
                                    .attr('x', '0')
                                    .attr('dy', '1.2em')
                                    .text(function () {
                                        return line;
                                    });
                            });
                            label.attr('transform', 'translate(10,-14)');
                        } else {
                            label.text(d.eventType).attrs({
                                'x': 10,
                                'y': 4
                            });
                        }
                    });
            };

            // updates the ui
            var update = function (provenanceTableCtrl) {
                // update the node data
                nodes = nodes.data(nodeLookup.values(), function (d) {
                    return d.id;
                });

                // exit
                nodes.exit()
                    .transition()
                    .delay(200)
                    .duration(400)
                    .attr('transform', function (d) {
                        if (d.incoming.length === 0) {
                            return 'translate(' + (width / 2) + ',50)';
                        } else {
                            return 'translate(' + d.incoming[0].source.x + ',' + d.incoming[0].source.y + ')';
                        }
                    })
                    .style('opacity', 0)
                    .remove();

                // enter
                var nodesEntered = nodes.enter()
                    .append('g')
                    .attr('id', function (d) {
                        return 'lineage-group-' + d.id;
                    })
                    .classed('node', true)
                    .attr('transform', function (d) {
                        if (d.incoming.length === 0) {
                            return 'translate(' + (width / 2) + ',50)';
                        } else {
                            return 'translate(' + d.incoming[0].source.x + ',' + d.incoming[0].source.y + ')';
                        }
                    })
                    .style('opacity', 0);

                // treat flowfiles and events differently
                nodesEntered.filter(function (d) {
                    return d.type === 'FLOWFILE';
                }).call(renderFlowFile);
                nodesEntered.filter(function (d) {
                    return d.type === 'EVENT';
                }).call(renderEvent, provenanceTableCtrl);

                // merge
                nodes = nodes.merge(nodesEntered);

                // update the nodes
                nodes.transition()
                    .duration(400)
                    .attr('transform', function (d) {
                        return 'translate(' + d.x + ', ' + d.y + ')';
                    })
                    .style('opacity', 1);

                // update the link data
                links = links.data(linkLookup.values(), function (d) {
                    return d.id;
                });

                // exit
                links.exit()
                    .attr('marker-end', '')
                    .transition()
                    .duration(400)
                    .attr('d', function (d) {
                        return 'M' + d.source.x + ',' + d.source.y + 'L' + d.source.x + ',' + d.source.y;
                    })
                    .style('opacity', 0)
                    .remove();

                // add new links
                var linksEntered = links.enter()
                    .insert('path', '.node')
                    .attrs({
                        'class': 'link',
                        'stroke-width': 1.5,
                        'stroke': '#000',
                        'fill': 'none',
                        'd': function (d) {
                            return 'M' + d.source.x + ',' + d.source.y + 'L' + d.source.x + ',' + d.source.y;
                        }
                    })
                    .style('opacity', 0);

                // merge
                links = links.merge(linksEntered)
                    .attr('marker-end', '');

                // update the links
                links.transition()
                    .delay(200)
                    .duration(400)
                    .attrs({
                        'marker-end': function (d) {
                            return 'url(#' + d.target.type + ')';
                        },
                        'd': function (d) {
                            return 'M' + d.source.x + ',' + d.source.y + 'L' + d.target.x + ',' + d.target.y;
                        }
                    })
                    .style('opacity', 1);
            };

            // show the lineage pane and hide the event search results
            $('#provenance-lineage').show();
            $('#provenance-event-search').hide();

            // add the initial lineage
            addLineage(lineageResults.nodes, lineageResults.links, provenanceTableCtrl);
        };

        function ProvenanceLineageCtrl() {
        }

        ProvenanceLineageCtrl.prototype = {
            constructor: ProvenanceLineageCtrl,

            /**
             * Initializes the lineage graph.
             */
            init: function () {
                $('#provenance-lineage-closer').on('click', function () {
                    // remove the svg from the dom
                    $('#provenance-lineage svg').remove();

                    // destroy the slider
                    $('#provenance-lineage-slider').slider('destroy');

                    // view the appropriate panel
                    $('#provenance-event-search').show();
                    $('#provenance-lineage').hide();

                    //reset table size
                    $('#provenance-table').data('gridInstance').resizeCanvas();
                });

                $('#provenance-lineage-downloader').on('click', function () {
                    var svg = $('#provenance-lineage-container').html();

                    // get the lineage to determine the actual dimensions
                    var lineage = $('g.lineage')[0];
                    var bbox = lineage.getBBox();

                    // adjust to provide some padding
                    var height = bbox.height + 60;
                    var width = bbox.width + 60;
                    var offsetX = bbox.x - 15;
                    var offsetY = bbox.y - 15;

                    // replace the svg height, width with the actual values
                    svg = svg.replace(/height=".*?"/, 'height="' + height + '"');
                    svg = svg.replace(/width=".*?"/, 'width="' + width + '"');

                    // remove any transform applied to the lineage
                    svg = svg.replace(/transform=".*?"/, '');

                    // adjust link positioning based on the offset of the bounding box
                    svg = svg.replace(/<path([^>]*?)d="M[\s]?([^\s]+?)[\s,]([^\s]+?)[\s]?L[\s]?([^\s]+?)[\s,]([^\s]+?)[\s]?"(.*?)>/g, function (match, before, rawMoveX, rawMoveY, rawLineX, rawLineY, after) {
                        // this regex captures the content before and after the d attribute in order to ensure that it contains the link class.
                        // within the svg image, there are other paths that are (within markers) that we do not want to offset
                        if (before.indexOf('link') === -1 && after.indexOf('link') === -1) {
                            return match;
                        }

                        var moveX = parseFloat(rawMoveX) - offsetX;
                        var moveY = parseFloat(rawMoveY) - offsetY;
                        var lineX = parseFloat(rawLineX) - offsetX;
                        var lineY = parseFloat(rawLineY) - offsetY;
                        return '<path' + before + 'd="M' + moveX + ',' + moveY + 'L' + lineX + ',' + lineY + '"' + after + '>';
                    });

                    // adjust node positioning based on the offset of the bounding box
                    svg = svg.replace(/<g([^>]*?)transform="translate\([\s]?([^\s]+?)[\s,]([^\s]+?)[\s]?\)"(.*?)>/g, function (match, before, rawX, rawY, after) {
                        // this regex captures the content before and after the transform attribute in order to ensure that it contains the
                        // node class. only node groups are translated with absolute coordinates since all other translated groups fall under
                        // a parent that is already positioned. this makes their translation relative and not appropriate for this adjustment
                        if (before.indexOf('node') === -1 && after.indexOf('node') === -1) {
                            return match;
                        }

                        var x = parseFloat(rawX) - offsetX;
                        var y = parseFloat(rawY) - offsetY;
                        return '<g' + before + 'transform="translate(' + x + ',' + y + ')"' + after + '>';
                    });

                    // namespaces
                    svg = svg.replace(/<svg ([^>]*)/, function (match) {
                        var svgString = match;
                        var nsSVG = ' xmlns="http://www.w3.org/2000/svg"';
                        var nsXlink = ' xmlns:xlink="http://www.w3.org/1999/xlink"';
                        var version = ' version="1.1"';

                        if (svgString.indexOf(nsSVG) === -1) {
                            svgString += nsSVG;
                        }

                        if (svgString.indexOf(nsXlink) === -1) {
                            svgString += nsXlink;
                        }

                        if (svgString.indexOf(version) === -1) {
                            svgString += version;
                        }

                        return svgString;
                    });

                    // doctype
                    svg = '<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">\n' + svg;

                    downloadSvgFile(svg);
                });

                initLineageQueryDialog();
            },

            /**
             * Shows the lineage for the specified flowfile uuid.
             *
             * @param {string} flowFileUuid     The flowfile uuid
             * @param {integer} eventId         The id of the event
             * @param {string} clusterNodeId    The id of the node in the cluster where this event/flowfile originated
             */
            showLineage: function (flowFileUuid, eventId, clusterNodeId, provenanceTableCtrl) {
                var lineageProgress = $('#lineage-percent-complete');

                // add support to cancel outstanding requests - when the button is pressed we
                // could be in one of two stages, 1) waiting to GET the status or 2)
                // in the process of GETting the status. Handle both cases by cancelling
                // the setTimeout (1) and by setting a flag to indicate that a request has
                // been request so we can ignore the results (2).

                var cancelled = false;
                var lineage = null;
                var lineageTimer = null;

                // build the lineage request
                var lineageRequest = {
                    lineageRequestType: 'FLOWFILE',
                    uuid: flowFileUuid,
                    clusterNodeId: clusterNodeId,
                    eventId: eventId
                };

                // update the progress bar value
                provenanceTableCtrl.updateProgress(lineageProgress, 0);

                // show the 'searching...' dialog
                $('#lineage-query-dialog').modal('setButtonModel', [{
                    buttonText: 'Cancel',
                    color: {
                        base: '#E3E8EB',
                        hover: '#C7D2D7',
                        text: '#004849'
                    },
                    handler: {
                        click: function () {
                            cancelled = true;

                            // we are waiting for the next poll attempt
                            if (lineageTimer !== null) {
                                // cancel it
                                clearTimeout(lineageTimer);

                                // cancel the provenance
                                closeDialog();
                            }
                        }
                    }
                }]).modal('show');

                // closes the searching dialog and cancels the query on the server
                var closeDialog = function () {
                    // cancel the provenance results since we've successfully processed the results
                    if (nfCommon.isDefinedAndNotNull(lineage)) {
                        cancelLineage(lineage);
                    }

                    // close the dialog
                    $('#lineage-query-dialog').modal('hide');
                };

                // polls the server for the status of the lineage
                var pollLineage = function (provenanceTableCtrl) {
                    getLineage(lineage).done(function (response) {
                        lineage = response.lineage;

                        // process the lineage
                        processLineage(provenanceTableCtrl);
                    }).fail(closeDialog);
                };

                var processLineage = function (provenanceTableCtrl) {
                    // if the request was cancelled just ignore the current response
                    if (cancelled === true) {
                        closeDialog();
                        return;
                    }

                    // close the dialog if the results contain an error
                    if (!nfCommon.isEmpty(lineage.results.errors)) {
                        var errors = lineage.results.errors;
                        nfDialog.showOkDialog({
                            headerText: 'Process Lineage',
                            dialogContent: nfCommon.formatUnorderedList(errors)
                        });

                        closeDialog();
                        return;
                    }

                    // update the precent complete
                    provenanceTableCtrl.updateProgress(lineageProgress, lineage.percentCompleted);

                    // process the results if they are finished
                    if (lineage.finished === true) {
                        // render the graph
                        renderLineage(lineage.results, eventId, clusterNodeId, provenanceTableCtrl);

                        // close the searching.. dialog
                        closeDialog();
                    } else {
                        // start the wait to poll again
                        lineageTimer = setTimeout(function () {
                            // clear the timer since we've been invoked
                            lineageTimer = null;

                            //  poll lineage
                            pollLineage(provenanceTableCtrl);
                        }, 2000);
                    }
                };

                // once the query is submitted wait until its finished
                submitLineage(lineageRequest).done(function (response) {
                    lineage = response.lineage;

                    // process the results, if they are not done wait 1 second before trying again
                    processLineage(provenanceTableCtrl);
                }).fail(closeDialog);
            }
        }

        var provenanceLineageCtrl = new ProvenanceLineageCtrl();
        return provenanceLineageCtrl;
    };

    return mySelf;
}));