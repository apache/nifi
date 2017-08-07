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
                'nf.Dialog',
                'nf.ErrorHandler',
                'nf.Client',
                'nf.CanvasUtils'],
            function ($, d3, nfCommon, nfDialog, nfErrorHandler, nfClient, nfCanvasUtils) {
                return (nf.Connection = factory($, d3, nfCommon, nfDialog, nfErrorHandler, nfClient, nfCanvasUtils));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Connection =
            factory(require('jquery'),
                require('d3'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.ErrorHandler'),
                require('nf.Client'),
                require('nf.CanvasUtils')));
    } else {
        nf.Connection = factory(root.$,
            root.d3,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.ErrorHandler,
            root.nf.Client,
            root.nf.CanvasUtils);
    }
}(this, function ($, d3, nfCommon, nfDialog, nfErrorHandler, nfClient, nfCanvasUtils) {
    'use strict';

    var nfSelectable;
    var nfConnectionConfiguration;
    var nfQuickSelect;
    var nfContextMenu;

    // the dimensions for the connection label
    var dimensions = {
        width: 200
    };

    // width of a backpressure indicator - half of width, left/right padding, left/right border
    var backpressureBarWidth = (dimensions.width / 2) - 15 - 2;

    /**
     * Gets the position of the label for the specified connection.
     *
     * @param {type} connectionLabel      The connection label
     */
    var getLabelPosition = function (connectionLabel) {
        var d = connectionLabel.datum();

        var x, y;
        if (d.bends.length > 0) {
            var i = Math.min(Math.max(0, d.labelIndex), d.bends.length - 1);
            x = d.bends[i].x;
            y = d.bends[i].y;
        } else {
            x = (d.start.x + d.end.x) / 2;
            y = (d.start.y + d.end.y) / 2;
        }

        // offset to account for the label dimensions
        x -= (dimensions.width / 2);
        y -= (connectionLabel.attr('height') / 2);

        return {
            x: x,
            y: y
        };
    };

    // ----------------------------------
    // connections currently on the graph
    // ----------------------------------

    var connectionMap;

    // -----------------------------------------------------------
    // cache for components that are added/removed from the canvas
    // -----------------------------------------------------------

    var removedCache;
    var addedCache;

    // ---------------------
    // connection containers
    // ---------------------

    var connectionContainer;

    // ------------------------
    // line point drag behavior
    // ------------------------

    var bendPointDrag;
    var endpointDrag;

    // ------------------------------
    // connection label drag behavior
    // ------------------------------

    var labelDrag;

    // function for generating lines
    var lineGenerator;

    // --------------------------
    // privately scoped functions
    // --------------------------

    /**
     * Calculates the distance between the two points specified squared.
     *
     * @param {object} v        First point
     * @param {object} w        Second point
     */
    var distanceSquared = function (v, w) {
        return Math.pow(v.x - w.x, 2) + Math.pow(v.y - w.y, 2);
    };

    /**
     * Calculates the distance between the two points specified.
     *
     * @param {object} v        First point
     * @param {object} w        Second point
     */
    var distanceBetweenPoints = function (v, w) {
        return Math.sqrt(distanceSquared(v, w));
    };

    /**
     * Calculates the distance between the point and the line created by s1 and s2.
     *
     * @param {object} p            The point
     * @param {object} s1           Segment start
     * @param {object} s2           Segment end
     */
    var distanceToSegment = function (p, s1, s2) {
        var l2 = distanceSquared(s1, s2);
        if (l2 === 0) {
            return Math.sqrt(distanceSquared(p, s1));
        }

        var t = ((p.x - s1.x) * (s2.x - s1.x) + (p.y - s1.y) * (s2.y - s1.y)) / l2;
        if (t < 0) {
            return Math.sqrt(distanceSquared(p, s1));
        }
        if (t > 1) {
            return Math.sqrt(distanceSquared(p, s2));
        }

        return Math.sqrt(distanceSquared(p, {
            'x': s1.x + t * (s2.x - s1.x),
            'y': s1.y + t * (s2.y - s1.y)
        }));
    };

    /**
     * Calculates the index of the bend point that is nearest to the specified point.
     *
     * @param {object} p
     * @param {object} connectionData
     */
    var getNearestSegment = function (p, connectionData) {
        if (connectionData.bends.length === 0) {
            return 0;
        }

        var minimumDistance;
        var index;

        // line is comprised of start -> [bends] -> end
        var line = [connectionData.start].concat(connectionData.bends, [connectionData.end]);

        // consider each segment
        for (var i = 0; i < line.length; i++) {
            if (i + 1 < line.length) {
                var distance = distanceToSegment(p, line[i], line[i + 1]);
                if (nfCommon.isUndefined(minimumDistance) || distance < minimumDistance) {
                    minimumDistance = distance;
                    index = i;
                }
            }
        }

        return index;
    };

    /**
     * Determines if the specified type is a type of input port.
     *
     * @argument {string} type      The port type
     */
    var isInputPortType = function (type) {
        return type.indexOf('INPUT_PORT') >= 0;
    };

    /**
     * Determines if the specified type is a type of output port.
     *
     * @argument {string} type      The port type
     */
    var isOutputPortType = function (type) {
        return type.indexOf('OUTPUT_PORT') >= 0;
    };

    /**
     * Determines whether the terminal of the connection (source|destination) is
     * a group.
     *
     * @param {object} terminal
     */
    var isGroup = function (terminal) {
        return terminal.groupId !== nfCanvasUtils.getGroupId() && (isInputPortType(terminal.type) || isOutputPortType(terminal.type));
    };

    /**
     * Determines whether expiration is configured for the specified connection.
     *
     * @param {object} connection
     * @return {boolean} Whether expiration is configured
     */
    var isExpirationConfigured = function (connection) {
        if (nfCommon.isDefinedAndNotNull(connection.flowFileExpiration)) {
            var match = connection.flowFileExpiration.match(/^(\d+).*/);
            if (match !== null && match.length > 0) {
                if (parseInt(match[0], 10) > 0) {
                    return true;
                }
            }
        }
        return false;
    };

    /**
     * Sorts the specified connections according to the z index.
     *
     * @param {type} connections
     */
    var sort = function (connections) {
        connections.sort(function (a, b) {
            return a.zIndex === b.zIndex ? 0 : a.zIndex > b.zIndex ? 1 : -1;
        });
    };

    /**
     * Selects the connection elements against the current connection map.
     */
    var select = function () {
        return connectionContainer.selectAll('g.connection').data(connectionMap.values(), function (d) {
            return d.id;
        });
    };

    var renderConnections = function (entered, selected) {
        if (entered.empty()) {
            return;
        }

        var connection = entered.append('g')
            .attr({
                'id': function (d) {
                    return 'id-' + d.id;
                },
                'class': 'connection'
            })
            .classed('selected', selected);

        // create a connection between the two components
        connection.append('path')
            .attr({
                'class': 'connection-path',
                'pointer-events': 'none'
            });

        // path to show when selection
        connection.append('path')
            .attr({
                'class': 'connection-selection-path',
                'pointer-events': 'none'
            });

        // path to make selection easier
        connection.append('path')
            .attr({
                'class': 'connection-path-selectable',
                'pointer-events': 'stroke'
            })
            .on('mousedown.selection', function () {
                // select the connection when clicking the selectable path
                nfSelectable.select(d3.select(this.parentNode));

                // update URL deep linking params
                nfCanvasUtils.setURLParameters();
            })
            .call(nfContextMenu.activate);
    };

    // determines whether the specified connection contains an unsupported relationship
    var hasUnavailableRelationship = function (d) {
        var unavailable = false;

        // verify each selected relationship is still available
        if (nfCommon.isDefinedAndNotNull(d.component.selectedRelationships) && nfCommon.isDefinedAndNotNull(d.component.availableRelationships)) {
            $.each(d.component.selectedRelationships, function (_, selectedRelationship) {
                if ($.inArray(selectedRelationship, d.component.availableRelationships) === -1) {
                    unavailable = true;
                    return false;
                }
            });
        }

        return unavailable;
    };

    // gets the appropriate end marker
    var getEndMarker = function (d) {
        var marker = 'normal';

        if (d.permissions.canRead) {
            // if the connection has a relationship that is unavailable, mark it a ghost relationship
            if (isFullBytes(d) || isFullCount(d)) {
                marker = 'full';
            } else if (hasUnavailableRelationship(d)) {
                marker = 'ghost';
            }
        } else {
            marker = 'unauthorized';
        }

        return 'url(#' + marker + ')';
    };

    // gets the appropriate drop shadow
    var getDropShadow = function (d) {
        if (isFullCount(d) || isFullBytes(d)) {
            return 'url(#connection-full-drop-shadow)';
        } else {
            return 'url(#component-drop-shadow)';
        }
    };

    // determines whether the connection is full based on the object count threshold
    var isFullCount = function (d) {
        return d.status.aggregateSnapshot.percentUseCount === 100;
    };

    // determines whether the connection is in warning based on the object count threshold
    var isWarningCount = function (d) {
        var percentUseCount = d.status.aggregateSnapshot.percentUseCount;
        if (nfCommon.isDefinedAndNotNull(percentUseCount)) {
            return percentUseCount >= 61 && percentUseCount <= 85;
        }

        return false;
    };

    // determines whether the connection is in error based on the object count threshold
    var isErrorCount = function (d) {
        var percentUseCount = d.status.aggregateSnapshot.percentUseCount;
        if (nfCommon.isDefinedAndNotNull(percentUseCount)) {
            return percentUseCount > 85;
        }

        return false;
    };

    // determines whether the connection is full based on the data size threshold
    var isFullBytes = function (d) {
        return d.status.aggregateSnapshot.percentUseBytes === 100
    };

    // determines whether the connection is in warning based on the data size threshold
    var isWarningBytes = function (d) {
        var percentUseBytes = d.status.aggregateSnapshot.percentUseBytes;
        if (nfCommon.isDefinedAndNotNull(percentUseBytes)) {
            return percentUseBytes >= 61 && percentUseBytes <= 85;
        }

        return false;
    };

    // determines whether the connection is in error based on the data size threshold
    var isErrorBytes = function (d) {
        var percentUseBytes = d.status.aggregateSnapshot.percentUseBytes;
        if (nfCommon.isDefinedAndNotNull(percentUseBytes)) {
            return percentUseBytes > 85;
        }

        return false;
    };

    // updates the specified connections
    var updateConnections = function (updated, options) {
        if (updated.empty()) {
            return;
        }

        var updatePath = true;
        var updateLabel = true;
        var transition = false;

        // extract the options if specified
        if (nfCommon.isDefinedAndNotNull(options)) {
            updatePath = nfCommon.isDefinedAndNotNull(options.updatePath) ? options.updatePath : updatePath;
            updateLabel = nfCommon.isDefinedAndNotNull(options.updateLabel) ? options.updateLabel : updateLabel;
            transition = nfCommon.isDefinedAndNotNull(options.transition) ? options.transition : transition;
        }

        if (updatePath === true) {
            updated
                .classed('grouped', function (d) {
                    var grouped = false;

                    if (d.permissions.canRead) {
                        // if there are more than one selected relationship, mark this as grouped
                        if (nfCommon.isDefinedAndNotNull(d.component.selectedRelationships) && d.component.selectedRelationships.length > 1) {
                            grouped = true;
                        }
                    }

                    return grouped;
                })
                .classed('ghost', function (d) {
                    var ghost = false;

                    if (d.permissions.canRead) {
                        // if the connection has a relationship that is unavailable, mark it a ghost relationship
                        if (hasUnavailableRelationship(d)) {
                            ghost = true;
                        }
                    }

                    return ghost;
                });

            // update connection path
            updated.select('path.connection-path')
                .classed('unauthorized', function (d) {
                    return d.permissions.canRead === false;
                });

            // update connection behavior
            updated.select('path.connection-path-selectable')
                .on('dblclick', function (d) {
                    if (d.permissions.canWrite && d.permissions.canRead) {
                        var position = d3.mouse(this.parentNode);

                        // find where to put this bend point
                        var bendIndex = getNearestSegment({
                            'x': position[0],
                            'y': position[1]
                        }, d);

                        // copy the original to restore if necessary
                        var bends = d.component.bends.slice();

                        // add it to the collection of points
                        bends.splice(bendIndex, 0, {
                            'x': position[0],
                            'y': position[1]
                        });

                        var connection = {
                            id: d.id,
                            bends: bends
                        };

                        // update the label index if necessary
                        var labelIndex = d.component.labelIndex;
                        if (bends.length === 1) {
                            connection.labelIndex = 0;
                        } else if (bendIndex <= labelIndex) {
                            connection.labelIndex = labelIndex + 1;
                        }

                        // save the new state
                        save(d, connection);

                        d3.event.stopPropagation();
                    } else {
                        return null;
                    }
                });
        }

        updated.each(function (d) {
            var connection = d3.select(this);

            if (updatePath === true) {
                // calculate the start and end points
                var sourceComponentId = nfCanvasUtils.getConnectionSourceComponentId(d);
                var sourceData = d3.select('#id-' + sourceComponentId).datum();
                var end;

                // get the appropriate end anchor point
                var endAnchor;
                if (d.bends.length > 0) {
                    endAnchor = d.bends[d.bends.length - 1];
                } else {
                    endAnchor = {
                        x: sourceData.position.x + (sourceData.dimensions.width / 2),
                        y: sourceData.position.y + (sourceData.dimensions.height / 2)
                    };
                }

                // if we are currently dragging the endpoint to a new target, use that
                // position, otherwise we need to calculate it for the current target
                if (nfCommon.isDefinedAndNotNull(d.end) && d.end.dragging === true) {
                    // since we're dragging, use the same object thats bound to the endpoint drag event
                    end = d.end;

                    // if we're not over a connectable destination use the current point
                    var newDestination = d3.select('g.hover.connectable-destination');
                    if (!newDestination.empty()) {
                        var newDestinationData = newDestination.datum();

                        // get the position on the new destination perimeter
                        var newEnd = nfCanvasUtils.getPerimeterPoint(endAnchor, {
                            'x': newDestinationData.position.x,
                            'y': newDestinationData.position.y,
                            'width': newDestinationData.dimensions.width,
                            'height': newDestinationData.dimensions.height
                        });

                        // update the coordinates with the new point
                        end.x = newEnd.x;
                        end.y = newEnd.y;
                    }
                } else {
                    var destinationComponentId = nfCanvasUtils.getConnectionDestinationComponentId(d);
                    var destinationData = d3.select('#id-' + destinationComponentId).datum();

                    // get the position on the destination perimeter
                    end = nfCanvasUtils.getPerimeterPoint(endAnchor, {
                        'x': destinationData.position.x,
                        'y': destinationData.position.y,
                        'width': destinationData.dimensions.width,
                        'height': destinationData.dimensions.height
                    });
                }

                // get the appropriate start anchor point
                var startAnchor;
                if (d.bends.length > 0) {
                    startAnchor = d.bends[0];
                } else {
                    startAnchor = end;
                }

                // get the position on the source perimeter
                var start = nfCanvasUtils.getPerimeterPoint(startAnchor, {
                    'x': sourceData.position.x,
                    'y': sourceData.position.y,
                    'width': sourceData.dimensions.width,
                    'height': sourceData.dimensions.height
                });

                // store the updated endpoints
                d.start = start;
                d.end = end;

                // update the connection paths
                nfCanvasUtils.transition(connection.select('path.connection-path'), transition)
                    .attr({
                        'd': function () {
                            var datum = [d.start].concat(d.bends, [d.end]);
                            return lineGenerator(datum);
                        }
                    });
                nfCanvasUtils.transition(connection.select('path.connection-selection-path'), transition)
                    .attr({
                        'd': function () {
                            var datum = [d.start].concat(d.bends, [d.end]);
                            return lineGenerator(datum);
                        }
                    });
                nfCanvasUtils.transition(connection.select('path.connection-path-selectable'), transition)
                    .attr({
                        'd': function () {
                            var datum = [d.start].concat(d.bends, [d.end]);
                            return lineGenerator(datum);
                        }
                    });

                // -----
                // bends
                // -----

                var startpoints = connection.selectAll('rect.startpoint');
                var endpoints = connection.selectAll('rect.endpoint');
                var midpoints = connection.selectAll('rect.midpoint');

                // require read and write permissions as it's required to read the connections available relationships
                // when connecting to a group or remote group
                if (d.permissions.canWrite && d.permissions.canRead) {

                    // ------------------
                    // bends - startpoint
                    // ------------------

                    startpoints = startpoints.data([d.start]);

                    // create a point for the start
                    startpoints.enter().append('rect')
                        .attr({
                            'class': 'startpoint linepoint',
                            'pointer-events': 'all',
                            'width': 8,
                            'height': 8
                        })
                        .on('mousedown.selection', function () {
                            // select the connection when clicking the label
                            nfSelectable.select(d3.select(this.parentNode));

                            // update URL deep linking params
                            nfCanvasUtils.setURLParameters();
                        })
                        .call(nfContextMenu.activate);

                    // update the start point
                    nfCanvasUtils.transition(startpoints, transition)
                        .attr('transform', function (p) {
                            return 'translate(' + (p.x - 4) + ', ' + (p.y - 4) + ')';
                        });

                    // remove old items
                    startpoints.exit().remove();

                    // ----------------
                    // bends - endpoint
                    // ----------------

                    var endpoints = endpoints.data([d.end]);

                    // create a point for the end
                    endpoints.enter().append('rect')
                        .call(endpointDrag)
                        .attr({
                            'class': 'endpoint linepoint',
                            'pointer-events': 'all',
                            'width': 8,
                            'height': 8
                        })
                        .on('mousedown.selection', function () {
                            // select the connection when clicking the label
                            nfSelectable.select(d3.select(this.parentNode));

                            // update URL deep linking params
                            nfCanvasUtils.setURLParameters();
                        })
                        .call(nfContextMenu.activate);

                    // update the end point
                    nfCanvasUtils.transition(endpoints, transition)
                        .attr('transform', function (p) {
                            return 'translate(' + (p.x - 4) + ', ' + (p.y - 4) + ')';
                        });

                    // remove old items
                    endpoints.exit().remove();

                    // -----------------
                    // bends - midpoints
                    // -----------------

                    var midpoints = midpoints.data(d.bends);

                    // create a point for the end
                    midpoints.enter().append('rect')
                        .attr({
                            'class': 'midpoint linepoint',
                            'pointer-events': 'all',
                            'width': 8,
                            'height': 8
                        })
                        .call(bendPointDrag)
                        .on('dblclick', function (p) {
                            // stop even propagation
                            d3.event.stopPropagation();

                            var connection = d3.select(this.parentNode);
                            var connectionData = connection.datum();

                            // if this is a self loop prevent removing the last two bends
                            var sourceComponentId = nfCanvasUtils.getConnectionSourceComponentId(connectionData);
                            var destinationComponentId = nfCanvasUtils.getConnectionDestinationComponentId(connectionData);
                            if (sourceComponentId === destinationComponentId && d.component.bends.length <= 2) {
                                nfDialog.showOkDialog({
                                    headerText: 'Connection',
                                    dialogContent: 'Looping connections must have at least two bend points.'
                                });
                                return;
                            }

                            var newBends = [];
                            var bendIndex = -1;

                            // create a new array of bends without the selected one
                            $.each(connectionData.component.bends, function (i, bend) {
                                if (p.x !== bend.x && p.y !== bend.y) {
                                    newBends.push(bend);
                                } else {
                                    bendIndex = i;
                                }
                            });

                            if (bendIndex < 0) {
                                return;
                            }

                            var connection = {
                                id: connectionData.id,
                                bends: newBends
                            };

                            // update the label index if necessary
                            var labelIndex = connectionData.component.labelIndex;
                            if (newBends.length <= 1) {
                                connection.labelIndex = 0;
                            } else if (bendIndex <= labelIndex) {
                                connection.labelIndex = Math.max(0, labelIndex - 1);
                            }

                            // save the updated connection
                            save(connectionData, connection);
                        })
                        .on('mousedown.selection', function () {
                            // select the connection when clicking the label
                            nfSelectable.select(d3.select(this.parentNode));

                            // update URL deep linking params
                            nfCanvasUtils.setURLParameters();
                        })
                        .call(nfContextMenu.activate);

                    // update the midpoints
                    nfCanvasUtils.transition(midpoints, transition)
                        .attr('transform', function (p) {
                            return 'translate(' + (p.x - 4) + ', ' + (p.y - 4) + ')';
                        });

                    // remove old items
                    midpoints.exit().remove();
                } else {
                    // remove the start, mid, and end points
                    startpoints.remove();
                    endpoints.remove();
                    midpoints.remove();
                }
            }

            if (updateLabel === true) {
                var connectionLabelContainer = connection.select('g.connection-label-container');

                // update visible connections
                if (connection.classed('visible')) {

                    // if there is no connection label this connection is becoming
                    // visible so we need to render it
                    if (connectionLabelContainer.empty()) {
                        // connection label container
                        connectionLabelContainer = connection.insert('g', 'rect.startpoint')
                            .attr({
                                'class': 'connection-label-container',
                                'pointer-events': 'all'
                            })
                            .on('mousedown.selection', function () {
                                // select the connection when clicking the label
                                nfSelectable.select(d3.select(this.parentNode));

                                // update URL deep linking params
                                nfCanvasUtils.setURLParameters();
                            })
                            .call(nfContextMenu.activate).call(nfQuickSelect.activate);

                        // connection label
                        connectionLabelContainer.append('rect')
                            .attr({
                                'class': 'body',
                                'width': dimensions.width,
                                'x': 0,
                                'y': 0
                            });

                        // processor border
                        connectionLabelContainer.append('rect')
                            .attr({
                                'class': 'border',
                                'width': dimensions.width,
                                'fill': 'transparent',
                                'stroke': 'transparent'
                            });
                    }

                    var labelCount = 0;
                    var rowHeight = 19;
                    var backgrounds = [];
                    var borders = [];

                    var connectionFrom = connectionLabelContainer.select('g.connection-from-container');
                    var connectionTo = connectionLabelContainer.select('g.connection-to-container');
                    var connectionName = connectionLabelContainer.select('g.connection-name-container');

                    if (d.permissions.canRead) {

                        // -----------------------
                        // connection label - from
                        // -----------------------

                        // determine if the connection require a from label
                        if (isGroup(d.component.source)) {
                            // see if the connection from label is already rendered
                            if (connectionFrom.empty()) {
                                connectionFrom = connectionLabelContainer.append('g')
                                    .attr({
                                        'class': 'connection-from-container'
                                    });

                                // background
                                backgrounds.push(connectionFrom.append('rect')
                                    .attr({
                                        'class': 'connection-label-background',
                                        'width': dimensions.width,
                                        'height': rowHeight
                                    }));

                                // border
                                borders.push(connectionFrom.append('rect')
                                    .attr({
                                        'class': 'connection-label-border',
                                        'width': dimensions.width,
                                        'height': 1
                                    }));

                                connectionFrom.append('text')
                                    .attr({
                                        'class': 'stats-label',
                                        'x': 5,
                                        'y': 14
                                    })
                                    .text('From');

                                connectionFrom.append('text')
                                    .attr({
                                        'class': 'stats-value connection-from',
                                        'x': 43,
                                        'y': 14,
                                        'width': 130
                                    });

                                connectionFrom.append('text')
                                    .attr({
                                        'class': 'connection-from-run-status',
                                        'x': 185,
                                        'y': 14
                                    });
                            } else {
                                backgrounds.push(connectionFrom.select('rect.connection-label-background'));
                                borders.push(connectionFrom.select('rect.connection-label-border'));
                            }

                            // update the connection from positioning
                            connectionFrom.attr('transform', function () {
                                var y = (rowHeight * labelCount++);
                                return 'translate(0, ' + y + ')';
                            });

                            // update the label text
                            connectionFrom.select('text.connection-from')
                                .each(function () {
                                    var connectionFromLabel = d3.select(this);

                                    // reset the label name to handle any previous state
                                    connectionFromLabel.text(null).selectAll('title').remove();

                                    // apply ellipsis to the label as necessary
                                    nfCanvasUtils.ellipsis(connectionFromLabel, d.component.source.name);
                                }).append('title').text(function () {
                                return d.component.source.name;
                            });

                            // update the label run status
                            connectionFrom.select('text.connection-from-run-status')
                                .text(function () {
                                    if (d.component.source.exists === false) {
                                        return '\uf071';
                                    } else if (d.component.source.running === true) {
                                        return '\uf04b';
                                    } else {
                                        return '\uf04d';
                                    }
                                })
                                .classed('running', function () {
                                    if (d.component.source.exists === false) {
                                        return false;
                                    } else {
                                        return d.component.source.running;
                                    }
                                })
                                .classed('stopped', function () {
                                    if (d.component.source.exists === false) {
                                        return false;
                                    } else {
                                        return !d.component.source.running;
                                    }
                                })
                                .classed('is-missing-port', function () {
                                    return d.component.source.exists === false;
                                });
                        } else {
                            // there is no connection from, remove the previous if necessary
                            connectionFrom.remove();
                        }

                        // ---------------------
                        // connection label - to
                        // ---------------------

                        // determine if the connection require a to label
                        if (isGroup(d.component.destination)) {
                            // see if the connection to label is already rendered
                            if (connectionTo.empty()) {
                                connectionTo = connectionLabelContainer.append('g')
                                    .attr({
                                        'class': 'connection-to-container'
                                    });

                                // background
                                backgrounds.push(connectionTo.append('rect')
                                    .attr({
                                        'class': 'connection-label-background',
                                        'width': dimensions.width,
                                        'height': rowHeight
                                    }));

                                // border
                                borders.push(connectionTo.append('rect')
                                    .attr({
                                        'class': 'connection-label-border',
                                        'width': dimensions.width,
                                        'height': 1
                                    }));

                                connectionTo.append('text')
                                    .attr({
                                        'class': 'stats-label',
                                        'x': 5,
                                        'y': 14
                                    })
                                    .text('To');

                                connectionTo.append('text')
                                    .attr({
                                        'class': 'stats-value connection-to',
                                        'x': 25,
                                        'y': 14,
                                        'width': 145
                                    });

                                connectionTo.append('text')
                                    .attr({
                                        'class': 'connection-to-run-status',
                                        'x': 185,
                                        'y': 14
                                    });
                            } else {
                                backgrounds.push(connectionTo.select('rect.connection-label-background'));
                                borders.push(connectionTo.select('rect.connection-label-border'));
                            }

                            // update the connection to positioning
                            connectionTo.attr('transform', function () {
                                var y = (rowHeight * labelCount++);
                                return 'translate(0, ' + y + ')';
                            });

                            // update the label text
                            connectionTo.select('text.connection-to')
                                .each(function (d) {
                                    var connectionToLabel = d3.select(this);

                                    // reset the label name to handle any previous state
                                    connectionToLabel.text(null).selectAll('title').remove();

                                    // apply ellipsis to the label as necessary
                                    nfCanvasUtils.ellipsis(connectionToLabel, d.component.destination.name);
                                }).append('title').text(function (d) {
                                return d.component.destination.name;
                            });

                            // update the label run status
                            connectionTo.select('text.connection-to-run-status')
                                .text(function () {
                                    if (d.component.destination.exists === false) {
                                        return '\uf071';
                                    } else if (d.component.destination.running === true) {
                                        return '\uf04b';
                                    } else {
                                        return '\uf04d';
                                    }
                                })
                                .classed('running', function () {
                                    if (d.component.destination.exists === false) {
                                        return false;
                                    } else {
                                        return d.component.destination.running;
                                    }
                                })
                                .classed('stopped', function () {
                                    if (d.component.destination.exists === false) {
                                        return false;
                                    } else {
                                        return !d.component.destination.running;
                                    }
                                })
                                .classed('is-missing-port', function () {
                                    return d.component.destination.exists === false;
                                });
                        } else {
                            // there is no connection to, remove the previous if necessary
                            connectionTo.remove();
                        }

                        // -----------------------
                        // connection label - name
                        // -----------------------

                        // get the connection name
                        var connectionNameValue = nfCanvasUtils.formatConnectionName(d.component);

                        // is there a name to render
                        if (!nfCommon.isBlank(connectionNameValue)) {
                            // see if the connection name label is already rendered
                            if (connectionName.empty()) {
                                connectionName = connectionLabelContainer.append('g')
                                    .attr({
                                        'class': 'connection-name-container'
                                    });

                                // background
                                backgrounds.push(connectionName.append('rect')
                                    .attr({
                                        'class': 'connection-label-background',
                                        'width': dimensions.width,
                                        'height': rowHeight
                                    }));

                                // border
                                borders.push(connectionName.append('rect')
                                    .attr({
                                        'class': 'connection-label-border',
                                        'width': dimensions.width,
                                        'height': 1
                                    }));

                                connectionName.append('text')
                                    .attr({
                                        'class': 'stats-label',
                                        'x': 5,
                                        'y': 14
                                    })
                                    .text('Name');

                                connectionName.append('text')
                                    .attr({
                                        'class': 'stats-value connection-name',
                                        'x': 45,
                                        'y': 14,
                                        'width': 142
                                    });
                            } else {
                                backgrounds.push(connectionName.select('rect.connection-label-background'));
                                borders.push(connectionName.select('rect.connection-label-border'));
                            }

                            // update the connection name positioning
                            connectionName.attr('transform', function () {
                                var y = (rowHeight * labelCount++);
                                return 'translate(0, ' + y + ')';
                            });

                            // update the connection name
                            connectionName.select('text.connection-name')
                                .each(function () {
                                    var connectionToLabel = d3.select(this);

                                    // reset the label name to handle any previous state
                                    connectionToLabel.text(null).selectAll('title').remove();

                                    // apply ellipsis to the label as necessary
                                    nfCanvasUtils.ellipsis(connectionToLabel, connectionNameValue);
                                }).append('title').text(function () {
                                return connectionNameValue;
                            });
                        } else {
                            // there is no connection name, remove the previous if necessary
                            connectionName.remove();
                        }
                    } else {
                        // no permissions to read to remove previous if necessary
                        connectionFrom.remove();
                        connectionTo.remove();
                        connectionName.remove();
                    }

                    // -------------------------
                    // connection label - queued
                    // -------------------------

                    var HEIGHT_FOR_BACKPRESSURE = 3;

                    // see if the queue label is already rendered
                    var queued = connectionLabelContainer.select('g.queued-container');
                    if (queued.empty()) {
                        queued = connectionLabelContainer.append('g')
                            .attr({
                                'class': 'queued-container'
                            });

                        // background
                        backgrounds.push(queued.append('rect')
                            .attr({
                                'class': 'connection-label-background',
                                'width': dimensions.width,
                                'height': rowHeight + HEIGHT_FOR_BACKPRESSURE
                            }));

                        // border
                        borders.push(queued.append('rect')
                            .attr({
                                'class': 'connection-label-border',
                                'width': dimensions.width,
                                'height': 1
                            }));

                        queued.append('text')
                            .attr({
                                'class': 'stats-label',
                                'x': 5,
                                'y': 14
                            })
                            .text('Queued');

                        var queuedText = queued.append('text')
                            .attr({
                                'class': 'stats-value queued',
                                'x': 55,
                                'y': 14
                            });

                        // queued count
                        queuedText.append('tspan')
                            .attr({
                                'class': 'count'
                            });

                        // queued size
                        queuedText.append('tspan')
                            .attr({
                                'class': 'size'
                            });

                        // expiration icon
                        queued.append('text')
                            .attr({
                                'class': 'expiration-icon',
                                'x': 185,
                                'y': 14
                            })
                            .text(function () {
                                return '\uf017';
                            })
                            .append('title');

                        var yBackpressureOffset = rowHeight + HEIGHT_FOR_BACKPRESSURE - 4;

                        // backpressure object threshold

                        // start
                        queued.append('rect')
                            .attr({
                                'class': 'backpressure-tick object',
                                'width': 1,
                                'height': 3,
                                'x': 5,
                                'y': yBackpressureOffset
                            });

                        // bar
                        var backpressureCountOffset = 6;
                        queued.append('rect')
                            .attr({
                                'class': 'backpressure-object',
                                'width': backpressureBarWidth,
                                'height': 3,
                                'x': backpressureCountOffset,
                                'y': yBackpressureOffset
                            })
                            .append('title');

                        // end
                        queued.append('rect')
                            .attr({
                                'class': 'backpressure-tick object',
                                'width': 1,
                                'height': 3,
                                'x': backpressureCountOffset + backpressureBarWidth,
                                'y': yBackpressureOffset
                            });

                        // percent full
                        queued.append('rect')
                            .attr({
                                'class': 'backpressure-percent object',
                                'width': 0,
                                'height': 3,
                                'x': backpressureCountOffset,
                                'y': yBackpressureOffset
                            });

                        // backpressure data size threshold

                        // start
                        queued.append('rect')
                            .attr({
                                'class': 'backpressure-tick data-size',
                                'width': 1,
                                'height': 3,
                                'x': (dimensions.width / 2) + 10,
                                'y': yBackpressureOffset
                            });

                        // bar
                        var backpressureDataSizeOffset = (dimensions.width / 2) + 10 + 1;
                        queued.append('rect')
                            .attr({
                                'class': 'backpressure-data-size',
                                'width': backpressureBarWidth,
                                'height': 3,
                                'x': backpressureDataSizeOffset,
                                'y': yBackpressureOffset
                            })
                            .append('title');

                        // end
                        queued.append('rect')
                            .attr({
                                'class': 'backpressure-tick data-size',
                                'width': 1,
                                'height': 3,
                                'x': backpressureDataSizeOffset + backpressureBarWidth,
                                'y': yBackpressureOffset
                            });

                        // percent full
                        queued.append('rect')
                            .attr({
                                'class': 'backpressure-percent data-size',
                                'width': 0,
                                'height': 3,
                                'x': backpressureDataSizeOffset,
                                'y': yBackpressureOffset
                            });
                    } else {
                        backgrounds.push(queued.select('rect.connection-label-background'));
                        borders.push(queued.select('rect.connection-label-border'));
                    }

                    // update the queued vertical positioning as necessary
                    queued.attr('transform', function () {
                        var y = (rowHeight * labelCount++);
                        return 'translate(0, ' + y + ')';
                    });

                    // update the height based on the labels being rendered
                    connectionLabelContainer.select('rect.body')
                        .attr('height', function () {
                            return (rowHeight * labelCount) + HEIGHT_FOR_BACKPRESSURE;
                        })
                        .classed('unauthorized', function () {
                            return d.permissions.canRead === false;
                        });
                    connectionLabelContainer.select('rect.border')
                        .attr('height', function () {
                            return (rowHeight * labelCount) + HEIGHT_FOR_BACKPRESSURE;
                        })
                        .classed('unauthorized', function () {
                            return d.permissions.canRead === false;
                        });

                    // update the coloring of the backgrounds
                    $.each(backgrounds, function (i, background) {
                        if (i % 2 === 0) {
                            background.attr('fill', '#f4f6f7');
                        } else {
                            background.attr('fill', '#ffffff');
                        }
                    });

                    // update the coloring of the label borders
                    $.each(borders, function (i, border) {
                        if (i > 0) {
                            border.attr('fill', '#c7d2d7');
                        } else {
                            border.attr('fill', 'transparent');
                        }
                    });

                    // determine whether or not to show the expiration icon
                    connectionLabelContainer.select('text.expiration-icon')
                        .classed('hidden', function () {
                            if (d.permissions.canRead) {
                                return !isExpirationConfigured(d.component);
                            } else {
                                return true;
                            }
                        })
                        .select('title').text(function () {
                        if (d.permissions.canRead) {
                            return 'Expires FlowFiles older than ' + d.component.flowFileExpiration;
                        } else {
                            return '';
                        }
                    });

                    // update backpressure object fill
                    connectionLabelContainer.select('rect.backpressure-object')
                        .classed('not-configured', function () {
                            return nfCommon.isUndefinedOrNull(d.status.aggregateSnapshot.percentUseCount);
                        });
                    connectionLabelContainer.selectAll('rect.backpressure-tick.object')
                        .classed('not-configured', function () {
                            return nfCommon.isUndefinedOrNull(d.status.aggregateSnapshot.percentUseCount);
                        });

                    // update backpressure data size fill
                    connectionLabelContainer.select('rect.backpressure-data-size')
                        .classed('not-configured', function () {
                            return nfCommon.isUndefinedOrNull(d.status.aggregateSnapshot.percentUseBytes);
                        });
                    connectionLabelContainer.selectAll('rect.backpressure-tick.data-size')
                        .classed('not-configured', function () {
                            return nfCommon.isUndefinedOrNull(d.status.aggregateSnapshot.percentUseBytes);
                        });

                    if (d.permissions.canWrite) {
                        // only support dragging the label when appropriate
                        connectionLabelContainer.call(labelDrag);
                    }

                    // update the connection status
                    connection.call(updateConnectionStatus);
                } else {
                    if (!connectionLabelContainer.empty()) {
                        connectionLabelContainer.remove();
                    }
                }
            }

            // update the position of the label if possible
            nfCanvasUtils.transition(connection.select('g.connection-label-container'), transition)
                .attr('transform', function () {
                    var label = d3.select(this).select('rect.body');
                    var position = getLabelPosition(label);
                    return 'translate(' + position.x + ', ' + position.y + ')';
                });
        });
    };

    /**
     * Updates the stats of the connections in the specified selection.
     *
     * @param {selection} updated       The selected connections to update
     */
    var updateConnectionStatus = function (updated) {
        if (updated.empty()) {
            return;
        }

        // update data size
        var dataSizeDeferred = $.Deferred(function (deferred) {
            // queued count value
            updated.select('text.queued tspan.count')
                .text(function (d) {
                    return nfCommon.substringBeforeFirst(d.status.aggregateSnapshot.queued, ' ');
                });

            var backpressurePercentDataSize = updated.select('rect.backpressure-percent.data-size');
            backpressurePercentDataSize.transition()
                .duration(400)
                .attr({
                    'width': function (d) {
                        if (nfCommon.isDefinedAndNotNull(d.status.aggregateSnapshot.percentUseBytes)) {
                            return (backpressureBarWidth * d.status.aggregateSnapshot.percentUseBytes) / 100;
                        } else {
                            return 0;
                        }
                    }
                }).each('end', function () {
                backpressurePercentDataSize
                    .classed('warning', function (d) {
                        return isWarningBytes(d);
                    })
                    .classed('error', function (d) {
                        return isErrorBytes(d);
                    });

                deferred.resolve();
            });

            updated.select('rect.backpressure-data-size').select('title').text(function (d) {
                if (nfCommon.isDefinedAndNotNull(d.status.aggregateSnapshot.percentUseBytes)) {
                    return 'Queue is ' + d.status.aggregateSnapshot.percentUseBytes + '% full based on Back Pressure Data Size Threshold';
                } else {
                    return 'Back Pressure Data Size Threshold is not configured';
                }
            });
        }).promise();

        // update object count
        var objectCountDeferred = $.Deferred(function (deferred) {
            // queued size value
            updated.select('text.queued tspan.size')
                .text(function (d) {
                    return ' ' + nfCommon.substringAfterFirst(d.status.aggregateSnapshot.queued, ' ');
                });

            var backpressurePercentObject = updated.select('rect.backpressure-percent.object');
            backpressurePercentObject.transition()
                .duration(400)
                .attr({
                    'width': function (d) {
                        if (nfCommon.isDefinedAndNotNull(d.status.aggregateSnapshot.percentUseCount)) {
                            return (backpressureBarWidth * d.status.aggregateSnapshot.percentUseCount) / 100;
                        } else {
                            return 0;
                        }
                    }
                }).each('end', function () {
                backpressurePercentObject
                    .classed('warning', function (d) {
                        return isWarningCount(d);
                    })
                    .classed('error', function (d) {
                        return isErrorCount(d);
                    });

                deferred.resolve();
            });

            updated.select('rect.backpressure-object').select('title').text(function (d) {
                if (nfCommon.isDefinedAndNotNull(d.status.aggregateSnapshot.percentUseCount)) {
                    return 'Queue is ' + d.status.aggregateSnapshot.percentUseCount + '% full based on Back Pressure Object Threshold';
                } else {
                    return 'Back Pressure Object Threshold is not configured';
                }
            });
        }).promise();

        // update connection once progress bars have transitioned
        $.when(dataSizeDeferred, objectCountDeferred).done(function () {
            // connection stroke
            updated.select('path.connection-path')
                .classed('full', function (d) {
                    return isFullCount(d) || isFullBytes(d);
                })
                .attr({
                    'marker-end': getEndMarker
                });

            // border
            updated.select('rect.border')
                .classed('full', function (d) {
                    return isFullCount(d) || isFullBytes(d);
                });

            // drop shadow
            updated.select('rect.body')
                .attr({
                    'filter': getDropShadow
                });
        });
    };

    /**
     * Saves the connection entry specified by d with the new configuration specified
     * in connection.
     *
     * @param {type} d
     * @param {type} connection
     */
    var save = function (d, connection) {
        var entity = {
            'revision': nfClient.getRevision(d),
            'component': connection
        };

        return $.ajax({
            type: 'PUT',
            url: d.uri,
            data: JSON.stringify(entity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (response) {
            // request was successful, update the entry
            nfConnection.set(response);
        }).fail(function (xhr, status, error) {
            if (xhr.status === 400 || xhr.status === 404 || xhr.status === 409) {
                nfDialog.showOkDialog({
                    headerText: 'Connection',
                    dialogContent: nfCommon.escapeHtml(xhr.responseText)
                });
            } else {
                nfErrorHandler.handleAjaxError(xhr, status, error);
            }
        });
    };

    // removes the specified connections
    var removeConnections = function (removed) {
        // consider reloading source/destination of connection being removed
        removed.each(function (d) {
            nfCanvasUtils.reloadConnectionSourceAndDestination(d.sourceId, d.destinationId);
        });

        // remove the connection
        removed.remove();
    };

    var nfConnection = {
        config: {
            selfLoopXOffset: (dimensions.width / 2) + 5,
            selfLoopYOffset: 25
        },

        /**
         * Initializes the connection.
         *
         * @param nfSelectableRef   The nfSelectable module.
         * @param nfContextMenuRef   The nfContextMenu module.
         * @param nfQuickSelectRef   The nfQuickSelect module.
         */
        init: function (nfSelectableRef, nfContextMenuRef, nfQuickSelectRef, nfConnectionConfigurationRef) {
            nfSelectable = nfSelectableRef;
            nfContextMenu = nfContextMenuRef;
            nfQuickSelect = nfQuickSelectRef;
            nfConnectionConfiguration = nfConnectionConfigurationRef;

            connectionMap = d3.map();
            removedCache = d3.map();
            addedCache = d3.map();

            // create the connection container
            connectionContainer = d3.select('#canvas').append('g')
                .attr({
                    'pointer-events': 'stroke',
                    'class': 'connections'
                });

            // define the line generator
            lineGenerator = d3.svg.line()
                .x(function (d) {
                    return d.x;
                })
                .y(function (d) {
                    return d.y;
                })
                .interpolate('linear');

            // handle bend point drag events
            bendPointDrag = d3.behavior.drag()
                .on('dragstart', function () {
                    // stop further propagation
                    d3.event.sourceEvent.stopPropagation();
                })
                .on('drag', function (d) {
                    d.x = d3.event.x;
                    d.y = d3.event.y;

                    // redraw this connection
                    d3.select(this.parentNode).call(updateConnections, {
                        'updatePath': true,
                        'updateLabel': false
                    });
                })
                .on('dragend', function () {
                    var connection = d3.select(this.parentNode);
                    var connectionData = connection.datum();
                    var bends = connection.selectAll('rect.midpoint').data();

                    // ensure the bend lengths are the same
                    if (bends.length === connectionData.component.bends.length) {
                        // determine if the bend points have moved
                        var different = false;
                        for (var i = 0; i < bends.length && !different; i++) {
                            if (bends[i].x !== connectionData.component.bends[i].x || bends[i].y !== connectionData.component.bends[i].y) {
                                different = true;
                            }
                        }

                        // only save the updated bends if necessary
                        if (different) {
                            save(connectionData, {
                                id: connectionData.id,
                                bends: bends
                            }).fail(function () {
                                // restore the previous bend points
                                connectionData.bends = $.map(connectionData.component.bends, function (bend) {
                                    return {
                                        x: bend.x,
                                        y: bend.y
                                    };
                                });

                                // refresh the connection
                                connection.call(updateConnections, {
                                    'updatePath': true,
                                    'updateLabel': false
                                });
                            });
                        }
                    }

                    // stop further propagation
                    d3.event.sourceEvent.stopPropagation();
                });

            // handle endpoint drag events
            endpointDrag = d3.behavior.drag()
                .on('dragstart', function (d) {
                    // indicate that dragging has begun
                    d.dragging = true;

                    // stop further propagation
                    d3.event.sourceEvent.stopPropagation();
                })
                .on('drag', function (d) {
                    d.x = d3.event.x - 8;
                    d.y = d3.event.y - 8;

                    // ensure the new destination is valid
                    d3.select('g.hover').classed('connectable-destination', function () {
                        return nfCanvasUtils.isValidConnectionDestination(d3.select(this));
                    });

                    // redraw this connection
                    d3.select(this.parentNode).call(updateConnections, {
                        'updatePath': true,
                        'updateLabel': false
                    });
                })
                .on('dragend', function (d) {
                    // indicate that dragging as stopped
                    d.dragging = false;

                    // get the corresponding connection
                    var connection = d3.select(this.parentNode);
                    var connectionData = connection.datum();
                    var previousDestinationId = connectionData.destinationId;

                    // attempt to select a new destination
                    var destination = d3.select('g.connectable-destination');

                    // resets the connection if we're not over a new destination
                    if (destination.empty()) {
                        connection.call(updateConnections, {
                            'updatePath': true,
                            'updateLabel': false
                        });
                    } else {
                        // prompt for the new port if appropriate
                        if (nfCanvasUtils.isProcessGroup(destination) || nfCanvasUtils.isRemoteProcessGroup(destination)) {
                            // user will select new port and updated connect details will be set accordingly
                            nfConnectionConfiguration.showConfiguration(connection, destination).done(function () {
                                // reload the previous destination
                                nfCanvasUtils.reloadConnectionSourceAndDestination(null, previousDestinationId);
                            }).fail(function () {
                                // reset the connection
                                connection.call(updateConnections, {
                                    'updatePath': true,
                                    'updateLabel': false
                                });
                            });
                        } else {
                            // get the destination details
                            var destinationData = destination.datum();
                            var destinationType = nfCanvasUtils.getConnectableTypeForDestination(destination);

                            var connectionEntity = {
                                'revision': nfClient.getRevision(connectionData),
                                'component': {
                                    'id': connectionData.id,
                                    'destination': {
                                        'id': destinationData.id,
                                        'groupId': nfCanvasUtils.getGroupId(),
                                        'type': destinationType
                                    }
                                }
                            };

                            // if this is a self loop and there are less than 2 bends, add them
                            if (connectionData.bends.length < 2 && connectionData.sourceId === destinationData.id) {
                                var rightCenter = {
                                    x: destinationData.position.x + (destinationData.dimensions.width),
                                    y: destinationData.position.y + (destinationData.dimensions.height / 2)
                                };
                                var xOffset = nfConnection.config.selfLoopXOffset;
                                var yOffset = nfConnection.config.selfLoopYOffset;

                                connectionEntity.component.bends = [];
                                connectionEntity.component.bends.push({
                                    'x': (rightCenter.x + xOffset),
                                    'y': (rightCenter.y - yOffset)
                                });
                                connectionEntity.component.bends.push({
                                    'x': (rightCenter.x + xOffset),
                                    'y': (rightCenter.y + yOffset)
                                });
                            }

                            $.ajax({
                                type: 'PUT',
                                url: connectionData.uri,
                                data: JSON.stringify(connectionEntity),
                                dataType: 'json',
                                contentType: 'application/json'
                            }).done(function (response) {
                                var updatedConnectionData = response.component;

                                // refresh to update the label
                                nfConnection.set(response);

                                // reload the previous destination and the new source/destination
                                nfCanvasUtils.reloadConnectionSourceAndDestination(null, previousDestinationId);
                                nfCanvasUtils.reloadConnectionSourceAndDestination(response.sourceId, response.destinationId);
                            }).fail(function (xhr, status, error) {
                                if (xhr.status === 400 || xhr.status === 401 || xhr.status === 403 || xhr.status === 404 || xhr.status === 409) {
                                    nfDialog.showOkDialog({
                                        headerText: 'Connection',
                                        dialogContent: nfCommon.escapeHtml(xhr.responseText)
                                    });

                                    // reset the connection
                                    connection.call(updateConnections, {
                                        'updatePath': true,
                                        'updateLabel': false
                                    });
                                } else {
                                    nfErrorHandler.handleAjaxError(xhr, status, error);
                                }
                            });
                        }
                    }

                    // stop further propagation
                    d3.event.sourceEvent.stopPropagation();
                });

            // label drag behavior
            labelDrag = d3.behavior.drag()
                .on('dragstart', function (d) {
                    // stop further propagation
                    d3.event.sourceEvent.stopPropagation();
                })
                .on('drag', function (d) {
                    if (d.bends.length > 1) {
                        // get the dragged component
                        var drag = d3.select('rect.label-drag');

                        // lazily create the drag selection box
                        if (drag.empty()) {
                            var connectionLabel = d3.select(this).select('rect.body');

                            var position = getLabelPosition(connectionLabel);
                            var width = dimensions.width;
                            var height = connectionLabel.attr('height');

                            // create a selection box for the move
                            drag = d3.select('#canvas').append('rect')
                                .attr('x', position.x)
                                .attr('y', position.y)
                                .attr('class', 'label-drag')
                                .attr('width', width)
                                .attr('height', height)
                                .attr('stroke-width', function () {
                                    return 1 / nfCanvasUtils.scaleCanvasView();
                                })
                                .attr('stroke-dasharray', function () {
                                    return 4 / nfCanvasUtils.scaleCanvasView();
                                })
                                .datum({
                                    x: position.x,
                                    y: position.y,
                                    width: width,
                                    height: height
                                });
                        } else {
                            // update the position of the drag selection
                            drag.attr('x', function (d) {
                                d.x += d3.event.dx;
                                return d.x;
                            })
                                .attr('y', function (d) {
                                    d.y += d3.event.dy;
                                    return d.y;
                                });
                        }

                        // calculate the current point
                        var datum = drag.datum();
                        var currentPoint = {
                            x: datum.x + (datum.width / 2),
                            y: datum.y + (datum.height / 2)
                        };

                        var closestBendIndex = -1;
                        var minDistance;
                        $.each(d.bends, function (i, bend) {
                            var bendPoint = {
                                'x': bend.x,
                                'y': bend.y
                            };

                            // get the distance
                            var distance = distanceBetweenPoints(currentPoint, bendPoint);

                            // see if its the minimum
                            if (closestBendIndex === -1 || distance < minDistance) {
                                closestBendIndex = i;
                                minDistance = distance;
                            }
                        });

                        // record the closest bend
                        d.labelIndex = closestBendIndex;

                        // refresh the connection
                        d3.select(this.parentNode).call(updateConnections, {
                            'updatePath': true,
                            'updateLabel': false
                        });
                    }
                })
                .on('dragend', function (d) {
                    if (d.bends.length > 1) {
                        // get the drag selection
                        var drag = d3.select('rect.label-drag');

                        // ensure we found a drag selection
                        if (!drag.empty()) {
                            // remove the drag selection
                            drag.remove();
                        }

                        // only save if necessary
                        if (d.labelIndex !== d.component.labelIndex) {
                            // get the connection to refresh below
                            var connection = d3.select(this.parentNode);

                            // save the new label index
                            save(d, {
                                id: d.id,
                                labelIndex: d.labelIndex
                            }).fail(function () {
                                // restore the previous label index
                                d.labelIndex = d.component.labelIndex;

                                // refresh the connection
                                connection.call(updateConnections, {
                                    'updatePath': true,
                                    'updateLabel': false
                                });
                            });
                        }
                    }

                    // stop further propagation
                    d3.event.sourceEvent.stopPropagation();
                });
        },

        /**
         * Adds the specified connection entity.
         *
         * @param connectionEntities       The connection
         * @param options           Configuration options
         */
        add: function (connectionEntities, options) {
            var selectAll = false;
            if (nfCommon.isDefinedAndNotNull(options)) {
                selectAll = nfCommon.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
            }

            // get the current time
            var now = new Date().getTime();

            var add = function (connectionEntity) {
                addedCache.set(connectionEntity.id, now);

                // add the connection
                connectionMap.set(connectionEntity.id, $.extend({
                    type: 'Connection'
                }, connectionEntity));
            };

            // determine how to handle the specified connection
            if ($.isArray(connectionEntities)) {
                $.each(connectionEntities, function (_, connectionEntity) {
                    add(connectionEntity);
                });
            } else if (nfCommon.isDefinedAndNotNull(connectionEntities)) {
                add(connectionEntities);
            }

            // apply the selection and handle new connections
            var selection = select();
            selection.enter().call(renderConnections, selectAll);
            selection.call(updateConnections, {
                'updatePath': true,
                'updateLabel': false
            }).call(sort);
        },

        /**
         * Determines if the specified selection is disconnected from other nodes.
         *
         * @argument {selection} selection          The selection
         */
        isDisconnected: function (selection) {

            // if nothing is selected return
            if (selection.empty()) {
                return false;
            }
            var connections = d3.map();
            var components = d3.map();
            var isDisconnected = true;

            // include connections
            selection.filter(function (d) {
                return d.type === 'Connection';
            }).each(function (d) {
                connections.set(d.id, d);
            });

            // include components and ensure their connections are included
            selection.filter(function (d) {
                return d.type !== 'Connection';
            }).each(function (d) {
                components.set(d.id, d.component);

                // check all connections of this component
                $.each(nfConnection.getComponentConnections(d.id), function (_, connection) {
                    if (!connections.has(connection.id)) {
                        isDisconnected = false;
                        return false;
                    }
                });
            });
            if (isDisconnected) {

                // go through each connection to ensure its source and destination are included
                connections.forEach(function (id, connection) {
                    if (isDisconnected) {

                        // determine whether this connection and its components are included within the selection
                        isDisconnected = components.has(nfCanvasUtils.getConnectionSourceComponentId(connection)) && components.has(nfCanvasUtils.getConnectionDestinationComponentId(connection));
                    }
                });
            }
            return isDisconnected;
        },

        /**
         * Populates the graph with the specified connections.
         *
         * @argument {object | array} connectionEntities               The connections to add
         * @argument {object} options                Configuration options
         */
        set: function (connectionEntities, options) {
            var selectAll = false;
            var transition = false;
            var overrideRevisionCheck = false;
            if (nfCommon.isDefinedAndNotNull(options)) {
                selectAll = nfCommon.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
                transition = nfCommon.isDefinedAndNotNull(options.transition) ? options.transition : transition;
                overrideRevisionCheck = nfCommon.isDefinedAndNotNull(options.overrideRevisionCheck) ? options.overrideRevisionCheck : overrideRevisionCheck;
            }

            var set = function (proposedConnectionEntity) {
                var currentConnectionEntity = connectionMap.get(proposedConnectionEntity.id);

                // set the connection if appropriate due to revision and wasn't previously removed
                if ((nfClient.isNewerRevision(currentConnectionEntity, proposedConnectionEntity) && !removedCache.has(proposedConnectionEntity.id)) || overrideRevisionCheck === true) {
                    connectionMap.set(proposedConnectionEntity.id, $.extend({
                        type: 'Connection'
                    }, proposedConnectionEntity));
                }
            };

            // determine how to handle the specified connection
            if ($.isArray(connectionEntities)) {
                $.each(connectionMap.keys(), function (_, key) {
                    var currentConnectionEntity = connectionMap.get(key);
                    var isPresent = $.grep(connectionEntities, function (proposedConnectionEntity) {
                        return proposedConnectionEntity.id === currentConnectionEntity.id;
                    });

                    // if the current connection is not present and was not recently added, remove it
                    if (isPresent.length === 0 && !addedCache.has(key)) {
                        connectionMap.remove(key);
                    }
                });
                $.each(connectionEntities, function (_, connectionEntity) {
                    set(connectionEntity);
                });
            } else if (nfCommon.isDefinedAndNotNull(connectionEntities)) {
                set(connectionEntities);
            }

            // apply the selection and handle all new connection
            var selection = select();
            selection.enter().call(renderConnections, selectAll);
            selection.call(updateConnections, {
                'updatePath': true,
                'updateLabel': true,
                'transition': transition
            }).call(sort);
            selection.exit().call(removeConnections);
        },

        /**
         * Refreshes the connection in the UI.
         *
         * @param {string} connectionId
         */
        refresh: function (connectionId) {
            if (nfCommon.isDefinedAndNotNull(connectionId)) {
                d3.select('#id-' + connectionId).call(updateConnections, {
                    'updatePath': true,
                    'updateLabel': true
                });
            } else {
                d3.selectAll('g.connection').call(updateConnections, {
                    'updatePath': true,
                    'updateLabel': true
                });
            }
        },

        /**
         * Refreshes the components necessary after a pan event.
         */
        pan: function () {
            d3.selectAll('g.connection.entering, g.connection.leaving').call(updateConnections, {
                'updatePath': false,
                'updateLabel': true
            });
        },

        /**
         * Removes the specified connection.
         *
         * @param {array|string} connectionIds      The connection id
         */
        remove: function (connectionIds) {
            var now = new Date().getTime();

            if ($.isArray(connectionIds)) {
                $.each(connectionIds, function (_, connectionId) {
                    removedCache.set(connectionId, now);
                    connectionMap.remove(connectionId);
                });
            } else {
                removedCache.set(connectionIds, now);
                connectionMap.remove(connectionIds);
            }

            // apply the selection and handle all removed connections
            select().exit().call(removeConnections);
        },

        /**
         * Removes all processors.
         */
        removeAll: function () {
            nfConnection.remove(connectionMap.keys());
        },

        /**
         * Reloads the connection state from the server and refreshes the UI.
         *
         * @param {string} id       The connection id
         */
        reload: function (id) {
            if (connectionMap.has(id)) {
                var connectionEntity = connectionMap.get(id);
                return $.ajax({
                    type: 'GET',
                    url: connectionEntity.uri,
                    dataType: 'json'
                }).done(function (response) {
                    nfConnection.set(response);
                });
            }
        },

        /**
         * Reloads the connection status from the server and refreshes the UI.
         *
         * @param {string} id       The connection id
         */
        reloadStatus: function (id) {
            if (connectionMap.has(id)) {
                return $.ajax({
                    type: 'GET',
                    url: '../nifi-api/flow/connections/' + encodeURIComponent(id) + '/status',
                    dataType: 'json'
                }).done(function (response) {
                    // update the existing connection
                    var connectionEntity = connectionMap.get(id);
                    connectionEntity.status = response.connectionStatus;
                    connectionMap.set(id, connectionEntity);

                    // update the UI
                    select().call(updateConnectionStatus);
                });
            }
        },

        /**
         * Gets the connection that have a source or destination component with the specified id.
         *
         * @param {string} id     component id
         * @returns {Array}     components connections
         */
        getComponentConnections: function (id) {
            var connections = [];
            connectionMap.forEach(function (_, entry) {
                // see if this component is the source or destination of this connection
                if (nfCanvasUtils.getConnectionSourceComponentId(entry) === id || nfCanvasUtils.getConnectionDestinationComponentId(entry) === id) {
                    connections.push(entry);
                }
            });
            return connections;
        },

        /**
         * If the connection id is specified it is returned. If no connection id
         * specified, all connections are returned.
         *
         * @param {string} id
         */
        get: function (id) {
            if (nfCommon.isUndefined(id)) {
                return connectionMap.values();
            } else {
                return connectionMap.get(id);
            }
        },

        /**
         * Expires the caches up to the specified timestamp.
         *
         * @param timestamp
         */
        expireCaches: function (timestamp) {
            var expire = function (cache) {
                cache.forEach(function (id, entryTimestamp) {
                    if (timestamp > entryTimestamp) {
                        cache.remove(id);
                    }
                });
            };

            expire(addedCache);
            expire(removedCache);
        }
    };

    return nfConnection;
}));