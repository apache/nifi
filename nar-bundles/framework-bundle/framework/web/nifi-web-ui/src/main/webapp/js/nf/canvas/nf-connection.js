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
nf.Connection = (function () {

    // the dimensions for the connection label
    var dimensions = {
        width: 188
    };

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
                if (nf.Common.isUndefined(minimumDistance) || distance < minimumDistance) {
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
        return terminal.groupId !== nf.Canvas.getGroupId() && (isInputPortType(terminal.type) || isOutputPortType(terminal.type));
    };

    /**
     * Sorts the specified connections according to the z index.
     * 
     * @param {type} connections
     */
    var sort = function (connections) {
        connections.sort(function (a, b) {
            return a.component.zIndex === b.component.zIndex ? 0 : a.component.zIndex > b.component.zIndex ? 1 : -1;
        });
    };

    /**
     * Selects the connection elements against the current connection map.
     */
    var select = function () {
        return connectionContainer.selectAll('g.connection').data(connectionMap.values(), function (d) {
            return d.component.id;
        });
    };

    var renderConnections = function (entered, selected) {
        if (entered.empty()) {
            return;
        }

        var connection = entered.append('g')
                .attr({
                    'id': function (d) {
                        return 'id-' + d.component.id;
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
        var selectableConnection = connection.append('path')
                .attr({
                    'class': 'connection-path-selectable',
                    'pointer-events': 'stroke'
                })
                .on('mousedown.selection', function () {
                    // select the connection when clicking the selectable path
                    nf.Selectable.select(d3.select(this.parentNode));
                })
                .call(nf.ContextMenu.activate);

        if (nf.Common.isDFM()) {
            // only support adding bend points when appropriate
            selectableConnection.on('dblclick', function (d) {
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
                    id: d.component.id,
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
            });
        }

        // update connection which will establish appropriate start/end points among other things
        connection.call(updateConnections, true, false);
    };

    // determines whether the specified connection contains an unsupported relationship
    var hasUnavailableRelationship = function (d) {
        var unavailable = false;

        // verify each selected relationship is still available
        if (nf.Common.isDefinedAndNotNull(d.component.selectedRelationships) && nf.Common.isDefinedAndNotNull(d.component.availableRelationships)) {
            $.each(d.component.selectedRelationships, function (_, selectedRelationship) {
                if ($.inArray(selectedRelationship, d.component.availableRelationships) === -1) {
                    unavailable = true;
                    return false;
                }
            });
        }

        return unavailable;
    };

    // updates the specified connections
    var updateConnections = function (updated, updatePath, updateLabel) {
        if (updated.empty()) {
            return;
        }

        if (updatePath === true) {
            updated.classed('grouped', function (d) {
                var grouped = false;

                // if there are more than one selected relationship, mark this as grouped
                if (nf.Common.isDefinedAndNotNull(d.component.selectedRelationships) && d.component.selectedRelationships.length > 1) {
                    grouped = true;
                }

                return grouped;
            })
                    .classed('ghost', function (d) {
                        var ghost = false;

                        // if the connection has a relationship that is unavailable, mark it a ghost relationship
                        if (hasUnavailableRelationship(d)) {
                            ghost = true;
                        }

                        return ghost;
                    });
        }

        updated.each(function (d) {
            var connection = d3.select(this);

            if (updatePath === true) {
                // calculate the start and end points
                var sourceComponentId = nf.CanvasUtils.getConnectionSourceComponentId(d.component);
                var sourceData = d3.select('#id-' + sourceComponentId).datum();
                var end;

                // get the appropriate end anchor point
                var endAnchor;
                if (d.bends.length > 0) {
                    endAnchor = d.bends[d.bends.length - 1];
                } else {
                    endAnchor = {
                        x: sourceData.component.position.x + (sourceData.dimensions.width / 2),
                        y: sourceData.component.position.y + (sourceData.dimensions.height / 2)
                    };
                }

                // if we are currently dragging the endpoint to a new target, use that 
                // position, otherwise we need to calculate it for the current target
                if (nf.Common.isDefinedAndNotNull(d.end) && d.end.dragging === true) {
                    // since we're dragging, use the same object thats bound to the endpoint drag event
                    end = d.end;

                    // if we're not over a connectable destination use the current point
                    var newDestination = d3.select('g.hover.connectable-destination');
                    if (!newDestination.empty()) {
                        var newDestinationData = newDestination.datum();

                        // get the position on the new destination perimeter
                        var newEnd = nf.CanvasUtils.getPerimeterPoint(endAnchor, {
                            'x': newDestinationData.component.position.x,
                            'y': newDestinationData.component.position.y,
                            'width': newDestinationData.dimensions.width,
                            'height': newDestinationData.dimensions.height
                        });

                        // update the coordinates with the new point
                        end.x = newEnd.x;
                        end.y = newEnd.y;
                    }
                } else {
                    var destinationComponentId = nf.CanvasUtils.getConnectionDestinationComponentId(d.component);
                    var destinationData = d3.select('#id-' + destinationComponentId).datum();

                    // get the position on the destination perimeter
                    end = nf.CanvasUtils.getPerimeterPoint(endAnchor, {
                        'x': destinationData.component.position.x,
                        'y': destinationData.component.position.y,
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
                var start = nf.CanvasUtils.getPerimeterPoint(startAnchor, {
                    'x': sourceData.component.position.x,
                    'y': sourceData.component.position.y,
                    'width': sourceData.dimensions.width,
                    'height': sourceData.dimensions.height
                });

                // store the updated endpoints
                d.start = start;
                d.end = end;

                // update the connection paths
                connection.select('path.connection-path')
                        .attr({
                            'd': function () {
                                var datum = [d.start].concat(d.bends, [d.end]);
                                return lineGenerator(datum);
                            },
                            'marker-end': function () {
                                var marker = 'normal';

                                // if the connection has a relationship that is unavailable, mark it a ghost relationship
                                if (hasUnavailableRelationship(d)) {
                                    marker = 'ghost';
                                }

                                return 'url(#' + marker + ')';
                            }
                        });
                connection.select('path.connection-selection-path')
                        .attr({
                            'd': function () {
                                var datum = [d.start].concat(d.bends, [d.end]);
                                return lineGenerator(datum);
                            }
                        });
                connection.select('path.connection-path-selectable')
                        .attr({
                            'd': function () {
                                var datum = [d.start].concat(d.bends, [d.end]);
                                return lineGenerator(datum);
                            }
                        });

                // -----
                // bends
                // -----

                if (nf.Common.isDFM()) {
                    // ------------------
                    // bends - startpoint
                    // ------------------

                    var startpoints = connection.selectAll('rect.startpoint').data([d.start]);

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
                                nf.Selectable.select(d3.select(this.parentNode));
                            })
                            .call(nf.ContextMenu.activate);

                    // update the start point
                    startpoints.attr('transform', function (p) {
                        return 'translate(' + (p.x - 4) + ', ' + (p.y - 4) + ')';
                    });

                    // remove old items
                    startpoints.exit().remove();

                    // ----------------
                    // bends - endpoint
                    // ----------------

                    var endpoints = connection.selectAll('rect.endpoint').data([d.end]);

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
                                nf.Selectable.select(d3.select(this.parentNode));
                            })
                            .call(nf.ContextMenu.activate);

                    // update the end point
                    endpoints.attr('transform', function (p) {
                        return 'translate(' + (p.x - 4) + ', ' + (p.y - 4) + ')';
                    });

                    // remove old items
                    endpoints.exit().remove();

                    // -----------------
                    // bends - midpoints
                    // -----------------

                    var midpoints = connection.selectAll('rect.midpoint').data(d.bends);

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

                                // if this is a self loop prevent removing the last two bends
                                var sourceComponentId = nf.CanvasUtils.getConnectionSourceComponentId(d.component);
                                var destinationComponentId = nf.CanvasUtils.getConnectionDestinationComponentId(d.component);
                                if (sourceComponentId === destinationComponentId && d.component.bends.length <= 2) {
                                    nf.Dialog.showOkDialog({
                                        dialogContent: 'Looping connections must have at least two bend points.',
                                        overlayBackground: false
                                    });
                                    return;
                                }

                                var newBends = [];
                                var bendIndex = -1;

                                // create a new array of bends without the selected one
                                $.each(d.component.bends, function (i, bend) {
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
                                    id: d.component.id,
                                    bends: newBends
                                };

                                // update the label index if necessary
                                var labelIndex = d.component.labelIndex;
                                if (newBends.length <= 1) {
                                    connection.labelIndex = 0;
                                } else if (bendIndex <= labelIndex) {
                                    connection.labelIndex = Math.max(0, labelIndex - 1);
                                }

                                // save the updated connection
                                save(d, connection);
                            })
                            .on('mousedown.selection', function () {
                                // select the connection when clicking the label
                                nf.Selectable.select(d3.select(this.parentNode));
                            })
                            .call(nf.ContextMenu.activate);

                    // update the midpoints
                    midpoints.attr('transform', function (p) {
                        return 'translate(' + (p.x - 4) + ', ' + (p.y - 4) + ')';
                    });

                    // remove old items
                    midpoints.exit().remove();
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
                                    nf.Selectable.select(d3.select(this.parentNode));
                                })
                                .call(nf.ContextMenu.activate);

                        // connection label
                        connectionLabelContainer.append('rect')
                                .attr({
                                    'class': 'connection-label',
                                    'width': dimensions.width,
                                    'x': 0,
                                    'y': 0
                                });
                    }

                    var labelCount = 0;

                    // -----------------------
                    // connection label - from
                    // -----------------------

                    var connectionFrom = connectionLabelContainer.select('g.connection-from-container');

                    // determine if the connection require a from label
                    if (isGroup(d.component.source)) {
                        // see if the connection from label is already rendered
                        if (connectionFrom.empty()) {
                            connectionFrom = connectionLabelContainer.append('g')
                                    .attr({
                                        'class': 'connection-from-container'
                                    });

                            connectionFrom.append('text')
                                    .attr({
                                        'class': 'connection-stats-label',
                                        'x': 0,
                                        'y': 10
                                    })
                                    .text('From');

                            connectionFrom.append('text')
                                    .attr({
                                        'class': 'connection-stats-value connection-from',
                                        'x': 33,
                                        'y': 10,
                                        'width': 130
                                    });

                            connectionFrom.append('image')
                                    .call(nf.CanvasUtils.disableImageHref)
                                    .attr({
                                        'class': 'connection-from-run-status',
                                        'width': 10,
                                        'height': 10,
                                        'x': 167,
                                        'y': 1
                                    });
                        }

                        // update the connection from positioning
                        connectionFrom.attr('transform', function () {
                            var y = 5 + (15 * labelCount++);
                            return 'translate(5, ' + y + ')';
                        });

                        // update the label text
                        connectionFrom.select('text.connection-from')
                                .each(function (d) {
                                    var connectionFromLabel = d3.select(this);

                                    // reset the label name to handle any previous state
                                    connectionFromLabel.text(null).selectAll('title').remove();

                                    // apply ellipsis to the label as necessary
                                    nf.CanvasUtils.ellipsis(connectionFromLabel, d.component.source.name);
                                }).append('title').text(function (d) {
                            return d.component.source.name;
                        });

                        // update the label run status
                        connectionFrom.select('image.connection-from-run-status').attr('xlink:href', function () {
                            if (d.component.source.exists === false) {
                                return 'images/portRemoved.png';
                            } else if (d.component.source.running === true) {
                                return 'images/portRunning.png';
                            } else {
                                return 'images/portStopped.png';
                            }
                        });
                    } else {
                        // there is no connection from, but check if the name was previous
                        // rendered so it can be removed
                        if (!connectionFrom.empty()) {
                            connectionFrom.remove();
                        }
                    }

                    // ---------------------
                    // connection label - to
                    // ---------------------

                    var connectionTo = connectionLabelContainer.select('g.connection-to-container');

                    // determine if the connection require a to label
                    if (isGroup(d.component.destination)) {
                        // see if the connection to label is already rendered
                        if (connectionTo.empty()) {
                            connectionTo = connectionLabelContainer.append('g')
                                    .attr({
                                        'class': 'connection-to-container'
                                    });

                            connectionTo.append('text')
                                    .attr({
                                        'class': 'connection-stats-label',
                                        'x': 0,
                                        'y': 10
                                    })
                                    .text('To');

                            connectionTo.append('text')
                                    .attr({
                                        'class': 'connection-stats-value connection-to',
                                        'x': 18,
                                        'y': 10,
                                        'width': 145
                                    });

                            connectionTo.append('image')
                                    .call(nf.CanvasUtils.disableImageHref)
                                    .attr({
                                        'class': 'connection-to-run-status',
                                        'width': 10,
                                        'height': 10,
                                        'x': 167,
                                        'y': 1
                                    });
                        }

                        // update the connection to positioning
                        connectionTo.attr('transform', function () {
                            var y = 5 + (15 * labelCount++);
                            return 'translate(5, ' + y + ')';
                        });

                        // update the label text
                        connectionTo.select('text.connection-to')
                                .each(function (d) {
                                    var connectionToLabel = d3.select(this);

                                    // reset the label name to handle any previous state
                                    connectionToLabel.text(null).selectAll('title').remove();

                                    // apply ellipsis to the label as necessary
                                    nf.CanvasUtils.ellipsis(connectionToLabel, d.component.destination.name);
                                }).append('title').text(function (d) {
                            return d.component.destination.name;
                        });

                        // update the label run status
                        connectionTo.select('image.connection-to-run-status').attr('xlink:href', function () {
                            if (d.component.destination.exists === false) {
                                return 'images/portRemoved.png';
                            } else if (d.component.destination.running === true) {
                                return 'images/portRunning.png';
                            } else {
                                return 'images/portStopped.png';
                            }
                        });
                    } else {
                        // there is no connection to, but check if the name was previous
                        // rendered so it can be removed
                        if (!connectionTo.empty()) {
                            connectionTo.remove();
                        }
                    }

                    // -----------------------
                    // connection label - name
                    // -----------------------

                    // get the connection name
                    var connectionNameValue = nf.CanvasUtils.formatConnectionName(d.component);
                    var connectionName = connectionLabelContainer.select('g.connection-name-container');

                    // is there a name to render
                    if (!nf.Common.isBlank(connectionNameValue)) {
                        // see if the connection name label is already rendered
                        if (connectionName.empty()) {
                            connectionName = connectionLabelContainer.append('g')
                                    .attr({
                                        'class': 'connection-name-container'
                                    });

                            connectionName.append('text')
                                    .attr({
                                        'class': 'connection-stats-label',
                                        'x': 0,
                                        'y': 10
                                    })
                                    .text('Name');

                            connectionName.append('text')
                                    .attr({
                                        'class': 'connection-stats-value connection-name',
                                        'x': 35,
                                        'y': 10,
                                        'width': 142
                                    });
                        }

                        // update the connection name positioning
                        connectionName.attr('transform', function () {
                            var y = 5 + (15 * labelCount++);
                            return 'translate(5, ' + y + ')';
                        });

                        // update the connection name
                        connectionName.select('text.connection-name')
                                .each(function (d) {
                                    var connectionToLabel = d3.select(this);

                                    // reset the label name to handle any previous state
                                    connectionToLabel.text(null).selectAll('title').remove();

                                    // apply ellipsis to the label as necessary
                                    nf.CanvasUtils.ellipsis(connectionToLabel, connectionNameValue);
                                }).append('title').text(function (d) {
                            return connectionNameValue;
                        });
                    } else {
                        // there is no connection name, but check if the name was previous
                        // rendered so it can be removed
                        if (!connectionName.empty()) {
                            connectionName.remove();
                        }
                    }

                    // -------------------------
                    // connection label - queued
                    // -------------------------

                    // see if the queue label is already rendered
                    var queued = connectionLabelContainer.select('g.queued-container');
                    if (queued.empty()) {
                        queued = connectionLabelContainer.append('g')
                                .attr({
                                    'class': 'queued-container'
                                });

                        queued.append('text')
                                .attr({
                                    'class': 'connection-stats-label',
                                    'x': 0,
                                    'y': 10
                                })
                                .text('Queued');

                        queued.append('text')
                                .attr({
                                    'class': 'connection-stats-value queued',
                                    'x': 46,
                                    'y': 10
                                });
                    }

                    // update the queued vertical positioning as necessary
                    queued.attr('transform', function () {
                        var y = 5 + (15 * labelCount++);
                        return 'translate(5, ' + y + ')';
                    });

                    // update the height based on the labels being rendered
                    connectionLabelContainer.select('rect.connection-label')
                            .attr('height', function () {
                                return 5 + (15 * labelCount) + 3;
                            });

                    if (nf.Common.isDFM()) {
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
            connection.select('g.connection-label-container')
                    .attr('transform', function () {
                        var label = d3.select(this).select('rect.connection-label');
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

        updated.select('text.queued')
                .text(function (d) {
                    if (nf.Common.isDefinedAndNotNull(d.status)) {
                        return d.status.queued;
                    } else {
                        return '- / -';
                    }
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
        var revision = nf.Client.getRevision();

        var entity = {
            revision: revision,
            connection: connection
        };

        return $.ajax({
            type: 'PUT',
            url: d.component.uri,
            data: JSON.stringify(entity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (response) {
            // update the revision
            nf.Client.setRevision(response.revision);

            // request was successful, update the entry
            nf.Connection.set(response.connection);
        }).fail(function (xhr, status, error) {
            if (xhr.status === 400 || xhr.status === 404 || xhr.status === 409) {
                nf.Dialog.showOkDialog({
                    dialogContent: nf.Common.escapeHtml(xhr.responseText),
                    overlayBackground: true
                });
            } else {
                nf.Common.handleAjaxError(xhr, status, error);
            }
        });
    };

    // removes the specified connections
    var removeConnections = function (removed) {
        removed.remove();
    };

    return {
        config: {
            selfLoopXOffset: (dimensions.width / 2) + 5,
            selfLoopYOffset: 25
        },
        
        init: function () {
            connectionMap = d3.map();

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
                        d3.select(this.parentNode).call(updateConnections, true, false);
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
                                    id: connectionData.component.id,
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
                                    connection.call(updateConnections, true, false);
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
                            return nf.CanvasUtils.isValidConnectionDestination(d3.select(this));
                        });

                        // redraw this connection
                        d3.select(this.parentNode).call(updateConnections, true, false);
                    })
                    .on('dragend', function (d) {
                        // indicate that dragging as stopped
                        d.dragging = false;

                        // get the corresponding connection
                        var connection = d3.select(this.parentNode);
                        var connectionData = connection.datum();

                        // attempt to select a new destination
                        var destination = d3.select('g.connectable-destination');

                        // resets the connection if we're not over a new destination
                        if (destination.empty()) {
                            connection.call(updateConnections, true, false);
                        } else {
                            // prompt for the new port if appropriate
                            if (nf.CanvasUtils.isProcessGroup(destination) || nf.CanvasUtils.isRemoteProcessGroup(destination)) {
                                // user will select new port and updated connect details will be set accordingly
                                nf.ConnectionConfiguration.showConfiguration(connection, destination).fail(function () {
                                    // reset the connection
                                    connection.call(updateConnections, true, false);
                                });
                            } else {
                                var revision = nf.Client.getRevision();

                                // get the destination details
                                var destinationData = destination.datum();
                                var destinationType = nf.CanvasUtils.getConnectableTypeForDestination(destination);

                                var updatedConnectionData = {
                                    version: revision.version,
                                    clientId: revision.clientId,
                                    destinationId: destinationData.component.id,
                                    destinationType: destinationType,
                                    destinationGroupId: nf.Canvas.getGroupId()
                                };

                                // if this is a self loop and there are less than 2 bends, add them
                                if (connectionData.bends.length < 2 && connectionData.component.source.id === destinationData.component.id) {
                                    var rightCenter = {
                                        x: destinationData.component.position.x + (destinationData.dimensions.width),
                                        y: destinationData.component.position.y + (destinationData.dimensions.height / 2)
                                    };
                                    var xOffset = nf.Connection.config.selfLoopXOffset;
                                    var yOffset = nf.Connection.config.selfLoopYOffset;

                                    updatedConnectionData.bends = [];
                                    updatedConnectionData.bends.push((rightCenter.x + xOffset) + ',' + (rightCenter.y - yOffset));
                                    updatedConnectionData.bends.push((rightCenter.x + xOffset) + ',' + (rightCenter.y + yOffset));
                                }

                                $.ajax({
                                    type: 'PUT',
                                    url: connectionData.component.uri,
                                    data: updatedConnectionData,
                                    dataType: 'json'
                                }).done(function (response) {
                                    var connectionData = response.connection;

                                    // update the revision
                                    nf.Client.setRevision(response.revision);

                                    // refresh to update the label
                                    nf.Connection.set(connectionData);
                                }).fail(function (xhr, status, error) {
                                    if (xhr.status === 400 || xhr.status === 404 || xhr.status === 409) {
                                        nf.Dialog.showOkDialog({
                                            dialogContent: nf.Common.escapeHtml(xhr.responseText),
                                            overlayBackground: true
                                        });

                                        // reset the connection
                                        connection.call(updateConnections, true, false);
                                    } else {
                                        nf.Common.handleAjaxError(xhr, status, error);
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
                                var connectionLabel = d3.select(this).select('rect.connection-label');

                                var position = getLabelPosition(connectionLabel);
                                var width = dimensions.width;
                                var height = connectionLabel.attr('height');

                                // create a selection box for the move
                                drag = d3.select('#canvas').append('rect')
                                        .attr('rx', 6)
                                        .attr('ry', 6)
                                        .attr('x', position.x)
                                        .attr('y', position.y)
                                        .attr('class', 'label-drag')
                                        .attr('width', width)
                                        .attr('height', height)
                                        .attr('stroke-width', function () {
                                            return 1 / nf.Canvas.View.scale();
                                        })
                                        .attr('stroke-dasharray', function () {
                                            return 4 / nf.Canvas.View.scale();
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
                            d3.select(this.parentNode).call(updateConnections, true, false);
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
                                    id: d.component.id,
                                    labelIndex: d.labelIndex
                                }).fail(function () {
                                    // restore the previous label index
                                    d.labelIndex = d.component.labelIndex;

                                    // refresh the connection
                                    connection.call(updateConnections, true, false);
                                });
                            }
                        }

                        // stop further propagation
                        d3.event.sourceEvent.stopPropagation();
                    });
        },
        
        /**
         * Populates the graph with the specified connections.
         * 
         * @argument {object | array} connections               The connections to add
         * @argument {boolean} selectAll                Whether or not to select the new contents
         */
        add: function (connections, selectAll) {
            selectAll = nf.Common.isDefinedAndNotNull(selectAll) ? selectAll : false;

            var add = function (connection) {
                // add the connection
                connectionMap.set(connection.id, {
                    type: 'Connection',
                    component: connection,
                    bends: $.map(connection.bends, function (bend) {
                        return {
                            x: bend.x,
                            y: bend.y
                        };
                    }),
                    labelIndex: connection.labelIndex
                });
            };

            // determine how to handle the specified connection
            if ($.isArray(connections)) {
                $.each(connections, function (_, connection) {
                    add(connection);
                });
            } else {
                add(connections);
            }

            // apply the selection and handle all new connection
            select().enter().call(renderConnections, selectAll);
        },
        
        /**
         * Reorders the connections based on their current z index.
         */
        reorder: function () {
            d3.selectAll('g.connection').call(sort);
        },
        
        /**
         * Sets the value of the specified connection.
         * 
         * @param {type} connection
         */
        set: function (connection) {
            var set = function (conn) {
                if (connectionMap.has(conn.id)) {
                    // update the current entry
                    var connectionEntry = connectionMap.get(conn.id);
                    connectionEntry.component = conn;
                    connectionEntry.bends = $.map(conn.bends, function (bend) {
                        return {
                            x: bend.x,
                            y: bend.y
                        };
                    });
                    connectionEntry.labelIndex = conn.labelIndex;

                    // update the connection in the UI
                    d3.select('#id-' + conn.id).call(updateConnections, true, true);
                }
            };

            // determine how to handle the specified connection
            if ($.isArray(connection)) {
                $.each(connection, function (_, conn) {
                    set(conn);
                });
            } else {
                set(connection);
            }
        },
        
        /**
         * Sets the connection status using the specified status.
         * 
         * @param {array} connectionStatus
         */
        setStatus: function (connectionStatus) {
            if (nf.Common.isEmpty(connectionStatus)) {
                return;
            }

            // update the connection status
            $.each(connectionStatus, function (_, status) {
                if (connectionMap.has(status.id)) {
                    var connection = connectionMap.get(status.id);
                    connection.status = status;
                }
            });

            // update the visible connections
            d3.selectAll('g.connection.visible').call(updateConnectionStatus);
        },
        
        /**
         * Refreshes the connection in the UI.
         * 
         * @param {string} connectionId
         */
        refresh: function (connectionId) {
            if (nf.Common.isDefinedAndNotNull(connectionId)) {
                d3.select('#id-' + connectionId).call(updateConnections, true, true);
            } else {
                d3.selectAll('g.connection').call(updateConnections, true, true);
            }
        },
        
        /**
         * Refreshes the components necessary after a pan event.
         */
        pan: function () {
            d3.selectAll('g.connection.entering, g.connection.leaving').call(updateConnections, false, true);
        },
        
        /**
         * Removes the specified connection.
         * 
         * @param {array|string} connections      The connection id
         */
        remove: function (connections) {
            if ($.isArray(connections)) {
                $.each(connections, function (_, connection) {
                    connectionMap.remove(connection);
                });
            } else {
                connectionMap.remove(connections);
            }

            // apply the selection and handle all removed connections
            select().exit().call(removeConnections);
        },
        
        /**
         * Removes all processors.
         */
        removeAll: function () {
            nf.Connection.remove(connectionMap.keys());
        },
        
        /**
         * Reloads the connection state from the server and refreshes the UI.
         * 
         * @param {object} connection       The connection to reload
         */
        reload: function (connection) {
            if (connectionMap.has(connection.id)) {
                return $.ajax({
                    type: 'GET',
                    url: connection.uri,
                    dataType: 'json'
                }).done(function (response) {
                    nf.Connection.set(response.connection);
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
                var connection = entry.component;

                // see if this component is the source or destination of this connection
                if (nf.CanvasUtils.getConnectionSourceComponentId(connection) === id || nf.CanvasUtils.getConnectionDestinationComponentId(connection) === id) {
                    connections.push(connection);
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
            if (nf.Common.isUndefined(id)) {
                return connectionMap.values();
            } else {
                return connectionMap.get(id);
            }
        }
    };
}());