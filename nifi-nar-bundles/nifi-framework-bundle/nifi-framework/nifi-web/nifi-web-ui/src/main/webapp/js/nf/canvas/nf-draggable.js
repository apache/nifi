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
                'nf.Storage',
                'nf.Connection',
                'nf.Birdseye',
                'nf.CanvasUtils',
                'nf.Common',
                'nf.Dialog',
                'nf.Client',
                'nf.ErrorHandler'],
            function ($, d3, nfStorage, nfConnection, nfBirdseye, nfCanvasUtils, nfCommon, nfDialog, nfClient, nfErrorHandler) {
                return (nf.Draggable = factory($, d3, nfStorage, nfConnection, nfBirdseye, nfCanvasUtils, nfCommon, nfDialog, nfClient, nfErrorHandler));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Draggable =
            factory(require('jquery'),
                require('d3'),
                require('nf.Storage'),
                require('nf.Connection'),
                require('nf.Birdseye'),
                require('nf.CanvasUtils'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.Client'),
                require('nf.ErrorHandler')));
    } else {
        nf.Draggable = factory(root.$,
            root.d3,
            root.nf.Storage,
            root.nf.Connection,
            root.nf.Birdseye,
            root.nf.CanvasUtils,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.Client,
            root.nf.ErrorHandler);
    }
}(this, function ($, d3, nfStorage, nfConnection, nfBirdseye, nfCanvasUtils, nfCommon, nfDialog, nfClient, nfErrorHandler) {
    'use strict';

    var nfCanvas;
    var drag;
    var snapAlignmentPixels = 8;
    var snapEnabled = true;

    /**
     * Updates the positioning of all selected components.
     *
     * @param {selection} dragSelection The current drag selection
     */
    var updateComponentsPosition = function (dragSelection) {
        var updates = d3.map();

        // determine the drag delta
        var dragData = dragSelection.datum();
        var delta = {
            x: dragData.x - dragData.original.x,
            y: dragData.y - dragData.original.y
        };

        // if the component didn't move, return
        if (delta.x === 0 && delta.y === 0) {
            return;
        }

        var selectedConnections = d3.selectAll('g.connection.selected');
        var selectedComponents = d3.selectAll('g.component.selected');

        // ensure every component is writable
        if (nfCanvasUtils.canModify(selectedConnections) === false || nfCanvasUtils.canModify(selectedComponents) === false) {
            nfDialog.showOkDialog({
                headerText: 'Component Position',
                dialogContent: 'Must be authorized to modify every component selected.'
            });
            return;
        }

        // go through each selected connection
        selectedConnections.each(function (d) {
            var connectionUpdate = nfDraggable.updateConnectionPosition(d, delta);
            if (connectionUpdate !== null) {
                updates.set(d.id, connectionUpdate);
            }
        });

        // go through each selected component
        selectedComponents.each(function (d) {
            // consider any self looping connections
            var connections = nfConnection.getComponentConnections(d.id);
            $.each(connections, function (_, connection) {
                if (!updates.has(connection.id) && nfCanvasUtils.getConnectionSourceComponentId(connection) === nfCanvasUtils.getConnectionDestinationComponentId(connection)) {
                    var connectionUpdate = nfDraggable.updateConnectionPosition(nfConnection.get(connection.id), delta);
                    if (connectionUpdate !== null) {
                        updates.set(connection.id, connectionUpdate);
                    }
                }
            });

            // consider the component itself
            updates.set(d.id, nfDraggable.updateComponentPosition(d, delta));
        });

        nfDraggable.refreshConnections(updates);
    };

    /**
     * Updates the parent group of all selected components.
     *
     * @param {selection} the destination group
     */
    var updateComponentsGroup = function (group) {
        // get the selection and deselect the components being moved
        var selection = d3.selectAll('g.component.selected, g.connection.selected').classed('selected', false);

        if (nfCanvasUtils.canModify(selection) === false) {
            nfDialog.showOkDialog({
                headerText: 'Component Position',
                dialogContent: 'Must be authorized to modify every component selected.'
            });
            return;
        }
        if (nfCanvasUtils.canModify(group) === false) {
            nfDialog.showOkDialog({
                headerText: 'Component Position',
                dialogContent: 'Not authorized to modify the destination group.'
            });
            return;
        }

        // move the seleciton into the group
        nfCanvasUtils.moveComponents(selection, group);
    };

    var nfDraggable = {
        init: function (canvas) {
            nfCanvas = canvas;

            // handle component drag events
            drag = d3.drag()
                .on('start', function () {
                    // stop further propagation
                    d3.event.sourceEvent.stopPropagation();
                })
                .on('drag', function () {
                    var dragSelection = d3.select('rect.drag-selection');

                    // lazily create the drag selection box
                    if (dragSelection.empty()) {
                        // get the current selection
                        var selection = d3.selectAll('g.component.selected');

                        // determine the appropriate bounding box
                        var minX = null, maxX = null, minY = null, maxY = null;
                        selection.each(function (d) {
                            if (minX === null || d.position.x < minX) {
                                minX = d.position.x;
                            }
                            if (minY === null || d.position.y < minY) {
                                minY = d.position.y;
                            }
                            var componentMaxX = d.position.x + d.dimensions.width;
                            var componentMaxY = d.position.y + d.dimensions.height;
                            if (maxX === null || componentMaxX > maxX) {
                                maxX = componentMaxX;
                            }
                            if (maxY === null || componentMaxY > maxY) {
                                maxY = componentMaxY;
                            }
                        });

                        // create a selection box for the move
                        d3.select('#canvas').append('rect')
                            .attr('rx', 6)
                            .attr('ry', 6)
                            .attr('x', minX)
                            .attr('y', minY)
                            .attr('class', 'drag-selection')
                            .attr('pointer-events', 'none')
                            .attr('width', maxX - minX)
                            .attr('height', maxY - minY)
                            .attr('stroke-width', function () {
                                return 1 / nfCanvasUtils.getCanvasScale();
                            })
                            .attr('stroke-dasharray', function () {
                                return 4 / nfCanvasUtils.getCanvasScale();
                            })
                            .datum({
                                original: {
                                    x: minX,
                                    y: minY
                                },
                                x: minX,
                                y: minY
                            });
                    } else {
                        // update the position of the drag selection
                        // snap align the position unless the user is holding shift
                        snapEnabled = !d3.event.sourceEvent.shiftKey;
                        dragSelection.attr('x', function (d) {
                            d.x += d3.event.dx;
                            return snapEnabled ? (Math.round(d.x/snapAlignmentPixels) * snapAlignmentPixels) : d.x;
                        }).attr('y', function (d) {
                                d.y += d3.event.dy;
                                return snapEnabled ? (Math.round(d.y/snapAlignmentPixels) * snapAlignmentPixels) : d.y;
                        });
                     }
                })
                .on('end', function () {
                    // stop further propagation
                    d3.event.sourceEvent.stopPropagation();

                    // get the drag selection
                    var dragSelection = d3.select('rect.drag-selection');

                    // ensure we found a drag selection
                    if (dragSelection.empty()) {
                        return;
                    }

                    // get the destination group if applicable... remove the drop flag if necessary to prevent
                    // subsequent drop events from triggering prior to this move's completion
                    var group = d3.select('g.drop').classed('drop', false);

                    // either move or update the selections group as appropriate
                    if (group.empty()) {
                        updateComponentsPosition(dragSelection);
                    } else {
                        updateComponentsGroup(group);
                    }

                    // remove the drag selection
                    dragSelection.remove();
                });
        },

        /**
         * Update the component's position
         *
         * @param d     The component
         * @param delta The change in position
         * @returns {*}
         */
        updateComponentPosition: function (d, delta) {
            var newPosition = {
                'x': snapEnabled ? (Math.round((d.position.x + delta.x)/snapAlignmentPixels) * snapAlignmentPixels) : d.position.x + delta.x,
                'y': snapEnabled ? (Math.round((d.position.y + delta.y)/snapAlignmentPixels) * snapAlignmentPixels) : d.position.y + delta.y
            };

            // build the entity
            var entity = {
                'revision': nfClient.getRevision(d),
                'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
                'component': {
                    'id': d.id,
                    'position': newPosition
                }
            };

            // update the component positioning
            return $.Deferred(function (deferred) {
                $.ajax({
                    type: 'PUT',
                    url: d.uri,
                    data: JSON.stringify(entity),
                    dataType: 'json',
                    contentType: 'application/json'
                }).done(function (response) {
                    // update the component
                    nfCanvasUtils.getComponentByType(d.type).set(response);

                    // resolve with an object so we can refresh when finished
                    deferred.resolve({
                        type: d.type,
                        id: d.id
                    });
                }).fail(function (xhr, status, error) {
                    nfErrorHandler.handleAjaxError(xhr, status, error);
                    deferred.reject();
                });
            }).promise();
        },

        /**
         * Update the connection's position
         *
         * @param d     The connection
         * @param delta The change in position
         * @returns {*}
         */
        updateConnectionPosition: function (d, delta) {
            // only update if necessary
            if (d.bends.length === 0) {
                return null;
            }

            // calculate the new bend points
            var newBends = $.map(d.bends, function (bend) {
                return {
                    x: bend.x + delta.x,
                    y: bend.y + delta.y
                };
            });

            var entity = {
                'revision': nfClient.getRevision(d),
                'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
                'component': {
                    id: d.id,
                    bends: newBends
                }
            };

            // update the component positioning
            return $.Deferred(function (deferred) {
                $.ajax({
                    type: 'PUT',
                    url: d.uri,
                    data: JSON.stringify(entity),
                    dataType: 'json',
                    contentType: 'application/json'
                }).done(function (response) {
                    // update the component
                    nfConnection.set(response);

                    // resolve with an object so we can refresh when finished
                    deferred.resolve({
                        type: d.type,
                        id: d.id
                    });
                }).fail(function (xhr, status, error) {
                    nfErrorHandler.handleAjaxError(xhr, status, error);

                    deferred.reject();
                });
            }).promise();
        },

        /**
         * Refresh the connections after dragging a component
         *
         * @param updates
         */
        refreshConnections: function (updates) {
            if (updates.size() > 0) {
                // wait for all updates to complete
                $.when.apply(window, updates.values()).done(function () {
                    var dragged = $.makeArray(arguments);
                    var connections = d3.set();

                    // refresh this component
                    $.each(dragged, function (_, component) {
                        // check if the component in question is a connection
                        if (component.type === 'Connection') {
                            connections.add(component.id);
                        } else {
                            // get connections that need to be refreshed because its attached to this component
                            var componentConnections = nfConnection.getComponentConnections(component.id);
                            $.each(componentConnections, function (_, connection) {
                                connections.add(connection.id);
                            });
                        }
                    });

                    // refresh the connections
                    connections.each(function (connectionId) {
                        nfConnection.refresh(connectionId);
                    });
                }).always(function () {
                    nfBirdseye.refresh();
                });
            }
        },

        /**
         * Activates the drag behavior for the components in the specified selection.
         *
         * @param {selection} components
         */
        activate: function (components) {
            components.classed('moveable', true).call(drag);
        },

        /**
         * Deactivates the drag behavior for the components in the specified selection.
         *
         * @param {selection} components
         */
        deactivate: function (components) {
            components.classed('moveable', false).on('.drag', null);
        }
    };

    return nfDraggable;
}));