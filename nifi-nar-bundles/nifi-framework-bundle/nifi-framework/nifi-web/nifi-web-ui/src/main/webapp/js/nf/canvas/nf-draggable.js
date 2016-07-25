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

nf.Draggable = (function () {

    var drag;

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
        
        var updateComponentPosition = function(d) {
            var newPosition = {
                'x': d.position.x + delta.x,
                'y': d.position.y + delta.y
            };

            // build the entity
            var entity = {
                'revision': nf.Client.getRevision(d),
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
                    nf[d.type].set(response);

                    // resolve with an object so we can refresh when finished
                    deferred.resolve({
                        type: d.type,
                        id: d.id
                    });
                }).fail(function (xhr, status, error) {
                    if (xhr.status === 400 || xhr.status === 404 || xhr.status === 409) {
                        nf.Dialog.showOkDialog({
                            headerText: 'Component Position',
                            dialogContent: nf.Common.escapeHtml(xhr.responseText)
                        });
                    } else {
                        nf.Common.handleAjaxError(xhr, status, error);
                    }

                    deferred.reject();
                });
            }).promise();
        };
        
        var updateConnectionPosition = function(d) {
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
                'revision': nf.Client.getRevision(d),
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
                    nf.Connection.set(response);

                    // resolve with an object so we can refresh when finished
                    deferred.resolve({
                        type: d.type,
                        id: d.id
                    });
                }).fail(function (xhr, status, error) {
                    if (xhr.status === 400 || xhr.status === 404 || xhr.status === 409) {
                        nf.Dialog.showOkDialog({
                            headerText: 'Component Position',
                            dialogContent: nf.Common.escapeHtml(xhr.responseText)
                        });
                    } else {
                        nf.Common.handleAjaxError(xhr, status, error);
                    }

                    deferred.reject();
                });
            }).promise();
        };

        var selectedConnections = d3.selectAll('g.connection.selected');
        var selectedComponents = d3.selectAll('g.component.selected');

        // ensure every component is writable
        if (nf.CanvasUtils.canModify(selectedConnections) === false || nf.CanvasUtils.canModify(selectedComponents) === false) {
            nf.Dialog.showOkDialog({
                headerText: 'Component Position',
                dialogContent: 'Must be authorized to modify every component selected.'
            });
            return;
        }

        // go through each selected connection
        selectedConnections.each(function (d) {
            var connectionUpdate = updateConnectionPosition(d);
            if (connectionUpdate !== null) {
                updates.set(d.id, connectionUpdate);
            }
        });
        
        // go through each selected component
        selectedComponents.each(function (d) {
            // consider any self looping connections
            var connections = nf.Connection.getComponentConnections(d.id);
            $.each(connections, function(_, connection) {
                if (!updates.has(connection.id) && nf.CanvasUtils.getConnectionSourceComponentId(connection) === nf.CanvasUtils.getConnectionDestinationComponentId(connection)) {
                    var connectionUpdate = updateConnectionPosition(nf.Connection.get(connection.id));
                    if (connectionUpdate !== null) {
                        updates.set(connection.id, connectionUpdate);
                    }
                }
            });
            
            // consider the component itself
            updates.set(d.id, updateComponentPosition(d));
        });

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
                    var componentConnections = nf.Connection.getComponentConnections(component.id);
                    $.each(componentConnections, function (_, connection) {
                        connections.add(connection.id);
                    });
                }
            });

            // refresh the connections
            connections.forEach(function (connectionId) {
                nf.Connection.refresh(connectionId);
            });
        }).always(function(){
            nf.Birdseye.refresh();
        });
    };

    /**
     * Updates the parent group of all selected components.
     */
    var updateComponentsGroup = function () {
        var selection = d3.selectAll('g.component.selected, g.connection.selected');
        var group = d3.select('g.drop');

        if (nf.CanvasUtils.canModify(selection) === false) {
            nf.Dialog.showOkDialog({
                headerText: 'Component Position',
                dialogContent: 'Must be authorized to modify every component selected.'
            });
            return;
        }
        if (nf.CanvasUtils.canModify(group) === false) {
            nf.Dialog.showOkDialog({
                headerText: 'Component Position',
                dialogContent: 'Not authorized to modify the destination group.'
            });
            return;
        }

        // move the seleciton into the group
        nf.CanvasUtils.moveComponents(selection, group);
    };

    return {
        init: function () {
            // handle component drag events
            drag = d3.behavior.drag()
                    .on('dragstart', function () {
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
                                        return 1 / nf.Canvas.View.scale();
                                    })
                                    .attr('stroke-dasharray', function () {
                                        return 4 / nf.Canvas.View.scale();
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
                            dragSelection.attr('x', function (d) {
                                        d.x += d3.event.dx;
                                        return d.x;
                                    })
                                    .attr('y', function (d) {
                                        d.y += d3.event.dy;
                                        return d.y;
                                    });
                        }
                    })
                    .on('dragend', function () {
                        // stop further propagation
                        d3.event.sourceEvent.stopPropagation();

                        // get the drag selection
                        var dragSelection = d3.select('rect.drag-selection');

                        // ensure we found a drag selection
                        if (dragSelection.empty()) {
                            return;
                        }

                        // either move or update the selections group as appropriate
                        if (d3.select('g.drop').empty()) {
                            updateComponentsPosition(dragSelection);
                        } else {
                            updateComponentsGroup();
                        }

                        // remove the drag selection
                        dragSelection.remove();
                    });
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
}());