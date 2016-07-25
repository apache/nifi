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

nf.CanvasUtils = (function () {

    var config = {
        storage: {
            namePrefix: 'nifi-view-'
        },
        urls: {
            controller: '../nifi-api/controller'
        }
    };

    var TWO_PI = 2 * Math.PI;

    var binarySearch = function (length, comparator) {
        var low = 0;
        var high = length - 1;
        var mid;

        var result = 0;
        while (low <= high) {
            mid = ~~((low + high) / 2);
            result = comparator(mid);
            if (result < 0) {
                high = mid - 1;
            } else if (result > 0) {
                low = mid + 1;
            } else {
                break;
            }
        }

        return mid;
    };
    
    var moveComponents = function (components, groupId) {
        return $.Deferred(function (deferred) {
            // ensure the current selection is eligible for move into the specified group
            nf.CanvasUtils.eligibleForMove(components, groupId).done(function () {
                // create a snippet for the specified components
                var snippet = nf.Snippet.marshal(components);
                nf.Snippet.create(snippet).done(function (response) {
                    // move the snippet into the target
                    nf.Snippet.move(response.snippet.id, groupId).done(function () {
                        var componentMap = d3.map();

                        // add the id to the type's array
                        var addComponent = function (type, id) {
                            if (!componentMap.has(type)) {
                                componentMap.set(type, []);
                            }
                            componentMap.get(type).push(id);
                        };

                        // go through each component being removed
                        components.each(function (d) {
                            addComponent(d.type, d.id);
                        });

                        // refresh all component types as necessary (handle components that have been removed)
                        componentMap.forEach(function (type, ids) {
                            nf[type].remove(ids);
                        });

                        // refresh the birdseye
                        nf.Birdseye.refresh();
                        deferred.resolve();
                    }).fail(nf.Common.handleAjaxError).fail(function () {
                        deferred.reject();
                    });
                }).fail(nf.Common.handleAjaxError).fail(function () {
                    deferred.reject();
                });
            }).fail(function () {
                deferred.reject();
            });
        }).promise();
    };

    return {
        config: {
            systemTooltipConfig: {
                style: {
                    classes: 'nifi-tooltip'
                },
                show: {
                    solo: true,
                    effect: false
                },
                hide: {
                    effect: false
                },
                position: {
                    at: 'bottom right',
                    my: 'top left'
                }
            }
        },

        /**
         * Calculates the point on the specified bounding box that is closest to the
         * specified point.
         * 
         * @param {object} p            The point
         * @param {object} bBox         The bounding box
         */
        getPerimeterPoint: function (p, bBox) {
            // calculate theta
            var theta = Math.atan2(bBox.height, bBox.width);

            // get the rectangle radius
            var xRadius = bBox.width / 2;
            var yRadius = bBox.height / 2;

            // get the center point
            var cx = bBox.x + xRadius;
            var cy = bBox.y + yRadius;

            // calculate alpha
            var dx = p.x - cx;
            var dy = p.y - cy;
            var alpha = Math.atan2(dy, dx);

            // normalize aphla into 0 <= alpha < 2 PI
            alpha = alpha % TWO_PI;
            if (alpha < 0) {
                alpha += TWO_PI;
            }

            // calculate beta
            var beta = (Math.PI / 2) - alpha;

            // detect the appropriate quadrant and return the point on the perimeter
            if ((alpha >= 0 && alpha < theta) || (alpha >= (TWO_PI - theta) && alpha < TWO_PI)) {
                // right quadrant
                return {
                    'x': bBox.x + bBox.width,
                    'y': cy + Math.tan(alpha) * xRadius
                };
            } else if (alpha >= theta && alpha < (Math.PI - theta)) {
                // bottom quadrant
                return {
                    'x': cx + Math.tan(beta) * yRadius,
                    'y': bBox.y + bBox.height
                };
            } else if (alpha >= (Math.PI - theta) && alpha < (Math.PI + theta)) {
                // left quadrant
                return {
                    'x': bBox.x,
                    'y': cy - Math.tan(alpha) * xRadius
                };
            } else {
                // top quadrant
                return {
                    'x': cx - Math.tan(beta) * yRadius,
                    'y': bBox.y
                };
            }
        },
        
        /**
         * Shows the specified component in the specified group.
         *
         * @argument {string} groupId       The id of the group
         * @argument {string} componentId   The id of the component
         */
        showComponent: function (groupId, componentId) {
            // ensure the group id is specified
            if (nf.Common.isDefinedAndNotNull(groupId)) {
                // initiate a graph refresh
                var refreshGraph = $.Deferred(function (deferred) {
                    // load a different group if necessary
                    if (groupId !== nf.Canvas.getGroupId()) {
                        // set the new group id
                        nf.Canvas.setGroupId(groupId);

                        // reload
                        nf.Canvas.reload().done(function () {
                            deferred.resolve();
                        }).fail(function () {
                            nf.Dialog.showOkDialog({
                                headerText: 'Process Group',
                                dialogContent: 'Unable to load the group for the specified component.'
                            });
                            deferred.reject();
                        });
                    } else {
                        deferred.resolve();
                    }
                }).promise();

                // when the refresh has completed, select the match
                refreshGraph.done(function () {
                    // attempt to locate the corresponding component
                    var component = d3.select('#id-' + componentId);
                    if (!component.empty()) {
                        nf.Actions.show(component);
                    } else {
                        nf.Dialog.showOkDialog({
                            headerText: 'Process Group',
                            dialogContent: 'Unable to find the specified component.'
                        });
                    }
                });
            }
        },
        
        /**
         * Gets the currently selected components and connections.
         * 
         * @returns {selection}     The currently selected components and connections
         */
        getSelection: function () {
            return d3.selectAll('g.component.selected, g.connection.selected');
        },

        /**
         * Centers the specified bounding box.
         * 
         * @param {type} boundingBox
         */
        centerBoundingBox: function (boundingBox) {
            var scale = nf.Canvas.View.scale();

            // get the canvas normalized width and height
            var canvasContainer = $('#canvas-container');
            var screenWidth = canvasContainer.width() / scale;
            var screenHeight = canvasContainer.height() / scale;

            // determine the center location for this component in canvas space
            var center = [(screenWidth / 2) - (boundingBox.width / 2), (screenHeight / 2) - (boundingBox.height / 2)];

            // calculate the difference between the center point and the position of this component and convert to screen space
            nf.Canvas.View.translate([(center[0] - boundingBox.x) * scale, (center[1] - boundingBox.y) * scale]);
        },

        /**
         * Enables/disables the editable behavior for the specified selection based on their access policies.
         * 
         * @param selection     selection
         */
        editable: function (selection) {
            if (nf.CanvasUtils.canModify(selection)) {
                if (!selection.classed('connectable')) {
                    selection.call(nf.Connectable.activate);
                }
                if (!selection.classed('moveable')) {
                    selection.call(nf.Draggable.activate);
                }
            } else {
                if (selection.classed('connectable')) {
                    selection.call(nf.Connectable.deactivate);
                }
                if (selection.classed('moveable')) {
                    selection.call(nf.Draggable.deactivate);
                }
            }
        },
        
        /**
         * Conditionally apply the transition.
         *
         * @param selection     selection
         * @param transition    transition
         */
        transition: function (selection, transition) {
            if (transition && !selection.empty()) {
                return selection.transition().duration(400);
            } else {
                return selection;
            }
        },

        /**
         * Position the component accordingly.
         * 
         * @param {selection} updated
         */
        position: function (updated, transition) {
            if (updated.empty()) {
                return;
            }
            
            return nf.CanvasUtils.transition(updated, transition)
                .attr('transform', function (d) {
                    return 'translate(' + d.position.x + ', ' + d.position.y + ')';
                });
        },
        
        /**
         * Applies single line ellipsis to the component in the specified selection if necessary.
         * 
         * @param {selection} selection
         * @param {string} text
         */
        ellipsis: function (selection, text) {
            var width = parseInt(selection.attr('width'), 10);
            var node = selection.node();

            // set the element text
            selection.text(text);

            // see if the field is too big for the field
            if (text.length > 0 && node.getSubStringLength(0, text.length - 1) > width) {
                // make some room for the ellipsis
                width -= 5;

                // determine the appropriate index
                var i = binarySearch(text.length, function (x) {
                    var length = node.getSubStringLength(0, x);
                    if (length > width) {
                        // length is too long, try the lower half
                        return -1;
                    } else if (length < width) {
                        // length is too short, try the upper half
                        return 1;
                    }
                    return 0;
                });

                // trim at the appropriate length and add ellipsis
                selection.text(text.substring(0, i) + String.fromCharCode(8230));
            }
        },
        
        /**
         * Applies multiline ellipsis to the component in the specified seleciton. Text will
         * wrap for the specified number of lines. The last line will be ellipsis if necessary.
         * 
         * @param {selection} selection
         * @param {integer} lineCount
         * @param {string} text
         */
        multilineEllipsis: function (selection, lineCount, text) {
            var i = 1;
            var words = text.split(/\s+/).reverse();

            // get the appropriate position
            var x = parseInt(selection.attr('x'), 10);
            var y = parseInt(selection.attr('y'), 10);
            var width = parseInt(selection.attr('width'), 10);

            var line = [];
            var tspan = selection.append('tspan')
                    .attr({
                        'x': x,
                        'y': y,
                        'width': width
                    });

            // go through each word
            var word = words.pop();
            while (nf.Common.isDefinedAndNotNull(word)) {
                // add the current word
                line.push(word);

                // update the label text
                tspan.text(line.join(' '));

                // if this word caused us to go too far
                if (tspan.node().getComputedTextLength() > width) {
                    // remove the current word
                    line.pop();

                    // update the label text
                    tspan.text(line.join(' '));

                    // create the tspan for the next line
                    tspan = selection.append('tspan')
                            .attr({
                                'x': x,
                                'dy': '1.2em',
                                'width': width
                            });

                    // if we've reached the last line, use single line ellipsis
                    if (++i >= lineCount) {
                        // get the remainder using the current word and 
                        // reversing whats left
                        var remainder = [word].concat(words.reverse());

                        // apply ellipsis to the last line
                        nf.CanvasUtils.ellipsis(tspan, remainder.join(' '));

                        // we've reached the line count
                        break;
                    } else {
                        tspan.text(word);

                        // prep the line for the next iteration
                        line = [word];
                    }
                }

                // get the next word
                word = words.pop();
            }
        },
        
        /**
         * Updates the active thread count on the specified selection.
         * 
         * @param {selection} selection         The selection
         * @param {object} d                    The data
         * @param {function} setOffset          Optional function to handle the width of the active thread count component
         * @return 
         */
        activeThreadCount: function (selection, d, setOffset) {
            // if there is active threads show the count, otherwise hide
            if (d.status.aggregateSnapshot.activeThreadCount > 0) {
                // update the active thread count
                var activeThreadCount = selection.select('text.active-thread-count')
                        .text(function () {
                            return d.status.aggregateSnapshot.activeThreadCount;
                        })
                        .style('display', 'block')
                        .each(function () {
                            var bBox = this.getBBox();
                            d3.select(this).attr('x', function () {
                                return d.dimensions.width - bBox.width - 15;
                            });
                        });

                // update the background width
                selection.select('text.active-thread-count-icon')
                        .attr('x', function () {
                            var bBox = activeThreadCount.node().getBBox();

                            // update the offset
                            if (typeof setOffset === 'function') {
                                setOffset(bBox.width + 6);
                            }

                            return d.dimensions.width - bBox.width - 20;
                        })
                        .style('display', 'block');
            } else {
                selection.selectAll('text.active-thread-count, text.active-thread-count-icon').style('display', 'none');
            }
        },
        
        /**
         * Disables the default browser behavior of following image href when control clicking.
         * 
         * @param {selection} selection                 The image
         */
        disableImageHref: function (selection) {
            selection.on('click.disableImageHref', function () {
                if (d3.event.ctrlKey || d3.event.shiftKey) {
                    d3.event.preventDefault();
                }
            });
        },
        
        /**
         * Handles component bulletins.
         * 
         * @param {selection} selection                    The component
         * @param {object} d                                The data
         * @param {function} getTooltipContainer            Function to get the tooltip container
         * @param {function} offset                         Optional offset
         */
        bulletins: function (selection, d, getTooltipContainer, offset) {
            offset = nf.Common.isDefinedAndNotNull(offset) ? offset : 0;

            // remove any existing tip if necessary
            var tip = d3.select('#bulletin-tip-' + d.id);
            if (!tip.empty()) {
                tip.remove();
            }

            // if there are bulletins show them, otherwise hide
            if (!nf.Common.isEmpty(d.bulletins)) {
                // update the tooltip
                selection.select('text.bulletin-icon')
                        .each(function () {
                            // if there are bulletins generate a tooltip
                            tip = getTooltipContainer().append('div')
                                    .attr('id', function () {
                                        return 'bulletin-tip-' + d.id;
                                    })
                                    .attr('class', 'tooltip nifi-tooltip')
                                    .html(function () {
                                        // format the bulletins
                                        var bulletins = nf.Common.getFormattedBulletins(d.bulletins);

                                        // create the unordered list based off the formatted bulletins
                                        var list = nf.Common.formatUnorderedList(bulletins);
                                        if (list === null || list.length === 0) {
                                            return '';
                                        } else {
                                            return $('<div></div>').append(list).html();
                                        }
                                    });

                            // add the tooltip
                            nf.CanvasUtils.canvasTooltip(tip, d3.select(this));
                        });

                // update the tooltip background
                selection.select('text.bulletin-icon').style("visibility", "visible");
                selection.select('rect.bulletin-background').style("visibility", "visible");
            } else {
                // update the tooltip background
                selection.select('text.bulletin-icon').style("visibility", "hidden");
                selection.select('rect.bulletin-background').style("visibility", "hidden");
            }
        },
        
        /**
         * Adds the specified tooltip to the specified target.
         * 
         * @param {selection} tip           The tooltip
         * @param {selection} target        The target of the tooltip
         */
        canvasTooltip: function (tip, target) {
            target.on('mouseenter', function () {
                tip.style('top', (d3.event.pageY + 15) + 'px').style('left', (d3.event.pageX + 15) + 'px').style('display', 'block');
            })
            .on('mousemove', function () {
                tip.style('top', (d3.event.pageY + 15) + 'px').style('left', (d3.event.pageX + 15) + 'px');
            })
            .on('mouseleave', function () {
                tip.style('display', 'none');
            });
        },
        
        /**
         * Determines if the specified selection is colorable (in a single action).
         * 
         * @param {selection} selection     The selection
         * @returns {boolean}
         */
        isColorable: function(selection) {
            if (selection.empty()) {
                return false;
            }

            // require read and write permissions
            if (nf.CanvasUtils.canRead(selection) === false || nf.CanvasUtils.canModify(selection) === false) {
                return false;
            }
            
            // determine if the current selection is entirely processors or labels
            var selectedProcessors = selection.filter(function(d) {
                var processor = d3.select(this);
                return nf.CanvasUtils.isProcessor(processor) && nf.CanvasUtils.canModify(processor);
            });
            var selectedLabels = selection.filter(function(d) {
                var label = d3.select(this);
                return nf.CanvasUtils.isLabel(label) && nf.CanvasUtils.canModify(label);
            });

            var allProcessors = selectedProcessors.size() === selection.size();
            var allLabels = selectedLabels.size() === selection.size();
            
            return allProcessors || allLabels;
        },
        
        /**
         * Determines if the specified selection is a connection.
         *
         * @argument {selection} selection      The selection
         */
        isConnection: function (selection) {
            return selection.classed('connection');
        },
        
        /**
         * Determines if the specified selection is a remote process group.
         *
         * @argument {selection} selection      The selection
         */
        isRemoteProcessGroup: function (selection) {
            return selection.classed('remote-process-group');
        },
        
        /**
         * Determines if the specified selection is a processor.
         *
         * @argument {selection} selection      The selection
         */
        isProcessor: function (selection) {
            return selection.classed('processor');
        },
        
        /**
         * Determines if the specified selection is a label.
         *
         * @argument {selection} selection      The selection
         */
        isLabel: function (selection) {
            return selection.classed('label');
        },
        
        /**
         * Determines if the specified selection is an input port.
         *
         * @argument {selection} selection      The selection
         */
        isInputPort: function (selection) {
            return selection.classed('input-port');
        },
        
        /**
         * Determines if the specified selection is an output port.
         *
         * @argument {selection} selection      The selection
         */
        isOutputPort: function (selection) {
            return selection.classed('output-port');
        },
        
        /**
         * Determines if the specified selection is a process group.
         *
         * @argument {selection} selection      The selection
         */
        isProcessGroup: function (selection) {
            return selection.classed('process-group');
        },
        
        /**
         * Determines if the specified selection is a funnel.
         *
         * @argument {selection} selection      The selection
         */
        isFunnel: function (selection) {
            return selection.classed('funnel');
        },
        
        /**
         * Determines if the components in the specified selection are runnable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}                    Whether the selection is runnable
         */
        areRunnable: function (selection) {
            if (selection.empty()) {
                return false;
            }

            var runnable = true;
            selection.each(function () {
                if (!nf.CanvasUtils.isRunnable(d3.select(this))) {
                    runnable = false;
                    return false;
                }
            });

            return runnable;
        },
        
        /**
         * Determines if the component in the specified selection is runnable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}                    Whether the selection is runnable
         */
        isRunnable: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            if (nf.CanvasUtils.isProcessGroup(selection)) {
                return true;
            }

            if (nf.CanvasUtils.canModify(selection) === false) {
                return false;
            }

            var runnable = false;
            var selectionData = selection.datum();
            if (nf.CanvasUtils.isProcessor(selection) || nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection)) {
                runnable = nf.CanvasUtils.supportsModification(selection) && selectionData.status.aggregateSnapshot.runStatus === 'Stopped';
            }

            return runnable;
        },
        
        /**
         * Determines if the components in the specified selection are stoppable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}                    Whether the selection is stoppable
         */
        areStoppable: function (selection) {
            if (selection.empty()) {
                return false;
            }

            var stoppable = true;
            selection.each(function () {
                if (!nf.CanvasUtils.isStoppable(d3.select(this))) {
                    stoppable = false;
                    return false;
                }
            });

            return stoppable;
        },
        
        /**
         * Determines if the component in the specified selection is runnable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}                    Whether the selection is runnable
         */
        isStoppable: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            if (nf.CanvasUtils.isProcessGroup(selection)) {
                return true;
            }

            if (nf.CanvasUtils.canModify(selection) === false) {
                return false;
            }

            var stoppable = false;
            var selectionData = selection.datum();
            if (nf.CanvasUtils.isProcessor(selection) || nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection)) {
                stoppable = selectionData.status.aggregateSnapshot.runStatus === 'Running';
            }

            return stoppable;
        },

        /**
         * Filters the specified selection for any components that supports enable.
         *
         * @argument {selection} selection      The selection
         */
        filterEnable: function (selection) {
            return selection.filter(function (d) {
                var selected = d3.select(this);
                var selectedData = selected.datum();

                // ensure its a processor, input port, or output port and supports modification and is disabled (can enable)
                return ((nf.CanvasUtils.isProcessor(selected) || nf.CanvasUtils.isInputPort(selected) || nf.CanvasUtils.isOutputPort(selected)) &&
                        nf.CanvasUtils.supportsModification(selected) &&
                        selectedData.status.aggregateSnapshot.runStatus === 'Disabled');
            });
        },

        /**
         * Determines if the specified selection contains any components that supports enable.
         *
         * @argument {selection} selection      The selection
         */
        canEnable: function (selection) {
            if (selection.empty()) {
                return false;
            }

            if (nf.CanvasUtils.canModify(selection) === false) {
                return false;
            }

            return nf.CanvasUtils.filterEnable(selection).size() === selection.size();
        },

        /**
         * Filters the specified selection for any components that supports disable.
         *
         * @argument {selection} selection      The selection
         */
        filterDisable: function (selection) {
            return selection.filter(function (d) {
                var selected = d3.select(this);
                var selectedData = selected.datum();

                // ensure its a processor, input port, or output port and supports modification and is stopped (can disable)
                return ((nf.CanvasUtils.isProcessor(selected) || nf.CanvasUtils.isInputPort(selected) || nf.CanvasUtils.isOutputPort(selected)) &&
                        nf.CanvasUtils.supportsModification(selected) &&
                        selectedData.status.aggregateSnapshot.runStatus === 'Stopped');
            });
        },

        /**
         * Determines if the specified selection contains any components that supports disable.
         *
         * @argument {selection} selection      The selection
         */
        canDisable: function (selection) {
            if (selection.empty()) {
                return false;
            }

            if (nf.CanvasUtils.canModify(selection) === false) {
                return false;
            }

            return nf.CanvasUtils.filterDisable(selection).size() === selection.size();
        },


        /**
         * Determines if the specified selection can all start transmitting.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}                    Whether the selection can start transmitting
         */
        canAllStartTransmitting: function (selection) {
            if (selection.empty()) {
                return false;
            }

            var canStartTransmitting = true;
            selection.each(function () {
                if (!nf.CanvasUtils.canStartTransmitting(d3.select(this))) {
                    canStartTransmitting = false;
                }
            });
            return canStartTransmitting;
        },

        /**
         * Determines if the specified selection supports starting transmission.
         *
         * @argument {selection} selection      The selection
         */
        canStartTransmitting: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            if (nf.CanvasUtils.canModify(selection) === false || nf.CanvasUtils.canRead(selection) === false) {
                return false;
            }

            return nf.CanvasUtils.isRemoteProcessGroup(selection);
        },
        
        /**
         * Determines if the specified selection can all stop transmitting.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}                    Whether the selection can stop transmitting
         */
        canAllStopTransmitting: function (selection) {
            if (selection.empty()) {
                return false;
            }

            var canStopTransmitting = true;
            selection.each(function () {
                if (!nf.CanvasUtils.canStopTransmitting(d3.select(this))) {
                    canStopTransmitting = false;
                }
            });
            return canStopTransmitting;
        },
        
        /**
         * Determines if the specified selection can stop transmission.
         *
         * @argument {selection} selection      The selection
         */
        canStopTransmitting: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            if (nf.CanvasUtils.canModify(selection) === false || nf.CanvasUtils.canRead(selection) === false) {
                return false;
            }

            return nf.CanvasUtils.isRemoteProcessGroup(selection);
        },

        /**
         * Determines whether the components in the specified selection are deletable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}            Whether the selection is deletable
         */
        areDeletable: function (selection) {
            if (selection.empty()) {
                return false;
            }

            var isDeletable = true;
            selection.each(function () {
                if (!nf.CanvasUtils.isDeletable(d3.select(this))) {
                    isDeletable = false;
                }
            });
            return isDeletable;
        },

        /**
         * Determines whether the component in the specified selection is deletable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}            Whether the selection is deletable
         */
        isDeletable: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            if (nf.CanvasUtils.canModify(selection) === false) {
                return false;
            }

            return nf.CanvasUtils.supportsModification(selection);
        },

        /**
         * Determines whether the specified selection is configurable.
         *
         * @param selection
         */
        isConfigurable: function (selection) {
            // ensure the correct number of components are selected
            if (selection.size() !== 1) {
                return false;
            }
            if (nf.CanvasUtils.canRead(selection) === false || nf.CanvasUtils.canModify(selection) === false) {
                return false;
            }
            if (nf.CanvasUtils.isFunnel(selection)) {
                return false;
            }

            return nf.CanvasUtils.supportsModification(selection);
        },

        /**
         * Determines whether the specified selection has details.
         *
         * @param selection
         */
        hasDetails: function (selection) {
            // ensure the correct number of components are selected
            if (selection.size() !== 1) {
                return false;
            }
            if (nf.CanvasUtils.canRead(selection) === false) {
                return false;
            }

            if (nf.CanvasUtils.canModify(selection)) {
                if (nf.CanvasUtils.isProcessor(selection) || nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection) || nf.CanvasUtils.isRemoteProcessGroup(selection) || nf.CanvasUtils.isConnection(selection)) {
                    return !nf.CanvasUtils.isConfigurable(selection);
                }
            } else {
                return nf.CanvasUtils.isProcessor(selection) || nf.CanvasUtils.isConnection(selection) || nf.CanvasUtils.isProcessGroup(selection) || nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection) || nf.CanvasUtils.isRemoteProcessGroup(selection);
            }

            return false;
        },

        /**
         * Determines whether the components in the specified selection are writable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}            Whether the selection is writable
         */
        canModify: function (selection) {
            var selectionSize = selection.size();
            var writableSize = selection.filter(function (d) {
                return d.permissions.canWrite;
            }).size();
            
            return selectionSize === writableSize;
        },

        /**
         * Determines whether the components in the specified selection are readable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}            Whether the selection is readable
         */
        canRead: function (selection) {
            var selectionSize = selection.size();
            var readableSize = selection.filter(function (d) {
                return d.permissions.canRead;
            }).size();

            return selectionSize === readableSize;
        },
        
        /**
         * Determines whether the specified selection is in a state to support modification.
         *
         * @argument {selection} selection      The selection
         */
        supportsModification: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            // get the selection data
            var selectionData = selection.datum();

            var supportsModification = false;
            if (nf.CanvasUtils.isProcessor(selection) || nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection)) {
                supportsModification = !(selectionData.status.aggregateSnapshot.runStatus === 'Running' || selectionData.status.aggregateSnapshot.activeThreadCount > 0);
            } else if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                supportsModification = !(selectionData.status.transmissionStatus === 'Transmitting' || selectionData.status.aggregateSnapshot.activeThreadCount > 0);
            } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                supportsModification = true;
            } else if (nf.CanvasUtils.isFunnel(selection)) {
                supportsModification = true;
            } else if (nf.CanvasUtils.isLabel(selection)) {
                supportsModification = true;
            } else if (nf.CanvasUtils.isConnection(selection)) {
                var isSourceConfigurable = false;
                var isDestinationConfigurable = false;

                var sourceComponentId = nf.CanvasUtils.getConnectionSourceComponentId(selectionData);
                var source = d3.select('#id-' + sourceComponentId);
                if (!source.empty()) {
                    if (nf.CanvasUtils.isRemoteProcessGroup(source) || nf.CanvasUtils.isProcessGroup(source)) {
                        isSourceConfigurable = true;
                    } else {
                        isSourceConfigurable = nf.CanvasUtils.supportsModification(source);
                    }
                }

                var destinationComponentId = nf.CanvasUtils.getConnectionDestinationComponentId(selectionData);
                var destination = d3.select('#id-' + destinationComponentId);
                if (!destination.empty()) {
                    if (nf.CanvasUtils.isRemoteProcessGroup(destination) || nf.CanvasUtils.isProcessGroup(destination)) {
                        isDestinationConfigurable = true;
                    } else {
                        isDestinationConfigurable = nf.CanvasUtils.supportsModification(destination);
                    }
                }

                supportsModification = isSourceConfigurable && isDestinationConfigurable;
            }
            return supportsModification;
        },

        /**
         * Determines the connectable type for the specified source selection.
         *
         * @argument {selection} selection      The selection
         */
        getConnectableTypeForSource: function (selection) {
            var type;
            if (nf.CanvasUtils.isProcessor(selection)) {
                type = 'PROCESSOR';
            } else if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                type = 'REMOTE_OUTPUT_PORT';
            } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                type = 'OUTPUT_PORT';
            } else if (nf.CanvasUtils.isInputPort(selection)) {
                type = 'INPUT_PORT';
            } else if (nf.CanvasUtils.isFunnel(selection)) {
                type = 'FUNNEL';
            }
            return type;
        },
        
        /**
         * Determines the connectable type for the specified destination selection.
         *
         * @argument {selection} selection      The selection
         */
        getConnectableTypeForDestination: function (selection) {
            var type;
            if (nf.CanvasUtils.isProcessor(selection)) {
                type = 'PROCESSOR';
            } else if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                type = 'REMOTE_INPUT_PORT';
            } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                type = 'INPUT_PORT';
            } else if (nf.CanvasUtils.isOutputPort(selection)) {
                type = 'OUTPUT_PORT';
            } else if (nf.CanvasUtils.isFunnel(selection)) {
                type = 'FUNNEL';
            }
            return type;
        },
        
        /**
         * Determines if the graph is currently in a state to copy.
         *
         * @argument {selection} selection    The selection
         */
        isCopyable: function (selection) {
            // if nothing is selected return
            if (selection.empty()) {
                return false;
            }

            if (nf.CanvasUtils.canRead(selection) === false) {
                return false;
            }

            // determine how many copyable components are selected
            var copyable = selection.filter(function (d) {
                var selected = d3.select(this);
                if (nf.CanvasUtils.isConnection(selected)) {
                    var sourceIncluded = !selection.filter(function (source) {
                        var sourceComponentId = nf.CanvasUtils.getConnectionSourceComponentId(d);
                        return sourceComponentId === source.id;
                    }).empty();
                    var destinationIncluded = !selection.filter(function (destination) {
                        var destinationComponentId = nf.CanvasUtils.getConnectionDestinationComponentId(d);
                        return destinationComponentId === destination.id;
                    }).empty();
                    return sourceIncluded && destinationIncluded;
                } else {
                    return nf.CanvasUtils.isProcessor(selected) || nf.CanvasUtils.isFunnel(selected) || nf.CanvasUtils.isLabel(selected) || nf.CanvasUtils.isProcessGroup(selected) || nf.CanvasUtils.isRemoteProcessGroup(selected) || nf.CanvasUtils.isInputPort(selected) || nf.CanvasUtils.isOutputPort(selected);
                }
            });

            // ensure everything selected is copyable
            return selection.size() === copyable.size();
        },
        
        /**
         * Determines if something is currently pastable.
         */
        isPastable: function () {
            return nf.Canvas.canWrite() && nf.Clipboard.isCopied();
        },
        
        /**
         * Persists the current user view.
         */
        persistUserView: function () {
            var name = config.storage.namePrefix + nf.Canvas.getGroupId();

            // create the item to store
            var translate = nf.Canvas.View.translate();
            var item = {
                scale: nf.Canvas.View.scale(),
                translateX: translate[0],
                translateY: translate[1]
            };

            // store the item
            nf.Storage.setItem(name, item);
        },
        
        /**
         * Gets the name for this connection.
         * 
         * @param {object} connection
         */
        formatConnectionName: function (connection) {
            if (!nf.Common.isBlank(connection.name)) {
                return connection.name;
            } else if (nf.Common.isDefinedAndNotNull(connection.selectedRelationships)) {
                return connection.selectedRelationships.join(', ');
            }
            return '';
        },
        
        /**
         * Reloads a connection's source and destination.
         * 
         * @param {string} sourceComponentId          The connection source id
         * @param {string} destinationComponentId     The connection destination id
         */
        reloadConnectionSourceAndDestination: function (sourceComponentId, destinationComponentId) {
            if (nf.Common.isBlank(sourceComponentId) === false) {
                var source = d3.select('#id-' + sourceComponentId);
                if (source.empty() === false) {
                    var sourceData = source.datum();

                    if (sourceData.permissions.canRead) {
                        // update the source status if necessary
                        if (nf.CanvasUtils.isProcessor(source)) {
                            nf.Processor.reload(sourceData.component);
                        } else if (nf.CanvasUtils.isInputPort(source)) {
                            nf.Port.reload(sourceData.component);
                        } else if (nf.CanvasUtils.isRemoteProcessGroup(source)) {
                            nf.RemoteProcessGroup.reload(sourceData.component);
                        }
                    }
                }
            }

            if (nf.Common.isBlank(destinationComponentId) === false) {
                var destination = d3.select('#id-' + destinationComponentId);
                if (destination.empty() === false) {
                    var destinationData = destination.datum();

                    if (destinationData.permissions.canRead) {
                        // update the destination component accordingly
                        if (nf.CanvasUtils.isProcessor(destination)) {
                            nf.Processor.reload(destinationData.component);
                        } else if (nf.CanvasUtils.isRemoteProcessGroup(destination)) {
                            nf.RemoteProcessGroup.reload(destinationData.component);
                        }
                    }
                }
            }
        },
        
        /**
         * Returns the component id of the source of this processor. If the connection is attached
         * to a port in a [sub|remote] group, the component id will be that of the group. Otherwise
         * it is the component itself.
         * 
         * @param {object} connection   The connection in question
         */
        getConnectionSourceComponentId: function (connection) {
            var sourceId = connection.sourceId;
            if (connection.sourceGroupId !== nf.Canvas.getGroupId()) {
                sourceId = connection.sourceGroupId;
            }
            return sourceId;
        },
        
        /**
         * Returns the component id of the source of this processor. If the connection is attached
         * to a port in a [sub|remote] group, the component id will be that of the group. Otherwise
         * it is the component itself.
         * 
         * @param {object} connection   The connection in question
         */
        getConnectionDestinationComponentId: function (connection) {
            var destinationId = connection.destinationId;
            if (connection.destinationGroupId !== nf.Canvas.getGroupId()) {
                destinationId = connection.destinationGroupId;
            }
            return destinationId;
        },
        
        /**
         * Attempts to restore a persisted view. Returns a flag that indicates if the
         * view was restored.
         */
        restoreUserView: function () {
            var viewRestored = false;

            try {
                // see if we can restore the view position from storage
                var name = config.storage.namePrefix + nf.Canvas.getGroupId();
                var item = nf.Storage.getItem(name);

                // ensure the item is valid
                if (nf.Common.isDefinedAndNotNull(item)) {
                    if (isFinite(item.scale) && isFinite(item.translateX) && isFinite(item.translateY)) {
                        // restore previous view
                        nf.Canvas.View.translate([item.translateX, item.translateY]);
                        nf.Canvas.View.scale(item.scale);

                        // refresh the canvas
                        nf.Canvas.View.refresh({
                            transition: true
                        });

                        // mark the view was restore
                        viewRestored = true;
                    }
                }
            } catch (e) {
                // likely could not parse item.. ignoring
            }

            return viewRestored;
        },
        
        /**
         * Enters the specified group.
         *
         * @param {string} groupId
         */
        enterGroup: function (groupId) {
            // set the new group id
            nf.Canvas.setGroupId(groupId);

            // reload the graph
            return nf.Canvas.reload().done(function () {
                // attempt to restore the view
                var viewRestored = nf.CanvasUtils.restoreUserView();

                // if the view was not restore attempt to fit
                if (viewRestored === false) {
                    nf.Canvas.View.fit();

                    // refresh the canvas
                    nf.Canvas.View.refresh({
                        transition: true
                    });
                }
            }).fail(function () {
                nf.Dialog.showOkDialog({
                    headerText: 'Process Group',
                    dialogContent: 'Unable to enter the selected group.'
                });
            });
        },
        
        /**
         * Gets the origin of the bounding box for the specified selection.
         *
         * @argument {selection} selection      The selection
         */
        getOrigin: function (selection) {
            var origin = {};

            selection.each(function (d) {
                var selected = d3.select(this);
                if (!nf.CanvasUtils.isConnection(selected)) {
                    if (nf.Common.isUndefined(origin.x) || d.position.x < origin.x) {
                        origin.x = d.position.x;
                    }
                    if (nf.Common.isUndefined(origin.y) || d.position.y < origin.y) {
                        origin.y = d.position.y;
                    }
                }
            });

            return origin;
        },
        
        /**
         * Moves the specified components into the current parent group.
         * 
         * @param {selection} components
         */
        moveComponentsToParent: function (components) {
            var groupId = nf.Canvas.getParentGroupId();
            
            // if the group id is null, we're already in the top most group
            if (groupId === null) {
                nf.Dialog.showOkDialog({
                    headerText: 'Process Group',
                    dialogContent: 'Components are already in the topmost group.'
                });
            } else {
                moveComponents(components, groupId);
            }
        },
        
        /**
         * Moves the specified components into the specified group.
         * 
         * @param {selection} components    The components to move
         * @param {selection} group         The destination group
         */
        moveComponents: function (components, group) {
            var groupData = group.datum();
            
            // move the components into the destination and...
            moveComponents(components, groupData.id).done(function () {
                // reload the target group
                nf.ProcessGroup.reload(groupData.component);
            });
        },
        
        /**
         * Removes any dangling edges. All components are retained as well as any
         * edges whose source and destination are also retained.
         *
         * @param {selection} selection
         * @returns {array}
         */
        trimDanglingEdges: function (selection) {
            // returns whether the source and destination of the specified connection are present in the specified selection
            var keepConnection = function (connection) {
                var sourceComponentId = nf.CanvasUtils.getConnectionSourceComponentId(connection);
                var destinationComponentId = nf.CanvasUtils.getConnectionDestinationComponentId(connection);

                // determine if both source and destination are selected
                var includesSource = false;
                var includesDestination = false;
                selection.each(function (d) {
                    if (d.id === sourceComponentId) {
                        includesSource = true;
                    }
                    if (d.id === destinationComponentId) {
                        includesDestination = true;
                    }
                });

                return includesSource && includesDestination;
            };

            // include all components and connections whose source/destination are also selected
            return selection.filter(function (d) {
                if (d.type === 'Connection') {
                    return keepConnection(d);
                } else {
                    return true;
                }
            });
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
                $.each(nf.Connection.getComponentConnections(d.id), function (_, connection) {
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
                        isDisconnected = components.has(nf.CanvasUtils.getConnectionSourceComponentId(connection)) &&
                                components.has(nf.CanvasUtils.getConnectionDestinationComponentId(connection));
                    }
                });
            }

            return isDisconnected;
        },
        
        /**
         * Ensures components are eligible to be moved. The new group can be optionally specified.
         *
         * 1) Ensuring that the input and output ports are not connected outside of this group
         * 2) If the target is specified; ensuring there are no port name conflicts in the target group
         *
         * @argument {selection} selection      The selection being moved
         * @argument {string} groupId           The id of the new group
         */
        eligibleForMove: function (selection, groupId) {
            var inputPorts = [];
            var outputPorts = [];

            // separate out the component type accordingly
            selection.each(function (d) {
                var selected = d3.select(this);
                if (nf.CanvasUtils.isInputPort(selected)) {
                    inputPorts.push(selected.datum());
                } else if (nf.CanvasUtils.isOutputPort(selected)) {
                    outputPorts.push(selected.datum());
                }
            });

            return $.Deferred(function (deferred) {
                if (inputPorts.length > 0 || outputPorts.length > 0) {
                    // create a deferred for checking input port connection status
                    var portConnectionCheck = function () {
                        return $.Deferred(function (portConnectionDeferred) {
                            // ports in the root group cannot be moved
                            if (nf.Canvas.getParentGroupId() === null) {
                                nf.Dialog.showOkDialog({
                                    headerText: 'Port',
                                    dialogContent: 'Cannot move Ports out of the root group'
                                });
                                portConnectionDeferred.reject();
                            } else {
                                $.ajax({
                                    type: 'GET',
                                    url: config.urls.controller + '/process-groups/' + encodeURIComponent(nf.Canvas.getParentGroupId()) + '/connections',
                                    dataType: 'json'
                                }).done(function (response) {
                                    var connections = response.connections;
                                    var conflictingPorts = [];

                                    if (!nf.Common.isEmpty(connections)) {
                                        // check the input ports
                                        $.each(inputPorts, function (i, inputPort) {
                                            $.each(connections, function (j, connection) {
                                                if (inputPort.id === connection.destination.id) {
                                                    conflictingPorts.push(nf.Common.escapeHtml(inputPort.component.name));
                                                }
                                            });
                                        });

                                        // check the output ports
                                        $.each(outputPorts, function (i, outputPort) {
                                            $.each(connections, function (j, connection) {
                                                if (outputPort.id === connection.source.id) {
                                                    conflictingPorts.push(nf.Common.escapeHtml(outputPort.component.name));
                                                }
                                            });
                                        });
                                    }

                                    // inform the user of the conflicting ports
                                    if (conflictingPorts.length > 0) {
                                        nf.Dialog.showOkDialog({
                                            headerText: 'Port',
                                            dialogContent: 'The following ports are currently connected outside of this group: <b>' + conflictingPorts.join('</b>, <b>') + '</b>'
                                        });
                                        portConnectionDeferred.reject();
                                    } else {
                                        portConnectionDeferred.resolve();
                                    }

                                }).fail(function () {
                                    portConnectionDeferred.reject();
                                });
                            }
                        }).promise();
                    };

                    // create a deferred for checking port names in the target
                    var portNameCheck = function () {
                        return $.Deferred(function (portNameDeferred) {

                            // add the get request
                            $.ajax({
                                type: 'GET',
                                url: config.urls.controller + '/process-groups/' + encodeURIComponent(groupId),
                                data: {
                                    verbose: true
                                },
                                dataType: 'json'
                            }).done(function (response) {
                                var processGroup = response.component;
                                var processGroupContents = processGroup.contents;

                                var conflictingPorts = [];
                                var getConflictingPorts = function (selectedPorts, ports) {
                                    if (selectedPorts.length > 0 && !nf.Common.isEmpty(ports)) {
                                        $.each(selectedPorts, function (i, selectedPort) {
                                            $.each(ports, function (j, port) {
                                                if (selectedPort.component.name === port.name) {
                                                    conflictingPorts.push(nf.Common.escapeHtml(port.name));
                                                }
                                            });
                                        });
                                    }
                                };

                                // check for conflicting ports
                                getConflictingPorts(inputPorts, processGroupContents.inputPorts);
                                getConflictingPorts(outputPorts, processGroupContents.outputPorts);

                                // inform the user of the conflicting ports
                                if (conflictingPorts.length > 0) {
                                    nf.Dialog.showOkDialog({
                                        headerText: 'Port',
                                        dialogContent: 'The following ports already exist in the target process group: <b>' + conflictingPorts.join('</b>, <b>') + '</b>'
                                    });
                                    portNameDeferred.reject();
                                } else {
                                    portNameDeferred.resolve();
                                }
                            }).fail(function () {
                                portNameDeferred.reject();
                            });
                        }).promise();
                    };

                    // execute the checks in order
                    portConnectionCheck().done(function () {
                        if (nf.Common.isDefinedAndNotNull(groupId)) {
                            $.when(portNameCheck()).done(function () {
                                deferred.resolve();
                            }).fail(function () {
                                deferred.reject();
                            });
                        } else {
                            deferred.resolve();
                        }
                    }).fail(function () {
                        deferred.reject();
                    });
                } else {
                    deferred.resolve();
                }
            }).promise();
        },
        
        /**
         * Determines if the component in the specified selection is a valid connection source.
         * 
         * @param {selection} selection         The selection
         * @return {boolean} Whether the selection is a valid connection source
         */
        isValidConnectionSource: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            // always allow connections from process groups
            if (nf.CanvasUtils.isProcessGroup(selection)) {
                return true;
            }

            // require read and write for a connection source since we'll need to read the source to obtain valid relationships, etc
            if (nf.CanvasUtils.canRead(selection) === false || nf.CanvasUtils.canModify(selection) === false) {
                return false;
            }

            return nf.CanvasUtils.isProcessor(selection) || nf.CanvasUtils.isRemoteProcessGroup(selection) ||
                nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isFunnel(selection);
        },
        
        /**
         * Determines if the component in the specified selection is a valid connection destination.
         * 
         * @param {selection} selection         The selection
         * @return {boolean} Whether the selection is a valid connection destination
         */
        isValidConnectionDestination: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            if (nf.CanvasUtils.isProcessGroup(selection)) {
                return true;
            }

            // require write for a connection destination
            if (nf.CanvasUtils.canModify(selection) === false) {
                return false;
            }

            if (nf.CanvasUtils.isRemoteProcessGroup(selection) || nf.CanvasUtils.isOutputPort(selection) || nf.CanvasUtils.isFunnel(selection)) {
                return true;
            }

            // if processor, ensure it supports input
            if (nf.CanvasUtils.isProcessor(selection)) {
                var destinationData = selection.datum();
                return destinationData.inputRequirement !== 'INPUT_FORBIDDEN';
            }
        }
    };
}());