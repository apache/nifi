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
                    my: 'top left',
                    adjust: {
                        method: 'flipinvert flipinvert'
                    }
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
                        nf.Canvas.setGroupId(groupId);
                        nf.Canvas.reload().done(function () {
                            deferred.resolve();
                        }).fail(function () {
                            nf.Dialog.showOkDialog({
                                dialogContent: 'Unable to load the group for the specified component.',
                                overlayBackground: false
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
                            dialogContent: 'Unable to find the specified component.',
                            overlayBackground: false
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
         * Position the component accordingly.
         * 
         * @param {selection} updated
         */
        position: function (updated) {
            if (updated.empty()) {
                return;
            }

            // update the processors positioning
            updated.attr('transform', function (d) {
                return 'translate(' + d.component.position.x + ', ' + d.component.position.y + ')';
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
            if (node.getSubStringLength(0, text.length - 1) > width) {
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
                        // restore the current word
                        var remainder = [word].concat(words);

                        // apply ellipsis to the last line
                        nf.CanvasUtils.ellipsis(tspan, remainder.join(' '));

                        // we've reached the line count
                        break;
                    } else {
                        tspan.text(word);

                        // other prep the line for the next iteration
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
            if (nf.Common.isDefinedAndNotNull(d.status) && d.status.activeThreadCount > 0) {
                // update the active thread count
                var activeThreadCount = selection.select('text.active-thread-count')
                        .text(function () {
                            return d.status.activeThreadCount;
                        })
                        .style('display', 'block')
                        .each(function () {
                            var bBox = this.getBBox();
                            d3.select(this).attr('x', function () {
                                return d.dimensions.width - bBox.width - 4;
                            });
                        });

                // update the background width
                selection.select('rect.active-thread-count-background')
                        .attr('width', function () {
                            var bBox = activeThreadCount.node().getBBox();
                            return bBox.width + 8;
                        })
                        .attr('x', function () {
                            var bBox = activeThreadCount.node().getBBox();

                            // update the offset
                            if (typeof setOffset === 'function') {
                                setOffset(bBox.width + 6);
                            }

                            return d.dimensions.width - bBox.width - 8;
                        })
                        .attr('stroke-dasharray', function() {
                            var rect = d3.select(this);
                            var width = parseFloat(rect.attr('width'));
                            var height = parseFloat(rect.attr('height'));
                            
                            var dashArray = [];
                            dashArray.push(0);
                            dashArray.push(width + height);
                            dashArray.push(width + height);
                            return dashArray.join(' ');
                        })
                        .style('display', 'block');
            } else {
                selection.selectAll('text.active-thread-count, rect.active-thread-count-background').style('display', 'none');
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
            var tip = d3.select('#bulletin-tip-' + d.component.id);
            if (!tip.empty()) {
                tip.remove();
            }

            // if there are bulletins show them, otherwise hide
            if (nf.Common.isDefinedAndNotNull(d.status) && !nf.Common.isEmpty(d.status.bulletins)) {
                // update the tooltip
                selection.select('image.bulletin-icon')
                        .style('display', 'block')
                        .each(function () {
                            var bBox = this.getBBox();
                            var bulletinIcon = d3.select(this);

                            bulletinIcon.attr('x', function () {
                                return d.dimensions.width - offset - bBox.width - 4;
                            });

                            // if there are bulletins generate a tooltip
                            tip = getTooltipContainer().append('div')
                                    .attr('id', function () {
                                        return 'bulletin-tip-' + d.component.id;
                                    })
                                    .attr('class', 'tooltip nifi-tooltip')
                                    .html(function () {
                                        // format the bulletins
                                        var bulletins = nf.Common.getFormattedBulletins(d.status.bulletins);

                                        // create the unordered list based off the formatted bulletins
                                        var list = nf.Common.formatUnorderedList(bulletins);
                                        if (list === null || list.length === 0) {
                                            return '';
                                        } else {
                                            return $('<div></div>').append(list).html();
                                        }
                                    });

                            // add the tooltip
                            nf.CanvasUtils.canvasTooltip(tip, bulletinIcon);
                        });
            } else {
                selection.selectAll('image.bulletin-icon').style('display', 'none');
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
            
            // determine if the current selection is entirely processors or labels
            var selectedProcessors = selection.filter(function(d) {
                return nf.CanvasUtils.isProcessor(d3.select(this));
            });
            var selectedLabels = selection.filter(function(d) {
                return nf.CanvasUtils.isLabel(d3.select(this));
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
            var runnable = false;

            var selectionData = selection.datum();
            if (nf.CanvasUtils.isProcessor(selection)) {
                runnable = nf.CanvasUtils.supportsModification(selection) && nf.Common.isEmpty(selectionData.component.validationErrors);
            } else if (nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection)) {
                runnable = nf.CanvasUtils.supportsModification(selection);
            } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                runnable = true;
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
            var stoppable = false;

            var selectionData = selection.datum();
            if (nf.CanvasUtils.isProcessor(selection) || nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection)) {
                stoppable = selectionData.component.state === 'RUNNING';
            } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                stoppable = true;
            }

            return stoppable;
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
                    return false;
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
                    return false;
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
            return nf.CanvasUtils.isRemoteProcessGroup(selection);
        },
        
        /**
         * Determines whether the components in the specified selection are deletable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}            Whether the selection is deletable
         */
        isDeletable: function (selection) {
            if (selection.empty()) {
                return false;
            }

            return nf.CanvasUtils.supportsModification(selection);
        },
        
        /**
         * Determines whether the specified selection is in a state to support modification.
         *
         * @argument {selection} selection      The selection
         */
        supportsModification: function (selection) {
            var selectionData = selection.datum();

            var supportsModification = false;
            if (nf.CanvasUtils.isProcessor(selection) || nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection)) {
                if (nf.Common.isDefinedAndNotNull(selectionData.status)) {
                    supportsModification = !(selectionData.component.state === 'RUNNING' || nf.Common.isDefinedAndNotNull(selectionData.status.activeThreadCount) && selectionData.status.activeThreadCount > 0);
                } else {
                    supportsModification = selectionData.component.state !== 'RUNNING';
                }
            } else if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                if (nf.Common.isDefinedAndNotNull(selectionData.status)) {
                    supportsModification = !(selectionData.component.transmitting === true || nf.Common.isDefinedAndNotNull(selectionData.status.activeThreadCount) && selectionData.status.activeThreadCount > 0);
                } else {
                    supportsModification = selectionData.component.transmitting !== true;
                }
            } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                supportsModification = true;
            } else if (nf.CanvasUtils.isFunnel(selection)) {
                supportsModification = true;
            } else if (nf.CanvasUtils.isLabel(selection)) {
                supportsModification = true;
            } else if (nf.CanvasUtils.isConnection(selection)) {
                var isSourceConfigurable = false;
                var isDestinationConfigurable = false;

                var sourceComponentId = nf.CanvasUtils.getConnectionSourceComponentId(selectionData.component);
                var source = d3.select('#id-' + sourceComponentId);
                if (!source.empty()) {
                    if (nf.CanvasUtils.isRemoteProcessGroup(source) || nf.CanvasUtils.isProcessGroup(source)) {
                        isSourceConfigurable = true;
                    } else {
                        isSourceConfigurable = nf.CanvasUtils.supportsModification(source);
                    }
                }

                var destinationComponentId = nf.CanvasUtils.getConnectionDestinationComponentId(selectionData.component);
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

            // determine how many copyable componets are selected
            var copyable = selection.filter(function (d) {
                var selected = d3.select(this);
                if (nf.CanvasUtils.isConnection(selected)) {
                    var sourceIncluded = !selection.filter(function (source) {
                        var sourceComponentId = nf.CanvasUtils.getConnectionSourceComponentId(d.component);
                        return sourceComponentId === source.component.id;
                    }).empty();
                    var destinationIncluded = !selection.filter(function (destination) {
                        var destinationComponentId = nf.CanvasUtils.getConnectionDestinationComponentId(d.component);
                        return destinationComponentId === destination.component.id;
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
            return nf.Clipboard.isCopied();
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
         * Returns the component id of the source of this processor. If the connection is attached
         * to a port in a [sub|remote] group, the component id will be that of the group. Otherwise
         * it is the component itself.
         * 
         * @param {object} connection   The connection in question
         */
        getConnectionSourceComponentId: function (connection) {
            var sourceId = connection.source.id;
            if (connection.source.groupId !== nf.Canvas.getGroupId()) {
                sourceId = connection.source.groupId;
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
            var destinationId = connection.destination.id;
            if (connection.destination.groupId !== nf.Canvas.getGroupId()) {
                destinationId = connection.destination.groupId;
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

            // clear the current components
            nf.Graph.removeAll();

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
                    dialogContent: 'Unable to enter the selected group.',
                    overlayBackground: false
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
                    if (nf.Common.isUndefined(origin.x) || d.component.position.x < origin.x) {
                        origin.x = d.component.position.x;
                    }
                    if (nf.Common.isUndefined(origin.y) || d.component.position.y < origin.y) {
                        origin.y = d.component.position.y;
                    }
                }
            });

            return origin;
        },
        
        /**
         * Moves the specified components into the specified group.
         * 
         * @param {selection} components    The components to move
         * @param {selection} group         The destination group
         */
        moveComponents: function (components, group) {
            var groupData = group.datum();

            // ensure the current selection is eligible for move into the specified group
            nf.CanvasUtils.eligibleForMove(components, group).done(function () {
                // create a snippet for the specified components and link to the data flow
                var snippetDetails = nf.Snippet.marshal(components, true);
                nf.Snippet.create(snippetDetails).done(function (response) {
                    var snippet = response.snippet;

                    // move the snippet into the target
                    nf.Snippet.move(snippet.id, groupData.component.id).done(function () {
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
                            addComponent(d.type, d.component.id);
                        });

                        // refresh all component types as necessary (handle components that have been removed)
                        componentMap.forEach(function (type, ids) {
                            nf[type].remove(ids);
                        });

                        // reload the target group
                        nf.ProcessGroup.reload(groupData.component);
                    }).fail(nf.Common.handleAjaxError).always(function () {
                        // unable to acutally move the components so attempt to
                        // unlink and remove just the snippet
                        nf.Snippet.unlink(snippet.id).done(function () {
                            nf.Snippet.remove(snippet.id);
                        });
                    });
                }).fail(nf.Common.handleAjaxError);
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
                    if (d.component.id === sourceComponentId) {
                        includesSource = true;
                    }
                    if (d.component.id === destinationComponentId) {
                        includesDestination = true;
                    }
                });

                return includesSource && includesDestination;
            };

            // include all components and connections whose source/destination are also selected
            return selection.filter(function (d) {
                if (d.type === 'Connection') {
                    return keepConnection(d.component);
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
            var connections = d3.map();
            var components = d3.map();
            var isDisconnected = true;

            // include connections 
            selection.filter(function (d) {
                return d.type === 'Connection';
            }).each(function (d) {
                connections.set(d.component.id, d.component);
            });

            // include components and ensure their connections are included
            selection.filter(function (d) {
                return d.type !== 'Connection';
            }).each(function (d) {
                components.set(d.component.id, d.component);

                // check all connections of this component
                $.each(nf.Connection.getComponentConnections(d.component.id), function (_, connection) {
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
         * Ensures components are eligible to be moved. The new target can be optionally specified.
         *
         * 1) Ensuring that the input and output ports are not connected outside of this group
         * 2) If the target is specified; ensuring there are no port name conflicts in the target group
         *
         * @argument {selection} selection      The selection being moved
         * @argument {selection} group          The selection containing the new group
         */
        eligibleForMove: function (selection, group) {
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
                                    dialogContent: 'Ports in the root group cannot be moved into another group.',
                                    overlayBackground: false
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
                                                if (inputPort.component.id === connection.destination.id) {
                                                    conflictingPorts.push(nf.Common.escapeHtml(inputPort.component.name));
                                                }
                                            });
                                        });

                                        // check the output ports
                                        $.each(outputPorts, function (i, outputPort) {
                                            $.each(connections, function (j, connection) {
                                                if (outputPort.component.id === connection.source.id) {
                                                    conflictingPorts.push(nf.Common.escapeHtml(outputPort.component.name));
                                                }
                                            });
                                        });
                                    }

                                    // inform the user of the conflicting ports
                                    if (conflictingPorts.length > 0) {
                                        nf.Dialog.showOkDialog({
                                            dialogContent: 'The following ports are currently connected outside of this group: <b>' + conflictingPorts.join('</b>, <b>') + '</b>',
                                            overlayBackground: false
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
                            var groupData = group.datum();

                            // add the get request
                            $.ajax({
                                type: 'GET',
                                url: config.urls.controller + '/process-groups/' + encodeURIComponent(groupData.component.id),
                                data: {
                                    verbose: true
                                },
                                dataType: 'json'
                            }).done(function (response) {
                                var processGroup = response.processGroup;
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
                                        dialogContent: 'The following ports already exist in the target process group: <b>' + conflictingPorts.join('</b>, <b>') + '</b>',
                                        overlayBackground: false
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
                        if (nf.Common.isDefinedAndNotNull(group)) {
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

            return nf.CanvasUtils.isProcessor(selection) || nf.CanvasUtils.isProcessGroup(selection) ||
                    nf.CanvasUtils.isRemoteProcessGroup(selection) || nf.CanvasUtils.isInputPort(selection) ||
                    nf.CanvasUtils.isFunnel(selection);
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

            return nf.CanvasUtils.isProcessor(selection) || nf.CanvasUtils.isProcessGroup(selection) ||
                    nf.CanvasUtils.isRemoteProcessGroup(selection) || nf.CanvasUtils.isOutputPort(selection) ||
                    nf.CanvasUtils.isFunnel(selection);
        }
    };
}());