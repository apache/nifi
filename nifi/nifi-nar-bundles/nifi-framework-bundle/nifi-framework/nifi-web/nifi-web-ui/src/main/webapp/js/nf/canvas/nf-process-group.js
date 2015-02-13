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
nf.ProcessGroup = (function () {

    var PREVIEW_NAME_LENGTH = 30;

    var dimensions = {
        width: 365,
        height: 142
    };

    // ----------------------------
    // process groups currently on the graph
    // ----------------------------

    var processGroupMap;

    // --------------------
    // component containers
    // --------------------

    var processGroupContainer;

    // --------------------------
    // privately scoped functions
    // --------------------------

    /**
     * Gets the process group comments.
     * 
     * @param {object} d    
     */
    var getProcessGroupComments = function (d) {
        if (nf.Common.isBlank(d.component.comments)) {
            return 'No comments specified';
        } else {
            return d.component.comments;
        }
    };

    /**
     * Selects the process group elements against the current process group map.
     */
    var select = function () {
        return processGroupContainer.selectAll('g.process-group').data(processGroupMap.values(), function (d) {
            return d.component.id;
        });
    };

    /**
     * Renders the process groups in the specified selection.
     *
     * @param {selection} entered           The selection of process groups to be rendered
     * @param {boolean} selected            Whether the process group should be selected
     */
    var renderProcessGroups = function (entered, selected) {
        if (entered.empty()) {
            return;
        }

        var processGroup = entered.append('g')
                .attr({
                    'id': function (d) {
                        return 'id-' + d.component.id;
                    },
                    'class': 'process-group component'
                })
                .classed('selected', selected)
                .call(nf.CanvasUtils.position);

        // ----
        // body
        // ----

        // process group border
        processGroup.append('rect')
                .attr({
                    'rx': 6,
                    'ry': 6,
                    'class': 'border',
                    'width': function (d) {
                        return d.dimensions.width;
                    },
                    'height': function (d) {
                        return d.dimensions.height;
                    },
                    'fill': 'transparent',
                    'stroke-opacity': 0.8,
                    'stroke-width': 2,
                    'stroke': '#294c58'
                });

        // process group body
        processGroup.append('rect')
                .attr({
                    'rx': 6,
                    'ry': 6,
                    'class': 'body',
                    'width': function (d) {
                        return d.dimensions.width;
                    },
                    'height': function (d) {
                        return d.dimensions.height;
                    },
                    'fill': '#294c58',
                    'fill-opacity': 0.8,
                    'stroke-width': 0
                });

        // process group name
        processGroup.append('text')
                .attr({
                    'x': 10,
                    'y': 15,
                    'width': 316,
                    'height': 16,
                    'font-size': '10pt',
                    'font-weight': 'bold',
                    'fill': '#ffffff',
                    'class': 'process-group-name'
                });

        // process group preview
        processGroup.append('image')
                .call(nf.CanvasUtils.disableImageHref)
                .attr({
                    'xlink:href': 'images/bgProcessGroupDetailsArea.png',
                    'width': 352,
                    'height': 113,
                    'x': 6,
                    'y': 22,
                    'class': 'process-group-preview'
                });

        // always support selecting and navigation
        processGroup.on('dblclick', function (d) {
                    // enter this group on double click
                    nf.CanvasUtils.enterGroup(d.component.id);
                })
                .call(nf.Selectable.activate).call(nf.ContextMenu.activate);

        // only support dragging, connection, and drag and drop if appropriate
        if (nf.Common.isDFM()) {
            processGroup
                    // Using mouseover/out to workaround chrome issue #122746
                    .on('mouseover.drop', function (d) {
                        // get the target and ensure its not already been marked for drop
                        var target = d3.select(this);
                        if (!target.classed('drop')) {
                            var targetData = target.datum();
                            
                            // see if there is a selection being dragged
                            var drag = d3.select('rect.drag-selection');
                            if (!drag.empty()) {
                                // filter the current selection by this group
                                var selection = nf.CanvasUtils.getSelection().filter(function(d) {
                                    return targetData.component.id === d.component.id;
                                });
                                
                                // ensure this group isn't in the selection
                                if (selection.empty()) {
                                    // mark that we are hovering over a drop area if appropriate 
                                    target.classed('drop', function () {
                                        // get the current selection and ensure its disconnected
                                        return nf.CanvasUtils.isDisconnected(nf.CanvasUtils.getSelection());
                                    });
                                }
                            }
                        }
                    })
                    .on('mouseout.drop', function (d) {
                        // mark that we are no longer hovering over a drop area unconditionally
                        d3.select(this).classed('drop', false);
                    })
                    .call(nf.Draggable.activate)
                    .call(nf.Connectable.activate);
        }

        // call update to trigger some rendering
        processGroup.call(updateProcessGroups);
    };

    // attempt of space between component count and icon for process group contents
    var CONTENTS_SPACER = 5;

    /**
     * Updates the process groups in the specified selection.
     *
     * @param {selection} updated               The process groups to be updated
     */
    var updateProcessGroups = function (updated) {
        if (updated.empty()) {
            return;
        }

        updated.each(function () {
            var processGroup = d3.select(this);
            var details = processGroup.select('g.process-group-details');

            // if this processor is visible, render everything
            if (processGroup.classed('visible')) {
                if (details.empty()) {
                    details = processGroup.append('g').attr('class', 'process-group-details');

                    // ----------------
                    // stats background
                    // ----------------

                    details.append('rect')
                            .attr({
                                'x': 6,
                                'y': 22,
                                'width': 352,
                                'height': 113,
                                'stroke-width': 1,
                                'stroke': '#6f97ac',
                                'fill': '#ffffff'
                            });

                    details.append('rect')
                            .attr({
                                'x': 6,
                                'y': 22,
                                'width': 352,
                                'height': 22,
                                'stroke-width': 1,
                                'stroke': '#6f97ac',
                                'fill': 'url(#process-group-stats-background)',
                                'class': 'process-group-contents-container'
                            });

                    details.append('rect')
                            .attr({
                                'x': 6,
                                'y': 104,
                                'width': 352,
                                'height': 33,
                                'stroke-width': 1,
                                'stroke': '#6f97ac',
                                'fill': 'url(#process-group-stats-background)'
                            });

                    // --------
                    // contents
                    // --------

                    // input ports icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'xlink:href': 'images/iconInputPortSmall.png',
                                'width': 16,
                                'height': 16,
                                'x': 10,
                                'y': 25
                            });

                    // input ports count
                    details.append('text')
                            .attr({
                                'x': 29,
                                'y': 37,
                                'class': 'process-group-input-port-count process-group-contents-count'
                            });

                    // output ports icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'xlink:href': 'images/iconOutputPortSmall.png',
                                'width': 16,
                                'height': 16,
                                'y': 25,
                                'class': 'process-group-output-port'
                            });

                    // output ports count
                    details.append('text')
                            .attr({
                                'y': 37,
                                'class': 'process-group-output-port-count process-group-contents-count'
                            });

                    // transmitting icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'xlink:href': 'images/iconTransmissionActive.png',
                                'width': 16,
                                'height': 16,
                                'y': 25,
                                'class': 'process-group-transmitting'
                            });

                    // transmitting count
                    details.append('text')
                            .attr({
                                'y': 37,
                                'class': 'process-group-transmitting-count process-group-contents-count'
                            });

                    // not transmitting icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'xlink:href': 'images/iconTransmissionInactive.png',
                                'width': 16,
                                'height': 16,
                                'y': 25,
                                'class': 'process-group-not-transmitting'
                            });

                    // not transmitting count
                    details.append('text')
                            .attr({
                                'y': 37,
                                'class': 'process-group-not-transmitting-count process-group-contents-count'
                            });

                    // running icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'xlink:href': 'images/iconRun.png',
                                'width': 16,
                                'height': 16,
                                'y': 25,
                                'class': 'process-group-running'
                            });

                    // running count
                    details.append('text')
                            .attr({
                                'y': 37,
                                'class': 'process-group-running-count process-group-contents-count'
                            });

                    // stopped icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'xlink:href': 'images/iconStop.png',
                                'width': 16,
                                'height': 16,
                                'y': 25,
                                'class': 'process-group-stopped'
                            });

                    // stopped count
                    details.append('text')
                            .attr({
                                'y': 37,
                                'class': 'process-group-stopped-count process-group-contents-count'
                            });

                    // invalid icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'xlink:href': 'images/iconAlert.png',
                                'width': 16,
                                'height': 16,
                                'y': 25,
                                'class': 'process-group-invalid'
                            });

                    // invalid count
                    details.append('text')
                            .attr({
                                'y': 37,
                                'class': 'process-group-invalid-count process-group-contents-count'
                            });

                    // disabled icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'xlink:href': 'images/iconDisable.png',
                                'width': 16,
                                'height': 16,
                                'y': 25,
                                'class': 'process-group-disabled'
                            });

                    // disabled count
                    details.append('text')
                            .attr({
                                'y': 37,
                                'class': 'process-group-disabled-count process-group-contents-count'
                            });

                    // -----
                    // stats
                    // -----

                    // stats label container
                    var processGroupStatsLabel = details.append('g')
                            .attr({
                                'transform': 'translate(6, 54)'
                            });

                    // queued label
                    processGroupStatsLabel.append('text')
                            .attr({
                                'width': 73,
                                'height': 10,
                                'x': 4,
                                'y': 4,
                                'class': 'process-group-stats-label'
                            })
                            .text('Queued');

                    // in label
                    processGroupStatsLabel.append('text')
                            .attr({
                                'width': 73,
                                'height': 10,
                                'x': 4,
                                'y': 17,
                                'class': 'process-group-stats-label'
                            })
                            .text('In');

                    // read/write label
                    processGroupStatsLabel.append('text')
                            .attr({
                                'width': 73,
                                'height': 10,
                                'x': 4,
                                'y': 30,
                                'class': 'process-group-stats-label'
                            })
                            .text('Read/Write');

                    // out label
                    processGroupStatsLabel.append('text')
                            .attr({
                                'width': 73,
                                'height': 10,
                                'x': 4,
                                'y': 43,
                                'class': 'process-group-stats-label'
                            })
                            .text('Out');

                    // stats value container
                    var processGroupStatsValue = details.append('g')
                            .attr({
                                'transform': 'translate(95, 54)'
                            });

                    // queued value
                    processGroupStatsValue.append('text')
                            .attr({
                                'width': 180,
                                'height': 10,
                                'x': 4,
                                'y': 4,
                                'class': 'process-group-queued process-group-stats-value'
                            });

                    // in value
                    processGroupStatsValue.append('text')
                            .attr({
                                'width': 180,
                                'height': 10,
                                'x': 4,
                                'y': 17,
                                'class': 'process-group-in process-group-stats-value'
                            });

                    // read/write value
                    processGroupStatsValue.append('text')
                            .attr({
                                'width': 180,
                                'height': 10,
                                'x': 4,
                                'y': 30,
                                'class': 'process-group-read-write process-group-stats-value'
                            });

                    // out value
                    processGroupStatsValue.append('text')
                            .attr({
                                'width': 180,
                                'height': 10,
                                'x': 4,
                                'y': 43,
                                'class': 'process-group-out process-group-stats-value'
                            });

                    // stats value container
                    var processGroupStatsInfo = details.append('g')
                            .attr({
                                'transform': 'translate(314, 54)'
                            });

                    // in info
                    processGroupStatsInfo.append('text')
                            .attr({
                                'width': 25,
                                'height': 10,
                                'x': 4,
                                'y': 17,
                                'class': 'process-group-stats-info'
                            })
                            .text('(5 min)');

                    // read/write info
                    processGroupStatsInfo.append('text')
                            .attr({
                                'width': 25,
                                'height': 10,
                                'x': 4,
                                'y': 30,
                                'class': 'process-group-stats-info'
                            })
                            .text('(5 min)');

                    // out info
                    processGroupStatsInfo.append('text')
                            .attr({
                                'width': 25,
                                'height': 10,
                                'x': 4,
                                'y': 43,
                                'class': 'process-group-stats-info'
                            })
                            .text('(5 min)');

                    // --------
                    // comments
                    // --------

                    // process group comments
                    details.append('text')
                            .attr({
                                'x': 10,
                                'y': 118,
                                'width': 342,
                                'height': 22,
                                'class': 'process-group-comments'
                            });

                    // -------------------
                    // active thread count
                    // -------------------

                    // active thread count
                    details.append('rect')
                            .attr({
                                'class': 'active-thread-count-background',
                                'height': 13,
                                'y': 0,
                                'fill': '#fff',
                                'fill-opacity': '0.4',
                                'stroke': '#aaa',
                                'stroke-width': '1'
                            });

                    // active thread bacground
                    details.append('text')
                            .attr({
                                'class': 'active-thread-count',
                                'height': 13,
                                'y': 10,
                                'fill': '#000'
                            });

                    // ---------
                    // bulletins
                    // ---------

                    // bulletin icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'class': 'bulletin-icon',
                                'xlink:href': 'images/iconBulletin.png',
                                'width': 12,
                                'height': 12,
                                'y': 2
                            });
                }

                // update the input ports
                var inputPortCount = details.select('text.process-group-input-port-count')
                        .text(function (d) {
                            return d.component.inputPortCount;
                        });

                // update the output ports
                var outputPort = details.select('image.process-group-output-port')
                        .attr('x', function () {
                            var inputPortCountX = parseInt(inputPortCount.attr('x'), 10);
                            return inputPortCountX + inputPortCount.node().getComputedTextLength() + CONTENTS_SPACER;
                        });
                details.select('text.process-group-output-port-count')
                        .attr('x', function () {
                            var outputPortImageX = parseInt(outputPort.attr('x'), 10);
                            var outputPortImageWidth = parseInt(outputPort.attr('width'), 10);
                            return outputPortImageX + outputPortImageWidth + CONTENTS_SPACER;
                        })
                        .text(function (d) {
                            return d.component.outputPortCount;
                        });

                // get the container to help right align
                var container = details.select('rect.process-group-contents-container');

                // update disabled
                var disabledCount = details.select('text.process-group-disabled-count')
                        .text(function (d) {
                            return d.component.disabledCount;
                        })
                        .attr('x', function () {
                            var containerX = parseInt(container.attr('x'), 10);
                            var containerWidth = parseInt(container.attr('width'), 10);
                            return containerX + containerWidth - this.getComputedTextLength() - CONTENTS_SPACER;
                        });
                var disabled = details.select('image.process-group-disabled')
                        .attr('x', function () {
                            var disabledCountX = parseInt(disabledCount.attr('x'), 10);
                            var width = parseInt(d3.select(this).attr('width'), 10);
                            return disabledCountX - width - CONTENTS_SPACER;
                        });

                // update invalid
                var invalidCount = details.select('text.process-group-invalid-count')
                        .text(function (d) {
                            return d.component.invalidCount;
                        })
                        .attr('x', function () {
                            var disabledX = parseInt(disabled.attr('x'), 10);
                            return disabledX - this.getComputedTextLength() - CONTENTS_SPACER;
                        });
                var invalid = details.select('image.process-group-invalid')
                        .attr('x', function () {
                            var invalidCountX = parseInt(invalidCount.attr('x'), 10);
                            var width = parseInt(d3.select(this).attr('width'), 10);
                            return invalidCountX - width - CONTENTS_SPACER;
                        });

                // update stopped
                var stoppedCount = details.select('text.process-group-stopped-count')
                        .text(function (d) {
                            return d.component.stoppedCount;
                        })
                        .attr('x', function () {
                            var invalidX = parseInt(invalid.attr('x'), 10);
                            return invalidX - this.getComputedTextLength() - CONTENTS_SPACER;
                        });
                var stopped = details.select('image.process-group-stopped')
                        .attr('x', function () {
                            var stoppedCountX = parseInt(stoppedCount.attr('x'), 10);
                            var width = parseInt(d3.select(this).attr('width'), 10);
                            return stoppedCountX - width - CONTENTS_SPACER;
                        });

                // update running
                var runningCount = details.select('text.process-group-running-count')
                        .text(function (d) {
                            return d.component.runningCount;
                        })
                        .attr('x', function () {
                            var stoppedX = parseInt(stopped.attr('x'), 10);
                            return stoppedX - this.getComputedTextLength() - CONTENTS_SPACER;
                        });
                var running = details.select('image.process-group-running')
                        .attr('x', function () {
                            var runningCountX = parseInt(runningCount.attr('x'), 10);
                            var width = parseInt(d3.select(this).attr('width'), 10);
                            return runningCountX - width - CONTENTS_SPACER;
                        });

                // update not transmitting
                var notTransmittingCount = details.select('text.process-group-not-transmitting-count')
                        .text(function (d) {
                            return d.component.inactiveRemotePortCount;
                        })
                        .attr('x', function () {
                            var runningX = parseInt(running.attr('x'), 10);
                            return runningX - this.getComputedTextLength() - CONTENTS_SPACER;
                        });
                var notTransmitting = details.select('image.process-group-not-transmitting')
                        .attr('x', function () {
                            var notTransmittingCountX = parseInt(notTransmittingCount.attr('x'), 10);
                            var width = parseInt(d3.select(this).attr('width'), 10);
                            return notTransmittingCountX - width - CONTENTS_SPACER;
                        });

                // update transmitting
                var transmittingCount = details.select('text.process-group-transmitting-count')
                        .text(function (d) {
                            return d.component.activeRemotePortCount;
                        })
                        .attr('x', function () {
                            var notTransmittingX = parseInt(notTransmitting.attr('x'), 10);
                            return notTransmittingX - this.getComputedTextLength() - CONTENTS_SPACER;
                        });
                details.select('image.process-group-transmitting')
                        .attr('x', function () {
                            var transmittingCountX = parseInt(transmittingCount.attr('x'), 10);
                            var width = parseInt(d3.select(this).attr('width'), 10);
                            return transmittingCountX - width - CONTENTS_SPACER;
                        });

                // update the process group comments
                details.select('text.process-group-comments')
                        .each(function (d) {
                            var processGroupComments = d3.select(this);

                            // reset the process group name to handle any previous state
                            processGroupComments.text(null).selectAll('tspan, title').remove();

                            // apply ellipsis to the port name as necessary
                            nf.CanvasUtils.multilineEllipsis(processGroupComments, 2, getProcessGroupComments(d));
                        }).classed('unset', function (d) {
                    return nf.Common.isBlank(d.component.comments);
                }).append('title').text(function (d) {
                    return getProcessGroupComments(d);
                });

                // update the process group name
                processGroup.select('text.process-group-name')
                        .each(function (d) {
                            var processGroupName = d3.select(this);

                            // reset the process group name to handle any previous state
                            processGroupName.text(null).selectAll('title').remove();

                            // apply ellipsis to the process group name as necessary
                            nf.CanvasUtils.ellipsis(processGroupName, d.component.name);
                        }).append('title').text(function (d) {
                    return d.component.name;
                });

                // hide the preview
                processGroup.select('image.process-group-preview').style('display', 'none');

                // populate the stats
                processGroup.call(updateProcessGroupStatus);
            } else {
                // update the process group name
                processGroup.select('text.process-group-name')
                        .text(function (d) {
                            var name = d.component.name;
                            if (name.length > PREVIEW_NAME_LENGTH) {
                                return name.substring(0, PREVIEW_NAME_LENGTH) + String.fromCharCode(8230);
                            } else {
                                return name;
                            }
                        });

                // show the preview
                processGroup.select('image.process-group-preview').style('display', 'block');

                // remove the tooltips
                processGroup.call(removeTooltips);

                // remove the details if necessary
                if (!details.empty()) {
                    details.remove();
                }
            }
        });
    };

    /**
     * Updates the process group status.
     * 
     * @param {selection} updated           The process groups to be updated
     */
    var updateProcessGroupStatus = function (updated) {
        if (updated.empty()) {
            return;
        }

        // queued value
        updated.select('text.process-group-queued')
                .text(function (d) {
                    if (nf.Common.isDefinedAndNotNull(d.status)) {
                        return d.status.queued;
                    } else {
                        return '- / -';
                    }
                });

        // in value
        updated.select('text.process-group-in')
                .text(function (d) {
                    if (nf.Common.isDefinedAndNotNull(d.status)) {
                        return d.status.input;
                    } else {
                        return '- / -';
                    }
                });

        // read/write value
        updated.select('text.process-group-read-write')
                .text(function (d) {
                    if (nf.Common.isDefinedAndNotNull(d.status)) {
                        return d.status.read + ' / ' + d.status.written;
                    } else {
                        return '- / -';
                    }
                });

        // out value
        updated.select('text.process-group-out')
                .text(function (d) {
                    if (nf.Common.isDefinedAndNotNull(d.status)) {
                        return d.status.output;
                    } else {
                        return '- / -';
                    }
                });

        updated.each(function (d) {
            var processGroup = d3.select(this);
            var offset = 0;

            // -------------------
            // active thread count
            // -------------------

            nf.CanvasUtils.activeThreadCount(processGroup, d, function (off) {
                offset = off;
            });

            // ---------
            // bulletins
            // ---------

            nf.CanvasUtils.bulletins(processGroup, d, function () {
                return d3.select('#process-group-tooltips');
            }, offset);
        });
    };

    /**
     * Removes the process groups in the specified selection.
     *
     * @param {selection} removed               The process groups to be removed
     */
    var removeProcessGroups = function (removed) {
        if (removed.empty()) {
            return;
        }

        removed.call(removeTooltips).remove();
    };

    /**
     * Removes the tooltips for the process groups in the specified selection.
     * 
     * @param {selection} removed
     */
    var removeTooltips = function (removed) {
        removed.each(function (d) {
            // remove any associated tooltips
            $('#bulletin-tip-' + d.component.id).remove();
        });
    };

    return {
        /**
         * Initializes of the Process Group handler.
         */
        init: function () {
            processGroupMap = d3.map();

            // create the process group container
            processGroupContainer = d3.select('#canvas').append('g')
                    .attr({
                        'pointer-events': 'all',
                        'class': 'process-groups'
                    });
        },
        
        /**
         * Populates the graph with the specified process groups.
         *
         * @argument {object | array} processGroups                    The process groups to add
         * @argument {boolean} selectAll                Whether or not to select the new contents
         */
        add: function (processGroups, selectAll) {
            selectAll = nf.Common.isDefinedAndNotNull(selectAll) ? selectAll : false;

            var add = function (processGroup) {
                // add the process group
                processGroupMap.set(processGroup.id, {
                    type: 'ProcessGroup',
                    component: processGroup,
                    dimensions: dimensions
                });
            };

            // determine how to handle the specified process groups
            if ($.isArray(processGroups)) {
                $.each(processGroups, function (_, processGroup) {
                    add(processGroup);
                });
            } else {
                add(processGroups);
            }

            // apply the selection and handle all new process group
            select().enter().call(renderProcessGroups, selectAll);
        },
        
        /**
         * If the process group id is specified it is returned. If no process group id
         * specified, all process groups are returned.
         *
         * @param {string} id
         */
        get: function (id) {
            if (nf.Common.isUndefined(id)) {
                return processGroupMap.values();
            } else {
                return processGroupMap.get(id);
            }
        },
        
        /**
         * If the process group id is specified it is refresh according to the current
         * state. If no process group id is specified, all process groups are refreshed.
         *
         * @param {string} id      Optional
         */
        refresh: function (id) {
            if (nf.Common.isDefinedAndNotNull(id)) {
                d3.select('#id-' + id).call(updateProcessGroups);
            } else {
                d3.selectAll('g.process-group').call(updateProcessGroups);
            }
        },
        
        /**
         * Refreshes the components necessary after a pan event.
         */
        pan: function () {
            d3.selectAll('g.process-group.entering, g.process-group.leaving').call(updateProcessGroups);
        },
        
        /**
         * Reloads the process group state from the server and refreshes the UI.
         * If the process group is currently unknown, this function just returns.
         *
         * @param {object} processGroup The process group to reload
         */
        reload: function (processGroup) {
            if (processGroupMap.has(processGroup.id)) {
                return $.ajax({
                    type: 'GET',
                    url: processGroup.uri,
                    dataType: 'json'
                }).done(function (response) {
                    nf.ProcessGroup.set(response.processGroup);
                });
            }
        },
        
        /**
         * Positions the component.
         * 
         * @param {string} id   The id
         */
        position: function (id) {
            d3.select('#id-' + id).call(nf.CanvasUtils.position);
        },
        
        /**
         * Sets the specified process group(s). If the is an array, it
         * will set each process group. If it is not an array, it will
         * attempt to set the specified process group.
         *
         * @param {object | array} processGroups
         */
        set: function (processGroups) {
            var set = function (processGroup) {
                if (processGroupMap.has(processGroup.id)) {
                    // update the current entry
                    var processGroupEntry = processGroupMap.get(processGroup.id);
                    processGroupEntry.component = processGroup;

                    // update the process group in the UI
                    d3.select('#id-' + processGroup.id).call(updateProcessGroups);
                }
            };

            // determine how to handle the specified process group
            if ($.isArray(processGroups)) {
                $.each(processGroups, function (_, processGroup) {
                    set(processGroup);
                });
            } else {
                set(processGroups);
            }
        },
        
        /**
         * Sets the process group status using the specified status.
         * 
         * @param {array} processGroupStatus       Process group status
         */
        setStatus: function (processGroupStatus) {
            if (nf.Common.isEmpty(processGroupStatus)) {
                return;
            }

            // update the specified process group status
            $.each(processGroupStatus, function (_, status) {
                if (processGroupMap.has(status.id)) {
                    var processGroup = processGroupMap.get(status.id);
                    processGroup.status = status;
                }
            });

            // update the visible process groups
            d3.selectAll('g.process-group.visible').call(updateProcessGroupStatus);
        },
        
        /**
         * Removes the specified process group.
         *
         * @param {string} processGroups      The process group id(s)
         */
        remove: function (processGroups) {
            if ($.isArray(processGroups)) {
                $.each(processGroups, function (_, processGroup) {
                    processGroupMap.remove(processGroup);
                });
            } else {
                processGroupMap.remove(processGroups);
            }

            // apply the selection and handle all removed process groups
            select().exit().call(removeProcessGroups);
        },
        
        /**
         * Removes all process groups.
         */
        removeAll: function () {
            nf.ProcessGroup.remove(processGroupMap.keys());
        }
    };
}());