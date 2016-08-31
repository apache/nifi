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

nf.ProcessGroup = (function () {

    var PREVIEW_NAME_LENGTH = 30;

    var dimensions = {
        width: 380,
        height: 172
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
            return d.id;
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
                    return 'id-' + d.id;
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
                'class': 'border',
                'width': function (d) {
                    return d.dimensions.width;
                },
                'height': function (d) {
                    return d.dimensions.height;
                },
                'fill': 'transparent',
                'stroke': 'transparent'
            });

        // process group body
        processGroup.append('rect')
            .attr({
                'class': 'body',
                'width': function (d) {
                    return d.dimensions.width;
                },
                'height': function (d) {
                    return d.dimensions.height;
                },
                'filter': 'url(#component-drop-shadow)',
                'stroke-width': 0
            });

        // process group name background
        processGroup.append('rect')
            .attr({
                'width': function (d) {
                    return d.dimensions.width;
                },
                'height': 32,
                'fill': '#b8c6cd'
            });

        // process group name
        processGroup.append('text')
            .attr({
                'x': 10,
                'y': 20,
                'width': 316,
                'height': 16,
                'class': 'process-group-name'
            });

        // process group preview
        processGroup.append('image')
            .call(nf.CanvasUtils.disableImageHref)
            .attr({
                'width': 352,
                'height': 113,
                'x': 6,
                'y': 22,
                'class': 'process-group-preview'
            });

        // always support selecting and navigation
        processGroup.on('dblclick', function (d) {
                // enter this group on double click
                nf.CanvasUtils.enterGroup(d.id);
            })
            .call(nf.Selectable.activate).call(nf.ContextMenu.activate);

        // only support dragging, connection, and drag and drop if appropriate
        processGroup.filter(function (d) {
                return d.permissions.canWrite && d.permissions.canRead;
            })
            .on('mouseover.drop', function (d) {
                // Using mouseover/out to workaround chrome issue #122746

                // get the target and ensure its not already been marked for drop
                var target = d3.select(this);
                if (!target.classed('drop')) {
                    var targetData = target.datum();

                    // see if there is a selection being dragged
                    var drag = d3.select('rect.drag-selection');
                    if (!drag.empty()) {
                        // filter the current selection by this group
                        var selection = nf.CanvasUtils.getSelection().filter(function (d) {
                            return targetData.id === d.id;
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
    };

    // attempt of space between component count and icon for process group contents
    var CONTENTS_SPACER = 10;

    /**
     * Updates the process groups in the specified selection.
     *
     * @param {selection} updated               The process groups to be updated
     */
    var updateProcessGroups = function (updated) {
        if (updated.empty()) {
            return;
        }

        // process group border authorization
        updated.select('rect.border')
            .classed('unauthorized', function (d) {
                return d.permissions.canRead === false;
            });

        // process group body authorization
        updated.select('rect.body')
            .classed('unauthorized', function (d) {
                return d.permissions.canRead === false;
            });

        updated.each(function (processGroupData) {
            var processGroup = d3.select(this);
            var details = processGroup.select('g.process-group-details');

            // update the component behavior as appropriate
            nf.CanvasUtils.editable(processGroup);

            // if this processor is visible, render everything
            if (processGroup.classed('visible')) {
                if (details.empty()) {
                    details = processGroup.append('g').attr('class', 'process-group-details');

                    // -------------------
                    // contents background
                    // -------------------

                    details.append('rect')
                        .attr({
                            'x': 0,
                            'y': 32,
                            'width': function () {
                                return processGroupData.dimensions.width
                            },
                            'height': 24,
                            'fill': '#e3e8eb'
                        });

                    // --------
                    // contents
                    // --------

                    // transmitting icon
                    details.append('text')
                        .attr({
                            'x': 10,
                            'y': 49,
                            'class': 'process-group-transmitting process-group-contents-icon',
                            'font-family': 'FontAwesome'
                        })
                        .text('\uf140');

                    // transmitting count
                    details.append('text')
                        .attr({
                            'x': 28,
                            'y': 49,
                            'class': 'process-group-transmitting-count process-group-contents-count'
                        });

                    // not transmitting icon
                    details.append('text')
                        .attr({
                            'y': 49,
                            'class': 'process-group-not-transmitting process-group-contents-icon',
                            'font-family': 'flowfont'
                        })
                        .text('\ue80a');

                    // not transmitting count
                    details.append('text')
                        .attr({
                            'y': 49,
                            'class': 'process-group-not-transmitting-count process-group-contents-count'
                        });

                    // running icon
                    details.append('text')
                        .attr({
                            'y': 49,
                            'class': 'process-group-running process-group-contents-icon',
                            'font-family': 'FontAwesome'
                        })
                        .text('\uf04b');

                    // running count
                    details.append('text')
                        .attr({
                            'y': 49,
                            'class': 'process-group-running-count process-group-contents-count'
                        });

                    // stopped icon
                    details.append('text')
                        .attr({
                            'y': 49,
                            'class': 'process-group-stopped process-group-contents-icon',
                            'font-family': 'FontAwesome'
                        })
                        .text('\uf04d');

                    // stopped count
                    details.append('text')
                        .attr({
                            'y': 49,
                            'class': 'process-group-stopped-count process-group-contents-count'
                        });

                    // invalid icon
                    details.append('text')
                        .attr({
                            'y': 49,
                            'class': 'process-group-invalid process-group-contents-icon',
                            'font-family': 'FontAwesome'
                        })
                        .text('\uf071');

                    // invalid count
                    details.append('text')
                        .attr({
                            'y': 49,
                            'class': 'process-group-invalid-count process-group-contents-count'
                        });

                    // disabled icon
                    details.append('text')
                        .attr({
                            'y': 49,
                            'class': 'process-group-disabled process-group-contents-icon',
                            'font-family': 'flowfont'
                        })
                        .text('\ue802');

                    // disabled count
                    details.append('text')
                        .attr({
                            'y': 49,
                            'class': 'process-group-disabled-count process-group-contents-count'
                        });

                    // ----------------
                    // stats background
                    // ----------------

                    // queued
                    details.append('rect')
                        .attr({
                            'width': function () {
                                return processGroupData.dimensions.width;
                            },
                            'height': 19,
                            'x': 0,
                            'y': 66,
                            'fill': '#f4f6f7'
                        });

                    // border
                    details.append('rect')
                        .attr({
                            'width': function () {
                                return processGroupData.dimensions.width;
                            },
                            'height': 1,
                            'x': 0,
                            'y': 84,
                            'fill': '#c7d2d7'
                        });

                    // in
                    details.append('rect')
                        .attr({
                            'width': function () {
                                return processGroupData.dimensions.width;
                            },
                            'height': 19,
                            'x': 0,
                            'y': 85,
                            'fill': '#ffffff'
                        });

                    // border
                    details.append('rect')
                        .attr({
                            'width': function () {
                                return processGroupData.dimensions.width;
                            },
                            'height': 1,
                            'x': 0,
                            'y': 103,
                            'fill': '#c7d2d7'
                        });

                    // read/write
                    details.append('rect')
                        .attr({
                            'width': function () {
                                return processGroupData.dimensions.width;
                            },
                            'height': 19,
                            'x': 0,
                            'y': 104,
                            'fill': '#f4f6f7'
                        });

                    // border
                    details.append('rect')
                        .attr({
                            'width': function () {
                                return processGroupData.dimensions.width;
                            },
                            'height': 1,
                            'x': 0,
                            'y': 122,
                            'fill': '#c7d2d7'
                        });

                    // out
                    details.append('rect')
                        .attr({
                            'width': function () {
                                return processGroupData.dimensions.width;
                            },
                            'height': 19,
                            'x': 0,
                            'y': 123,
                            'fill': '#ffffff'
                        });

                    // -----
                    // stats
                    // -----

                    // stats label container
                    var processGroupStatsLabel = details.append('g')
                        .attr({
                            'transform': 'translate(6, 75)'
                        });

                    // queued label
                    processGroupStatsLabel.append('text')
                        .attr({
                            'width': 73,
                            'height': 10,
                            'x': 4,
                            'y': 5,
                            'class': 'stats-label'
                        })
                        .text('Queued');

                    // in label
                    processGroupStatsLabel.append('text')
                        .attr({
                            'width': 73,
                            'height': 10,
                            'x': 4,
                            'y': 24,
                            'class': 'stats-label'
                        })
                        .text('In');

                    // read/write label
                    processGroupStatsLabel.append('text')
                        .attr({
                            'width': 73,
                            'height': 10,
                            'x': 4,
                            'y': 42,
                            'class': 'stats-label'
                        })
                        .text('Read/Write');

                    // out label
                    processGroupStatsLabel.append('text')
                        .attr({
                            'width': 73,
                            'height': 10,
                            'x': 4,
                            'y': 60,
                            'class': 'stats-label'
                        })
                        .text('Out');

                    // stats value container
                    var processGroupStatsValue = details.append('g')
                        .attr({
                            'transform': 'translate(95, 75)'
                        });

                    // queued value
                    var queuedText = processGroupStatsValue.append('text')
                        .attr({
                            'width': 180,
                            'height': 10,
                            'x': 4,
                            'y': 5,
                            'class': 'process-group-queued stats-value'
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

                    // in value
                    var inText = processGroupStatsValue.append('text')
                        .attr({
                            'width': 180,
                            'height': 10,
                            'x': 4,
                            'y': 24,
                            'class': 'process-group-in stats-value'
                        });

                    // in count
                    inText.append('tspan')
                        .attr({
                            'class': 'count'
                        });

                    // in size
                    inText.append('tspan')
                        .attr({
                            'class': 'size'
                        });

                    // in
                    inText.append('tspan')
                        .attr({
                            'class': 'ports'
                        });

                    // read/write value
                    processGroupStatsValue.append('text')
                        .attr({
                            'width': 180,
                            'height': 10,
                            'x': 4,
                            'y': 42,
                            'class': 'process-group-read-write stats-value'
                        });

                    // out value
                    var outText = processGroupStatsValue.append('text')
                        .attr({
                            'width': 180,
                            'height': 10,
                            'x': 4,
                            'y': 60,
                            'class': 'process-group-out stats-value'
                        });
                    
                    // out ports
                    outText.append('tspan')
                        .attr({
                            'class': 'ports'
                        });

                    // out count
                    outText.append('tspan')
                        .attr({
                            'class': 'count'
                        });

                    // out size
                    outText.append('tspan')
                        .attr({
                            'class': 'size'
                        });

                    // stats value container
                    var processGroupStatsInfo = details.append('g')
                        .attr({
                            'transform': 'translate(335, 75)'
                        });

                    // in info
                    processGroupStatsInfo.append('text')
                        .attr({
                            'width': 25,
                            'height': 10,
                            'x': 4,
                            'y': 24,
                            'class': 'stats-info'
                        })
                        .text('5 min');

                    // read/write info
                    processGroupStatsInfo.append('text')
                        .attr({
                            'width': 25,
                            'height': 10,
                            'x': 4,
                            'y': 42,
                            'class': 'stats-info'
                        })
                        .text('5 min');

                    // out info
                    processGroupStatsInfo.append('text')
                        .attr({
                            'width': 25,
                            'height': 10,
                            'x': 4,
                            'y': 60,
                            'class': 'stats-info'
                        })
                        .text('5 min');

                    // --------
                    // comments
                    // --------

                    // process group comments
                    details.append('text')
                        .attr({
                            'x': 10,
                            'y': 160,
                            'width': 342,
                            'height': 22,
                            'class': 'process-group-comments'
                        });

                    // -------------------
                    // active thread count
                    // -------------------

                    // active thread count
                    details.append('text')
                        .attr({
                            'class': 'active-thread-count-icon',
                            'y': 20
                        })
                        .text('\ue83f');

                    // active thread icon
                    details.append('text')
                        .attr({
                            'class': 'active-thread-count',
                            'y': 20
                        });

                    // ---------
                    // bulletins
                    // ---------

                    // bulletin background
                    details.append('rect')
                        .attr({
                            'class': 'bulletin-background',
                            'x': function () {
                                return processGroupData.dimensions.width - 24;
                            },
                            'y': 32,
                            'width': 24,
                            'height': 24
                        });

                    // bulletin icon
                    details.append('text')
                        .attr({
                            'class': 'bulletin-icon',
                            'x': function () {
                                return processGroupData.dimensions.width - 17;
                            },
                            'y': 49
                        })
                        .text('\uf24a');
                }

                // update transmitting
                var transmittingCount = details.select('text.process-group-transmitting-count')
                    .text(function (d) {
                        return d.activeRemotePortCount;
                    });

                // update not transmitting
                var notTransmitting = details.select('text.process-group-not-transmitting')
                    .attr('x', function () {
                        var transmittingX = parseInt(transmittingCount.attr('x'), 10);
                        return transmittingX + transmittingCount.node().getComputedTextLength() + CONTENTS_SPACER;
                    });
                var notTransmittingCount = details.select('text.process-group-not-transmitting-count')
                    .attr('x', function () {
                        var notTransmittingCountX = parseInt(notTransmitting.attr('x'), 10);
                        return notTransmittingCountX + notTransmitting.node().getComputedTextLength() + CONTENTS_SPACER;
                    })
                    .text(function (d) {
                        return d.inactiveRemotePortCount;
                    });

                // update running
                var running = details.select('text.process-group-running')
                    .attr('x', function () {
                        var notTransmittingX = parseInt(notTransmittingCount.attr('x'), 10);
                        return notTransmittingX + notTransmittingCount.node().getComputedTextLength() + CONTENTS_SPACER;
                    });
                var runningCount = details.select('text.process-group-running-count')
                    .attr('x', function () {
                        var runningCountX = parseInt(running.attr('x'), 10);
                        return runningCountX + running.node().getComputedTextLength() + CONTENTS_SPACER;
                    })
                    .text(function (d) {
                        return d.runningCount;
                    });

                // update stopped
                var stopped = details.select('text.process-group-stopped')
                    .attr('x', function () {
                        var runningX = parseInt(runningCount.attr('x'), 10);
                        return runningX + runningCount.node().getComputedTextLength() + CONTENTS_SPACER;
                    });
                var stoppedCount = details.select('text.process-group-stopped-count')
                    .attr('x', function () {
                        var stoppedCountX = parseInt(stopped.attr('x'), 10);
                        return stoppedCountX + stopped.node().getComputedTextLength() + CONTENTS_SPACER;
                    })
                    .text(function (d) {
                        return d.stoppedCount;
                    });

                // update invalid
                var invalid = details.select('text.process-group-invalid')
                    .attr('x', function () {
                        var stoppedX = parseInt(stoppedCount.attr('x'), 10);
                        return stoppedX + stoppedCount.node().getComputedTextLength() + CONTENTS_SPACER;
                    })
                    .classed('has-validation-errors', function (d) {
                        return d.permissions.canRead && d.component.invalidCount > 0;
                    });
                var invalidCount = details.select('text.process-group-invalid-count')
                    .attr('x', function () {
                        var invalidCountX = parseInt(invalid.attr('x'), 10);
                        return invalidCountX + invalid.node().getComputedTextLength() + CONTENTS_SPACER;
                    })
                    .text(function (d) {
                        return d.invalidCount;
                    });

                // update disabled
                var disabled = details.select('text.process-group-disabled')
                    .attr('x', function () {
                        var invalidX = parseInt(invalidCount.attr('x'), 10);
                        return invalidX + invalidCount.node().getComputedTextLength() + CONTENTS_SPACER;
                    });
                details.select('text.process-group-disabled-count')
                    .attr('x', function () {
                        var disabledCountX = parseInt(disabled.attr('x'), 10);
                        return disabledCountX + disabled.node().getComputedTextLength() + CONTENTS_SPACER;
                    })
                    .text(function (d) {
                        return d.disabledCount;
                    });

                if (processGroupData.permissions.canRead) {
                    // update the process group comments
                    details.select('text.process-group-comments')
                        .each(function (d) {
                            var processGroupComments = d3.select(this);

                            // reset the process group name to handle any previous state
                            processGroupComments.text(null).selectAll('tspan, title').remove();

                            // apply ellipsis to the port name as necessary
                            nf.CanvasUtils.ellipsis(processGroupComments, getProcessGroupComments(d));
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
                } else {
                    // clear the process group comments
                    details.select('text.process-group-comments').text(null);
                    
                    // clear the process group name
                    processGroup.select('text.process-group-name').text(null);
                }

                // hide the preview
                processGroup.select('image.process-group-preview').style('display', 'none');

                // populate the stats
                processGroup.call(updateProcessGroupStatus);
            } else {
                if (processGroupData.permissions.canRead) {
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
                }

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

        // queued count value
        updated.select('text.process-group-queued tspan.count')
            .text(function (d) {
                return nf.Common.substringBeforeFirst(d.status.aggregateSnapshot.queued, ' ');
            });

        // queued size value
        updated.select('text.process-group-queued tspan.size')
            .text(function (d) {
                return ' ' + nf.Common.substringAfterFirst(d.status.aggregateSnapshot.queued, ' ');
            });

        // in count value
        updated.select('text.process-group-in tspan.count')
            .text(function (d) {
                return nf.Common.substringBeforeFirst(d.status.aggregateSnapshot.input, ' ');
            });

        // in size value
        updated.select('text.process-group-in tspan.size')
            .text(function (d) {
                return ' ' + nf.Common.substringAfterFirst(d.status.aggregateSnapshot.input, ' ');
            });

        // in ports value
        updated.select('text.process-group-in tspan.ports')
            .text(function (d) {
                return ' ' + String.fromCharCode(8594) + ' ' + d.inputPortCount;
            });

        // read/write value
        updated.select('text.process-group-read-write')
            .text(function (d) {
                return d.status.aggregateSnapshot.read + ' / ' + d.status.aggregateSnapshot.written;
            });

        // out ports value
        updated.select('text.process-group-out tspan.ports')
            .text(function (d) {
                return d.outputPortCount + ' ' + String.fromCharCode(8594) + ' ';
            });
        
        // out count value
        updated.select('text.process-group-out tspan.count')
            .text(function (d) {
                return nf.Common.substringBeforeFirst(d.status.aggregateSnapshot.output, ' ');
            });

        // out size value
        updated.select('text.process-group-out tspan.size')
            .text(function (d) {
                return ' ' + nf.Common.substringAfterFirst(d.status.aggregateSnapshot.output, ' ');
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

            processGroup.select('rect.bulletin-background').classed('has-bulletins', function () {
                return !nf.Common.isEmpty(d.status.aggregateSnapshot.bulletins);
            });

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
            $('#bulletin-tip-' + d.id).remove();
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
         * Adds the specified process group entity.
         *
         * @param processGroupEntities       The process group
         * @param options           Configuration options
         */
        add: function (processGroupEntities, options) {
            var selectAll = false;
            if (nf.Common.isDefinedAndNotNull(options)) {
                selectAll = nf.Common.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
            }

            var add = function (processGroupEntity) {
                // add the process group
                processGroupMap.set(processGroupEntity.id, $.extend({
                    type: 'ProcessGroup',
                    dimensions: dimensions
                }, processGroupEntity));
            };

            // determine how to handle the specified process groups
            if ($.isArray(processGroupEntities)) {
                $.each(processGroupEntities, function (_, processGroupEntity) {
                    add(processGroupEntity);
                });
            } else if (nf.Common.isDefinedAndNotNull(processGroupEntities)) {
                add(processGroupEntities);
            }

            // apply the selection and handle new process groups
            var selection = select();
            selection.enter().call(renderProcessGroups, selectAll);
            selection.call(updateProcessGroups);
        },
        
        /**
         * Populates the graph with the specified process groups.
         *
         * @argument {object | array} processGroupEntities                    The process groups to add
         * @argument {object} options                Configuration options
         */
        set: function (processGroupEntities, options) {
            var selectAll = false;
            var transition = false;
            if (nf.Common.isDefinedAndNotNull(options)) {
                selectAll = nf.Common.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
                transition = nf.Common.isDefinedAndNotNull(options.transition) ? options.transition : transition;
            }

            var set = function (proposedProcessGroupEntity) {
                var currentProcessGroupEntity = processGroupMap.get(proposedProcessGroupEntity.id);

                // set the process group if appropriate
                if (nf.Client.isNewerRevision(currentProcessGroupEntity, proposedProcessGroupEntity)) {
                    processGroupMap.set(proposedProcessGroupEntity.id, $.extend({
                        type: 'ProcessGroup',
                        dimensions: dimensions
                    }, proposedProcessGroupEntity));
                }
            };

            // determine how to handle the specified process groups
            if ($.isArray(processGroupEntities)) {
                $.each(processGroupMap.keys(), function (_, key) {
                    var currentProcessGroupEntity = processGroupMap.get(key);
                    var isPresent = $.grep(processGroupEntities, function (proposedProcessGroupEntity) {
                        return proposedProcessGroupEntity.id === currentProcessGroupEntity.id;
                    });

                    // if the current process group is not present, remove it
                    if (isPresent.length === 0) {
                        processGroupMap.remove(key);
                    }
                });
                $.each(processGroupEntities, function (_, processGroupEntity) {
                    set(processGroupEntity);
                });
            } else if (nf.Common.isDefinedAndNotNull(processGroupEntities)) {
                set(processGroupEntities);
            }

            // apply the selection and handle all new process group
            var selection = select();
            selection.enter().call(renderProcessGroups, selectAll);
            selection.call(updateProcessGroups).call(nf.CanvasUtils.position, transition);
            selection.exit().call(removeProcessGroups);
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
         * If the process group is currently unknown, this function reloads the canvas.
         *
         * @param {string} id The process group id
         */
        reload: function (id) {
            if (processGroupMap.has(id)) {
                var processGroupEntity = processGroupMap.get(id);
                return $.ajax({
                    type: 'GET',
                    url: processGroupEntity.uri,
                    dataType: 'json'
                }).done(function (response) {
                    nf.ProcessGroup.set(response);
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