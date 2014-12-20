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
nf.RemoteProcessGroup = (function () {

    var PREVIEW_NAME_LENGTH = 30;

    var dimensions = {
        width: 365,
        height: 140
    };

    // --------------------------------------------
    // remote process groups currently on the graph
    // --------------------------------------------

    var remoteProcessGroupMap;

    // --------------------
    // component containers
    // --------------------

    var remoteProcessGroupContainer;

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
     * Selects the remote process group elements against the current remote process group map.
     */
    var select = function () {
        return remoteProcessGroupContainer.selectAll('g.remote-process-group').data(remoteProcessGroupMap.values(), function (d) {
            return d.component.id;
        });
    };

    /**
     * Renders the remote process groups in the specified selection.
     *
     * @param {selection} entered           The selection of remote process groups to be rendered
     * @param {boolean} selected            Whether the remote process group is selected
     */
    var renderRemoteProcessGroups = function (entered, selected) {
        if (entered.empty()) {
            return;
        }

        var remoteProcessGroup = entered.append('g')
                .attr({
                    'id': function (d) {
                        return 'id-' + d.component.id;
                    },
                    'class': 'remote-process-group component'
                })
                .classed('selected', selected)
                .call(nf.CanvasUtils.position);

        // ----
        // body
        // ----

        // remote process group border
        remoteProcessGroup.append('rect')
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

        // remote process group body
        remoteProcessGroup.append('rect')
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

        // remote process group name
        remoteProcessGroup.append('text')
                .attr({
                    'x': 25,
                    'y': 17,
                    'width': 305,
                    'height': 16,
                    'font-size': '10pt',
                    'font-weight': 'bold',
                    'fill': '#ffffff',
                    'class': 'remote-process-group-name'
                });

        // process group icon
        remoteProcessGroup.append('image')
                .call(nf.CanvasUtils.disableImageHref)
                .attr({
                    'xlink:href': 'images/bgRemoteProcessGroupDetailsArea.png',
                    'width': 352,
                    'height': 89,
                    'x': 6,
                    'y': 38,
                    'class': 'remote-process-group-preview'
                });

        // always support selection
        remoteProcessGroup.call(nf.Selectable.activate).call(nf.ContextMenu.activate);

        // only support dragging and connecting when appropriate
        if (nf.Common.isDFM()) {
            remoteProcessGroup.call(nf.Draggable.activate).call(nf.Connectable.activate);
        }

        // call update to trigger some rendering
        remoteProcessGroup.call(updateRemoteProcessGroups);
    };

    // attempt of space between component count and icon for process group contents
    var CONTENTS_SPACER = 5;

    /**
     * Updates the process groups in the specified selection.
     *
     * @param {selection} updated               The process groups to be updated
     */
    var updateRemoteProcessGroups = function (updated) {
        if (updated.empty()) {
            return;
        }

        updated.each(function () {
            var remoteProcessGroup = d3.select(this);
            var details = remoteProcessGroup.select('g.remote-process-group-details');

            // if this processor is visible, render everything
            if (remoteProcessGroup.classed('visible')) {
                if (details.empty()) {
                    details = remoteProcessGroup.append('g').attr('class', 'remote-process-group-details');

                    // remote process group transmission status
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'class': 'remote-process-group-transmission-status',
                                'width': 16,
                                'height': 16,
                                'x': 5,
                                'y': 5
                            });

                    // remote process group secure transfer
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'class': 'remote-process-group-transmission-secure',
                                'width': 14,
                                'height': 12,
                                'x': 7,
                                'y': 23
                            });

                    // remote process group uri
                    details.append('text')
                            .attr({
                                'x': 25,
                                'y': 32,
                                'width': 305,
                                'height': 12,
                                'font-size': '8pt',
                                'fill': '#91b9ce',
                                'class': 'remote-process-group-uri'
                            })
                            .each(function (d) {
                                var remoteProcessGroupUri = d3.select(this);

                                // reset the remote process group name to handle any previous state
                                remoteProcessGroupUri.text(null).selectAll('title').remove();

                                // apply ellipsis to the remote process group name as necessary
                                nf.CanvasUtils.ellipsis(remoteProcessGroupUri, d.component.targetUri);
                            }).append('title').text(function (d) {
                        return d.component.name;
                    });

                    // ----------------
                    // stats background
                    // ----------------

                    details.append('rect')
                            .attr({
                                'x': 6,
                                'y': 38,
                                'width': 352,
                                'height': 89,
                                'stroke-width': 1,
                                'stroke': '#6f97ac',
                                'fill': '#ffffff'
                            });

                    details.append('rect')
                            .attr({
                                'x': 6,
                                'y': 38,
                                'width': 176,
                                'height': 22,
                                'stroke-width': 1,
                                'stroke': '#6f97ac',
                                'fill': 'url(#process-group-stats-background)',
                                'class': 'remote-process-group-input-container'
                            });

                    details.append('rect')
                            .attr({
                                'x': 182,
                                'y': 38,
                                'width': 176,
                                'height': 22,
                                'stroke-width': 1,
                                'stroke': '#6f97ac',
                                'fill': 'url(#process-group-stats-background)',
                                'class': 'remote-process-group-output-container'
                            });

                    details.append('rect')
                            .attr({
                                'x': 6,
                                'y': 94,
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
                                'y': 41
                            });

                    // input ports count
                    details.append('text')
                            .attr({
                                'x': 30,
                                'y': 53,
                                'class': 'remote-process-group-input-port-count process-group-contents-count'
                            });

                    // input transmitting icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'xlink:href': 'images/iconTransmissionActive.png',
                                'width': 16,
                                'height': 16,
                                'y': 41,
                                'class': 'remote-process-group-input-transmitting'
                            });

                    // input transmitting count
                    details.append('text')
                            .attr({
                                'y': 53,
                                'class': 'remote-process-group-input-transmitting-count process-group-contents-count'
                            });

                    // input not transmitting icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'xlink:href': 'images/iconTransmissionInactive.png',
                                'width': 16,
                                'height': 16,
                                'y': 41,
                                'class': 'remote-process-group-input-not-transmitting'
                            });

                    // input not transmitting count
                    details.append('text')
                            .attr({
                                'y': 53,
                                'class': 'remote-process-group-input-not-transmitting-count process-group-contents-count'
                            });

                    // output ports icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'xlink:href': 'images/iconOutputPortSmall.png',
                                'width': 16,
                                'height': 16,
                                'x': 186,
                                'y': 41,
                                'class': 'remote-process-group-output-port'
                            });

                    // output ports count
                    details.append('text')
                            .attr({
                                'x': 206,
                                'y': 53,
                                'class': 'remote-process-group-output-port-count process-group-contents-count'
                            });

                    // output transmitting icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'xlink:href': 'images/iconTransmissionActive.png',
                                'width': 16,
                                'height': 16,
                                'y': 41,
                                'class': 'remote-process-group-output-transmitting'
                            });

                    // output transmitting count
                    details.append('text')
                            .attr({
                                'y': 53,
                                'class': 'remote-process-group-output-transmitting-count process-group-contents-count'
                            });

                    // output not transmitting icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'xlink:href': 'images/iconTransmissionInactive.png',
                                'width': 16,
                                'height': 16,
                                'y': 41,
                                'class': 'remote-process-group-output-not-transmitting'
                            });

                    // output not transmitting count
                    details.append('text')
                            .attr({
                                'y': 53,
                                'class': 'remote-process-group-output-not-transmitting-count process-group-contents-count'
                            });

                    // -----
                    // stats
                    // -----

                    // stats label container
                    var remoteProcessGroupStatsLabel = details.append('g')
                            .attr({
                                'transform': 'translate(6, 70)'
                            });

                    // sent label
                    remoteProcessGroupStatsLabel.append('text')
                            .attr({
                                'width': 73,
                                'height': 10,
                                'x': 4,
                                'y': 4,
                                'class': 'process-group-stats-label'
                            })
                            .text('Sent');

                    // received label
                    remoteProcessGroupStatsLabel.append('text')
                            .attr({
                                'width': 73,
                                'height': 10,
                                'x': 4,
                                'y': 17,
                                'class': 'process-group-stats-label'
                            })
                            .text('Received');

                    // stats value container
                    var remoteProcessGroupStatsValue = details.append('g')
                            .attr({
                                'transform': 'translate(95, 70)'
                            });

                    // queued value
                    remoteProcessGroupStatsValue.append('text')
                            .attr({
                                'width': 180,
                                'height': 10,
                                'x': 4,
                                'y': 4,
                                'class': 'remote-process-group-sent process-group-stats-value'
                            });

                    // received value
                    remoteProcessGroupStatsValue.append('text')
                            .attr({
                                'width': 180,
                                'height': 10,
                                'x': 4,
                                'y': 17,
                                'class': 'remote-process-group-received process-group-stats-value'
                            });

                    // stats value container
                    var processGroupStatsInfo = details.append('g')
                            .attr({
                                'transform': 'translate(315, 70)'
                            });

                    // sent info
                    processGroupStatsInfo.append('text')
                            .attr({
                                'width': 25,
                                'height': 10,
                                'x': 4,
                                'y': 4,
                                'class': 'process-group-stats-info'
                            })
                            .text('(5 min)');

                    // received info
                    processGroupStatsInfo.append('text')
                            .attr({
                                'width': 25,
                                'height': 10,
                                'x': 4,
                                'y': 17,
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
                                'y': 108,
                                'width': 342,
                                'height': 22,
                                'class': 'remote-process-group-comments'
                            });

                    // -------------------
                    // last refreshed time
                    // -------------------

                    details.append('text')
                            .attr({
                                'x': 358,
                                'y': 137,
                                'class': 'remote-process-group-last-refresh'
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
                                'fill-opacity': '0.65',
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

                // update the process groups transmission status
                details.select('image.remote-process-group-transmission-status')
                        .attr('xlink:href', function (d) {
                            var img = '';
                            if (nf.Common.isDefinedAndNotNull(d.status) && !nf.Common.isEmpty(d.status.authorizationIssues)) {
                                img = 'images/iconAlert.png';
                            } else if (d.component.transmitting === true) {
                                img = 'images/iconTransmissionActive.png';
                            } else {
                                img = 'images/iconTransmissionInactive.png';
                            }
                            return img;
                        })
                        .each(function (d) {
                            // remove the existing tip if necessary
                            var tip = d3.select('#authorization-issues-' + d.component.id);
                            if (!tip.empty()) {
                                tip.remove();
                            }

                            // if there are validation errors generate a tooltip
                            if (nf.Common.isDefinedAndNotNull(d.status) && !nf.Common.isEmpty(d.status.authorizationIssues)) {
                                tip = d3.select('#remote-process-group-tooltips').append('div')
                                        .attr('id', function () {
                                            return 'authorization-issues-' + d.component.id;
                                        })
                                        .attr('class', 'tooltip')
                                        .html(function () {
                                            var list = nf.Common.formatUnorderedList(d.status.authorizationIssues);
                                            if (list === null || list.length === 0) {
                                                return '';
                                            } else {
                                                return $('<div></div>').append(list).html();
                                            }
                                        });

                                // add the tooltip
                                nf.CanvasUtils.canvasTooltip(tip, d3.select(this));
                            }
                        });

                // update the process groups transmission status
                details.select('image.remote-process-group-transmission-secure')
                        .attr('xlink:href', function (d) {
                            var img = '';
                            if (d.component.targetSecure === true) {
                                img = 'images/iconSecure.png';
                            } else {
                                img = 'images/iconNotSecure.png';
                            }
                            return img;
                        })
                        .each(function (d) {
                            // remove the existing tip if necessary
                            var tip = d3.select('#transmission-secure-' + d.component.id);
                            if (!tip.empty()) {
                                tip.remove();
                            }

                            tip = d3.select('#remote-process-group-tooltips').append('div')
                                    .attr('id', function () {
                                        return 'transmission-secure-' + d.component.id;
                                    })
                                    .attr('class', 'tooltip')
                                    .text(function () {
                                        if (d.component.targetSecure === true) {
                                            return 'Site-to-Site is secure.';
                                        } else {
                                            return 'Site-to-Site is NOT secure.';
                                        }
                                    });

                            // add the tooltip
                            nf.CanvasUtils.canvasTooltip(tip, d3.select(this));
                        });

                // ----------------------
                // update the input ports
                // ----------------------

                // input port count
                details.select('text.remote-process-group-input-port-count')
                        .text(function (d) {
                            return d.component.inputPortCount;
                        });

                // get the input port container to help right align
                var inputContainer = details.select('rect.remote-process-group-input-container');

                // update input not transmitting
                var inputNotTransmittingCount = details.select('text.remote-process-group-input-not-transmitting-count')
                        .text(function (d) {
                            return d.component.inactiveRemoteInputPortCount;
                        })
                        .attr('x', function () {
                            var containerX = parseInt(inputContainer.attr('x'), 10);
                            var containerWidth = parseInt(inputContainer.attr('width'), 10);
                            return containerX + containerWidth - this.getComputedTextLength() - CONTENTS_SPACER;
                        });
                var inputNotTransmitting = details.select('image.remote-process-group-input-not-transmitting')
                        .attr('x', function () {
                            var inputNotTransmittingCountX = parseInt(inputNotTransmittingCount.attr('x'), 10);
                            var width = parseInt(d3.select(this).attr('width'), 10);
                            return inputNotTransmittingCountX - width - CONTENTS_SPACER;
                        });

                // update input transmitting
                var inputTransmittingCount = details.select('text.remote-process-group-input-transmitting-count')
                        .text(function (d) {
                            return d.component.activeRemoteInputPortCount;
                        })
                        .attr('x', function () {
                            var inputNotTransmittingX = parseInt(inputNotTransmitting.attr('x'), 10);
                            return inputNotTransmittingX - this.getComputedTextLength() - CONTENTS_SPACER;
                        });
                details.select('image.remote-process-group-input-transmitting')
                        .attr('x', function () {
                            var inputTransmittingCountX = parseInt(inputTransmittingCount.attr('x'), 10);
                            var width = parseInt(d3.select(this).attr('width'), 10);
                            return inputTransmittingCountX - width - CONTENTS_SPACER;
                        });

                // -----------------------
                // update the output ports
                // -----------------------

                // output port count
                details.select('text.remote-process-group-output-port-count')
                        .text(function (d) {
                            return d.component.outputPortCount;
                        });

                // get the output port container to help right align
                var outputContainer = details.select('rect.remote-process-group-output-container');

                // update input not transmitting
                var outputNotTransmittingCount = details.select('text.remote-process-group-output-not-transmitting-count')
                        .text(function (d) {
                            return d.component.inactiveRemoteOutputPortCount;
                        })
                        .attr('x', function () {
                            var containerX = parseInt(outputContainer.attr('x'), 10);
                            var containerWidth = parseInt(outputContainer.attr('width'), 10);
                            return containerX + containerWidth - this.getComputedTextLength() - CONTENTS_SPACER;
                        });
                var outputNotTransmitting = details.select('image.remote-process-group-output-not-transmitting')
                        .attr('x', function () {
                            var outputNotTransmittingCountX = parseInt(outputNotTransmittingCount.attr('x'), 10);
                            var width = parseInt(d3.select(this).attr('width'), 10);
                            return outputNotTransmittingCountX - width - CONTENTS_SPACER;
                        });

                // update output transmitting
                var outputTransmittingCount = details.select('text.remote-process-group-output-transmitting-count')
                        .text(function (d) {
                            return d.component.activeRemoteOutputPortCount;
                        })
                        .attr('x', function () {
                            var outputNotTransmittingX = parseInt(outputNotTransmitting.attr('x'), 10);
                            return outputNotTransmittingX - this.getComputedTextLength() - CONTENTS_SPACER;
                        });
                details.select('image.remote-process-group-output-transmitting')
                        .attr('x', function () {
                            var outputTransmittingCountX = parseInt(outputTransmittingCount.attr('x'), 10);
                            var width = parseInt(d3.select(this).attr('width'), 10);
                            return outputTransmittingCountX - width - CONTENTS_SPACER;
                        });

                // ---------------
                // update comments
                // ---------------

                // update the process group comments
                details.select('text.remote-process-group-comments')
                        .each(function (d) {
                            var remoteProcessGroupComments = d3.select(this);

                            // reset the processor name to handle any previous state
                            remoteProcessGroupComments.text(null).selectAll('tspan, title').remove();

                            // apply ellipsis to the port name as necessary
                            nf.CanvasUtils.multilineEllipsis(remoteProcessGroupComments, 2, getProcessGroupComments(d));
                        }).classed('unset', function (d) {
                    return nf.Common.isBlank(d.component.comments);
                }).append('title').text(function (d) {
                    return getProcessGroupComments(d);
                });

                // --------------
                // last refreshed
                // --------------

                details.select('text.remote-process-group-last-refresh')
                        .text(function (d) {
                            if (nf.Common.isDefinedAndNotNull(d.component.flowRefreshed)) {
                                return d.component.flowRefreshed;
                            } else {
                                return 'Remote flow not current';
                            }
                        });

                // update the process group name
                remoteProcessGroup.select('text.remote-process-group-name')
                        .each(function (d) {
                            var remoteProcessGroupName = d3.select(this);

                            // reset the remote process group name to handle any previous state
                            remoteProcessGroupName.text(null).selectAll('title').remove();

                            // apply ellipsis to the remote process group name as necessary
                            nf.CanvasUtils.ellipsis(remoteProcessGroupName, d.component.name);
                        }).append('title').text(function (d) {
                    return d.component.name;
                });

                // show the preview
                remoteProcessGroup.select('image.remote-process-group-preview').style('display', 'none');

                // populate the stats
                remoteProcessGroup.call(updateProcessGroupStatus);
            } else {
                // update the process group name
                remoteProcessGroup.select('text.remote-process-group-name')
                        .text(function (d) {
                            var name = d.component.name;
                            if (name.length > PREVIEW_NAME_LENGTH) {
                                return name.substring(0, PREVIEW_NAME_LENGTH) + String.fromCharCode(8230);
                            } else {
                                return name;
                            }
                        });

                // show the preview
                remoteProcessGroup.select('image.remote-process-group-preview').style('display', 'block');

                // remove the tooltips
                remoteProcessGroup.call(removeTooltips);

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

        // sent value
        updated.select('text.remote-process-group-sent')
                .text(function (d) {
                    if (nf.Common.isDefinedAndNotNull(d.status)) {
                        return d.status.sent;
                    } else {
                        return '- / -';
                    }
                });

        // received value
        updated.select('text.remote-process-group-received')
                .text(function (d) {
                    if (nf.Common.isDefinedAndNotNull(d.status)) {
                        return d.status.received;
                    } else {
                        return '- / -';
                    }
                });

        updated.each(function (d) {
            var remoteProcessGroup = d3.select(this);
            var offset = 0;

            // -------------------
            // active thread count
            // -------------------            

            nf.CanvasUtils.activeThreadCount(remoteProcessGroup, d, function (off) {
                offset = off;
            });

            // ---------
            // bulletins
            // ---------

            nf.CanvasUtils.bulletins(remoteProcessGroup, d, function () {
                return d3.select('#remote-process-group-tooltips');
            }, offset);
        });
    };

    /**
     * Removes the remote process groups in the specified selection.
     *
     * @param {selection} removed               The process groups to be removed
     */
    var removeRemoteProcessGroups = function (removed) {
        if (removed.empty()) {
            return;
        }

        removed.call(removeTooltips).remove();
    };

    /**
     * Removes the tooltips for the remote process groups in the specified selection.
     * 
     * @param {type} removed
     */
    var removeTooltips = function (removed) {
        removed.each(function (d) {
            // remove any associated tooltips
            $('#bulletin-tip-' + d.component.id).remove();
            $('#authorization-issues-' + d.component.id).remove();
            $('#transmission-secure-' + d.component.id).remove();
        });
    };

    return {
        /**
         * Initializes of the Process Group handler.
         */
        init: function () {
            remoteProcessGroupMap = d3.map();

            // create the process group container
            remoteProcessGroupContainer = d3.select('#canvas').append('g')
                    .attr({
                        'pointer-events': 'all',
                        'class': 'remote-process-groups'
                    });
        },
        
        /**
         * Populates the graph with the specified remote process groups.
         *
         * @argument {object | array} remoteProcessGroups                   The remote process groups to add
         * @argument {boolean} selectAll                                    Whether or not to select the new contents
         */
        add: function (remoteProcessGroups, selectAll) {
            selectAll = nf.Common.isDefinedAndNotNull(selectAll) ? selectAll : false;

            var add = function (remoteProcessGroup) {
                // add the remote process group
                remoteProcessGroupMap.set(remoteProcessGroup.id, {
                    type: 'RemoteProcessGroup',
                    component: remoteProcessGroup,
                    dimensions: dimensions
                });
            };

            // determine how to handle the specified remote process groups
            if ($.isArray(remoteProcessGroups)) {
                $.each(remoteProcessGroups, function (_, remoteProcessGroup) {
                    add(remoteProcessGroup);
                });
            } else {
                add(remoteProcessGroups);
            }

            // apply the selection and handle all new remote process groups
            select().enter().call(renderRemoteProcessGroups, selectAll);
        },
        
        /**
         * If the remote process group id is specified it is returned. If no remote process 
         * group id specified, all remote process groups are returned.
         *
         * @param {string} id
         */
        get: function (id) {
            if (nf.Common.isUndefined(id)) {
                return remoteProcessGroupMap.values();
            } else {
                return remoteProcessGroupMap.get(id);
            }
        },
        
        /**
         * If the remote process group id is specified it is refresh according to the current
         * state. If no remote process group id is specified, all remote process groups are refreshed.
         *
         * @param {string} id      Optional
         */
        refresh: function (id) {
            if (nf.Common.isDefinedAndNotNull(id)) {
                d3.select('#id-' + id).call(updateRemoteProcessGroups);
            } else {
                d3.selectAll('g.remote-process-group').call(updateRemoteProcessGroups);
            }
        },
        
        /**
         * Refreshes the components necessary after a pan event.
         */
        pan: function () {
            d3.selectAll('g.remote-process-group.entering, g.remote-process-group.leaving').call(updateRemoteProcessGroups);
        },
        
        /**
         * Reloads the remote process group state from the server and refreshes the UI.
         * If the remote process group is currently unknown, this function just returns.
         *
         * @param {object} remoteProcessGroup       The remote process group to reload
         */
        reload: function (remoteProcessGroup) {
            if (remoteProcessGroupMap.has(remoteProcessGroup.id)) {
                return $.ajax({
                    type: 'GET',
                    url: remoteProcessGroup.uri,
                    dataType: 'json'
                }).done(function (response) {
                    nf.RemoteProcessGroup.set(response.remoteProcessGroup);

                    // reload the group's connections
                    var connections = nf.Connection.getComponentConnections(remoteProcessGroup.id);
                    $.each(connections, function (_, connection) {
                        nf.Connection.reload(connection);
                    });
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
         * Sets the specified remote process group(s). If the is an array, it
         * will set each remote process group. If it is not an array, it will
         * attempt to set the specified remote process group.
         *
         * @param {object | array} remoteProcessGroups
         */
        set: function (remoteProcessGroups) {
            var set = function (remoteProcessGroup) {
                if (remoteProcessGroupMap.has(remoteProcessGroup.id)) {
                    // update the current entry
                    var remoteProcessGroupEntry = remoteProcessGroupMap.get(remoteProcessGroup.id);
                    remoteProcessGroupEntry.component = remoteProcessGroup;

                    // update the remote process group in the UI
                    d3.select('#id-' + remoteProcessGroup.id).call(updateRemoteProcessGroups);
                }
            };

            // determine how to handle the specified remote process group
            if ($.isArray(remoteProcessGroups)) {
                $.each(remoteProcessGroups, function (_, remoteProcessGroup) {
                    set(remoteProcessGroup);
                });
            } else {
                set(remoteProcessGroups);
            }
        },
        
        /**
         * Sets the remote process group status using the specified status.
         * 
         * @param {array | object} remoteProcessGroupStatus       Remote process group status
         */
        setStatus: function (remoteProcessGroupStatus) {
            if (nf.Common.isEmpty(remoteProcessGroupStatus)) {
                return;
            }

            // update the specified process group status
            $.each(remoteProcessGroupStatus, function (_, status) {
                if (remoteProcessGroupMap.has(status.id)) {
                    var entry = remoteProcessGroupMap.get(status.id);
                    entry.status = status;
                }
            });

            // only update the visible components
            d3.selectAll('g.remote-process-group.visible').call(updateProcessGroupStatus);
        },
        
        /**
         * Removes the specified process group.
         *
         * @param {array|string} remoteProcessGroups      The remote process group id(s)
         */
        remove: function (remoteProcessGroups) {
            if ($.isArray(remoteProcessGroups)) {
                $.each(remoteProcessGroups, function (_, remoteProcessGroup) {
                    remoteProcessGroupMap.remove(remoteProcessGroup);
                });
            } else {
                remoteProcessGroupMap.remove(remoteProcessGroups);
            }

            // apply the selection and handle all removed remote process groups
            select().exit().call(removeRemoteProcessGroups);
        },
        
        /**
         * Removes all remote process groups.
         */
        removeAll: function () {
            nf.RemoteProcessGroup.remove(remoteProcessGroupMap.keys());
        }
    };
}());