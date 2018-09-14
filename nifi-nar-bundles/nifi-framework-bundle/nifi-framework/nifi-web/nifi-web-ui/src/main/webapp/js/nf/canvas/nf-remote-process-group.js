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
                'nf.Connection',
                'nf.Common',
                'nf.Client',
                'nf.CanvasUtils'],
            function ($, d3, nfConnection, nfCommon, nfClient, nfCanvasUtils) {
                return (nf.RemoteProcessGroup = factory($, d3, nfConnection, nfCommon, nfClient, nfCanvasUtils));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.RemoteProcessGroup =
            factory(require('jquery'),
                require('d3'),
                require('nf.Connection'),
                require('nf.Common'),
                require('nf.Client'),
                require('nf.CanvasUtils')));
    } else {
        nf.RemoteProcessGroup = factory(root.$,
            root.d3,
            root.nf.Connection,
            root.nf.Common,
            root.nf.Client,
            root.nf.CanvasUtils);
    }
}(this, function ($, d3, nfConnection, nfCommon, nfClient, nfCanvasUtils) {
    'use strict';

    var nfConnectable;
    var nfDraggable;
    var nfSelectable;
    var nfQuickSelect;
    var nfContextMenu;

    var PREVIEW_NAME_LENGTH = 30;

    var dimensions = {
        width: 380,
        height: 158
    };

    // --------------------------------------------
    // remote process groups currently on the graph
    // --------------------------------------------

    var remoteProcessGroupMap;

    // -----------------------------------------------------------
    // cache for components that are added/removed from the canvas
    // -----------------------------------------------------------

    var removedCache;
    var addedCache;

    // --------------------
    // component containers
    // --------------------

    var remoteProcessGroupContainer;

    // --------------------------
    // privately scoped functions
    // --------------------------

    /**
     * Selects the remote process group elements against the current remote process group map.
     */
    var select = function () {
        return remoteProcessGroupContainer.selectAll('g.remote-process-group').data(remoteProcessGroupMap.values(), function (d) {
            return d.id;
        });
    };

    /**
     * Renders the remote process groups in the specified selection.
     *
     * @param {selection} entered           The selection of remote process groups to be rendered
     * @param {boolean} selected            Whether the remote process group is selected
     * @return the entered selection
     */
    var renderRemoteProcessGroups = function (entered, selected) {
        if (entered.empty()) {
            return entered;
        }

        var remoteProcessGroup = entered.append('g')
            .attrs({
                'id': function (d) {
                    return 'id-' + d.id;
                },
                'class': 'remote-process-group component'
            })
            .classed('selected', selected)
            .call(nfCanvasUtils.position);

        // ----
        // body
        // ----

        // remote process group border
        remoteProcessGroup.append('rect')
            .attrs({
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

        // remote process group body
        remoteProcessGroup.append('rect')
            .attrs({
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

        // remote process group name background
        remoteProcessGroup.append('rect')
            .attrs({
                'width': function (d) {
                    return d.dimensions.width;
                },
                'height': 32,
                'fill': '#b8c6cd'
            });

        // remote process group name
        remoteProcessGroup.append('text')
            .attrs({
                'x': 30,
                'y': 20,
                'width': 305,
                'height': 16,
                'class': 'remote-process-group-name'
            });

        // always support selection
        remoteProcessGroup.call(nfSelectable.activate).call(nfContextMenu.activate).call(nfQuickSelect.activate);

        return remoteProcessGroup;
    };

    /**
     * Updates the process groups in the specified selection.
     *
     * @param {selection} updated               The process groups to be updated
     */
    var updateRemoteProcessGroups = function (updated) {
        if (updated.empty()) {
            return;
        }

        // remote process group border authorization
        updated.select('rect.border')
            .classed('unauthorized', function (d) {
                return d.permissions.canRead === false;
            });

        // remote process group body authorization
        updated.select('rect.body')
            .classed('unauthorized', function (d) {
                return d.permissions.canRead === false;
            });

        updated.each(function (remoteProcessGroupData) {
            var remoteProcessGroup = d3.select(this);
            var details = remoteProcessGroup.select('g.remote-process-group-details');

            // update the component behavior as appropriate
            nfCanvasUtils.editable(remoteProcessGroup, nfConnectable, nfDraggable);

            // if this processor is visible, render everything
            if (remoteProcessGroup.classed('visible')) {
                if (details.empty()) {
                    details = remoteProcessGroup.append('g').attr('class', 'remote-process-group-details');

                    // remote process group transmission status
                    details.append('text')
                        .attrs({
                            'class': 'remote-process-group-transmission-status',
                            'x': 10,
                            'y': 20
                        });

                    // ------------------
                    // details background
                    // ------------------

                    details.append('rect')
                        .attrs({
                            'x': 0,
                            'y': 32,
                            'width': function () {
                                return remoteProcessGroupData.dimensions.width
                            },
                            'height': 24,
                            'fill': '#e3e8eb'
                        });

                    // -------
                    // details
                    // -------

                    // remote process group secure transfer
                    details.append('text')
                        .attrs({
                            'class': 'remote-process-group-transmission-secure',
                            'x': 10,
                            'y': 48
                        });

                    // remote process group uri
                    details.append('text')
                        .attrs({
                            'x': 30,
                            'y': 48,
                            'width': 305,
                            'height': 12,
                            'class': 'remote-process-group-uri'
                        });

                    // ----------------
                    // stats background
                    // ----------------

                    // sent
                    details.append('rect')
                        .attrs({
                            'width': function () {
                                return remoteProcessGroupData.dimensions.width;
                            },
                            'height': 19,
                            'x': 0,
                            'y': 66,
                            'fill': '#f4f6f7'
                        });

                    // border
                    details.append('rect')
                        .attrs({
                            'width': function () {
                                return remoteProcessGroupData.dimensions.width;
                            },
                            'height': 1,
                            'x': 0,
                            'y': 84,
                            'fill': '#c7d2d7'
                        });

                    // received
                    details.append('rect')
                        .attrs({
                            'width': function () {
                                return remoteProcessGroupData.dimensions.width;
                            },
                            'height': 19,
                            'x': 0,
                            'y': 85,
                            'fill': '#ffffff'
                        });

                    // -----
                    // stats
                    // -----

                    // stats label container
                    var remoteProcessGroupStatsLabel = details.append('g')
                        .attrs({
                            'transform': 'translate(6, 75)'
                        });

                    // sent label
                    remoteProcessGroupStatsLabel.append('text')
                        .attrs({
                            'width': 73,
                            'height': 10,
                            'x': 4,
                            'y': 5,
                            'class': 'stats-label'
                        })
                        .text('Sent');

                    // received label
                    remoteProcessGroupStatsLabel.append('text')
                        .attrs({
                            'width': 73,
                            'height': 10,
                            'x': 4,
                            'y': 23,
                            'class': 'stats-label'
                        })
                        .text('Received');

                    // stats value container
                    var remoteProcessGroupStatsValue = details.append('g')
                        .attrs({
                            'transform': 'translate(95, 75)'
                        });

                    // sent value
                    var sentText = remoteProcessGroupStatsValue.append('text')
                        .attrs({
                            'width': 180,
                            'height': 10,
                            'x': 4,
                            'y': 5,
                            'class': 'remote-process-group-sent stats-value'
                        });

                    // sent count
                    sentText.append('tspan')
                        .attrs({
                            'class': 'count'
                        });

                    // sent size
                    sentText.append('tspan')
                        .attrs({
                            'class': 'size'
                        });

                    // sent ports
                    sentText.append('tspan')
                        .attrs({
                            'class': 'ports'
                        });

                    // received value
                    var receivedText = remoteProcessGroupStatsValue.append('text')
                        .attrs({
                            'width': 180,
                            'height': 10,
                            'x': 4,
                            'y': 23,
                            'class': 'remote-process-group-received stats-value'
                        });

                    // received ports
                    receivedText.append('tspan')
                        .attrs({
                            'class': 'ports'
                        });

                    // received count
                    receivedText.append('tspan')
                        .attrs({
                            'class': 'count'
                        });

                    // received size
                    receivedText.append('tspan')
                        .attrs({
                            'class': 'size'
                        });

                    // stats value container
                    var processGroupStatsInfo = details.append('g')
                        .attrs({
                            'transform': 'translate(335, 75)'
                        });

                    // sent info
                    processGroupStatsInfo.append('text')
                        .attrs({
                            'width': 25,
                            'height': 10,
                            'x': 4,
                            'y': 5,
                            'class': 'stats-info'
                        })
                        .text('5 min');

                    // received info
                    processGroupStatsInfo.append('text')
                        .attrs({
                            'width': 25,
                            'height': 10,
                            'x': 4,
                            'y': 23,
                            'class': 'stats-info'
                        })
                        .text('5 min');

                    // -------------------
                    // last refreshed time
                    // -------------------

                    details.append('rect')
                        .attrs({
                            'x': 0,
                            'y': function () {
                                return remoteProcessGroupData.dimensions.height - 24;
                            },
                            'width': function () {
                                return remoteProcessGroupData.dimensions.width;
                            },
                            'height': 24,
                            'fill': '#e3e8eb'
                        });

                    details.append('text')
                        .attrs({
                            'x': 10,
                            'y': 150,
                            'class': 'remote-process-group-last-refresh'
                        });

                    // --------
                    // comments
                    // --------

                    details.append('path')
                        .attrs({
                            'class': 'component-comments',
                            'transform': 'translate(' + (remoteProcessGroupData.dimensions.width - 2) + ', ' + (remoteProcessGroupData.dimensions.height - 10) + ')',
                            'd': 'm0,0 l0,8 l-8,0 z'
                        });

                    // -------------------
                    // active thread count
                    // -------------------

                    // active thread count
                    details.append('text')
                        .attrs({
                            'class': 'active-thread-count-icon',
                            'y': 20
                        })
                        .text('\ue83f');

                    // active thread icon
                    details.append('text')
                        .attrs({
                            'class': 'active-thread-count',
                            'y': 20
                        });

                    // ---------
                    // bulletins
                    // ---------

                    // bulletin background
                    details.append('rect')
                        .attrs({
                            'class': 'bulletin-background',
                            'x': function () {
                                return remoteProcessGroupData.dimensions.width - 24;
                            },
                            'y': 32,
                            'width': 24,
                            'height': 24
                        });

                    // bulletin icon
                    details.append('text')
                        .attrs({
                            'class': 'bulletin-icon',
                            'x': function () {
                                return remoteProcessGroupData.dimensions.width - 17;
                            },
                            'y': 49
                        })
                        .text('\uf24a');
                }

                if (remoteProcessGroupData.permissions.canRead) {
                    // remote process group uri
                    details.select('text.remote-process-group-uri')
                        .each(function (d) {
                            var remoteProcessGroupUri = d3.select(this);

                            // reset the remote process group name to handle any previous state
                            remoteProcessGroupUri.text(null).selectAll('title').remove();

                            // apply ellipsis to the remote process group name as necessary
                            nfCanvasUtils.ellipsis(remoteProcessGroupUri, d.component.targetUris);
                        }).append('title').text(function (d) {
                        return d.component.name;
                    });

                    // update the process groups transmission status
                    details.select('text.remote-process-group-transmission-secure')
                        .text(function (d) {
                            var icon = '';
                            if (d.component.targetSecure === true) {
                                icon = '\uf023';
                            } else {
                                icon = '\uf09c';
                            }
                            return icon;
                        })
                        .each(function (d) {
                            // get the tip
                            var tip = d3.select('#transmission-secure-' + d.id);

                            // remove the tip if necessary
                            if (tip.empty()) {
                                tip = d3.select('#remote-process-group-tooltips').append('div')
                                    .attr('id', function () {
                                        return 'transmission-secure-' + d.id;
                                    })
                                    .attr('class', 'tooltip nifi-tooltip');
                            }

                            // update the tip
                            tip.text(function () {
                                if (d.component.targetSecure === true) {
                                    return 'Site-to-Site is secure.';
                                } else {
                                    return 'Site-to-Site is NOT secure.';
                                }
                            });

                            // add the tooltip
                            nfCanvasUtils.canvasTooltip(tip, d3.select(this));
                        });

                    // ---------------
                    // update comments
                    // ---------------

                    // update the remote process group comments
                    details.select('path.component-comments')
                        .style('visibility', nfCommon.isBlank(remoteProcessGroupData.component.comments) ? 'hidden' : 'visible')
                        .each(function () {
                            // get the tip
                            var tip = d3.select('#comments-tip-' + remoteProcessGroupData.id);

                            // if there are validation errors generate a tooltip
                            if (nfCommon.isBlank(remoteProcessGroupData.component.comments)) {
                                // remove the tip if necessary
                                if (!tip.empty()) {
                                    tip.remove();
                                }
                            } else {
                                // create the tip if necessary
                                if (tip.empty()) {
                                    tip = d3.select('#remote-process-group-tooltips').append('div')
                                        .attr('id', function () {
                                            return 'comments-tip-' + remoteProcessGroupData.id;
                                        })
                                        .attr('class', 'tooltip nifi-tooltip');
                                }

                                // update the tip
                                tip.text(remoteProcessGroupData.component.comments);

                                // add the tooltip
                                nfCanvasUtils.canvasTooltip(tip, d3.select(this));
                            }
                        });

                    // --------------
                    // last refreshed
                    // --------------

                    details.select('text.remote-process-group-last-refresh')
                        .text(function (d) {
                            if (nfCommon.isDefinedAndNotNull(d.component.flowRefreshed)) {
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
                            nfCanvasUtils.ellipsis(remoteProcessGroupName, d.component.name);
                        }).append('title').text(function (d) {
                        return d.component.name;
                    });
                } else {
                    // clear the target uri
                    details.select('text.remote-process-group-uri').text(null);

                    // clear the transmission secure icon
                    details.select('text.remote-process-group-transmission-secure').text(null);

                    // clear the comments
                    details.select('path.component-comments').style('visibility', 'hidden');

                    // clear the last refresh
                    details.select('text.remote-process-group-last-refresh').text(null);

                    // clear the name
                    remoteProcessGroup.select('text.remote-process-group-name').text(null);

                    // clear tooltips
                    remoteProcessGroup.call(removeTooltips);
                }

                // populate the stats
                remoteProcessGroup.call(updateProcessGroupStatus);
            } else {
                if (remoteProcessGroupData.permissions.canRead) {
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
                } else {
                    // clear the name
                    remoteProcessGroup.select('text.remote-process-group-name').text(null);
                }

                // remove the tooltips
                remoteProcessGroup.call(removeTooltips);

                // remove the details if necessary
                if (!details.empty()) {
                    details.remove();
                }
            }
        });
    };

    var hasIssues = function (d) {
        return d.status.validationStatus === 'INVALID';
    };

    var getIssues = function (d) {
        var issues = [];
        if (!nfCommon.isEmpty(d.component.authorizationIssues)) {
            issues = issues.concat(d.component.authorizationIssues);
        }
        if (!nfCommon.isEmpty(d.component.validationErrors)) {
            issues = issues.concat(d.component.validationErrors);
        }
        return issues;
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

        // sent count value
        updated.select('text.remote-process-group-sent tspan.count')
            .text(function (d) {
                return nfCommon.substringBeforeFirst(d.status.aggregateSnapshot.sent, ' ');
            });

        // sent size value
        updated.select('text.remote-process-group-sent tspan.size')
            .text(function (d) {
                return ' ' + nfCommon.substringAfterFirst(d.status.aggregateSnapshot.sent, ' ');
            });

        // sent ports value
        updated.select('text.remote-process-group-sent tspan.ports')
            .text(function (d) {
                return ' ' + String.fromCharCode(8594) + ' ' + d.inputPortCount;
            });

        // received ports value
        updated.select('text.remote-process-group-received tspan.ports')
            .text(function (d) {
                return d.outputPortCount + ' ' + String.fromCharCode(8594) + ' ';
            });

        // received count value
        updated.select('text.remote-process-group-received tspan.count')
            .text(function (d) {
                return nfCommon.substringBeforeFirst(d.status.aggregateSnapshot.received, ' ');
            });

        // received size value
        updated.select('text.remote-process-group-received tspan.size')
            .text(function (d) {
                return ' ' + nfCommon.substringAfterFirst(d.status.aggregateSnapshot.received, ' ');
            });

        // --------------------
        // authorization issues
        // --------------------

        // update the process groups transmission status
        updated.select('text.remote-process-group-transmission-status')
            .text(function (d) {
                var icon = '';
                if (hasIssues(d)) {
                    icon = '\uf071';
                } else if (d.status.transmissionStatus === 'Transmitting') {
                    icon = '\uf140';
                } else {
                    icon = '\ue80a';
                }
                return icon;
            })
            .attr('font-family', function (d) {
                var family = '';
                if (hasIssues(d) || d.status.transmissionStatus === 'Transmitting') {
                    family = 'FontAwesome';
                } else {
                    family = 'flowfont';
                }
                return family;
            })
            .classed('invalid', function (d) {
                return hasIssues(d);
            })
            .classed('transmitting', function (d) {
                return !hasIssues(d) && d.status.transmissionStatus === 'Transmitting';
            })
            .classed('not-transmitting', function (d) {
                return !hasIssues(d) && d.status.transmissionStatus !== 'Transmitting';
            })
            .each(function (d) {
                // get the tip
                var tip = d3.select('#authorization-issues-' + d.id);

                // if there are validation errors generate a tooltip
                if (d.permissions.canRead && hasIssues(d)) {
                    // create the tip if necessary
                    if (tip.empty()) {
                        tip = d3.select('#remote-process-group-tooltips').append('div')
                            .attr('id', function () {
                                return 'authorization-issues-' + d.id;
                            })
                            .attr('class', 'tooltip nifi-tooltip');
                    }

                    // update the tip
                    tip.html(function () {
                        var list = nfCommon.formatUnorderedList(getIssues(d));
                        if (list === null || list.length === 0) {
                            return '';
                        } else {
                            return $('<div></div>').append(list).html();
                        }
                    });

                    // add the tooltip
                    nfCanvasUtils.canvasTooltip(tip, d3.select(this));
                } else {
                    if (!tip.empty()) {
                        tip.remove();
                    }
                }
            });

        updated.each(function (d) {
            var remoteProcessGroup = d3.select(this);
            var offset = 0;

            // -------------------
            // active thread count
            // -------------------            

            nfCanvasUtils.activeThreadCount(remoteProcessGroup, d, function (off) {
                offset = off;
            });

            // ---------
            // bulletins
            // ---------

            remoteProcessGroup.select('rect.bulletin-background').classed('has-bulletins', function () {
                return !nfCommon.isEmpty(d.status.aggregateSnapshot.bulletins);
            });

            nfCanvasUtils.bulletins(remoteProcessGroup, d, function () {
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
            $('#bulletin-tip-' + d.id).remove();
            $('#authorization-issues-' + d.id).remove();
            $('#transmission-secure-' + d.id).remove();
            $('#comments-tip-' + d.id).remove();
        });
    };

    var nfRemoteProcessGroup = {
        /**
         * Initializes of the Process Group handler.
         *
         * @param nfConnectableRef   The nfConnectable module.
         * @param nfDraggableRef   The nfDraggable module.
         * @param nfSelectableRef   The nfSelectable module.
         * @param nfContextMenuRef   The nfContextMenu module.
         * @param nfQuickSelectRef   The nfQuickSelect module.
         */
        init: function (nfConnectableRef, nfDraggableRef, nfSelectableRef, nfContextMenuRef, nfQuickSelectRef) {
            nfConnectable = nfConnectableRef;
            nfDraggable = nfDraggableRef;
            nfSelectable = nfSelectableRef;
            nfContextMenu = nfContextMenuRef;
            nfQuickSelect = nfQuickSelectRef;

            remoteProcessGroupMap = d3.map();
            removedCache = d3.map();
            addedCache = d3.map();

            // create the process group container
            remoteProcessGroupContainer = d3.select('#canvas').append('g')
                .attrs({
                    'pointer-events': 'all',
                    'class': 'remote-process-groups'
                });
        },

        /**
         * Adds the specified remote process group entity.
         *
         * @param remoteProcessGroupEntities       The remote process group
         * @param options           Configuration options
         */
        add: function (remoteProcessGroupEntities, options) {
            var selectAll = false;
            if (nfCommon.isDefinedAndNotNull(options)) {
                selectAll = nfCommon.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
            }

            // get the current time
            var now = new Date().getTime();

            var add = function (remoteProcessGroupEntity) {
                addedCache.set(remoteProcessGroupEntity.id, now);

                // add the remote process group
                remoteProcessGroupMap.set(remoteProcessGroupEntity.id, $.extend({
                    type: 'RemoteProcessGroup',
                    dimensions: dimensions
                }, remoteProcessGroupEntity));
            };

            // determine how to handle the specified remote process groups
            if ($.isArray(remoteProcessGroupEntities)) {
                $.each(remoteProcessGroupEntities, function (_, remoteProcessGroupEntity) {
                    add(remoteProcessGroupEntity);
                });
            } else if (nfCommon.isDefinedAndNotNull(remoteProcessGroupEntities)) {
                add(remoteProcessGroupEntities);
            }

            // select
            var selection = select();

            // enter
            var entered = renderRemoteProcessGroups(selection.enter(), selectAll);

            // update
            updateRemoteProcessGroups(selection.merge(entered));
        },

        /**
         * Populates the graph with the specified remote process groups.
         *
         * @argument {object | array} remoteProcessGroupEntities                   The remote process groups to add
         * @argument {object} options                                    Configuration options
         */
        set: function (remoteProcessGroupEntities, options) {
            var selectAll = false;
            var transition = false;
            var overrideRevisionCheck = false;
            if (nfCommon.isDefinedAndNotNull(options)) {
                selectAll = nfCommon.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
                transition = nfCommon.isDefinedAndNotNull(options.transition) ? options.transition : transition;
                overrideRevisionCheck = nfCommon.isDefinedAndNotNull(options.overrideRevisionCheck) ? options.overrideRevisionCheck : overrideRevisionCheck;
            }

            var set = function (proposedRemoteProcessGroupEntity) {
                var currentRemoteProcessGroupEntity = remoteProcessGroupMap.get(proposedRemoteProcessGroupEntity.id);

                // set the remote process group if appropriate due to revision and wasn't previously removed
                if ((nfClient.isNewerRevision(currentRemoteProcessGroupEntity, proposedRemoteProcessGroupEntity) && !removedCache.has(proposedRemoteProcessGroupEntity.id)) || overrideRevisionCheck === true) {
                    remoteProcessGroupMap.set(proposedRemoteProcessGroupEntity.id, $.extend({
                        type: 'RemoteProcessGroup',
                        dimensions: dimensions
                    }, proposedRemoteProcessGroupEntity));
                }
            };

            // determine how to handle the specified remote process groups
            if ($.isArray(remoteProcessGroupEntities)) {
                $.each(remoteProcessGroupMap.keys(), function (_, key) {
                    var currentRemoteProcessGroupEntity = remoteProcessGroupMap.get(key);
                    var isPresent = $.grep(remoteProcessGroupEntities, function (proposedRemoteProcessGroupEntity) {
                        return proposedRemoteProcessGroupEntity.id === currentRemoteProcessGroupEntity.id;
                    });

                    // if the current remote process group is not present and was not recently added, remove it
                    if (isPresent.length === 0 && !addedCache.has(key)) {
                        remoteProcessGroupMap.remove(key);
                    }
                });
                $.each(remoteProcessGroupEntities, function (_, remoteProcessGroupEntity) {
                    set(remoteProcessGroupEntity);
                });
            } else if (nfCommon.isDefinedAndNotNull(remoteProcessGroupEntities)) {
                set(remoteProcessGroupEntities);
            }

            // select
            var selection = select();

            // enter
            var entered = renderRemoteProcessGroups(selection.enter(), selectAll);

            // update
            var updated = selection.merge(entered);
            updated.call(updateRemoteProcessGroups).call(nfCanvasUtils.position, transition);

            // exit
            selection.exit().call(removeRemoteProcessGroups);
        },

        /**
         * If the remote process group id is specified it is returned. If no remote process
         * group id specified, all remote process groups are returned.
         *
         * @param {string} id
         */
        get: function (id) {
            if (nfCommon.isUndefined(id)) {
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
            if (nfCommon.isDefinedAndNotNull(id)) {
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
         * @param {string} id       The remote process group id
         */
        reload: function (id) {
            if (remoteProcessGroupMap.has(id)) {
                var remoteProcessGroupEntity = remoteProcessGroupMap.get(id);
                return $.ajax({
                    type: 'GET',
                    url: remoteProcessGroupEntity.uri,
                    dataType: 'json'
                }).done(function (response) {
                    nfRemoteProcessGroup.set(response);

                    // reload the group's connections
                    var connections = nfConnection.getComponentConnections(id);
                    $.each(connections, function (_, connection) {
                        if (connection.permissions.canRead) {
                            nfConnection.reload(connection.id);
                        }
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
            d3.select('#id-' + id).call(nfCanvasUtils.position);
        },

        /**
         * Removes the specified process group.
         *
         * @param {array|string} remoteProcessGroupIds      The remote process group id(s)
         */
        remove: function (remoteProcessGroupIds) {
            var now = new Date().getTime();

            if ($.isArray(remoteProcessGroupIds)) {
                $.each(remoteProcessGroupIds, function (_, remoteProcessGroupId) {
                    removedCache.set(remoteProcessGroupId, now);
                    remoteProcessGroupMap.remove(remoteProcessGroupId);
                });
            } else {
                removedCache.set(remoteProcessGroupIds, now);
                remoteProcessGroupMap.remove(remoteProcessGroupIds);
            }

            // apply the selection and handle all removed remote process groups
            select().exit().call(removeRemoteProcessGroups);
        },

        /**
         * Removes all remote process groups.
         */
        removeAll: function () {
            nfRemoteProcessGroup.remove(remoteProcessGroupMap.keys());
        },

        /**
         * Expires the caches up to the specified timestamp.
         *
         * @param timestamp
         */
        expireCaches: function (timestamp) {
            var expire = function (cache) {
                cache.each(function (entryTimestamp, id) {
                    if (timestamp > entryTimestamp) {
                        cache.remove(id);
                    }
                });
            };

            expire(addedCache);
            expire(removedCache);
        }
    };

    return nfRemoteProcessGroup;
}));