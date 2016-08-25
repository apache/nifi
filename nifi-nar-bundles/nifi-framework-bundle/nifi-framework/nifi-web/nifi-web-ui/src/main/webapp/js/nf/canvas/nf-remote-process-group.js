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

nf.RemoteProcessGroup = (function () {

    var PREVIEW_NAME_LENGTH = 30;

    var dimensions = {
        width: 380,
        height: 158
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
            return d.id;
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
                    return 'id-' + d.id;
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

        // remote process group name background
        remoteProcessGroup.append('rect')
            .attr({
                'width': function (d) {
                    return d.dimensions.width;
                },
                'height': 32,
                'fill': '#b8c6cd'
            });

        // remote process group name
        remoteProcessGroup.append('text')
            .attr({
                'x': 30,
                'y': 20,
                'width': 305,
                'height': 16,
                'class': 'remote-process-group-name'
            });

        // remote process group icon
        remoteProcessGroup.append('image')
            .call(nf.CanvasUtils.disableImageHref)
            .attr({
                'width': 352,
                'height': 89,
                'x': 6,
                'y': 38,
                'class': 'remote-process-group-preview'
            });

        // always support selection
        remoteProcessGroup.call(nf.Selectable.activate).call(nf.ContextMenu.activate);
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
            nf.CanvasUtils.editable(remoteProcessGroup);

            // if this processor is visible, render everything
            if (remoteProcessGroup.classed('visible')) {
                if (details.empty()) {
                    details = remoteProcessGroup.append('g').attr('class', 'remote-process-group-details');

                    // remote process group transmission status
                    details.append('text')
                        .attr({
                            'class': 'remote-process-group-transmission-status',
                            'x': 10,
                            'y': 20
                        });

                    // ------------------
                    // details background
                    // ------------------

                    details.append('rect')
                        .attr({
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
                        .attr({
                            'class': 'remote-process-group-transmission-secure',
                            'x': 10,
                            'y': 48
                        });

                    // remote process group uri
                    details.append('text')
                        .attr({
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
                        .attr({
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
                        .attr({
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
                        .attr({
                            'width': function () {
                                return remoteProcessGroupData.dimensions.width;
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
                                return remoteProcessGroupData.dimensions.width;
                            },
                            'height': 1,
                            'x': 0,
                            'y': 103,
                            'fill': '#c7d2d7'
                        });

                    // -----
                    // stats
                    // -----

                    // stats label container
                    var remoteProcessGroupStatsLabel = details.append('g')
                        .attr({
                            'transform': 'translate(6, 75)'
                        });

                    // sent label
                    remoteProcessGroupStatsLabel.append('text')
                        .attr({
                            'width': 73,
                            'height': 10,
                            'x': 4,
                            'y': 5,
                            'class': 'stats-label'
                        })
                        .text('Sent');

                    // received label
                    remoteProcessGroupStatsLabel.append('text')
                        .attr({
                            'width': 73,
                            'height': 10,
                            'x': 4,
                            'y': 23,
                            'class': 'stats-label'
                        })
                        .text('Received');

                    // stats value container
                    var remoteProcessGroupStatsValue = details.append('g')
                        .attr({
                            'transform': 'translate(95, 75)'
                        });

                    // sent value
                    var sentText = remoteProcessGroupStatsValue.append('text')
                        .attr({
                            'width': 180,
                            'height': 10,
                            'x': 4,
                            'y': 5,
                            'class': 'remote-process-group-sent stats-value'
                        });

                    // sent count
                    sentText.append('tspan')
                        .attr({
                            'class': 'count'
                        });

                    // sent size
                    sentText.append('tspan')
                        .attr({
                            'class': 'size'
                        });

                    // sent ports
                    sentText.append('tspan')
                        .attr({
                            'class': 'ports'
                        });

                    // received value
                    var receivedText = remoteProcessGroupStatsValue.append('text')
                        .attr({
                            'width': 180,
                            'height': 10,
                            'x': 4,
                            'y': 23,
                            'class': 'remote-process-group-received stats-value'
                        });

                    // received ports
                    receivedText.append('tspan')
                        .attr({
                            'class': 'ports'
                        });

                    // received count
                    receivedText.append('tspan')
                        .attr({
                            'class': 'count'
                        });

                    // received size
                    receivedText.append('tspan')
                        .attr({
                            'class': 'size'
                        });

                    // stats value container
                    var processGroupStatsInfo = details.append('g')
                        .attr({
                            'transform': 'translate(335, 75)'
                        });

                    // sent info
                    processGroupStatsInfo.append('text')
                        .attr({
                            'width': 25,
                            'height': 10,
                            'x': 4,
                            'y': 5,
                            'class': 'stats-info'
                        })
                        .text('5 min');

                    // received info
                    processGroupStatsInfo.append('text')
                        .attr({
                            'width': 25,
                            'height': 10,
                            'x': 4,
                            'y': 23,
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
                            'y': 121,
                            'width': 342,
                            'height': 22,
                            'class': 'remote-process-group-comments'
                        });

                    // -------------------
                    // last refreshed time
                    // -------------------

                    details.append('rect')
                        .attr({
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
                        .attr({
                            'x': 370,
                            'y': 150,
                            'class': 'remote-process-group-last-refresh'
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
                                return remoteProcessGroupData.dimensions.width - 24;
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
                                return remoteProcessGroupData.dimensions.width - 17;
                            },
                            'y': 50
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
                            nf.CanvasUtils.ellipsis(remoteProcessGroupUri, d.component.targetUri);
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
                            nf.CanvasUtils.canvasTooltip(tip, d3.select(this));
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
                            nf.CanvasUtils.ellipsis(remoteProcessGroupComments, getProcessGroupComments(d));
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
                } else {
                    // clear the target uri
                    details.select('text.remote-process-group-uri').text(null);

                    // clear the transmission secure icon
                    details.select('text.remote-process-group-transmission-secure').text(null);

                    // clear the process group comments
                    details.select('text.remote-process-group-comments').text(null);

                    // clear the last refresh
                    details.select('text.remote-process-group-last-refresh').text(null);

                    // clear the name
                    remoteProcessGroup.select('text.remote-process-group-name').text(null);
                }

                // show the preview
                remoteProcessGroup.select('image.remote-process-group-preview').style('display', 'none');

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
                }

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

        // sent count value
        updated.select('text.remote-process-group-sent tspan.count')
            .text(function (d) {
                return nf.Common.substringBeforeFirst(d.status.aggregateSnapshot.sent, ' ');
            });

        // sent size value
        updated.select('text.remote-process-group-sent tspan.size')
            .text(function (d) {
                return ' ' + nf.Common.substringAfterFirst(d.status.aggregateSnapshot.sent, ' ');
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
                return nf.Common.substringBeforeFirst(d.status.aggregateSnapshot.received, ' ');
            });

        // received size value
        updated.select('text.remote-process-group-received tspan.size')
            .text(function (d) {
                return ' ' + nf.Common.substringAfterFirst(d.status.aggregateSnapshot.received, ' ');
            });

        // --------------------
        // authorization issues
        // --------------------

        // TODO - only consider state from the status
        // update the process groups transmission status
        updated.select('text.remote-process-group-transmission-status')
            .text(function (d) {
                var icon = '';
                if (d.permissions.canRead) {
                    if (!nf.Common.isEmpty(d.component.authorizationIssues)) {
                        icon = '\uf071';
                    } else if (d.component.transmitting === true) {
                        icon = '\uf140';
                    } else {
                        icon = '\ue80a';
                    }
                }
                return icon;
            })
            .attr('font-family', function (d) {
                var family = '';
                if (d.permissions.canRead) {
                    if (!nf.Common.isEmpty(d.component.authorizationIssues) || d.component.transmitting) {
                        family = 'FontAwesome';
                    } else {
                        family = 'flowfont';
                    }
                }
                return family;
            })
            .classed('has-authorization-errors', function (d) {
                return d.permissions.canRead && !nf.Common.isEmpty(d.component.authorizationIssues);
            })
            .each(function (d) {
                // get the tip
                var tip = d3.select('#authorization-issues-' + d.id);

                // if there are validation errors generate a tooltip
                if (d.permissions.canRead && !nf.Common.isEmpty(d.component.authorizationIssues)) {
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
                            var list = nf.Common.formatUnorderedList(d.component.authorizationIssues);
                            if (list === null || list.length === 0) {
                                return '';
                            } else {
                                return $('<div></div>').append(list).html();
                            }
                        });

                    // add the tooltip
                    nf.CanvasUtils.canvasTooltip(tip, d3.select(this));
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

            nf.CanvasUtils.activeThreadCount(remoteProcessGroup, d, function (off) {
                offset = off;
            });

            // ---------
            // bulletins
            // ---------

            remoteProcessGroup.select('rect.bulletin-background').classed('has-bulletins', function () {
                return !nf.Common.isEmpty(d.status.aggregateSnapshot.bulletins);
            });

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
            $('#bulletin-tip-' + d.id).remove();
            $('#authorization-issues-' + d.id).remove();
            $('#transmission-secure-' + d.id).remove();
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
         * Adds the specified remote process group entity.
         *
         * @param remoteProcessGroupEntities       The remote process group
         * @param options           Configuration options
         */
        add: function (remoteProcessGroupEntities, options) {
            var selectAll = false;
            if (nf.Common.isDefinedAndNotNull(options)) {
                selectAll = nf.Common.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
            }

            var add = function (remoteProcessGroupEntity) {
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
            } else if (nf.Common.isDefinedAndNotNull(remoteProcessGroupEntities)) {
                add(remoteProcessGroupEntities);
            }

            // apply the selection and handle new remote process groups
            var selection = select();
            selection.enter().call(renderRemoteProcessGroups, selectAll);
            selection.call(updateRemoteProcessGroups);
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
            if (nf.Common.isDefinedAndNotNull(options)) {
                selectAll = nf.Common.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
                transition = nf.Common.isDefinedAndNotNull(options.transition) ? options.transition : transition;
            }

            var set = function (remoteProcessGroupEntity) {
                // add the remote process group
                remoteProcessGroupMap.set(remoteProcessGroupEntity.id, $.extend({
                    type: 'RemoteProcessGroup',
                    dimensions: dimensions
                }, remoteProcessGroupEntity));
            };

            // determine how to handle the specified remote process groups
            if ($.isArray(remoteProcessGroupEntities)) {
                $.each(remoteProcessGroupMap.keys(), function (_, key) {
                    remoteProcessGroupMap.remove(key);
                });
                $.each(remoteProcessGroupEntities, function (_, remoteProcessGroupEntity) {
                    set(remoteProcessGroupEntity);
                });
            } else if (nf.Common.isDefinedAndNotNull(remoteProcessGroupEntities)) {
                set(remoteProcessGroupEntities);
            }

            // apply the selection and handle all new remote process groups
            var selection = select();
            selection.enter().call(renderRemoteProcessGroups, selectAll);
            selection.call(updateRemoteProcessGroups).call(nf.CanvasUtils.position, transition);
            selection.exit().call(removeRemoteProcessGroups);
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
                    nf.RemoteProcessGroup.set(response);

                    // reload the group's connections
                    var connections = nf.Connection.getComponentConnections(id);
                    $.each(connections, function (_, connection) {
                        if (connection.permissions.canRead) {
                            nf.Connection.reload(connection.id);
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
            d3.select('#id-' + id).call(nf.CanvasUtils.position);
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