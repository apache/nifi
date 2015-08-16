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

nf.Port = (function () {

    var PREVIEW_NAME_LENGTH = 15;
    var OFFSET_VALUE = 12;

    var portDimensions = {
        width: 160,
        height: 40
    };
    var remotePortDimensions = {
        width: 160,
        height: 56
    };

    // ----------------------------
    // ports currently on the graph
    // ----------------------------

    var portMap;

    // --------------------
    // component containers
    // --------------------

    var portContainer;

    // --------------------------
    // privately scoped functions
    // --------------------------

    /**
     * Selects the port elements against the current port map.
     */
    var select = function () {
        return portContainer.selectAll('g.input-port, g.output-port').data(portMap.values(), function (d) {
            return d.component.id;
        });
    };

    /**
     * Renders the ports in the specified selection.
     *
     * @param {selection} entered           The selection of ports to be rendered
     * @param {boolean} selected            Whether the port should be selected
     */
    var renderPorts = function (entered, selected) {
        if (entered.empty()) {
            return;
        }

        var port = entered.append('g')
                .attr({
                    'id': function (d) {
                        return 'id-' + d.component.id;
                    },
                    'class': function (d) {
                        if (d.component.type === 'INPUT_PORT') {
                            return 'input-port component';
                        } else {
                            return 'output-port component';
                        }
                    }
                })
                .classed('selected', selected)
                .call(nf.CanvasUtils.position);

        // port border
        port.append('rect')
                .attr({
                    'class': 'border',
                    'width': function (d) {
                        return d.dimensions.width;
                    },
                    'height': function (d) {
                        return d.dimensions.height;
                    },
                    'fill': 'transparent',
                    'stroke-opacity': 0.8,
                    'stroke-width': 1,
                    'stroke': '#aaaaaa'
                });

        var offset = 0;

        // conditionally render the remote banner
        if (nf.Canvas.getParentGroupId() === null) {
            offset = OFFSET_VALUE;

            // port remote banner
            port.append('rect')
                    .attr({
                        'class': 'remote-banner',
                        'width': function (d) {
                            return d.dimensions.width;
                        },
                        'height': offset,
                        'fill': '#294c58',
                        'fill-opacity': 0.95
                    });
        }

        // port body
        port.append('rect')
                .attr({
                    'x': 0,
                    'y': offset,
                    'class': 'port-body',
                    'width': function (d) {
                        return d.dimensions.width;
                    },
                    'height': function (d) {
                        return d.dimensions.height - offset;
                    },
                    'fill': 'url(#port-background)',
                    'fill-opacity': 0.8,
                    'stroke-opacity': 0.8,
                    'stroke-width': 0,
                    'stroke': '#aaaaaa'
                });

        // port icon
        port.append('image')
                .call(nf.CanvasUtils.disableImageHref)
                .attr({
                    'xlink:href': function (d) {
                        if (d.component.type === 'INPUT_PORT') {
                            return 'images/iconInputPort.png';
                        } else {
                            return 'images/iconOutputPort.png';
                        }
                    },
                    'width': 46,
                    'height': 31,
                    'x': function (d) {
                        if (d.component.type === 'INPUT_PORT') {
                            return 0;
                        } else {
                            return 114;
                        }
                    },
                    'y': 5 + offset
                });

        // port name
        port.append('text')
                .attr({
                    'x': function (d) {
                        if (d.component.type === 'INPUT_PORT') {
                            return 52;
                        } else {
                            return 5;
                        }
                    },
                    'y': 18 + offset,
                    'width': 95,
                    'height': 30,
                    'font-size': '10px',
                    'font-weight': 'bold',
                    'fill': '#294c58',
                    'class': 'port-name'
                });

        // make ports selectable
        port.call(nf.Selectable.activate).call(nf.ContextMenu.activate);

        // only activate dragging and connecting if appropriate
        if (nf.Common.isDFM()) {
            port.call(nf.Draggable.activate).call(nf.Connectable.activate);
        }

        // call update to trigger some rendering
        port.call(updatePorts);
    };

    /**
     * Updates the ports in the specified selection.
     *
     * @param {selection} updated               The ports to be updated
     */
    var updatePorts = function (updated) {
        if (updated.empty()) {
            return;
        }

        updated.each(function () {
            var port = d3.select(this);
            var details = port.select('g.port-details');

            // if this process group is visible, render everything
            if (port.classed('visible')) {
                if (details.empty()) {
                    details = port.append('g').attr('class', 'port-details');

                    var offset = 0;
                    if (nf.Canvas.getParentGroupId() === null) {
                        offset = OFFSET_VALUE;

                        // port transmitting icon
                        details.append('image')
                                .call(nf.CanvasUtils.disableImageHref)
                                .attr({
                                    'class': 'port-transmission-icon',
                                    'width': 10,
                                    'height': 10,
                                    'x': 3,
                                    'y': 1
                                });

                        // bulletin icon
                        details.append('image')
                                .call(nf.CanvasUtils.disableImageHref)
                                .attr({
                                    'class': 'bulletin-icon',
                                    'xlink:href': 'images/iconBulletin.png',
                                    'width': 12,
                                    'height': 12,
                                    'x': 147,
                                    'y': 0
                                });
                    }

                    // run status icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'class': 'port-run-status-icon',
                                'width': 16,
                                'height': 16,
                                'x': function (d) {
                                    if (d.component.type === 'INPUT_PORT') {
                                        return 33;
                                    } else {
                                        return 107;
                                    }
                                },
                                'y': function () {
                                    return 24 + offset;
                                }
                            });

                    // active thread count
                    details.append('rect')
                            .attr({
                                'class': 'active-thread-count-background',
                                'height': 11,
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
                                'height': 11,
                                'y': 9,
                                'fill': '#000'
                            });
                }

                // update the run status
                details.select('image.port-run-status-icon')
                        .attr('xlink:href', function (d) {
                            var img = '';
                            if (d.component.state === 'DISABLED') {
                                img = 'images/iconDisable.png';
                            } else if (!nf.Common.isEmpty(d.component.validationErrors)) {
                                img = 'images/iconAlert.png';
                            } else if (d.component.state === 'RUNNING') {
                                img = 'images/iconRun.png';
                            } else if (d.component.state === 'STOPPED') {
                                img = 'images/iconStop.png';
                            }
                            return img;
                        })
                        .each(function (d) {
                            // remove the existing tip if necessary
                            var tip = d3.select('#run-status-tip-' + d.component.id);
                            if (!tip.empty()) {
                                tip.remove();
                            }

                            // if there are validation errors generate a tooltip
                            if (!nf.Common.isEmpty(d.component.validationErrors)) {
                                tip = d3.select('#port-tooltips').append('div')
                                        .attr('id', function () {
                                            return 'run-status-tip-' + d.component.id;
                                        })
                                        .attr('class', 'tooltip nifi-tooltip')
                                        .html(function () {
                                            var list = nf.Common.formatUnorderedList(d.component.validationErrors);
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

                // update the port name
                port.select('text.port-name')
                        .each(function (d) {
                            var portName = d3.select(this);
                            var name = d.component.name;
                            var words = name.split(/\s+/);

                            // reset the port name to handle any previous state
                            portName.text(null).selectAll('tspan, title').remove();

                            // handle based on the number of tokens in the port name
                            if (words.length === 1) {
                                // apply ellipsis to the port name as necessary
                                nf.CanvasUtils.ellipsis(portName, name);
                            } else {
                                nf.CanvasUtils.multilineEllipsis(portName, 2, name);
                            }
                        }).append('title').text(function (d) {
                    return d.component.name;
                });

                // populate the stats
                port.call(updatePortStatus);
            } else {
                // update the port name
                port.select('text.port-name')
                        .text(function (d) {
                            var name = d.component.name;
                            if (name.length > PREVIEW_NAME_LENGTH) {
                                return name.substring(0, PREVIEW_NAME_LENGTH) + String.fromCharCode(8230);
                            } else {
                                return name;
                            }
                        });

                // remove tooltips if necessary
                port.call(removeTooltips);

                // remove the details if necessary
                if (!details.empty()) {
                    details.remove();
                }
            }
        });
    };

    /**
     * Updates the port status.
     * 
     * @param {selection} updated           The ports to be updated
     */
    var updatePortStatus = function (updated) {
        if (updated.empty()) {
            return;
        }

        updated.select('image.port-transmission-icon')
                .attr('xlink:href', function (d) {
                    if (d.status.transmitting === true) {
                        return 'images/iconPortTransmitting.png';
                    } else {
                        return 'images/iconPortNotTransmitting.png';
                    }
                });

        updated.each(function (d) {
            var port = d3.select(this);
            var offset = 0;

            // -------------------
            // active thread count
            // -------------------

            nf.CanvasUtils.activeThreadCount(port, d, function (off) {
                offset = off;
            });

            // ---------
            // bulletins
            // ---------

            nf.CanvasUtils.bulletins(port, d, function () {
                return d3.select('#port-tooltips');
            }, offset);
        });
    };

    /**
     * Removes the ports in the specified selection.
     *
     * @param {selection} removed               The ports to be removed
     */
    var removePorts = function (removed) {
        if (removed.empty()) {
            return;
        }

        removed.call(removeTooltips).remove();
    };

    /**
     * Removes the tooltips for the ports in the specified selection.
     * 
     * @param {selection} removed
     */
    var removeTooltips = function (removed) {
        removed.each(function (d) {
            // remove any associated tooltips
            $('#run-status-tip-' + d.component.id).remove();
            $('#bulletin-tip-' + d.component.id).remove();
        });
    };

    return {
        /**
         * Initializes of the Port handler.
         */
        init: function () {
            portMap = d3.map();

            // create the port container
            portContainer = d3.select('#canvas').append('g')
                    .attr({
                        'pointer-events': 'all',
                        'class': 'ports'
                    });
        },
        
        /**
         * Populates the graph with the specified ports.
         *
         * @argument {object | array} ports                    The ports to add
         * @argument {boolean} selectAll                Whether or not to select the new contents
         */
        add: function (ports, selectAll) {
            selectAll = nf.Common.isDefinedAndNotNull(selectAll) ? selectAll : false;

            // determine the appropriate dimensions for this port
            var dimensions = portDimensions;
            if (nf.Canvas.getParentGroupId() === null) {
                dimensions = remotePortDimensions;
            }

            var add = function (ports) {
                // add the port
                portMap.set(ports.id, {
                    type: 'Port',
                    component: ports,
                    dimensions: dimensions,
                    status: {
                        activeThreadCount: 0
                    }
                });
            };

            // determine how to handle the specified port status
            if ($.isArray(ports)) {
                $.each(ports, function (_, port) {
                    add(port);
                });
            } else {
                add(ports);
            }

            // apply the selection and handle all new ports
            select().enter().call(renderPorts, selectAll);
        },
        
        /**
         * If the port id is specified it is returned. If no port id
         * specified, all ports are returned.
         *
         * @param {string} id
         */
        get: function (id) {
            if (nf.Common.isUndefined(id)) {
                return portMap.values();
            } else {
                return portMap.get(id);
            }
        },
        
        /**
         * If the port id is specified it is refresh according to the current
         * state. If not port id is specified, all ports are refreshed.
         *
         * @param {string} id      Optional
         */
        refresh: function (id) {
            if (nf.Common.isDefinedAndNotNull(id)) {
                d3.select('#id-' + id).call(updatePorts);
            } else {
                d3.selectAll('g.input-port, g.output-port').call(updatePorts);
            }
        },
        
        /**
         * Refreshes the components necessary after a pan event.
         */
        pan: function () {
            d3.selectAll('g.input-port.entering, g.output-port.entering, g.input-port.leaving, g.output-port.leaving').call(updatePorts);
        },
        
        /**
         * Reloads the port state from the server and refreshes the UI.
         * If the port is currently unknown, this function just returns.
         *
         * @param {object} port The port to reload
         */
        reload: function (port) {
            if (portMap.has(port.id)) {
                return $.ajax({
                    type: 'GET',
                    url: port.uri,
                    dataType: 'json'
                }).done(function (response) {
                    if (nf.Common.isDefinedAndNotNull(response.inputPort)) {
                        nf.Port.set(response.inputPort);
                    } else {
                        nf.Port.set(response.outputPort);
                    }
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
         * Sets the specified port(s). If the is an array, it
         * will set each port. If it is not an array, it will
         * attempt to set the specified port.
         *
         * @param {object | array} ports
         */
        set: function (ports) {
            var set = function (port) {
                if (portMap.has(port.id)) {
                    // update the current entry
                    var portEntry = portMap.get(port.id);
                    portEntry.component = port;

                    // update the connection in the UI
                    d3.select('#id-' + port.id).call(updatePorts);
                }
            };

            // determine how to handle the specified ports
            if ($.isArray(ports)) {
                $.each(ports, function (_, port) {
                    set(port);
                });
            } else {
                set(ports);
            }
        },
        
        /**
         * Sets the port status using the specified status.
         * 
         * @param {array} portStatus       Port status
         */
        setStatus: function (portStatus) {
            if (nf.Common.isEmpty(portStatus)) {
                return;
            }

            // update the specified port status
            $.each(portStatus, function (_, status) {
                if (portMap.has(status.id)) {
                    var port = portMap.get(status.id);
                    port.status = status;
                }
            });

            // update the visible ports
            d3.selectAll('g.input-port.visible, g.output-port.visible').call(updatePortStatus);
        },
        
        /**
         * Removes the specified port.
         *
         * @param {string} ports      The port id(s)
         */
        remove: function (ports) {
            if ($.isArray(ports)) {
                $.each(ports, function (_, port) {
                    portMap.remove(port);
                });
            } else {
                portMap.remove(ports);
            }

            // apply the selection and handle all removed ports
            select().exit().call(removePorts);
        },
        
        /**
         * Removes all ports..
         */
        removeAll: function () {
            nf.Port.remove(portMap.keys());
        }
    };
}());