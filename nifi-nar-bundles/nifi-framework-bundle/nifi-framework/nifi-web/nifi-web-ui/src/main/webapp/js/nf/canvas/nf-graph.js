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
                'nf.Common',
                'nf.ng.Bridge',
                'nf.Label',
                'nf.Funnel',
                'nf.Port',
                'nf.RemoteProcessGroup',
                'nf.ProcessGroup',
                'nf.Processor',
                'nf.Connection',
                'nf.ConnectionConfiguration',
                'nf.CanvasUtils',
                'nf.Connectable',
                'nf.Draggable',
                'nf.Selectable',
                'nf.ContextMenu',
                'nf.QuickSelect'],
            function ($, d3, nfCommon, nfNgBridge, nfLabel, nfFunnel, nfPort, nfRemoteProcessGroup, nfProcessGroup, nfProcessor, nfConnection, nfConnectionConfiguration, nfCanvasUtils, nfConnectable, nfDraggable, nfSelectable, nfContextMenu, nfQuickSelect) {
                return (nf.Graph = factory($, d3, nfCommon, nfNgBridge, nfLabel, nfFunnel, nfPort, nfRemoteProcessGroup, nfProcessGroup, nfProcessor, nfConnection, nfConnectionConfiguration, nfCanvasUtils, nfConnectable, nfDraggable, nfSelectable, nfContextMenu, nfQuickSelect));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Graph =
            factory(require('jquery'),
                require('d3'),
                require('nf.Common'),
                require('nf.ng.Bridge'),
                require('nf.Label'),
                require('nf.Funnel'),
                require('nf.Port'),
                require('nf.RemoteProcessGroup'),
                require('nf.ProcessGroup'),
                require('nf.Processor'),
                require('nf.Connection'),
                require('nf.ConnectionConfiguration'),
                require('nf.CanvasUtils'),
                require('nf.Connectable'),
                require('nf.Draggable'),
                require('nf.Selectable'),
                require('nf.ContextMenu'),
                require('nf.QuickSelect')));
    } else {
        nf.Graph = factory(root.$,
            root.d3,
            root.nf.Common,
            root.nf.ng.Bridge,
            root.nf.Label,
            root.nf.Funnel,
            root.nf.Port,
            root.nf.RemoteProcessGroup,
            root.nf.ProcessGroup,
            root.nf.Processor,
            root.nf.Connection,
            root.nf.ConnectionConfiguration,
            root.nf.CanvasUtils,
            root.nf.Connectable,
            root.nf.Draggable,
            root.nf.Selectable,
            root.nf.ContextMenu,
            root.nf.QuickSelect);
    }
}(this, function ($, d3, nfCommon, nfNgBridge, nfLabel, nfFunnel, nfPort, nfRemoteProcessGroup, nfProcessGroup, nfProcessor, nfConnection, nfConnectionConfiguration, nfCanvasUtils, nfConnectable, nfDraggable, nfSelectable, nfContextMenu, nfQuickSelect) {
    'use strict';

    var combinePorts = function (contents) {
        if (nfCommon.isDefinedAndNotNull(contents.inputPorts) && nfCommon.isDefinedAndNotNull(contents.outputPorts)) {
            return contents.inputPorts.concat(contents.outputPorts);
        } else if (nfCommon.isDefinedAndNotNull(contents.inputPorts)) {
            return contents.inputPorts;
        } else if (nfCommon.isDefinedAndNotNull(contents.outputPorts)) {
            return contents.outputPorts;
        } else {
            return [];
        }
    };

    var combinePortStatus = function (status) {
        if (nfCommon.isDefinedAndNotNull(status.inputPortStatusSnapshots) && nfCommon.isDefinedAndNotNull(status.outputPortStatusSnapshots)) {
            return status.inputPortStatusSnapshots.concat(status.outputPortStatusSnapshots);
        } else if (nfCommon.isDefinedAndNotNull(status.inputPortStatusSnapshots)) {
            return status.inputPortStatusSnapshots;
        } else if (nfCommon.isDefinedAndNotNull(status.outputPortStatusSnapshots)) {
            return status.outputPortStatusSnapshots;
        } else {
            return [];
        }
    };

    /**
     * Updates component visibility based on their proximity to the screen's viewport.
     */
    var updateComponentVisibility = function () {
        var canvasContainer = $('#canvas-container');
        var translate = nfCanvasUtils.translateCanvasView();
        var scale = nfCanvasUtils.scaleCanvasView();

        // scale the translation
        translate = [translate[0] / scale, translate[1] / scale];

        // get the normalized screen width and height
        var screenWidth = canvasContainer.width() / scale;
        var screenHeight = canvasContainer.height() / scale;

        // calculate the screen bounds one screens worth in each direction
        var screenLeft = -translate[0] - screenWidth;
        var screenTop = -translate[1] - screenHeight;
        var screenRight = screenLeft + (screenWidth * 3);
        var screenBottom = screenTop + (screenHeight * 3);

        // detects whether a component is visible and should be rendered
        var isComponentVisible = function (d) {
            if (!nfCanvasUtils.shouldRenderPerScale()) {
                return false;
            }

            var left = d.position.x;
            var top = d.position.y;
            var right = left + d.dimensions.width;
            var bottom = top + d.dimensions.height;

            // determine if the component is now visible
            return screenLeft < right && screenRight > left && screenTop < bottom && screenBottom > top;
        };

        // detects whether a connection is visible and should be rendered
        var isConnectionVisible = function (d) {
            if (!nfCanvasUtils.shouldRenderPerScale()) {
                return false;
            }

            var x, y;
            if (d.bends.length > 0) {
                var i = Math.min(Math.max(0, d.labelIndex), d.bends.length - 1);
                x = d.bends[i].x;
                y = d.bends[i].y;
            } else {
                x = (d.start.x + d.end.x) / 2;
                y = (d.start.y + d.end.y) / 2;
            }

            return screenLeft < x && screenRight > x && screenTop < y && screenBottom > y;
        };

        // marks the specific component as visible and determines if its entering or leaving visibility
        var updateVisibility = function (d, isVisible) {
            var selection = d3.select('#id-' + d.id);
            var visible = isVisible(d);
            var wasVisible = selection.classed('visible');

            // mark the selection as appropriate
            selection.classed('visible', visible)
                .classed('entering', function () {
                    return visible && !wasVisible;
                }).classed('leaving', function () {
                return !visible && wasVisible;
            });
        };

        // get the all components
        var graph = nfGraph.get();

        // update the visibility for each component
        $.each(graph.processors, function (_, d) {
            updateVisibility(d, isComponentVisible);
        });
        $.each(graph.ports, function (_, d) {
            updateVisibility(d, isComponentVisible);
        });
        $.each(graph.processGroups, function (_, d) {
            updateVisibility(d, isComponentVisible);
        });
        $.each(graph.remoteProcessGroups, function (_, d) {
            updateVisibility(d, isComponentVisible);
        });
        $.each(graph.connections, function (_, d) {
            updateVisibility(d, isConnectionVisible);
        });
    };

    var nfGraph = {
        init: function () {
            // initialize the object responsible for each type of component
            nfLabel.init(nfConnectable, nfDraggable, nfSelectable, nfContextMenu, nfQuickSelect);
            nfFunnel.init(nfConnectable, nfDraggable, nfSelectable, nfContextMenu);
            nfPort.init(nfConnectable, nfDraggable, nfSelectable, nfContextMenu, nfQuickSelect);
            nfRemoteProcessGroup.init(nfConnectable, nfDraggable, nfSelectable, nfContextMenu, nfQuickSelect);
            nfProcessGroup.init(nfConnectable, nfDraggable, nfSelectable, nfContextMenu);
            nfProcessor.init(nfConnectable, nfDraggable, nfSelectable, nfContextMenu, nfQuickSelect);
            nfConnection.init(nfSelectable, nfContextMenu, nfQuickSelect, nfConnectionConfiguration);

            // display the deep link
            return nfCanvasUtils.showDeepLink(true);
        },

        /**
         * Populates the graph with the resources defined in the response.
         *
         * @argument {object} processGroupContents      The contents of the process group
         * @argument {boolean} selectAll                Whether or not to select the new contents
         */
        add: function (processGroupContents, options) {
            var selectAll = false;
            if (nfCommon.isDefinedAndNotNull(options)) {
                selectAll = nfCommon.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
            }

            // if we are going to select the new components, deselect the previous selection
            if (selectAll) {
                nfCanvasUtils.getSelection().classed('selected', false);
            }

            // merge the ports together
            var ports = combinePorts(processGroupContents);

            // add the components to the responsible object
            nfLabel.add(processGroupContents.labels, options);
            nfFunnel.add(processGroupContents.funnels, options);
            nfRemoteProcessGroup.add(processGroupContents.remoteProcessGroups, options);
            nfPort.add(ports, options);
            nfProcessGroup.add(processGroupContents.processGroups, options);
            nfProcessor.add(processGroupContents.processors, options);
            nfConnection.add(processGroupContents.connections, options);

            // inform Angular app if the selection is changing
            if (selectAll) {
                nfNgBridge.digest();
            }
        },

        /**
         * Populates the graph with the resources defined in the response.
         *
         * @argument {object} processGroupContents      The contents of the process group
         * @argument {object} options                   Configuration options
         */
        set: function (processGroupContents, options) {
            var selectAll = false;
            if (nfCommon.isDefinedAndNotNull(options)) {
                selectAll = nfCommon.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
            }

            // if we are going to select the new components, deselect the previous selection
            if (selectAll) {
                nfCanvasUtils.getSelection().classed('selected', false);
            }

            // merge the ports together
            var ports = combinePorts(processGroupContents);

            // add the components to the responsible object
            nfLabel.set(processGroupContents.labels, options);
            nfFunnel.set(processGroupContents.funnels, options);
            nfRemoteProcessGroup.set(processGroupContents.remoteProcessGroups, options);
            nfPort.set(ports, options);
            nfProcessGroup.set(processGroupContents.processGroups, options);
            nfProcessor.set(processGroupContents.processors, options);
            nfConnection.set(processGroupContents.connections, options);

            // inform Angular app if the selection is changing
            if (selectAll) {
                nfNgBridge.digest();
            }
        },

        /**
         * Expires any caches prior to setting updated components via .set(...) above. This is necessary
         * if an ajax request returns out of order. The caches will ensure that added/removed components
         * will not be removed/added due to process group refreshes. Whether or not a component is present
         * is ambiguous whether the request is from before the component was added/removed or if another
         * client has legitimately removed/added it. Once a request is initiated after the component is
         * added/removed we can remove the entry from the cache.
         *
         * @param timestamp expire caches before
         */
        expireCaches: function (timestamp) {
            nfLabel.expireCaches(timestamp);
            nfFunnel.expireCaches(timestamp);
            nfRemoteProcessGroup.expireCaches(timestamp);
            nfPort.expireCaches(timestamp);
            nfProcessGroup.expireCaches(timestamp);
            nfProcessor.expireCaches(timestamp);
            nfConnection.expireCaches(timestamp);
        },

        /**
         * Gets the components currently on the canvas.
         */
        get: function () {
            return {
                labels: nfLabel.get(),
                funnels: nfFunnel.get(),
                ports: nfPort.get(),
                remoteProcessGroups: nfRemoteProcessGroup.get(),
                processGroups: nfProcessGroup.get(),
                processors: nfProcessor.get(),
                connections: nfConnection.get()
            };
        },

        /**
         * Gets a graph component `type`.
         *
         * @param type  The type of component.
         */
        getComponentByType: function (type) {
            switch (type)
            {
                case "Label":
                    return nfLabel;
                    break;

                case "Funnel":
                    return nfFunnel;
                    break;

                case "Port":
                    return nfPort;
                    break;

                case "RemoteProcessGroup":
                    return nfRemoteProcessGroup;
                    break;

                case "ProcessGroup":
                    return nfProcessGroup;
                    break;

                case "Processor":
                    return nfProcessor;
                    break;

                case "Connection":
                    return nfConnection;
                    break;

                default:
                    throw new Error('Unknown component type.');
                    break;
            }
        },

        /**
         * Clears all the components currently on the canvas. This function does not automatically refresh.
         */
        removeAll: function () {
            // remove all the components
            nfLabel.removeAll();
            nfFunnel.removeAll();
            nfPort.removeAll();
            nfRemoteProcessGroup.removeAll();
            nfProcessGroup.removeAll();
            nfProcessor.removeAll();
            nfConnection.removeAll();
        },

        /**
         * Refreshes all components currently on the canvas.
         */
        pan: function () {
            // refresh the components
            nfPort.pan();
            nfRemoteProcessGroup.pan();
            nfProcessGroup.pan();
            nfProcessor.pan();
            nfConnection.pan();
        },

        /**
         * Updates component visibility based on the current translation/scale.
         */
        updateVisibility: function () {
            updateComponentVisibility();
            nfGraph.pan();

            // update URL deep linking params
            nfCanvasUtils.setURLParameters();
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
         * Reload the component on the canvas.
         *
         * @param component     The component.
         */
        reload: function (component) {
            var componentData = component.datum();
            if (componentData.permissions.canRead) {
                if (nfCanvasUtils.isProcessor(component)) {
                    nfProcessor.reload(componentData.id);
                } else if (nfCanvasUtils.isInputPort(component)) {
                    nfPort.reload(componentData.id);
                } else if (nfCanvasUtils.isRemoteProcessGroup(component)) {
                    nfRemoteProcessGroup.reload(componentData.id);
                }
            }
        }
    };

    return nfGraph;
}));