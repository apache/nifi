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

/* global nf, define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['nf.Common',
                'nf.CanvasUtils',
                'nf.ng.Bridge',
                'nf.Label',
                'nf.Funnel',
                'nf.Port',
                'nf.RemoteProcessGroup',
                'nf.ProcessGroup',
                'nf.Processor',
                'nf.Connection'],
            function (common,
                      canvasUtils,
                      angularBridge,
                      label,
                      funnel,
                      port,
                      remoteProcessGroup,
                      processGroup,
                      processor,
                      connection) {
                return (nf.Graph = factory(common,
                    canvasUtils,
                    angularBridge,
                    label,
                    funnel,
                    port,
                    remoteProcessGroup,
                    processGroup,
                    processor,
                    connection));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Graph =
            factory(require('nf.Common'),
                require('nf.CanvasUtils'),
                require('nf.ng.Bridge'),
                require('nf.Label'),
                require('nf.Funnel'),
                require('nf.Port'),
                require('nf.RemoteProcessGroup'),
                require('nf.ProcessGroup'),
                require('nf.Processor'),
                require('nf.Connection')));
    } else {
        nf.Graph = factory(root.nf.Common,
            root.nf.CanvasUtils,
            root.nf.ng.Bridge,
            root.nf.Label,
            root.nf.Funnel,
            root.nf.Port,
            root.nf.RemoteProcessGroup,
            root.nf.ProcessGroup,
            root.nf.Processor,
            root.nf.Connection);
    }
}(this, function (common,
                  canvasUtils,
                  angularBridge,
                  label,
                  funnel,
                  port,
                  remoteProcessGroup,
                  processGroup,
                  processor,
                  connection) {
    'use strict';

    var nfLabel = label;
    var nfFunnel = funnel;
    var nfPort = port;
    var nfRemoteProcessGroup = remoteProcessGroup;
    var nfProcessGroup = processGroup;
    var nfProcessor = processor;
    var nfConnection = connection;

    var combinePorts = function (contents) {
        if (common.isDefinedAndNotNull(contents.inputPorts) && common.isDefinedAndNotNull(contents.outputPorts)) {
            return contents.inputPorts.concat(contents.outputPorts);
        } else if (common.isDefinedAndNotNull(contents.inputPorts)) {
            return contents.inputPorts;
        } else if (common.isDefinedAndNotNull(contents.outputPorts)) {
            return contents.outputPorts;
        } else {
            return [];
        }
    };

    var combinePortStatus = function (status) {
        if (common.isDefinedAndNotNull(status.inputPortStatusSnapshots) && common.isDefinedAndNotNull(status.outputPortStatusSnapshots)) {
            return status.inputPortStatusSnapshots.concat(status.outputPortStatusSnapshots);
        } else if (common.isDefinedAndNotNull(status.inputPortStatusSnapshots)) {
            return status.inputPortStatusSnapshots;
        } else if (common.isDefinedAndNotNull(status.outputPortStatusSnapshots)) {
            return status.outputPortStatusSnapshots;
        } else {
            return [];
        }
    };

    return {
        init: function (groupId) {
            // initialize the object responsible for each type of component
            nfLabel.init();
            nfFunnel.init();
            nfPort.init();
            nfRemoteProcessGroup.init();
            nfProcessGroup.init();
            nfProcessor.init();
            nfConnection.init();

            // load the graph
            return nfProcessGroup.enterGroup(groupId);
        },

        /**
         * Populates the graph with the resources defined in the response.
         *
         * @argument {object} processGroupContents      The contents of the process group
         * @argument {boolean} selectAll                Whether or not to select the new contents
         */
        add: function (processGroupContents, options) {
            var selectAll = false;
            if (common.isDefinedAndNotNull(options)) {
                selectAll = common.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
            }

            // if we are going to select the new components, deselect the previous selection
            if (selectAll) {
                canvasUtils.getSelection().classed('selected', false);
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
                angularBridge.digest();
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
            if (common.isDefinedAndNotNull(options)) {
                selectAll = common.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
            }

            // if we are going to select the new components, deselect the previous selection
            if (selectAll) {
                canvasUtils.getSelection().classed('selected', false);
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
                angularBridge.digest();
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
         * Reload the component on the canvas.
         *
         * @param component     The component.
         */
        reload: function (component) {
            var componentData = component.datum();
            if (componentData.permissions.canRead) {
                if (canvasUtils.isProcessor(component)) {
                    nfProcessor.reload(componentData.id);
                } else if (canvasUtils.isInputPort(component)) {
                    nfPort.reload(componentData.id);
                } else if (canvasUtils.isRemoteProcessGroup(component)) {
                    nfRemoteProcessGroup.reload(componentData.id);
                }
            }
        }
    };
}));