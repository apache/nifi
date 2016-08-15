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

/* global nf */

nf.Graph = (function () {

    var combinePorts = function (contents) {
        if (nf.Common.isDefinedAndNotNull(contents.inputPorts) && nf.Common.isDefinedAndNotNull(contents.outputPorts)) {
            return contents.inputPorts.concat(contents.outputPorts);
        } else if (nf.Common.isDefinedAndNotNull(contents.inputPorts)) {
            return contents.inputPorts;
        } else if (nf.Common.isDefinedAndNotNull(contents.outputPorts)) {
            return contents.outputPorts;
        } else {
            return [];
        }
    };

    var combinePortStatus = function (status) {
        if (nf.Common.isDefinedAndNotNull(status.inputPortStatusSnapshots) && nf.Common.isDefinedAndNotNull(status.outputPortStatusSnapshots)) {
            return status.inputPortStatusSnapshots.concat(status.outputPortStatusSnapshots);
        } else if (nf.Common.isDefinedAndNotNull(status.inputPortStatusSnapshots)) {
            return status.inputPortStatusSnapshots;
        } else if (nf.Common.isDefinedAndNotNull(status.outputPortStatusSnapshots)) {
            return status.outputPortStatusSnapshots;
        } else {
            return [];
        }
    };

    return {
        init: function () {

            // initialize the object responsible for each type of component
            nf.Label.init();
            nf.Funnel.init();
            nf.Port.init();
            nf.RemoteProcessGroup.init();
            nf.ProcessGroup.init();
            nf.Processor.init();
            nf.Connection.init();

            // load the graph
            return nf.CanvasUtils.enterGroup(nf.Canvas.getGroupId());
        },

        /**
         * Populates the graph with the resources defined in the response.
         *
         * @argument {object} processGroupContents      The contents of the process group
         * @argument {boolean} selectAll                Whether or not to select the new contents
         */
        add: function (processGroupContents, options) {
            var selectAll = false;
            if (nf.Common.isDefinedAndNotNull(options)) {
                selectAll = nf.Common.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
            }

            // if we are going to select the new components, deselect the previous selection
            if (selectAll) {
                nf.CanvasUtils.getSelection().classed('selected', false);
            }

            // merge the ports together
            var ports = combinePorts(processGroupContents);

            // add the components to the responsible object
            nf.Label.add(processGroupContents.labels, options);
            nf.Funnel.add(processGroupContents.funnels, options);
            nf.RemoteProcessGroup.add(processGroupContents.remoteProcessGroups, options);
            nf.Port.add(ports, options);
            nf.ProcessGroup.add(processGroupContents.processGroups, options);
            nf.Processor.add(processGroupContents.processors, options);
            nf.Connection.add(processGroupContents.connections, options);

            // inform Angular app if the selection is changing
            if (selectAll) {
                nf.ng.Bridge.digest();
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
            if (nf.Common.isDefinedAndNotNull(options)) {
                selectAll = nf.Common.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
            }

            // if we are going to select the new components, deselect the previous selection
            if (selectAll) {
                nf.CanvasUtils.getSelection().classed('selected', false);
            }

            // merge the ports together
            var ports = combinePorts(processGroupContents);

            // add the components to the responsible object
            nf.Label.set(processGroupContents.labels, options);
            nf.Funnel.set(processGroupContents.funnels, options);
            nf.RemoteProcessGroup.set(processGroupContents.remoteProcessGroups, options);
            nf.Port.set(ports, options);
            nf.ProcessGroup.set(processGroupContents.processGroups, options);
            nf.Processor.set(processGroupContents.processors, options);
            nf.Connection.set(processGroupContents.connections, options);

            // inform Angular app if the selection is changing
            if (selectAll) {
                nf.ng.Bridge.digest();
            }
        },
        
        /**
         * Gets the components currently on the canvas.
         */
        get: function () {
            return {
                labels: nf.Label.get(),
                funnels: nf.Funnel.get(),
                ports: nf.Port.get(),
                remoteProcessGroups: nf.RemoteProcessGroup.get(),
                processGroups: nf.ProcessGroup.get(),
                processors: nf.Processor.get(),
                connections: nf.Connection.get()
            };
        },
        
        /**
         * Clears all the components currently on the canvas. This function does not automatically refresh.
         */
        removeAll: function () {
            // remove all the components
            nf.Label.removeAll();
            nf.Funnel.removeAll();
            nf.Port.removeAll();
            nf.RemoteProcessGroup.removeAll();
            nf.ProcessGroup.removeAll();
            nf.Processor.removeAll();
            nf.Connection.removeAll();
        },
        
        /**
         * Refreshes all components currently on the canvas.
         */
        pan: function () {
            // refresh the components
            nf.Port.pan();
            nf.RemoteProcessGroup.pan();
            nf.ProcessGroup.pan();
            nf.Processor.pan();
            nf.Connection.pan();
        }
    };
}());