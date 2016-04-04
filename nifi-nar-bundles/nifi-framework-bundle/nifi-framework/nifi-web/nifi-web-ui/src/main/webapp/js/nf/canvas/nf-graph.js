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
        add: function (processGroupContents, selectAll) {
            selectAll = nf.Common.isDefinedAndNotNull(selectAll) ? selectAll : false;

            // if we are going to select the new components, deselect the previous selection
            if (selectAll) {
                nf.CanvasUtils.getSelection().classed('selected', false);
            }

            // merge the ports together
            var ports = combinePorts(processGroupContents);

            // add the components to the responsible object
            if (!nf.Common.isEmpty(processGroupContents.labels)) {
                nf.Label.add(processGroupContents.labels, selectAll);
            }
            if (!nf.Common.isEmpty(processGroupContents.funnels)) {
                nf.Funnel.add(processGroupContents.funnels, selectAll);
            }
            if (!nf.Common.isEmpty(processGroupContents.remoteProcessGroups)) {
                nf.RemoteProcessGroup.add(processGroupContents.remoteProcessGroups, selectAll);
            }
            if (!nf.Common.isEmpty(ports)) {
                nf.Port.add(ports, selectAll);
            }
            if (!nf.Common.isEmpty(processGroupContents.processGroups)) {
                nf.ProcessGroup.add(processGroupContents.processGroups, selectAll);
            }
            if (!nf.Common.isEmpty(processGroupContents.processors)) {
                nf.Processor.add(processGroupContents.processors, selectAll);
            }
            if (!nf.Common.isEmpty(processGroupContents.connections)) {
                nf.Connection.add(processGroupContents.connections, selectAll);
            }
            
            // trigger the toolbar to refresh if the selection is changing
            if (selectAll) {
                nf.CanvasToolbar.refresh();
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
         * Sets the components contained within the specified process group.
         * 
         * @param {type} processGroupContents
         */
        set: function (processGroupContents) {
            // merge the ports together
            var ports = combinePorts(processGroupContents);

            // set the components
            if (!nf.Common.isEmpty(processGroupContents.labels)) {
                nf.Label.set(processGroupContents.labels);
            }
            if (!nf.Common.isEmpty(processGroupContents.funnels)) {
                nf.Funnel.set(processGroupContents.funnels);
            }
            if (!nf.Common.isEmpty(ports)) {
                nf.Port.set(ports);
            }
            if (!nf.Common.isEmpty(processGroupContents.remoteProcessGroups)) {
                nf.RemoteProcessGroup.set(processGroupContents.remoteProcessGroups);
            }
            if (!nf.Common.isEmpty(processGroupContents.processGroups)) {
                nf.ProcessGroup.set(processGroupContents.processGroups);
            }
            if (!nf.Common.isEmpty(processGroupContents.processors)) {
                nf.Processor.set(processGroupContents.processors);
            }
            if (!nf.Common.isEmpty(processGroupContents.connections)) {
                nf.Connection.set(processGroupContents.connections);
            }
        },
        
        /**
         * Populates the status for the components specified. This will update the content 
         * of the existing components on the graph and will not cause them to be repainted. 
         * This operation must be very inexpensive due to the frequency it is called.
         * 
         * @argument {object} aggregateSnapshot    The status of the process group aggregated accross the cluster
         */
        setStatus: function (aggregateSnapshot) {
            // merge the port status together
            var portStatus = combinePortStatus(aggregateSnapshot);

            // set the component status
            nf.Port.setStatus(portStatus);
            nf.RemoteProcessGroup.setStatus(aggregateSnapshot.remoteProcessGroupStatusSnapshots);
            nf.ProcessGroup.setStatus(aggregateSnapshot.processGroupStatusSnapshots);
            nf.Processor.setStatus(aggregateSnapshot.processorStatusSnapshots);
            nf.Connection.setStatus(aggregateSnapshot.connectionStatusSnapshots);
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