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

package org.apache.nifi.groups;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.service.ControllerServiceNode;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ComponentAdditions {

    private Set<ProcessGroup> processGroups;
    private Set<RemoteProcessGroup> remoteProcessGroups;
    private Set<ProcessorNode> processors;
    private Set<Port> inputPorts;
    private Set<Port> outputPorts;
    private Set<Connection> connections;
    private Set<Label> labels;
    private Set<Funnel> funnels;
    private Set<ControllerServiceNode> controllerServices;

    private ComponentAdditions(final Builder builder) {
        this.processGroups = Collections.unmodifiableSet(builder.processGroups);
        this.remoteProcessGroups = Collections.unmodifiableSet(builder.remoteProcessGroups);
        this.processors = Collections.unmodifiableSet(builder.processors);
        this.inputPorts = Collections.unmodifiableSet(builder.inputPorts);
        this.outputPorts = Collections.unmodifiableSet(builder.outputPorts);
        this.connections = Collections.unmodifiableSet(builder.connections);
        this.labels = Collections.unmodifiableSet(builder.labels);
        this.funnels = Collections.unmodifiableSet(builder.funnels);
        this.controllerServices = Collections.unmodifiableSet(builder.controllerServices);
    }

    public Set<ProcessGroup> getProcessGroups() {
        return processGroups;
    }

    public Set<RemoteProcessGroup> getRemoteProcessGroups() {
        return remoteProcessGroups;
    }

    public Set<ProcessorNode> getProcessors() {
        return processors;
    }

    public Set<Port> getInputPorts() {
        return inputPorts;
    }

    public Set<Port> getOutputPorts() {
        return outputPorts;
    }

    public Set<Connection> getConnections() {
        return connections;
    }

    public Set<Label> getLabels() {
        return labels;
    }

    public Set<Funnel> getFunnels() {
        return funnels;
    }

    public Set<ControllerServiceNode> getControllerServices() {
        return controllerServices;
    }

    public static class Builder {
        private Set<ProcessGroup> processGroups = new HashSet<>();
        private Set<RemoteProcessGroup> remoteProcessGroups = new HashSet<>();
        private Set<ProcessorNode> processors = new HashSet<>();
        private Set<Port> inputPorts = new HashSet<>();
        private Set<Port> outputPorts = new HashSet<>();
        private Set<Connection> connections = new HashSet<>();
        private Set<Label> labels = new HashSet<>();
        private Set<Funnel> funnels = new HashSet<>();
        private Set<ControllerServiceNode> controllerServices = new HashSet<>();

        public Builder addProcessGroup(ProcessGroup processGroup) {
            this.processGroups.add(processGroup);
            return this;
        }

        public Builder addRemoteProcessGroup(RemoteProcessGroup remoteProcessGroup) {
            this.remoteProcessGroups.add(remoteProcessGroup);
            return this;
        }

        public Builder addProcessor(ProcessorNode processor) {
            this.processors.add(processor);
            return this;
        }

        public Builder addInputPort(Port inputPort) {
            this.inputPorts.add(inputPort);
            return this;
        }

        public Builder addOutputPort(Port outputPort) {
            this.outputPorts.add(outputPort);
            return this;
        }

        public Builder addConnection(Connection connection) {
            this.connections.add(connection);
            return this;
        }

        public Builder addLabel(Label label) {
            this.labels.add(label);
            return this;
        }

        public Builder addFunnel(Funnel funnel) {
            this.funnels.add(funnel);
            return this;
        }

        public Builder addControllerService(ControllerServiceNode controllerService) {
            this.controllerServices.add(controllerService);
            return this;
        }

        public ComponentAdditions build() {
            return new ComponentAdditions(this);
        }
    }
}
