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

import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedFunnel;
import org.apache.nifi.flow.VersionedLabel;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class VersionedComponentAdditions {

    private Set<VersionedProcessGroup> processGroups;
    private Set<VersionedRemoteProcessGroup> remoteProcessGroups;
    private Set<VersionedProcessor> processors;
    private Set<VersionedPort> inputPorts;
    private Set<VersionedPort> outputPorts;
    private Set<VersionedConnection> connections;
    private Set<VersionedLabel> labels;
    private Set<VersionedFunnel> funnels;
    private Set<VersionedControllerService> controllerServices;
    private Map<String, VersionedParameterContext> parameterContexts;
    private Map<String, ParameterProviderReference> parameterProviders;

    private VersionedComponentAdditions(final Builder builder) {
        this.processGroups = builder.processGroups == null ? Collections.emptySet() : Collections.unmodifiableSet(builder.processGroups);
        this.remoteProcessGroups = builder.remoteProcessGroups == null ? Collections.emptySet() : Collections.unmodifiableSet(builder.remoteProcessGroups);
        this.processors = builder.processors == null ? Collections.emptySet() : Collections.unmodifiableSet(builder.processors);
        this.inputPorts = builder.inputPorts == null ? Collections.emptySet() : Collections.unmodifiableSet(builder.inputPorts);
        this.outputPorts = builder.outputPorts == null ? Collections.emptySet() : Collections.unmodifiableSet(builder.outputPorts);
        this.connections = builder.connections == null ? Collections.emptySet() : Collections.unmodifiableSet(builder.connections);
        this.labels = builder.labels == null ? Collections.emptySet() : Collections.unmodifiableSet(builder.labels);
        this.funnels = builder.funnels == null ? Collections.emptySet() : Collections.unmodifiableSet(builder.funnels);
        this.controllerServices = builder.controllerServices == null ? Collections.emptySet() : Collections.unmodifiableSet(builder.controllerServices);
        this.parameterContexts = builder.parameterContexts == null ? Collections.emptyMap() : Collections.unmodifiableMap(builder.parameterContexts);
        this.parameterProviders = builder.parameterProviders == null ? Collections.emptyMap() : Collections.unmodifiableMap(builder.parameterProviders);
    }

    public Set<VersionedProcessGroup> getProcessGroups() {
        return processGroups;
    }

    public Set<VersionedRemoteProcessGroup> getRemoteProcessGroups() {
        return remoteProcessGroups;
    }

    public Set<VersionedProcessor> getProcessors() {
        return processors;
    }

    public Set<VersionedPort> getInputPorts() {
        return inputPorts;
    }

    public Set<VersionedPort> getOutputPorts() {
        return outputPorts;
    }

    public Set<VersionedConnection> getConnections() {
        return connections;
    }

    public Set<VersionedLabel> getLabels() {
        return labels;
    }

    public Set<VersionedFunnel> getFunnels() {
        return funnels;
    }

    public Set<VersionedControllerService> getControllerServices() {
        return controllerServices;
    }

    public Map<String, VersionedParameterContext> getParameterContexts() {
        return parameterContexts;
    }

    public Map<String, ParameterProviderReference> getParameterProviders() {
        return parameterProviders;
    }

    public static class Builder {
        private Set<VersionedProcessGroup> processGroups;
        private Set<VersionedRemoteProcessGroup> remoteProcessGroups;
        private Set<VersionedProcessor> processors;
        private Set<VersionedPort> inputPorts;
        private Set<VersionedPort> outputPorts;
        private Set<VersionedConnection> connections;
        private Set<VersionedLabel> labels;
        private Set<VersionedFunnel> funnels;
        private Set<VersionedControllerService> controllerServices;
        private Map<String, VersionedParameterContext> parameterContexts;
        private Map<String, ParameterProviderReference> parameterProviders;

        public Builder setProcessGroups(Set<VersionedProcessGroup> processGroups) {
            this.processGroups = processGroups;
            return this;
        }

        public Builder setRemoteProcessGroups(Set<VersionedRemoteProcessGroup> remoteProcessGroups) {
            this.remoteProcessGroups = remoteProcessGroups;
            return this;
        }

        public Builder setProcessors(Set<VersionedProcessor> processors) {
            this.processors = processors;
            return this;
        }

        public Builder setInputPorts(Set<VersionedPort> inputPorts) {
            this.inputPorts = inputPorts;
            return this;
        }

        public Builder setOutputPorts(Set<VersionedPort> outputPorts) {
            this.outputPorts = outputPorts;
            return this;
        }

        public Builder setConnections(Set<VersionedConnection> connections) {
            this.connections = connections;
            return this;
        }

        public Builder setLabels(Set<VersionedLabel> labels) {
            this.labels = labels;
            return this;
        }

        public Builder setFunnels(Set<VersionedFunnel> funnels) {
            this.funnels = funnels;
            return this;
        }

        public Builder setControllerServices(Set<VersionedControllerService> controllerServices) {
            this.controllerServices = controllerServices;
            return this;
        }

        public Builder setParameterContexts(Map<String, VersionedParameterContext> parameterContexts) {
            this.parameterContexts = parameterContexts;
            return this;
        }

        public Builder setParameterProviders(Map<String, ParameterProviderReference> parameterProviders) {
            this.parameterProviders = parameterProviders;
            return this;
        }

        public VersionedComponentAdditions build() {
            return new VersionedComponentAdditions(this);
        }
    }
}
