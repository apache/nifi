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
package org.apache.nifi.web.api.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedFunnel;
import org.apache.nifi.flow.VersionedLabel;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A response to copy a portion of the flow.
 */
@XmlType(name = "copyResponseEntity")
public class CopyResponseEntity extends Entity {

    private String id;

    private Map<String, ExternalControllerServiceReference> externalControllerServiceReferences = new HashMap<>();
    private Map<String, VersionedParameterContext> parameterContexts = new HashMap<>();
    private Map<String, ParameterProviderReference> parameterProviders = new HashMap<>();

    private Set<VersionedProcessGroup> processGroups = new HashSet<>();
    private Set<VersionedRemoteProcessGroup> remoteProcessGroups = new HashSet<>();
    private Set<VersionedProcessor> processors = new HashSet<>();
    private Set<VersionedPort> inputPorts = new HashSet<>();
    private Set<VersionedPort> outputPorts = new HashSet<>();
    private Set<VersionedConnection> connections = new HashSet<>();
    private Set<VersionedLabel> labels = new HashSet<>();
    private Set<VersionedFunnel> funnels = new HashSet<>();

    /**
     * The id for this copy action.
     *
     * @return The id
     */
    @Schema(description = "The id for this copy action."
    )
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * The external controller service references.
     *
     * @return The external controller service reference
     */
    @Schema(description = "The external controller service references."
    )
    public Map<String, ExternalControllerServiceReference> getExternalControllerServiceReferences() {
        return externalControllerServiceReferences;
    }

    public void setExternalControllerServiceReferences(Map<String, ExternalControllerServiceReference> externalControllerServiceReferences) {
        this.externalControllerServiceReferences = externalControllerServiceReferences;
    }

    /**
     * The referenced parameter contexts.
     *
     * @return The referenced parameter contexts
     */
    @Schema(description = "The referenced parameter contexts."
    )
    public Map<String, VersionedParameterContext> getParameterContexts() {
        return parameterContexts;
    }

    public void setParameterContexts(Map<String, VersionedParameterContext> parameterContexts) {
        this.parameterContexts = parameterContexts;
    }

    /**
     * The referenced parameter providers.
     *
     * @return The referenced parameter providers
     */
    @Schema(description = "The referenced parameter providers."
    )
    public Map<String, ParameterProviderReference> getParameterProviders() {
        return parameterProviders;
    }

    public void setParameterProviders(Map<String, ParameterProviderReference> parameterProviders) {
        this.parameterProviders = parameterProviders;
    }

    /**
     * @return the connections being copied.
     */
    @Schema(description = "The connections being copied."
    )
    public Set<VersionedConnection> getConnections() {
        return connections;
    }

    public void setConnections(Set<VersionedConnection> connections) {
        this.connections = connections;
    }

    /**
     * @return the funnels being copied.
     */
    @Schema(description = "The funnels being copied."
    )
    public Set<VersionedFunnel> getFunnels() {
        return funnels;
    }

    public void setFunnels(Set<VersionedFunnel> funnels) {
        this.funnels = funnels;
    }

    /**
     * @return the input port being copied.
     */
    @Schema(description = "The input ports being copied."
    )
    public Set<VersionedPort> getInputPorts() {
        return inputPorts;
    }

    public void setInputPorts(Set<VersionedPort> inputPorts) {
        this.inputPorts = inputPorts;
    }

    /**
     * @return the labels being copied.
     */
    @Schema(description = "The labels being copied."
    )
    public Set<VersionedLabel> getLabels() {
        return labels;
    }

    public void setLabels(Set<VersionedLabel> labels) {
        this.labels = labels;
    }

    /**
     * @return the output ports being copied.
     */
    @Schema(description = "The output ports being copied."
    )
    public Set<VersionedPort> getOutputPorts() {
        return outputPorts;
    }

    public void setOutputPorts(Set<VersionedPort> outputPorts) {
        this.outputPorts = outputPorts;
    }

    /**
     * @return The process groups being copied.
     */
    @Schema(description = "The process groups being copied."
    )
    public Set<VersionedProcessGroup> getProcessGroups() {
        return processGroups;
    }

    public void setProcessGroups(Set<VersionedProcessGroup> processGroups) {
        this.processGroups = processGroups;
    }

    /**
     * @return The processors being copied.
     */
    @Schema(description = "The processors being copied."
    )
    public Set<VersionedProcessor> getProcessors() {
        return processors;
    }

    public void setProcessors(Set<VersionedProcessor> processors) {
        this.processors = processors;
    }

    /**
     * @return the remote process groups being copied.
     */
    @Schema(description = "The remote process groups being copied."
    )
    public Set<VersionedRemoteProcessGroup> getRemoteProcessGroups() {
        return remoteProcessGroups;
    }

    public void setRemoteProcessGroups(Set<VersionedRemoteProcessGroup> remoteProcessGroups) {
        this.remoteProcessGroups = remoteProcessGroups;
    }
}
