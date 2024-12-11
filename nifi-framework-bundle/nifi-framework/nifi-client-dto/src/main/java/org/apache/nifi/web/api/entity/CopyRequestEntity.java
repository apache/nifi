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

import java.util.HashSet;
import java.util.Set;

/**
 * A request to copy a portion of the flow.
 */
@XmlType(name = "copyRequestEntity")
public class CopyRequestEntity extends Entity {

    private Set<String> processGroups = new HashSet<>();
    private Set<String> remoteProcessGroups = new HashSet<>();
    private Set<String> processors = new HashSet<>();
    private Set<String> inputPorts = new HashSet<>();
    private Set<String> outputPorts = new HashSet<>();
    private Set<String> connections = new HashSet<>();
    private Set<String> labels = new HashSet<>();
    private Set<String> funnels = new HashSet<>();

    /**
     * @return the ids of the connections to be copied.
     */
    @Schema(description = "The ids of the connections to be copied."
    )
    public Set<String> getConnections() {
        return connections;
    }

    public void setConnections(Set<String> connections) {
        this.connections = connections;
    }

    /**
     * @return the ids of the funnels to be copied.
     */
    @Schema(description = "The ids of the funnels to be copied."
    )
    public Set<String> getFunnels() {
        return funnels;
    }

    public void setFunnels(Set<String> funnels) {
        this.funnels = funnels;
    }

    /**
     * @return the ids of the input port to be copied.
     */
    @Schema(description = "The ids of the input ports to be copied."
    )
    public Set<String> getInputPorts() {
        return inputPorts;
    }

    public void setInputPorts(Set<String> inputPorts) {
        this.inputPorts = inputPorts;
    }

    /**
     * @return the ids of the labels to be copied.
     */
    @Schema(description = "The ids of the labels to be copied."
    )
    public Set<String> getLabels() {
        return labels;
    }

    public void setLabels(Set<String> labels) {
        this.labels = labels;
    }

    /**
     * @return the ids of the output ports to be copied.
     */
    @Schema(description = "The ids of the output ports to be copied."
    )
    public Set<String> getOutputPorts() {
        return outputPorts;
    }

    public void setOutputPorts(Set<String> outputPorts) {
        this.outputPorts = outputPorts;
    }

    /**
     * @return The ids of the process groups to be copied.
     */
    @Schema(description = "The ids of the process groups to be copied."
    )
    public Set<String> getProcessGroups() {
        return processGroups;
    }

    public void setProcessGroups(Set<String> processGroups) {
        this.processGroups = processGroups;
    }

    /**
     * @return The ids of the processors to be copied.
     */
    @Schema(description = "The ids of the processors to be copied."
    )
    public Set<String> getProcessors() {
        return processors;
    }

    public void setProcessors(Set<String> processors) {
        this.processors = processors;
    }

    /**
     * @return the ids of the remote process groups to be copied.
     */
    @Schema(description = "The ids of the remote process groups to be copied."
    )
    public Set<String> getRemoteProcessGroups() {
        return remoteProcessGroups;
    }

    public void setRemoteProcessGroups(Set<String> remoteProcessGroups) {
        this.remoteProcessGroups = remoteProcessGroups;
    }

}
