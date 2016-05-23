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
package org.apache.nifi.web.api.dto.flow;

import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;

import javax.xml.bind.annotation.XmlType;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The structure of the flow.
 */
@XmlType(name = "flow")
public class FlowDTO {

    private Set<ProcessGroupEntity> processGroups = new LinkedHashSet<>();
    private Set<RemoteProcessGroupEntity> remoteProcessGroups = new LinkedHashSet<>();
    private Set<ProcessorEntity> processors = new LinkedHashSet<>();
    private Set<PortEntity> inputPorts = new LinkedHashSet<>();
    private Set<PortEntity> outputPorts = new LinkedHashSet<>();
    private Set<ConnectionEntity> connections = new LinkedHashSet<>();
    private Set<LabelEntity> labels = new LinkedHashSet<>();
    private Set<FunnelEntity> funnels = new LinkedHashSet<>();

    /**
     * @return connections in this flow
     */
    @ApiModelProperty(
            value = "The connections in this flow."
    )
    public Set<ConnectionEntity> getConnections() {
        return connections;
    }

    public void setConnections(Set<ConnectionEntity> connections) {
        this.connections = connections;
    }

    /**
     * @return input ports in this flow
     */
    @ApiModelProperty(
            value = "The input ports in this flow."
    )
    public Set<PortEntity> getInputPorts() {
        return inputPorts;
    }

    public void setInputPorts(Set<PortEntity> inputPorts) {
        this.inputPorts = inputPorts;
    }

    /**
     * @return labels in this flow
     */
    @ApiModelProperty(
            value = "The labels in this flow."
    )
    public Set<LabelEntity> getLabels() {
        return labels;
    }

    public void setLabels(Set<LabelEntity> labels) {
        this.labels = labels;
    }

    /**
     * @return funnels in this flow
     */
    @ApiModelProperty(
            value = "The funnels in this flow."
    )
    public Set<FunnelEntity> getFunnels() {
        return funnels;
    }

    public void setFunnels(Set<FunnelEntity> funnels) {
        this.funnels = funnels;
    }

    /**
     * @return output ports in this flow
     */
    @ApiModelProperty(
            value = "The output ports in this flow."
    )
    public Set<PortEntity> getOutputPorts() {
        return outputPorts;
    }

    public void setOutputPorts(Set<PortEntity> outputPorts) {
        this.outputPorts = outputPorts;
    }

    /**
     * @return process groups in this flow
     */
    @ApiModelProperty(
            value = "The process groups in this flow."
    )
    public Set<ProcessGroupEntity> getProcessGroups() {
        return processGroups;
    }

    public void setProcessGroups(Set<ProcessGroupEntity> processGroups) {
        this.processGroups = processGroups;
    }

    /**
     * @return processors in this flow
     */
    @ApiModelProperty(
            value = "The processors in this flow."
    )
    public Set<ProcessorEntity> getProcessors() {
        return processors;
    }

    public void setProcessors(Set<ProcessorEntity> processors) {
        this.processors = processors;
    }

    /**
     * @return remote process groups in this flow
     */
    @ApiModelProperty(
            value = "The remote process groups in this flow."
    )
    public Set<RemoteProcessGroupEntity> getRemoteProcessGroups() {
        return remoteProcessGroups;
    }

    public void setRemoteProcessGroups(Set<RemoteProcessGroupEntity> remoteProcessGroups) {
        this.remoteProcessGroups = remoteProcessGroups;
    }

}
