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
package org.apache.nifi.web.api.dto;

import java.util.LinkedHashSet;
import java.util.Set;
import javax.xml.bind.annotation.XmlType;

/**
 * The contents of a flow snippet.
 */
@XmlType(name = "flowSnippet")
public class FlowSnippetDTO {

    private Set<ProcessGroupDTO> processGroups = new LinkedHashSet<>();
    private Set<RemoteProcessGroupDTO> remoteProcessGroups = new LinkedHashSet<>();
    private Set<ProcessorDTO> processors = new LinkedHashSet<>();
    private Set<PortDTO> inputPorts = new LinkedHashSet<>();
    private Set<PortDTO> outputPorts = new LinkedHashSet<>();
    private Set<ConnectionDTO> connections = new LinkedHashSet<>();
    private Set<LabelDTO> labels = new LinkedHashSet<>();
    private Set<FunnelDTO> funnels = new LinkedHashSet<>();
    private Set<ControllerServiceDTO> controllerServices = new LinkedHashSet<>();

    /**
     * @return connections in this flow snippet
     */
    public Set<ConnectionDTO> getConnections() {
        return connections;
    }

    public void setConnections(Set<ConnectionDTO> connections) {
        this.connections = connections;
    }

    /**
     * @return input ports in this flow snippet
     */
    public Set<PortDTO> getInputPorts() {
        return inputPorts;
    }

    public void setInputPorts(Set<PortDTO> inputPorts) {
        this.inputPorts = inputPorts;
    }

    /**
     * @return labels in this flow snippet
     */
    public Set<LabelDTO> getLabels() {
        return labels;
    }

    public void setLabels(Set<LabelDTO> labels) {
        this.labels = labels;
    }

    /**
     * @return funnels in this flow snippet
     */
    public Set<FunnelDTO> getFunnels() {
        return funnels;
    }

    public void setFunnels(Set<FunnelDTO> funnels) {
        this.funnels = funnels;
    }

    /**
     * @return output ports in this flow snippet
     */
    public Set<PortDTO> getOutputPorts() {
        return outputPorts;
    }

    public void setOutputPorts(Set<PortDTO> outputPorts) {
        this.outputPorts = outputPorts;
    }

    /**
     * @return process groups in this flow snippet
     */
    public Set<ProcessGroupDTO> getProcessGroups() {
        return processGroups;
    }

    public void setProcessGroups(Set<ProcessGroupDTO> processGroups) {
        this.processGroups = processGroups;
    }

    /**
     * @return processors in this flow group
     */
    public Set<ProcessorDTO> getProcessors() {
        return processors;
    }

    public void setProcessors(Set<ProcessorDTO> processors) {
        this.processors = processors;
    }

    /**
     * @return remote process groups in this flow snippet
     */
    public Set<RemoteProcessGroupDTO> getRemoteProcessGroups() {
        return remoteProcessGroups;
    }

    public void setRemoteProcessGroups(Set<RemoteProcessGroupDTO> remoteProcessGroups) {
        this.remoteProcessGroups = remoteProcessGroups;
    }

    /**
     * @return the Controller Services in this flow snippet
     */
    public Set<ControllerServiceDTO> getControllerServices() {
        return controllerServices;
    }

    public void setControllerServices(Set<ControllerServiceDTO> controllerServices) {
        this.controllerServices = controllerServices;
    }
}
