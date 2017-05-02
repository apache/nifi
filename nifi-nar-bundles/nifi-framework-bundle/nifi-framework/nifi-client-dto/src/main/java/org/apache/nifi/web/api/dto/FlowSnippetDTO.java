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

import com.wordnik.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * The contents of a flow snippet.
 */
@XmlType(name = "flowSnippet")
public class FlowSnippetDTO {

    private static final long LSB = 0;

    private Set<ProcessGroupDTO> processGroups = new LinkedHashSet<>();
    private Set<RemoteProcessGroupDTO> remoteProcessGroups = new LinkedHashSet<>();
    private Set<ProcessorDTO> processors = new LinkedHashSet<>();
    private Set<PortDTO> inputPorts = new LinkedHashSet<>();
    private Set<PortDTO> outputPorts = new LinkedHashSet<>();
    private Set<ConnectionDTO> connections = new LinkedHashSet<>();
    private Set<LabelDTO> labels = new LinkedHashSet<>();
    private Set<FunnelDTO> funnels = new LinkedHashSet<>();
    private Set<ControllerServiceDTO> controllerServices = new LinkedHashSet<>();

    private final boolean newTemplate;
    private Set<String> convertedUuids = new HashSet<>();

    public FlowSnippetDTO() {
        this(false);
    }

    public FlowSnippetDTO(boolean newTemplate) {
        this.newTemplate = newTemplate;
    }
    /**
     * @return connections in this flow snippet
     */
    @ApiModelProperty(
            value = "The connections in this flow snippet."
    )
    public Set<ConnectionDTO> getConnections() {
        return connections;
    }

    public void setConnections(Set<ConnectionDTO> connections) {
        this.removeInstanceIdentifierIfNecessary(connections);
        this.connections = this.orderedById(connections);
    }

    /**
     * @return input ports in this flow snippet
     */
    @ApiModelProperty(
            value = "The input ports in this flow snippet."
    )
    public Set<PortDTO> getInputPorts() {
        return inputPorts;
    }

    public void setInputPorts(Set<PortDTO> inputPorts) {
        this.removeInstanceIdentifierIfNecessary(inputPorts);
        this.inputPorts = this.orderedById(inputPorts);
    }

    /**
     * @return labels in this flow snippet
     */
    @ApiModelProperty(
            value = "The labels in this flow snippet."
    )
    public Set<LabelDTO> getLabels() {
        return labels;
    }

    public void setLabels(Set<LabelDTO> labels) {
        this.removeInstanceIdentifierIfNecessary(labels);
        this.labels = this.orderedById(labels);
    }

    /**
     * @return funnels in this flow snippet
     */
    @ApiModelProperty(
            value = "The funnels in this flow snippet."
    )
    public Set<FunnelDTO> getFunnels() {
        return funnels;
    }

    public void setFunnels(Set<FunnelDTO> funnels) {
        this.removeInstanceIdentifierIfNecessary(funnels);
        this.funnels = this.orderedById(funnels);
    }

    /**
     * @return output ports in this flow snippet
     */
    @ApiModelProperty(
            value = "The output ports in this flow snippet."
    )
    public Set<PortDTO> getOutputPorts() {
        return outputPorts;
    }

    public void setOutputPorts(Set<PortDTO> outputPorts) {
        this.removeInstanceIdentifierIfNecessary(outputPorts);
        this.outputPorts = this.orderedById(outputPorts);
    }

    /**
     * @return process groups in this flow snippet
     */
    @ApiModelProperty(
            value = "The process groups in this flow snippet."
    )
    public Set<ProcessGroupDTO> getProcessGroups() {
        return processGroups;
    }

    public void setProcessGroups(Set<ProcessGroupDTO> processGroups) {
        this.removeInstanceIdentifierIfNecessary(processGroups);
        this.processGroups = this.orderedById(processGroups);
    }

    /**
     * @return processors in this flow group
     */
    @ApiModelProperty(
            value = "The processors in this flow snippet."
    )
    public Set<ProcessorDTO> getProcessors() {
        return processors;
    }

    public void setProcessors(Set<ProcessorDTO> processors) {
        this.removeInstanceIdentifierIfNecessary(processors);
        this.processors = this.orderedById(processors);
    }

    /**
     * @return remote process groups in this flow snippet
     */
    @ApiModelProperty(
            value = "The remote process groups in this flow snippet."
    )
    public Set<RemoteProcessGroupDTO> getRemoteProcessGroups() {
        return remoteProcessGroups;
    }

    public void setRemoteProcessGroups(Set<RemoteProcessGroupDTO> remoteProcessGroups) {
        this.removeInstanceIdentifierIfNecessary(remoteProcessGroups);
        this.remoteProcessGroups = this.orderedById(remoteProcessGroups);
    }

    /**
     * @return the Controller Services in this flow snippet
     */
    @ApiModelProperty(
            value = "The controller services in this flow snippet."
    )
    public Set<ControllerServiceDTO> getControllerServices() {
        return controllerServices;
    }

    public void setControllerServices(Set<ControllerServiceDTO> controllerServices) {
        this.removeInstanceIdentifierIfNecessary(controllerServices);
        this.controllerServices = this.orderedById(controllerServices);
    }

    private <T extends ComponentDTO> Set<T> orderedById(Set<T> dtos) {
        TreeSet<T> components = new TreeSet<>(new Comparator<ComponentDTO>() {
            @Override
            public int compare(ComponentDTO c1, ComponentDTO c2) {
                return UUID.fromString(c1.getId()).compareTo(UUID.fromString(c2.getId()));
            }
        });
        components.addAll(dtos);
        return components;
    }

    private long generateMsb(String id) {
        final UUID temp;
        if (convertedUuids.contains(id)) {
            temp = UUID.fromString(id);
        } else {
            temp = UUID.nameUUIDFromBytes(id.getBytes(StandardCharsets.UTF_8));

            // record what the converted uuid will be so we do not re-convert a previously converted uuid
            convertedUuids.add(new UUID(temp.getMostSignificantBits(), LSB).toString());
        }
        return temp.getMostSignificantBits();
    }

    private void removeInstanceIdentifierIfNecessary(Set<? extends ComponentDTO> componentDtos) {
        if (this.newTemplate) {
            for (ComponentDTO componentDto : componentDtos) {
                UUID id = new UUID(this.generateMsb(componentDto.getId()), LSB);
                componentDto.setId(id.toString());

                id = new UUID(this.generateMsb(componentDto.getParentGroupId()), LSB);
                componentDto.setParentGroupId(id.toString());
                if (componentDto instanceof ControllerServiceDTO) {
                    ControllerServiceDTO csDTO = (ControllerServiceDTO) componentDto;
                    Map<String, PropertyDescriptorDTO> map = csDTO.getDescriptors();
                    Map<String, String> props = csDTO.getProperties();
                    for (Entry<String, PropertyDescriptorDTO> entry : map.entrySet()) {
                        if (entry.getValue().getIdentifiesControllerService() != null && props.get(entry.getKey()) != null) {
                            String key = entry.getKey();
                            String value = props.get(key);
                            id = new UUID(this.generateMsb(value), LSB);
                            props.put(key, id.toString());
                        }
                    }
                } else if (componentDto instanceof ProcessorDTO) {
                    ProcessorDTO processorDTO = (ProcessorDTO) componentDto;
                    Map<String, PropertyDescriptorDTO> map = processorDTO.getConfig().getDescriptors();
                    Map<String, String> props = processorDTO.getConfig().getProperties();
                    for (Entry<String, PropertyDescriptorDTO> entry : map.entrySet()) {
                        if (entry.getValue().getIdentifiesControllerService() != null && props.get(entry.getKey()) != null) {
                            String key = entry.getKey();
                            String value = props.get(key);
                            id = new UUID(this.generateMsb(value), LSB);
                            props.put(key, id.toString());
                        }
                    }
                } else if (componentDto instanceof ConnectionDTO) {
                    ConnectionDTO connectionDTO = (ConnectionDTO) componentDto;

                    ConnectableDTO cdto = connectionDTO.getSource();
                    if (!cdto.getType().equals("REMOTE_INPUT_PORT") && !cdto.getType().equals("REMOTE_OUTPUT_PORT")) {
                        id = new UUID(this.generateMsb(cdto.getId()), LSB);
                        cdto.setId(id.toString());
                    }

                    id = new UUID(this.generateMsb(cdto.getGroupId()), LSB);
                    cdto.setGroupId(id.toString());

                    cdto = connectionDTO.getDestination();
                    if (!cdto.getType().equals("REMOTE_INPUT_PORT") && !cdto.getType().equals("REMOTE_OUTPUT_PORT")) {
                        id = new UUID(this.generateMsb(cdto.getId()), LSB);
                        cdto.setId(id.toString());
                    }

                    id = new UUID(this.generateMsb(cdto.getGroupId()), LSB);
                    cdto.setGroupId(id.toString());
                } else if (componentDto instanceof ProcessGroupDTO) {
                    FlowSnippetDTO fsDTO = ((ProcessGroupDTO) componentDto).getContents();

                    this.removeInstanceIdentifierIfNecessary(fsDTO.getConnections());
                    fsDTO.connections = this.orderedById(fsDTO.getConnections());

                    this.removeInstanceIdentifierIfNecessary(fsDTO.getControllerServices());
                    fsDTO.controllerServices = this.orderedById(fsDTO.getControllerServices());

                    this.removeInstanceIdentifierIfNecessary(fsDTO.getFunnels());
                    fsDTO.funnels = this.orderedById(fsDTO.getFunnels());

                    this.removeInstanceIdentifierIfNecessary(fsDTO.getInputPorts());
                    fsDTO.inputPorts = this.orderedById(fsDTO.getInputPorts());

                    this.removeInstanceIdentifierIfNecessary(fsDTO.getLabels());
                    fsDTO.labels = this.orderedById(fsDTO.getLabels());

                    this.removeInstanceIdentifierIfNecessary(fsDTO.getOutputPorts());
                    fsDTO.outputPorts = this.orderedById(fsDTO.getOutputPorts());

                    this.removeInstanceIdentifierIfNecessary(fsDTO.getProcessGroups());
                    fsDTO.processGroups = this.orderedById(fsDTO.getProcessGroups());

                    this.removeInstanceIdentifierIfNecessary(fsDTO.getProcessors());
                    fsDTO.processors = this.orderedById(fsDTO.getProcessors());

                    this.removeInstanceIdentifierIfNecessary(fsDTO.getRemoteProcessGroups());
                    fsDTO.remoteProcessGroups = this.orderedById(fsDTO.getRemoteProcessGroups());
                } else if (componentDto instanceof RemoteProcessGroupDTO) {
                    RemoteProcessGroupContentsDTO contentsDTO = ((RemoteProcessGroupDTO) componentDto).getContents();
                    contentsDTO.setInputPorts(this.orderedRemotePortsById(contentsDTO.getInputPorts()));
                    contentsDTO.setOutputPorts(this.orderedRemotePortsById(contentsDTO.getOutputPorts()));
                }
            }
        }
    }

    private <T extends RemoteProcessGroupPortDTO> Set<T> orderedRemotePortsById(Set<T> dtos) {
        TreeSet<T> components = new TreeSet<>(new Comparator<RemoteProcessGroupPortDTO>() {
            @Override
            public int compare(RemoteProcessGroupPortDTO c1, RemoteProcessGroupPortDTO c2) {
                return UUID.fromString(c1.getId()).compareTo(UUID.fromString(c2.getId()));
            }
        });
        components.addAll(dtos);
        return components;
    }
}
