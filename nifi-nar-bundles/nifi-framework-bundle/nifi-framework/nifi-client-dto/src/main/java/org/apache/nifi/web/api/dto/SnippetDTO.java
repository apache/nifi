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

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlType;

import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * The contents of a snippet of a flow.
 */
@XmlType(name = "snippet")
public class SnippetDTO {

    private String id;
    private String uri;
    private String parentGroupId;

    // when specified these are only considered during creation
    private Map<String, RevisionDTO> processGroups = new HashMap<>();
    private Map<String, RevisionDTO> remoteProcessGroups = new HashMap<>();
    private Map<String, RevisionDTO> processors = new HashMap<>();
    private Map<String, RevisionDTO> inputPorts = new HashMap<>();
    private Map<String, RevisionDTO> outputPorts = new HashMap<>();
    private Map<String, RevisionDTO> connections = new HashMap<>();
    private Map<String, RevisionDTO> labels = new HashMap<>();
    private Map<String, RevisionDTO> funnels = new HashMap<>();

    /**
     * @return id of this snippet
     */
    @ApiModelProperty(
            value = "The id of the snippet."
    )
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return uri of this snippet
     */
    @ApiModelProperty(
            value = "The URI of the snippet."
    )
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * @return group id for the components in this snippet
     */
    @ApiModelProperty(
            value = "The group id for the components in the snippet."
    )
    public String getParentGroupId() {
        return parentGroupId;
    }

    public void setParentGroupId(String parentGroupId) {
        this.parentGroupId = parentGroupId;
    }

    /**
     * @return the ids of the connections in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet has been created its
     * contents cannot be modified (these ids are ignored during update requests)
     */
    @ApiModelProperty(
            value = "The ids of the connections in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet "
                    + "has been created its contents cannot be modified (these ids are ignored during update requests)."
    )
    public Map<String, RevisionDTO> getConnections() {
        return connections;
    }

    public void setConnections(Map<String, RevisionDTO> connections) {
        this.connections = connections;
    }

    /**
     * @return the ids of the funnels in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet has been created its
     * contents cannot be modified (these ids are ignored during update requests)
     */
    @ApiModelProperty(
            value = "The ids of the funnels in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet "
                    + "has been created its contents cannot be modified (these ids are ignored during update requests)."
    )
    public Map<String, RevisionDTO> getFunnels() {
        return funnels;
    }

    public void setFunnels(Map<String, RevisionDTO> funnels) {
        this.funnels = funnels;
    }

    /**
     * @return the ids of the input port in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet has been created its
     * contents cannot be modified (these ids are ignored during update requests)
     */
    @ApiModelProperty(
            value = "The ids of the input ports in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet "
                    + "has been created its contents cannot be modified (these ids are ignored during update requests)."
    )
    public Map<String, RevisionDTO> getInputPorts() {
        return inputPorts;
    }

    public void setInputPorts(Map<String, RevisionDTO> inputPorts) {
        this.inputPorts = inputPorts;
    }

    /**
     * @return the ids of the labels in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet has been created its
     * contents cannot be modified (these ids are ignored during update requests)
     */
    @ApiModelProperty(
            value = "The ids of the labels in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet "
                    + "has been created its contents cannot be modified (these ids are ignored during update requests)."
    )
    public Map<String, RevisionDTO> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, RevisionDTO> labels) {
        this.labels = labels;
    }

    /**
     * @return the ids of the output ports in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet has been created
     * its contents cannot be modified (these ids are ignored during update requests)
     */
    @ApiModelProperty(
            value = "The ids of the output ports in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet "
                    + "has been created its contents cannot be modified (these ids are ignored during update requests)."
    )
    public Map<String, RevisionDTO> getOutputPorts() {
        return outputPorts;
    }

    public void setOutputPorts(Map<String, RevisionDTO> outputPorts) {
        this.outputPorts = outputPorts;
    }

    /**
     * @return The ids of the process groups in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet has been created
     * its contents cannot be modified (these ids are ignored during update requests)
     */
    @ApiModelProperty(
            value = "The ids of the process groups in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet "
                    + "has been created its contents cannot be modified (these ids are ignored during update requests)."
    )
    public Map<String, RevisionDTO> getProcessGroups() {
        return processGroups;
    }

    public void setProcessGroups(Map<String, RevisionDTO> processGroups) {
        this.processGroups = processGroups;
    }

    /**
     * @return The ids of the processors in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet has been created its
     * contents cannot be modified (these ids are ignored during update requests)
     */
    @ApiModelProperty(
            value = "The ids of the processors in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet "
                    + "has been created its contents cannot be modified (these ids are ignored during update requests)."
    )
    public Map<String, RevisionDTO> getProcessors() {
        return processors;
    }

    public void setProcessors(Map<String, RevisionDTO> processors) {
        this.processors = processors;
    }

    /**
     * @return the ids of the remote process groups in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet has been
     * created its contents cannot be modified (these ids are ignored during update requests)
     */
    @ApiModelProperty(
            value = "The ids of the remote process groups in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet "
                    + "has been created its contents cannot be modified (these ids are ignored during update requests)."
    )
    public Map<String, RevisionDTO> getRemoteProcessGroups() {
        return remoteProcessGroups;
    }

    public void setRemoteProcessGroups(Map<String, RevisionDTO> remoteProcessGroups) {
        this.remoteProcessGroups = remoteProcessGroups;
    }

}
