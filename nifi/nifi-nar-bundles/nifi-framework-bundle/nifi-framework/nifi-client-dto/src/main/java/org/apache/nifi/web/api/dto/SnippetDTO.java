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
import java.util.HashSet;
import java.util.Set;
import javax.xml.bind.annotation.XmlType;

/**
 * The contents of a snippet of a flow.
 */
@XmlType(name = "snippet")
public class SnippetDTO {

    private String id;
    private String uri;
    private String parentGroupId;
    private Boolean linked;

    // when specified these are only considered during creation
    private Set<String> processGroups = new HashSet<>();
    private Set<String> remoteProcessGroups = new HashSet<>();
    private Set<String> processors = new HashSet<>();
    private Set<String> inputPorts = new HashSet<>();
    private Set<String> outputPorts = new HashSet<>();
    private Set<String> connections = new HashSet<>();
    private Set<String> labels = new HashSet<>();
    private Set<String> funnels = new HashSet<>();

    private FlowSnippetDTO contents;

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
     * @return whether or not this snippet is linked to the underlying data flow
     */
    @ApiModelProperty(
            value = "Whether or not the snippet is linked to the underlying data flow. For instance if linked was set to true and the snippet was deleted "
                    + "it would also deleted the components in the snippet. If the snippet was not linked, deleting the snippet would only remove the "
                    + "snippet and leave the component intact."
    )
    public Boolean isLinked() {
        return linked;
    }

    public void setLinked(Boolean linked) {
        this.linked = linked;
    }

    /**
     * @return the ids of the connections in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet has been created its
     * contents cannot be modified (these ids are ignored during update requests)
     */
    @ApiModelProperty(
            value = "The ids of the connections in this snippet. These ids will be populated within each response. They can be specified when creating a snippet. However, once a snippet "
                    + "has been created its contents cannot be modified (these ids are ignored during update requests)."
    )
    public Set<String> getConnections() {
        return connections;
    }

    public void setConnections(Set<String> connections) {
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
    public Set<String> getFunnels() {
        return funnels;
    }

    public void setFunnels(Set<String> funnels) {
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
    public Set<String> getInputPorts() {
        return inputPorts;
    }

    public void setInputPorts(Set<String> inputPorts) {
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
    public Set<String> getLabels() {
        return labels;
    }

    public void setLabels(Set<String> labels) {
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
    public Set<String> getOutputPorts() {
        return outputPorts;
    }

    public void setOutputPorts(Set<String> outputPorts) {
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
    public Set<String> getProcessGroups() {
        return processGroups;
    }

    public void setProcessGroups(Set<String> processGroups) {
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
    public Set<String> getProcessors() {
        return processors;
    }

    public void setProcessors(Set<String> processors) {
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
    public Set<String> getRemoteProcessGroups() {
        return remoteProcessGroups;
    }

    public void setRemoteProcessGroups(Set<String> remoteProcessGroups) {
        this.remoteProcessGroups = remoteProcessGroups;
    }

    /**
     * @return the contents of the configuration for this snippet
     */
    @ApiModelProperty(
            value = "The contents of the configuration for the snippet."
    )
    public FlowSnippetDTO getContents() {
        return contents;
    }

    public void setContents(FlowSnippetDTO contents) {
        this.contents = contents;
    }

}
