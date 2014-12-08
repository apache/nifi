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
package org.apache.nifi.controller;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Represents a data flow snippet.
 */
@XmlRootElement(name = "snippet")
public class StandardSnippet implements Snippet {

    private String id;
    private String parentGroupId;
    private Boolean linked;

    private Set<String> processGroups = new HashSet<>();
    private Set<String> remoteProcessGroups = new HashSet<>();
    private Set<String> processors = new HashSet<>();
    private Set<String> inputPorts = new HashSet<>();
    private Set<String> outputPorts = new HashSet<>();
    private Set<String> connections = new HashSet<>();
    private Set<String> labels = new HashSet<>();
    private Set<String> funnels = new HashSet<>();

    /**
     * The id of this snippet.
     *
     * @return
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * Whether or not this snippet is linked to the data flow.
     *
     * @return
     */
    public boolean isLinked() {
        if (linked == null) {
            return false;
        } else {
            return linked;
        }
    }

    public void setLinked(Boolean linked) {
        this.linked = linked;
    }

    /**
     * The parent group id of the components in this snippet.
     *
     * @return
     */
    public String getParentGroupId() {
        return parentGroupId;
    }

    public void setParentGroupId(String parentGroupId) {
        this.parentGroupId = parentGroupId;
    }

    /**
     * The connections in this snippet.
     *
     * @return
     */
    public Set<String> getConnections() {
        return Collections.unmodifiableSet(connections);
    }

    public void addConnections(Collection<String> ids) {
        connections.addAll(ids);
    }

    /**
     * The funnels in this snippet.
     *
     * @return
     */
    public Set<String> getFunnels() {
        return Collections.unmodifiableSet(funnels);
    }

    public void addFunnels(Collection<String> ids) {
        funnels.addAll(ids);
    }

    /**
     * The input ports in this snippet.
     *
     * @return
     */
    public Set<String> getInputPorts() {
        return Collections.unmodifiableSet(inputPorts);
    }

    public void addInputPorts(Collection<String> ids) {
        inputPorts.addAll(ids);
    }

    /**
     * The output ports in this snippet.
     *
     * @return
     */
    public Set<String> getOutputPorts() {
        return Collections.unmodifiableSet(outputPorts);
    }

    public void addOutputPorts(Collection<String> ids) {
        outputPorts.addAll(ids);
    }

    /**
     * The labels in this snippet.
     *
     * @return
     */
    public Set<String> getLabels() {
        return Collections.unmodifiableSet(labels);
    }

    public void addLabels(Collection<String> ids) {
        labels.addAll(ids);
    }

    public Set<String> getProcessGroups() {
        return Collections.unmodifiableSet(processGroups);
    }

    public void addProcessGroups(Collection<String> ids) {
        processGroups.addAll(ids);
    }

    public Set<String> getProcessors() {
        return Collections.unmodifiableSet(processors);
    }

    public void addProcessors(Collection<String> ids) {
        processors.addAll(ids);
    }

    public Set<String> getRemoteProcessGroups() {
        return Collections.unmodifiableSet(remoteProcessGroups);
    }

    public void addRemoteProcessGroups(Collection<String> ids) {
        remoteProcessGroups.addAll(ids);
    }

    /**
     * Determines if this snippet is empty.
     *
     * @return
     */
    public boolean isEmpty() {
        return processors.isEmpty() && processGroups.isEmpty() && remoteProcessGroups.isEmpty() && labels.isEmpty()
                && inputPorts.isEmpty() && outputPorts.isEmpty() && connections.isEmpty() && funnels.isEmpty();
    }
}
