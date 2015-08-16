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

    private final Set<String> processGroups = new HashSet<>();
    private final Set<String> remoteProcessGroups = new HashSet<>();
    private final Set<String> processors = new HashSet<>();
    private final Set<String> inputPorts = new HashSet<>();
    private final Set<String> outputPorts = new HashSet<>();
    private final Set<String> connections = new HashSet<>();
    private final Set<String> labels = new HashSet<>();
    private final Set<String> funnels = new HashSet<>();

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
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

    @Override
    public String getParentGroupId() {
        return parentGroupId;
    }

    public void setParentGroupId(String parentGroupId) {
        this.parentGroupId = parentGroupId;
    }

    @Override
    public Set<String> getConnections() {
        return Collections.unmodifiableSet(connections);
    }

    public void addConnections(Collection<String> ids) {
        connections.addAll(ids);
    }

    @Override
    public Set<String> getFunnels() {
        return Collections.unmodifiableSet(funnels);
    }

    public void addFunnels(Collection<String> ids) {
        funnels.addAll(ids);
    }

    @Override
    public Set<String> getInputPorts() {
        return Collections.unmodifiableSet(inputPorts);
    }

    public void addInputPorts(Collection<String> ids) {
        inputPorts.addAll(ids);
    }

    @Override
    public Set<String> getOutputPorts() {
        return Collections.unmodifiableSet(outputPorts);
    }

    public void addOutputPorts(Collection<String> ids) {
        outputPorts.addAll(ids);
    }

    @Override
    public Set<String> getLabels() {
        return Collections.unmodifiableSet(labels);
    }

    public void addLabels(Collection<String> ids) {
        labels.addAll(ids);
    }

    @Override
    public Set<String> getProcessGroups() {
        return Collections.unmodifiableSet(processGroups);
    }

    public void addProcessGroups(Collection<String> ids) {
        processGroups.addAll(ids);
    }

    @Override
    public Set<String> getProcessors() {
        return Collections.unmodifiableSet(processors);
    }

    public void addProcessors(Collection<String> ids) {
        processors.addAll(ids);
    }

    @Override
    public Set<String> getRemoteProcessGroups() {
        return Collections.unmodifiableSet(remoteProcessGroups);
    }

    public void addRemoteProcessGroups(Collection<String> ids) {
        remoteProcessGroups.addAll(ids);
    }

    @Override
    public boolean isEmpty() {
        return processors.isEmpty() && processGroups.isEmpty() && remoteProcessGroups.isEmpty() && labels.isEmpty()
                && inputPorts.isEmpty() && outputPorts.isEmpty() && connections.isEmpty() && funnels.isEmpty();
    }
}
