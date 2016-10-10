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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.nifi.web.Revision;


/**
 * Represents a data flow snippet.
 */
@XmlRootElement(name = "snippet")
public class StandardSnippet implements Snippet {

    private String id;
    private String parentGroupId;

    private Map<String, Revision> processGroups = new HashMap<>();
    private Map<String, Revision> remoteProcessGroups = new HashMap<>();
    private Map<String, Revision> processors = new HashMap<>();
    private Map<String, Revision> inputPorts = new HashMap<>();
    private Map<String, Revision> outputPorts = new HashMap<>();
    private Map<String, Revision> connections = new HashMap<>();
    private Map<String, Revision> labels = new HashMap<>();
    private Map<String, Revision> funnels = new HashMap<>();

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getParentGroupId() {
        return parentGroupId;
    }

    public void setParentGroupId(String parentGroupId) {
        this.parentGroupId = parentGroupId;
    }

    @Override
    public Map<String, Revision> getConnections() {
        return Collections.unmodifiableMap(connections);
    }

    public void addConnections(Map<String, Revision> ids) {
        connections.putAll(ids);
    }

    @Override
    public Map<String, Revision> getFunnels() {
        return Collections.unmodifiableMap(funnels);
    }

    public void addFunnels(Map<String, Revision> ids) {
        funnels.putAll(ids);
    }

    @Override
    public Map<String, Revision> getInputPorts() {
        return Collections.unmodifiableMap(inputPorts);
    }

    public void addInputPorts(Map<String, Revision> ids) {
        inputPorts.putAll(ids);
    }

    @Override
    public Map<String, Revision> getOutputPorts() {
        return Collections.unmodifiableMap(outputPorts);
    }

    public void addOutputPorts(Map<String, Revision> ids) {
        outputPorts.putAll(ids);
    }

    @Override
    public Map<String, Revision> getLabels() {
        return Collections.unmodifiableMap(labels);
    }

    public void addLabels(Map<String, Revision> ids) {
        labels.putAll(ids);
    }

    @Override
    public Map<String, Revision> getProcessGroups() {
        return Collections.unmodifiableMap(processGroups);
    }

    public void addProcessGroups(Map<String, Revision> ids) {
        processGroups.putAll(ids);
    }

    @Override
    public Map<String, Revision> getProcessors() {
        return Collections.unmodifiableMap(processors);
    }

    public void addProcessors(Map<String, Revision> ids) {
        processors.putAll(ids);
    }

    @Override
    public Map<String, Revision> getRemoteProcessGroups() {
        return Collections.unmodifiableMap(remoteProcessGroups);
    }

    public void addRemoteProcessGroups(Map<String, Revision> ids) {
        remoteProcessGroups.putAll(ids);
    }

    @Override
    public boolean isEmpty() {
        return processors.isEmpty() && processGroups.isEmpty() && remoteProcessGroups.isEmpty() && labels.isEmpty()
                && inputPorts.isEmpty() && outputPorts.isEmpty() && connections.isEmpty() && funnels.isEmpty();
    }
}
