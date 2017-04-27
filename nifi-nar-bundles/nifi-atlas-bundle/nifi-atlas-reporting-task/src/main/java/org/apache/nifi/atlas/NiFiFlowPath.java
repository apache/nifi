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
package org.apache.nifi.atlas;

import org.apache.atlas.model.instance.AtlasObjectId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NiFiFlowPath implements AtlasProcess {
    private final List<String> processorIds = new ArrayList<>();

    private final String id;
    private final Set<AtlasObjectId> inputs = new HashSet<>();
    private final Set<AtlasObjectId> outputs = new HashSet<>();

    private final Set<NiFiFlowPath> incomingPaths = new HashSet<>();
    private final Set<NiFiFlowPath> outgoingPaths = new HashSet<>();

    private String name;

    public NiFiFlowPath(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void addProcessor(String processorId) {
        processorIds.add(processorId);
    }

    public Set<AtlasObjectId> getInputs() {
        return inputs;
    }

    public Set<AtlasObjectId> getOutputs() {
        return outputs;
    }

    public List<String> getProcessorIds() {
        return processorIds;
    }

    public String getId() {
        return id;
    }

    public Set<NiFiFlowPath> getIncomingPaths() {
        return incomingPaths;
    }

    public Set<NiFiFlowPath> getOutgoingPaths() {
        return outgoingPaths;
    }

    @Override
    public String toString() {
        return "NiFiFlowPath{" +
                "name='" + name + '\'' +
                ", inputs=" + inputs +
                ", outputs=" + outputs +
                ", processorIds=" + processorIds +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NiFiFlowPath that = (NiFiFlowPath) o;

        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
