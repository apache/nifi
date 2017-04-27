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

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.atlas.processors.DataSetEntityCreator;
import org.apache.nifi.atlas.processors.Egress;
import org.apache.nifi.atlas.processors.Ingress;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;

public class NiFiFlow implements AtlasProcess {

    private static final Logger logger = LoggerFactory.getLogger(NiFiFlowAnalyzer.class);

    private final String flowName;
    private final String rootProcessGroupId;
    private final String url;
    private final AtlasObjectId id;
    private String description;
    private final Set<AtlasObjectId> inputs = new HashSet<>();
    private final Set<AtlasObjectId> outputs = new HashSet<>();
    private final Map<String, Set<AtlasObjectId>> processorInputs = new HashMap<>();
    private final Map<String, Set<AtlasObjectId>> processorOutputs = new HashMap<>();
    private final Map<AtlasObjectId, EntityCreationInfo> entityCreators = new HashMap<>();
    private final Map<String, ProcessorDTO> processors = new HashMap<>();
    private final Map<String, RemoteProcessGroupDTO> remoteProcessGroups = new HashMap<>();
    private final Map<String, List<ConnectionDTO>> incomingRelationShips = new HashMap<>();
    private final Map<String, List<ConnectionDTO>> outGoingRelationShips = new HashMap<>();

    private final Map<AtlasObjectId, AtlasEntity> createdData = new HashMap<>();
    private final Map<AtlasObjectId, AtlasEntity> queues = new HashMap<>();
    private final Map<AtlasObjectId, AtlasEntity> inputPorts = new HashMap<>();
    private final Map<AtlasObjectId, AtlasEntity> outputPorts = new HashMap<>();

    private static class EntityCreationInfo {
        private DataSetEntityCreator creator;
        private Map<String, String> properties;
    }

    public NiFiFlow(String flowName, String rootProcessGroupId, String url) {
        this.flowName = flowName;
        id = new AtlasObjectId(TYPE_NIFI_FLOW, ATTR_QUALIFIED_NAME, rootProcessGroupId);
        this.rootProcessGroupId = rootProcessGroupId;
        this.url = url;
    }

    public AtlasObjectId getId() {
        return id;
    }

    public String getRootProcessGroupId() {
        return rootProcessGroupId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Set<AtlasObjectId> getInputs() {
        return inputs;
    }

    public Set<AtlasObjectId> getOutputs() {
        return outputs;
    }

    public void putInput(String processorId, AtlasObjectId input, Ingress processor, Map<String, String> properties) {
        final Set<AtlasObjectId> ids = processorInputs.computeIfAbsent(processorId, k -> new HashSet<>());
        ids.add(input);

        if (processor instanceof DataSetEntityCreator) {
            addEntityCreator(input, (DataSetEntityCreator) processor, properties);
        }
    }

    public void putOutput(String processorId, AtlasObjectId output, Egress processor, Map<String, String> properties) {
        final Set<AtlasObjectId> ids = processorOutputs.computeIfAbsent(processorId, k -> new HashSet<>());
        ids.add(output);

        if (processor instanceof DataSetEntityCreator) {
            addEntityCreator(output, (DataSetEntityCreator) processor, properties);
        }
    }

    private void addEntityCreator(AtlasObjectId objectId, DataSetEntityCreator creator, Map<String, String> properties) {
        final EntityCreationInfo info = new EntityCreationInfo();
        info.creator =  creator;
        info.properties = properties;
        entityCreators.put(objectId, info);
    }

    public void addConnection(ConnectionDTO c) {
        final String sourceId = c.getSource().getId();
        final String destId = c.getDestination().getId();
        outGoingRelationShips.computeIfAbsent(sourceId, k -> new ArrayList<>()).add(c);
        incomingRelationShips.computeIfAbsent(destId, k -> new ArrayList<>()).add(c);
    }

    public void addProcessor(ProcessorEntity p) {
        processors.put(p.getId(), p.getComponent());
    }

    public Map<String, ProcessorDTO> getProcessors() {
        return processors;
    }

    public void addRemoteProcessGroup(RemoteProcessGroupEntity r) {
        remoteProcessGroups.put(r.getId(), r.getComponent());
    }

    public String getFlowName() {
        return flowName;
    }

    public String getUrl() {
        return url;
    }

    public AtlasEntity createDataSetEntity(AtlasObjectId objectId) {

        final String typeName = objectId.getTypeName();
        switch (typeName) {
            default: {
                final EntityCreationInfo creationInfo = entityCreators.get(objectId);
                if (creationInfo == null) {
                    return null;
                }

                return creationInfo.creator.create(objectId, creationInfo.properties);
            }
        }

    }

    public List<ConnectionDTO> getIncomingRelationShips(String processorId) {
        return incomingRelationShips.get(processorId);
    }

    public List<ConnectionDTO> getOutgoingRelationShips(String processorId) {
        return outGoingRelationShips.get(processorId);
    }

    public Set<AtlasObjectId> getInputs(String processorId) {
        return processorInputs.get(processorId);
    }

    public Set<AtlasObjectId> getOutputs(String processorId) {
        return processorOutputs.get(processorId);
    }

    public Map<AtlasObjectId, AtlasEntity> getInputPorts() {
        return inputPorts;
    }

    public Map<AtlasObjectId, AtlasEntity> getOutputPorts() {
        return outputPorts;
    }

    public Map<AtlasObjectId, AtlasEntity> getQueues() {
        return queues;
    }

    public Map<AtlasObjectId, AtlasEntity> getCreatedData() {
        return createdData;
    }

    public void dump() {
        logger.info("flowName: {}", flowName);
        Function<String, String> toName = pid -> processors.get(pid).getName();
        processors.forEach((pid, p) -> {
            logger.info("{}:{} receives from {}", pid, toName.apply(pid), incomingRelationShips.get(pid));
            logger.info("{}:{} sends to {}", pid, toName.apply(pid), outGoingRelationShips.get(pid));
        });

        logger.info("## Input ObjectIds");
        inputs.forEach(in -> logger.info("{}", in));
        logger.info("## Output ObjectIds");
        outputs.forEach(out -> logger.info("{}", out));
    }

}
