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

package org.apache.nifi.minifi.commons.schema;

import org.apache.nifi.minifi.commons.schema.common.BaseSchema;
import org.apache.nifi.minifi.commons.schema.common.ConvertableSchema;
import org.apache.nifi.minifi.commons.schema.common.StringUtil;
import org.apache.nifi.minifi.commons.schema.common.WritableSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.COMPONENT_STATUS_REPO_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CONNECTIONS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CONTENT_REPO_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CORE_PROPS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.FLOWFILE_REPO_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.FLOW_CONTROLLER_PROPS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.PROCESSORS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.PROVENANCE_REPORTING_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.PROVENANCE_REPO_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.REMOTE_PROCESSING_GROUPS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SECURITY_PROPS_KEY;

/**
 *
 */
public class ConfigSchema extends BaseSchema implements WritableSchema, ConvertableSchema<ConfigSchema> {
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_PROCESSOR_IDS = "Found the following duplicate processor ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_IDS = "Found the following duplicate connection ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_REMOTE_PROCESSING_GROUP_NAMES = "Found the following duplicate remote processing group names: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_REMOTE_INPUT_PORT_IDS = "Found the following duplicate remote input port ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_IDS = "Found the following ids that occur both in Processors and Remote Input Ports: ";
    public static final int CONFIG_VERSION = 2;
    public static final String VERSION = "MiNiFi Config Version";
    public static final String CONNECTION_WITH_ID = "Connection with id ";
    public static final String HAS_INVALID_SOURCE_ID = " has invalid source id ";
    public static final String HAS_INVALID_DESTINATION_ID = " has invalid destination id ";
    public static String TOP_LEVEL_NAME = "top level";
    private FlowControllerSchema flowControllerProperties;
    private CorePropertiesSchema coreProperties;
    private FlowFileRepositorySchema flowfileRepositoryProperties;
    private ContentRepositorySchema contentRepositoryProperties;
    private ComponentStatusRepositorySchema componentStatusRepositoryProperties;
    private SecurityPropertiesSchema securityProperties;
    private List<ProcessorSchema> processors;
    private List<ConnectionSchema> connections;
    private List<RemoteProcessingGroupSchema> remoteProcessingGroups;
    private ProvenanceReportingSchema provenanceReportingProperties;

    private ProvenanceRepositorySchema provenanceRepositorySchema;

    public ConfigSchema(Map map) {
        this(map, Collections.emptyList());
    }

    public ConfigSchema(Map map, List<String> validationIssues) {
        validationIssues.stream().forEach(this::addValidationIssue);
        flowControllerProperties = getMapAsType(map, FLOW_CONTROLLER_PROPS_KEY, FlowControllerSchema.class, TOP_LEVEL_NAME, true);

        coreProperties = getMapAsType(map, CORE_PROPS_KEY, CorePropertiesSchema.class, TOP_LEVEL_NAME, false);
        flowfileRepositoryProperties = getMapAsType(map, FLOWFILE_REPO_KEY, FlowFileRepositorySchema.class, TOP_LEVEL_NAME, false);
        contentRepositoryProperties = getMapAsType(map, CONTENT_REPO_KEY, ContentRepositorySchema.class, TOP_LEVEL_NAME, false);
        provenanceRepositorySchema = getMapAsType(map, PROVENANCE_REPO_KEY, ProvenanceRepositorySchema.class, TOP_LEVEL_NAME, false);
        componentStatusRepositoryProperties = getMapAsType(map, COMPONENT_STATUS_REPO_KEY, ComponentStatusRepositorySchema.class, TOP_LEVEL_NAME, false);
        securityProperties = getMapAsType(map, SECURITY_PROPS_KEY, SecurityPropertiesSchema.class, TOP_LEVEL_NAME, false);

        processors = convertListToType(getOptionalKeyAsType(map, PROCESSORS_KEY, List.class, TOP_LEVEL_NAME, new ArrayList<>()), "processor", ProcessorSchema.class, PROCESSORS_KEY);

        remoteProcessingGroups = convertListToType(getOptionalKeyAsType(map, REMOTE_PROCESSING_GROUPS_KEY, List.class, TOP_LEVEL_NAME, new ArrayList<>()), "remote processing group",
                RemoteProcessingGroupSchema.class, REMOTE_PROCESSING_GROUPS_KEY);

        connections = convertListToType(getOptionalKeyAsType(map, CONNECTIONS_KEY, List.class, TOP_LEVEL_NAME, new ArrayList<>()),
                "connection", ConnectionSchema.class, CONNECTIONS_KEY);

        provenanceReportingProperties = getMapAsType(map, PROVENANCE_REPORTING_KEY, ProvenanceReportingSchema.class, TOP_LEVEL_NAME, false, false);

        addIssuesIfNotNull(flowControllerProperties);
        addIssuesIfNotNull(coreProperties);
        addIssuesIfNotNull(flowfileRepositoryProperties);
        addIssuesIfNotNull(contentRepositoryProperties);
        addIssuesIfNotNull(componentStatusRepositoryProperties);
        addIssuesIfNotNull(securityProperties);
        addIssuesIfNotNull(provenanceReportingProperties);
        addIssuesIfNotNull(provenanceRepositorySchema);
        addIssuesIfNotNull(processors);
        addIssuesIfNotNull(connections);
        addIssuesIfNotNull(remoteProcessingGroups);

        Set<String> processorIds = new HashSet<>();
        List<String> processorIdList = processors.stream().map(ProcessorSchema::getId).collect(Collectors.toList());
        processorIds.addAll(processorIdList);

        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_PROCESSOR_IDS, processorIdList);
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_IDS, connections.stream().map(ConnectionSchema::getId).collect(Collectors.toList()));
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_REMOTE_PROCESSING_GROUP_NAMES,
                remoteProcessingGroups.stream().map(RemoteProcessingGroupSchema::getName).collect(Collectors.toList()));

        Set<String> remoteInputPortIds = new HashSet<>();
        List<String> remoteInputPortIdList = remoteProcessingGroups.stream().filter(r -> r.getInputPorts() != null)
                .flatMap(r -> r.getInputPorts().stream()).map(RemoteInputPortSchema::getId).collect(Collectors.toList());
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_REMOTE_INPUT_PORT_IDS, remoteInputPortIdList);
        remoteInputPortIds.addAll(remoteInputPortIdList);

        Set<String> duplicateIds = new HashSet<>(processorIds);
        duplicateIds.retainAll(remoteInputPortIds);
        if (duplicateIds.size() > 0) {
            addValidationIssue(FOUND_THE_FOLLOWING_DUPLICATE_IDS + duplicateIds.stream().sorted().collect(Collectors.joining(", ")));
        }

        Set<String> connectableIds = new HashSet<>(processorIds);
        connectableIds.addAll(remoteInputPortIds);
        connections.forEach(c -> {
            String destinationId = c.getDestinationId();
            if (!StringUtil.isNullOrEmpty(destinationId) && !connectableIds.contains(destinationId)) {
                addValidationIssue(CONNECTION_WITH_ID + c.getId() + HAS_INVALID_DESTINATION_ID + destinationId);
            }
            String sourceId = c.getSourceId();
            if (!StringUtil.isNullOrEmpty(sourceId) && !connectableIds.contains(sourceId)) {
                addValidationIssue(CONNECTION_WITH_ID + c.getId() + HAS_INVALID_SOURCE_ID + sourceId);
            }
        });
    }

    public Map<String, Object> toMap() {
        Map<String, Object> result = mapSupplier.get();
        result.put(VERSION, getVersion());
        putIfNotNull(result, FLOW_CONTROLLER_PROPS_KEY, flowControllerProperties);
        putIfNotNull(result, CORE_PROPS_KEY, coreProperties);
        putIfNotNull(result, FLOWFILE_REPO_KEY, flowfileRepositoryProperties);
        putIfNotNull(result, CONTENT_REPO_KEY, contentRepositoryProperties);
        putIfNotNull(result, PROVENANCE_REPO_KEY, provenanceRepositorySchema);
        putIfNotNull(result, COMPONENT_STATUS_REPO_KEY, componentStatusRepositoryProperties);
        putIfNotNull(result, SECURITY_PROPS_KEY, securityProperties);
        putListIfNotNull(result, PROCESSORS_KEY, processors);
        putListIfNotNull(result, CONNECTIONS_KEY, connections);
        putListIfNotNull(result, REMOTE_PROCESSING_GROUPS_KEY, remoteProcessingGroups);
        putIfNotNull(result, PROVENANCE_REPORTING_KEY, provenanceReportingProperties);
        return result;
    }

    public FlowControllerSchema getFlowControllerProperties() {
        return flowControllerProperties;
    }

    public CorePropertiesSchema getCoreProperties() {
        return coreProperties;
    }

    public FlowFileRepositorySchema getFlowfileRepositoryProperties() {
        return flowfileRepositoryProperties;
    }

    public ContentRepositorySchema getContentRepositoryProperties() {
        return contentRepositoryProperties;
    }

    public SecurityPropertiesSchema getSecurityProperties() {
        return securityProperties;
    }

    public List<ProcessorSchema> getProcessors() {
        return processors;
    }

    public List<ConnectionSchema> getConnections() {
        return connections;
    }

    public List<RemoteProcessingGroupSchema> getRemoteProcessingGroups() {
        return remoteProcessingGroups;
    }

    public ProvenanceReportingSchema getProvenanceReportingProperties() {
        return provenanceReportingProperties;
    }

    public ComponentStatusRepositorySchema getComponentStatusRepositoryProperties() {
        return componentStatusRepositoryProperties;
    }

    public ProvenanceRepositorySchema getProvenanceRepositorySchema() {
        return provenanceRepositorySchema;
    }

    @Override
    public int getVersion() {
        return CONFIG_VERSION;
    }

    @Override
    public ConfigSchema convert() {
        return this;
    }
}
