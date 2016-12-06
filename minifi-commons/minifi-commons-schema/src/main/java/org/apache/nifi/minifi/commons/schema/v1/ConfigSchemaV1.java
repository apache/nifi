/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.nifi.minifi.commons.schema.v1;

import org.apache.nifi.minifi.commons.schema.ComponentStatusRepositorySchema;
import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.ConnectionSchema;
import org.apache.nifi.minifi.commons.schema.ContentRepositorySchema;
import org.apache.nifi.minifi.commons.schema.CorePropertiesSchema;
import org.apache.nifi.minifi.commons.schema.FlowControllerSchema;
import org.apache.nifi.minifi.commons.schema.FlowFileRepositorySchema;
import org.apache.nifi.minifi.commons.schema.ProcessorSchema;
import org.apache.nifi.minifi.commons.schema.ProvenanceReportingSchema;
import org.apache.nifi.minifi.commons.schema.ProvenanceRepositorySchema;
import org.apache.nifi.minifi.commons.schema.RemoteInputPortSchema;
import org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema;
import org.apache.nifi.minifi.commons.schema.SecurityPropertiesSchema;
import org.apache.nifi.minifi.commons.schema.common.BaseSchema;
import org.apache.nifi.minifi.commons.schema.common.CollectionOverlap;
import org.apache.nifi.minifi.commons.schema.common.ConvertableSchema;
import org.apache.nifi.minifi.commons.schema.common.StringUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.nifi.minifi.commons.schema.ConfigSchema.TOP_LEVEL_NAME;
import static org.apache.nifi.minifi.commons.schema.ConfigSchema.VERSION;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.COMPONENT_STATUS_REPO_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CONNECTIONS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CONTENT_REPO_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CORE_PROPS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.FLOWFILE_REPO_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.FLOW_CONTROLLER_PROPS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.PROCESSORS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.PROVENANCE_REPORTING_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.PROVENANCE_REPO_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.REMOTE_PROCESS_GROUPS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SECURITY_PROPS_KEY;

public class ConfigSchemaV1 extends BaseSchema implements ConvertableSchema<ConfigSchema> {

    public static final String REMOTE_PROCESS_GROUPS_KEY_V1 = "Remote Processing Groups";

    public static final String FOUND_THE_FOLLOWING_DUPLICATE_PROCESSOR_NAMES = "Found the following duplicate processor names: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_NAMES = "Found the following duplicate connection names: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_REMOTE_PROCESSING_GROUP_NAMES = "Found the following duplicate remote processing group names: ";
    public static final String CANNOT_LOOK_UP_PROCESSOR_ID_FROM_PROCESSOR_NAME_DUE_TO_DUPLICATE_PROCESSOR_NAMES = "Cannot look up Processor id from Processor name due to duplicate Processor names: ";
    public static final int CONFIG_VERSION = 1;
    public static final String CONNECTION_WITH_NAME = "Connection with name ";
    public static final String HAS_INVALID_DESTINATION_NAME = " has invalid destination name ";
    public static final String HAS_INVALID_SOURCE_NAME = " has invalid source name ";
    private FlowControllerSchema flowControllerProperties;
    private CorePropertiesSchema coreProperties;
    private FlowFileRepositorySchema flowfileRepositoryProperties;
    private ContentRepositorySchema contentRepositoryProperties;
    private ComponentStatusRepositorySchema componentStatusRepositoryProperties;
    private SecurityPropertiesSchema securityProperties;
    private List<ProcessorSchemaV1> processors;
    private List<ConnectionSchemaV1> connections;
    private List<RemoteProcessGroupSchemaV1> remoteProcessingGroups;
    private ProvenanceReportingSchema provenanceReportingProperties;

    private ProvenanceRepositorySchema provenanceRepositorySchema;

    public ConfigSchemaV1(Map map) {
        flowControllerProperties = getMapAsType(map, FLOW_CONTROLLER_PROPS_KEY, FlowControllerSchema.class, TOP_LEVEL_NAME, true);

        coreProperties = getMapAsType(map, CORE_PROPS_KEY, CorePropertiesSchema.class, TOP_LEVEL_NAME, false);
        flowfileRepositoryProperties = getMapAsType(map, FLOWFILE_REPO_KEY, FlowFileRepositorySchema.class, TOP_LEVEL_NAME, false);
        contentRepositoryProperties = getMapAsType(map, CONTENT_REPO_KEY, ContentRepositorySchema.class, TOP_LEVEL_NAME, false);
        provenanceRepositorySchema = getMapAsType(map, PROVENANCE_REPO_KEY, ProvenanceRepositorySchema.class, TOP_LEVEL_NAME, false);
        componentStatusRepositoryProperties = getMapAsType(map, COMPONENT_STATUS_REPO_KEY, ComponentStatusRepositorySchema.class, TOP_LEVEL_NAME, false);
        securityProperties = getMapAsType(map, SECURITY_PROPS_KEY, SecurityPropertiesSchema.class, TOP_LEVEL_NAME, false);

        processors = convertListToType(getOptionalKeyAsType(map, PROCESSORS_KEY, List.class, TOP_LEVEL_NAME, new ArrayList<>()), PROCESSORS_KEY, ProcessorSchemaV1.class, TOP_LEVEL_NAME);

        remoteProcessingGroups = convertListToType(getOptionalKeyAsType(map, REMOTE_PROCESS_GROUPS_KEY_V1, List.class, TOP_LEVEL_NAME, new ArrayList<>()), "remote processing group",
                RemoteProcessGroupSchemaV1.class, REMOTE_PROCESS_GROUPS_KEY_V1);

        connections = convertListToType(getOptionalKeyAsType(map, CONNECTIONS_KEY, List.class, TOP_LEVEL_NAME, new ArrayList<>()), CONNECTIONS_KEY, ConnectionSchemaV1.class, TOP_LEVEL_NAME);

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

        List<String> processorNames = processors.stream().map(ProcessorSchemaV1::getName).collect(Collectors.toList());

        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_PROCESSOR_NAMES, processorNames);
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_NAMES, connections.stream().map(ConnectionSchemaV1::getName).collect(Collectors.toList()));
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_REMOTE_PROCESSING_GROUP_NAMES, remoteProcessingGroups.stream().map(RemoteProcessGroupSchemaV1::getName)
                .collect(Collectors.toList()));

        Set<String> connectableNames = new HashSet<>(processorNames);
        connectableNames.addAll(remoteProcessingGroups.stream().flatMap(r -> r.getInputPorts().stream()).map(RemoteInputPortSchema::getId).collect(Collectors.toList()));
        connections.forEach(c -> {
            String destinationName = c.getDestinationName();
            if (!StringUtil.isNullOrEmpty(destinationName) && !connectableNames.contains(destinationName)) {
                addValidationIssue(CONNECTION_WITH_NAME + c.getName() + HAS_INVALID_DESTINATION_NAME + destinationName);
            }
            String sourceName = c.getSourceName();
            if (!StringUtil.isNullOrEmpty(sourceName) && !connectableNames.contains(sourceName)) {
                addValidationIssue(CONNECTION_WITH_NAME + c.getName() + HAS_INVALID_SOURCE_NAME + sourceName);
            }
        });
    }

    protected List<ProcessorSchema> getProcessorSchemas() {
        Set<UUID> ids = new HashSet<>();
        List<ProcessorSchema> processorSchemas = new ArrayList<>(processors.size());

        for (ProcessorSchemaV1 processor : processors) {
            ProcessorSchema processorSchema = processor.convert();
            processorSchema.setId(getUniqueId(ids, processorSchema.getName()));
            processorSchemas.add(processorSchema);
        }

        return processorSchemas;
    }

    protected List<ConnectionSchema> getConnectionSchemas(List<ProcessorSchema> processors, List<String> validationIssues) {
        Set<UUID> ids = new HashSet<>();

        Map<String, String> processorNameToIdMap = new HashMap<>();

        // We can't look up id by name for names that appear more than once
        Set<String> duplicateProcessorNames = new HashSet<>();

        if (processors != null) {
            processors.stream().forEachOrdered(p -> processorNameToIdMap.put(p.getName(), p.getId()));
            duplicateProcessorNames = new CollectionOverlap<>(processors.stream().map(ProcessorSchema::getName)).getDuplicates();
        }

        Set<String> remoteInputPortIds = new HashSet<>();
        if (remoteProcessingGroups != null) {
            remoteInputPortIds.addAll(remoteProcessingGroups.stream().filter(r -> r.getInputPorts() != null)
                    .flatMap(r -> r.getInputPorts().stream()).map(RemoteInputPortSchema::getId).collect(Collectors.toSet()));
        }

        Set<String> problematicDuplicateNames = new HashSet<>();

        List<ConnectionSchema> connectionSchemas = new ArrayList<>(connections.size());
        for (ConnectionSchemaV1 connection : connections) {
            ConnectionSchema convert = connection.convert();
            convert.setId(getUniqueId(ids, convert.getName()));

            String sourceName = connection.getSourceName();
            if (remoteInputPortIds.contains(sourceName)) {
                convert.setSourceId(sourceName);
            } else {
                if (duplicateProcessorNames.contains(sourceName)) {
                    problematicDuplicateNames.add(sourceName);
                }
                String sourceId = processorNameToIdMap.get(sourceName);
                if (!StringUtil.isNullOrEmpty(sourceId)) {
                    convert.setSourceId(sourceId);
                }
            }

            String destinationName = connection.getDestinationName();
            if (remoteInputPortIds.contains(destinationName)) {
                convert.setDestinationId(destinationName);
            } else {
                if (duplicateProcessorNames.contains(destinationName)) {
                    problematicDuplicateNames.add(destinationName);
                }
                String destinationId = processorNameToIdMap.get(destinationName);
                if (!StringUtil.isNullOrEmpty(destinationId)) {
                    convert.setDestinationId(destinationId);
                }
            }
            connectionSchemas.add(convert);
        }

        if (problematicDuplicateNames.size() > 0) {
            validationIssues.add(CANNOT_LOOK_UP_PROCESSOR_ID_FROM_PROCESSOR_NAME_DUE_TO_DUPLICATE_PROCESSOR_NAMES
                    + problematicDuplicateNames.stream().collect(Collectors.joining(", ")));
        }
        return connectionSchemas;
    }

    protected List<RemoteProcessGroupSchema> getRemoteProcessGroupSchemas() {
        Set<UUID> ids = new HashSet<>();
        List<RemoteProcessGroupSchema> rpgSchemas= new ArrayList<>(remoteProcessingGroups.size());

        for (RemoteProcessGroupSchemaV1 rpg : remoteProcessingGroups) {
            RemoteProcessGroupSchema rpgSchema = rpg.convert();
            rpgSchema.setId(getUniqueId(ids, rpgSchema.getName()));
            rpgSchemas.add(rpgSchema);
        }

        return rpgSchemas;

    }
    @Override
    public ConfigSchema convert() {
        Map<String, Object> map = new HashMap<>();
        map.put(VERSION, getVersion());
        putIfNotNull(map, FLOW_CONTROLLER_PROPS_KEY, flowControllerProperties);
        putIfNotNull(map, CORE_PROPS_KEY, coreProperties);
        putIfNotNull(map, FLOWFILE_REPO_KEY, flowfileRepositoryProperties);
        putIfNotNull(map, CONTENT_REPO_KEY, contentRepositoryProperties);
        putIfNotNull(map, PROVENANCE_REPO_KEY, provenanceRepositorySchema);
        putIfNotNull(map, COMPONENT_STATUS_REPO_KEY, componentStatusRepositoryProperties);
        putIfNotNull(map, SECURITY_PROPS_KEY, securityProperties);
        List<ProcessorSchema> processorSchemas = getProcessorSchemas();
        putListIfNotNull(map, PROCESSORS_KEY, processorSchemas);
        List<String> validationIssues = getValidationIssues();
        putListIfNotNull(map, CONNECTIONS_KEY, getConnectionSchemas(processorSchemas, validationIssues));
        putListIfNotNull(map, REMOTE_PROCESS_GROUPS_KEY, getRemoteProcessGroupSchemas());
        putIfNotNull(map, PROVENANCE_REPORTING_KEY, provenanceReportingProperties);
        return new ConfigSchema(map, validationIssues);
    }

    /**
     * Will deterministically (per config file in the case of collisions) map the name to a uuid.
     *
     * @param ids  the set of UUIDs already assigned
     * @param name the name
     * @return a UUID string
     */
    public static String getUniqueId(Set<UUID> ids, String name) {
        UUID id = UUID.nameUUIDFromBytes(name == null ? EMPTY_NAME.getBytes(StandardCharsets.UTF_8) : name.getBytes(StandardCharsets.UTF_8));
        while (ids.contains(id)) {
            id = new UUID(id.getMostSignificantBits(), id.getLeastSignificantBits() + 1);
        }
        ids.add(id);
        return id.toString();
    }

    @Override
    public int getVersion() {
        return CONFIG_VERSION;
    }
}
