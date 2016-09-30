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
import org.apache.nifi.minifi.commons.schema.common.StringUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
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
public class ConfigSchema extends BaseSchema {
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_PROCESSOR_IDS = "Found the following duplicate processor ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_IDS = "Found the following duplicate connection ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_REMOTE_PROCESSING_GROUP_NAMES = "Found the following duplicate remote processing group names: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_REMOTE_INPUT_PORT_IDS = "Found the following duplicate remote input port ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_IDS = "Found the following ids that occur both in Processors and Remote Input Ports: ";
    public static final String CANNOT_LOOK_UP_PROCESSOR_ID_FROM_PROCESSOR_NAME_DUE_TO_DUPLICATE_PROCESSOR_NAMES = "Cannot look up Processor id from Processor name due to duplicate Processor names: ";
    public static final int CONFIG_VERSION = 2;
    public static final String CONNECTIONS_REFER_TO_PROCESSOR_NAMES_THAT_DONT_EXIST = "Connection(s) refer to Processor names that don't exist: ";
    public static String TOP_LEVEL_NAME = "top level";
    public static final String VERSION = "MiNiFi Config Version";
    public static final String EMPTY_NAME = "empty_name";
    public static final Pattern ID_REPLACE_PATTERN = Pattern.compile("[^A-Za-z0-9_-]");

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
        flowControllerProperties = getMapAsType(map, FLOW_CONTROLLER_PROPS_KEY, FlowControllerSchema.class, TOP_LEVEL_NAME, true);

        coreProperties = getMapAsType(map, CORE_PROPS_KEY, CorePropertiesSchema.class, TOP_LEVEL_NAME, false);
        flowfileRepositoryProperties = getMapAsType(map, FLOWFILE_REPO_KEY, FlowFileRepositorySchema.class, TOP_LEVEL_NAME, false);
        contentRepositoryProperties = getMapAsType(map, CONTENT_REPO_KEY, ContentRepositorySchema.class, TOP_LEVEL_NAME, false);
        provenanceRepositorySchema = getMapAsType(map, PROVENANCE_REPO_KEY, ProvenanceRepositorySchema.class, TOP_LEVEL_NAME, false);
        componentStatusRepositoryProperties = getMapAsType(map, COMPONENT_STATUS_REPO_KEY, ComponentStatusRepositorySchema.class, TOP_LEVEL_NAME, false);
        securityProperties = getMapAsType(map, SECURITY_PROPS_KEY, SecurityPropertiesSchema.class, TOP_LEVEL_NAME, false);

        processors = getProcessorSchemas(getOptionalKeyAsType(map, PROCESSORS_KEY, List.class, TOP_LEVEL_NAME, null));

        remoteProcessingGroups = convertListToType(getOptionalKeyAsType(map, REMOTE_PROCESSING_GROUPS_KEY, List.class, TOP_LEVEL_NAME, null), "remote processing group",
                RemoteProcessingGroupSchema.class, REMOTE_PROCESSING_GROUPS_KEY);

        connections = getConnectionSchemas(getOptionalKeyAsType(map, CONNECTIONS_KEY, List.class, TOP_LEVEL_NAME, null));

        provenanceReportingProperties = getMapAsType(map, PROVENANCE_REPORTING_KEY, ProvenanceReportingSchema.class, TOP_LEVEL_NAME, false, false);

        addIssuesIfNotNull(flowControllerProperties);
        addIssuesIfNotNull(coreProperties);
        addIssuesIfNotNull(flowfileRepositoryProperties);
        addIssuesIfNotNull(contentRepositoryProperties);
        addIssuesIfNotNull(componentStatusRepositoryProperties);
        addIssuesIfNotNull(securityProperties);
        addIssuesIfNotNull(provenanceReportingProperties);
        addIssuesIfNotNull(provenanceRepositorySchema);

        Set<String> processorIds = new HashSet<>();
        if (processors != null) {
            List<String> processorIdList = processors.stream().map(ProcessorSchema::getId).collect(Collectors.toList());
            checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_PROCESSOR_IDS, processorIdList);
            for (ProcessorSchema processorSchema : processors) {
                addIssuesIfNotNull(processorSchema);
            }
            processorIds.addAll(processorIdList);
        }

        if (connections != null) {
            List<String> idList = connections.stream().map(ConnectionSchema::getId).filter(s -> !StringUtil.isNullOrEmpty(s)).collect(Collectors.toList());
            checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_IDS, idList);
            for (ConnectionSchema connectionSchema : connections) {
                addIssuesIfNotNull(connectionSchema);
            }
        }

        Set<String> remoteInputPortIds = new HashSet<>();
        if (remoteProcessingGroups != null) {
            checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_REMOTE_PROCESSING_GROUP_NAMES,
                    remoteProcessingGroups.stream().map(RemoteProcessingGroupSchema::getName).collect(Collectors.toList()));
            for (RemoteProcessingGroupSchema remoteProcessingGroupSchema : remoteProcessingGroups) {
                addIssuesIfNotNull(remoteProcessingGroupSchema);
            }
            List<RemoteProcessingGroupSchema> remoteProcessingGroups = getRemoteProcessingGroups();
            if (remoteProcessingGroups != null) {
                List<String> remoteInputPortIdList = remoteProcessingGroups.stream().filter(r -> r.getInputPorts() != null)
                        .flatMap(r -> r.getInputPorts().stream()).map(RemoteInputPortSchema::getId).collect(Collectors.toList());
                checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_REMOTE_INPUT_PORT_IDS, remoteInputPortIdList);
                remoteInputPortIds.addAll(remoteInputPortIdList);
            }
        }

        Set<String> duplicateIds = new HashSet<>(processorIds);
        duplicateIds.retainAll(remoteInputPortIds);
        if (duplicateIds.size() > 0) {
            addValidationIssue(FOUND_THE_FOLLOWING_DUPLICATE_IDS + duplicateIds.stream().sorted().collect(Collectors.joining(", ")));
        }
    }

    protected List<ProcessorSchema> getProcessorSchemas(List<Map> processorMaps) {
        if (processorMaps == null) {
            return null;
        }
        List<ProcessorSchema> processors = convertListToType(processorMaps, "processor", ProcessorSchema.class, PROCESSORS_KEY);

        Map<String, Integer> idMap = processors.stream().map(ProcessorSchema::getId).filter(
                s -> !StringUtil.isNullOrEmpty(s)).collect(Collectors.toMap(Function.identity(), s -> 2, Integer::compareTo));

        // Set unset ids
        processors.stream().filter(connection -> StringUtil.isNullOrEmpty(connection.getId())).forEachOrdered(processor -> processor.setId(getUniqueId(idMap, processor.getName())));

        return processors;
    }

    protected List<ConnectionSchema> getConnectionSchemas(List<Map> connectionMaps) {
        if (connectionMaps == null) {
            return null;
        }
        List<ConnectionSchema> connections = convertListToType(connectionMaps, "connection", ConnectionSchema.class, CONNECTIONS_KEY);
        Map<String, Integer> idMap = connections.stream().map(ConnectionSchema::getId).filter(
                s -> !StringUtil.isNullOrEmpty(s)).collect(Collectors.toMap(Function.identity(), s -> 2, Integer::compareTo));

        Map<String, String> processorNameToIdMap = new HashMap<>();

        // We can't look up id by name for names that appear more than once
        Set<String> duplicateProcessorNames = new HashSet<>();

        List<ProcessorSchema> processors = getProcessors();
        if (processors != null) {
            processors.stream().forEachOrdered(p -> processorNameToIdMap.put(p.getName(), p.getId()));

            Set<String> processorNames = new HashSet<>();
            processors.stream().map(ProcessorSchema::getName).forEachOrdered(n -> {
                if (!processorNames.add(n)) {
                    duplicateProcessorNames.add(n);
                }
            });
        }

        Set<String> remoteInputPortIds = new HashSet<>();
        List<RemoteProcessingGroupSchema> remoteProcessingGroups = getRemoteProcessingGroups();
        if (remoteProcessingGroups != null) {
            remoteInputPortIds.addAll(remoteProcessingGroups.stream().filter(r -> r.getInputPorts() != null)
                    .flatMap(r -> r.getInputPorts().stream()).map(RemoteInputPortSchema::getId).collect(Collectors.toSet()));
        }

        Set<String> problematicDuplicateNames = new HashSet<>();
        Set<String> missingProcessorNames = new HashSet<>();
        // Set unset ids
        connections.stream().filter(connection -> StringUtil.isNullOrEmpty(connection.getId())).forEachOrdered(connection -> connection.setId(getUniqueId(idMap, connection.getName())));

        connections.stream().filter(connection -> StringUtil.isNullOrEmpty(connection.getSourceId())).forEach(connection -> {
            String sourceName = connection.getSourceName();
            if (remoteInputPortIds.contains(sourceName)) {
                connection.setSourceId(sourceName);
            } else {
                if (duplicateProcessorNames.contains(sourceName)) {
                    problematicDuplicateNames.add(sourceName);
                }
                String sourceId = processorNameToIdMap.get(sourceName);
                if (StringUtil.isNullOrEmpty(sourceId)) {
                    missingProcessorNames.add(sourceName);
                } else {
                    connection.setSourceId(sourceId);
                }
            }
        });

        connections.stream().filter(connection -> StringUtil.isNullOrEmpty(connection.getDestinationId()))
                .forEach(connection -> {
                    String destinationName = connection.getDestinationName();
                    if (remoteInputPortIds.contains(destinationName)) {
                        connection.setDestinationId(destinationName);
                    } else {
                        if (duplicateProcessorNames.contains(destinationName)) {
                            problematicDuplicateNames.add(destinationName);
                        }
                        String destinationId = processorNameToIdMap.get(destinationName);
                        if (StringUtil.isNullOrEmpty(destinationId)) {
                            missingProcessorNames.add(destinationName);
                        } else {
                            connection.setDestinationId(destinationId);
                        }
                    }
                });

        if (problematicDuplicateNames.size() > 0) {
            addValidationIssue(CANNOT_LOOK_UP_PROCESSOR_ID_FROM_PROCESSOR_NAME_DUE_TO_DUPLICATE_PROCESSOR_NAMES
                    + problematicDuplicateNames.stream().collect(Collectors.joining(", ")));
        }
        if (missingProcessorNames.size() > 0) {
            addValidationIssue(CONNECTIONS_REFER_TO_PROCESSOR_NAMES_THAT_DONT_EXIST + missingProcessorNames.stream().sorted().collect(Collectors.joining(", ")));
        }
        return connections;
    }

    protected static void checkForDuplicates(Consumer<String> duplicateMessageConsumer, String errorMessagePrefix, List<String> strings) {
        if (strings != null) {
            Set<String> seen = new HashSet<>();
            Set<String> duplicates = new TreeSet<>();
            for (String string : strings) {
                if (!seen.add(string)) {
                    duplicates.add(String.valueOf(string));
                }
            }
            if (duplicates.size() > 0) {
                StringBuilder errorMessage = new StringBuilder(errorMessagePrefix);
                for (String duplicateName : duplicates) {
                    errorMessage.append(duplicateName);
                    errorMessage.append(", ");
                }
                errorMessage.setLength(errorMessage.length() - 2);
                duplicateMessageConsumer.accept(errorMessage.toString());
            }
        }
    }

    public Map<String, Object> toMap() {
        Map<String, Object> result = mapSupplier.get();
        result.put(VERSION, getVersion());
        result.put(FLOW_CONTROLLER_PROPS_KEY, flowControllerProperties.toMap());
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

    public int getVersion() {
        return CONFIG_VERSION;
    }

    /**
     * Will replace all characters not in [A-Za-z0-9_] with _
     * <p>
     * This has potential for collisions so it will also append numbers as necessary to prevent that
     *
     * @param ids  id map of already incremented numbers
     * @param name the name
     * @return a unique filesystem-friendly id
     */
    protected static String getUniqueId(Map<String, Integer> ids, String name) {
        String baseId = StringUtil.isNullOrEmpty(name) ? EMPTY_NAME : ID_REPLACE_PATTERN.matcher(name).replaceAll("_");
        String id = baseId;
        Integer idNum = ids.get(baseId);
        while (ids.containsKey(id)) {
            id = baseId + "_" + idNum++;
        }
        // Using != on a string comparison here is intentional.  The two will be reference equal iff the body of the while loop was never executed.
        if (id != baseId) {
            ids.put(baseId, idNum);
        }
        ids.put(id, 2);
        return id;
    }
}
