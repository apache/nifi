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

package org.apache.nifi.minifi.toolkit.schema.v2;

import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.COMPONENT_STATUS_REPO_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.CONTENT_REPO_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.CORE_PROPS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.FLOWFILE_REPO_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.FLOW_CONTROLLER_PROPS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.PROVENANCE_REPORTING_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.PROVENANCE_REPO_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.SECURITY_PROPS_KEY;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.nifi.minifi.toolkit.schema.ComponentStatusRepositorySchema;
import org.apache.nifi.minifi.toolkit.schema.ConfigSchema;
import org.apache.nifi.minifi.toolkit.schema.ConnectionSchema;
import org.apache.nifi.minifi.toolkit.schema.ContentRepositorySchema;
import org.apache.nifi.minifi.toolkit.schema.FlowControllerSchema;
import org.apache.nifi.minifi.toolkit.schema.FlowFileRepositorySchema;
import org.apache.nifi.minifi.toolkit.schema.FunnelSchema;
import org.apache.nifi.minifi.toolkit.schema.PortSchema;
import org.apache.nifi.minifi.toolkit.schema.ProcessorSchema;
import org.apache.nifi.minifi.toolkit.schema.ProvenanceReportingSchema;
import org.apache.nifi.minifi.toolkit.schema.ProvenanceRepositorySchema;
import org.apache.nifi.minifi.toolkit.schema.RemotePortSchema;
import org.apache.nifi.minifi.toolkit.schema.SecurityPropertiesSchema;
import org.apache.nifi.minifi.toolkit.schema.common.BaseSchema;
import org.apache.nifi.minifi.toolkit.schema.common.CollectionOverlap;
import org.apache.nifi.minifi.toolkit.schema.common.ConvertableSchema;
import org.apache.nifi.minifi.toolkit.schema.common.StringUtil;

public class ConfigSchemaV2 extends BaseSchema implements ConvertableSchema<ConfigSchema> {
    public static final int CONFIG_VERSION = 2;
    public static final String VERSION = "MiNiFi Config Version";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_INPUT_PORT_IDS = "Found the following duplicate input port ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_OUTPUT_PORT_IDS = "Found the following duplicate output port ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_IDS = "Found the following ids that occur both in more than one Processor(s), Input Port(s), Output Port(s) and/or Remote Input Port(s): ";
    public static final String CONNECTION_WITH_ID = "Connection with id ";
    public static final String HAS_INVALID_SOURCE_ID = " has invalid source id ";
    public static final String HAS_INVALID_DESTINATION_ID = " has invalid destination id ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_PROCESSOR_IDS = "Found the following duplicate processor ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_IDS = "Found the following duplicate connection ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_FUNNEL_IDS = "Found the following duplicate funnel ids: ";
    public static String TOP_LEVEL_NAME = "top level";

    private FlowControllerSchema flowControllerProperties;
    private CorePropertiesSchemaV2 coreProperties;
    private FlowFileRepositorySchema flowfileRepositoryProperties;
    private ContentRepositorySchema contentRepositoryProperties;
    private ComponentStatusRepositorySchema componentStatusRepositoryProperties;
    private SecurityPropertiesSchema securityProperties;
    private ProcessGroupSchemaV2 processGroupSchema;
    private ProvenanceReportingSchema provenanceReportingProperties;
    private ProvenanceRepositorySchema provenanceRepositorySchema;

    public ConfigSchemaV2(Map map) {
        this(map, Collections.emptyList());
    }

    public ConfigSchemaV2(Map map, List<String> validationIssues) {
        validationIssues.stream().forEach(this::addValidationIssue);
        flowControllerProperties = getMapAsType(map, FLOW_CONTROLLER_PROPS_KEY, FlowControllerSchema.class, TOP_LEVEL_NAME, true);

        coreProperties = getMapAsType(map, CORE_PROPS_KEY, CorePropertiesSchemaV2.class, TOP_LEVEL_NAME, false);
        flowfileRepositoryProperties = getMapAsType(map, FLOWFILE_REPO_KEY, FlowFileRepositorySchema.class, TOP_LEVEL_NAME, false);
        contentRepositoryProperties = getMapAsType(map, CONTENT_REPO_KEY, ContentRepositorySchema.class, TOP_LEVEL_NAME, false);
        provenanceRepositorySchema = getMapAsType(map, PROVENANCE_REPO_KEY, ProvenanceRepositorySchema.class, TOP_LEVEL_NAME, false);
        componentStatusRepositoryProperties = getMapAsType(map, COMPONENT_STATUS_REPO_KEY, ComponentStatusRepositorySchema.class, TOP_LEVEL_NAME, false);
        securityProperties = getMapAsType(map, SECURITY_PROPS_KEY, SecurityPropertiesSchema.class, TOP_LEVEL_NAME, false);

        processGroupSchema = new ProcessGroupSchemaV2(map, TOP_LEVEL_NAME);

        provenanceReportingProperties = getMapAsType(map, PROVENANCE_REPORTING_KEY, ProvenanceReportingSchema.class, TOP_LEVEL_NAME, false, false);

        addIssuesIfNotNull(flowControllerProperties);
        addIssuesIfNotNull(coreProperties);
        addIssuesIfNotNull(flowfileRepositoryProperties);
        addIssuesIfNotNull(contentRepositoryProperties);
        addIssuesIfNotNull(componentStatusRepositoryProperties);
        addIssuesIfNotNull(securityProperties);
        addIssuesIfNotNull(processGroupSchema);
        addIssuesIfNotNull(provenanceReportingProperties);
        addIssuesIfNotNull(provenanceRepositorySchema);

        List<ProcessGroupSchemaV2> allProcessGroups = getAllProcessGroups(processGroupSchema);
        List<ConnectionSchema> allConnectionSchemas = allProcessGroups.stream().flatMap(p -> p.getConnections().stream()).collect(Collectors.toList());
        List<RemoteProcessGroupSchemaV2> allRemoteProcessGroups = allProcessGroups.stream().flatMap(p -> p.getRemoteProcessGroups().stream()).collect(Collectors.toList());

        List<String> allProcessorIds = allProcessGroups.stream().flatMap(p -> p.getProcessors().stream()).map(ProcessorSchema::getId).collect(Collectors.toList());
        List<String> allFunnelIds = allProcessGroups.stream().flatMap(p -> p.getFunnels().stream()).map(FunnelSchema::getId).collect(Collectors.toList());
        List<String> allConnectionIds = allConnectionSchemas.stream().map(ConnectionSchema::getId).collect(Collectors.toList());
        List<String> allRemoteInputPortIds = allRemoteProcessGroups.stream().filter(r -> r.getInputPorts() != null)
                .flatMap(r -> r.getInputPorts().stream()).map(RemotePortSchema::getId).collect(Collectors.toList());
        List<String> allInputPortIds = allProcessGroups.stream().flatMap(p -> p.getInputPortSchemas().stream()).map(PortSchema::getId).collect(Collectors.toList());
        List<String> allOutputPortIds = allProcessGroups.stream().flatMap(p -> p.getOutputPortSchemas().stream()).map(PortSchema::getId).collect(Collectors.toList());

        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_PROCESSOR_IDS, allProcessorIds);
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_FUNNEL_IDS, allFunnelIds);
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_IDS, allConnectionIds);
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_INPUT_PORT_IDS, allInputPortIds);
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_OUTPUT_PORT_IDS, allOutputPortIds);

        // Potential connection sources and destinations need to have unique ids
        CollectionOverlap<String> overlapResults = new CollectionOverlap<>(new HashSet<>(allProcessorIds), new HashSet<>(allRemoteInputPortIds), new HashSet<>(allInputPortIds),
                new HashSet<>(allOutputPortIds), new HashSet<>(allFunnelIds));
        if (overlapResults.getDuplicates().size() > 0) {
            addValidationIssue(FOUND_THE_FOLLOWING_DUPLICATE_IDS + overlapResults.getDuplicates().stream().sorted().collect(Collectors.joining(", ")));
        }

        allConnectionSchemas.forEach(c -> {
            String destinationId = c.getDestinationId();
            if (!StringUtil.isNullOrEmpty(destinationId) && !overlapResults.getElements().contains(destinationId)) {
                addValidationIssue(CONNECTION_WITH_ID + c.getId() + HAS_INVALID_DESTINATION_ID + destinationId);
            }
            String sourceId = c.getSourceId();
            if (!StringUtil.isNullOrEmpty(sourceId) && !overlapResults.getElements().contains(sourceId)) {
                addValidationIssue(CONNECTION_WITH_ID + c.getId() + HAS_INVALID_SOURCE_ID + sourceId);
            }
        });
    }

    public static List<ProcessGroupSchemaV2> getAllProcessGroups(ProcessGroupSchemaV2 processGroupSchema) {
        List<ProcessGroupSchemaV2> result = new ArrayList<>();
        addProcessGroups(processGroupSchema, result);
        return result;
    }

    private static void addProcessGroups(ProcessGroupSchemaV2 processGroupSchema, List<ProcessGroupSchemaV2> result) {
        result.add(processGroupSchema);
        processGroupSchema.getProcessGroupSchemas().forEach(p -> addProcessGroups(p, result));
    }

    @Override
    public int getVersion() {
        return CONFIG_VERSION;
    }

    @Override
    public ConfigSchema convert() {
        Map<String, Object> result = mapSupplier.get();
        result.put(VERSION, getVersion());
        putIfNotNull(result, FLOW_CONTROLLER_PROPS_KEY, flowControllerProperties);
        putIfNotNull(result, CORE_PROPS_KEY, coreProperties.convert());
        putIfNotNull(result, FLOWFILE_REPO_KEY, flowfileRepositoryProperties);
        putIfNotNull(result, CONTENT_REPO_KEY, contentRepositoryProperties);
        putIfNotNull(result, PROVENANCE_REPO_KEY, provenanceRepositorySchema);
        putIfNotNull(result, COMPONENT_STATUS_REPO_KEY, componentStatusRepositoryProperties);
        putIfNotNull(result, SECURITY_PROPS_KEY, securityProperties);
        result.putAll(processGroupSchema.toMap());
        putIfNotNull(result, PROVENANCE_REPORTING_KEY, provenanceReportingProperties);
        return new ConfigSchema(result, getValidationIssues());
    }
}
