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

package org.apache.nifi.minifi.toolkit.schema;

import org.apache.nifi.minifi.toolkit.schema.common.BaseSchema;
import org.apache.nifi.minifi.toolkit.schema.common.CollectionOverlap;
import org.apache.nifi.minifi.toolkit.schema.common.ConvertableSchema;
import org.apache.nifi.minifi.toolkit.schema.common.StringUtil;
import org.apache.nifi.minifi.toolkit.schema.common.WritableSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.COMPONENT_STATUS_REPO_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.CONTENT_REPO_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.CORE_PROPS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.FLOWFILE_REPO_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.FLOW_CONTROLLER_PROPS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.GENERAL_REPORTING_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.NIFI_PROPERTIES_OVERRIDES_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.PROVENANCE_REPORTING_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.PROVENANCE_REPO_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.SECURITY_PROPS_KEY;

public class ConfigSchema extends BaseSchema implements WritableSchema, ConvertableSchema<ConfigSchema> {
    public static final int CONFIG_VERSION = 3;
    public static final String VERSION = "MiNiFi Config Version";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_INPUT_PORT_IDS = "Found the following duplicate input port ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_OUTPUT_PORT_IDS = "Found the following duplicate output port ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_IDS = "Found the following ids that occur both in more than one Processor(s), Input Port(s), Output Port(s) and/or Remote Input Port(s): ";
    public static final String CONNECTION_WITH_ID = "Connection with id ";
    public static final String HAS_INVALID_SOURCE_ID = " has invalid source id ";
    public static final String HAS_INVALID_DESTINATION_ID = " has invalid destination id ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_PROCESSOR_IDS = "Found the following duplicate processor ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_CONTROLLER_SERVICE_IDS = "Found the following duplicate controller service ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_IDS = "Found the following duplicate connection ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_FUNNEL_IDS = "Found the following duplicate funnel ids: ";
    public static final String FOUND_THE_FOLLOWING_DUPLICATE_REPORTING_IDS = "Found the following duplicate reporting ids: ";
    public static String TOP_LEVEL_NAME = "top level";
    private FlowControllerSchema flowControllerProperties;
    private CorePropertiesSchema coreProperties;
    private FlowFileRepositorySchema flowfileRepositoryProperties;
    private ContentRepositorySchema contentRepositoryProperties;
    private ComponentStatusRepositorySchema componentStatusRepositoryProperties;
    private SecurityPropertiesSchema securityProperties;
    private ProcessGroupSchema processGroupSchema;
    private ProvenanceReportingSchema provenanceReportingProperties;
    private List<ReportingSchema> reportingTasks;

    private ProvenanceRepositorySchema provenanceRepositorySchema;

    private Map<String, String> nifiPropertiesOverrides;

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

        processGroupSchema = new ProcessGroupSchema(map, TOP_LEVEL_NAME);

        provenanceReportingProperties = getMapAsType(map, PROVENANCE_REPORTING_KEY, ProvenanceReportingSchema.class, TOP_LEVEL_NAME, false, false);
        reportingTasks = getOptionalKeyAsList(map, GENERAL_REPORTING_KEY, ReportingSchema::new, TOP_LEVEL_NAME);

        nifiPropertiesOverrides = (Map<String, String>) map.get(NIFI_PROPERTIES_OVERRIDES_KEY);
        if (nifiPropertiesOverrides == null) {
            nifiPropertiesOverrides = new HashMap<>();
        }

        addIssuesIfNotNull(flowControllerProperties);
        addIssuesIfNotNull(coreProperties);
        addIssuesIfNotNull(flowfileRepositoryProperties);
        addIssuesIfNotNull(contentRepositoryProperties);
        addIssuesIfNotNull(componentStatusRepositoryProperties);
        addIssuesIfNotNull(securityProperties);
        addIssuesIfNotNull(processGroupSchema);
        addIssuesIfNotNull(provenanceReportingProperties);
        addIssuesIfNotNull(reportingTasks);
        addIssuesIfNotNull(provenanceRepositorySchema);

        List<ProcessGroupSchema> allProcessGroups = getAllProcessGroups(processGroupSchema);
        List<ConnectionSchema> allConnectionSchemas = allProcessGroups.stream().flatMap(p -> p.getConnections().stream()).collect(Collectors.toList());
        List<RemoteProcessGroupSchema> allRemoteProcessGroups = allProcessGroups.stream().flatMap(p -> p.getRemoteProcessGroups().stream()).collect(Collectors.toList());

        List<String> allProcessorIds = allProcessGroups.stream().flatMap(p -> p.getProcessors().stream()).map(ProcessorSchema::getId).collect(Collectors.toList());
        List<String> allControllerServiceIds = allProcessGroups.stream().flatMap(p -> p.getControllerServices().stream()).map(ControllerServiceSchema::getId).collect(Collectors.toList());
        List<String> allFunnelIds = allProcessGroups.stream().flatMap(p -> p.getFunnels().stream()).map(FunnelSchema::getId).collect(Collectors.toList());
        List<String> allConnectionIds = allConnectionSchemas.stream().map(ConnectionSchema::getId).collect(Collectors.toList());
        List<String> allRemoteInputPortIds = allRemoteProcessGroups.stream().filter(r -> r.getInputPorts() != null)
                .flatMap(r -> r.getInputPorts().stream()).map(RemotePortSchema::getId).collect(Collectors.toList());
        List<String> allRemoteOutputPortIds = allRemoteProcessGroups.stream().filter(r -> r.getOutputPorts() != null)
                .flatMap(r -> r.getOutputPorts().stream()).map(RemotePortSchema::getId).collect(Collectors.toList());
        List<String> allInputPortIds = allProcessGroups.stream().flatMap(p -> p.getInputPortSchemas().stream()).map(PortSchema::getId).collect(Collectors.toList());
        List<String> allOutputPortIds = allProcessGroups.stream().flatMap(p -> p.getOutputPortSchemas().stream()).map(PortSchema::getId).collect(Collectors.toList());
        List<String> allReportingIds = reportingTasks.stream().map(ReportingSchema::getId).collect(Collectors.toList());

        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_PROCESSOR_IDS, allProcessorIds);
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_CONTROLLER_SERVICE_IDS, allControllerServiceIds);
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_FUNNEL_IDS, allFunnelIds);
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_IDS, allConnectionIds);
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_INPUT_PORT_IDS, allInputPortIds);
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_OUTPUT_PORT_IDS, allOutputPortIds);
        checkForDuplicates(this::addValidationIssue, FOUND_THE_FOLLOWING_DUPLICATE_REPORTING_IDS, allReportingIds);

        // Potential connection sources and destinations need to have unique ids
        CollectionOverlap<String> overlapResults = new CollectionOverlap<>(new HashSet<>(allProcessorIds), new HashSet<>(allRemoteInputPortIds), new HashSet<>(allRemoteOutputPortIds),
                new HashSet<>(allInputPortIds), new HashSet<>(allOutputPortIds), new HashSet<>(allFunnelIds));
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

    public static List<ProcessGroupSchema> getAllProcessGroups(ProcessGroupSchema processGroupSchema) {
        List<ProcessGroupSchema> result = new ArrayList<>();
        addProcessGroups(processGroupSchema, result);
        return result;
    }

    private static void addProcessGroups(ProcessGroupSchema processGroupSchema, List<ProcessGroupSchema> result) {
        result.add(processGroupSchema);
        processGroupSchema.getProcessGroupSchemas().forEach(p -> addProcessGroups(p, result));
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
        result.putAll(processGroupSchema.toMap());
        putIfNotNull(result, PROVENANCE_REPORTING_KEY, provenanceReportingProperties);
        if (!reportingTasks.isEmpty()) {
            // for backward compatibility
            putListIfNotNull(result, GENERAL_REPORTING_KEY, reportingTasks);
        }
        result.put(NIFI_PROPERTIES_OVERRIDES_KEY, nifiPropertiesOverrides);
        return result;
    }

    public FlowControllerSchema getFlowControllerProperties() {
        return flowControllerProperties;
    }

    public void setFlowControllerProperties(final FlowControllerSchema flowControllerProperties) {
        this.flowControllerProperties = flowControllerProperties;
    }

    public CorePropertiesSchema getCoreProperties() {
        return coreProperties;
    }

    public void setCoreProperties(final CorePropertiesSchema coreProperties) {
        this.coreProperties = coreProperties;
    }

    public FlowFileRepositorySchema getFlowfileRepositoryProperties() {
        return flowfileRepositoryProperties;
    }

    public void setFlowfileRepositoryProperties(final FlowFileRepositorySchema flowfileRepositoryProperties) {
        this.flowfileRepositoryProperties = flowfileRepositoryProperties;
    }

    public ContentRepositorySchema getContentRepositoryProperties() {
        return contentRepositoryProperties;
    }

    public void setContentRepositoryProperties(final ContentRepositorySchema contentRepositoryProperties) {
        this.contentRepositoryProperties = contentRepositoryProperties;
    }

    public SecurityPropertiesSchema getSecurityProperties() {
        return securityProperties;
    }

    public void setSecurityProperties(SecurityPropertiesSchema securityProperties) {
        this.securityProperties = securityProperties;
    }

    public ProcessGroupSchema getProcessGroupSchema() {
        return processGroupSchema;
    }

    public void setProcessGroupSchema(final ProcessGroupSchema processGroupSchema) {
        this.processGroupSchema = processGroupSchema;
    }

    public ProvenanceReportingSchema getProvenanceReportingProperties() {
        return provenanceReportingProperties;
    }

    public void setProvenanceReportingProperties(ProvenanceReportingSchema provenanceReportingProperties) {
        this.provenanceReportingProperties = provenanceReportingProperties;
    }

    public List<ReportingSchema> getReportingTasksSchema() {
        return reportingTasks;
    }

    public void setReportingTasks(final List<ReportingSchema> reportingTasks) {
        this.reportingTasks = reportingTasks;
    }

    public ComponentStatusRepositorySchema getComponentStatusRepositoryProperties() {
        return componentStatusRepositoryProperties;
    }

    public void setComponentStatusRepositoryProperties(final ComponentStatusRepositorySchema componentStatusRepositoryProperties) {
        this.componentStatusRepositoryProperties = componentStatusRepositoryProperties;
    }

    public ProvenanceRepositorySchema getProvenanceRepositorySchema() {
        return provenanceRepositorySchema;
    }

    public void setProvenanceRepositorySchema(final ProvenanceRepositorySchema provenanceRepositorySchema) {
        this.provenanceRepositorySchema = provenanceRepositorySchema;
    }

    public Map<String, String> getNifiPropertiesOverrides() {
        return nifiPropertiesOverrides;
    }

    public void setNifiPropertiesOverrides(final Map<String, String> nifiPropertiesOverrides) {
        this.nifiPropertiesOverrides = nifiPropertiesOverrides;
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
