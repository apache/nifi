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
package org.apache.nifi.connectors.tests.system;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.AssetReference;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.ConnectorPropertyValue;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.migration.ConnectorMigrationContext;
import org.apache.nifi.components.connector.migration.MigratableConnector;
import org.apache.nifi.flow.VersionedAsset;
import org.apache.nifi.flow.VersionedComponentState;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test connector used to verify version-controlled flow migration into a Connector.
 *
 * <h2>Persistence model</h2>
 *
 * <p>
 * The framework persists a Connector's <em>configuration</em> (the properties of its
 * {@link ConfigurationStep ConfigurationSteps}) to {@code flow.json.gz}. It does <strong>not</strong> persist the
 * managed Process Group that the Connector exposes through its {@link FlowContext}. On restart the framework
 * rehydrates the persisted configuration and calls {@link #applyUpdate(FlowContext, FlowContext)}; the Connector is
 * then responsible for rebuilding its managed Process Group from that configuration.
 *
 * <h2>Two-phase migration</h2>
 *
 * <p>
 * Migration runs in two phases driven by the framework:
 * </p>
 * <ol>
 *     <li>{@link #migrateConfiguration(ConnectorMigrationContext)} serializes the source flow to JSON and records it
 *         on this connector's configuration via
 *         {@link ConnectorMigrationContext#setProperties(String, java.util.Map) setProperties(...)}. The framework
 *         applies that recorded configuration to the working configuration and drives
 *         {@code applyUpdate(workingContext, activeContext)} so the managed Process Group is rebuilt from the
 *         merged configuration before phase 2 runs.</li>
 *     <li>{@link #migrateState(ConnectorMigrationContext)} walks the source flow's
 *         {@link VersionedConfigurableExtension} components and, for each one that carries a non-null
 *         {@link VersionedComponentState}, records it via
 *         {@link ConnectorMigrationContext#setComponentState(String, VersionedComponentState) setComponentState(...)}
 *         so the framework writes the state into the live {@code StateManager} of the corresponding managed
 *         component.</li>
 * </ol>
 *
 * <p>
 * {@link #applyUpdate(FlowContext, FlowContext)} reads the persisted source-flow JSON back from the active
 * configuration and calls {@code getInitializationContext().updateFlow(activeFlowContext, migratedFlow)} to install
 * the managed Process Group. The same call path runs on every restart because the framework drives
 * {@code applyUpdate(...)} via {@code inheritConfiguration(...)} during flow load, so the migrated flow survives
 * restarts without the connector having to mirror anything outside its own configuration.
 * </p>
 */
public class MigrationTargetConnector extends AbstractConnector implements MigratableConnector {
    private static final String REQUIRED_PARAMETER_NAME = "Source Topic";
    private static final String REQUIRED_PROCESSOR_TYPE = "org.apache.nifi.processors.tests.system.StatefulCountProcessor";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Name of the configuration step that holds the persisted migration state. The step is intentionally present
     * even before any migration occurs so the framework persists an entry for it and the connector can later add or
     * replace its single property.
     */
    static final String MIGRATION_STATE_STEP_NAME = "Migration State";

    /**
     * Property used to persist the most recently migrated source flow as JSON. The property is set during
     * {@link #migrateConfiguration(ConnectorMigrationContext)} via
     * {@link ConnectorMigrationContext#setProperties(String, java.util.Map) setProperties(...)} and read by
     * {@link #applyUpdate(FlowContext, FlowContext)} to rebuild the managed Process Group on every restart and right
     * after migration.
     */
    static final ConnectorPropertyDescriptor MIGRATED_SOURCE_FLOW_PROPERTY = new ConnectorPropertyDescriptor.Builder()
            .name("Migrated Source Flow JSON")
            .description("Holds the JSON-serialized source flow that was applied during the most recent migration. "
                    + "Used to rebuild the managed Process Group on restart, since the framework only persists configuration.")
            .build();

    private static final ConnectorPropertyGroup MIGRATION_STATE_PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
            .name("Migration State")
            .description("Internal state captured during migration so the migrated flow survives a restart.")
            .addProperty(MIGRATED_SOURCE_FLOW_PROPERTY)
            .build();

    private static final ConfigurationStep MIGRATION_STATE_STEP = new ConfigurationStep.Builder()
            .name(MIGRATION_STATE_STEP_NAME)
            .description("Holds state captured during migration. This step is updated by the Connector itself, not by the user.")
            .propertyGroups(List.of(MIGRATION_STATE_PROPERTY_GROUP))
            .build();

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return emptyVersionedExternalFlow("Migration Target Flow");
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        final VersionedExternalFlow migratedFlow = readPersistedMigratedFlow(activeFlowContext);
        return migratedFlow == null ? getInitialFlow() : migratedFlow;
    }

    @Override
    public boolean isMigrationSupported(final ConnectorMigrationContext context) {
        final VersionedExternalFlow sourceFlow = context.getSourceFlow();
        if (sourceFlow == null || sourceFlow.getFlowContents() == null) {
            return false;
        }

        return containsRequiredParameter(sourceFlow.getParameterContexts()) && containsRequiredProcessor(sourceFlow.getFlowContents());
    }

    @Override
    public void migrateConfiguration(final ConnectorMigrationContext context) throws FlowUpdateException {
        final VersionedExternalFlow sourceFlow = context.getSourceFlow();
        if (sourceFlow == null) {
            throw new FlowUpdateException("A source flow is required for migration");
        }

        rewriteReferencedAssets(sourceFlow, context);

        // Serialize the source flow and record it as a property on this connector's configuration. The framework
        // applies this change to the working configuration and then drives applyUpdate(workingContext,
        // activeContext); applyUpdate(...) reads the persisted JSON and calls
        // getInitializationContext().updateFlow(...) to install the managed Process Group. The same applyUpdate(...)
        // path runs on every restart, so the migrated flow survives restarts without storing anything outside the
        // connector's own configuration.
        final String sourceFlowJson;
        try {
            sourceFlowJson = OBJECT_MAPPER.writeValueAsString(sourceFlow);
        } catch (final JsonProcessingException e) {
            throw new FlowUpdateException("Failed to serialize migrated source flow for persistence", e);
        }

        context.setProperties(MIGRATION_STATE_STEP_NAME, Map.of(MIGRATED_SOURCE_FLOW_PROPERTY.getName(), sourceFlowJson));
    }

    @Override
    public void migrateState(final ConnectorMigrationContext context) throws FlowUpdateException {
        // Walk the source flow's processors and controller services and record the StateManager state the framework
        // should seed onto the corresponding managed component. The managed components were created when the
        // framework drove applyUpdate(...) at the end of phase 1, so each source versioned identifier maps to a
        // real component in the connector's managed Process Group at this point.
        final VersionedExternalFlow sourceFlow = context.getSourceFlow();
        if (sourceFlow == null || sourceFlow.getFlowContents() == null) {
            return;
        }
        recordComponentStates(sourceFlow.getFlowContents(), context);
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        return List.of();
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(MIGRATION_STATE_STEP);
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) throws FlowUpdateException {
        // applyUpdate(...) is called by the framework both during normal property updates and during restart (via
        // inheritConfiguration), as well as right after migrateConfiguration(...) returns. This is the single point
        // at which the managed Process Group is rebuilt from the connector's persisted configuration. When no
        // migration has been performed there is nothing to apply; the empty initial flow installed by
        // loadInitialFlow() remains in place.
        final VersionedExternalFlow migratedFlow = readPersistedMigratedFlow(workingFlowContext);
        if (migratedFlow == null) {
            return;
        }

        getInitializationContext().updateFlow(activeFlowContext, migratedFlow);
    }

    private boolean containsRequiredParameter(final Map<String, VersionedParameterContext> parameterContexts) {
        if (parameterContexts == null || parameterContexts.isEmpty()) {
            return false;
        }

        for (final VersionedParameterContext parameterContext : parameterContexts.values()) {
            final Set<VersionedParameter> parameters = parameterContext.getParameters();
            if (parameters == null) {
                continue;
            }

            for (final VersionedParameter parameter : parameters) {
                if (REQUIRED_PARAMETER_NAME.equals(parameter.getName())) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean containsRequiredProcessor(final VersionedProcessGroup processGroup) {
        final Set<VersionedProcessor> processors = processGroup.getProcessors();
        if (processors != null) {
            for (final VersionedProcessor processor : processors) {
                if (REQUIRED_PROCESSOR_TYPE.equals(processor.getType())) {
                    return true;
                }
            }
        }

        final Set<VersionedProcessGroup> childGroups = processGroup.getProcessGroups();
        if (childGroups == null) {
            return false;
        }

        for (final VersionedProcessGroup childGroup : childGroups) {
            if (containsRequiredProcessor(childGroup)) {
                return true;
            }
        }

        return false;
    }

    private void rewriteReferencedAssets(final VersionedExternalFlow sourceFlow, final ConnectorMigrationContext context) {
        final Map<String, VersionedParameterContext> parameterContexts = sourceFlow.getParameterContexts();
        if (parameterContexts == null || parameterContexts.isEmpty()) {
            return;
        }

        for (final VersionedParameterContext parameterContext : parameterContexts.values()) {
            final Set<VersionedParameter> parameters = parameterContext.getParameters();
            if (parameters == null) {
                continue;
            }

            for (final VersionedParameter parameter : parameters) {
                final List<VersionedAsset> referencedAssets = parameter.getReferencedAssets();
                if (referencedAssets == null || referencedAssets.isEmpty()) {
                    continue;
                }

                final List<VersionedAsset> migratedAssets = new ArrayList<>();
                for (final VersionedAsset referencedAsset : referencedAssets) {
                    final AssetReference migratedReference = context.copyAssetFromSource(referencedAsset.getIdentifier());
                    for (final String migratedAssetId : migratedReference.getAssetIdentifiers()) {
                        final VersionedAsset migratedAsset = new VersionedAsset();
                        migratedAsset.setIdentifier(migratedAssetId);
                        migratedAsset.setName(referencedAsset.getName());
                        migratedAssets.add(migratedAsset);
                    }
                }

                parameter.setReferencedAssets(migratedAssets);
            }
        }
    }

    private void recordComponentStates(final VersionedProcessGroup group, final ConnectorMigrationContext context) {
        if (group == null) {
            return;
        }
        if (group.getProcessors() != null) {
            for (final VersionedProcessor processor : group.getProcessors()) {
                recordComponentState(processor, context);
            }
        }
        if (group.getControllerServices() != null) {
            for (final VersionedControllerService controllerService : group.getControllerServices()) {
                recordComponentState(controllerService, context);
            }
        }
        if (group.getProcessGroups() != null) {
            for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
                recordComponentStates(childGroup, context);
            }
        }
    }

    private void recordComponentState(final VersionedConfigurableExtension extension, final ConnectorMigrationContext context) {
        final VersionedComponentState componentState = extension.getComponentState();
        if (componentState == null) {
            return;
        }
        final boolean hasClusterState = componentState.getClusterState() != null && !componentState.getClusterState().isEmpty();
        final boolean hasLocalState = componentState.getLocalNodeStates() != null && !componentState.getLocalNodeStates().isEmpty();
        if (!hasClusterState && !hasLocalState) {
            return;
        }

        final String versionedId = extension.getIdentifier();
        if (versionedId == null || versionedId.isBlank()) {
            return;
        }
        context.setComponentState(versionedId, componentState);
    }

    /**
     * Reads the most recently migrated source flow back from the supplied flow context's configuration. Returns
     * {@code null} when no migration has been performed yet; callers should fall back to the empty initial flow in
     * that case.
     */
    private VersionedExternalFlow readPersistedMigratedFlow(final FlowContext flowContext) {
        final ConnectorPropertyValue propertyValue = flowContext.getConfigurationContext()
                .getProperty(MIGRATION_STATE_STEP_NAME, MIGRATED_SOURCE_FLOW_PROPERTY.getName());
        final String sourceFlowJson = propertyValue == null ? null : propertyValue.getValue();
        if (sourceFlowJson == null || sourceFlowJson.isBlank()) {
            return null;
        }

        try {
            return OBJECT_MAPPER.readValue(sourceFlowJson, VersionedExternalFlow.class);
        } catch (final JsonProcessingException e) {
            getLogger().error("Failed to deserialize persisted migrated source flow for {}; the managed Process Group will be rebuilt as empty",
                    getInitializationContext().getIdentifier(), e);
            return null;
        }
    }

    private static VersionedExternalFlow emptyVersionedExternalFlow(final String name) {
        final VersionedProcessGroup processGroup = new VersionedProcessGroup();
        processGroup.setName(name);
        processGroup.setProcessors(new HashSet<>());
        processGroup.setConnections(new HashSet<>());
        processGroup.setProcessGroups(new HashSet<>());
        processGroup.setControllerServices(new HashSet<>());

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(processGroup);
        flow.setParameterContexts(Collections.emptyMap());
        return flow;
    }
}
