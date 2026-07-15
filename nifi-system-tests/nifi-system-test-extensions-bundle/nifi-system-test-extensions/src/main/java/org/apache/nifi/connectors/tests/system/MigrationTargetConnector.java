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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.AssetReference;
import org.apache.nifi.components.connector.BundleCompatibility;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorConfigurationContext;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.ConnectorPropertyValue;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.migration.ConnectorMigrationContext;
import org.apache.nifi.components.connector.migration.MigratableConnector;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedAsset;
import org.apache.nifi.flow.VersionedComponentState;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedExternalFlowMetadata;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Test connector used to verify migration of a version-controlled Process Group into a Connector.
 *
 * <p>
 * The connector models one specific flow: a {@code GenerateFlowFile} source feeds a
 * {@code StatefulCountProcessor}, which in turn feeds an {@code AssetReadingProcessor} that copies the bytes of a
 * file-backed asset to an output file. The connector understands this exact flow, so migration is not a generic
 * copy of arbitrary components: each configuration value, asset, and piece of component state is mapped from the
 * source flow onto the specific component the connector rebuilds.
 * </p>
 *
 * <h2>Persistence model</h2>
 *
 * <p>
 * The framework persists a Connector's configuration (the properties of its {@link ConfigurationStep
 * ConfigurationSteps}) to {@code flow.json.gz}. It does not persist the managed Process Group. On restart the
 * framework rehydrates the configuration and calls {@link #applyUpdate(FlowContext, FlowContext)}, which rebuilds the
 * managed Process Group from that configuration. The connector therefore stores everything it needs to rebuild the
 * flow as ordinary configuration properties.
 * </p>
 *
 * <h2>Migration</h2>
 *
 * <ol>
 *     <li>{@link #migrateConfiguration(ConnectorMigrationContext)} reads the source flow's {@code AssetReadingProcessor}
 *         and {@code GenerateFlowFile} to determine the output file, the source file (or referenced asset), and the
 *         generate schedule, and records them as this connector's configuration. When the source references an asset
 *         and the migration is from a local Versioned Process Group, the asset is copied via
 *         {@link ConnectorMigrationContext#copyAssetFromSource(String)} and the returned {@link AssetReference} is
 *         recorded on the {@code Asset File} property.</li>
 *     <li>{@link #migrateState(ConnectorMigrationContext)} copies the {@link VersionedComponentState} of the source
 *         {@code GenerateFlowFile} and {@code StatefulCountProcessor} onto the matching managed components, located by
 *         component name rather than by identifier.</li>
 * </ol>
 */
public class MigrationTargetConnector extends AbstractConnector implements MigratableConnector {
    static final String EXPECTED_FLOW_NAME = "Asset Ingest Flow";

    static final String STEP_NAME = "Flow Configuration";

    static final ConnectorPropertyDescriptor ASSET_FILE = new ConnectorPropertyDescriptor.Builder()
            .name("Asset File")
            .description("The file-backed asset whose contents the flow reads. Set during migration when the source flow references an asset.")
            .type(PropertyType.ASSET)
            .required(false)
            .build();

    static final ConnectorPropertyDescriptor SOURCE_FILE = new ConnectorPropertyDescriptor.Builder()
            .name("Source File")
            .description("A literal path to the file whose contents the flow reads. Used when the source flow references a file by path rather than by asset.")
            .type(PropertyType.STRING)
            .required(false)
            .build();

    static final ConnectorPropertyDescriptor OUTPUT_FILE = new ConnectorPropertyDescriptor.Builder()
            .name("Output File")
            .description("The path where the flow writes the contents that were read.")
            .type(PropertyType.STRING)
            .required(true)
            .build();

    static final ConnectorPropertyDescriptor GENERATE_SCHEDULE = new ConnectorPropertyDescriptor.Builder()
            .name("Generate Schedule")
            .description("The scheduling period of the GenerateFlowFile source processor.")
            .type(PropertyType.STRING)
            .required(true)
            .defaultValue("10 sec")
            .build();

    private static final ConnectorPropertyGroup PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
            .name(STEP_NAME)
            .description("Configuration for the modeled asset-ingest flow.")
            .properties(List.of(ASSET_FILE, SOURCE_FILE, OUTPUT_FILE, GENERATE_SCHEDULE))
            .build();

    private static final ConfigurationStep CONFIGURATION_STEP = new ConfigurationStep.Builder()
            .name(STEP_NAME)
            .description("Configuration for the modeled asset-ingest flow.")
            .propertyGroups(List.of(PROPERTY_GROUP))
            .build();

    private static final Bundle SYSTEM_TEST_EXTENSIONS_BUNDLE = new Bundle("org.apache.nifi", "nifi-system-test-extensions-nar", "2.11.0-SNAPSHOT");

    private static final String ROOT_GROUP_ID = "migration-target-root";
    private static final String GENERATE_NAME = "GenerateFlowFile";
    private static final String COUNT_NAME = "StatefulCountProcessor";
    private static final String ASSET_READER_NAME = "AssetReadingProcessor";
    private static final String PROCESSORS_PACKAGE = "org.apache.nifi.processors.tests.system";
    private static final String GENERATE_TYPE = PROCESSORS_PACKAGE + ".GenerateFlowFile";
    private static final String COUNT_TYPE = PROCESSORS_PACKAGE + ".StatefulCountProcessor";
    private static final String ASSET_READER_TYPE = PROCESSORS_PACKAGE + ".AssetReadingProcessor";
    private static final String SOURCE_FILE_PROPERTY = "Source File";
    private static final String OUTPUT_FILE_PROPERTY = "Output File";

    // The modeled source flow references its asset through a Parameter Context parameter with this exact name. The
    // connector understands the flow it is migrating, so it looks this parameter up by name rather than discovering
    // parameter references generically.
    private static final String SOURCE_ASSET_PARAMETER_NAME = "Asset File";

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(CONFIGURATION_STEP);
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return createEmptyFlow();
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        return buildFlow(activeFlowContext.getConfigurationContext());
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) throws FlowUpdateException {
        final VersionedExternalFlow flow = buildFlow(workingFlowContext.getConfigurationContext());
        getInitializationContext().updateFlow(activeFlowContext, flow, BundleCompatibility.RESOLVE_BUNDLE);
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
        final VersionedExternalFlow flow = buildFlow(workingContext.getConfigurationContext());
        getInitializationContext().updateFlow(workingContext, flow, BundleCompatibility.RESOLVE_BUNDLE);
    }

    @Override
    public boolean isMigrationSupported(final ConnectorMigrationContext context) {
        final VersionedExternalFlow sourceFlow = context.getSourceFlow();
        if (sourceFlow == null) {
            return false;
        }

        final VersionedExternalFlowMetadata metadata = sourceFlow.getMetadata();
        return metadata != null && EXPECTED_FLOW_NAME.equals(metadata.getFlowName());
    }

    @Override
    public void migrateConfiguration(final ConnectorMigrationContext context) throws FlowUpdateException {
        final VersionedExternalFlow sourceFlow = context.getSourceFlow();
        if (sourceFlow == null) {
            throw new FlowUpdateException("A source flow is required for migration");
        }

        final VersionedProcessor assetReader = VersionedFlowUtils.findProcessor(sourceFlow.getFlowContents(), processor -> ASSET_READER_TYPE.equals(processor.getType()))
                .orElseThrow(() -> new FlowUpdateException("Source flow does not contain the expected " + ASSET_READER_NAME));
        final VersionedProcessor generate = VersionedFlowUtils.findProcessor(sourceFlow.getFlowContents(), processor -> GENERATE_TYPE.equals(processor.getType()))
                .orElseThrow(() -> new FlowUpdateException("Source flow does not contain the expected " + GENERATE_NAME));

        final Map<String, String> stringProperties = new HashMap<>();
        final String outputFile = assetReader.getProperties().get(OUTPUT_FILE_PROPERTY);
        if (outputFile != null) {
            stringProperties.put(OUTPUT_FILE.getName(), outputFile);
        }

        if (generate.getSchedulingPeriod() != null) {
            stringProperties.put(GENERATE_SCHEDULE.getName(), generate.getSchedulingPeriod());
        }

        final AssetReference assetReference = copyReferencedAsset(sourceFlow, context);
        if (assetReference == null) {
            final String sourceFileValue = assetReader.getProperties().get(SOURCE_FILE_PROPERTY);
            if (sourceFileValue != null) {
                stringProperties.put(SOURCE_FILE.getName(), sourceFileValue);
            }
        } else {
            context.setValueReferences(STEP_NAME, Map.of(ASSET_FILE.getName(), assetReference));
        }

        if (!stringProperties.isEmpty()) {
            context.setProperties(STEP_NAME, stringProperties);
        }
    }

    @Override
    public void migrateState(final ConnectorMigrationContext context) {
        final VersionedExternalFlow sourceFlow = context.getSourceFlow();
        if (sourceFlow == null || sourceFlow.getFlowContents() == null) {
            return;
        }

        final ProcessGroupFacade managedGroup = context.getActiveFlowContext().getRootGroup();
        copyComponentState(sourceFlow, managedGroup, GENERATE_NAME, context);
        copyComponentState(sourceFlow, managedGroup, COUNT_NAME, context);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        final ConnectorConfigurationContext configurationContext = flowContext.getConfigurationContext().createWithOverrides(stepName, propertyValueOverrides);
        final List<ConfigVerificationResult> results = new ArrayList<>();

        final String sourceFile = resolveSourceFile(configurationContext);
        if (sourceFile == null || sourceFile.isBlank()) {
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Source File Readable")
                    .outcome(Outcome.FAILED)
                    .explanation("No Asset File or Source File is configured")
                    .build());
        } else {
            final File file = new File(sourceFile);
            final boolean readable = file.isFile() && file.canRead();
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Source File Readable")
                    .outcome(readable ? Outcome.SUCCESSFUL : Outcome.FAILED)
                    .explanation(readable ? "Source file exists and is readable: " + sourceFile : "Source file does not exist or is not readable: " + sourceFile)
                    .build());
        }

        final String outputFile = configurationContext.getProperty(STEP_NAME, OUTPUT_FILE.getName()).getValue();
        if (outputFile == null || outputFile.isBlank()) {
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Output File Writable")
                    .outcome(Outcome.FAILED)
                    .explanation("No Output File is configured")
                    .build());
        } else {
            final File parent = new File(outputFile).getAbsoluteFile().getParentFile();
            final boolean writable = parent != null && (parent.isDirectory() || parent.mkdirs());
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Output File Writable")
                    .outcome(writable ? Outcome.SUCCESSFUL : Outcome.FAILED)
                    .explanation(writable ? "Output directory is writable: " + parent : "Output directory could not be created: " + parent)
                    .build());
        }

        return results;
    }

    private VersionedExternalFlow buildFlow(final ConnectorConfigurationContext configurationContext) {
        final String sourceFile = resolveSourceFile(configurationContext);
        final String outputFile = configurationContext.getProperty(STEP_NAME, OUTPUT_FILE.getName()).getValue();
        if (sourceFile == null || sourceFile.isBlank() || outputFile == null || outputFile.isBlank()) {
            return createEmptyFlow();
        }

        final String generateSchedule = configurationContext.getProperty(STEP_NAME, GENERATE_SCHEDULE.getName()).getValue();

        final VersionedProcessGroup rootGroup = VersionedFlowUtils.createProcessGroup(ROOT_GROUP_ID, EXPECTED_FLOW_NAME);

        final VersionedProcessor generate = VersionedFlowUtils.addProcessor(rootGroup, GENERATE_TYPE, SYSTEM_TEST_EXTENSIONS_BUNDLE, GENERATE_NAME, new Position(0, 0));
        generate.getProperties().put("Max FlowFiles", "1");
        generate.getProperties().put("File Size", "0 B");
        generate.setSchedulingPeriod(generateSchedule);

        final VersionedProcessor count = VersionedFlowUtils.addProcessor(rootGroup, COUNT_TYPE, SYSTEM_TEST_EXTENSIONS_BUNDLE, COUNT_NAME, new Position(0, 200));

        final VersionedProcessor assetReader = VersionedFlowUtils.addProcessor(rootGroup, ASSET_READER_TYPE, SYSTEM_TEST_EXTENSIONS_BUNDLE, ASSET_READER_NAME, new Position(0, 400));
        assetReader.getProperties().put(SOURCE_FILE_PROPERTY, sourceFile);
        assetReader.getProperties().put(OUTPUT_FILE_PROPERTY, outputFile);
        assetReader.setAutoTerminatedRelationships(Set.of("success", "failure"));

        VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(generate),
                VersionedFlowUtils.createConnectableComponent(count), Set.of("success"));
        VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(count),
                VersionedFlowUtils.createConnectableComponent(assetReader), Set.of("success"));

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(rootGroup);
        flow.setParameterContexts(Collections.emptyMap());
        return flow;
    }

    private String resolveSourceFile(final ConnectorConfigurationContext configurationContext) {
        final ConnectorPropertyValue assetValue = configurationContext.getProperty(STEP_NAME, ASSET_FILE.getName());
        final String assetPath = assetValue == null ? null : assetValue.getValue();
        if (assetPath != null && !assetPath.isBlank()) {
            return assetPath;
        }

        final ConnectorPropertyValue sourceValue = configurationContext.getProperty(STEP_NAME, SOURCE_FILE.getName());
        return sourceValue == null ? null : sourceValue.getValue();
    }

    private AssetReference copyReferencedAsset(final VersionedExternalFlow sourceFlow, final ConnectorMigrationContext context) {
        // Assets can only be copied from a local source; uploaded payloads do not carry the asset binaries.
        if (!context.isLocalMigration()) {
            return null;
        }

        final VersionedParameter assetParameter = findParameter(sourceFlow, SOURCE_ASSET_PARAMETER_NAME);
        if (assetParameter == null || assetParameter.getReferencedAssets() == null || assetParameter.getReferencedAssets().isEmpty()) {
            return null;
        }

        final VersionedAsset referencedAsset = assetParameter.getReferencedAssets().getFirst();
        return context.copyAssetFromSource(referencedAsset.getIdentifier());
    }

    private VersionedParameter findParameter(final VersionedExternalFlow sourceFlow, final String parameterName) {
        final Map<String, VersionedParameterContext> parameterContexts = sourceFlow.getParameterContexts();
        if (parameterContexts == null) {
            return null;
        }

        for (final VersionedParameterContext parameterContext : parameterContexts.values()) {
            if (parameterContext.getParameters() == null) {
                continue;
            }

            for (final VersionedParameter parameter : parameterContext.getParameters()) {
                if (parameterName.equals(parameter.getName())) {
                    return parameter;
                }
            }
        }

        return null;
    }

    private void copyComponentState(final VersionedExternalFlow sourceFlow, final ProcessGroupFacade managedGroup, final String componentName, final ConnectorMigrationContext context) {
        final Optional<VersionedProcessor> sourceProcessor = VersionedFlowUtils.findProcessor(sourceFlow.getFlowContents(), processor -> componentName.equals(processor.getName()));
        if (sourceProcessor.isEmpty()) {
            return;
        }

        final VersionedComponentState componentState = sourceProcessor.get().getComponentState();
        if (componentState == null) {
            return;
        }

        final ProcessorFacade managedProcessor = findManagedProcessor(managedGroup, componentName);
        if (managedProcessor == null) {
            return;
        }

        context.setComponentState(managedProcessor.getDefinition().getIdentifier(), componentState);
    }

    private ProcessorFacade findManagedProcessor(final ProcessGroupFacade group, final String componentName) {
        for (final ProcessorFacade processor : group.getProcessors()) {
            if (componentName.equals(processor.getDefinition().getName())) {
                return processor;
            }
        }

        return null;
    }

    private VersionedExternalFlow createEmptyFlow() {
        final VersionedProcessGroup rootGroup = VersionedFlowUtils.createProcessGroup(ROOT_GROUP_ID, EXPECTED_FLOW_NAME);
        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(rootGroup);
        flow.setParameterContexts(Collections.emptyMap());
        return flow;
    }
}
