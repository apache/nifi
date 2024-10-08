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

package org.apache.nifi.controller.serialization;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerCapabilityDetection;
import org.apache.nifi.authorization.ManagedAuthorizer;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.AbstractComponentNode;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.MissingBundleException;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.SnippetManager;
import org.apache.nifi.controller.StandardSnippet;
import org.apache.nifi.controller.UninheritableFlowException;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.controller.flow.VersionedFlowEncodingVersion;
import org.apache.nifi.controller.flowanalysis.FlowAnalysisRuleInstantiationException;
import org.apache.nifi.controller.inheritance.AuthorizerCheck;
import org.apache.nifi.controller.inheritance.BundleCompatibilityCheck;
import org.apache.nifi.controller.inheritance.ConnectionMissingCheck;
import org.apache.nifi.controller.inheritance.FlowInheritability;
import org.apache.nifi.controller.inheritance.FlowInheritabilityCheck;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedAsset;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedFlowAnalysisRule;
import org.apache.nifi.flow.VersionedFlowRegistryClient;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedParameterProvider;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.groups.AbstractComponentScheduler;
import org.apache.nifi.groups.BundleUpdateStrategy;
import org.apache.nifi.groups.ComponentIdGenerator;
import org.apache.nifi.groups.ComponentScheduler;
import org.apache.nifi.groups.FlowSynchronizationOptions;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.migration.ControllerServiceFactory;
import org.apache.nifi.migration.StandardControllerServiceFactory;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterGroup;
import org.apache.nifi.parameter.ParameterProviderConfiguration;
import org.apache.nifi.parameter.StandardParameterProviderConfiguration;
import org.apache.nifi.persistence.FlowConfigurationArchiveManager;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.registry.flow.diff.ComparableDataFlow;
import org.apache.nifi.registry.flow.diff.DifferenceDescriptor;
import org.apache.nifi.registry.flow.diff.FlowComparator;
import org.apache.nifi.registry.flow.diff.FlowComparatorVersionedStrategy;
import org.apache.nifi.registry.flow.diff.FlowComparison;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.apache.nifi.registry.flow.diff.StandardComparableDataFlow;
import org.apache.nifi.registry.flow.diff.StandardFlowComparator;
import org.apache.nifi.registry.flow.diff.StaticDifferenceDescriptor;
import org.apache.nifi.registry.flow.mapping.ComponentIdLookup;
import org.apache.nifi.registry.flow.mapping.FlowMappingOptions;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.util.FlowDifferenceFilters;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static org.apache.nifi.controller.serialization.FlowSynchronizationUtils.createBundleCoordinate;
import static org.apache.nifi.controller.serialization.FlowSynchronizationUtils.decrypt;
import static org.apache.nifi.controller.serialization.FlowSynchronizationUtils.decryptProperties;
import static org.apache.nifi.controller.serialization.FlowSynchronizationUtils.getSensitiveDynamicPropertyNames;

public class VersionedFlowSynchronizer implements FlowSynchronizer {
    private static final Logger logger = LoggerFactory.getLogger(VersionedFlowSynchronizer.class);

    private final ExtensionManager extensionManager;
    private final File flowStorageFile;
    private final FlowConfigurationArchiveManager archiveManager;

    public VersionedFlowSynchronizer(final ExtensionManager extensionManager, final File flowStorageFile, final FlowConfigurationArchiveManager archiveManager) {
        this.extensionManager = extensionManager;
        this.flowStorageFile = flowStorageFile;
        this.archiveManager = archiveManager;
    }

    @Override
    public synchronized void sync(final FlowController controller, final DataFlow proposedFlow, final FlowService flowService,
                                  final BundleUpdateStrategy bundleUpdateStrategy)
                    throws FlowSerializationException, UninheritableFlowException, FlowSynchronizationException, MissingBundleException {

        final long start = System.currentTimeMillis();
        final FlowManager flowManager = controller.getFlowManager();
        final ProcessGroup root = flowManager.getRootGroup();

        // handle corner cases involving no proposed flow
        if (isFlowEmpty(proposedFlow)) {
            if (!root.isEmpty()) {
                throw new UninheritableFlowException("Attempted to inherit an empty flow, but this NiFi instance already has a flow loaded.");
            }
        }

        // determine if the controller already had flow sync'd to it
        final boolean flowAlreadySynchronized = controller.isFlowSynchronized();
        logger.info("Synchronizing FlowController with proposed flow: Controller Already Synchronized = {}", flowAlreadySynchronized);

        final DataFlow existingDataFlow = getExistingDataFlow(controller);
        boolean existingFlowEmpty = isFlowEmpty(existingDataFlow);

        // If bundle update strategy is configured to allow for compatible bundles, update any components to use compatible bundles if
        // the exact bundle does not exist.
        if (bundleUpdateStrategy == BundleUpdateStrategy.USE_SPECIFIED_OR_COMPATIBLE_OR_GHOST) {
            mapCompatibleBundles(proposedFlow, controller.getExtensionManager());
        }

        // serialize controller state to bytes
        checkFlowInheritability(existingDataFlow, proposedFlow, controller, bundleUpdateStrategy);

        FlowComparison flowComparison = null;
        AffectedComponentSet affectedComponents = null;
        AffectedComponentSet activeSet = null;

        if (!existingFlowEmpty) {
            flowComparison = compareFlows(existingDataFlow, proposedFlow, controller.getEncryptor());
            final Set<FlowDifference> flowDifferences = flowComparison.getDifferences();

            if (flowDifferences.isEmpty()) {
                logger.debug("No differences between current flow and proposed flow. Will not create backup of existing flow.");
            } else if (isExistingFlowEmpty(controller)) {
                logger.debug("Currently loaded dataflow is empty. Will not create backup of existing flow.");
            } else {
                backupExistingFlow();
            }

            affectedComponents = determineAffectedComponents(flowComparison, controller);
            activeSet = affectedComponents.toActiveSet();

            // Stop the active components, and then wait for all components to be stopped.
            logger.info("In order to inherit proposed dataflow, will stop any components that will be affected by the update");
            if (logger.isDebugEnabled()) {
                logger.debug("Will stop the following components:");
                logger.debug("{}", activeSet);
                final String differencesToString = flowDifferences.stream()
                        .map(FlowDifference::toString)
                        .collect(Collectors.joining("\n"));
                logger.debug("This Active Set was determined from the following Flow Differences:\n{}",
                        differencesToString);
            }

            activeSet.stop();
        }

        try {
            // Ensure that the proposed flow doesn't remove any Connections for which there is currently data queued
            if (!existingFlowEmpty) {
                verifyNoConnectionsWithDataRemoved(existingDataFlow, proposedFlow, controller, flowComparison);
            }

            synchronizeFlow(controller, existingDataFlow, proposedFlow, affectedComponents);
        } finally {
            // We have to call toExistingSet() here because some of the components that existed in the active set may no longer exist,
            // so attempting to start them will fail.

            if (!existingFlowEmpty) {
                final AffectedComponentSet startable = activeSet.toExistingSet().toStartableSet();

                final ComponentSetFilter runningComponentFilter = new RunningComponentSetFilter(proposedFlow.getVersionedDataflow());
                final ComponentSetFilter stoppedComponentFilter = runningComponentFilter.reverse();
                startable.removeComponents(stoppedComponentFilter);
                startable.start();
            }
        }

        final long millis = System.currentTimeMillis() - start;
        logger.info("Successfully synchronized dataflow with the proposed flow in {} millis", millis);
    }

    private void verifyNoConnectionsWithDataRemoved(final DataFlow existingFlow, final DataFlow proposedFlow, final FlowController controller, final FlowComparison flowComparison) {
        logger.debug("Checking that no connections were removed that have data");
        final FlowInheritabilityCheck processGroupInheritableCheck = new ConnectionMissingCheck(flowComparison);
        final FlowInheritability inheritability = processGroupInheritableCheck.checkInheritability(existingFlow, proposedFlow, controller);

        if (inheritability.isInheritable()) {
            logger.debug("Proposed flow contains all connections that currently have data queued. Will backup existing flow and replace, provided all other checks pass");
        } else {
            throw new UninheritableFlowException("Proposed flow is not inheritable by the flow controller and cannot completely replace the current flow due to: "
                + inheritability.getExplanation());
        }
    }

    private void mapCompatibleBundles(final DataFlow proposedFlow, final ExtensionManager extensionManager) {
        final Set<String> missingComponentIds = proposedFlow.getMissingComponents();
        final VersionedDataflow dataflow = proposedFlow.getVersionedDataflow();

        if (isFlowEmpty(dataflow)) {
            return;
        }

        if (dataflow.getReportingTasks() == null) {
            dataflow.setReportingTasks(new ArrayList<>());
        }
        for (final VersionedReportingTask reportingTask : dataflow.getReportingTasks()) {
            if (missingComponentIds.contains(reportingTask.getInstanceIdentifier())) {
                continue;
            }

            final Bundle compatibleBundle = getCompatibleBundle(reportingTask.getBundle(), extensionManager, reportingTask.getType());
            if (compatibleBundle != null) {
                reportingTask.setBundle(compatibleBundle);
            }
        }

        if (dataflow.getFlowAnalysisRules() == null) {
            dataflow.setFlowAnalysisRules(new ArrayList<>());
        }
        for (final VersionedFlowAnalysisRule flowAnalysisRule : dataflow.getFlowAnalysisRules()) {
            if (missingComponentIds.contains(flowAnalysisRule.getInstanceIdentifier())) {
                continue;
            }

            final Bundle compatibleBundle = getCompatibleBundle(flowAnalysisRule.getBundle(), extensionManager, flowAnalysisRule.getType());
            if (compatibleBundle != null) {
                flowAnalysisRule.setBundle(compatibleBundle);
            }
        }

        if (dataflow.getRegistries() == null) {
            dataflow.setRegistries(new ArrayList<>());
        }

        for (final VersionedFlowRegistryClient registry : dataflow.getRegistries()) {
            if (missingComponentIds.contains(registry.getInstanceIdentifier())) {
                continue;
            }

            final Bundle compatibleBundle = getCompatibleBundle(registry.getBundle(), extensionManager, registry.getType());
            if (compatibleBundle != null) {
                registry.setBundle(compatibleBundle);
            }
        }

        if (dataflow.getParameterProviders() == null) {
            dataflow.setParameterProviders(new ArrayList<>());
        }
        for (final VersionedParameterProvider parameterProvider : dataflow.getParameterProviders()) {
            if (missingComponentIds.contains(parameterProvider.getInstanceIdentifier())) {
                continue;
            }

            final Bundle compatibleBundle = getCompatibleBundle(parameterProvider.getBundle(), extensionManager, parameterProvider.getType());
            if (compatibleBundle != null) {
                parameterProvider.setBundle(compatibleBundle);
            }
        }

        if (dataflow.getControllerServices() == null) {
            dataflow.setControllerServices(new ArrayList<>());
        }
        for (final VersionedControllerService service : dataflow.getControllerServices()) {
            if (missingComponentIds.contains(service.getInstanceIdentifier())) {
                continue;
            }

            final Bundle compatibleBundle = getCompatibleBundle(service.getBundle(), extensionManager, service.getType());
            if (compatibleBundle != null) {
                service.setBundle(compatibleBundle);
            }
        }

        mapCompatibleBundles(dataflow.getRootGroup(), extensionManager, missingComponentIds);
    }

    private void mapCompatibleBundles(final VersionedProcessGroup group, final ExtensionManager extensionManager, final Set<String> missingComponentIds) {
        for (final VersionedControllerService service : group.getControllerServices()) {
            if (missingComponentIds.contains(service.getInstanceIdentifier())) {
                continue;
            }

            final Bundle compatibleBundle = getCompatibleBundle(service.getBundle(), extensionManager, service.getType());
            if (compatibleBundle != null) {
                service.setBundle(compatibleBundle);
            }
        }

        for (final VersionedProcessor processor : group.getProcessors()) {
            if (missingComponentIds.contains(processor.getInstanceIdentifier())) {
                continue;
            }

            final Bundle compatibleBundle = getCompatibleBundle(processor.getBundle(), extensionManager, processor.getType());
            if (compatibleBundle != null) {
                processor.setBundle(compatibleBundle);
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            mapCompatibleBundles(childGroup, extensionManager, missingComponentIds);
        }
    }

    private Bundle getCompatibleBundle(final Bundle bundle, final ExtensionManager extensionManager, final String type) {
        final org.apache.nifi.bundle.Bundle exactBundle = extensionManager.getBundle(new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion()));
        if (exactBundle != null) {
            return bundle;
        }

        final BundleDTO bundleDto = new BundleDTO(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
        final Optional<BundleCoordinate> optionalCoordinate = BundleUtils.getOptionalCompatibleBundle(extensionManager, type, bundleDto);
        if (optionalCoordinate.isPresent()) {
            final BundleCoordinate coordinate = optionalCoordinate.get();
            logger.debug("Found compatible bundle {} for {}:{}:{} and type {}", coordinate.getCoordinate(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion(), type);
            return new Bundle(coordinate.getGroup(), coordinate.getId(), coordinate.getVersion());
        }

        logger.debug("Could not find a compatible bundle for {}:{}:{} type {}", bundle.getGroup(), bundle.getArtifact(), bundle.getVersion(), type);
        return null;
    }

    private void synchronizeFlow(final FlowController controller, final DataFlow existingFlow, final DataFlow proposedFlow, final AffectedComponentSet affectedComponentSet) {
        // attempt to sync controller with proposed flow
        try {
            final VersionedDataflow versionedFlow = proposedFlow.getVersionedDataflow();

            final PropertyEncryptor encryptor = controller.getEncryptor();

            if (versionedFlow != null) {
                controller.setMaxTimerDrivenThreadCount(versionedFlow.getMaxTimerDrivenThreadCount());
                ProcessGroup rootGroup = controller.getFlowManager().getRootGroup();

                final Map<String, VersionedParameterContext> versionedParameterContextMap = new HashMap<>();
                versionedFlow.getParameterContexts().forEach(context -> versionedParameterContextMap.put(context.getName(), context));

                final VersionedExternalFlow versionedExternalFlow = new VersionedExternalFlow();
                versionedExternalFlow.setParameterContexts(versionedParameterContextMap);
                versionedExternalFlow.setFlowContents(versionedFlow.getRootGroup());

                // Inherit controller-level components.
                inheritControllerServices(controller, versionedFlow, affectedComponentSet);
                inheritParameterProviders(controller, versionedFlow, affectedComponentSet);
                inheritParameterContexts(controller, versionedFlow);
                inheritReportingTasks(controller, versionedFlow, affectedComponentSet);
                inheritFlowAnalysisRules(controller, versionedFlow, affectedComponentSet);
                inheritRegistryClients(controller, versionedFlow, affectedComponentSet);

                final ComponentIdGenerator componentIdGenerator = (proposedId, instanceId, destinationGroupId) -> instanceId;

                // Use a Versioned Component State Lookup that will check to see if the component is scheduled to start upon FlowController initialization.
                // Otherwise, fallback to the identity lookup (i.e., use whatever is set on the component itself).
                final VersionedComponentStateLookup stateLookup = controller.createVersionedComponentStateLookup(VersionedComponentStateLookup.IDENTITY_LOOKUP);

                final ComponentScheduler componentScheduler = new FlowControllerComponentScheduler(controller, stateLookup);

                if (rootGroup.isEmpty()) {
                    final VersionedProcessGroup versionedRoot = versionedExternalFlow.getFlowContents();
                    rootGroup = controller.getFlowManager().createProcessGroup(versionedRoot.getInstanceIdentifier());
                    rootGroup.setComments(versionedRoot.getComments());
                    rootGroup.setPosition(new Position(versionedRoot.getPosition().getX(), versionedRoot.getPosition().getY()));
                    rootGroup.setName(versionedRoot.getName());
                    controller.setRootGroup(rootGroup);
                }

                // Synchronize the root group
                final FlowSynchronizationOptions syncOptions = new FlowSynchronizationOptions.Builder()
                    .componentIdGenerator(componentIdGenerator)
                    .componentComparisonIdLookup(VersionedComponent::getInstanceIdentifier) // compare components by Instance ID because both versioned flows are derived from instantiated flows
                    .componentScheduler(componentScheduler)
                    .ignoreLocalModifications(true)
                    .updateGroupSettings(true)
                    .updateDescendantVersionedFlows(true)
                    .updateGroupVersionControlSnapshot(false)
                    .updateRpgUrls(true)
                    .propertyDecryptor(encryptor::decrypt)
                    .build();

                final FlowMappingOptions flowMappingOptions = new FlowMappingOptions.Builder()
                    .mapSensitiveConfiguration(true)
                    .mapPropertyDescriptors(false)
                    .stateLookup(stateLookup)
                    .sensitiveValueEncryptor(encryptor::encrypt)
                    .componentIdLookup(ComponentIdLookup.VERSIONED_OR_GENERATE)
                    .mapInstanceIdentifiers(true)
                    .mapControllerServiceReferencesToVersionedId(false)
                    .mapFlowRegistryClientId(true)
                    .mapAssetReferences(true)
                    .build();

                rootGroup.synchronizeFlow(versionedExternalFlow, syncOptions, flowMappingOptions);
            }

            inheritSnippets(controller, proposedFlow);
            inheritAuthorizations(existingFlow, proposedFlow, controller);

            removeMissingParameterContexts(controller, versionedFlow);
            removeMissingRegistryClients(controller.getFlowManager(), versionedFlow);
        } catch (final Exception ex) {
            throw new FlowSynchronizationException(ex);
        }
    }

    private FlowComparison compareFlows(final DataFlow existingFlow, final DataFlow proposedFlow, final PropertyEncryptor encryptor) {
        final DifferenceDescriptor differenceDescriptor = new StaticDifferenceDescriptor();

        final VersionedDataflow clusterVersionedFlow = proposedFlow.getVersionedDataflow();
        final ComparableDataFlow clusterDataFlow = new StandardComparableDataFlow(
            "Cluster Flow",
            clusterVersionedFlow.getRootGroup(),
            toSet(clusterVersionedFlow.getControllerServices()),
            toSet(clusterVersionedFlow.getReportingTasks()),
            toSet(clusterVersionedFlow.getFlowAnalysisRules()),
            toSet(clusterVersionedFlow.getParameterContexts()),
            toSet(clusterVersionedFlow.getParameterProviders()),
            toSet(clusterVersionedFlow.getRegistries())
        );

        final VersionedProcessGroup proposedRootGroup = clusterVersionedFlow.getRootGroup();
        final String proposedRootGroupId = proposedRootGroup == null ? null : proposedRootGroup.getInstanceIdentifier();

        final VersionedDataflow existingVersionedFlow = existingFlow.getVersionedDataflow() == null ? createEmptyVersionedDataflow(proposedRootGroupId) : existingFlow.getVersionedDataflow();
        final ComparableDataFlow localDataFlow = new StandardComparableDataFlow(
            "Local Flow",
            existingVersionedFlow.getRootGroup(),
            toSet(existingVersionedFlow.getControllerServices()),
            toSet(existingVersionedFlow.getReportingTasks()),
            toSet(existingVersionedFlow.getFlowAnalysisRules()),
            toSet(existingVersionedFlow.getParameterContexts()),
            toSet(existingVersionedFlow.getParameterProviders()),
            toSet(existingVersionedFlow.getRegistries())
        );

        final FlowComparator flowComparator = new StandardFlowComparator(localDataFlow, clusterDataFlow, Collections.emptySet(),
            differenceDescriptor, encryptor::decrypt, VersionedComponent::getInstanceIdentifier, FlowComparatorVersionedStrategy.DEEP);
        return flowComparator.compare();
    }

    private <T> Set<T> toSet(final List<T> values) {
        if (values == null || values.isEmpty()) {
            return new HashSet<>();
        }

        return new HashSet<>(values);
    }

    private VersionedDataflow createEmptyVersionedDataflow(final String rootGroupId) {
        final VersionedDataflow dataflow = new VersionedDataflow();
        dataflow.setControllerServices(Collections.emptyList());
        dataflow.setEncodingVersion(new VersionedFlowEncodingVersion(2, 0));
        dataflow.setParameterContexts(Collections.emptyList());
        dataflow.setParameterProviders(Collections.emptyList());
        dataflow.setRegistries(Collections.emptyList());
        dataflow.setReportingTasks(Collections.emptyList());
        dataflow.setFlowAnalysisRules(Collections.emptyList());

        final VersionedProcessGroup rootGroup = new VersionedProcessGroup();
        rootGroup.setInstanceIdentifier(rootGroupId);
        dataflow.setRootGroup(rootGroup);

        return dataflow;
    }

    private AffectedComponentSet determineAffectedComponents(final FlowComparison flowComparison, final FlowController controller) {
        final List<FlowDifference> relevantDifferences = flowComparison.getDifferences().stream()
            .filter(FlowDifferenceFilters.FILTER_ADDED_REMOVED_REMOTE_PORTS)
            .collect(Collectors.toList());

        logger.debug("The differences between Local Flow and Cluster Flow that are relevant for finding affected components are: {}", relevantDifferences);

        final AffectedComponentSet affectedComponentSet = new AffectedComponentSet(controller);
        for (final FlowDifference difference : relevantDifferences) {
            affectedComponentSet.addAffectedComponents(difference);
        }

        logger.debug("Components affected by inheriting the flow are: {}", affectedComponentSet);
        return affectedComponentSet;
    }

    private void inheritRegistryClients(final FlowController controller, final VersionedDataflow dataflow, final AffectedComponentSet affectedComponentSet) {
        final FlowManager flowManager = controller.getFlowManager();

        for (final VersionedFlowRegistryClient versionedFlowRegistryClient : dataflow.getRegistries()) {
            final FlowRegistryClientNode existing = flowManager.getFlowRegistryClient(versionedFlowRegistryClient.getIdentifier());

            if (existing == null) {
                addFlowRegistryClient(controller, versionedFlowRegistryClient);
            } else if (affectedComponentSet.isFlowRegistryClientAffected(existing.getIdentifier())) {
                updateRegistry(existing, versionedFlowRegistryClient, controller);
            }
        }
    }

    private void removeMissingRegistryClients(final FlowManager flowManager, final VersionedDataflow dataflow) {
        final List<VersionedFlowRegistryClient> versionedRegistryClients = dataflow == null ? null : dataflow.getRegistries();
        final Set<String> versionedClientIds = versionedRegistryClients == null ? Set.of() :
            versionedRegistryClients.stream()
                .map(VersionedFlowRegistryClient::getInstanceIdentifier)
                .collect(Collectors.toSet());

        for (final FlowRegistryClientNode clientNode : flowManager.getAllFlowRegistryClients()) {
            if (!versionedClientIds.contains(clientNode.getIdentifier())) {
                flowManager.removeFlowRegistryClient(clientNode);
            }
        }
    }

    private void addFlowRegistryClient(final FlowController flowController, final VersionedFlowRegistryClient versionedFlowRegistryClient) {
        final BundleCoordinate coordinate = createBundleCoordinate(extensionManager, versionedFlowRegistryClient.getBundle(), versionedFlowRegistryClient.getType());

        final FlowRegistryClientNode flowRegistryClient = flowController.getFlowManager().createFlowRegistryClient(
                versionedFlowRegistryClient.getType(), versionedFlowRegistryClient.getIdentifier(), coordinate, Collections.emptySet(), false, true, null);
        updateRegistry(flowRegistryClient, versionedFlowRegistryClient, flowController);
    }

    private void updateRegistry(final FlowRegistryClientNode flowRegistryClient, final VersionedFlowRegistryClient versionedFlowRegistryClient, final FlowController flowController) {
        flowRegistryClient.setName(versionedFlowRegistryClient.getName());
        flowRegistryClient.setDescription(versionedFlowRegistryClient.getDescription());
        flowRegistryClient.setAnnotationData(versionedFlowRegistryClient.getAnnotationData());

        final Set<String> sensitiveDynamicPropertyNames = getSensitiveDynamicPropertyNames(flowRegistryClient, versionedFlowRegistryClient);
        final Map<String, String> decryptedProperties = decryptProperties(versionedFlowRegistryClient.getProperties(), flowController.getEncryptor());
        flowRegistryClient.setProperties(decryptedProperties, false, sensitiveDynamicPropertyNames);
    }

    private void inheritReportingTasks(final FlowController controller, final VersionedDataflow dataflow, final AffectedComponentSet affectedComponentSet) {
        final Set<String> versionedTaskIds = new HashSet<>();
        for (final VersionedReportingTask versionedReportingTask : dataflow.getReportingTasks()) {
            versionedTaskIds.add(versionedReportingTask.getInstanceIdentifier());
            final ReportingTaskNode existing = controller.getReportingTaskNode(versionedReportingTask.getInstanceIdentifier());
            if (existing == null) {
                addReportingTask(controller, versionedReportingTask);
            } else if (affectedComponentSet.isReportingTaskAffected(existing.getIdentifier())) {
                updateReportingTask(existing, versionedReportingTask, controller);
            }
        }

        for (final ReportingTaskNode reportingTask : controller.getAllReportingTasks()) {
            if (!versionedTaskIds.contains(reportingTask.getIdentifier())) {
                controller.removeReportingTask(reportingTask);
            }
        }
    }

    private void addReportingTask(final FlowController controller, final VersionedReportingTask reportingTask) {
        final BundleCoordinate coordinate = createBundleCoordinate(extensionManager, reportingTask.getBundle(), reportingTask.getType());

        final ReportingTaskNode taskNode = controller.createReportingTask(reportingTask.getType(), reportingTask.getInstanceIdentifier(), coordinate, false);
        updateReportingTask(taskNode, reportingTask, controller);

        final ControllerServiceFactory serviceFactory = new StandardControllerServiceFactory(controller.getExtensionManager(), controller.getFlowManager(),
            controller.getControllerServiceProvider(), taskNode);
        Map<String, String> rawPropertyValues = taskNode.getRawPropertyValues().entrySet().stream()
                .collect(HashMap::new,
                        (m, e) -> m.put(e.getKey().getName(), e.getValue()),
                        HashMap::putAll);
        taskNode.migrateConfiguration(rawPropertyValues, serviceFactory);
    }

    private void updateReportingTask(final ReportingTaskNode taskNode, final VersionedReportingTask reportingTask, final FlowController controller) {
        taskNode.setName(reportingTask.getName());
        taskNode.setComments(reportingTask.getComments());
        taskNode.setSchedulingPeriod(reportingTask.getSchedulingPeriod());
        taskNode.setSchedulingStrategy(SchedulingStrategy.valueOf(reportingTask.getSchedulingStrategy()));

        taskNode.setAnnotationData(reportingTask.getAnnotationData());

        final Set<String> sensitiveDynamicPropertyNames = getSensitiveDynamicPropertyNames(taskNode, reportingTask);
        final Map<String, String> decryptedProperties = decryptProperties(reportingTask.getProperties(), controller.getEncryptor());
        taskNode.setProperties(decryptedProperties, false, sensitiveDynamicPropertyNames);

        // enable/disable/start according to the ScheduledState
        switch (reportingTask.getScheduledState()) {
            case DISABLED:
                if (taskNode.isRunning()) {
                    controller.stopReportingTask(taskNode);
                }
                controller.disableReportingTask(taskNode);
                break;
            case ENABLED:
                if (taskNode.getScheduledState() == org.apache.nifi.controller.ScheduledState.DISABLED) {
                    controller.enableReportingTask(taskNode);
                } else if (taskNode.isRunning()) {
                    controller.stopReportingTask(taskNode);
                }
                break;
            case RUNNING:
                if (taskNode.getScheduledState() == org.apache.nifi.controller.ScheduledState.DISABLED) {
                    controller.enableReportingTask(taskNode);
                }
                if (!taskNode.isRunning()) {
                    controller.startReportingTask(taskNode);
                }
                break;
        }
    }

    private void inheritFlowAnalysisRules(final FlowController controller, final VersionedDataflow dataflow, final AffectedComponentSet affectedComponentSet)
        throws FlowAnalysisRuleInstantiationException {
        // Guard state in order to be able to read flow.json from before adding the flow analysis rules
        if (dataflow.getFlowAnalysisRules() == null) {
            return;
        }

        final Set<String> versionedAnalysisRuleIds = new HashSet<>();
        for (final VersionedFlowAnalysisRule versionedFlowAnalysisRule : dataflow.getFlowAnalysisRules()) {
            versionedAnalysisRuleIds.add(versionedFlowAnalysisRule.getInstanceIdentifier());

            final FlowAnalysisRuleNode existing = controller.getFlowAnalysisRuleNode(versionedFlowAnalysisRule.getInstanceIdentifier());
            if (existing == null) {
                addFlowAnalysisRule(controller, versionedFlowAnalysisRule);
            } else if (affectedComponentSet.isFlowAnalysisRuleAffected(existing.getIdentifier())) {
                updateFlowAnalysisRule(existing, versionedFlowAnalysisRule, controller);
            }
        }

        for (final FlowAnalysisRuleNode ruleNode : controller.getAllFlowAnalysisRules()) {
            if (!versionedAnalysisRuleIds.contains(ruleNode.getIdentifier())) {
                controller.removeFlowAnalysisRule(ruleNode);
            }
        }
    }

    private void addFlowAnalysisRule(final FlowController controller, final VersionedFlowAnalysisRule flowAnalysisRule) throws FlowAnalysisRuleInstantiationException {
        final BundleCoordinate coordinate = createBundleCoordinate(extensionManager, flowAnalysisRule.getBundle(), flowAnalysisRule.getType());

        final FlowAnalysisRuleNode ruleNode = controller.createFlowAnalysisRule(flowAnalysisRule.getType(), flowAnalysisRule.getInstanceIdentifier(), coordinate, false);
        updateFlowAnalysisRule(ruleNode, flowAnalysisRule, controller);
    }

    private void updateFlowAnalysisRule(final FlowAnalysisRuleNode ruleNode, final VersionedFlowAnalysisRule flowAnalysisRule, final FlowController controller) {
        ruleNode.setName(flowAnalysisRule.getName());
        ruleNode.setComments(flowAnalysisRule.getComments());
        ruleNode.setEnforcementPolicy(flowAnalysisRule.getEnforcementPolicy());

        final Set<String> sensitiveDynamicPropertyNames = getSensitiveDynamicPropertyNames(ruleNode, flowAnalysisRule);
        final Map<String, String> decryptedProperties = decryptProperties(flowAnalysisRule.getProperties(), controller.getEncryptor());
        ruleNode.setProperties(decryptedProperties, false, sensitiveDynamicPropertyNames);

        switch (flowAnalysisRule.getScheduledState()) {
            case DISABLED:
                if (ruleNode.isEnabled()) {
                    controller.disableFlowAnalysisRule(ruleNode);
                }
                break;
            case ENABLED:
                if (!ruleNode.isEnabled()) {
                    controller.enableFlowAnalysisRule(ruleNode);
                }
                break;
        }
    }

    private void inheritParameterProviders(final FlowController controller, final VersionedDataflow dataflow, final AffectedComponentSet affectedComponentSet) {
        if (dataflow.getParameterProviders() == null) {
            return;
        }

        final FlowManager flowManager = controller.getFlowManager();

        final Set<String> versionedProviderIds = new HashSet<>();
        for (final VersionedParameterProvider versionedParameterProvider : dataflow.getParameterProviders()) {
            versionedProviderIds.add(versionedParameterProvider.getInstanceIdentifier());

            final ParameterProviderNode existing = flowManager.getParameterProvider(versionedParameterProvider.getInstanceIdentifier());
            if (existing == null) {
                addParameterProvider(controller, versionedParameterProvider, controller.getEncryptor());
            } else if (affectedComponentSet.isParameterProviderAffected(existing.getIdentifier())) {
                updateParameterProvider(existing, versionedParameterProvider, controller.getEncryptor());
            }
        }

        for (final ParameterProviderNode parameterProvider : flowManager.getAllParameterProviders()) {
            if (!versionedProviderIds.contains(parameterProvider.getIdentifier())) {
                flowManager.removeParameterProvider(parameterProvider);
            }
        }
    }

    private void addParameterProvider(final FlowController controller, final VersionedParameterProvider parameterProvider, final PropertyEncryptor encryptor) {
        final BundleCoordinate coordinate = createBundleCoordinate(extensionManager, parameterProvider.getBundle(), parameterProvider.getType());

        final ParameterProviderNode parameterProviderNode = controller.getFlowManager()
                .createParameterProvider(parameterProvider.getType(), parameterProvider.getInstanceIdentifier(), coordinate, false);
        updateParameterProvider(parameterProviderNode, parameterProvider, encryptor);
    }

    private void updateParameterProvider(final ParameterProviderNode parameterProviderNode, final VersionedParameterProvider parameterProvider,
                                         final  PropertyEncryptor encryptor) {
        parameterProviderNode.setName(parameterProvider.getName());
        parameterProviderNode.setComments(parameterProvider.getComments());

        parameterProviderNode.setAnnotationData(parameterProvider.getAnnotationData());
        final Map<String, String> decryptedProperties = decryptProperties(parameterProvider.getProperties(), encryptor);
        parameterProviderNode.setProperties(decryptedProperties);
    }

    private void removeMissingParameterContexts(final FlowController controller, final VersionedDataflow dataflow) {
        controller.getFlowManager().withParameterContextResolution(() -> {
            final ParameterContextManager contextManager = controller.getFlowManager().getParameterContextManager();
            removeMissingParameterContexts(contextManager, dataflow);
        });
    }

    private void removeMissingParameterContexts(final ParameterContextManager contextManager, final VersionedDataflow dataflow) {
        if (dataflow == null) {
            return;
        }

        // Build mapping of name to context for resolution of inherited contexts
        final List<VersionedParameterContext> proposedParameterContexts = dataflow.getParameterContexts();
        final Set<String> proposedContextNames = proposedParameterContexts.stream()
            .map(VersionedParameterContext::getName)
            .collect(Collectors.toSet());

        final Map<String, ParameterContext> existingContextsByName = contextManager.getParameterContextNameMapping();
        for (final Map.Entry<String, ParameterContext> entry : existingContextsByName.entrySet()) {
            if (!proposedContextNames.contains(entry.getKey())) {
                contextManager.removeParameterContext(entry.getValue().getIdentifier());
            }
        }
    }

    private void inheritParameterContexts(final FlowController controller, final VersionedDataflow dataflow) {
        controller.getFlowManager().withParameterContextResolution(() -> {
            final List<VersionedParameterContext> parameterContexts = dataflow.getParameterContexts();

            // Build mapping of name to context for resolution of inherited contexts
            final Map<String, VersionedParameterContext> namedParameterContexts = new HashMap<>();
            parameterContexts.forEach(context -> namedParameterContexts.put(context.getName(), context));

            for (final VersionedParameterContext versionedParameterContext : parameterContexts) {
                inheritParameterContext(versionedParameterContext, controller.getFlowManager(), namedParameterContexts, controller.getEncryptor(), controller.getAssetManager());
            }
        });
    }

    private void inheritParameterContext(
            final VersionedParameterContext versionedParameterContext,
            final FlowManager flowManager,
            final Map<String, VersionedParameterContext> namedParameterContexts,
            final PropertyEncryptor encryptor,
            final AssetManager assetManager
    ) {
        final ParameterContextManager contextManager = flowManager.getParameterContextManager();
        final ParameterContext existingContext = contextManager.getParameterContextNameMapping().get(versionedParameterContext.getName());
        if (existingContext == null) {
            addParameterContext(versionedParameterContext, flowManager, namedParameterContexts, encryptor, assetManager);
        } else {
            updateParameterContext(versionedParameterContext, existingContext, flowManager, namedParameterContexts, encryptor, assetManager);
        }
    }

    private void addParameterContext(
            final VersionedParameterContext versionedParameterContext,
            final FlowManager flowManager,
            final Map<String, VersionedParameterContext> namedParameterContexts,
            final PropertyEncryptor encryptor,
            final AssetManager assetManager
    ) {
        final Map<String, Parameter> parameters = createParameterMap(flowManager, versionedParameterContext, encryptor, assetManager);

        final ParameterContextManager contextManager = flowManager.getParameterContextManager();
        final List<String> referenceIds = findReferencedParameterContextIds(versionedParameterContext, contextManager, namedParameterContexts);

        ParameterProviderConfiguration parameterProviderConfiguration = null;
        if (versionedParameterContext.getParameterProvider() != null) {
            parameterProviderConfiguration = new StandardParameterProviderConfiguration(versionedParameterContext.getParameterProvider(),
                    versionedParameterContext.getParameterGroupName(), versionedParameterContext.isSynchronized());
        }
        flowManager.createParameterContext(versionedParameterContext.getInstanceIdentifier(), versionedParameterContext.getName(), versionedParameterContext.getDescription(),
                parameters, referenceIds, parameterProviderConfiguration);
        logger.info("Added Parameter Context {}", versionedParameterContext.getName());
    }

    private List<String> findReferencedParameterContextIds(
            final VersionedParameterContext versionedParameterContext,
            final ParameterContextManager contextManager,
            final Map<String, VersionedParameterContext> namedParameterContexts
    ) {
        final List<String> referenceIds = new ArrayList<>();
        final Map<String, ParameterContext> parameterContextsByName = contextManager.getParameterContextNameMapping();

        if (versionedParameterContext.getInheritedParameterContexts() != null) {
            for (final String inheritedContextName : versionedParameterContext.getInheritedParameterContexts()) {
                // Lookup inherited Parameter Context Name in Versioned Data Flow
                final VersionedParameterContext inheritedParameterContext = namedParameterContexts.get(inheritedContextName);
                if (inheritedParameterContext == null) {
                    // Lookup inherited Parameter Context Name in Parameter Context Manager
                    final ParameterContext existingContext = parameterContextsByName.get(inheritedContextName);
                    if (existingContext == null) {
                        logger.warn("Parameter Context {} inherits from Parameter Context {} but cannot find a Parameter Context with name {}",
                                versionedParameterContext.getName(), inheritedContextName, inheritedContextName);
                    } else {
                        referenceIds.add(existingContext.getIdentifier());
                    }
                } else {
                    referenceIds.add(inheritedParameterContext.getInstanceIdentifier());
                }
            }
        }

        return referenceIds;
    }

    private Map<String, Parameter> createParameterMap(
            final FlowManager flowManager,
            final VersionedParameterContext versionedParameterContext,
            final PropertyEncryptor encryptor,
            final AssetManager assetManager
    ) {
        final Map<String, Parameter> providedParameters = getProvidedParameters(flowManager, versionedParameterContext);

        final Map<String, Parameter> parameters = new HashMap<>();
        for (final VersionedParameter versioned : versionedParameterContext.getParameters()) {
            final String parameterValue;
            final String rawValue = versioned.getValue();
            if (rawValue == null) {
                parameterValue = null;
            } else if (versioned.isProvided()) {
                final String name = versioned.getName();
                final Parameter providedParameter = providedParameters.get(name);
                if (providedParameter == null) {
                    logger.warn("Parameter Context [{}] Provided Parameter [{}] not found", versionedParameterContext.getIdentifier(), name);
                    parameterValue = null;
                } else {
                    parameterValue = providedParameter.getValue();
                }
            } else if (versioned.isSensitive()) {
                parameterValue = decrypt(rawValue, encryptor);
            } else {
                parameterValue = rawValue;
            }

            final List<Asset> referencedAssets = getReferencedAssets(versioned, assetManager, versionedParameterContext.getInstanceIdentifier());
            final Parameter parameter = new Parameter.Builder()
                .parameterContextId(null)
                .name(versioned.getName())
                .description(versioned.getDescription())
                .sensitive(versioned.isSensitive())
                .value(referencedAssets.isEmpty() ? parameterValue : null)
                .referencedAssets(referencedAssets)
                .provided(versioned.isProvided())
                .build();

            parameters.put(versioned.getName(), parameter);
        }

        return parameters;
    }

    private List<Asset> getReferencedAssets(final VersionedParameter versionedParameter, final AssetManager assetManager, final String contextId) {
        final List<VersionedAsset> versionedAssets = versionedParameter.getReferencedAssets();
        if (versionedAssets == null || versionedAssets.isEmpty()) {
            return List.of();
        }

        final List<Asset> assets = new ArrayList<>();
        for (final VersionedAsset versionedAsset : versionedAssets) {
            final Optional<Asset> assetOption = assetManager.getAsset(versionedAsset.getIdentifier());
            if (assetOption.isPresent()) {
                assets.add(assetOption.get());
            } else {
                assets.add(assetManager.createMissingAsset(contextId, versionedAsset.getName()));
            }
        }

        return assets;
    }

    private void updateParameterContext(
            final VersionedParameterContext versionedParameterContext,
            final ParameterContext parameterContext,
            final FlowManager flowManager,
            final Map<String, VersionedParameterContext> namedParameterContexts,
            final PropertyEncryptor encryptor,
            final AssetManager assetManager
    ) {
        final Map<String, Parameter> parameters = createParameterMap(flowManager, versionedParameterContext, encryptor, assetManager);

        final Map<String, String> currentValues = new HashMap<>();
        final Map<String, Set<String>> currentAssetReferences = new HashMap<>();
        parameterContext.getParameters().values().forEach(param -> {
            currentValues.put(param.getDescriptor().getName(), param.getValue());
            currentAssetReferences.put(param.getDescriptor().getName(), getAssetIds(param));
        });

        final Map<String, Parameter> updatedParameters = new HashMap<>();
        final Set<String> proposedParameterNames = new HashSet<>();
        for (final VersionedParameter parameter : versionedParameterContext.getParameters()) {
            final String parameterName = parameter.getName();
            final String currentValue = currentValues.get(parameterName);
            final Set<String> currentAssetIds = currentAssetReferences.getOrDefault(parameterName, Collections.emptySet());

            final Parameter updatedParameterObject = parameters.get(parameterName);
            final String updatedValue = updatedParameterObject.getValue();
            final Set<String> updatedAssetIds = getAssetIds(updatedParameterObject);
            if (!Objects.equals(currentValue, updatedValue) || !currentAssetIds.equals(updatedAssetIds)) {
                updatedParameters.put(parameterName, updatedParameterObject);
            }
            proposedParameterNames.add(parameterName);
        }

        // If any parameters are removed, need to add a null value to the map in order to make sure that the parameter is removed.
        for (final ParameterDescriptor existingParameterDescriptor : parameterContext.getParameters().keySet()) {
            final String name = existingParameterDescriptor.getName();
            if (!proposedParameterNames.contains(name)) {
                updatedParameters.put(name, null);
            }
        }

        if (updatedParameters.isEmpty()) {
            logger.debug("No Parameters to update for Parameter Context {}", parameterContext.getName());
        } else {
            parameterContext.setParameters(updatedParameters);
            logger.info("Updated the following Parameters for Parameter Context {}: {}", parameterContext.getName(), updatedParameters.keySet());
        }

        final ParameterContextManager contextManager = flowManager.getParameterContextManager();
        final List<String> inheritedContextIds = findReferencedParameterContextIds(versionedParameterContext, contextManager, namedParameterContexts);
        final List<ParameterContext> referencedContexts = inheritedContextIds.stream()
                .map(contextManager::getParameterContext)
                .collect(Collectors.toList());
        parameterContext.setInheritedParameterContexts(referencedContexts);
    }

    private Set<String> getAssetIds(final Parameter parameter) {
        return Stream.ofNullable(parameter.getReferencedAssets())
                .flatMap(Collection::stream)
                .map(Asset::getIdentifier)
                .collect(Collectors.toSet());
    }

    private void inheritControllerServices(final FlowController controller, final VersionedDataflow dataflow, final AffectedComponentSet affectedComponentSet) {
        final FlowManager flowManager = controller.getFlowManager();

        final Set<ControllerServiceNode> toEnable = new HashSet<>();
        final Set<ControllerServiceNode> toDisable = new HashSet<>();

        // We need to add any Controller Services that are not yet part of the flow. We must then
        // update the Controller Services to match what is proposed. Finally, we can enable the services.
        // We have to do this in 3 parts because if we just configure the Controller Service as we add it,
        // we will have a situation where Service A references Service B. And if Service A is added first,
        // Service B's references won't be updated. To avoid this, we create them all first, and then configure/update
        // them so that when AbstractComponentNode#setProperty is called, it properly establishes that reference.
        final List<VersionedControllerService> controllerServices = dataflow.getControllerServices();
        final Map<ControllerServiceNode, Map<String, String>> controllerServicesAddedAndProperties = new HashMap<>();
        for (final VersionedControllerService versionedControllerService : controllerServices) {
            final ControllerServiceNode serviceNode = flowManager.getRootControllerService(versionedControllerService.getInstanceIdentifier());
            if (serviceNode == null) {
                final ControllerServiceNode added = addRootControllerService(controller, versionedControllerService);
                controllerServicesAddedAndProperties.put(added, versionedControllerService.getProperties());
            }
        }

        for (final VersionedControllerService versionedControllerService : controllerServices) {
            final ControllerServiceNode serviceNode = flowManager.getRootControllerService(versionedControllerService.getInstanceIdentifier());
            if (controllerServicesAddedAndProperties.containsKey(serviceNode) || affectedComponentSet.isControllerServiceAffected(serviceNode.getIdentifier())) {
                updateRootControllerService(serviceNode, versionedControllerService, controller.getEncryptor());
            }
        }

        for (final Map.Entry<ControllerServiceNode, Map<String, String>> entry : controllerServicesAddedAndProperties.entrySet()) {
            final ControllerServiceNode service = entry.getKey();
            final Map<String, String> originalPropertyValues = entry.getValue();

            final ControllerServiceFactory serviceFactory = new StandardControllerServiceFactory(controller.getExtensionManager(), controller.getFlowManager(),
                controller.getControllerServiceProvider(), service);
            service.migrateConfiguration(originalPropertyValues, serviceFactory);
        }

        for (final VersionedControllerService versionedControllerService : controllerServices) {
            final ControllerServiceNode serviceNode = flowManager.getRootControllerService(versionedControllerService.getInstanceIdentifier());

            if (versionedControllerService.getScheduledState() == ScheduledState.ENABLED) {
                toEnable.add(serviceNode);
            } else {
                toDisable.add(serviceNode);
            }
        }

        // Enable any Controller-level services that are intended to be enabled.
        if (!toEnable.isEmpty()) {
            controller.getControllerServiceProvider().enableControllerServices(toEnable);

            // Validate Controller-level services
            for (final ControllerServiceNode serviceNode : toEnable) {
                serviceNode.performValidation();
            }
        }

        // Disable any Controller-level services that are intended to be disabled.
        if (!toDisable.isEmpty()) {
            controller.getControllerServiceProvider().disableControllerServicesAsync(toDisable);
        }

        removeMissingServices(controller, dataflow);
    }

    private Map<String, Parameter> getProvidedParameters(final FlowManager flowManager, final VersionedParameterContext versionedParameterContext) {
        final Map<String, Parameter> providedParameters;
        final String parameterProviderId = versionedParameterContext.getParameterProvider();
        if (parameterProviderId == null) {
            providedParameters = Collections.emptyMap();
        } else {
            final ParameterProviderNode parameterProviderNode = flowManager.getParameterProvider(parameterProviderId);
            final ValidationStatus validationStatus = getParameterProviderValidationStatus(parameterProviderNode);

            final String parameterGroupName = versionedParameterContext.getParameterGroupName();
            if (ValidationStatus.VALID == validationStatus) {
                providedParameters = getProvidedParameters(parameterProviderNode, parameterGroupName);
            } else {
                logger.warn("Parameter Provider [{}] {}: Parameters not available for Group [{}]", parameterProviderId, validationStatus, parameterGroupName);
                providedParameters = Collections.emptyMap();
            }
        }
        return providedParameters;
    }

    private Map<String, Parameter> getProvidedParameters(final ParameterProviderNode parameterProviderNode, final String parameterGroupName) {
        final String providerId = parameterProviderNode.getIdentifier();
        logger.debug("Fetching Parameters for Group [{}] from Provider [{}]", parameterGroupName, providerId);

        try {
            parameterProviderNode.fetchParameters();
        } catch (final Exception e) {
            logger.warn("Fetching Parameters for Group [{}] from Provider [{}] failed", parameterGroupName, providerId, e);
        }

        final Map<String, Parameter> parameters;
        final Optional<ParameterGroup> foundParameterGroup = parameterProviderNode.findFetchedParameterGroup(parameterGroupName);
        if (foundParameterGroup.isPresent()) {
            final ParameterGroup parameterGroup = foundParameterGroup.get();
            parameters = parameterGroup.getParameters().stream()
                    .collect(
                            Collectors.toMap(parameter -> parameter.getDescriptor().getName(), Function.identity())
                    );
        } else {
            parameters = Collections.emptyMap();
        }

        logger.info("Fetched Parameters [{}] for Group [{}] from Provider [{}]", parameters.size(), parameterGroupName, providerId);
        return parameters;
    }

    private ValidationStatus getParameterProviderValidationStatus(final ParameterProviderNode parameterProviderNode) {
        final ValidationStatus workingValidationStatus;

        final ValidationStatus currentValidationStatus = parameterProviderNode.getValidationStatus();
        if (ValidationStatus.VALIDATING == currentValidationStatus) {
            // Perform validation to determine status before fetching Parameters
            workingValidationStatus = parameterProviderNode.performValidation();
        } else {
            workingValidationStatus = currentValidationStatus;
        }

        final ValidationStatus validationStatus;
        if (ValidationStatus.INVALID == workingValidationStatus) {
            // Wait for Validation as needed for Parameter Providers with Controller Services or other dependencies
            parameterProviderNode.resetValidationState();
            validationStatus = parameterProviderNode.getValidationStatus(10, TimeUnit.SECONDS);
        } else {
            validationStatus = workingValidationStatus;
        }

        return validationStatus;
    }

    private void removeMissingServices(final FlowController controller, final VersionedDataflow dataflow) {
        if (dataflow == null) {
            return;
        }

        final Set<String> retainedServiceIds = dataflow.getControllerServices().stream()
            .map(VersionedControllerService::getInstanceIdentifier)
            .collect(Collectors.toSet());

        final List<ControllerServiceNode> toRemove = controller.getFlowManager().getRootControllerServices().stream()
            .filter(service -> !retainedServiceIds.contains(service.getIdentifier()))
            .toList();

        for (final ControllerServiceNode serviceToRemove : toRemove) {
            try {
                controller.getFlowManager().removeRootControllerService(serviceToRemove);
            } catch (final Exception e) {
                throw new IllegalStateException("Inherited Dataflow does not have Controller-Level Controller Service %s but failed to remove it from flow".formatted(serviceToRemove), e);
            }
        }
    }

    private ControllerServiceNode addRootControllerService(final FlowController controller, final VersionedControllerService versionedControllerService) {
        final BundleCoordinate bundleCoordinate = createBundleCoordinate(extensionManager, versionedControllerService.getBundle(), versionedControllerService.getType());
        final ControllerServiceNode serviceNode = controller.getFlowManager().createControllerService(versionedControllerService.getType(),
            versionedControllerService.getInstanceIdentifier(), bundleCoordinate, Collections.emptySet(), true, true, null);

        controller.getFlowManager().addRootControllerService(serviceNode);
        return serviceNode;
    }

    private void updateRootControllerService(final ControllerServiceNode serviceNode, final VersionedControllerService versionedControllerService,
                                             final PropertyEncryptor encryptor) {
        serviceNode.pauseValidationTrigger();
        try {
            serviceNode.setName(versionedControllerService.getName());
            serviceNode.setAnnotationData(versionedControllerService.getAnnotationData());
            serviceNode.setComments(versionedControllerService.getComments());

            if (versionedControllerService.getBulletinLevel() != null) {
                serviceNode.setBulletinLevel(LogLevel.valueOf(versionedControllerService.getBulletinLevel()));
            } else {
                // this situation exists for backward compatibility with nifi 1.16 and earlier where controller services do not have bulletinLevels set in flow.xml/flow.json
                // and bulletinLevels are at the WARN level by default
                serviceNode.setBulletinLevel(LogLevel.WARN);
            }

            final Set<String> sensitiveDynamicPropertyNames = getSensitiveDynamicPropertyNames(serviceNode, versionedControllerService);
            final Map<String, String> decryptedProperties = decryptProperties(versionedControllerService.getProperties(), encryptor);
            serviceNode.setProperties(decryptedProperties, false, sensitiveDynamicPropertyNames);
        } finally {
            serviceNode.resumeValidationTrigger();
        }
    }

    private void inheritAuthorizations(final DataFlow existingFlow, final DataFlow proposedFlow, final FlowController controller) {
        final Authorizer authorizer = controller.getAuthorizer();
        if (!(authorizer instanceof final ManagedAuthorizer managedAuthorizer)) {
            return;
        }

        final String proposedAuthFingerprint = proposedFlow.getAuthorizerFingerprint() == null ? "" : new String(proposedFlow.getAuthorizerFingerprint(), StandardCharsets.UTF_8);

        final FlowInheritabilityCheck authorizerCheck = new AuthorizerCheck();
        final FlowInheritability authorizerInheritability = authorizerCheck.checkInheritability(existingFlow, proposedFlow, controller);

        if (authorizerInheritability.isInheritable()) {
            logger.debug("Authorizations are inheritable. Will inherit from proposed fingerprint {}", proposedAuthFingerprint);
            managedAuthorizer.inheritFingerprint(proposedAuthFingerprint);
        } else if (!Objects.equals(managedAuthorizer.getFingerprint(), proposedAuthFingerprint)) {
            // At this point, the flow is not inheritable, but we've made it this far. This can only happen if the existing flow is empty, so we can
            // just forcibly inherit the authorizations.
            logger.debug("Authorizations are not inheritable. Will force inheritance of proposed fingerprint {}", proposedAuthFingerprint);
            managedAuthorizer.forciblyInheritFingerprint(proposedAuthFingerprint);
        }
    }

    private void checkFlowInheritability(final DataFlow existingFlow, final DataFlow proposedFlow, final FlowController controller, final BundleUpdateStrategy bundleUpdateStrategy) {
        logger.debug("Checking if proposed dataflow is inheritable: {}", proposedFlow);
        final boolean existingFlowEmpty = isExistingFlowEmpty(controller);

        // If the Bundle Update Strategy indicates that we cannot inherit the flow if we are missing a bundle, then we must
        // check that we have the Bundle for every component except for those that are missing in the proposed flow.
        if (bundleUpdateStrategy == BundleUpdateStrategy.USE_SPECIFIED_OR_FAIL) {
            final BundleCompatibilityCheck bundleCompatibilityCheck = new BundleCompatibilityCheck();
            final FlowInheritability bundleInheritability = bundleCompatibilityCheck.checkInheritability(existingFlow, proposedFlow, controller);
            if (!bundleInheritability.isInheritable()) {
                throw new UninheritableFlowException("Proposed flow could not be inherited because it references one or more Bundles that are not available in this NiFi instance: "
                    + bundleInheritability.getExplanation());
            }

            logger.debug("Bundle Compatibility check passed");
        }

        logger.debug("Checking authorizer inheritability");
        final FlowInheritabilityCheck authorizerCheck = new AuthorizerCheck();
        final FlowInheritability authorizerInheritability = authorizerCheck.checkInheritability(existingFlow, proposedFlow, controller);
        final Authorizer authorizer = controller.getAuthorizer();

        if (existingFlowEmpty) {
            logger.debug("Existing flow is empty so will not check Authorizer inheritability. Authorizers will be forcibly inherited if necessary.");
        } else {
            if (!controller.isInitialized() && authorizer instanceof ManagedAuthorizer) {
                logger.debug("Authorizations are not inheritable, but Authorizer is a Managed Authorizer and the Controller has not yet been initialized, so it can be forcibly inherited.");
            } else {
                if (!authorizerInheritability.isInheritable() && authorizerInheritability.getExplanation() != null) {
                    throw new UninheritableFlowException("Proposed Authorizer is not inheritable by the Flow Controller because NiFi has already started the dataflow " +
                        "and Authorizer has differences: " + authorizerInheritability.getExplanation());
                }

                logger.debug("Authorizer inheritability check passed");
            }
        }
    }

    private boolean isExistingFlowEmpty(final FlowController controller) {
        final FlowManager flowManager = controller.getFlowManager();

        final ProcessGroup rootGroup = flowManager.getRootGroup();
        if (!rootGroup.isEmpty()) {
            logger.debug("Existing Dataflow is not empty because Root Group is not empty");
            return false;
        }

        final Set<ControllerServiceNode> rootControllerServices = flowManager.getRootControllerServices();
        if (!rootControllerServices.isEmpty()) {
            logger.debug("Existing Dataflow is not empty because there are {} Root-Level Controller Services", rootControllerServices.size());
            return false;
        }

        final Set<ReportingTaskNode> reportingTaskNodes = flowManager.getAllReportingTasks();
        if (!reportingTaskNodes.isEmpty()) {
            logger.debug("Existing Dataflow is not empty because there are {} Reporting Tasks", reportingTaskNodes.size());
            return false;
        }

        final Set<ParameterContext> parameterContexts = flowManager.getParameterContextManager().getParameterContexts();
        if (!parameterContexts.isEmpty()) {
            logger.debug("Existing Dataflow is not empty because there are {} Parameter Contexts", parameterContexts.size());
            return false;
        }

        final Set<ParameterProviderNode> parameterProviders = flowManager.getAllParameterProviders();
        if (!parameterProviders.isEmpty()) {
            logger.debug("Existing Dataflow is not empty because there are {} Parameter Providers", parameterProviders.size());
            return false;
        }

        final Set<FlowRegistryClientNode> registryClients = controller.getFlowManager().getAllFlowRegistryClients();
        if (!registryClients.isEmpty()) {
            logger.debug("Existing Dataflow is not empty because there are {} NiFi Registries", registryClients.size());
            return false;
        }

        logger.debug("Existing Dataflow is empty");
        return true;
    }

    public static boolean isFlowEmpty(final DataFlow dataFlow) {
        if (dataFlow == null || dataFlow.getFlow() == null || dataFlow.getFlow().length == 0) {
            return true;
        }

        return isFlowEmpty(dataFlow.getVersionedDataflow());
    }

    private static boolean isFlowEmpty(final VersionedDataflow dataflow) {
        if (dataflow == null) {
            return true;
        }

        if (!CollectionUtils.isEmpty(dataflow.getReportingTasks())) {
            return false;
        }
        if (!CollectionUtils.isEmpty(dataflow.getParameterProviders())) {
            return false;
        }
        if (!CollectionUtils.isEmpty(dataflow.getControllerServices())) {
            return false;
        }
        if (!CollectionUtils.isEmpty(dataflow.getRegistries())) {
            return false;
        }
        if (!CollectionUtils.isEmpty(dataflow.getParameterContexts())) {
            return false;
        }

        final VersionedProcessGroup rootGroup = dataflow.getRootGroup();
        return isFlowEmpty(rootGroup);
    }

    private static boolean isFlowEmpty(final VersionedProcessGroup group) {
        if (group == null) {
            return true;
        }

        return CollectionUtils.isEmpty(group.getProcessors())
            && CollectionUtils.isEmpty(group.getConnections())
            && CollectionUtils.isEmpty(group.getFunnels())
            && CollectionUtils.isEmpty(group.getLabels())
            && CollectionUtils.isEmpty(group.getInputPorts())
            && CollectionUtils.isEmpty(group.getOutputPorts())
            && CollectionUtils.isEmpty(group.getProcessGroups())
            && CollectionUtils.isEmpty(group.getRemoteProcessGroups())
            && CollectionUtils.isEmpty(group.getControllerServices())
            && group.getParameterContextName() == null;
    }


    private DataFlow getExistingDataFlow(final FlowController controller) {
        final FlowManager flowManager = controller.getFlowManager();
        final ProcessGroup root = flowManager.getRootGroup();

        // Determine missing components
        final Set<String> missingComponents = new HashSet<>();
        flowManager.getAllControllerServices().stream().filter(ComponentNode::isExtensionMissing).forEach(cs -> missingComponents.add(cs.getIdentifier()));
        flowManager.getAllReportingTasks().stream().filter(ComponentNode::isExtensionMissing).forEach(r -> missingComponents.add(r.getIdentifier()));
        flowManager.getAllParameterProviders().stream().filter(ComponentNode::isExtensionMissing).forEach(r -> missingComponents.add(r.getIdentifier()));
        flowManager.getAllFlowRegistryClients().stream().filter(ComponentNode::isExtensionMissing).forEach(c -> missingComponents.add(c.getIdentifier()));
        root.findAllProcessors().stream().filter(AbstractComponentNode::isExtensionMissing).forEach(p -> missingComponents.add(p.getIdentifier()));

        logger.trace("Exporting snippets from controller");
        final byte[] existingSnippets = controller.getSnippetManager().export();

        final byte[] existingAuthFingerprint;
        final Authorizer authorizer = controller.getAuthorizer();
        if (AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) authorizer;
            existingAuthFingerprint = managedAuthorizer.getFingerprint().getBytes(StandardCharsets.UTF_8);
        } else {
            existingAuthFingerprint = null;
        }

        // serialize controller state to bytes
        final byte[] existingFlow;
        try {
            existingFlow = controller.isFlowSynchronized() ? toBytes(controller) : readFlowFromDisk();
            return new StandardDataFlow(existingFlow, existingSnippets, existingAuthFingerprint, missingComponents);
        } catch (final IOException e) {
            throw new FlowSerializationException(e);
        }
    }

    private byte[] toBytes(final FlowController flowController) throws FlowSerializationException {
        final ByteArrayOutputStream result = new ByteArrayOutputStream();
        final FlowSerializer<VersionedDataflow> flowSerializer = new VersionedFlowSerializer(extensionManager);
        flowController.serialize(flowSerializer, result);
        return result.toByteArray();
    }


    private byte[] readFlowFromDisk() throws IOException {
        if (flowStorageFile.length() == 0) {
            return new byte[0];
        }

        try (final InputStream in = new FileInputStream(flowStorageFile);
             final InputStream gzipIn = new GZIPInputStream(in);
             final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            FileUtils.copy(gzipIn, baos);

            return baos.toByteArray();
        }
    }


    private void inheritSnippets(final FlowController controller, final DataFlow proposedFlow) {
        // clear the snippets that are currently in memory
        logger.trace("Clearing existing snippets");
        final SnippetManager snippetManager = controller.getSnippetManager();
        snippetManager.clear();

        // if proposed flow has any snippets, load them
        logger.trace("Loading proposed snippets");
        final byte[] proposedSnippets = proposedFlow.getSnippets();
        if (proposedSnippets != null && proposedSnippets.length > 0) {
            for (final StandardSnippet snippet : SnippetManager.parseBytes(proposedSnippets)) {
                snippetManager.addSnippet(snippet);
            }
        }
    }

    private void backupExistingFlow() {
        if (flowStorageFile.exists()) {
            try {
                final File archiveFile = archiveManager.archive(flowStorageFile);
                logger.info("Successfully created backup of existing flow to {} before inheriting dataflow", archiveFile.getAbsolutePath());
            } catch (final IOException ioe) {
                throw new UninheritableFlowException("Could not inherit flow because failed to make a backup of existing flow", ioe);
            }
        }
    }

    private static class FlowControllerComponentScheduler extends AbstractComponentScheduler implements ComponentScheduler {
        private final FlowController flowController;

        public FlowControllerComponentScheduler(final FlowController flowController, final VersionedComponentStateLookup stateLookup) {
            super(flowController.getControllerServiceProvider(), stateLookup);
            this.flowController = flowController;
        }

        @Override
        public void startNow(final Connectable component) {
            if (ExecutionEngine.STATELESS == component.getProcessGroup().resolveExecutionEngine()) {
                logger.info("{} should be running but will not start it because its Process Group is configured to run Stateless", component);
                return;
            }

            switch (component.getConnectableType()) {
                case PROCESSOR -> flowController.startProcessor(component.getProcessGroupIdentifier(), component.getIdentifier());
                case INPUT_PORT, OUTPUT_PORT -> flowController.startConnectable(component);
                case REMOTE_INPUT_PORT, REMOTE_OUTPUT_PORT -> flowController.startTransmitting((RemoteGroupPort) component);
            }
        }

        @Override
        public void stopComponent(final Connectable component) {
            flowController.stopConnectable(component);
        }

        @Override
        protected void startNow(final ReportingTaskNode reportingTask) {
            flowController.startReportingTask(reportingTask);
        }

        @Override
        protected void startNow(final ProcessGroup statelessGroup) {
            flowController.startProcessGroup(statelessGroup);
        }
    }
}
