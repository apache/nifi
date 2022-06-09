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
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerCapabilityDetection;
import org.apache.nifi.authorization.ManagedAuthorizer;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.AbstractComponentNode;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.MissingBundleException;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.SnippetManager;
import org.apache.nifi.controller.StandardSnippet;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.UninheritableFlowException;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.controller.flow.VersionedFlowEncodingVersion;
import org.apache.nifi.controller.flow.VersionedRegistry;
import org.apache.nifi.controller.flow.VersionedTemplate;
import org.apache.nifi.controller.inheritance.AuthorizerCheck;
import org.apache.nifi.controller.inheritance.BundleCompatibilityCheck;
import org.apache.nifi.controller.inheritance.ConnectionMissingCheck;
import org.apache.nifi.controller.inheritance.FlowInheritability;
import org.apache.nifi.controller.inheritance.FlowInheritabilityCheck;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.encrypt.EncryptionException;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.groups.AbstractComponentScheduler;
import org.apache.nifi.groups.BundleUpdateStrategy;
import org.apache.nifi.groups.ComponentIdGenerator;
import org.apache.nifi.groups.ComponentScheduler;
import org.apache.nifi.groups.GroupSynchronizationOptions;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.persistence.FlowConfigurationArchiveManager;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.diff.ComparableDataFlow;
import org.apache.nifi.registry.flow.diff.DifferenceDescriptor;
import org.apache.nifi.registry.flow.diff.FlowComparator;
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
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class VersionedFlowSynchronizer implements FlowSynchronizer {
    private static final Logger logger = LoggerFactory.getLogger(VersionedFlowSynchronizer.class);
    private static final String ENCRYPTED_VALUE_PREFIX = "enc{";
    private static final String ENCRYPTED_VALUE_SUFFIX = "}";

    private final PropertyEncryptor encryptor;
    private final ExtensionManager extensionManager;
    private final File flowStorageFile;
    private final FlowConfigurationArchiveManager archiveManager;

    public VersionedFlowSynchronizer(final PropertyEncryptor encryptor, final ExtensionManager extensionManager, final File flowStorageFile, final FlowConfigurationArchiveManager archiveManager) {
        this.encryptor = encryptor;
        this.extensionManager = extensionManager;
        this.flowStorageFile = flowStorageFile;
        this.archiveManager = archiveManager;
    }


    public synchronized void sync(final FlowController controller, final DataFlow proposedFlow, final PropertyEncryptor encryptor, final FlowService flowService,
                                  final BundleUpdateStrategy bundleUpdateStrategy)
                    throws FlowSerializationException, UninheritableFlowException, FlowSynchronizationException, MissingBundleException {

        final long start = System.currentTimeMillis();
        final FlowManager flowManager = controller.getFlowManager();
        final ProcessGroup root = flowManager.getRootGroup();

        // handle corner cases involving no proposed flow
        if (proposedFlow == null) {
            if (root.isEmpty()) {
                return;  // no sync to perform
            } else {
                throw new UninheritableFlowException("Proposed configuration is empty, but the controller contains a data flow.");
            }
        }

        // determine if the controller already had flow sync'd to it
        final boolean flowAlreadySynchronized = controller.isFlowSynchronized();
        logger.info("Synchronizing FlowController with proposed flow: Controller Already Synchronized = {}", flowAlreadySynchronized);

        // If bundle update strategy is configured to allow for compatible bundles, update any components to use compatible bundles if
        // the exact bundle does not exist.
        if (bundleUpdateStrategy == BundleUpdateStrategy.USE_SPECIFIED_OR_COMPATIBLE_OR_GHOST) {
            mapCompatibleBundles(proposedFlow, controller.getExtensionManager());
        }

        // serialize controller state to bytes
        final DataFlow existingDataFlow = getExistingDataFlow(controller);
        checkFlowInheritability(existingDataFlow, proposedFlow, controller, bundleUpdateStrategy);

        final FlowComparison flowComparison = compareFlows(existingDataFlow, proposedFlow, encryptor);
        final Set<FlowDifference> flowDifferences = flowComparison.getDifferences();
        if (flowDifferences.isEmpty()) {
            logger.debug("No differences between current flow and proposed flow. Will not create backup of existing flow.");
        } else if (isExistingFlowEmpty(controller)) {
            logger.debug("Currently loaded dataflow is empty. Will not create backup of existing flow.");
        } else {
            backupExistingFlow();
        }

        final AffectedComponentSet affectedComponents = determineAffectedComponents(flowComparison, controller);
        final AffectedComponentSet activeSet = affectedComponents.toActiveSet();

        // Stop the active components, and then wait for all components to be stopped.
        logger.info("In order to inherit proposed dataflow, will stop any components that will be affected by the update");
        activeSet.stop();

        try {
            // Ensure that the proposed flow doesn't remove any Connections for which there is currently data queued
            verifyNoConnectionsWithDataRemoved(existingDataFlow, proposedFlow, controller, flowComparison);

            synchronizeFlow(controller, existingDataFlow, proposedFlow, affectedComponents);
        } finally {
            // We have to call toExistingSet() here because some of the components that existed in the active set may no longer exist,
            // so attempting to start them will fail.
            final AffectedComponentSet startable = activeSet.toExistingSet().toStartableSet();

            final ComponentSetFilter runningComponentFilter = new RunningComponentSetFilter(proposedFlow.getVersionedDataflow());
            final ComponentSetFilter stoppedComponentFilter = runningComponentFilter.reverse();
            startable.removeComponents(stoppedComponentFilter);
            startable.start();
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

        for (final VersionedReportingTask reportingTask : dataflow.getReportingTasks()) {
            if (missingComponentIds.contains(reportingTask.getInstanceIdentifier())) {
                continue;
            }

            final Bundle compatibleBundle = getCompatibleBundle(reportingTask.getBundle(), extensionManager, reportingTask.getType());
            if (compatibleBundle != null) {
                reportingTask.setBundle(compatibleBundle);
            }
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
                inheritParameterContexts(controller, versionedFlow);
                inheritReportingTasks(controller, versionedFlow, affectedComponentSet);
                inheritRegistries(controller, versionedFlow);

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

                // We must remove templates before attempting to synchronize the Process Group, as synchronizing may result in removal of a Process Group,
                // which cannot be done while Templates exist. After synchronizing root Process Group, we will inherit any templates in the proposed flow
                final Set<Template> allTemplates = controller.getFlowManager().getRootGroup().findAllTemplates();
                allTemplates.forEach(template -> template.getProcessGroup().removeTemplate(template));

                // Synchronize the root group
                final GroupSynchronizationOptions syncOptions = new GroupSynchronizationOptions.Builder()
                    .componentIdGenerator(componentIdGenerator)
                    .componentComparisonIdLookup(VersionedComponent::getInstanceIdentifier) // compare components by Instance ID because both versioned flows are derived from instantiated flows
                    .componentScheduler(componentScheduler)
                    .ignoreLocalModifications(true)
                    .updateGroupSettings(true)
                    .updateDescendantVersionedFlows(true)
                    .updateExistingVariables(true)
                    .updateGroupVersionControlSnapshot(false)
                    .updateExistingVariables(true)
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
                    .build();

                rootGroup.synchronizeFlow(versionedExternalFlow, syncOptions, flowMappingOptions);

                // Inherit templates, now that all necessary Process Groups have been created
                inheritTemplates(controller, versionedFlow);
            }

            inheritSnippets(controller, proposedFlow);
            inheritAuthorizations(existingFlow, proposedFlow, controller);
        } catch (final Exception ex) {
            throw new FlowSynchronizationException(ex);
        }
    }

    private FlowComparison compareFlows(final DataFlow existingFlow, final DataFlow proposedFlow, final PropertyEncryptor encryptor) {
        final DifferenceDescriptor differenceDescriptor = new StaticDifferenceDescriptor();

        final VersionedDataflow existingVersionedFlow = existingFlow.getVersionedDataflow() == null ? createEmptyVersionedDataflow() : existingFlow.getVersionedDataflow();
        final ComparableDataFlow localDataFlow = new StandardComparableDataFlow("Local Flow", existingVersionedFlow.getRootGroup(), toSet(existingVersionedFlow.getControllerServices()),
            toSet(existingVersionedFlow.getReportingTasks()), toSet(existingVersionedFlow.getParameterContexts()));

        final VersionedDataflow clusterVersionedFlow = proposedFlow.getVersionedDataflow();
        final ComparableDataFlow clusterDataFlow = new StandardComparableDataFlow("Cluster Flow", clusterVersionedFlow.getRootGroup(), toSet(clusterVersionedFlow.getControllerServices()),
            toSet(clusterVersionedFlow.getReportingTasks()), toSet(clusterVersionedFlow.getParameterContexts()));

        final FlowComparator flowComparator = new StandardFlowComparator(localDataFlow, clusterDataFlow, Collections.emptySet(),
            differenceDescriptor, encryptor::decrypt, VersionedComponent::getInstanceIdentifier);
        final FlowComparison flowComparison = flowComparator.compare();
        return flowComparison;
    }

    private <T> Set<T> toSet(final List<T> values) {
        if (values == null || values.isEmpty()) {
            return new HashSet<>();
        }

        return new HashSet<>(values);
    }

    private VersionedDataflow createEmptyVersionedDataflow() {
        final VersionedDataflow dataflow = new VersionedDataflow();
        dataflow.setControllerServices(Collections.emptyList());
        dataflow.setEncodingVersion(new VersionedFlowEncodingVersion(2, 0));
        dataflow.setParameterContexts(Collections.emptyList());
        dataflow.setRegistries(Collections.emptyList());
        dataflow.setReportingTasks(Collections.emptyList());
        dataflow.setRootGroup(new VersionedProcessGroup());
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


    private void inheritTemplates(final FlowController controller, final VersionedDataflow dataflow) {
        if (dataflow.getTemplates() == null) {
            return;
        }

        logger.debug("Synchronizing templates in dataflow");
        final FlowManager flowManager = controller.getFlowManager();
        for (final VersionedTemplate versionedTemplate : dataflow.getTemplates()) {
            final ProcessGroup group = flowManager.getGroup(versionedTemplate.getGroupIdentifier());
            if (group == null) {
                logger.warn("Found Template for Process Group with ID {} but no Process Group exists with that ID", versionedTemplate.getGroupIdentifier());
                continue;
            }

            group.addTemplate(new Template(versionedTemplate.getTemplateDto()));
        }
    }

    private void inheritRegistries(final FlowController controller, final VersionedDataflow dataflow) {
        final FlowRegistryClient registryClient = controller.getFlowRegistryClient();

        for (final VersionedRegistry versionedRegistry : dataflow.getRegistries()) {
            final FlowRegistry flowRegistry = registryClient.getFlowRegistry(versionedRegistry.getId());
            if (flowRegistry == null) {
                // Check if there's any registry with the same name. If so, remove it so that we can add this one in.
                for (final String registryId : registryClient.getRegistryIdentifiers()) {
                    final FlowRegistry registry = registryClient.getFlowRegistry(registryId);
                    if (Objects.equals(versionedRegistry.getName(), registry.getName())) {
                        registryClient.removeFlowRegistry(registryId);
                        break;
                    }
                }

                addRegistry(registryClient, versionedRegistry);
            } else {
                updateRegistry(flowRegistry, versionedRegistry);
            }
        }
    }

    private void addRegistry(final FlowRegistryClient registryClient, final VersionedRegistry versionedRegistry) {
        registryClient.addFlowRegistry(versionedRegistry.getId(), versionedRegistry.getName(), versionedRegistry.getUrl(), versionedRegistry.getDescription());
    }

    private void updateRegistry(final FlowRegistry registry, final VersionedRegistry versionedRegistry) {
        registry.setDescription(versionedRegistry.getDescription());
        registry.setName(versionedRegistry.getName());
        registry.setURL(versionedRegistry.getUrl());
    }

    private void inheritReportingTasks(final FlowController controller, final VersionedDataflow dataflow, final AffectedComponentSet affectedComponentSet) throws ReportingTaskInstantiationException {
        for (final VersionedReportingTask versionedReportingTask : dataflow.getReportingTasks()) {
            final ReportingTaskNode existing = controller.getReportingTaskNode(versionedReportingTask.getInstanceIdentifier());
            if (existing == null) {
                addReportingTask(controller, versionedReportingTask);
            } else if (affectedComponentSet.isReportingTaskAffected(existing.getIdentifier())) {
                updateReportingTask(existing, versionedReportingTask, controller);
            }
        }
    }

    private void addReportingTask(final FlowController controller, final VersionedReportingTask reportingTask) throws ReportingTaskInstantiationException {
        final BundleCoordinate coordinate = createBundleCoordinate(reportingTask.getBundle(), reportingTask.getType());

        final ReportingTaskNode taskNode = controller.createReportingTask(reportingTask.getType(), reportingTask.getInstanceIdentifier(), coordinate, false);
        updateReportingTask(taskNode, reportingTask, controller);
    }

    private void updateReportingTask(final ReportingTaskNode taskNode, final VersionedReportingTask reportingTask, final FlowController controller) {
        taskNode.setName(reportingTask.getName());
        taskNode.setComments(reportingTask.getComments());
        taskNode.setSchedulingPeriod(reportingTask.getSchedulingPeriod());
        taskNode.setSchedulingStrategy(SchedulingStrategy.valueOf(reportingTask.getSchedulingStrategy()));

        taskNode.setAnnotationData(reportingTask.getAnnotationData());

        final Map<String, String> decryptedProperties = decryptProperties(reportingTask.getProperties());
        taskNode.setProperties(decryptedProperties);

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

    private void inheritParameterContexts(final FlowController controller, final VersionedDataflow dataflow) {
        controller.getFlowManager().withParameterContextResolution(() -> {
            final List<VersionedParameterContext> parameterContexts = dataflow.getParameterContexts();

            // Build mapping of name to context for resolution of inherited contexts
            final Map<String, VersionedParameterContext> namedParameterContexts = parameterContexts.stream()
                    .collect(
                            Collectors.toMap(VersionedParameterContext::getName, Function.identity())
                    );
            for (final VersionedParameterContext versionedParameterContext : parameterContexts) {
                inheritParameterContext(versionedParameterContext, controller.getFlowManager(), namedParameterContexts);
            }
        });
    }

    private void inheritParameterContext(
            final VersionedParameterContext versionedParameterContext,
            final FlowManager flowManager,
            final Map<String, VersionedParameterContext> namedParameterContexts
    ) {
        final ParameterContextManager contextManager = flowManager.getParameterContextManager();
        final ParameterContext existingContext = contextManager.getParameterContextNameMapping().get(versionedParameterContext.getName());
        if (existingContext == null) {
            addParameterContext(versionedParameterContext, flowManager, namedParameterContexts);
        } else {
            updateParameterContext(versionedParameterContext, existingContext, flowManager, namedParameterContexts);
        }
    }

    private void addParameterContext(
            final VersionedParameterContext versionedParameterContext,
            final FlowManager flowManager,
            final Map<String, VersionedParameterContext> namedParameterContexts
    ) {
        final Map<String, Parameter> parameters = createParameterMap(versionedParameterContext);

        final ParameterContextManager contextManager = flowManager.getParameterContextManager();
        final List<String> referenceIds = findReferencedParameterContextIds(versionedParameterContext, contextManager, namedParameterContexts);

        flowManager.createParameterContext(versionedParameterContext.getInstanceIdentifier(), versionedParameterContext.getName(), parameters, referenceIds);
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

    private Map<String, Parameter> createParameterMap(final VersionedParameterContext versionedParameterContext) {
        final Map<String, Parameter> parameters = new HashMap<>();
        for (final VersionedParameter versioned : versionedParameterContext.getParameters()) {
            final ParameterDescriptor descriptor = new ParameterDescriptor.Builder()
                .description(versioned.getDescription())
                .name(versioned.getName())
                .sensitive(versioned.isSensitive())
                .build();

            final String parameterValue;
            final String rawValue = versioned.getValue();
            if (rawValue == null) {
                parameterValue = null;
            } else if (versioned.isSensitive() && rawValue.startsWith(ENCRYPTED_VALUE_PREFIX) && rawValue.endsWith(ENCRYPTED_VALUE_SUFFIX)) {
                final String extractedValue = rawValue.substring(ENCRYPTED_VALUE_PREFIX.length(), rawValue.length() - ENCRYPTED_VALUE_SUFFIX.length());
                parameterValue = encryptor.decrypt(extractedValue);
            } else {
                parameterValue = rawValue;
            }

            final Parameter parameter = new Parameter(descriptor, parameterValue);
            parameters.put(versioned.getName(), parameter);
        }

        return parameters;
    }

    private void updateParameterContext(
            final VersionedParameterContext versionedParameterContext,
            final ParameterContext parameterContext,
            final FlowManager flowManager,
            final Map<String, VersionedParameterContext> namedParameterContexts
    ) {
        final Map<String, Parameter> parameters = createParameterMap(versionedParameterContext);

        final Map<String, String> currentValues = new HashMap<>();
        parameterContext.getParameters().values().forEach(param -> currentValues.put(param.getDescriptor().getName(), param.getValue()));

        if (logger.isDebugEnabled()) {
            final Map<String, String> proposedValues = parameters.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().getValue()));
            logger.debug("For Parameter Context {}, current parameters = {}, proposed = {}", parameterContext.getName(), currentValues, proposedValues);
        }

        final Map<String, Parameter> updatedParameters = new HashMap<>();
        final Set<String> proposedParameterNames = new HashSet<>();
        for (final VersionedParameter parameter : versionedParameterContext.getParameters()) {
            final String parameterName = parameter.getName();
            final String currentValue = currentValues.get(parameterName);

            proposedParameterNames.add(parameterName);
            if (!Objects.equals(currentValue, parameter.getValue())) {
                final Parameter updatedParameterObject = parameters.get(parameterName);
                updatedParameters.put(parameterName, updatedParameterObject);
            }
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
        final Set<ControllerServiceNode> controllerServicesAdded = new HashSet<>();
        for (final VersionedControllerService versionedControllerService : controllerServices) {
            final ControllerServiceNode serviceNode = flowManager.getRootControllerService(versionedControllerService.getInstanceIdentifier());
            if (serviceNode == null) {
                final ControllerServiceNode added = addRootControllerService(controller, versionedControllerService);
                controllerServicesAdded.add(added);
            }
        }

        for (final VersionedControllerService versionedControllerService : controllerServices) {
            final ControllerServiceNode serviceNode = flowManager.getRootControllerService(versionedControllerService.getInstanceIdentifier());
            if (controllerServicesAdded.contains(serviceNode) || affectedComponentSet.isControllerServiceAffected(serviceNode.getIdentifier())) {
                updateRootControllerService(serviceNode, versionedControllerService);
            }
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
        }

        // Disable any Controller-level services that are intended to be disabled.
        if (!toDisable.isEmpty()) {
            controller.getControllerServiceProvider().disableControllerServicesAsync(toDisable);
        }
    }

    private ControllerServiceNode addRootControllerService(final FlowController controller, final VersionedControllerService versionedControllerService) {
        final BundleCoordinate bundleCoordinate = createBundleCoordinate(versionedControllerService.getBundle(), versionedControllerService.getType());
        final ControllerServiceNode serviceNode = controller.getFlowManager().createControllerService(versionedControllerService.getType(),
            versionedControllerService.getInstanceIdentifier(), bundleCoordinate,Collections.emptySet(), true, true, null);

        controller.getFlowManager().addRootControllerService(serviceNode);
        return serviceNode;
    }

    private void updateRootControllerService(final ControllerServiceNode serviceNode, final VersionedControllerService versionedControllerService) {
        serviceNode.pauseValidationTrigger();
        try {
            serviceNode.setName(versionedControllerService.getName());
            serviceNode.setAnnotationData(versionedControllerService.getAnnotationData());
            serviceNode.setComments(versionedControllerService.getComments());

            final Map<String, String> decryptedProperties = decryptProperties(versionedControllerService.getProperties());
            serviceNode.setProperties(decryptedProperties);
        } finally {
            serviceNode.resumeValidationTrigger();
        }
    }

    private Map<String, String> decryptProperties(final Map<String, String> encrypted) {
        final Map<String, String> decrypted = new HashMap<>(encrypted.size());
        encrypted.forEach((key, value) -> decrypted.put(key, decrypt(value)));
        return decrypted;
    }

    private String decrypt(final String value) {
        if (value != null && value.startsWith(FlowSerializer.ENC_PREFIX) && value.endsWith(FlowSerializer.ENC_SUFFIX)) {
            try {
                return encryptor.decrypt(value.substring(FlowSerializer.ENC_PREFIX.length(), value.length() - FlowSerializer.ENC_SUFFIX.length()));
            } catch (EncryptionException e) {
                final String moreDescriptiveMessage = "There was a problem decrypting a sensitive flow configuration value. " +
                    "Check that the nifi.sensitive.props.key value in nifi.properties matches the value used to encrypt the flow.json.gz file";
                logger.error(moreDescriptiveMessage, e);
                throw new EncryptionException(moreDescriptiveMessage, e);
            }
        } else {
            return value;
        }
    }

    private BundleCoordinate createBundleCoordinate(final Bundle bundle, final String componentType) {
        BundleCoordinate coordinate;
        try {
            final BundleDTO bundleDto = new BundleDTO(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
            coordinate = BundleUtils.getCompatibleBundle(extensionManager, componentType, bundleDto);
        } catch (final IllegalStateException e) {
            coordinate = new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
        }

        return coordinate;
    }

    private void inheritAuthorizations(final DataFlow existingFlow, final DataFlow proposedFlow, final FlowController controller) {
        final Authorizer authorizer = controller.getAuthorizer();
        if (!(authorizer instanceof ManagedAuthorizer)) {
            return;
        }

        final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) authorizer;
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

        final Set<String> registryIdentifiers = controller.getFlowRegistryClient().getRegistryIdentifiers();
        if (!registryIdentifiers.isEmpty()) {
            logger.debug("Existing Dataflow is not empty because there are {} NiFi Registries", registryIdentifiers.size());
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

        // Determine missing components
        final Set<String> missingComponents = new HashSet<>();
        flowManager.getAllControllerServices().stream().filter(ComponentNode::isExtensionMissing).forEach(cs -> missingComponents.add(cs.getIdentifier()));
        flowManager.getAllReportingTasks().stream().filter(ComponentNode::isExtensionMissing).forEach(r -> missingComponents.add(r.getIdentifier()));
        flowManager.findAllProcessors(AbstractComponentNode::isExtensionMissing).forEach(p -> missingComponents.add(p.getIdentifier()));

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
        final FlowSerializer<VersionedDataflow> flowSerializer = new VersionedFlowSerializer(encryptor, extensionManager);
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

            final byte[] contents = baos.toByteArray();
            return contents;
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
            switch (component.getConnectableType()) {
                case PROCESSOR:
                    flowController.startProcessor(component.getProcessGroupIdentifier(), component.getIdentifier());
                    break;
                case INPUT_PORT:
                case OUTPUT_PORT:
                    flowController.startConnectable(component);
                    break;
                case REMOTE_INPUT_PORT:
                case REMOTE_OUTPUT_PORT:
                    flowController.startTransmitting((RemoteGroupPort) component);
                    break;
            }
        }

        @Override
        public void stopComponent(final Connectable component) {
            flowController.stopConnectable(component);
        }

        @Override
        protected void enableNow(final Collection<ControllerServiceNode> controllerServices) {
            flowController.getControllerServiceProvider().enableControllerServices(controllerServices);
        }
    }
}
