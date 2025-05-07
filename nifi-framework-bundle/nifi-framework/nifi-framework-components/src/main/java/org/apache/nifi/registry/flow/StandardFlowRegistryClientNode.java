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
package org.apache.nifi.registry.flow;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.AbstractComponentNode;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public final class StandardFlowRegistryClientNode extends AbstractComponentNode implements FlowRegistryClientNode {
    private static final Logger logger = LoggerFactory.getLogger(StandardFlowRegistryClientNode.class);

    private final FlowManager flowManager;
    private final Authorizable parent;
    private final AtomicReference<LoggableComponent<FlowRegistryClient>> client = new AtomicReference<>();
    private final ControllerServiceProvider serviceProvider;
    private volatile String description;

    public StandardFlowRegistryClientNode(
            final Authorizable parent,
            final FlowManager flowManager,
            final LoggableComponent<FlowRegistryClient> client,
            final String id,
            final ValidationContextFactory validationContextFactory,
            final ControllerServiceProvider serviceProvider,
            final String componentType,
            final String componentCanonicalClass,
            final ReloadComponent reloadComponent,
            final ExtensionManager extensionManager,
            final ValidationTrigger validationTrigger,
            final boolean isExtensionMissing) {
        super(id, validationContextFactory, serviceProvider, componentType, componentCanonicalClass, reloadComponent, extensionManager, validationTrigger, isExtensionMissing);
        this.parent = parent;
        this.flowManager = flowManager;
        this.serviceProvider = serviceProvider;
        this.client.set(client);
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return parent;
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.RegistryClient, getIdentifier(), getName());
    }

    @Override
    public String getProcessGroupIdentifier() {
        return null;
    }

    @Override
    protected List<ValidationResult> validateConfig() {
        return Collections.emptyList();
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {
        // FlowRegistryClients has no running state
    }

    @Override
    protected ParameterContext getParameterContext() {
        return null;
    }

    @Override
    public void reload(Set<URL> additionalUrls) throws Exception {
        final String additionalResourcesFingerprint = ClassLoaderUtils.generateAdditionalUrlsFingerprint(additionalUrls, determineClasloaderIsolationKey());
        setAdditionalResourcesFingerprint(additionalResourcesFingerprint);
        getReloadComponent().reload(this, getCanonicalClassName(), getBundleCoordinate(), additionalUrls);
    }

    @Override
    public BundleCoordinate getBundleCoordinate() {
        return client.get().getBundleCoordinate();
    }

    @Override
    public ConfigurableComponent getComponent() {
        return client.get().getComponent();
    }

    @Override
    public TerminationAwareLogger getLogger() {
        return client.get().getLogger();
    }

    @Override
    public Class<?> getComponentClass() {
        return client.get().getComponent().getClass();
    }

    @Override
    public boolean isRestricted() {
        return getComponentClass().isAnnotationPresent(Restricted.class);
    }

    @Override
    public boolean isDeprecated() {
        return getComponentClass().isAnnotationPresent(DeprecationNotice.class);
    }

    @Override
    public boolean isValidationNecessary() {
        return getValidationStatus() != ValidationStatus.VALID;
    }

    @Override
    public Optional<ProcessGroup> getParentProcessGroup() {
        return Optional.empty();
    }

    @Override
    public ParameterLookup getParameterLookup() {
        return ParameterLookup.EMPTY;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(final String description) {
        this.description = CharacterFilterUtils.filterInvalidXmlCharacters(description);
    }

    @Override
    public boolean isStorageLocationApplicable(final String location) {
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), client.getClass(), getIdentifier())) {
            return client.get().getComponent().isStorageLocationApplicable(getConfigurationContext(), location);
        }
    }

    @Override
    public boolean isBranchingSupported() {
        return client.get().getComponent().isBranchingSupported(getConfigurationContext());
    }

    @Override
    public Set<FlowRegistryBranch> getBranches(final FlowRegistryClientUserContext context) throws FlowRegistryException, IOException {
        return execute(() -> client.get().getComponent().getBranches(getConfigurationContext(context)));
    }

    @Override
    public FlowRegistryBranch getDefaultBranch(final FlowRegistryClientUserContext context) throws FlowRegistryException, IOException {
        return execute(() -> client.get().getComponent().getDefaultBranch(getConfigurationContext(context)));
    }

    @Override
    public Set<FlowRegistryBucket> getBuckets(final FlowRegistryClientUserContext context, final String branch) throws FlowRegistryException, IOException {
        return execute(() -> client.get().getComponent().getBuckets(getConfigurationContext(context), branch));
    }

    @Override
    public FlowRegistryBucket getBucket(final FlowRegistryClientUserContext context, final BucketLocation bucketLocation) throws FlowRegistryException, IOException {
        return execute(() -> client.get().getComponent().getBucket(getConfigurationContext(context), bucketLocation));
    }

    @Override
    public RegisteredFlow registerFlow(final FlowRegistryClientUserContext context, final RegisteredFlow flow) throws FlowRegistryException, IOException {
        return execute(() -> client.get().getComponent().registerFlow(getConfigurationContext(context), flow));
    }

    @Override
    public RegisteredFlow deregisterFlow(final FlowRegistryClientUserContext context, final FlowLocation flowLocation) throws FlowRegistryException, IOException {
        return execute(() -> client.get().getComponent().deregisterFlow(getConfigurationContext(context), flowLocation));
    }

    @Override
    public RegisteredFlow getFlow(final FlowRegistryClientUserContext context, final FlowLocation flowLocation) throws FlowRegistryException, IOException {
        return execute(() -> client.get().getComponent().getFlow(getConfigurationContext(context), flowLocation));
    }

    @Override
    public Set<RegisteredFlow> getFlows(final FlowRegistryClientUserContext context, final BucketLocation bucketLocation) throws FlowRegistryException, IOException {
        return execute(() -> client.get().getComponent().getFlows(getConfigurationContext(context), bucketLocation));
    }

    @Override
    public FlowSnapshotContainer getFlowContents(final FlowRegistryClientUserContext context, final FlowVersionLocation flowVersionLocation, final boolean fetchRemoteFlows)
            throws FlowRegistryException, IOException {
        final RegisteredFlowSnapshot flowSnapshot = execute(() -> client.get().getComponent().getFlowContents(getConfigurationContext(context), flowVersionLocation));

        final FlowSnapshotContainer snapshotContainer = new FlowSnapshotContainer(flowSnapshot);
        if (fetchRemoteFlows) {
            final VersionedProcessGroup contents = flowSnapshot.getFlowContents();
            for (final VersionedProcessGroup child : contents.getProcessGroups()) {
                final Map<String, VersionedParameterContext> childParameterContexts = populateVersionedContentsRecursively(context, child, snapshotContainer);

                for (final Map.Entry<String, VersionedParameterContext> childParameterContext : childParameterContexts.entrySet()) {
                    flowSnapshot.getParameterContexts().putIfAbsent(childParameterContext.getKey(), childParameterContext.getValue());
                }
                populateVersionedContentsRecursively(context, child, snapshotContainer);
            }
        }

        return snapshotContainer;
    }

    @Override
    public RegisteredFlowSnapshot registerFlowSnapshot(
            final FlowRegistryClientUserContext context,
            final RegisteredFlow flow,
            final VersionedProcessGroup snapshot,
            final Map<String, ExternalControllerServiceReference> externalControllerServices,
            final Map<String, VersionedParameterContext> parameterContexts,
            final Map<String, ParameterProviderReference> parameterProviderReferences,
            final String comments,
            final String expectedVersion,
            final RegisterAction registerAction) throws FlowRegistryException, IOException {

        final RegisteredFlowSnapshot registeredFlowSnapshot = createRegisteredFlowSnapshot(
                context, flow, snapshot, externalControllerServices, parameterContexts, parameterProviderReferences, comments, expectedVersion);
        return execute(() -> client.get().getComponent().registerFlowSnapshot(getConfigurationContext(context), registeredFlowSnapshot, registerAction));
    }

    @Override
    public Set<RegisteredFlowSnapshotMetadata> getFlowVersions(final FlowRegistryClientUserContext context, final FlowLocation flowLocation) throws FlowRegistryException, IOException {
        return execute(() -> client.get().getComponent().getFlowVersions(getConfigurationContext(context), flowLocation));
    }

    @Override
    public Optional<String> getLatestVersion(final FlowRegistryClientUserContext context, final FlowLocation flowLocation) throws FlowRegistryException, IOException {
        return execute(() -> client.get().getComponent().getLatestVersion(getConfigurationContext(context), flowLocation));
    }

    @Override
    public String generateFlowId(final String flowName) throws IOException, FlowRegistryException {
        return execute(() -> client.get().getComponent().generateFlowId(flowName));
    }

    @Override
    public void setComponent(final LoggableComponent<FlowRegistryClient> component) {
        client.set(component);
    }

    private <T> T execute(final FlowRegistryClientAction<T> action) throws FlowRegistryException, IOException {
        final ValidationStatus validationStatus = getValidationStatus();

        if (validationStatus != ValidationStatus.VALID) {
            Collection<ValidationResult> validationResults = getValidationErrors();
            if (validationResults == null) {
                validationResults = Collections.emptyList();
            }

            final List<String> validationProblems = validationResults.stream()
                .map(ValidationResult::getExplanation)
                .collect(Collectors.toList());

            throw new FlowRegistryInvalidException(validationProblems);
        }

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), client.getClass(), getIdentifier())) {
            return action.execute();
        }
    }

    private FlowRegistryClientConfigurationContext getConfigurationContext() {
        return new StandardFlowRegistryClientConfigurationContext(null, getRawPropertyValues(), this, serviceProvider);
    }

    private FlowRegistryClientConfigurationContext getConfigurationContext(FlowRegistryClientUserContext clientContext) {
        final String userIdentity = clientContext.getNiFiUserIdentity().orElse(null);
        return new StandardFlowRegistryClientConfigurationContext(userIdentity, getRawPropertyValues(), this, serviceProvider);
    }

    private String extractIdentity(final FlowRegistryClientUserContext context) {
        return context.getNiFiUserIdentity().orElse(null);
    }

    private Map<String, VersionedParameterContext> populateVersionedContentsRecursively(
            final FlowRegistryClientUserContext context,
            final VersionedProcessGroup group,
            final FlowSnapshotContainer snapshotContainer
    ) throws FlowRegistryException {
        Map<String, VersionedParameterContext> accumulatedParameterContexts = new HashMap<>();

        if (group == null) {
            return accumulatedParameterContexts;
        }

        final VersionedFlowCoordinates coordinates = group.getVersionedFlowCoordinates();

        if (coordinates != null) {
            final RegisteredFlowSnapshot snapshot = fetchFlowContents(context, coordinates, true);
            final VersionedProcessGroup contents = snapshot.getFlowContents();

            group.setVersionedFlowCoordinates(coordinates);
            group.setComments(contents.getComments());
            group.setConnections(contents.getConnections());
            group.setControllerServices(contents.getControllerServices());
            group.setFunnels(contents.getFunnels());
            group.setInputPorts(contents.getInputPorts());
            group.setLabels(contents.getLabels());
            group.setOutputPorts(contents.getOutputPorts());
            group.setProcessGroups(contents.getProcessGroups());
            group.setProcessors(contents.getProcessors());
            group.setRemoteProcessGroups(contents.getRemoteProcessGroups());
            group.setParameterContextName(contents.getParameterContextName());
            group.setFlowFileConcurrency(contents.getFlowFileConcurrency());
            group.setFlowFileOutboundPolicy(contents.getFlowFileOutboundPolicy());
            group.setDefaultFlowFileExpiration(contents.getDefaultFlowFileExpiration());
            group.setDefaultBackPressureObjectThreshold(contents.getDefaultBackPressureObjectThreshold());
            group.setDefaultBackPressureDataSizeThreshold(contents.getDefaultBackPressureDataSizeThreshold());
            group.setLogFileSuffix(contents.getLogFileSuffix());
            coordinates.setLatest(snapshot.isLatest());

            for (final Map.Entry<String, VersionedParameterContext> parameterContext : snapshot.getParameterContexts().entrySet()) {
                accumulatedParameterContexts.put(parameterContext.getKey(), parameterContext.getValue());
            }

            snapshotContainer.addChildSnapshot(snapshot, group);
        }

        for (final VersionedProcessGroup child : group.getProcessGroups()) {
            final Map<String, VersionedParameterContext> childParameterContexts = populateVersionedContentsRecursively(context, child, snapshotContainer);

            for (final Map.Entry<String, VersionedParameterContext> childParameterContext : childParameterContexts.entrySet()) {
                // We favor the context instance from the enclosing versioned flow
                accumulatedParameterContexts.putIfAbsent(childParameterContext.getKey(), childParameterContext.getValue());
            }
        }

        return accumulatedParameterContexts;
    }

    private RegisteredFlowSnapshot fetchFlowContents(final FlowRegistryClientUserContext context, final VersionedFlowCoordinates coordinates,
                                                     final boolean fetchRemoteFlows) throws FlowRegistryException {

        final String storageLocation = coordinates.getStorageLocation();
        final String branch = coordinates.getBranch();
        final String bucketId = coordinates.getBucketId();
        final String flowId = coordinates.getFlowId();
        final String version = coordinates.getVersion();
        final FlowVersionLocation flowVersionLocation = new FlowVersionLocation(branch, bucketId, flowId, version);

        final List<FlowRegistryClientNode> clientNodes = getRegistryClientsForInternalFlow(storageLocation);
        for (final FlowRegistryClientNode clientNode : clientNodes) {
            try {
                logger.debug("Attempting to fetch flow from Branch [{}] for Bucket [{}] Flow [{}] Version [{}] using {}", branch, bucketId, flowId, version, clientNode);
                final FlowSnapshotContainer snapshotContainer = clientNode.getFlowContents(context, flowVersionLocation, fetchRemoteFlows);
                final RegisteredFlowSnapshot snapshot = snapshotContainer.getFlowSnapshot();
                coordinates.setRegistryId(clientNode.getIdentifier());

                logger.debug("Successfully fetched flow from Branch [{}] for Bucket [{}] Flow [{}] Version [{}] using {}", branch, bucketId, flowId, version, clientNode);
                return snapshot;
            } catch (final Exception e) {
                logger.debug("Failed to fetch flow", e);
            }
        }

        throw new FlowRegistryException(String.format("Could not find any Registry Client that was able to fetch flow with Branch [%s] Bucket [%s] Flow [%s] Version [%s] with Storage Location [%s]",
            branch, bucketId, flowId, version, storageLocation));
    }

    private List<FlowRegistryClientNode> getRegistryClientsForInternalFlow(final String storageLocation) {
        // Sort clients based on whether or not they believe they are applicable for the given storage location
        final List<FlowRegistryClientNode> matchingClients = new ArrayList<>(flowManager.getAllFlowRegistryClients());
        matchingClients.sort(Comparator.comparing(client -> client.isStorageLocationApplicable(storageLocation) ? -1 : 1));
        return matchingClients;
    }

    private RegisteredFlowSnapshot createRegisteredFlowSnapshot(
            final FlowRegistryClientUserContext context,
            final RegisteredFlow flow,
            final VersionedProcessGroup snapshot,
            final Map<String, ExternalControllerServiceReference> externalControllerServices,
            final Map<String, VersionedParameterContext> parameterContexts,
            final Map<String, ParameterProviderReference> parameterProviderReferences,
            final String comments,
            final String expectedVersion) {
        final RegisteredFlowSnapshotMetadata metadata = new RegisteredFlowSnapshotMetadata();
        metadata.setBranch(flow.getBranch());
        metadata.setBucketIdentifier(flow.getBucketIdentifier());
        metadata.setFlowIdentifier(flow.getIdentifier());
        metadata.setAuthor(extractIdentity(context));
        metadata.setTimestamp(System.currentTimeMillis());
        metadata.setVersion(expectedVersion);
        metadata.setComments(comments);

        final RegisteredFlowSnapshot registeredFlowSnapshot = new RegisteredFlowSnapshot();
        registeredFlowSnapshot.setFlowContents(snapshot);
        registeredFlowSnapshot.setExternalControllerServices(externalControllerServices);
        registeredFlowSnapshot.setParameterContexts(parameterContexts);
        registeredFlowSnapshot.setFlowEncodingVersion(FlowRegistryUtil.FLOW_ENCODING_VERSION);
        registeredFlowSnapshot.setSnapshotMetadata(metadata);
        registeredFlowSnapshot.setParameterProviders(parameterProviderReferences);
        return registeredFlowSnapshot;
    }

    private interface FlowRegistryClientAction<T> {
        T execute() throws FlowRegistryException, IOException;
    }
}
