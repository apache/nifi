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
package org.apache.nifi.controller.registry;

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
import org.apache.nifi.controller.FlowController;
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
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.flow.FlowRegistryBucket;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.registry.flow.FlowRegistryClientUserContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.FlowRegistryInvalidException;
import org.apache.nifi.registry.flow.FlowRegistryUtil;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.SimpleRegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.SimpleRegisteredFlowSnapshotMetadata;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class StandardFlowRegistryClientNode extends AbstractComponentNode implements FlowRegistryClientNode {

    private final FlowController flowController;
    private final LoggableComponent<FlowRegistryClient> client;
    private volatile String description;

    public StandardFlowRegistryClientNode(
            final FlowController flowController,
            final LoggableComponent<FlowRegistryClient> client,
            final String id,
            final ValidationContextFactory validationContextFactory,
            final ControllerServiceProvider serviceProvider,
            final String componentType,
            final String componentCanonicalClass,
            final ComponentVariableRegistry variableRegistry,
            final ReloadComponent reloadComponent,
            final ExtensionManager extensionManager,
            final ValidationTrigger validationTrigger,
            final boolean isExtensionMissing) {
        super(id, validationContextFactory, serviceProvider, componentType, componentCanonicalClass, variableRegistry, reloadComponent, extensionManager, validationTrigger, isExtensionMissing);
        this.flowController = flowController;
        this.client = client;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return flowController;
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.Controller, getIdentifier(), getName());
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
    public void setProperties(final Map<String, String> properties, final boolean allowRemovalOfRequiredProperties, final Set<String> updatedSensitiveDynamicPropertyNames) {
        super.setProperties(properties, allowRemovalOfRequiredProperties, updatedSensitiveDynamicPropertyNames);
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
        return client.getBundleCoordinate();
    }

    @Override
    public ConfigurableComponent getComponent() {
        return client.getComponent();
    }

    @Override
    public TerminationAwareLogger getLogger() {
        return client.getLogger();
    }

    @Override
    public Class<?> getComponentClass() {
        return client.getComponent().getClass();
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
    public Set<FlowRegistryBucket> getBuckets(final FlowRegistryClientUserContext context) throws FlowRegistryException, IOException {
        return execute(() -> client.getComponent().getBuckets(getConfigurationContext(context)));
    }

    @Override
    public FlowRegistryBucket getBucket(final FlowRegistryClientUserContext context, final String bucketId) throws FlowRegistryException, IOException {
        return execute(() -> client.getComponent().getBucket(getConfigurationContext(context), bucketId));
    }

    @Override
    public RegisteredFlow registerFlow(final FlowRegistryClientUserContext context, final RegisteredFlow flow) throws FlowRegistryException, IOException {
        return execute(() -> client.getComponent().registerFlow(getConfigurationContext(context), flow));
    }

    @Override
    public RegisteredFlow deregisterFlow(final FlowRegistryClientUserContext context, final String bucketId, final String flowId) throws FlowRegistryException, IOException {
        return execute(() -> client.getComponent().deregisterFlow(getConfigurationContext(context), bucketId, flowId));
    }

    @Override
    public RegisteredFlow getFlow(final FlowRegistryClientUserContext context, final String bucketId, final String flowId) throws FlowRegistryException, IOException {
        return execute(() -> client.getComponent().getFlow(getConfigurationContext(context), bucketId, flowId));
    }

    @Override
    public Set<RegisteredFlow> getFlows(final FlowRegistryClientUserContext context, final String bucketId) throws FlowRegistryException, IOException {
        return execute(() -> client.getComponent().getFlows(getConfigurationContext(context), bucketId));
    }

    @Override
    public RegisteredFlowSnapshot getFlowContents(
            final FlowRegistryClientUserContext context, final String bucketId, final String flowId, final int version, final boolean fetchRemoteFlows
    ) throws FlowRegistryException, IOException {
        final RegisteredFlowSnapshot flowSnapshot = execute(() ->client.getComponent().getFlowContents(getConfigurationContext(context), bucketId, flowId, version));

        if (fetchRemoteFlows) {
            final VersionedProcessGroup contents = flowSnapshot.getFlowContents();
            for (final VersionedProcessGroup child : contents.getProcessGroups()) {
                populateVersionedContentsRecursively(context, child);
            }
        }

        return flowSnapshot;
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
            final int expectedVersion) throws FlowRegistryException, IOException {

        final SimpleRegisteredFlowSnapshot registeredFlowSnapshot = createRegisteredFlowSnapshot(
                context, flow, snapshot, externalControllerServices, parameterContexts, parameterProviderReferences, comments, expectedVersion);
        return execute(() -> client.getComponent().registerFlowSnapshot(getConfigurationContext(context), registeredFlowSnapshot));
    }

    @Override
    public Set<RegisteredFlowSnapshotMetadata> getFlowVersions(final FlowRegistryClientUserContext context, final String bucketId, final String flowId) throws FlowRegistryException, IOException {
        return execute(() -> client.getComponent().getFlowVersions(getConfigurationContext(context), bucketId, flowId));
    }

    @Override
    public int getLatestVersion(final FlowRegistryClientUserContext context, final String bucketId, final String flowId) throws FlowRegistryException, IOException {
        return execute(() -> client.getComponent().getLatestVersion(getConfigurationContext(context), bucketId, flowId));
    }

    private <T> T execute(final FlowRegistryClientAction<T> action) throws FlowRegistryException, IOException {
        final ValidationStatus validationStatus = getValidationStatus();

        if (validationStatus != ValidationStatus.VALID) {
            throw new FlowRegistryInvalidException(getValidationErrors().stream().map(e -> e.getExplanation()).collect(Collectors.toList()));
        }

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getExtensionManager(), client.getClass(), getIdentifier())) {
            return action.execute();
        }
    }

    private FlowRegistryClientConfigurationContext getConfigurationContext(FlowRegistryClientUserContext clientContext) {
        return new StandardFlowRegistryClientConfigurationContext(clientContext.getNiFiUserIdentity(), getRawPropertyValues(), this);
    }

    private String extractIdentity(final FlowRegistryClientUserContext context) {
        return context.getNiFiUserIdentity().orElse(null);
    }

    private void populateVersionedContentsRecursively(final FlowRegistryClientUserContext context, final VersionedProcessGroup group) throws IOException, FlowRegistryException {
        if (group == null) {
            return;
        }

        final VersionedFlowCoordinates coordinates = group.getVersionedFlowCoordinates();

        if (coordinates != null) {
            final String registryId = coordinates.getRegistryId();
            final String bucketId = coordinates.getBucketId();
            final String flowId = coordinates.getFlowId();
            final int version = coordinates.getVersion();

            final FlowManager manager = flowController.getFlowManager();
            final FlowRegistryClientNode flowRegistry = manager.getFlowRegistryClient(registryId);
            final RegisteredFlowSnapshot snapshot = flowRegistry.getFlowContents(context, bucketId, flowId, version, true);
            final VersionedProcessGroup contents = snapshot.getFlowContents();

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
            group.setVariables(contents.getVariables());
            group.setParameterContextName(contents.getParameterContextName());
            group.setFlowFileConcurrency(contents.getFlowFileConcurrency());
            group.setFlowFileOutboundPolicy(contents.getFlowFileOutboundPolicy());
            group.setDefaultFlowFileExpiration(contents.getDefaultFlowFileExpiration());
            group.setDefaultBackPressureObjectThreshold(contents.getDefaultBackPressureObjectThreshold());
            group.setDefaultBackPressureDataSizeThreshold(contents.getDefaultBackPressureDataSizeThreshold());
            coordinates.setLatest(snapshot.isLatest());
        }

        for (final VersionedProcessGroup child : group.getProcessGroups()) {
            populateVersionedContentsRecursively(context, child);
        }
    }

    private SimpleRegisteredFlowSnapshot createRegisteredFlowSnapshot(
            final FlowRegistryClientUserContext context,
            final RegisteredFlow flow,
            final VersionedProcessGroup snapshot,
            final Map<String, ExternalControllerServiceReference> externalControllerServices,
            final Map<String, VersionedParameterContext> parameterContexts,
            final Map<String, ParameterProviderReference> parameterProviderReferences,
            final String comments,
            final int expectedVersion) {
        final SimpleRegisteredFlowSnapshotMetadata metadata = new SimpleRegisteredFlowSnapshotMetadata();
        metadata.setBucketIdentifier(flow.getBucketIdentifier());
        metadata.setFlowIdentifier(flow.getIdentifier());
        metadata.setAuthor(extractIdentity(context));
        metadata.setTimestamp(System.currentTimeMillis());
        metadata.setVersion(expectedVersion);
        metadata.setComments(comments);

        final SimpleRegisteredFlowSnapshot registeredFlowSnapshot = new SimpleRegisteredFlowSnapshot();
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
