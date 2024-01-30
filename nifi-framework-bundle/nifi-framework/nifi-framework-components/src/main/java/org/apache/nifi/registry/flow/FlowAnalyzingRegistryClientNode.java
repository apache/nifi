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

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.flowanalysis.FlowAnalyzer;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flowanalysis.EnforcementPolicy;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.parameter.ParameterUpdate;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessGroup;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.validation.RuleViolation;
import org.apache.nifi.validation.RuleViolationsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class FlowAnalyzingRegistryClientNode implements FlowRegistryClientNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowAnalyzingRegistryClientNode.class);

    private final FlowRegistryClientNode node;
    private final ControllerServiceProvider serviceProvider;
    private final FlowAnalyzer flowAnalyzer;
    private final RuleViolationsManager ruleViolationsManager;
    private final FlowManager flowManager;
    private final NiFiRegistryFlowMapper flowMapper;

    public FlowAnalyzingRegistryClientNode(
            final FlowRegistryClientNode node,
            final ControllerServiceProvider serviceProvider,
            final FlowAnalyzer flowAnalyzer,
            final RuleViolationsManager ruleViolationsManager,
            final FlowManager flowManager,
            final NiFiRegistryFlowMapper flowMapper
    ) {
        this.node = Objects.requireNonNull(node);
        this.serviceProvider = Objects.requireNonNull(serviceProvider);
        this.flowAnalyzer = Objects.requireNonNull(flowAnalyzer);
        this.ruleViolationsManager = Objects.requireNonNull(ruleViolationsManager);
        this.flowManager = Objects.requireNonNull(flowManager);
        this.flowMapper = Objects.requireNonNull(flowMapper);
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
            final RegisterAction registerAction
    ) throws FlowRegistryException, IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Snapshot for flow {} is checked for violations before commit", snapshot.getInstanceIdentifier());
        }

        if (analyzeProcessGroupToRegister(snapshot)) {
            return node.registerFlowSnapshot(context, flow, snapshot, externalControllerServices, parameterContexts, parameterProviderReferences, comments, expectedVersion, registerAction);
        } else {
            throw new FlowRegistryPreCommitException("There are unresolved rule violations");
        }
    }

    private boolean analyzeProcessGroupToRegister(final VersionedProcessGroup snapshot) {
        final InstantiatedVersionedProcessGroup nonVersionedProcessGroup = flowMapper.mapNonVersionedProcessGroup(flowManager.getGroup(snapshot.getInstanceIdentifier()), serviceProvider);

        flowAnalyzer.analyzeProcessGroup(nonVersionedProcessGroup);
        final List<RuleViolation> ruleViolations = ruleViolationsManager.getRuleViolationsForGroup(snapshot.getInstanceIdentifier()).stream()
                .filter(ruleViolation -> EnforcementPolicy.ENFORCE.equals(ruleViolation.getEnforcementPolicy()))
                .toList();

        final boolean result = ruleViolations.isEmpty();

        if (LOGGER.isDebugEnabled()) {
            final String violations = ruleViolations.stream().map(RuleViolation::getViolationMessage).collect(Collectors.joining(", "));
            LOGGER.debug("Snapshot for {} has following violation(s): {}", snapshot.getInstanceIdentifier(), violations);
        }

        return result;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return node.getParentAuthorizable();
    }

    @Override
    public Resource getResource() {
        return node.getResource();
    }

    @Override
    public String getProcessGroupIdentifier() {
        return node.getProcessGroupIdentifier();
    }

    @Override
    public String getIdentifier() {
        return node.getIdentifier();
    }

    @Override
    public String getName() {
        return node.getName();
    }

    @Override
    public void setName(final String name) {
        node.setName(name);
    }

    @Override
    public String getAnnotationData() {
        return node.getAnnotationData();
    }

    @Override
    public void setAnnotationData(final String data) {
        node.setAnnotationData(data);
    }

    @Override
    public void setProperties(final Map<String, String> properties, final boolean allowRemovalOfRequiresProperties, final Set<String> sensitiveDynamicPropertyNames) {
        node.setProperties(properties, allowRemovalOfRequiresProperties, sensitiveDynamicPropertyNames);
    }

    @Override
    public void verifyCanUpdateProperties(final Map<String, String> properties) {
        node.verifyCanUpdateProperties(properties);
    }

    @Override
    public Set<String> getReferencedParameterNames() {
        return node.getReferencedParameterNames();
    }

    @Override
    public boolean isReferencingParameter() {
        return node.isReferencingParameter();
    }

    @Override
    public boolean isReferencingParameter(final String parameterName) {
        return node.isReferencingParameter(parameterName);
    }

    @Override
    public void onParametersModified(final Map<String, ParameterUpdate> parameterUpdates) {
        node.onParametersModified(parameterUpdates);
    }

    @Override
    public Set<String> getReferencedAttributeNames() {
        return node.getReferencedAttributeNames();
    }

    @Override
    public void pauseValidationTrigger() {
        node.pauseValidationTrigger();
    }

    @Override
    public void resumeValidationTrigger() {
        node.resumeValidationTrigger();
    }

    @Override
    public Map<PropertyDescriptor, String> getRawPropertyValues() {
        return node.getRawPropertyValues();
    }

    @Override
    public Map<PropertyDescriptor, String> getEffectivePropertyValues() {
        return node.getEffectivePropertyValues();
    }

    @Override
    public PropertyConfiguration getProperty(final PropertyDescriptor property) {
        return node.getProperty(property);
    }

    @Override
    public String getEffectivePropertyValue(final PropertyDescriptor property) {
        return node.getEffectivePropertyValue(property);
    }

    @Override
    public String getRawPropertyValue(final PropertyDescriptor property) {
        return node.getRawPropertyValue(property);
    }

    @Override
    public Map<PropertyDescriptor, PropertyConfiguration> getProperties() {
        return node.getProperties();
    }

    @Override
    public void reload(final Set<URL> additionalUrls) throws Exception {
        node.reload(additionalUrls);
    }

    @Override
    public void refreshProperties() {
        node.refreshProperties();
    }

    @Override
    public Set<URL> getAdditionalClasspathResources(final List<PropertyDescriptor> propertyDescriptors) {
        return node.getAdditionalClasspathResources(propertyDescriptors);
    }

    @Override
    public BundleCoordinate getBundleCoordinate() {
        return node.getBundleCoordinate();
    }

    @Override
    public ConfigurableComponent getComponent() {
        return node.getComponent();
    }

    @Override
    public TerminationAwareLogger getLogger() {
        return node.getLogger();
    }

    @Override
    public boolean isExtensionMissing() {
        return node.isExtensionMissing();
    }

    @Override
    public void setExtensionMissing(final boolean extensionMissing) {
        node.setExtensionMissing(extensionMissing);
    }

    @Override
    public void verifyCanUpdateBundle(final BundleCoordinate bundleCoordinate) throws IllegalStateException {
        node.verifyCanUpdateBundle(bundleCoordinate);
    }

    @Override
    public boolean isReloadAdditionalResourcesNecessary() {
        return node.isReloadAdditionalResourcesNecessary();
    }

    @Override
    public void reloadAdditionalResourcesIfNecessary() {
        node.reloadAdditionalResourcesIfNecessary();
    }

    @Override
    public void resetValidationState() {
        node.resetValidationState();
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        return node.getValidationErrors();
    }

    @Override
    public String getComponentType() {
        return node.getComponentType();
    }

    @Override
    public Class<?> getComponentClass() {
        return node.getComponentClass();
    }

    @Override
    public String getCanonicalClassName() {
        return node.getCanonicalClassName();
    }

    @Override
    public boolean isRestricted() {
        return node.isRestricted();
    }

    @Override
    public boolean isDeprecated() {
        return node.isDeprecated();
    }

    @Override
    public boolean isValidationNecessary() {
        return node.isValidationNecessary();
    }

    @Override
    public ValidationStatus getValidationStatus() {
        return node.getValidationStatus();
    }

    @Override
    public ValidationStatus getValidationStatus(final long timeout, final TimeUnit unit) {
        return node.getValidationStatus(timeout, unit);
    }

    @Override
    public ValidationStatus performValidation() {
        return node.performValidation();
    }

    @Override
    public ValidationState performValidation(final ValidationContext validationContext) {
        return node.performValidation(validationContext);
    }

    @Override
    public ValidationState performValidation(final Map<PropertyDescriptor, PropertyConfiguration> properties, final String annotationData, final ParameterContext parameterContext) {
        return node.performValidation(properties, annotationData, parameterContext);
    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        return node.getPropertyDescriptors();
    }

    @Override
    public PropertyDescriptor getPropertyDescriptor(final String name) {
        return node.getPropertyDescriptor(name);
    }

    @Override
    public boolean isSensitiveDynamicProperty(final String name) {
        return node.isSensitiveDynamicProperty(name);
    }

    @Override
    public Optional<ProcessGroup> getParentProcessGroup() {
        return node.getParentProcessGroup();
    }

    @Override
    public ParameterLookup getParameterLookup() {
        return node.getParameterLookup();
    }

    @Override
    public String getDescription() {
        return node.getDescription();
    }

    @Override
    public void setDescription(final String description) {
        node.setDescription(description);
    }

    @Override
    public boolean isStorageLocationApplicable(final String location) {
        return node.isStorageLocationApplicable(location);
    }

    @Override
    public boolean isBranchingSupported() {
        return node.isBranchingSupported();
    }

    @Override
    public Set<FlowRegistryBranch> getBranches(final FlowRegistryClientUserContext context) throws FlowRegistryException, IOException {
        return node.getBranches(context);
    }

    @Override
    public FlowRegistryBranch getDefaultBranch(final FlowRegistryClientUserContext context) throws FlowRegistryException, IOException {
        return node.getDefaultBranch(context);
    }

    @Override
    public Set<FlowRegistryBucket> getBuckets(final FlowRegistryClientUserContext context, final String branch) throws FlowRegistryException, IOException {
        return node.getBuckets(context, branch);
    }

    @Override
    public FlowRegistryBucket getBucket(final FlowRegistryClientUserContext context, final BucketLocation bucketLocation) throws FlowRegistryException, IOException {
        return node.getBucket(context, bucketLocation);
    }

    @Override
    public RegisteredFlow registerFlow(final FlowRegistryClientUserContext context, final RegisteredFlow flow) throws FlowRegistryException, IOException {
        return node.registerFlow(context, flow);
    }

    @Override
    public RegisteredFlow deregisterFlow(final FlowRegistryClientUserContext context, final FlowLocation flowLocation) throws FlowRegistryException, IOException {
        return node.deregisterFlow(context, flowLocation);
    }

    @Override
    public RegisteredFlow getFlow(final FlowRegistryClientUserContext context, final FlowLocation flowLocation) throws FlowRegistryException, IOException {
        return node.getFlow(context, flowLocation);
    }

    @Override
    public Set<RegisteredFlow> getFlows(final FlowRegistryClientUserContext context, final BucketLocation bucketLocation) throws FlowRegistryException, IOException {
        return node.getFlows(context, bucketLocation);
    }

    @Override
    public FlowSnapshotContainer getFlowContents(final FlowRegistryClientUserContext context, final FlowVersionLocation flowVersionLocation, final boolean fetchRemoteFlows)
            throws FlowRegistryException, IOException {
        return node.getFlowContents(context, flowVersionLocation, fetchRemoteFlows);
    }

    @Override
    public Set<RegisteredFlowSnapshotMetadata> getFlowVersions(final FlowRegistryClientUserContext context, final FlowLocation flowLocation) throws FlowRegistryException, IOException {
        return node.getFlowVersions(context, flowLocation);
    }

    @Override
    public Optional<String> getLatestVersion(final FlowRegistryClientUserContext context, final FlowLocation flowLocation) throws FlowRegistryException, IOException {
        return node.getLatestVersion(context, flowLocation);
    }

    @Override
    public String generateFlowId(final String flowName) throws IOException, FlowRegistryException {
        return node.generateFlowId(flowName);
    }

    @Override
    public void setComponent(final LoggableComponent<FlowRegistryClient> component) {
        node.setComponent(component);
    }
}
