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
package org.apache.nifi.runtime.manifest.impl;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.c2.protocol.component.api.BuildInfo;
import org.apache.nifi.c2.protocol.component.api.Bundle;
import org.apache.nifi.c2.protocol.component.api.ConfigurableComponentDefinition;
import org.apache.nifi.c2.protocol.component.api.ControllerServiceDefinition;
import org.apache.nifi.c2.protocol.component.api.DefinedType;
import org.apache.nifi.c2.protocol.component.api.ExtensionComponent;
import org.apache.nifi.c2.protocol.component.api.ProcessorDefinition;
import org.apache.nifi.c2.protocol.component.api.PropertyAllowableValue;
import org.apache.nifi.c2.protocol.component.api.PropertyDependency;
import org.apache.nifi.c2.protocol.component.api.PropertyDescriptor;
import org.apache.nifi.c2.protocol.component.api.PropertyResourceDefinition;
import org.apache.nifi.c2.protocol.component.api.Relationship;
import org.apache.nifi.c2.protocol.component.api.ReportingTaskDefinition;
import org.apache.nifi.c2.protocol.component.api.Restriction;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.c2.protocol.component.api.SchedulingDefaults;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.extension.manifest.AllowableValue;
import org.apache.nifi.extension.manifest.Attribute;
import org.apache.nifi.extension.manifest.DefaultSchedule;
import org.apache.nifi.extension.manifest.DefaultSettings;
import org.apache.nifi.extension.manifest.Dependency;
import org.apache.nifi.extension.manifest.DependentValues;
import org.apache.nifi.extension.manifest.DeprecationNotice;
import org.apache.nifi.extension.manifest.DynamicProperty;
import org.apache.nifi.extension.manifest.DynamicRelationship;
import org.apache.nifi.extension.manifest.Extension;
import org.apache.nifi.extension.manifest.ExtensionManifest;
import org.apache.nifi.extension.manifest.Property;
import org.apache.nifi.extension.manifest.ProvidedServiceAPI;
import org.apache.nifi.extension.manifest.ResourceDefinition;
import org.apache.nifi.extension.manifest.Restricted;
import org.apache.nifi.extension.manifest.Stateful;
import org.apache.nifi.extension.manifest.SystemResourceConsideration;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.runtime.manifest.ComponentManifestBuilder;
import org.apache.nifi.runtime.manifest.ExtensionManifestContainer;
import org.apache.nifi.runtime.manifest.RuntimeManifestBuilder;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Standard builder for RuntimeManifest.
 */
public class StandardRuntimeManifestBuilder implements RuntimeManifestBuilder {

    private static final String DEFAULT_YIELD_PERIOD = "1 sec";
    private static final String DEFAULT_PENALIZATION_PERIOD = "30 sec";
    private static final String DEFAULT_BULLETIN_LEVEL = LogLevel.WARN.name();

    private String identifier;
    private String version;
    private String runtimeType;
    private BuildInfo buildInfo;
    private List<Bundle> bundles = new ArrayList<>();
    private SchedulingDefaults schedulingDefaults;

    @Override
    public RuntimeManifestBuilder identifier(final String identifier) {
        this.identifier = identifier;
        return this;
    }

    @Override
    public RuntimeManifestBuilder version(final String version) {
        this.version = version;
        return this;
    }

    @Override
    public RuntimeManifestBuilder runtimeType(final String runtimeType) {
        this.runtimeType = runtimeType;
        return this;
    }

    @Override
    public RuntimeManifestBuilder buildInfo(final BuildInfo buildInfo) {
        this.buildInfo = buildInfo;
        return this;
    }

    @Override
    public RuntimeManifestBuilder addBundle(final ExtensionManifestContainer extensionManifestContainer) {
        if (extensionManifestContainer == null) {
            throw new IllegalArgumentException("Extension manifest container is required");
        }

        final ExtensionManifest extensionManifest = extensionManifestContainer.getManifest();
        if (extensionManifest == null) {
            throw new IllegalArgumentException("Extension manifest is required");
        }
        if (extensionManifest.getGroupId() == null || extensionManifest.getGroupId().trim().isEmpty()) {
            throw new IllegalArgumentException("Extension manifest groupId is required");
        }
        if (extensionManifest.getArtifactId() == null || extensionManifest.getArtifactId().trim().isEmpty()) {
            throw new IllegalArgumentException("Extension manifest artifactId is required");
        }
        if (extensionManifest.getVersion() == null || extensionManifest.getVersion().trim().isEmpty()) {
            throw new IllegalArgumentException("Extension manifest version is required");
        }

        final Bundle bundle = new Bundle();
        bundle.setGroup(extensionManifest.getGroupId());
        bundle.setArtifact(extensionManifest.getArtifactId());
        bundle.setVersion(extensionManifest.getVersion());

        if (extensionManifest.getExtensions() != null) {
            final Map<String, String> additionalDetailsMap = extensionManifestContainer.getAdditionalDetails();
            final ComponentManifestBuilder componentManifestBuilder = new StandardComponentManifestBuilder();
            extensionManifest.getExtensions().forEach(extension -> {
                final String additionalDetails = additionalDetailsMap.get(extension.getName());
                addExtension(extensionManifest, extension, additionalDetails, componentManifestBuilder);
            });
            bundle.setComponentManifest(componentManifestBuilder.build());
        }
        bundles.add(bundle);

        return this;
    }

    @Override
    public RuntimeManifestBuilder addBundles(final Iterable<ExtensionManifestContainer> extensionManifests) {
        extensionManifests.forEach(em -> addBundle(em));
        return this;
    }

    @Override
    public RuntimeManifestBuilder addBundle(Bundle bundle) {
        if (bundle == null) {
            throw new IllegalArgumentException("Bundle is required");
        }
        bundles.add(bundle);
        return this;
    }

    @Override
    public RuntimeManifestBuilder schedulingDefaults(final SchedulingDefaults schedulingDefaults) {
        this.schedulingDefaults = schedulingDefaults;
        return this;
    }

    @Override
    public RuntimeManifest build() {
        final RuntimeManifest runtimeManifest = new RuntimeManifest();
        runtimeManifest.setIdentifier(identifier);
        runtimeManifest.setVersion(version);
        runtimeManifest.setAgentType(runtimeType);
        runtimeManifest.setBuildInfo(buildInfo);
        runtimeManifest.setBundles(new ArrayList<>(bundles));
        runtimeManifest.setSchedulingDefaults(schedulingDefaults);
        return runtimeManifest;
    }

    private void addExtension(final ExtensionManifest extensionManifest, final Extension extension, final String additionalDetails,
                              final ComponentManifestBuilder componentManifestBuilder) {
        if (extension == null) {
            throw new IllegalArgumentException("Extension cannot be null");
        }

        switch(extension.getType()) {
            case PROCESSOR:
                addProcessorDefinition(extensionManifest, extension, additionalDetails, componentManifestBuilder);
                break;
            case CONTROLLER_SERVICE:
                addControllerServiceDefinition(extensionManifest, extension, additionalDetails, componentManifestBuilder);
                break;
            case REPORTING_TASK:
                addReportingTaskDefinition(extensionManifest, extension, additionalDetails, componentManifestBuilder);
                break;
            default:
                throw new IllegalArgumentException("Unknown extension type: " + extension.getType());
        }
    }

    private void addProcessorDefinition(final ExtensionManifest extensionManifest, final Extension extension, final String additionalDetails,
                                        final ComponentManifestBuilder componentManifestBuilder) {
        final ProcessorDefinition processorDefinition = new ProcessorDefinition();
        populateDefinedType(extensionManifest, extension, processorDefinition);
        populateExtensionComponent(extensionManifest, extension, additionalDetails, processorDefinition);
        populateConfigurableComponent(extension, processorDefinition);

        // processor specific fields
        processorDefinition.setInputRequirement(getInputRequirement(extension.getInputRequirement()));
        processorDefinition.setSupportedRelationships(getSupportedRelationships(extension.getRelationships()));
        processorDefinition.setTriggerWhenEmpty(extension.getTriggerWhenEmpty());
        processorDefinition.setTriggerSerially(extension.getTriggerSerially());
        processorDefinition.setTriggerWhenAnyDestinationAvailable(extension.getTriggerWhenAnyDestinationAvailable());
        processorDefinition.setSupportsBatching(extension.getSupportsBatching());
        processorDefinition.setSupportsEventDriven(extension.getEventDriven());
        processorDefinition.setPrimaryNodeOnly(extension.getPrimaryNodeOnly());
        processorDefinition.setSideEffectFree(extension.getSideEffectFree());

        final DynamicRelationship dynamicRelationship = extension.getDynamicRelationship();
        if (dynamicRelationship != null) {
            processorDefinition.setSupportsDynamicRelationships(true);
            processorDefinition.setDynamicRelationship(getDynamicRelationship(dynamicRelationship));
        }

        final DefaultSettings defaultSettings = extension.getDefaultSettings();
        processorDefinition.setDefaultPenaltyDuration(defaultSettings == null ? DEFAULT_PENALIZATION_PERIOD : defaultSettings.getPenaltyDuration());
        processorDefinition.setDefaultYieldDuration(defaultSettings == null ? DEFAULT_YIELD_PERIOD : defaultSettings.getYieldDuration());
        processorDefinition.setDefaultBulletinLevel(defaultSettings == null ? DEFAULT_BULLETIN_LEVEL : defaultSettings.getBulletinLevel());

        final List<String> schedulingStrategies = new ArrayList<>();
        schedulingStrategies.add(SchedulingStrategy.TIMER_DRIVEN.name());
        schedulingStrategies.add(SchedulingStrategy.CRON_DRIVEN.name());
        if (extension.getEventDriven()) {
            schedulingStrategies.add(SchedulingStrategy.EVENT_DRIVEN.name());
        }

        // If a default schedule is provided then use that, otherwise default to TIMER_DRIVEN
        final DefaultSchedule defaultSchedule = extension.getDefaultSchedule();
        final String defaultSchedulingStrategy = defaultSchedule == null
                ? SchedulingStrategy.TIMER_DRIVEN.name() : extension.getDefaultSchedule().getStrategy();

        final Map<String, Integer> defaultConcurrentTasks = new LinkedHashMap<>(3);
        defaultConcurrentTasks.put(SchedulingStrategy.TIMER_DRIVEN.name(), SchedulingStrategy.TIMER_DRIVEN.getDefaultConcurrentTasks());
        defaultConcurrentTasks.put(SchedulingStrategy.CRON_DRIVEN.name(), SchedulingStrategy.CRON_DRIVEN.getDefaultConcurrentTasks());
        if (extension.getEventDriven()) {
            defaultConcurrentTasks.put(SchedulingStrategy.EVENT_DRIVEN.name(), SchedulingStrategy.EVENT_DRIVEN.getDefaultConcurrentTasks());
        }

        final Map<String, String> defaultSchedulingPeriods = new LinkedHashMap<>(2);
        defaultSchedulingPeriods.put(SchedulingStrategy.TIMER_DRIVEN.name(), SchedulingStrategy.TIMER_DRIVEN.getDefaultSchedulingPeriod());
        defaultSchedulingPeriods.put(SchedulingStrategy.CRON_DRIVEN.name(), SchedulingStrategy.CRON_DRIVEN.getDefaultSchedulingPeriod());

        // If a default schedule is provided then replace the default values for the default strategy
        if (defaultSchedule != null) {
            defaultSchedulingPeriods.put(defaultSchedule.getStrategy(), defaultSchedule.getPeriod());
            defaultConcurrentTasks.put(defaultSchedule.getStrategy(), Integer.valueOf(defaultSchedule.getConcurrentTasks()));
        }

        processorDefinition.setSupportedSchedulingStrategies(schedulingStrategies);
        processorDefinition.setDefaultSchedulingStrategy(defaultSchedulingStrategy);
        processorDefinition.setDefaultConcurrentTasksBySchedulingStrategy(defaultConcurrentTasks);
        processorDefinition.setDefaultSchedulingPeriodBySchedulingStrategy(defaultSchedulingPeriods);

        final List<Attribute> readsAttributes = extension.getReadsAttributes();
        if (isNotEmpty(readsAttributes)) {
            processorDefinition.setReadsAttributes(
                    readsAttributes.stream()
                            .map(this::getAttribute)
                            .collect(Collectors.toList())
            );
        }

        final List<Attribute> writesAttributes = extension.getWritesAttributes();
        if (isNotEmpty(writesAttributes)) {
            processorDefinition.setWritesAttributes(
                    writesAttributes.stream()
                            .map(this::getAttribute)
                            .collect(Collectors.toList())
            );
        }

        componentManifestBuilder.addProcessor(processorDefinition);
    }

    private org.apache.nifi.c2.protocol.component.api.Attribute getAttribute(final Attribute attribute) {
        final org.apache.nifi.c2.protocol.component.api.Attribute c2Attribute = new org.apache.nifi.c2.protocol.component.api.Attribute();
        c2Attribute.setName(attribute.getName());
        c2Attribute.setDescription(attribute.getDescription());
        return c2Attribute;
    }

    private org.apache.nifi.c2.protocol.component.api.DynamicRelationship getDynamicRelationship(final DynamicRelationship dynamicRelationship) {
        final org.apache.nifi.c2.protocol.component.api.DynamicRelationship c2DynamicRelationship = new org.apache.nifi.c2.protocol.component.api.DynamicRelationship();
        c2DynamicRelationship.setName(dynamicRelationship.getName());
        c2DynamicRelationship.setDescription(dynamicRelationship.getDescription());
        return c2DynamicRelationship;
    }

    private InputRequirement.Requirement getInputRequirement(final org.apache.nifi.extension.manifest.InputRequirement inputRequirement) {
        if (inputRequirement == null) {
            return null;
        }

        switch (inputRequirement) {
            case INPUT_ALLOWED:
                return InputRequirement.Requirement.INPUT_ALLOWED;
            case INPUT_REQUIRED:
                return InputRequirement.Requirement.INPUT_REQUIRED;
            case INPUT_FORBIDDEN:
                return InputRequirement.Requirement.INPUT_FORBIDDEN;
            default:
                throw new IllegalArgumentException("Unknown input requirement: " + inputRequirement.name());
        }
    }

    private List<Relationship> getSupportedRelationships(final List<org.apache.nifi.extension.manifest.Relationship> relationships) {
        if (relationships == null || relationships.isEmpty()) {
            return null;
        }

        final List<Relationship> componentRelationships = new ArrayList<>();
        for (final org.apache.nifi.extension.manifest.Relationship relationship : relationships) {
            final Relationship componentRelationship = new Relationship();
            componentRelationship.setName(relationship.getName());
            componentRelationship.setDescription(relationship.getDescription());
            componentRelationships.add(componentRelationship);
        }
        return componentRelationships;
    }

    private void addControllerServiceDefinition(final ExtensionManifest extensionManifest, final Extension extension, final String additionalDetails,
                                                final ComponentManifestBuilder componentManifestBuilder) {
        final ControllerServiceDefinition controllerServiceDefinition = new ControllerServiceDefinition();
        populateDefinedType(extensionManifest, extension, controllerServiceDefinition);
        populateExtensionComponent(extensionManifest, extension, additionalDetails, controllerServiceDefinition);
        populateConfigurableComponent(extension, controllerServiceDefinition);
        componentManifestBuilder.addControllerService(controllerServiceDefinition);
    }

    private void addReportingTaskDefinition(final ExtensionManifest extensionManifest, final Extension extension, final String additionalDetails,
                                            final ComponentManifestBuilder componentManifestBuilder) {
        final ReportingTaskDefinition reportingTaskDefinition = new ReportingTaskDefinition();
        populateDefinedType(extensionManifest, extension, reportingTaskDefinition);
        populateExtensionComponent(extensionManifest, extension, additionalDetails, reportingTaskDefinition);
        populateConfigurableComponent(extension, reportingTaskDefinition);

        final List<String> schedulingStrategies = new ArrayList<>();
        schedulingStrategies.add(SchedulingStrategy.TIMER_DRIVEN.name());
        schedulingStrategies.add(SchedulingStrategy.CRON_DRIVEN.name());

        // If a default schedule is provided then use that, otherwise default to TIMER_DRIVEN
        final DefaultSchedule defaultSchedule = extension.getDefaultSchedule();
        final String defaultSchedulingStrategy = defaultSchedule == null
                ? SchedulingStrategy.TIMER_DRIVEN.name() : extension.getDefaultSchedule().getStrategy();

        final Map<String, String> defaultSchedulingPeriods = new LinkedHashMap<>(2);
        defaultSchedulingPeriods.put(SchedulingStrategy.TIMER_DRIVEN.name(), SchedulingStrategy.TIMER_DRIVEN.getDefaultSchedulingPeriod());
        defaultSchedulingPeriods.put(SchedulingStrategy.CRON_DRIVEN.name(), SchedulingStrategy.CRON_DRIVEN.getDefaultSchedulingPeriod());

        // If a default schedule is provided then replace the default values for the default strategy
        if (defaultSchedule != null) {
            defaultSchedulingPeriods.put(defaultSchedule.getStrategy(), defaultSchedule.getPeriod());
        }

        reportingTaskDefinition.setSupportedSchedulingStrategies(schedulingStrategies);
        reportingTaskDefinition.setDefaultSchedulingStrategy(defaultSchedulingStrategy);
        reportingTaskDefinition.setDefaultSchedulingPeriodBySchedulingStrategy(defaultSchedulingPeriods);

        componentManifestBuilder.addReportingTask(reportingTaskDefinition);
    }

    private void populateDefinedType(final ExtensionManifest extensionManifest, final Extension extension, final DefinedType definedType) {
        definedType.setType(extension.getName());
        definedType.setTypeDescription(extension.getDescription());
        definedType.setGroup(extensionManifest.getGroupId());
        definedType.setArtifact(extensionManifest.getArtifactId());
        definedType.setVersion(extensionManifest.getVersion());
    }

    private void populateExtensionComponent(final ExtensionManifest extensionManifest, final Extension extension, final String additionalDetails,
                                            final ExtensionComponent extensionComponent) {
        final org.apache.nifi.extension.manifest.BuildInfo buildInfo = extensionManifest.getBuildInfo();
        if (buildInfo != null) {
            final BuildInfo componentBuildInfo = new BuildInfo();
            componentBuildInfo.setRevision(buildInfo.getRevision());
            extensionComponent.setBuildInfo(componentBuildInfo);
        }

        final List<String> tags = extension.getTags();
        if (isNotEmpty(tags)) {
            extensionComponent.setTags(new TreeSet<>(tags));
        }

        final List<String> seeAlso = extension.getSeeAlso();
        if (isNotEmpty(seeAlso)) {
            extensionComponent.setSeeAlso(new TreeSet<>(seeAlso));
        }

        // the extension-manifest.xml will have <deprecationNotice/> for non-deprecated components which unmarshalls into
        // a non-null DeprecationNotice, so we need to check if the reason is also non-null before setting the boolean here
        final DeprecationNotice deprecationNotice = extension.getDeprecationNotice();
        if (deprecationNotice != null && deprecationNotice.getReason() != null) {
            extensionComponent.setDeprecated(true);
            extensionComponent.setDeprecationReason(deprecationNotice.getReason());
            final List<String> alternatives = deprecationNotice.getAlternatives();
            if (isNotEmpty(alternatives)) {
                extensionComponent.setDeprecationAlternatives(new TreeSet<>(alternatives));
            }
        }

        final List<ProvidedServiceAPI> providedServiceApis = extension.getProvidedServiceAPIs();
        if (isNotEmpty(providedServiceApis)) {
            final List<DefinedType> providedApiTypes = new ArrayList<>();
            providedServiceApis.forEach(providedServiceApi -> providedApiTypes.add(createProvidedApiType(providedServiceApi)));
            extensionComponent.setProvidedApiImplementations(providedApiTypes);
        }

        final Restricted restricted = extension.getRestricted();
        if (restricted != null) {
            extensionComponent.setRestricted(true);
            extensionComponent.setRestrictedExplanation(restricted.getGeneralRestrictionExplanation());
            if (restricted.getRestrictions() != null) {
                final Set<Restriction> explicitRestrictions = new HashSet<>();
                restricted.getRestrictions().forEach(r -> explicitRestrictions.add(createRestriction(r)));
                extensionComponent.setExplicitRestrictions(explicitRestrictions);
            }
        }

        final Stateful stateful = extension.getStateful();
        if (stateful != null) {
            final org.apache.nifi.c2.protocol.component.api.Stateful componentStateful = new org.apache.nifi.c2.protocol.component.api.Stateful();
            componentStateful.setDescription(stateful.getDescription());
            if (stateful.getScopes() != null) {
                componentStateful.setScopes(
                        stateful.getScopes().stream()
                                .map(this::getScope)
                                .collect(Collectors.toSet())
                );
                extensionComponent.setStateful(componentStateful);
            }
        }

        final List<SystemResourceConsideration> systemResourceConsiderations = extension.getSystemResourceConsiderations();
        if (isNotEmpty(systemResourceConsiderations)) {
            extensionComponent.setSystemResourceConsiderations(
                    systemResourceConsiderations.stream()
                            .map(this::getSystemResourceConsideration)
                            .collect(Collectors.toList())
            );
        }

        if (additionalDetails != null) {
            extensionComponent.setAdditionalDetails(true);
        }
    }

    private org.apache.nifi.c2.protocol.component.api.SystemResourceConsideration getSystemResourceConsideration(final SystemResourceConsideration systemResourceConsideration) {
        final org.apache.nifi.c2.protocol.component.api.SystemResourceConsideration c2consideration = new org.apache.nifi.c2.protocol.component.api.SystemResourceConsideration();
        c2consideration.setResource(systemResourceConsideration.getResource());
        c2consideration.setDescription(systemResourceConsideration.getDescription());
        return c2consideration;
    }

    private Scope getScope(final org.apache.nifi.extension.manifest.Scope sourceScope) {
        switch (sourceScope) {
            case LOCAL:
                return Scope.LOCAL;
            case CLUSTER:
                return Scope.CLUSTER;
            default:
                throw new IllegalArgumentException("Unknown scope: " + sourceScope);
        }
    }

    private Restriction createRestriction(final org.apache.nifi.extension.manifest.Restriction extensionRestriction) {
        final Restriction restriction = new Restriction();
        restriction.setExplanation(extensionRestriction.getExplanation());
        restriction.setRequiredPermission(extensionRestriction.getRequiredPermission());
        return restriction;
    }

    private DefinedType createProvidedApiType(final ProvidedServiceAPI providedServiceApi) {
        final DefinedType providedApiType = new DefinedType();
        providedApiType.setType(providedServiceApi.getClassName());
        providedApiType.setGroup(providedServiceApi.getGroupId());
        providedApiType.setArtifact(providedServiceApi.getArtifactId());
        providedApiType.setVersion(providedServiceApi.getVersion());
        return providedApiType;
    }

    private void populateConfigurableComponent(final Extension extension, final ConfigurableComponentDefinition configurableComponentDefinition) {
        final List<Property> properties = extension.getProperties();
        if (isNotEmpty(properties)) {
            final LinkedHashMap<String, PropertyDescriptor> propertyDescriptors = new LinkedHashMap<>();
            properties.forEach(property -> addPropertyDescriptor(propertyDescriptors, property));
            configurableComponentDefinition.setPropertyDescriptors(propertyDescriptors);
        }

        final List<DynamicProperty> dynamicProperties = extension.getDynamicProperties();
        if (isNotEmpty(dynamicProperties)) {
            configurableComponentDefinition.setSupportsDynamicProperties(true);
            configurableComponentDefinition.setSupportsSensitiveDynamicProperties(extension.getSupportsSensitiveDynamicProperties());
            configurableComponentDefinition.setDynamicProperties(
                    dynamicProperties.stream()
                            .map(this::getDynamicProperty)
                            .collect(Collectors.toList())
            );
        }
    }

    private org.apache.nifi.c2.protocol.component.api.DynamicProperty getDynamicProperty(final DynamicProperty dynamicProperty) {
        final org.apache.nifi.c2.protocol.component.api.DynamicProperty c2DynamicProperty = new org.apache.nifi.c2.protocol.component.api.DynamicProperty();
        c2DynamicProperty.setName(dynamicProperty.getName());
        c2DynamicProperty.setValue(dynamicProperty.getValue());
        c2DynamicProperty.setDescription(dynamicProperty.getDescription());
        c2DynamicProperty.setExpressionLanguageScope(getELScope(dynamicProperty.getExpressionLanguageScope()));
        return c2DynamicProperty;
    }

    private void addPropertyDescriptor(final Map<String, PropertyDescriptor> propertyDescriptors, final Property property) {
        final PropertyDescriptor propertyDescriptor = createPropertyDescriptor(property);
        propertyDescriptors.put(propertyDescriptor.getName(), propertyDescriptor);
    }

    private PropertyDescriptor createPropertyDescriptor(final Property property) {
        final PropertyDescriptor descriptor = new PropertyDescriptor();
        descriptor.setName(property.getName());
        descriptor.setDisplayName(property.getDisplayName());
        descriptor.setDescription(property.getDescription());
        descriptor.setDefaultValue(property.getDefaultValue());
        descriptor.setRequired(property.isRequired());
        descriptor.setSensitive(property.isSensitive());
        descriptor.setExpressionLanguageScope(getELScope(property.getExpressionLanguageScope()));
        descriptor.setDynamic(property.isDynamic());
        descriptor.setAllowableValues(getPropertyAllowableValues(property.getAllowableValues()));
        descriptor.setTypeProvidedByValue(getControllerServiceDefinedType(property.getControllerServiceDefinition()));
        descriptor.setResourceDefinition(getPropertyResourceDefinition(property.getResourceDefinition()));
        descriptor.setDependencies(getPropertyDependencies(property.getDependencies()));
        return descriptor;
    }

    private List<PropertyDependency> getPropertyDependencies(final List<Dependency> dependencies) {
        if (dependencies == null || dependencies.isEmpty()) {
            return null;
        }

        final List<PropertyDependency> propertyDependencies = new ArrayList<>(dependencies.size());
        for (final Dependency dependency : dependencies) {
            final PropertyDependency propertyDependency = new PropertyDependency();
            propertyDependency.setPropertyName(dependency.getPropertyName());
            propertyDependency.setPropertyDisplayName(dependency.getPropertyDisplayName());

            final DependentValues dependentValues = dependency.getDependentValues();
            if (dependentValues != null && dependentValues.getValues() != null) {
                final List<String> values = new ArrayList();
                values.addAll(dependentValues.getValues());
                propertyDependency.setDependentValues(values);
            }
            propertyDependencies.add(propertyDependency);
        }
        return propertyDependencies;
    }

    private PropertyResourceDefinition getPropertyResourceDefinition(final ResourceDefinition resourceDefinition) {
        if (resourceDefinition == null || resourceDefinition.getCardinality() == null) {
            return null;
        }

        final PropertyResourceDefinition propertyResourceDefinition = new PropertyResourceDefinition();
        switch (resourceDefinition.getCardinality()) {
            case SINGLE:
                propertyResourceDefinition.setCardinality(ResourceCardinality.SINGLE);
                break;
            case MULTIPLE:
                propertyResourceDefinition.setCardinality(ResourceCardinality.MULTIPLE);
                break;
        }

        propertyResourceDefinition.setResourceTypes(
                resourceDefinition.getResourceTypes().stream()
                        .map(rt -> getResourceType(rt))
                        .collect(Collectors.toSet())
        );

        return propertyResourceDefinition;
    }

    private ResourceType getResourceType(final org.apache.nifi.extension.manifest.ResourceType resourceType) {
        switch (resourceType) {
            case URL:
                return ResourceType.URL;
            case FILE:
                return ResourceType.FILE;
            case TEXT:
                return ResourceType.TEXT;
            case DIRECTORY:
                return ResourceType.DIRECTORY;
            default:
                throw new IllegalArgumentException("Unknown resource type: " + resourceType);
        }
    }

    private ExpressionLanguageScope getELScope(final org.apache.nifi.extension.manifest.ExpressionLanguageScope elScope) {
        if (elScope == null) {
            return null;
        }

        switch (elScope) {
            case NONE:
                return ExpressionLanguageScope.NONE;
            case FLOWFILE_ATTRIBUTES:
                return ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
            case VARIABLE_REGISTRY:
                return ExpressionLanguageScope.VARIABLE_REGISTRY;
            default:
                throw new IllegalArgumentException("Unknown Expression Language Scope: " + elScope.name());
        }
    }

    private List<PropertyAllowableValue> getPropertyAllowableValues(final List<AllowableValue> allowableValues) {
        if (allowableValues == null || allowableValues.isEmpty()) {
            return null;
        }

        final List<PropertyAllowableValue> propertyAllowableValues = new ArrayList<>();
        for (final AllowableValue allowableValue : allowableValues) {
            final PropertyAllowableValue propertyAllowableValue = new PropertyAllowableValue();
            propertyAllowableValue.setValue(allowableValue.getValue());
            propertyAllowableValue.setDisplayName(allowableValue.getDisplayName());
            propertyAllowableValue.setDescription(allowableValue.getDescription());
            propertyAllowableValues.add(propertyAllowableValue);
        }
        return propertyAllowableValues;
    }

    private DefinedType getControllerServiceDefinedType(
            final org.apache.nifi.extension.manifest.ControllerServiceDefinition controllerServiceDefinition) {
        if (controllerServiceDefinition == null) {
            return null;
        }

        final DefinedType serviceDefinitionType = new DefinedType();
        serviceDefinitionType.setType(controllerServiceDefinition.getClassName());
        serviceDefinitionType.setGroup(controllerServiceDefinition.getGroupId());
        serviceDefinitionType.setArtifact(controllerServiceDefinition.getArtifactId());
        serviceDefinitionType.setVersion(controllerServiceDefinition.getVersion());
        return serviceDefinitionType;
    }

    private <T> boolean isNotEmpty(final Collection<T> collection) {
        return collection != null && !collection.isEmpty();
    }

}
