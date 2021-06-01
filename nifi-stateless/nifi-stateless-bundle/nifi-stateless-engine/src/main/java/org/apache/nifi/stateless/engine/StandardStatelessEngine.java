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

package org.apache.nifi.stateless.engine;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.state.StatelessStateManagerProvider;
import org.apache.nifi.components.validation.StandardValidationTrigger;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.reporting.LogComponentStatuses;
import org.apache.nifi.controller.repository.CounterRepository;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.extensions.ExtensionRepository;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionDefinition;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.InstanceClassLoader;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.processor.StandardValidationContext;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.stateless.config.ConfigurableExtensionDefinition;
import org.apache.nifi.stateless.config.ParameterContextDefinition;
import org.apache.nifi.stateless.config.ParameterDefinition;
import org.apache.nifi.stateless.config.ParameterProviderDefinition;
import org.apache.nifi.stateless.config.ReportingTaskDefinition;
import org.apache.nifi.stateless.flow.DataflowDefinition;
import org.apache.nifi.stateless.flow.StandardStatelessFlow;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.parameter.CompositeParameterProvider;
import org.apache.nifi.stateless.parameter.ParameterProvider;
import org.apache.nifi.stateless.parameter.ParameterProviderInitializationContext;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class StandardStatelessEngine implements StatelessEngine<VersionedFlowSnapshot> {
    private static final Logger logger = LoggerFactory.getLogger(StandardStatelessEngine.class);
    private static final int CONCURRENT_EXTENSION_DOWNLOADS = 4;

    // Member Variables injected via Builder
    private final ExtensionManager extensionManager;
    private final BulletinRepository bulletinRepository;
    private final StatelessStateManagerProvider stateManagerProvider;
    private final PropertyEncryptor propertyEncryptor;
    private final FlowRegistryClient flowRegistryClient;
    private final VariableRegistry rootVariableRegistry;
    private final ProcessScheduler processScheduler;
    private final KerberosConfig kerberosConfig;
    private final FlowFileEventRepository flowFileEventRepository;
    private final ProvenanceRepository provenanceRepository;
    private final ExtensionRepository extensionRepository;
    private final CounterRepository counterRepository;

    // Member Variables created/managed internally
    private final ReloadComponent reloadComponent;
    private final ValidationTrigger validationTrigger;

    // Member Variables injected via initialization. Effectively final.
    private FlowManager flowManager;
    private ControllerServiceProvider controllerServiceProvider;
    private ProcessContextFactory processContextFactory;
    private RepositoryContextFactory repositoryContextFactory;
    private boolean initialized = false;


    private StandardStatelessEngine(final Builder builder) {
        this.extensionManager = requireNonNull(builder.extensionManager, "Extension Manager must be provided");
        this.bulletinRepository = requireNonNull(builder.bulletinRepository, "Bulletin Repository must be provided");
        this.stateManagerProvider = requireNonNull(builder.stateManagerProvider, "State Manager Provider must be provided");
        this.propertyEncryptor = requireNonNull(builder.propertyEncryptor, "Encryptor must be provided");
        this.flowRegistryClient = requireNonNull(builder.flowRegistryClient, "Flow Registry Client must be provided");
        this.rootVariableRegistry = requireNonNull(builder.variableRegistry, "Variable Registry must be provided");
        this.processScheduler = requireNonNull(builder.processScheduler, "Process Scheduler must be provided");
        this.kerberosConfig = requireNonNull(builder.kerberosConfig, "Kerberos Configuration must be provided");
        this.flowFileEventRepository = requireNonNull(builder.flowFileEventRepository, "FlowFile Event Repository must be provided");
        this.provenanceRepository = requireNonNull(builder.provenanceRepository, "Provenance Repository must be provided");
        this.extensionRepository = requireNonNull(builder.extensionRepository, "Extension Repository must be provided");
        this.counterRepository = requireNonNull(builder.counterRepository, "Counter Repository must be provided");

        this.reloadComponent = new StatelessReloadComponent(this);
        this.validationTrigger = new StandardValidationTrigger(new FlowEngine(1, "Component Validation", true), () -> true);
    }

    @Override
    public void initialize(final StatelessEngineInitializationContext initContext) {
        this.flowManager = initContext.getFlowManager();
        this.controllerServiceProvider = initContext.getControllerServiceProvider();
        this.processContextFactory = initContext.getProcessContextFactory();
        this.repositoryContextFactory = initContext.getRepositoryContextFactory();
        this.initialized = true;
    }

    @Override
    public StatelessDataflow createFlow(final DataflowDefinition<VersionedFlowSnapshot> dataflowDefinition) {
        if (!this.initialized) {
            throw new IllegalStateException("Cannot create Flow without first initializing Stateless Engine");
        }

        final VersionedFlow versionedFlow = dataflowDefinition.getFlowSnapshot().getFlow();
        logger.info("Building Dataflow {}", versionedFlow == null ? "" : versionedFlow.getName());

        loadNecessaryExtensions(dataflowDefinition);

        extensionManager.logClassLoaderDetails();

        // Create a child group and add it to the root group. We do this, rather than interacting with the root group directly
        // because the flow may well have Local Input/Output ports, and those are not allowed on the Root Group.
        final ProcessGroup rootGroup = flowManager.getRootGroup();
        final ProcessGroup childGroup = flowManager.createProcessGroup("stateless-flow");
        childGroup.setName("Stateless Flow");
        rootGroup.addProcessGroup(childGroup);

        childGroup.updateFlow(dataflowDefinition.getFlowSnapshot(), "stateless-component-id-seed", false, true, true);

        final ParameterProvider parameterProvider = createParameterProvider(dataflowDefinition);

        // Map existing parameter contexts by name
        final Set<ParameterContext> parameterContexts = flowManager.getParameterContextManager().getParameterContexts();
        final Map<String, ParameterContext> parameterContextMap = parameterContexts.stream()
            .collect(Collectors.toMap(ParameterContext::getName, context -> context));

        // Update Parameters to match those that are provided in the flow configuration, plus those overrides provided
        final List<ParameterContextDefinition> parameterContextDefinitions = dataflowDefinition.getParameterContexts();
        if (parameterContextDefinitions != null) {
            parameterContextDefinitions.forEach(contextDefinition -> registerParameterContext(contextDefinition, parameterContextMap));
        }

        overrideParameters(parameterContextMap, parameterProvider);

        final List<ReportingTaskNode> reportingTaskNodes = createReportingTasks(dataflowDefinition);
        final StandardStatelessFlow dataflow = new StandardStatelessFlow(childGroup, reportingTaskNodes, controllerServiceProvider, processContextFactory,
            repositoryContextFactory, dataflowDefinition, stateManagerProvider, processScheduler);

        final LogComponentStatuses logComponentStatuses = new LogComponentStatuses(flowFileEventRepository, counterRepository, flowManager);
        dataflow.scheduleBackgroundTask(logComponentStatuses, 1, TimeUnit.MINUTES);

        return dataflow;
    }

    private ParameterProvider createParameterProvider(final DataflowDefinition<?> dataflowDefinition) {
        // Create a Provider for each definition
        final List<ParameterProvider> providers = new ArrayList<>();
        for (final ParameterProviderDefinition definition : dataflowDefinition.getParameterProviderDefinitions()) {
            providers.add(createParameterProvider(definition));
        }

        // Create a Composite Parameter Provider that wraps all of the others.
        final CompositeParameterProvider provider = new CompositeParameterProvider(providers);
        final ParameterProviderInitializationContext initializationContext = new StandardParameterProviderInitializationContext(provider, Collections.emptyMap(), UUID.randomUUID().toString());
        provider.initialize(initializationContext);
        return provider;
    }

    private ParameterProvider createParameterProvider(final ParameterProviderDefinition definition) {
        final BundleCoordinate bundleCoordinate = determineBundleCoordinate(definition, "Parameter Provider");
        final Bundle bundle = extensionManager.getBundle(bundleCoordinate);
        if (bundle == null) {
            throw new IllegalStateException("Unable to find bundle for coordinate " + bundleCoordinate.getCoordinate());
        }

        final String providerType = definition.getType();

        final String providerId = UUID.randomUUID().toString();
        final InstanceClassLoader classLoader = extensionManager.createInstanceClassLoader(providerType, providerId, bundle, Collections.emptySet());

        try {
            final Class<?> rawClass = Class.forName(providerType, true, classLoader);
            Thread.currentThread().setContextClassLoader(classLoader);

            final ParameterProvider parameterProvider = (ParameterProvider) rawClass.newInstance();

            // Initialize the provider
            final Map<String, String> properties = resolveProperties(definition.getPropertyValues(), parameterProvider, parameterProvider.getPropertyDescriptors());
            final ParameterProviderInitializationContext initializationContext = new StandardParameterProviderInitializationContext(parameterProvider, properties, providerId);
            parameterProvider.initialize(initializationContext);

            // Ensure that the Parameter Provider is valid.
            final List<ValidationResult> validationResults = validate(parameterProvider, properties, providerId);
            if (!validationResults.isEmpty()) {
                throw new IllegalStateException("Parameter Provider with name <" + definition.getName() + "> is not valid: " + validationResults);
            }

            return parameterProvider;
        } catch (final Exception e) {
            throw new IllegalStateException("Could not create Parameter Provider " + definition.getName() + " of type " + definition.getType(), e);
        }
    }

    private List<ValidationResult> validate(final ConfigurableComponent component, final Map<String, String> properties, final String componentId) {
        final Map<PropertyDescriptor, PropertyConfiguration> explicitlyConfiguredPropertyMap = new HashMap<>();

        for (final Map.Entry<String, String> property : properties.entrySet()) {
            final String propertyName = property.getKey();
            final String propertyValue = property.getValue();

            final PropertyDescriptor descriptor = component.getPropertyDescriptor(propertyName);
            final PropertyConfiguration propertyConfiguration = new PropertyConfiguration(propertyValue, null, Collections.emptyList());

            explicitlyConfiguredPropertyMap.put(descriptor, propertyConfiguration);
        }

        final Map<PropertyDescriptor, PropertyConfiguration> fullPropertyMap = buildConfiguredAndDefaultPropertyMap(component, explicitlyConfiguredPropertyMap);

        final ValidationContext validationContext = new StandardValidationContext(controllerServiceProvider, fullPropertyMap,
            null, null, componentId, VariableRegistry.EMPTY_REGISTRY, null);

        final Collection<ValidationResult> validationResults = component.validate(validationContext);
        return validationResults.stream()
            .filter(validationResult -> !validationResult.isValid())
            .collect(Collectors.toList());
    }

    public Map<PropertyDescriptor, PropertyConfiguration> buildConfiguredAndDefaultPropertyMap(final ConfigurableComponent component, final Map<PropertyDescriptor, PropertyConfiguration> properties) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, component.getClass(), component.getIdentifier())) {
            final List<PropertyDescriptor> supported = component.getPropertyDescriptors();
            if (supported == null || supported.isEmpty()) {
                return Collections.unmodifiableMap(properties);
            } else {
                final Map<PropertyDescriptor, PropertyConfiguration> props = new LinkedHashMap<>();

                for (final PropertyDescriptor descriptor : supported) {
                    props.put(descriptor, null);
                }

                props.putAll(properties);
                return props;
            }
        }
    }


    private void loadNecessaryExtensions(final DataflowDefinition<VersionedFlowSnapshot> dataflowDefinition) {
        final VersionedProcessGroup group = dataflowDefinition.getFlowSnapshot().getFlowContents();
        final Set<BundleCoordinate> requiredBundles = gatherRequiredBundles(group);

        for (final ReportingTaskDefinition reportingTaskDefinition : dataflowDefinition.getReportingTaskDefinitions()) {
            final BundleCoordinate coordinate = parseBundleCoordinate(reportingTaskDefinition);
            if (coordinate == null) {
                continue;
            }

            requiredBundles.add(coordinate);
        }

        for (final ParameterProviderDefinition parameterProviderDefinition : dataflowDefinition.getParameterProviderDefinitions()) {
            final BundleCoordinate coordinate = parseBundleCoordinate(parameterProviderDefinition);
            if (coordinate == null) {
                continue;
            }

            requiredBundles.add(coordinate);
        }

        final ExecutorService executor = new FlowEngine(CONCURRENT_EXTENSION_DOWNLOADS, "Download Extensions", true);
        final Future<Set<Bundle>> future = extensionRepository.fetch(requiredBundles, executor, CONCURRENT_EXTENSION_DOWNLOADS);
        executor.shutdown();

        logger.info("Waiting for bundles to complete download...");
        final long downloadStart = System.currentTimeMillis();
        final Set<Bundle> downloadedBundles;
        try {
            downloadedBundles = future.get();
        } catch (Exception e) {
            logger.error("Failed to obtain all necessary extension bundles", e);
            throw new RuntimeException(e);
        }

        final long downloadMillis = System.currentTimeMillis() - downloadStart;
        logger.info("Successfully downloaded {} bundles in {} millis", downloadedBundles.size(), downloadMillis);
    }

    private Set<BundleCoordinate> gatherRequiredBundles(final VersionedProcessGroup group) {
        final Set<BundleCoordinate> requiredBundles = new HashSet<>();
        gatherRequiredBundles(group, requiredBundles);
        return requiredBundles;
    }

    private void gatherRequiredBundles(final VersionedProcessGroup group, final Set<BundleCoordinate> requiredBundles) {
        group.getControllerServices().forEach(cs -> requiredBundles.add(toBundleCoordinate(cs.getBundle())));
        group.getProcessors().forEach(processor -> requiredBundles.add(toBundleCoordinate(processor.getBundle())));

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            gatherRequiredBundles(childGroup, requiredBundles);
        }
    }

    private BundleCoordinate toBundleCoordinate(final org.apache.nifi.registry.flow.Bundle bundle) {
        return new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
    }

    private List<ReportingTaskNode> createReportingTasks(final DataflowDefinition<?> dataflowDefinition) {
        final List<ReportingTaskNode> reportingTaskNodes = new ArrayList<>();
        for (final ReportingTaskDefinition taskDefinition : dataflowDefinition.getReportingTaskDefinitions()) {
            final ReportingTaskNode taskNode = createReportingTask(taskDefinition);
            reportingTaskNodes.add(taskNode);
        }

        return reportingTaskNodes;
    }

    private ReportingTaskNode createReportingTask(final ReportingTaskDefinition taskDefinition) {
        final BundleCoordinate bundleCoordinate = determineBundleCoordinate(taskDefinition, "Reporting Task");
        final ReportingTaskNode taskNode = flowManager.createReportingTask(taskDefinition.getType(), UUID.randomUUID().toString(), bundleCoordinate, Collections.emptySet(), true, true);

        final Map<String, String> properties = resolveProperties(taskDefinition.getPropertyValues(), taskNode.getComponent(), taskNode.getProperties().keySet());
        taskNode.setProperties(properties);
        taskNode.setSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
        taskNode.setSchedulingPeriod(taskDefinition.getSchedulingFrequency());

        // Ensure that the Parameter Provider is valid.
        final List<ValidationResult> validationResults = validate(taskNode.getComponent(), properties, taskNode.getIdentifier());
        if (!validationResults.isEmpty()) {
            throw new IllegalStateException("Reporting Task with name <" + taskNode.getName() + "> is not valid: " + validationResults);
        }

        return taskNode;
    }

    private Map<String, String> resolveProperties(final Map<String, String> configured, final ConfigurableComponent component, final Collection<PropertyDescriptor> componentDescriptors) {
        // Map property display name to actual names.
        final Map<String, String> displayNameToActualName = new HashMap<>();
        for (final PropertyDescriptor descriptor : componentDescriptors) {
            displayNameToActualName.put(descriptor.getDisplayName(), descriptor.getName());
        }

        // Map friendly property name in the task definition to the actual property name that we need in the 'resolved' map
        final Map<String, String> resolved = new HashMap<>();
        for (final Map.Entry<String, String> entry : configured.entrySet()) {
            final String configuredName = entry.getKey();
            final String configuredValue = entry.getValue();

            final String actual = displayNameToActualName.get(configuredName);
            final String resolvedName = actual == null ? configuredName : actual;

            // If property has allowable values, resolve the user-friendly name to the name that the Reporting Task needs.
            String resolvedValue = configuredValue;
            if (actual != null) {
                // This is a 'known' / non-dynamic property
                final PropertyDescriptor descriptor = component.getPropertyDescriptor(actual);
                final List<AllowableValue> allowableValues = descriptor.getAllowableValues();
                if (allowableValues != null && !allowableValues.isEmpty()) {
                    for (final AllowableValue allowableValue : allowableValues) {
                        if (allowableValue.getDisplayName().equalsIgnoreCase(configuredValue)) {
                            resolvedValue = allowableValue.getValue();
                            logger.debug("Resolving property value of {} for {} of {} to {}", configuredValue, configuredName, component, resolvedValue);
                            break;
                        }
                    }
                }
            }

            resolved.put(resolvedName, resolvedValue);
        }

        // Return map containing the desired names.
        return resolved;
    }


    private BundleCoordinate determineBundleCoordinate(final ConfigurableExtensionDefinition extensionDefinition, final String extensionType) {
        final String explicitCoordinates = extensionDefinition.getBundleCoordinates();
        if (explicitCoordinates != null && !explicitCoordinates.trim().isEmpty()) {
            final String resolvedClassName = resolveExtensionClassName(extensionDefinition, extensionType);
            extensionDefinition.setType(resolvedClassName);

            final BundleCoordinate coordinate = parseBundleCoordinate(extensionDefinition);
            return coordinate;
        }

        final String specifiedType = extensionDefinition.getType();
        String resolvedClassName = specifiedType;
        if (!specifiedType.contains(".")) {
            final List<Bundle> possibleBundles = extensionManager.getBundles(extensionDefinition.getType());
            if (possibleBundles.isEmpty()) {
                logger.debug("Could not find extension type of <{}>. Will try to find matching Reporting Task type based on class name", specifiedType);

                resolvedClassName = resolveExtensionClassName(extensionDefinition, extensionType);
                extensionDefinition.setType(resolvedClassName);
                logger.info("Resolved extension class {} to {}", specifiedType, resolvedClassName);
            }
        }

        final List<Bundle> possibleBundles = extensionManager.getBundles(resolvedClassName);
        if (possibleBundles.isEmpty()) {
            throw new IllegalArgumentException("Extension '" + extensionDefinition.getName() + "' (" + extensionDefinition.getType() +
                ") does not specify a Bundle and no Bundles could be found for type " + extensionDefinition.getType());
        }

        if (possibleBundles.size() > 1) {
            throw new IllegalArgumentException("Extension '" + extensionDefinition.getName() + "' (" + extensionDefinition.getType() +
                ") does not specify a Bundle and multiple Bundles exist for this type. The extension must specify a bundle to use.");
        }

        final Bundle bundle = possibleBundles.get(0);
        final BundleCoordinate coordinate = bundle.getBundleDetails().getCoordinate();
        return coordinate;
    }

    private BundleCoordinate parseBundleCoordinate(final ConfigurableExtensionDefinition extensionDefinition) {
        final String specifiedCoordinates = extensionDefinition.getBundleCoordinates();
        if (specifiedCoordinates == null) {
            return null;
        }

        final String[] splits = specifiedCoordinates.split(":", 3);
        if (splits.length != 3) {
            throw new IllegalArgumentException("Reporting Task '" + extensionDefinition.getName() + "' (" + extensionDefinition.getType() + ") specifies bundle as '" +
                specifiedCoordinates + "', but this " + "is not a valid Bundle format. Format should be <group>:<id>:<version>");
        }

        return new BundleCoordinate(splits[0], splits[1], splits[2]);
    }


    private String resolveExtensionClassName(final ConfigurableExtensionDefinition extensionDefinition, final String extensionType) {
        final String specifiedType = extensionDefinition.getType();
        if (specifiedType.contains(".")) {
            return specifiedType;
        }

        final Set<String> possibleResolvedClassNames = new HashSet<>();

        final Set<ExtensionDefinition> definitions = extensionManager.getExtensions(ReportingTask.class);
        for (final ExtensionDefinition definition : definitions) {
            final String implementationClassName = definition.getImplementationClassName();
            final String simpleName = implementationClassName.contains(".") ? StringUtils.substringAfterLast(implementationClassName, ".") : implementationClassName;
            if (simpleName.equals(specifiedType)) {
                logger.debug("Found possible matching class {}", implementationClassName);

                possibleResolvedClassNames.add(implementationClassName);
            }
        }

        if (possibleResolvedClassNames.isEmpty()) {
            throw new IllegalArgumentException(String.format("%s '%s' (%s) does not specify a Bundle, and no %s implementations exist with a class name of %s.",
                extensionType, extensionDefinition.getName(), extensionDefinition.getType(), extensionType, extensionDefinition.getType()));
        }

        if (possibleResolvedClassNames.size() > 1) {
            throw new IllegalArgumentException(String.format("%s '%s' (%s) does not specify a Bundle, and no %s implementations exist with a class name of %s. Perhaps you meant one of: %s",
                extensionType, extensionDefinition.getName(), extensionDefinition.getType(), extensionType, extensionDefinition.getType(), possibleResolvedClassNames));
        }

        return possibleResolvedClassNames.iterator().next();
    }

    private void overrideParameters(final Map<String, ParameterContext> parameterContextMap, final ParameterProvider parameterProvider) {
        for (final ParameterContext context : parameterContextMap.values()) {
            final String contextName = context.getName();
            final Map<ParameterDescriptor, Parameter> parameters = context.getParameters();

            final Map<String, Parameter> updatedParameters = new HashMap<>();
            for (final Parameter parameter : parameters.values()) {
                final String parameterName = parameter.getDescriptor().getName();
                if (parameterProvider.isParameterDefined(contextName, parameterName)) {
                    final String providedValue = parameterProvider.getParameterValue(contextName, parameterName);
                    final Parameter updatedParameter = new Parameter(parameter.getDescriptor(), providedValue);
                    updatedParameters.put(parameterName, updatedParameter);
                }
            }

            context.setParameters(updatedParameters);
        }
    }


    private void registerParameterContext(final ParameterContextDefinition parameterContextDefinition, final Map<String, ParameterContext> parameterContextMap) {
        final String contextName = parameterContextDefinition.getName();
        final ParameterContext existingContext = parameterContextMap.get(contextName);
        if (existingContext == null) {
            logger.warn("Configuration contains a Parameter Context with name <" + contextName + "> but the flow does not contain any Parameter Context with this name. " +
                "These Parameters will be ignored.");
            return;
        }

        final Map<String, Parameter> parameters = new HashMap<>();
        final List<ParameterDefinition> parameterDefinitions = parameterContextDefinition.getParameters();
        if (parameterDefinitions != null) {
            for (final ParameterDefinition parameterDefinition : parameterDefinitions) {
                final String parameterName = parameterDefinition.getName();

                final Optional<Parameter> optionalParameter = existingContext.getParameter(parameterName);
                if (!optionalParameter.isPresent()) {
                    logger.warn("Configuration contains a Parameter with name <{}> for Parameter Context <{}> but the Parameter Context does not have a Parameter with that name. This Parameter will" +
                        " be ignored.", parameterName, contextName);
                    continue;
                }

                final Parameter existingParameter = optionalParameter.get();
                final Parameter updatedParameter = new Parameter(existingParameter.getDescriptor(), parameterDefinition.getValue());
                parameters.put(parameterName, updatedParameter);
            }
        }

        existingContext.setParameters(parameters);

        logger.info("Registered Parameter Context {}", parameterContextDefinition.getName());
    }

    @Override
    public ExtensionManager getExtensionManager() {
        return extensionManager;
    }

    @Override
    public BulletinRepository getBulletinRepository() {
        return bulletinRepository;
    }

    @Override
    public StateManagerProvider getStateManagerProvider() {
        return stateManagerProvider;
    }

    @Override
    public PropertyEncryptor getPropertyEncryptor() {
        return propertyEncryptor;
    }

    @Override
    public FlowRegistryClient getFlowRegistryClient() {
        return flowRegistryClient;
    }

    @Override
    public VariableRegistry getRootVariableRegistry() {
        return rootVariableRegistry;
    }

    @Override
    public ProcessScheduler getProcessScheduler() {
        return processScheduler;
    }

    @Override
    public ReloadComponent getReloadComponent() {
        return reloadComponent;
    }

    @Override
    public ControllerServiceProvider getControllerServiceProvider() {
        return controllerServiceProvider;
    }

    @Override
    public ProvenanceRepository getProvenanceRepository() {
        return provenanceRepository;
    }

    @Override
    public FlowFileEventRepository getFlowFileEventRepository() {
        return flowFileEventRepository;
    }

    @Override
    public KerberosConfig getKerberosConfig() {
        return kerberosConfig;
    }

    @Override
    public ValidationTrigger getValidationTrigger() {
        return validationTrigger;
    }

    @Override
    public FlowManager getFlowManager() {
        return flowManager;
    }

    @Override
    public CounterRepository getCounterRepository() {
        return counterRepository;
    }

    public static class Builder {
        private ExtensionManager extensionManager = null;
        private BulletinRepository bulletinRepository = null;
        private StatelessStateManagerProvider stateManagerProvider = null;
        private PropertyEncryptor propertyEncryptor = null;
        private FlowRegistryClient flowRegistryClient = null;
        private VariableRegistry variableRegistry = null;
        private ProcessScheduler processScheduler = null;
        private KerberosConfig kerberosConfig = null;
        private FlowFileEventRepository flowFileEventRepository = null;
        private ProvenanceRepository provenanceRepository = null;
        private ExtensionRepository extensionRepository = null;
        private CounterRepository counterRepository = null;

        public Builder extensionManager(final ExtensionManager extensionManager) {
            this.extensionManager = extensionManager;
            return this;
        }

        public Builder bulletinRepository(final BulletinRepository bulletinRepository) {
            this.bulletinRepository = bulletinRepository;
            return this;
        }

        public Builder stateManagerProvider(final StatelessStateManagerProvider stateManagerProvider) {
            this.stateManagerProvider = stateManagerProvider;
            return this;
        }

        public Builder encryptor(final PropertyEncryptor propertyEncryptor) {
            this.propertyEncryptor = propertyEncryptor;
            return this;
        }

        public Builder flowRegistryClient(final FlowRegistryClient flowRegistryClient) {
            this.flowRegistryClient = flowRegistryClient;
            return this;
        }

        public Builder variableRegistry(final VariableRegistry variableRegistry) {
            this.variableRegistry = variableRegistry;
            return this;
        }

        public Builder processScheduler(final ProcessScheduler processScheduler) {
            this.processScheduler = processScheduler;
            return this;
        }

        public Builder kerberosConfiguration(final KerberosConfig kerberosConfig) {
            this.kerberosConfig = kerberosConfig;
            return this;
        }

        public Builder flowFileEventRepository(final FlowFileEventRepository flowFileEventRepository) {
            this.flowFileEventRepository = flowFileEventRepository;
            return this;
        }

        public Builder provenanceRepository(final ProvenanceRepository provenanceRepository) {
            this.provenanceRepository = provenanceRepository;
            return this;
        }

        public Builder extensionRepository(final ExtensionRepository extensionRepository) {
            this.extensionRepository = extensionRepository;
            return this;
        }

        public Builder counterRepository(final CounterRepository counterRepository) {
            this.counterRepository = counterRepository;
            return this;
        }

        public StandardStatelessEngine build() {
            return new StandardStatelessEngine(this);
        }
    }
}
