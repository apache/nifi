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

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.StandardValidationTrigger;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.extensions.ExtensionRepository;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.stateless.config.ParameterContextDefinition;
import org.apache.nifi.stateless.config.ParameterDefinition;
import org.apache.nifi.stateless.config.ParameterOverride;
import org.apache.nifi.stateless.config.ReportingTaskDefinition;
import org.apache.nifi.stateless.flow.DataflowDefinition;
import org.apache.nifi.stateless.flow.StandardStatelessFlow;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class StandardStatelessEngine implements StatelessEngine<VersionedFlowSnapshot> {
    private static final Logger logger = LoggerFactory.getLogger(StandardStatelessEngine.class);
    private static final int CONCURRENT_EXTENSION_DOWNLOADS = 4;

    // Member Variables injected via Builder
    private final ExtensionManager extensionManager;
    private final BulletinRepository bulletinRepository;
    private final StateManagerProvider stateManagerProvider;
    private final StringEncryptor encryptor;
    private final FlowRegistryClient flowRegistryClient;
    private final VariableRegistry rootVariableRegistry;
    private final ProcessScheduler processScheduler;
    private final KerberosConfig kerberosConfig;
    private final FlowFileEventRepository flowFileEventRepository;
    private final ProvenanceRepository provenanceRepository;
    private final ExtensionRepository extensionRepository;

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
        this.encryptor = requireNonNull(builder.encryptor, "Encryptor must be provided");
        this.flowRegistryClient = requireNonNull(builder.flowRegistryClient, "Flow Registry Client must be provided");
        this.rootVariableRegistry = requireNonNull(builder.variableRegistry, "Variable Registry must be provided");
        this.processScheduler = requireNonNull(builder.processScheduler, "Process Scheduler must be provided");
        this.kerberosConfig = requireNonNull(builder.kerberosConfig, "Kerberos Configuration must be provided");
        this.flowFileEventRepository = requireNonNull(builder.flowFileEventRepository, "FlowFile Event Repository must be provided");
        this.provenanceRepository = requireNonNull(builder.provenanceRepository, "Provenance Repository must be provided");
        this.extensionRepository = requireNonNull(builder.extensionRepository, "Extension Repository must be provided");

        this.reloadComponent = new StatelessReloadComponent();
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
    public StatelessDataflow createFlow(final DataflowDefinition<VersionedFlowSnapshot> dataflowDefinition, final List<ParameterOverride> parameterOverrides) {
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

        // Map existing parameter contexts by name
        final Set<ParameterContext> parameterContexts = flowManager.getParameterContextManager().getParameterContexts();
        final Map<String, ParameterContext> parameterContextMap = parameterContexts.stream()
            .collect(Collectors.toMap(ParameterContext::getName, context -> context));

        // Update Parameters to match those that are provided in the flow configuration, plus those overrides provided
        final List<ParameterContextDefinition> parameterContextDefinitions = dataflowDefinition.getParameterContexts();
        if (parameterContextDefinitions != null) {
            parameterContextDefinitions.forEach(contextDefinition -> registerParameterContext(contextDefinition, parameterContextMap));
        }

        overrideParameters(parameterContextMap, parameterOverrides);

        final List<ReportingTaskNode> reportingTaskNodes = createReportingTasks(dataflowDefinition);
        final StandardStatelessFlow dataflow = new StandardStatelessFlow(childGroup, reportingTaskNodes, controllerServiceProvider, processContextFactory,
            repositoryContextFactory, dataflowDefinition);
        dataflow.initialize(processScheduler);
        return dataflow;
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
        final BundleCoordinate bundleCoordinate = determineBundleCoordinate(taskDefinition);
        final ReportingTaskNode taskNode = flowManager.createReportingTask(taskDefinition.getType(), UUID.randomUUID().toString(), bundleCoordinate, Collections.emptySet(), true, true);
        taskNode.setProperties(resolveProperties(taskDefinition.getPropertyValues(), taskNode));
        taskNode.setSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
        taskNode.setSchedulingPeriod(taskDefinition.getSchedulingFrequency());
        return taskNode;
    }

    private Map<String, String> resolveProperties(final Map<String, String> configured, final ReportingTaskNode taskNode) {
        // Map property display name to actual names.
        final Map<String, String> displayNameToActualName = new HashMap<>();
        for (final PropertyDescriptor descriptor : taskNode.getProperties().keySet()) {
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
                final PropertyDescriptor descriptor = taskNode.getPropertyDescriptor(actual);
                final List<AllowableValue> allowableValues = descriptor.getAllowableValues();
                if (allowableValues != null && !allowableValues.isEmpty()) {
                    for (final AllowableValue allowableValue : allowableValues) {
                        if (allowableValue.getDisplayName().equalsIgnoreCase(configuredValue)) {
                            resolvedValue = allowableValue.getValue();
                            logger.debug("Resolving property value of {} for {} of {} to {}", configuredValue, configuredName, taskNode, resolvedValue);
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

    private BundleCoordinate determineBundleCoordinate(final ReportingTaskDefinition taskDefinition) {
        final String explicitCoordinates = taskDefinition.getBundleCoordinates();
        if (explicitCoordinates != null && !explicitCoordinates.trim().isEmpty()) {
            final String resolvedClassName = resolveReportingTaskClassName(taskDefinition);
            taskDefinition.setType(resolvedClassName);

            final BundleCoordinate coordinate = parseBundleCoordinate(taskDefinition);
            return coordinate;
        }

        final String specifiedType = taskDefinition.getType();
        String resolvedClassName = specifiedType;
        if (!specifiedType.contains(".")) {
            final List<Bundle> possibleBundles = extensionManager.getBundles(taskDefinition.getType());
            if (possibleBundles.isEmpty()) {
                logger.debug("Could not find Reporting Task type of <{}>. Will try to find matching Reporting Task type based on class name", specifiedType);

                resolvedClassName = resolveReportingTaskClassName(taskDefinition);
                taskDefinition.setType(resolvedClassName);
                logger.info("Resolved Reporting Task class {} to {}", specifiedType, resolvedClassName);
            }
        }

        final List<Bundle> possibleBundles = extensionManager.getBundles(resolvedClassName);
        if (possibleBundles.isEmpty()) {
            throw new IllegalArgumentException("Reporting Task '" + taskDefinition.getName() + "' (" + taskDefinition.getType() +
                ") does not specify a Bundle and no Bundles could be found for type " + taskDefinition.getType());
        }

        if (possibleBundles.size() > 1) {
            throw new IllegalArgumentException("Reporting Task '" + taskDefinition.getName() + "' (" + taskDefinition.getType() +
                ") does not specify a Bundle and multiple Bundles exist for this type. The reporting task must specify a bundle to use.");
        }

        final Bundle bundle = possibleBundles.get(0);
        final BundleCoordinate coordinate = bundle.getBundleDetails().getCoordinate();
        return coordinate;
    }

    private BundleCoordinate parseBundleCoordinate(final ReportingTaskDefinition taskDefinition) {
        final String specifiedCoordinates = taskDefinition.getBundleCoordinates();
        if (specifiedCoordinates == null) {
            return null;
        }

        final String[] splits = specifiedCoordinates.split(":", 3);
        if (splits.length != 3) {
            throw new IllegalArgumentException("Reporting Task '" + taskDefinition.getName() + "' (" + taskDefinition.getType() + ") specifies bundle as '" + specifiedCoordinates + "', but this " +
                "is not a valid Bundle format. Format should be <group>:<id>:<version>");
        }

        return new BundleCoordinate(splits[0], splits[1], splits[2]);
    }

    private String resolveReportingTaskClassName(final ReportingTaskDefinition taskDefinition) {
        final String specifiedType = taskDefinition.getType();
        if (specifiedType.contains(".")) {
            return specifiedType;
        }

        final Set<String> possibleResolvedClassNames = new HashSet<>();

        final Set<Class> implementationClasses = extensionManager.getExtensions(ReportingTask.class);
        for (final Class<?> implementationClass : implementationClasses) {
            if (implementationClass.getSimpleName().equals(specifiedType)) {
                logger.debug("Found possible matching class {}", implementationClass);

                possibleResolvedClassNames.add(implementationClass.getName());
            }
        }

        if (possibleResolvedClassNames.isEmpty()) {
            throw new IllegalArgumentException("Reporting Task '" + taskDefinition.getName() + "' (" + taskDefinition.getType() + ") does not specify a Bundle, and no Reporting Task" +
                " implementations exist with a class name of " + taskDefinition.getType() + ".");
        }

        if (possibleResolvedClassNames.size() > 1) {
            throw new IllegalArgumentException("Reporting Task '" + taskDefinition.getName() + "' (" + taskDefinition.getType() + ") does not specify a Bundle, and no Reporting Task" +
                " implementations exist with a class name of " + taskDefinition.getType() + ". Perhaps you meant one of: " + possibleResolvedClassNames);
        }

        return possibleResolvedClassNames.iterator().next();
    }

    private void overrideParameters(final Map<String, ParameterContext> parameterContextMap, final List<ParameterOverride> overrides) {
        for (final ParameterOverride override : overrides) {
            final String contextName = override.getContextName();
            final ParameterContext context = parameterContextMap.get(contextName);
            if (context == null) {
                logger.warn("Received Parameter override {} but no Parameter Context exists in the dataflow with name <{}>. Will ignore this Parameter.", override, contextName);
                continue;
            }

            final String parameterName = override.getParameterName();
            final Optional<Parameter> optionalParameter = context.getParameter(parameterName);
            if (!optionalParameter.isPresent()) {
                logger.warn("Received Parameter override {} but no Parameter exists in the dataflow with that Parameter Name for that Context. Will ignore this Parameter.", override);
                continue;
            }

            final Parameter existingParameter = optionalParameter.get();
            final Parameter updatedParameter = new Parameter(existingParameter.getDescriptor(), override.getParameterValue());
            final Map<String, Parameter> updatedParameters = Collections.singletonMap(parameterName, updatedParameter);
            context.setParameters(updatedParameters);
            logger.debug("Updated Parameter with Override for {}", override);
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

    public ExtensionManager getExtensionManager() {
        return extensionManager;
    }

    public BulletinRepository getBulletinRepository() {
        return bulletinRepository;
    }

    public StateManagerProvider getStateManagerProvider() {
        return stateManagerProvider;
    }

    public StringEncryptor getEncryptor() {
        return encryptor;
    }

    public FlowRegistryClient getFlowRegistryClient() {
        return flowRegistryClient;
    }

    public VariableRegistry getRootVariableRegistry() {
        return rootVariableRegistry;
    }

    public ProcessScheduler getProcessScheduler() {
        return processScheduler;
    }

    public ReloadComponent getReloadComponent() {
        return reloadComponent;
    }

    public ControllerServiceProvider getControllerServiceProvider() {
        return controllerServiceProvider;
    }

    public ProvenanceRepository getProvenanceRepository() {
        return provenanceRepository;
    }

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


    public static class Builder {
        private ExtensionManager extensionManager = null;
        private BulletinRepository bulletinRepository = null;
        private StateManagerProvider stateManagerProvider = null;
        private StringEncryptor encryptor = null;
        private FlowRegistryClient flowRegistryClient = null;
        private VariableRegistry variableRegistry = null;
        private ProcessScheduler processScheduler = null;
        private KerberosConfig kerberosConfig = null;
        private FlowFileEventRepository flowFileEventRepository = null;
        private ProvenanceRepository provenanceRepository = null;
        private ExtensionRepository extensionRepository = null;

        public Builder extensionManager(final ExtensionManager extensionManager) {
            this.extensionManager = extensionManager;
            return this;
        }

        public Builder bulletinRepository(final BulletinRepository bulletinRepository) {
            this.bulletinRepository = bulletinRepository;
            return this;
        }

        public Builder stateManagerProvider(final StateManagerProvider stateManagerProvider) {
            this.stateManagerProvider = stateManagerProvider;
            return this;
        }

        public Builder encryptor(final StringEncryptor encryptor) {
            this.encryptor = encryptor;
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

        public StandardStatelessEngine build() {
            return new StandardStatelessEngine(this);
        }
    }
}
