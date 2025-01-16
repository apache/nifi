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
package org.apache.nifi.controller.flow;

import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.LocalPort;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ExtensionBuilder;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.FlowSnippet;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.StandardFlowSnippet;
import org.apache.nifi.controller.StandardFunnel;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.label.StandardLabel;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.deprecation.log.DeprecationLogger;
import org.apache.nifi.deprecation.log.DeprecationLoggerFactory;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.StandardProcessGroup;
import org.apache.nifi.groups.StatelessGroupNodeFactory;
import org.apache.nifi.logging.ControllerServiceLogObserver;
import org.apache.nifi.logging.FlowAnalysisRuleLogObserver;
import org.apache.nifi.logging.FlowRegistryClientLogObserver;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.logging.ParameterProviderLogObserver;
import org.apache.nifi.logging.ProcessorLogObserver;
import org.apache.nifi.logging.ReportingTaskLogObserver;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.StandardPublicPort;
import org.apache.nifi.remote.StandardRemoteProcessGroup;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class StandardFlowManager extends AbstractFlowManager implements FlowManager {
    static final String MAX_CONCURRENT_TASKS_PROP_NAME = "_nifi.funnel.max.concurrent.tasks";
    static final String MAX_TRANSFERRED_FLOWFILES_PROP_NAME = "_nifi.funnel.max.transferred.flowfiles";

    private static final Logger logger = LoggerFactory.getLogger(StandardFlowManager.class);

    private final NiFiProperties nifiProperties;
    private final BulletinRepository bulletinRepository;
    private final StandardProcessScheduler processScheduler;
    private final Authorizer authorizer;
    private final SSLContext sslContext;
    private final FlowController flowController;

    private final ConcurrentMap<String, ControllerServiceNode> rootControllerServices = new ConcurrentHashMap<>();

    private final boolean isSiteToSiteSecure;


    public StandardFlowManager(final NiFiProperties nifiProperties, final SSLContext sslContext, final FlowController flowController,
                               final FlowFileEventRepository flowFileEventRepository, final ParameterContextManager parameterContextManager) {
        super(flowFileEventRepository, parameterContextManager, flowController::isInitialized);
        this.nifiProperties = nifiProperties;
        this.flowController = flowController;
        this.bulletinRepository = flowController.getBulletinRepository();
        this.processScheduler = flowController.getProcessScheduler();
        this.authorizer = flowController.getAuthorizer();
        this.sslContext = sslContext;

        this.isSiteToSiteSecure = Boolean.TRUE.equals(nifiProperties.isSiteToSiteSecure());
    }

    public Port createPublicInputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);
        return new StandardPublicPort(id, name,
            TransferDirection.RECEIVE, ConnectableType.INPUT_PORT, authorizer, bulletinRepository,
            processScheduler, isSiteToSiteSecure, nifiProperties.getBoredYieldDuration(),
            IdentityMappingUtil.getIdentityMappings(nifiProperties));
    }

    public Port createPublicOutputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);
        return new StandardPublicPort(id, name,
            TransferDirection.SEND, ConnectableType.OUTPUT_PORT, authorizer, bulletinRepository,
            processScheduler, isSiteToSiteSecure, nifiProperties.getBoredYieldDuration(),
            IdentityMappingUtil.getIdentityMappings(nifiProperties));
    }

    /**
     * Gets all remotely accessible ports in any process group.
     *
     * @return input ports
     */
    public Set<Port> getPublicInputPorts() {
        return getPublicPorts(ProcessGroup::getInputPorts);
    }

    /**
     * Gets all remotely accessible ports in any process group.
     *
     * @return output ports
     */
    public Set<Port> getPublicOutputPorts() {
        return getPublicPorts(ProcessGroup::getOutputPorts);
    }

    private Set<Port> getPublicPorts(final Function<ProcessGroup, Set<Port>> getPorts) {
        final Set<Port> publicPorts = new HashSet<>();
        ProcessGroup rootGroup = getRootGroup();
        getPublicPorts(publicPorts, rootGroup, getPorts);
        return publicPorts;
    }

    private void getPublicPorts(final Set<Port> publicPorts, final ProcessGroup group, final Function<ProcessGroup, Set<Port>> getPorts) {
        for (final Port port : getPorts.apply(group)) {
            if (port instanceof PublicPort) {
                publicPorts.add(port);
            }
        }
        group.getProcessGroups().forEach(childGroup -> getPublicPorts(publicPorts, childGroup, getPorts));
    }

    @Override
    public Optional<Port> getPublicInputPort(String name) {
        return findPort(name, getPublicInputPorts());
    }

    @Override
    public Optional<Port> getPublicOutputPort(String name) {
        return findPort(name, getPublicOutputPorts());
    }

    private Optional<Port> findPort(final String portName, final Set<Port> ports) {
        if (ports != null) {
            for (final Port port : ports) {
                if (portName.equals(port.getName())) {
                    return Optional.of(port);
                }
            }
        }
        return Optional.empty();
    }

    public RemoteProcessGroup createRemoteProcessGroup(final String id, final String uris) {
        final String expirationPeriod = nifiProperties.getProperty(NiFiProperties.REMOTE_CONTENTS_CACHE_EXPIRATION, "30 secs");
        final long remoteContentsCacheExpirationMillis = FormatUtils.getTimeDuration(expirationPeriod, TimeUnit.MILLISECONDS);

        return new StandardRemoteProcessGroup(requireNonNull(id), uris, null,
            processScheduler, bulletinRepository, sslContext,
            flowController.getStateManagerProvider().getStateManager(id), remoteContentsCacheExpirationMillis);
    }

    private void verifyPortIdDoesNotExist(final String id) {
        final ProcessGroup rootGroup = getRootGroup();
        Port port = rootGroup.findOutputPort(id);
        if (port != null) {
            throw new IllegalStateException("An Output Port already exists with ID " + id);
        }
        port = rootGroup.findInputPort(id);
        if (port != null) {
            throw new IllegalStateException("An Input Port already exists with ID " + id);
        }
    }

    public Label createLabel(final String id, final String text) {
        return new StandardLabel(requireNonNull(id).intern(), text);
    }

    public Funnel createFunnel(final String id) {
        final int maxConcurrentTasks = Integer.parseInt(nifiProperties.getProperty(MAX_CONCURRENT_TASKS_PROP_NAME, "1"));
        final int maxBatchSize = Integer.parseInt(nifiProperties.getProperty(MAX_TRANSFERRED_FLOWFILES_PROP_NAME, "10000"));

        return new StandardFunnel(id.intern(), maxConcurrentTasks, maxBatchSize);
    }

    public Port createLocalInputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);

        final int maxConcurrentTasks = Integer.parseInt(nifiProperties.getProperty(MAX_CONCURRENT_TASKS_PROP_NAME, "1"));
        final int maxTransferredFlowFiles = Integer.parseInt(nifiProperties.getProperty(MAX_TRANSFERRED_FLOWFILES_PROP_NAME, "10000"));
        final String boredYieldDuration = nifiProperties.getBoredYieldDuration();

        return new LocalPort(id, name, ConnectableType.INPUT_PORT, processScheduler, maxConcurrentTasks, maxTransferredFlowFiles, boredYieldDuration);
    }

    public Port createLocalOutputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);

        final int maxConcurrentTasks = Integer.parseInt(nifiProperties.getProperty(MAX_CONCURRENT_TASKS_PROP_NAME, "1"));
        final int maxTransferredFlowFiles = Integer.parseInt(nifiProperties.getProperty(MAX_TRANSFERRED_FLOWFILES_PROP_NAME, "10000"));
        final String boredYieldDuration = nifiProperties.getBoredYieldDuration();

        return new LocalPort(id, name, ConnectableType.OUTPUT_PORT, processScheduler, maxConcurrentTasks, maxTransferredFlowFiles, boredYieldDuration);
    }

    public ProcessGroup createProcessGroup(final String id) {
        final StatelessGroupNodeFactory statelessGroupNodeFactory = new StandardStatelessGroupNodeFactory(flowController, sslContext, flowController.createKerberosConfig(nifiProperties));

        final ProcessGroup group = new StandardProcessGroup(requireNonNull(id), flowController.getControllerServiceProvider(), processScheduler, flowController.getEncryptor(),
            flowController.getExtensionManager(), flowController.getStateManagerProvider(), this,
            flowController.getReloadComponent(), flowController, nifiProperties, statelessGroupNodeFactory,
            flowController.getAssetManager());
        onProcessGroupAdded(group);

        return group;
    }


    public void instantiateSnippet(final ProcessGroup group, final FlowSnippetDTO dto) throws ProcessorInstantiationException {
        requireNonNull(group);
        requireNonNull(dto);

        final FlowSnippet snippet = new StandardFlowSnippet(dto, flowController.getExtensionManager());
        snippet.validate(group);
        snippet.instantiate(this, flowController, group);

        group.findAllRemoteProcessGroups().forEach(RemoteProcessGroup::initialize);
    }

    public FlowFilePrioritizer createPrioritizer(final String type) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        FlowFilePrioritizer prioritizer;

        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final List<Bundle> prioritizerBundles = flowController.getExtensionManager().getBundles(type);
            if (prioritizerBundles.size() == 0) {
                throw new IllegalStateException(String.format("The specified class '%s' is not known to this nifi.", type));
            }
            if (prioritizerBundles.size() > 1) {
                throw new IllegalStateException(String.format("Multiple bundles found for the specified class '%s', only one is allowed.", type));
            }

            final Bundle bundle = prioritizerBundles.get(0);
            final ClassLoader detectedClassLoaderForType = bundle.getClassLoader();
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoaderForType);

            Thread.currentThread().setContextClassLoader(detectedClassLoaderForType);
            final Class<? extends FlowFilePrioritizer> prioritizerClass = rawClass.asSubclass(FlowFilePrioritizer.class);
            final Object processorObj;
            try {
                processorObj = prioritizerClass.getDeclaredConstructor().newInstance();
            } catch (final InvocationTargetException | NoSuchMethodException e) {
                throw new ClassNotFoundException("Could not find class or default no-arg constructor for " + type, e);
            }

            prioritizer = prioritizerClass.cast(processorObj);

            return prioritizer;
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }

    @Override
    public ProcessorNode createProcessor(final String type, String id, final BundleCoordinate coordinate, final Set<URL> additionalUrls,
                                         final boolean firstTimeAdded, final boolean registerLogObserver, final String classloaderIsolationKey) {

        // make sure the first reference to LogRepository happens outside of a NarCloseable so that we use the framework's ClassLoader
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
        final ExtensionManager extensionManager = flowController.getExtensionManager();

        final ProcessorNode procNode = new ExtensionBuilder()
            .identifier(id)
            .type(type)
            .bundleCoordinate(coordinate)
            .controllerServiceProvider(flowController.getControllerServiceProvider())
            .processScheduler(processScheduler)
            .nodeTypeProvider(flowController)
            .validationTrigger(flowController.getValidationTrigger())
            .reloadComponent(flowController.getReloadComponent())
            .addClasspathUrls(additionalUrls)
            .kerberosConfig(flowController.createKerberosConfig(nifiProperties))
            .extensionManager(extensionManager)
            .flowAnalyzer(getFlowAnalyzer().orElse(null))
            .ruleViolationsManager(getRuleViolationsManager().orElse(null))
            .classloaderIsolationKey(classloaderIsolationKey)
            .pythonBridge(flowController.getPythonBridge())
            .buildProcessor();

        LogRepositoryFactory.getRepository(procNode.getIdentifier()).setLogger(procNode.getLogger());
        if (registerLogObserver) {
            logRepository.addObserver(procNode.getBulletinLevel(), new ProcessorLogObserver(bulletinRepository, procNode));
        }

        if (firstTimeAdded) {
            final Processor processor = procNode.getProcessor();
            try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, processor.getClass(), processor.getIdentifier())) {
                ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, processor);
                logDeprecationNotice(processor);
            } catch (final Exception e) {
                if (registerLogObserver) {
                    logRepository.removeAllObservers();
                }
                throw new ComponentLifeCycleException("Failed to invoke @OnAdded methods of " + procNode.getProcessor(), e);
            }
        }

        return procNode;
    }

    public Connection createConnection(final String id, final String name, final Connectable source, final Connectable destination, final Collection<String> relationshipNames) {
        return flowController.createConnection(id, name, source, destination, relationshipNames);
    }

    @Override
    public FlowRegistryClientNode createFlowRegistryClient(
            final String type, final String id, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls,
            final boolean firstTimeAdded, final boolean registerLogObserver, String classloaderIsolationKey) {
        requireNonNull(type);
        requireNonNull(id);
        requireNonNull(bundleCoordinate);

        // make sure the first reference to LogRepository happens outside of a NarCloseable so that we use the framework's ClassLoader
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
        final ExtensionManager extensionManager = flowController.getExtensionManager();

        final FlowRegistryClientNode clientNode = new ExtensionBuilder()
                .identifier(id)
                .type(type)
                .flowController(flowController)
                .bundleCoordinate(bundleCoordinate)
                .processScheduler(processScheduler)
                .controllerServiceProvider(flowController.getControllerServiceProvider())
                .nodeTypeProvider(flowController)
                .validationTrigger(flowController.getValidationTrigger())
                .reloadComponent(flowController.getReloadComponent())
                .addClasspathUrls(additionalUrls)
                .kerberosConfig(flowController.createKerberosConfig(nifiProperties))
                .systemSslContext(sslContext)
                .extensionManager(extensionManager)
                .classloaderIsolationKey(classloaderIsolationKey)
                .flowAnalysisAtRegistryCommit(nifiProperties.flowRegistryCheckForRuleViolationsBeforeCommit())
                .flowAnalyzer(getFlowAnalyzer().orElse(null))
                .ruleViolationsManager(getRuleViolationsManager().orElse(null))
                .buildFlowRegistryClient();

        LogRepositoryFactory.getRepository(clientNode.getIdentifier()).setLogger(clientNode.getLogger());

        if (firstTimeAdded) {
            final Class<?> clientClass = clientNode.getComponent().getClass();
            final String identifier = clientNode.getComponent().getIdentifier();

            try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), clientClass, identifier)) {
                ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, clientNode.getComponent());

                if (flowController.isInitialized()) {
                    final ConfigurationContext configurationContext = new StandardConfigurationContext(
                            clientNode, flowController.getControllerServiceProvider(), null);
                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, clientNode.getComponent(), configurationContext);
                }
            } catch (final Exception e) {
                throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + clientNode.getComponent(), e);
            }
        }

        if (registerLogObserver) {
            onFlowRegistryClientAdded(clientNode);

            // Register log observer to provide bulletins when reporting task logs anything at WARN level or above
            logRepository.addObserver(LogLevel.WARN, new FlowRegistryClientLogObserver(bulletinRepository, clientNode));
        }

        return clientNode;

    }

    @Override
    public void removeFlowRegistryClient(final FlowRegistryClientNode clientNode) {
        final FlowRegistryClientNode existing = getFlowRegistryClient(clientNode.getIdentifier());

        if (existing == null || existing != clientNode) {
            throw new IllegalStateException("Flow Registry Client " + clientNode + " does not exist in this Flow");
        }

        final Class<?> clientClass = clientNode.getComponent().getClass();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), clientClass, clientNode.getComponent().getIdentifier())) {
            final ConfigurationContext configurationContext = new StandardConfigurationContext(clientNode, flowController.getControllerServiceProvider(), null);
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, clientNode.getComponent(), configurationContext);
        }

        onFlowRegistryClientRemoved(clientNode);
        LogRepositoryFactory.removeRepository(clientNode.getIdentifier());
        processScheduler.submitFrameworkTask(() -> flowController.getStateManagerProvider().onComponentRemoved(clientNode.getIdentifier()));

        getExtensionManager().removeInstanceClassLoader(clientNode.getIdentifier());
    }

    public ReportingTaskNode createReportingTask(final String type, final String id, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls,
                                                 final boolean firstTimeAdded, final boolean register, final String classloaderIsolationKey) {
        requireNonNull(type);
        requireNonNull(id);
        requireNonNull(bundleCoordinate);

        // make sure the first reference to LogRepository happens outside of a NarCloseable so that we use the framework's ClassLoader
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
        final ExtensionManager extensionManager = flowController.getExtensionManager();

        final ReportingTaskNode taskNode = new ExtensionBuilder()
            .identifier(id)
            .type(type)
            .bundleCoordinate(bundleCoordinate)
            .controllerServiceProvider(flowController.getControllerServiceProvider())
            .processScheduler(processScheduler)
            .nodeTypeProvider(flowController)
            .validationTrigger(flowController.getValidationTrigger())
            .reloadComponent(flowController.getReloadComponent())
            .addClasspathUrls(additionalUrls)
            .kerberosConfig(flowController.createKerberosConfig(nifiProperties))
            .flowController(flowController)
            .extensionManager(extensionManager)
            .classloaderIsolationKey(classloaderIsolationKey)
            .buildReportingTask();

        LogRepositoryFactory.getRepository(taskNode.getIdentifier()).setLogger(taskNode.getLogger());

        if (firstTimeAdded) {
            final ReportingTask reportingTask = taskNode.getReportingTask();
            final Class<? extends ConfigurableComponent> taskClass = reportingTask.getClass();
            final String identifier = reportingTask.getIdentifier();

            try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), taskClass, identifier)) {
                ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, reportingTask);
                logDeprecationNotice(reportingTask);

                if (flowController.isInitialized()) {
                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, reportingTask, taskNode.getConfigurationContext());
                }
            } catch (final Exception e) {
                throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + reportingTask, e);
            }
        }

        if (register) {
            onReportingTaskAdded(taskNode);

            // Register log observer to provide bulletins when reporting task logs anything at WARN level or above
            logRepository.addObserver(LogLevel.WARN, new ReportingTaskLogObserver(bulletinRepository, taskNode));
        }

        return taskNode;
    }

    @Override
    public FlowAnalysisRuleNode createFlowAnalysisRule(
            final String type,
            final String id,
            final BundleCoordinate bundleCoordinate,
            final Set<URL> additionalUrls,
            final boolean firstTimeAdded,
            final boolean register,
            final String classloaderIsolationKey
    ) {
        requireNonNull(type);
        requireNonNull(id);
        requireNonNull(bundleCoordinate);

        // make sure the first reference to LogRepository happens outside of a NarCloseable so that we use the framework's ClassLoader
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
        final ExtensionManager extensionManager = flowController.getExtensionManager();

        final FlowAnalysisRuleNode flowAnalysisRuleNode = new ExtensionBuilder()
            .identifier(id)
            .type(type)
            .bundleCoordinate(bundleCoordinate)
            .controllerServiceProvider(flowController.getControllerServiceProvider())
            .processScheduler(processScheduler)
            .nodeTypeProvider(flowController)
            .validationTrigger(flowController.getValidationTrigger())
            .reloadComponent(flowController.getReloadComponent())
            .addClasspathUrls(additionalUrls)
            .kerberosConfig(flowController.createKerberosConfig(nifiProperties))
            .flowController(flowController)
            .extensionManager(extensionManager)
            .flowAnalyzer(getFlowAnalyzer().orElse(null))
            .ruleViolationsManager(getRuleViolationsManager().orElse(null))
            .classloaderIsolationKey(classloaderIsolationKey)
            .buildFlowAnalysisRuleNode();

        LogRepositoryFactory.getRepository(flowAnalysisRuleNode.getIdentifier()).setLogger(flowAnalysisRuleNode.getLogger());

        if (firstTimeAdded) {
            FlowAnalysisRule flowAnalysisRule = flowAnalysisRuleNode.getFlowAnalysisRule();
            final Class<? extends ConfigurableComponent> flowAnalysisRuleClass = flowAnalysisRule.getClass();
            final String identifier = flowAnalysisRule.getIdentifier();

            try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), flowAnalysisRuleClass, identifier)) {
                ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, flowAnalysisRule);
                logDeprecationNotice(flowAnalysisRule);

                if (flowController.isInitialized()) {
                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, flowAnalysisRule, flowAnalysisRuleNode.getConfigurationContext());
                }
            } catch (final Exception e) {
                throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + flowAnalysisRule, e);
            }
        }

        if (register) {
            onFlowAnalysisRuleAdded(flowAnalysisRuleNode);

            // Register log observer to provide bulletins when flow analysis rule logs anything at WARN level or above
            logRepository.addObserver(LogLevel.WARN, new FlowAnalysisRuleLogObserver(bulletinRepository, flowAnalysisRuleNode));
        }

        return flowAnalysisRuleNode;
    }

    @Override
    public ParameterProviderNode createParameterProvider(final String type, final String id, final BundleCoordinate bundleCoordinate,
                                                         final Set<URL> additionalUrls, final boolean firstTimeAdded, final boolean registerLogObserver) {
        requireNonNull(type);
        requireNonNull(id);
        requireNonNull(bundleCoordinate);

        // make sure the first reference to LogRepository happens outside of a NarCloseable so that we use the framework's ClassLoader
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
        final ExtensionManager extensionManager = flowController.getExtensionManager();

        final ParameterProviderNode parameterProviderNode = new ExtensionBuilder()
                .identifier(id)
                .type(type)
                .bundleCoordinate(bundleCoordinate)
                .controllerServiceProvider(flowController.getControllerServiceProvider())
                .processScheduler(processScheduler)
                .nodeTypeProvider(flowController)
                .validationTrigger(flowController.getValidationTrigger())
                .reloadComponent(flowController.getReloadComponent())
                .addClasspathUrls(additionalUrls)
                .kerberosConfig(flowController.createKerberosConfig(nifiProperties))
                .flowController(flowController)
                .extensionManager(extensionManager)
                .buildParameterProvider();

        LogRepositoryFactory.getRepository(parameterProviderNode.getIdentifier()).setLogger(parameterProviderNode.getLogger());

        if (firstTimeAdded) {
            final ParameterProvider parameterProvider = parameterProviderNode.getParameterProvider();
            final Class<? extends ConfigurableComponent> parameterProviderClass = parameterProvider.getClass();
            final String identifier = parameterProvider.getIdentifier();

            try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), parameterProviderClass, identifier)) {
                ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, parameterProvider);
                logDeprecationNotice(parameterProvider);

                if (flowController.isInitialized()) {
                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, parameterProvider);
                }
            } catch (final Exception e) {
                throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + parameterProvider, e);
            }
        }

        if (registerLogObserver) {
            onParameterProviderAdded(parameterProviderNode);

            // Register log observer to provide bulletins when reporting task logs anything at WARN level or above
            logRepository.addObserver(LogLevel.WARN, new ParameterProviderLogObserver(bulletinRepository, parameterProviderNode));
        }

        return parameterProviderNode;
    }

    public Set<ControllerServiceNode> getRootControllerServices() {
        return new HashSet<>(rootControllerServices.values());
    }

    public void addRootControllerService(final ControllerServiceNode serviceNode) {
        final ControllerServiceNode existing = rootControllerServices.putIfAbsent(serviceNode.getIdentifier(), serviceNode);
        if (existing != null) {
            throw new IllegalStateException("Controller Service with ID " + serviceNode.getIdentifier() + " already exists at the Controller level");
        }
    }

    public ControllerServiceNode getRootControllerService(final String serviceIdentifier) {
        return rootControllerServices.get(serviceIdentifier);
    }

    public void removeRootControllerService(final ControllerServiceNode service) {
        final ControllerServiceNode existing = rootControllerServices.get(requireNonNull(service).getIdentifier());
        if (existing == null) {
            throw new IllegalStateException(service + " is not a member of this Process Group");
        }

        service.verifyCanDelete();

        final ExtensionManager extensionManager = flowController.getExtensionManager();

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, service.getControllerServiceImplementation().getClass(), service.getIdentifier())) {
            final ConfigurationContext configurationContext = new StandardConfigurationContext(service, flowController.getControllerServiceProvider(), null);
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, service.getControllerServiceImplementation(), configurationContext);
        }

        for (final Map.Entry<PropertyDescriptor, String> entry : service.getEffectivePropertyValues().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() != null) {
                final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                if (value != null) {
                    final ControllerServiceNode referencedNode = getRootControllerService(value);
                    if (referencedNode != null) {
                        referencedNode.removeReference(service, descriptor);
                    }
                }
            }
        }

        rootControllerServices.remove(service.getIdentifier());

        processScheduler.submitFrameworkTask(() -> flowController.getStateManagerProvider().onComponentRemoved(service.getIdentifier()));

        getRuleViolationsManager().ifPresent(
            ruleViolationsManager -> processScheduler.submitFrameworkTask(
                () -> ruleViolationsManager.removeRuleViolationsForSubject(service.getIdentifier())
            )
        );

        extensionManager.removeInstanceClassLoader(service.getIdentifier());

        logger.info("{} removed from Flow Controller", service);
    }

    public ControllerServiceNode createControllerService(final String type, final String id, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls, final boolean firstTimeAdded,
                                                         final boolean registerLogObserver, final String classloaderIsolationKey) {
        // make sure the first reference to LogRepository happens outside of a NarCloseable so that we use the framework's ClassLoader
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
        final ExtensionManager extensionManager = flowController.getExtensionManager();
        final ControllerServiceProvider controllerServiceProvider = flowController.getControllerServiceProvider();

        final ControllerServiceNode serviceNode = new ExtensionBuilder()
            .identifier(id)
            .type(type)
            .bundleCoordinate(bundleCoordinate)
            .controllerServiceProvider(flowController.getControllerServiceProvider())
            .processScheduler(processScheduler)
            .nodeTypeProvider(flowController)
            .validationTrigger(flowController.getValidationTrigger())
            .reloadComponent(flowController.getReloadComponent())
            .addClasspathUrls(additionalUrls)
            .kerberosConfig(flowController.createKerberosConfig(nifiProperties))
            .stateManagerProvider(flowController.getStateManagerProvider())
            .extensionManager(extensionManager)
            .flowAnalyzer(getFlowAnalyzer().orElse(null))
            .ruleViolationsManager(getRuleViolationsManager().orElse(null))
            .classloaderIsolationKey(classloaderIsolationKey)
            .buildControllerService();

        LogRepositoryFactory.getRepository(serviceNode.getIdentifier()).setLogger(serviceNode.getLogger());
        if (registerLogObserver) {
            logRepository.addObserver(serviceNode.getBulletinLevel(), new ControllerServiceLogObserver(bulletinRepository, serviceNode));
        }

        if (firstTimeAdded) {
            final ControllerService service = serviceNode.getControllerServiceImplementation();

            if (flowController.isInitialized()) {
                try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, service.getClass(), service.getIdentifier())) {
                    final ConfigurationContext configurationContext =
                            new StandardConfigurationContext(serviceNode, controllerServiceProvider, null);
                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, service, configurationContext);
                }
            }

            final ControllerService serviceImpl = serviceNode.getControllerServiceImplementation();
            try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, serviceImpl.getClass(), serviceImpl.getIdentifier())) {
                ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, serviceImpl);
                logDeprecationNotice(serviceImpl);
            } catch (final Exception e) {
                throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + serviceImpl, e);
            }
        }

        controllerServiceProvider.onControllerServiceAdded(serviceNode);

        return serviceNode;
    }

    @Override
    protected ExtensionManager getExtensionManager() {
        return flowController.getExtensionManager();
    }

    @Override
    protected ProcessScheduler getProcessScheduler() {
        return flowController.getProcessScheduler();
    }

    @Override
    protected Authorizable getParameterContextParent() {
        return flowController;
    }

    private void logDeprecationNotice(final ConfigurableComponent component) {
        final Class<? extends ConfigurableComponent> componentClass = component.getClass();

        final DeprecationNotice deprecationNotice = componentClass.getAnnotation(DeprecationNotice.class);
        if (deprecationNotice != null) {
            final DeprecationLogger deprecationLogger = DeprecationLoggerFactory.getLogger(componentClass);
            final List<String> alternatives = new ArrayList<>();

            for (final Class<? extends ConfigurableComponent> alternativeClass : deprecationNotice.alternatives()) {
                alternatives.add(alternativeClass.getSimpleName());
            }
            Collections.addAll(alternatives, deprecationNotice.classNames());

            deprecationLogger.warn("Added Deprecated Component {}[id={}] See alternatives {}",
                    componentClass.getSimpleName(),
                    component.getIdentifier(),
                    alternatives
            );
        }
    }
}
