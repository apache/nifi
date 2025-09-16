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

import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.LocalPort;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.StandardConnection;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.StandardFunnel;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.flow.AbstractFlowManager;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.flowrepository.FlowRepositoryClientInstantiationException;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.label.StandardLabel;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.FlowFileQueueFactory;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.StandardProcessGroup;
import org.apache.nifi.logging.ControllerServiceLogObserver;
import org.apache.nifi.logging.FlowRegistryClientLogObserver;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.logging.ProcessorLogObserver;
import org.apache.nifi.logging.ReportingTaskLogObserver;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.remote.StandardRemoteProcessGroup;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.stateless.queue.StatelessFlowFileQueue;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.net.ssl.SSLContext;

import static java.util.Objects.requireNonNull;

public class StatelessFlowManager extends AbstractFlowManager implements FlowManager {
    private static final Logger logger = LoggerFactory.getLogger(StatelessFlowManager.class);

    private final StatelessEngine statelessEngine;
    private final SSLContext sslContext;
    private final BulletinRepository bulletinRepository;

    public StatelessFlowManager(final FlowFileEventRepository flowFileEventRepository, final ParameterContextManager parameterContextManager,
                                final StatelessEngine statelessEngine, final BooleanSupplier flowInitializedCheck,
                                final SSLContext sslContext, final BulletinRepository bulletinRepository) {
        super(flowFileEventRepository, parameterContextManager, flowInitializedCheck);

        this.statelessEngine = statelessEngine;
        this.sslContext = sslContext;
        this.bulletinRepository = bulletinRepository;
    }

    @Override
    public Port createPublicInputPort(final String id, final String name) {
        throw new UnsupportedOperationException("Cannot create Public Input Port with name '" + name + "' because Public Input Ports and Public Output Ports are not supported in Stateless NiFi");
    }

    @Override
    public Port createPublicOutputPort(final String id, final String name) {
        throw new UnsupportedOperationException("Cannot create Public Output Port with name '" + name + "' because Public Input Ports and Public Output Ports are not supported in Stateless NiFi");
    }

    @Override
    public Set<Port> getPublicInputPorts() {
        return Collections.emptySet();
    }

    @Override
    public Set<Port> getPublicOutputPorts() {
        return Collections.emptySet();
    }

    @Override
    public Optional<Port> getPublicInputPort(final String name) {
        return Optional.empty();
    }

    @Override
    public Optional<Port> getPublicOutputPort(final String name) {
        return Optional.empty();
    }

    @Override
    public RemoteProcessGroup createRemoteProcessGroup(final String id, final String uris) {
        return new StandardRemoteProcessGroup(id, uris, null, statelessEngine.getProcessScheduler(), statelessEngine.getBulletinRepository(), sslContext,
            statelessEngine.getStateManagerProvider().getStateManager(id, StandardRemoteProcessGroup.class), TimeUnit.SECONDS.toMillis(30));
    }

    @Override
    public void instantiateSnippet(final ProcessGroup group, final FlowSnippetDTO dto) {
        throw new UnsupportedOperationException("Flow Snippets are not supported in Stateless NiFi");
    }

    @Override
    public FlowFilePrioritizer createPrioritizer(final String type) {
        // This will never actually be used, as the the Stateless FlowFile Queues will not take prioritizers into account.
        // However, we avoid returning null in order to ensure that we don't encounter any NullPointerExceptions, etc.
        return (o1, o2) -> o1.getLastQueueDate().compareTo(o2.getLastQueueDate());
    }

    @Override
    public ProcessorNode createProcessor(final String type, final String id, final BundleCoordinate coordinate, final Set<URL> additionalUrls, final boolean firstTimeAdded,
                                         final boolean registerLogObserver, final String classloaderIsolationKey) {
        logger.debug("Creating Processor of type {} with id {}", type, id);

        // make sure the first reference to LogRepository happens outside of a NarCloseable so that we use the framework's ClassLoader
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
        final ExtensionManager extensionManager = statelessEngine.getExtensionManager();

        try {
            final ProcessorNode procNode = new ComponentBuilder()
                .identifier(id)
                .type(type)
                .bundleCoordinate(coordinate)
                .statelessEngine(statelessEngine)
                .additionalClassPathUrls(additionalUrls)
                .buildProcessor();

            try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, procNode.getProcessor().getClass(), procNode.getProcessor().getIdentifier())) {
                ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, procNode.getProcessor());
            } catch (final Exception e) {
                throw new ComponentLifeCycleException("Failed to invoke @OnAdded methods of " + procNode.getProcessor(), e);
            }

            try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, procNode.getProcessor().getClass(), procNode.getProcessor().getIdentifier())) {
                final Class<?> componentClass = procNode.getProcessor() == null ? null : procNode.getProcessor().getClass();
                final StateManager stateManager = statelessEngine.getStateManagerProvider().getStateManager(id, componentClass);
                final StandardProcessContext processContext = new StandardProcessContext(procNode, statelessEngine.getControllerServiceProvider(),
                        stateManager, () -> false, new StatelessNodeTypeProvider());
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, procNode.getProcessor(), processContext);
            }

            LogRepositoryFactory.getRepository(procNode.getIdentifier()).setLogger(procNode.getLogger());

            // Only register an observer is there is no currently one registered. We do this because in stateless we create
            // a new ProcessorNode for each Concurrent Task every time that a dataflow is triggered, and we don't want to
            // register multiple observers for the same ProcessorNode.
            if (registerLogObserver && !logRepository.hasObserver()) {
                logRepository.addObserver(procNode.getBulletinLevel(), new ProcessorLogObserver(bulletinRepository, procNode));
            }

            logger.debug("Processor with id {} successfully created", id);

            return procNode;
        } catch (final ProcessorInstantiationException e) {
            throw new IllegalStateException("Could not create Processor of type " + type, e);
        }
    }

    @Override
    public Label createLabel(final String id, final String text) {
        return new StandardLabel(id, text);
    }

    @Override
    public Funnel createFunnel(final String id) {
        return new StandardFunnel(id, 1, 50000);
    }

    @Override
    public Port createLocalInputPort(final String id, final String name) {
        return new LocalPort(id, name, ConnectableType.INPUT_PORT, statelessEngine.getProcessScheduler(), 1, 50000, "10 millis");

    }

    @Override
    public Port createLocalOutputPort(final String id, final String name) {
        return new LocalPort(id, name, ConnectableType.OUTPUT_PORT, statelessEngine.getProcessScheduler(), 1, 50000, "10 millis");
    }

    @Override
    public ProcessGroup createProcessGroup(final String id) {
        final ProcessGroup created = new StandardProcessGroup(id, statelessEngine.getControllerServiceProvider(),
            statelessEngine.getProcessScheduler(),
            statelessEngine.getPropertyEncryptor(),
            statelessEngine.getExtensionManager(),
            statelessEngine.getStateManagerProvider(),
            this,
            statelessEngine.getReloadComponent(),
            new StatelessNodeTypeProvider(),
            null,
            group -> null,
            statelessEngine.getAssetManager());

        onProcessGroupAdded(created);
        return created;
    }

    @Override
    public Connection createConnection(final String id, final String name, final Connectable source, final Connectable destination, final Collection<String> relationshipNames) {
        final StandardConnection.Builder builder = new StandardConnection.Builder(statelessEngine.getProcessScheduler());

        final List<Relationship> relationships = new ArrayList<>();
        for (final String relationshipName : requireNonNull(relationshipNames)) {
            relationships.add(new Relationship.Builder().name(relationshipName).build());
        }

        final FlowFileQueueFactory flowFileQueueFactory = new FlowFileQueueFactory() {
            @Override
            public FlowFileQueue createFlowFileQueue(final LoadBalanceStrategy loadBalanceStrategy, final String partitioningAttribute,
                                                     final ProcessGroup processGroup) {
                return new StatelessFlowFileQueue(id);
            }
        };

        final Connection connection = builder.id(requireNonNull(id).intern())
            .name(name == null ? null : name.intern())
            .relationships(relationships)
            .source(requireNonNull(source))
            .destination(destination)
            .flowFileQueueFactory(flowFileQueueFactory)
            .processGroup(destination.getProcessGroup())
            .build();

        return connection;
    }

    @Override
    public ReportingTaskNode createReportingTask(final String type, final String id, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls, final boolean firstTimeAdded,
                                                 final boolean register, final String classloaderIsolationKey) {

        if (type == null || id == null || bundleCoordinate == null) {
            throw new NullPointerException("Must supply type, id, and bundle coordinate in order to create Reporting Task. Provided arguments were type=" + type + ", id=" + id
                + ", bundle coordinate = " + bundleCoordinate);
        }

        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);

        final ReportingTaskNode taskNode;
        try {
            taskNode = new ComponentBuilder()
                .identifier(id)
                .type(type)
                .bundleCoordinate(bundleCoordinate)
                .statelessEngine(statelessEngine)
                .additionalClassPathUrls(additionalUrls)
                .flowManager(this)
                .buildReportingTask();
        } catch (final ReportingTaskInstantiationException e) {
            throw new IllegalStateException("Could not create Reporting Task of type " + type + " with ID " + id, e);
        }

        LogRepositoryFactory.getRepository(taskNode.getIdentifier()).setLogger(taskNode.getLogger());

        if (firstTimeAdded) {
            final Class<?> taskClass = taskNode.getReportingTask().getClass();
            final String identifier = taskNode.getReportingTask().getIdentifier();

            try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(statelessEngine.getExtensionManager(), taskClass, identifier)) {
                ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, taskNode.getReportingTask());

                if (isFlowInitialized()) {
                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, taskNode.getReportingTask(), taskNode.getConfigurationContext());
                }
            } catch (final Exception e) {
                throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + taskNode.getReportingTask(), e);
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
    public ParameterProviderNode createParameterProvider(final String type, final String id, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls, final boolean firstTimeAdded,
                                                         final boolean register) {

        throw new UnsupportedOperationException("Parameter Providers are not supported in Stateless NiFi");
    }

    @Override
    public FlowRegistryClientNode createFlowRegistryClient(final String type, final String id, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls,
                                                           final boolean firstTimeAdded, final boolean registerLogObserver, final String classloaderIsolationKey) {
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);

        final FlowRegistryClientNode clientNode;
        try {
            clientNode = new ComponentBuilder()
                    .identifier(id)
                    .type(type)
                    .statelessEngine(statelessEngine)
                    .additionalClassPathUrls(additionalUrls)
                    .flowManager(this)
                    .buildFlowRegistryClient();
        } catch (final FlowRepositoryClientInstantiationException e) {
            throw new IllegalStateException("Could not create Flow Registry Client of type " + type + " with ID " + id, e);
        }

        onFlowRegistryClientAdded(clientNode);
        LogRepositoryFactory.getRepository(clientNode.getIdentifier()).setLogger(clientNode.getLogger());

        if (registerLogObserver) {

            // Register log observer to provide bulletins when reporting task logs anything at WARN level or above
            logRepository.addObserver(LogLevel.WARN, new FlowRegistryClientLogObserver(bulletinRepository, clientNode));
        }

        return clientNode;
    }

    @Override
    public void removeFlowRegistryClient(FlowRegistryClientNode clientNode) {
        throw new UnsupportedOperationException("Removing Flow Registry Client is not supported in Stateless NiFi");
    }

    @Override
    protected ExtensionManager getExtensionManager() {
        return statelessEngine.getExtensionManager();
    }

    @Override
    protected ProcessScheduler getProcessScheduler() {
        return statelessEngine.getProcessScheduler();
    }

    @Override
    public Set<ReportingTaskNode> getAllReportingTasks() {
        return Collections.emptySet();
    }

    @Override
    public ControllerServiceNode createControllerService(final String type, final String id, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls,
                                                         final boolean firstTimeAdded, final boolean registerLogObserver, final String classloaderIsolationKey) {

        logger.debug("Creating Controller Service of type {} with id {}", type, id);
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);

        final ControllerServiceNode serviceNode = new ComponentBuilder()
            .identifier(id)
            .type(type)
            .bundleCoordinate(bundleCoordinate)
            .statelessEngine(statelessEngine)
            .additionalClassPathUrls(additionalUrls)
            .buildControllerService();

        final ControllerService service = serviceNode.getControllerServiceImplementation();
        final ExtensionManager extensionManager = statelessEngine.getExtensionManager();

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, service.getClass(), service.getIdentifier())) {
            final ConfigurationContext configurationContext =
                    new StandardConfigurationContext(serviceNode, statelessEngine.getControllerServiceProvider(), null);
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, service, configurationContext);
        }

        final ControllerService serviceImpl = serviceNode.getControllerServiceImplementation();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, serviceImpl.getClass(), serviceImpl.getIdentifier())) {
            ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, serviceImpl);
        } catch (final Exception e) {
            throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + serviceImpl, e);
        }

        LogRepositoryFactory.getRepository(serviceNode.getIdentifier()).setLogger(serviceNode.getLogger());
        if (registerLogObserver) {
            // Register log observer to provide bulletins when reporting task logs anything at WARN level or above
            logRepository.addObserver(LogLevel.WARN, new ControllerServiceLogObserver(bulletinRepository, serviceNode));
        }

        statelessEngine.getControllerServiceProvider().onControllerServiceAdded(serviceNode);
        logger.debug("Controller Service with id {} successfully created", id);

        return serviceNode;
    }

    @Override
    public Set<ControllerServiceNode> getRootControllerServices() {
        return Collections.emptySet();
    }

    @Override
    public void addRootControllerService(final ControllerServiceNode serviceNode) {
        throw new UnsupportedOperationException("Root-Level Controller Services are not supported in Stateless NiFi");
    }

    @Override
    public ControllerServiceNode getRootControllerService(final String serviceIdentifier) {
        return null;
    }

    @Override
    public void removeRootControllerService(final ControllerServiceNode service) {
    }

    @Override
    protected Authorizable getParameterContextParent() {
        return null;
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
        return null;
    }
}
