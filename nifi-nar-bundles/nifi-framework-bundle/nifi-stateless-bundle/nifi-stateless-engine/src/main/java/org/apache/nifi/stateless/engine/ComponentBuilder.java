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

import org.apache.commons.lang3.ClassUtils;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.reporting.StandardReportingInitializationContext;
import org.apache.nifi.controller.reporting.StatelessReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardControllerServiceInitializationContext;
import org.apache.nifi.controller.service.StandardControllerServiceInvocationHandler;
import org.apache.nifi.controller.service.StandardControllerServiceNode;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.processor.StandardProcessorInitializationContext;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.variable.StandardComponentVariableRegistry;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ComponentBuilder {
    private static final Logger logger = LoggerFactory.getLogger(ComponentBuilder.class);

    private StatelessEngine<VersionedFlowSnapshot> statelessEngine;
    private FlowManager flowManager;
    private String identifier;
    private String type;
    private BundleCoordinate bundleCoordinate;
    private Set<URL> additionalClassPathUrls;

    public ComponentBuilder statelessEngine(final StatelessEngine<VersionedFlowSnapshot> statelessEngine) {
        this.statelessEngine = statelessEngine;
        return this;
    }

    public ComponentBuilder identifier(final String identifier) {
        this.identifier = identifier;
        return this;
    }

    public ComponentBuilder type(final String type) {
        this.type = type;
        return this;
    }

    public ComponentBuilder bundleCoordinate(final BundleCoordinate bundleCoordinate) {
        this.bundleCoordinate = bundleCoordinate;
        return this;
    }

    public ComponentBuilder additionalClassPathUrls(final Set<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            return this;
        }

        if (this.additionalClassPathUrls == null) {
            this.additionalClassPathUrls = new HashSet<>();
        }

        this.additionalClassPathUrls.addAll(urls);
        return this;
    }

    public ComponentBuilder flowManager(final FlowManager flowManager) {
        this.flowManager = flowManager;
        return this;
    }

    public ProcessorNode buildProcessor() throws ProcessorInstantiationException {
        final LoggableComponent<Processor> loggableProcessor = createLoggableProcessor();
        final ProcessScheduler processScheduler = statelessEngine.getProcessScheduler();
        final ControllerServiceProvider controllerServiceProvider = statelessEngine.getControllerServiceProvider();
        final ComponentVariableRegistry componentVariableRegistry = new StandardComponentVariableRegistry(statelessEngine.getRootVariableRegistry());
        final ReloadComponent reloadComponent = statelessEngine.getReloadComponent();
        final ExtensionManager extensionManager = statelessEngine.getExtensionManager();
        final ValidationTrigger validationTrigger = statelessEngine.getValidationTrigger();
        final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(controllerServiceProvider, componentVariableRegistry);

        final ProcessorNode procNode = new StandardProcessorNode(loggableProcessor, identifier, validationContextFactory, processScheduler, controllerServiceProvider,
            componentVariableRegistry, reloadComponent, extensionManager, validationTrigger);

        logger.info("Created Processor of type {} with identifier {}", type, identifier);

        return procNode;
    }

    public ReportingTaskNode buildReportingTask() throws ReportingTaskInstantiationException {
        final LoggableComponent<ReportingTask> reportingTaskComponent = createLoggableReportingTask();
        final ProcessScheduler processScheduler = statelessEngine.getProcessScheduler();
        final ControllerServiceProvider controllerServiceProvider = statelessEngine.getControllerServiceProvider();
        final ComponentVariableRegistry componentVariableRegistry = new StandardComponentVariableRegistry(statelessEngine.getRootVariableRegistry());
        final ReloadComponent reloadComponent = statelessEngine.getReloadComponent();
        final ExtensionManager extensionManager = statelessEngine.getExtensionManager();
        final ValidationTrigger validationTrigger = statelessEngine.getValidationTrigger();
        final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(controllerServiceProvider, componentVariableRegistry);

        final ReportingTaskNode taskNode = new StatelessReportingTaskNode(reportingTaskComponent, identifier, statelessEngine, flowManager,
            processScheduler, validationContextFactory, componentVariableRegistry, reloadComponent, extensionManager, validationTrigger);

        logger.info("Created Reporting Task of type {} with identifier {}", type, identifier);
        return taskNode;
    }

    private LoggableComponent<ReportingTask> createLoggableReportingTask() throws ReportingTaskInstantiationException {
        try {
            final LoggableComponent<ReportingTask> taskComponent = createLoggableComponent(ReportingTask.class);
            final String taskName = taskComponent.getComponent().getClass().getSimpleName();
            final NodeTypeProvider nodeTypeProvider = new StatelessNodeTypeProvider();

            final ReportingInitializationContext config = new StandardReportingInitializationContext(identifier, taskName,
                SchedulingStrategy.TIMER_DRIVEN, "1 min", taskComponent.getLogger(), statelessEngine.getControllerServiceProvider(),
                statelessEngine.getKerberosConfig(), nodeTypeProvider);

            taskComponent.getComponent().initialize(config);

            return taskComponent;
        } catch (final Exception e) {
            throw new ReportingTaskInstantiationException(type, e);
        }
    }


    public ControllerServiceNode buildControllerService() {
        final ExtensionManager extensionManager = statelessEngine.getExtensionManager();
        final StateManagerProvider stateManagerProvider = statelessEngine.getStateManagerProvider();
        final ControllerServiceProvider serviceProvider = statelessEngine.getControllerServiceProvider();
        final KerberosConfig kerberosConfig = statelessEngine.getKerberosConfig();
        final VariableRegistry rootVariableRegistry = statelessEngine.getRootVariableRegistry();
        final ReloadComponent reloadComponent = statelessEngine.getReloadComponent();
        final ValidationTrigger validationTrigger = statelessEngine.getValidationTrigger();
        final NodeTypeProvider nodeTypeProvider = new StatelessNodeTypeProvider();

        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Bundle bundle = extensionManager.getBundle(bundleCoordinate);
            if (bundle == null) {
                final List<Bundle> possibleBundles = extensionManager.getBundles(type);
                if (possibleBundles.size() == 1) {
                    bundle = possibleBundles.get(0);
                    logger.warn("Flow specifies bundle coordinates of {} for Controller Service of type {} but could not find that Bundle. Will use {} instead", bundleCoordinate, type, bundle);
                } else {
                    throw new IllegalStateException("Unable to find bundle for coordinate " + bundleCoordinate.getCoordinate());
                }
            }

            final Set<URL> classpathUrls = additionalClassPathUrls == null ? Collections.emptySet() : additionalClassPathUrls;
            final ClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(type, identifier, bundle, classpathUrls);
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoader);
            Thread.currentThread().setContextClassLoader(detectedClassLoader);

            final Class<? extends ControllerService> controllerServiceClass = rawClass.asSubclass(ControllerService.class);
            final ControllerService serviceImpl = controllerServiceClass.newInstance();
            final StandardControllerServiceInvocationHandler invocationHandler = new StandardControllerServiceInvocationHandler(extensionManager, serviceImpl);

            // extract all interfaces... controllerServiceClass is non null so getAllInterfaces is non null
            final List<Class<?>> interfaceList = ClassUtils.getAllInterfaces(controllerServiceClass);
            final Class<?>[] interfaces = interfaceList.toArray(new Class<?>[0]);

            final ControllerService proxiedService;
            if (detectedClassLoader == null) {
                proxiedService = (ControllerService) Proxy.newProxyInstance(getClass().getClassLoader(), interfaces, invocationHandler);
            } else {
                proxiedService = (ControllerService) Proxy.newProxyInstance(detectedClassLoader, interfaces, invocationHandler);
            }

            logger.info("Created Controller Service of type {} with identifier {}", type, identifier);
            final ComponentLog serviceLogger = new SimpleProcessLogger(identifier, serviceImpl);
            final TerminationAwareLogger terminationAwareLogger = new TerminationAwareLogger(serviceLogger);

            final StateManager stateManager = stateManagerProvider.getStateManager(identifier);
            final ControllerServiceInitializationContext initContext = new StandardControllerServiceInitializationContext(identifier, terminationAwareLogger,
                serviceProvider, stateManager, kerberosConfig, nodeTypeProvider);
            serviceImpl.initialize(initContext);

            final LoggableComponent<ControllerService> originalLoggableComponent = new LoggableComponent<>(serviceImpl, bundleCoordinate, terminationAwareLogger);
            final LoggableComponent<ControllerService> proxiedLoggableComponent = new LoggableComponent<>(proxiedService, bundleCoordinate, terminationAwareLogger);

            final ComponentVariableRegistry componentVarRegistry = new StandardComponentVariableRegistry(rootVariableRegistry);
            final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(serviceProvider, componentVarRegistry);
            final ControllerServiceNode serviceNode = new StandardControllerServiceNode(originalLoggableComponent, proxiedLoggableComponent, invocationHandler,
                identifier, validationContextFactory, serviceProvider, componentVarRegistry, reloadComponent, extensionManager, validationTrigger);
            serviceNode.setName(rawClass.getSimpleName());

            invocationHandler.setServiceNode(serviceNode);
            return serviceNode;
        } catch (final Exception e) {
            throw new ControllerServiceInstantiationException("Failed to create Controller Service of type " + type, e);
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }

    private LoggableComponent<Processor> createLoggableProcessor() throws ProcessorInstantiationException {
        try {
            final LoggableComponent<Processor> processorComponent = createLoggableComponent(Processor.class);
            final ProcessorInitializationContext initiContext = new StandardProcessorInitializationContext(identifier, processorComponent.getLogger(),
                statelessEngine.getControllerServiceProvider(), new StatelessNodeTypeProvider(), statelessEngine.getKerberosConfig());
            processorComponent.getComponent().initialize(initiContext);

            return processorComponent;
        } catch (final Exception e) {
            throw new ProcessorInstantiationException(type, e);
        }
    }

    private <T extends ConfigurableComponent> LoggableComponent<T> createLoggableComponent(Class<T> nodeType) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final ExtensionManager extensionManager = statelessEngine.getExtensionManager();

            Bundle bundle = extensionManager.getBundle(bundleCoordinate);
            if (bundle == null) {
                final List<Bundle> possibleBundles = extensionManager.getBundles(type);
                if (possibleBundles.size() == 1) {
                    bundle = possibleBundles.get(0);
                    logger.warn("Flow specifies bundle coordinates of {} for {} of type {} but could not find that Bundle. Will use {} instead",
                        bundleCoordinate, nodeType.getSimpleName(), type, bundle);
                } else {
                    throw new IllegalStateException("Unable to find bundle for coordinate " + bundleCoordinate.getCoordinate());
                }
            }

            final Set<URL>  classpathUrls = additionalClassPathUrls == null ? Collections.emptySet() : additionalClassPathUrls;
            final ClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(type, identifier, bundle, classpathUrls);
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoader);
            Thread.currentThread().setContextClassLoader(detectedClassLoader);

            final Object extensionInstance = rawClass.newInstance();
            final ComponentLog componentLog = new SimpleProcessLogger(identifier, extensionInstance);
            final TerminationAwareLogger terminationAwareLogger = new TerminationAwareLogger(componentLog);

            final T cast = nodeType.cast(extensionInstance);
            return new LoggableComponent<>(cast, bundleCoordinate, terminationAwareLogger);
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }
}
