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
package org.apache.nifi.controller;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.reporting.StandardReportingInitializationContext;
import org.apache.nifi.controller.reporting.StandardReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceInvocationHandler;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.GhostControllerService;
import org.apache.nifi.controller.service.StandardControllerServiceInitializationContext;
import org.apache.nifi.controller.service.StandardControllerServiceInvocationHandler;
import org.apache.nifi.controller.service.StandardControllerServiceNode;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.GhostProcessor;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.processor.StandardProcessorInitializationContext;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.variable.StandardComponentVariableRegistry;
import org.apache.nifi.reporting.GhostReportingTask;
import org.apache.nifi.reporting.InitializationException;
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

public class ExtensionBuilder {
    private static final Logger logger = LoggerFactory.getLogger(ExtensionBuilder.class);

    private String type;
    private String identifier;
    private BundleCoordinate bundleCoordinate;
    private ExtensionManager extensionManager;
    private Set<URL> classpathUrls;
    private KerberosConfig kerberosConfig = KerberosConfig.NOT_CONFIGURED;
    private ControllerServiceProvider serviceProvider;
    private NodeTypeProvider nodeTypeProvider;
    private VariableRegistry variableRegistry;
    private ProcessScheduler processScheduler;
    private ValidationTrigger validationTrigger;
    private ReloadComponent reloadComponent;
    private FlowController flowController;
    private StateManagerProvider stateManagerProvider;

    public ExtensionBuilder type(final String type) {
        this.type = type;
        return this;
    }

    public ExtensionBuilder identifier(final String identifier) {
        this.identifier = identifier;
        return this;
    }

    public ExtensionBuilder bundleCoordinate(final BundleCoordinate coordinate) {
        this.bundleCoordinate = coordinate;
        return this;
    }

    public ExtensionBuilder addClasspathUrls(final Set<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            return this;
        }

        if (this.classpathUrls == null) {
            this.classpathUrls = new HashSet<>();
        }

        this.classpathUrls.addAll(urls);
        return this;
    }

    public ExtensionBuilder kerberosConfig(final KerberosConfig kerberosConfig) {
        this.kerberosConfig = kerberosConfig;
        return this;
    }

    public ExtensionBuilder controllerServiceProvider(final ControllerServiceProvider serviceProvider) {
        this.serviceProvider = serviceProvider;
        return this;
    }

    public ExtensionBuilder nodeTypeProvider(final NodeTypeProvider nodeTypeProvider) {
        this.nodeTypeProvider = nodeTypeProvider;
        return this;
    }

    public ExtensionBuilder variableRegistry(final VariableRegistry variableRegistry) {
        this.variableRegistry = variableRegistry;
        return this;
    }

    public ExtensionBuilder processScheduler(final ProcessScheduler scheduler) {
        this.processScheduler = scheduler;
        return this;
    }

    public ExtensionBuilder validationTrigger(final ValidationTrigger validationTrigger) {
        this.validationTrigger = validationTrigger;
        return this;
    }

    public ExtensionBuilder reloadComponent(final ReloadComponent reloadComponent) {
        this.reloadComponent = reloadComponent;
        return this;
    }

    public ExtensionBuilder flowController(final FlowController flowController) {
        this.flowController = flowController;
        return this;
    }

    public ExtensionBuilder stateManagerProvider(final StateManagerProvider stateManagerProvider) {
        this.stateManagerProvider = stateManagerProvider;
        return this;
    }

    public ExtensionBuilder extensionManager(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
        return this;
    }

    public ProcessorNode buildProcessor() {
        if (identifier == null) {
            throw new IllegalStateException("Processor ID must be specified");
        }
        if (type == null) {
            throw new IllegalStateException("Processor Type must be specified");
        }
        if (bundleCoordinate == null) {
            throw new IllegalStateException("Bundle Coordinate must be specified");
        }
        if (extensionManager == null) {
            throw new IllegalStateException("Extension Manager must be specified");
        }
        if (serviceProvider == null) {
            throw new IllegalStateException("Controller Service Provider must be specified");
        }
        if (nodeTypeProvider == null) {
            throw new IllegalStateException("Node Type Provider must be specified");
        }
        if (variableRegistry == null) {
            throw new IllegalStateException("Variable Registry must be specified");
        }
        if (reloadComponent == null) {
            throw new IllegalStateException("Reload Component must be specified");
        }

        boolean creationSuccessful = true;
        LoggableComponent<Processor> loggableComponent;
        try {
            loggableComponent = createLoggableProcessor();
        } catch (final ProcessorInstantiationException pie) {
            logger.error("Could not create Processor of type " + type + " for ID " + identifier + "; creating \"Ghost\" implementation", pie);
            final GhostProcessor ghostProc = new GhostProcessor();
            ghostProc.setIdentifier(identifier);
            ghostProc.setCanonicalClassName(type);
            loggableComponent = new LoggableComponent<>(ghostProc, bundleCoordinate, null);
            creationSuccessful = false;
        }

        final ProcessorNode processorNode = createProcessorNode(loggableComponent, creationSuccessful);
        return processorNode;
    }

    public ReportingTaskNode buildReportingTask() {
        if (identifier == null) {
            throw new IllegalStateException("ReportingTask ID must be specified");
        }
        if (type == null) {
            throw new IllegalStateException("ReportingTask Type must be specified");
        }
        if (bundleCoordinate == null) {
            throw new IllegalStateException("Bundle Coordinate must be specified");
        }
        if (extensionManager == null) {
            throw new IllegalStateException("Extension Manager must be specified");
        }
        if (serviceProvider == null) {
            throw new IllegalStateException("Controller Service Provider must be specified");
        }
        if (nodeTypeProvider == null) {
            throw new IllegalStateException("Node Type Provider must be specified");
        }
        if (variableRegistry == null) {
            throw new IllegalStateException("Variable Registry must be specified");
        }
        if (reloadComponent == null) {
            throw new IllegalStateException("Reload Component must be specified");
        }
        if (flowController == null) {
            throw new IllegalStateException("FlowController must be specified");
        }

        boolean creationSuccessful = true;
        LoggableComponent<ReportingTask> loggableComponent;
        try {
            loggableComponent = createLoggableReportingTask();
        } catch (final ReportingTaskInstantiationException rtie) {
            logger.error("Could not create ReportingTask of type " + type + " for ID " + identifier + "; creating \"Ghost\" implementation", rtie);
            final GhostReportingTask ghostReportingTask = new GhostReportingTask();
            ghostReportingTask.setIdentifier(identifier);
            ghostReportingTask.setCanonicalClassName(type);
            loggableComponent = new LoggableComponent<>(ghostReportingTask, bundleCoordinate, null);
            creationSuccessful = false;
        }

        final ReportingTaskNode taskNode = createReportingTaskNode(loggableComponent, creationSuccessful);
        return taskNode;
    }

    public ControllerServiceNode buildControllerService() {
        if (identifier == null) {
            throw new IllegalStateException("ReportingTask ID must be specified");
        }
        if (type == null) {
            throw new IllegalStateException("ReportingTask Type must be specified");
        }
        if (bundleCoordinate == null) {
            throw new IllegalStateException("Bundle Coordinate must be specified");
        }
        if (extensionManager == null) {
            throw new IllegalStateException("Extension Manager must be specified");
        }
        if (serviceProvider == null) {
            throw new IllegalStateException("Controller Service Provider must be specified");
        }
        if (nodeTypeProvider == null) {
            throw new IllegalStateException("Node Type Provider must be specified");
        }
        if (variableRegistry == null) {
            throw new IllegalStateException("Variable Registry must be specified");
        }
        if (reloadComponent == null) {
            throw new IllegalStateException("Reload Component must be specified");
        }
        if (stateManagerProvider == null) {
            throw new IllegalStateException("State Manager Provider must be specified");
        }

        try {
            return createControllerServiceNode();
        } catch (final Exception e) {
            logger.error("Could not create Controller Service of type " + type + " for ID " + identifier + "; creating \"Ghost\" implementation", e);
            return createGhostControllerServiceNode();
        }
    }


    private ProcessorNode createProcessorNode(final LoggableComponent<Processor> processor, final boolean creationSuccessful) {
        final ComponentVariableRegistry componentVarRegistry = new StandardComponentVariableRegistry(this.variableRegistry);
        final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(serviceProvider, componentVarRegistry);

        final ProcessorNode procNode;
        if (creationSuccessful) {
            procNode = new StandardProcessorNode(processor, identifier, validationContextFactory, processScheduler, serviceProvider,
                componentVarRegistry, reloadComponent, extensionManager, validationTrigger);
        } else {
            final String simpleClassName = type.contains(".") ? StringUtils.substringAfterLast(type, ".") : type;
            final String componentType = "(Missing) " + simpleClassName;
            procNode = new StandardProcessorNode(processor, identifier, validationContextFactory, processScheduler, serviceProvider,
                componentType, type, componentVarRegistry, reloadComponent, extensionManager, validationTrigger, true);
        }

        applyDefaultSettings(procNode);
        return procNode;
    }


    private ReportingTaskNode createReportingTaskNode(final LoggableComponent<ReportingTask> reportingTask, final boolean creationSuccessful) {
        final ComponentVariableRegistry componentVarRegistry = new StandardComponentVariableRegistry(this.variableRegistry);
        final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(serviceProvider, componentVarRegistry);
        final ReportingTaskNode taskNode;
        if (creationSuccessful) {
            taskNode = new StandardReportingTaskNode(reportingTask, identifier, flowController, processScheduler,
                validationContextFactory, componentVarRegistry, reloadComponent, extensionManager, validationTrigger);
            taskNode.setName(taskNode.getReportingTask().getClass().getSimpleName());
        } else {
            final String simpleClassName = type.contains(".") ? StringUtils.substringAfterLast(type, ".") : type;
            final String componentType = "(Missing) " + simpleClassName;

            taskNode = new StandardReportingTaskNode(reportingTask, identifier, flowController, processScheduler, validationContextFactory,
                componentType, type, componentVarRegistry, reloadComponent, extensionManager, validationTrigger, true);
            taskNode.setName(componentType);
        }

        return taskNode;
    }

    private void applyDefaultSettings(final ProcessorNode processorNode) {
        try {
            final Class<?> procClass = processorNode.getProcessor().getClass();

            final DefaultSettings ds = procClass.getAnnotation(DefaultSettings.class);
            if (ds != null) {
                processorNode.setYieldPeriod(ds.yieldDuration());
                processorNode.setPenalizationPeriod(ds.penaltyDuration());
                processorNode.setBulletinLevel(ds.bulletinLevel());
            }
        } catch (final Exception ex) {
            logger.error("Error while setting default settings from DefaultSettings annotation: {}", ex.toString(), ex);
        }
    }

    private ControllerServiceNode createControllerServiceNode() throws ClassNotFoundException, IllegalAccessException, InstantiationException, InitializationException {
        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final Bundle bundle = extensionManager.getBundle(bundleCoordinate);
            if (bundle == null) {
                throw new IllegalStateException("Unable to find bundle for coordinate " + bundleCoordinate.getCoordinate());
            }

            final ClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(type, identifier, bundle, classpathUrls == null ? Collections.emptySet() : classpathUrls);
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
                serviceProvider, stateManager, kerberosConfig);
            serviceImpl.initialize(initContext);

            final LoggableComponent<ControllerService> originalLoggableComponent = new LoggableComponent<>(serviceImpl, bundleCoordinate, terminationAwareLogger);
            final LoggableComponent<ControllerService> proxiedLoggableComponent = new LoggableComponent<>(proxiedService, bundleCoordinate, terminationAwareLogger);

            final ComponentVariableRegistry componentVarRegistry = new StandardComponentVariableRegistry(this.variableRegistry);
            final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(serviceProvider, componentVarRegistry);
            final ControllerServiceNode serviceNode = new StandardControllerServiceNode(originalLoggableComponent, proxiedLoggableComponent, invocationHandler,
                identifier, validationContextFactory, serviceProvider, componentVarRegistry, reloadComponent, extensionManager, validationTrigger);
            serviceNode.setName(rawClass.getSimpleName());

            invocationHandler.setServiceNode(serviceNode);
            return serviceNode;
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }

    private ControllerServiceNode createGhostControllerServiceNode() {
        final String simpleClassName = type.contains(".") ? StringUtils.substringAfterLast(type, ".") : type;
        final String componentType = "(Missing) " + simpleClassName;

        final GhostControllerService ghostService = new GhostControllerService(identifier, type);
        final LoggableComponent<ControllerService> proxiedLoggableComponent = new LoggableComponent<>(ghostService, bundleCoordinate, null);

        final ControllerServiceInvocationHandler invocationHandler = new StandardControllerServiceInvocationHandler(extensionManager, ghostService);

        final ComponentVariableRegistry componentVarRegistry = new StandardComponentVariableRegistry(this.variableRegistry);
        final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(serviceProvider, variableRegistry);
        final ControllerServiceNode serviceNode = new StandardControllerServiceNode(proxiedLoggableComponent, proxiedLoggableComponent, invocationHandler, identifier,
            validationContextFactory, serviceProvider, componentType, type, componentVarRegistry, reloadComponent, extensionManager, validationTrigger, true);

        return serviceNode;
    }

    private LoggableComponent<Processor> createLoggableProcessor() throws ProcessorInstantiationException {
        try {
            final LoggableComponent<Processor> processorComponent = createLoggableComponent(Processor.class);

            final ProcessorInitializationContext initiContext = new StandardProcessorInitializationContext(identifier, processorComponent.getLogger(),
                serviceProvider, nodeTypeProvider, kerberosConfig);
            processorComponent.getComponent().initialize(initiContext);

            return processorComponent;
        } catch (final Exception e) {
            throw new ProcessorInstantiationException(type, e);
        }
    }


    private LoggableComponent<ReportingTask> createLoggableReportingTask() throws ReportingTaskInstantiationException {
        try {
            final LoggableComponent<ReportingTask> taskComponent = createLoggableComponent(ReportingTask.class);

            final String taskName = taskComponent.getComponent().getClass().getSimpleName();
            final ReportingInitializationContext config = new StandardReportingInitializationContext(identifier, taskName,
                SchedulingStrategy.TIMER_DRIVEN, "1 min", taskComponent.getLogger(), serviceProvider, kerberosConfig, nodeTypeProvider);

            taskComponent.getComponent().initialize(config);

            return taskComponent;
        } catch (final Exception e) {
            throw new ReportingTaskInstantiationException(type, e);
        }
    }

    private <T extends ConfigurableComponent> LoggableComponent<T> createLoggableComponent(Class<T> nodeType) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final Bundle bundle = extensionManager.getBundle(bundleCoordinate);
            if (bundle == null) {
                throw new IllegalStateException("Unable to find bundle for coordinate " + bundleCoordinate.getCoordinate());
            }

            final ClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(type, identifier, bundle, classpathUrls == null ? Collections.emptySet() : classpathUrls);
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
